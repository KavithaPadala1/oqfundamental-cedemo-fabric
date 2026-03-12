import logging
import os
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from threading import Lock, Thread, Event, Timer
import atexit
import queue

class BlobLogHandler(logging.Handler):
    """Custom logging handler that writes logs directly to Azure Blob Storage"""
    
    def __init__(self):
        super().__init__()
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = "gasopsoqfundamentallogs"
        self.log_queue = queue.Queue()
        self.flush_delay = 5  # Write 5 seconds after last log
        self.retention_days = 30
        self.logs_received = 0
        self._flush_timer = None
        self._timer_lock = Lock()
        
        if not self.connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found")
        
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            self._ensure_container_exists()
        except Exception as e:
            print(f"[BlobLogHandler] Failed to initialize blob client: {e}")
            self.blob_service_client = None
            return
        
        # Use Event for cleanup thread
        self._running = Event()
        self._running.set()
        
        # Start cleanup thread (runs once per day)
        self._cleanup_thread = Thread(target=self._periodic_cleanup, daemon=True)
        self._cleanup_thread.start()
        
        # Register cleanup on exit (for when you stop development server)
        atexit.register(self.close)
        
        print(f"[BlobLogHandler] Initialized - writes {self.flush_delay}s after last log, retention {self.retention_days} days")
    
    def _ensure_container_exists(self):
        """Create container if it doesn't exist"""
        try:
            self.blob_service_client.create_container(self.container_name)
            print(f"[BlobLogHandler] Container created")
        except ResourceExistsError:
            print(f"[BlobLogHandler] Container exists")
        except Exception as e:
            print(f"[BlobLogHandler] Container check warning: {e}")
    
    def _schedule_flush(self):
        """Schedule a flush to occur after flush_delay seconds of inactivity"""
        with self._timer_lock:
            # Cancel existing timer if any
            if self._flush_timer is not None:
                self._flush_timer.cancel()
            
            # Schedule new flush - will write logs 5 seconds after last log received
            self._flush_timer = Timer(self.flush_delay, self._flush_to_blob)
            self._flush_timer.daemon = True
            self._flush_timer.start()
    
    def _periodic_cleanup(self):
        """Delete logs older than retention_days (runs once per day)"""
        while self._running.is_set():
            try:
                self._delete_old_logs()
                # Wait 24 hours or until shutdown
                if self._running.wait(timeout=86400):
                    break
            except Exception as e:
                print(f"[BlobLogHandler] Cleanup error: {e}")
                if self._running.wait(timeout=3600):
                    break
    
    def _delete_old_logs(self):
        """Delete log files older than retention_days"""
        if not self.blob_service_client:
            return
            
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            cutoff_date = datetime.now() - timedelta(days=self.retention_days)
            
            blobs = container_client.list_blobs(name_starts_with="terminal_logs/")
            deleted_count = 0
            
            for blob in blobs:
                if blob.last_modified.replace(tzinfo=None) < cutoff_date:
                    blob_client = container_client.get_blob_client(blob.name)
                    blob_client.delete_blob()
                    deleted_count += 1
            
            if deleted_count > 0:
                print(f"[BlobLogHandler] Deleted {deleted_count} old logs")
                
        except Exception as e:
            print(f"[BlobLogHandler] Delete error: {e}")
    
    def emit(self, record):
        """Called for each log record - schedules delayed flush"""
        if not self.blob_service_client:
            return
        
        # Ignore logs from this handler itself to prevent infinite loop
        if record.name == 'stdout' and '[BlobLogHandler]' in record.getMessage():
            return
        
        try:
            log_entry = self.format(record)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            formatted_log = f"[{timestamp}] {log_entry}\n"
            
            # Add to queue
            self.log_queue.put_nowait(formatted_log)
            self.logs_received += 1
            
            # Schedule flush (resets timer each time a log comes in)
            self._schedule_flush()
            
            # Debug every 50 logs
            if self.logs_received % 50 == 0:
                print(f"[BlobLogHandler] Received {self.logs_received} logs, flush scheduled in {self.flush_delay}s")
                    
        except Exception as e:
            print(f"[BlobLogHandler] Emit error: {e}")
    
    def _flush_to_blob(self):
        """Write queued logs to blob storage"""
        if not self.blob_service_client or self.log_queue.empty():
            return
        
        # Collect all logs from queue
        logs_to_write = []
        while not self.log_queue.empty():
            try:
                logs_to_write.append(self.log_queue.get_nowait())
            except queue.Empty:
                break
        
        if not logs_to_write:
            return
        
        try:
            date_str = datetime.now().strftime("%Y-%m-%d")
            blob_name = f"terminal_logs/logs_{date_str}.log"
            
            print(f"[BlobLogHandler] Writing {len(logs_to_write)} logs to {blob_name}...")
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            # Get existing content
            existing_content = ""
            try:
                download_stream = blob_client.download_blob()
                existing_content = download_stream.readall().decode('utf-8')
            except ResourceNotFoundError:
                pass
            except Exception as e:
                print(f"[BlobLogHandler] Download error: {e}")
            
            # Append new logs
            new_content = existing_content + "".join(logs_to_write)
            blob_client.upload_blob(new_content.encode('utf-8'), overwrite=True)
            
            print(f"[BlobLogHandler] SUCCESS - Wrote {len(logs_to_write)} logs (Total: {self.logs_received})")
            
        except Exception as e:
            print(f"[BlobLogHandler] Flush error: {e}")
            import traceback
            traceback.print_exc()
    
    def flush(self):
        """Flush remaining logs immediately"""
        with self._timer_lock:
            if self._flush_timer is not None:
                self._flush_timer.cancel()
                self._flush_timer = None
        
        if self.blob_service_client and not self.log_queue.empty():
            print(f"[BlobLogHandler] Manual flush - {self.log_queue.qsize()} logs")
            self._flush_to_blob()
        super().flush()
    
    def close(self):
        """Close handler and flush remaining logs (called when dev server stops)"""
        if not self._running.is_set():
            return
            
        print("[BlobLogHandler] Closing...")
        self._running.clear()
        
        # Cancel timer and flush immediately
        with self._timer_lock:
            if self._flush_timer is not None:
                self._flush_timer.cancel()
        
        self.flush()
        
        # Wait for cleanup thread
        if hasattr(self, '_cleanup_thread') and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=2)
        
        print(f"[BlobLogHandler] Closed - Total logs: {self.logs_received}")
        super().close()