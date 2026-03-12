# # FROM python:3.12-slim

# # # Set working directory
# # WORKDIR /app

# # # Install system dependencies for pyodbc and ODBC drivers
# # RUN apt-get update && apt-get install -y \
# #     curl \
# #     gnupg2 \
# #     apt-transport-https \
# #     unixodbc \
# #     unixodbc-dev \
# #     && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
# #     && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
# #     && apt-get update \
# #     && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
# #     && apt-get clean \
# #     && rm -rf /var/lib/apt/lists/*

# # # Copy requirements file
# # COPY requirements.txt .

# # # Install Python dependencies
# # RUN pip install --no-cache-dir -r requirements.txt

# # # Copy application code
# # COPY . .

# # # Expose port
# # EXPOSE 8000

# # # Set environment variables
# # ENV PYTHONUNBUFFERED=1
# # ENV PYTHONDONTWRITEBYTECODE=1

# # # Run the application
# # CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]



# FROM python:3.12-slim

# # Set working directory
# WORKDIR /app

# # Install system dependencies for pyodbc, ODBC drivers, and Azure CLI
# RUN apt-get update && apt-get install -y \
#     curl \
#     gnupg2 \
#     apt-transport-https \
#     unixodbc \
#     unixodbc-dev \
#     ca-certificates \
#     lsb-release \
#     && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
#     && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
#     && apt-get update \
#     && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
#     && curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# # Copy requirements file
# COPY requirements.txt .

# # Install Python dependencies
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy application code
# COPY . .

# # Expose port
# EXPOSE 8000

# # Set environment variables
# ENV PYTHONUNBUFFERED=1
# ENV PYTHONDONTWRITEBYTECODE=1

# # Run the application
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]




FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies for pyodbc, ODBC drivers, Azure CLI, and LibreOffice
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    apt-transport-https \
    unixodbc \
    unixodbc-dev \
    ca-certificates \
    lsb-release \
    libreoffice \
    libreoffice-writer \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]