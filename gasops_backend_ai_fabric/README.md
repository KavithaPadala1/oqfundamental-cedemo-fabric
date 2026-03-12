# GasOps Transmission AI Backend - Fabric
# Overview
FastAPI-based backend service for GasOps Transmission AI, providing intelligent and conversational natural language query processing using Azure and Fabric.

# Prerequisites
- Python 3.12.5 
- Azure CLI (for Docker deployment) 
- Docker (for containerized deployment)

# Local Development Setup ( Windows)
1. Create Python Virtual Environment
   ```bash
   python -m venv venv
2. Activate virtual environment
   ```bash
   venv\Scripts\activate
3. Install Dependencies
   ```bash
   cd gasops_backend_ai_fabric
   
   pip install -r requirements.txt
5. Configure Environment Variables
   Create a .env file in the project root with necessary configuration
6. Run the Application
   Start the FastAPI server
   ```bash
   uvicorn main:app
7. The application will be available at:
- API: http://localhost:8000
- Interactive API docs (swagger): http://localhost:8000/docs
