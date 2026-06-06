# Acme Ltd - Financial Market Data Warehouse

An enterprise-grade, big-data platform engineered for high-throughput financial data ingestion, temporal/bi-temporal asset versioning, and advanced analytical workflows.

## 🛠️ Infrastructure & Requirements
- **Runtime Environment:** Python 3.11+
- **Containerization:** Docker Desktop (Engine must be running)
- **Database Engine:** MongoDB 7.0+ (Deployed via container)
- **Data Processing Engine:** Apache Spark / PySpark

## 🚀 Deployment & Execution Instructions

1. **Spin up the NoSQL Database Container:**
   Launch the containerized MongoDB infrastructure in detached mode using Docker Compose:
   ```bash
   docker compose up -d

2. **Install Python Libraries:**
pip install -r requirements.txt

3. **Initialize & Seed the Data Warehouse:**
python database.py

4. **Run the Core Test Suite:**
python test_dal.py

5. **Start the Production Core REST API:**
uvicorn app:app --reload

6. **Initialize the Agentic AI Interface (MCP Server):**
python mcp_server.py


