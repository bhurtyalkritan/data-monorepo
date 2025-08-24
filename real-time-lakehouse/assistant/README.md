# LLM Data Assistant Setup

## Required Environment Variables (.env)

```bash
# Databricks SQL Warehouse
DATABRICKS_HOST=adb-xxxxx.azuredatabricks.net
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxx
DATABRICKS_TOKEN=dapi-xxxxx

# Model Serving (Llama)
MODEL_ENDPOINT_URL=https://adb-xxxxx.azuredatabricks.net/serving-endpoints/llama-2-70b/invocations
MODEL_TOKEN=dapi-xxxxx  # or same as DATABRICKS_TOKEN

# Vector Search
VS_ENDPOINT=https://adb-xxxxx.azuredatabricks.net/api/2.0/vector-search/endpoints/vs-endpoint
VS_INDEX=demo.ecommerce_rt.kb_docs_index

# Optional overrides
CATALOG=demo
SCHEMA=ecommerce_rt
```

## Setup Steps

1. **Knowledge Base**: Run `sql/kb_docs.sql` to create and seed the knowledge table

2. **Vector Search Index**: 
   - Create a VS endpoint in Databricks
   - Create an index on `demo.ecommerce_rt.kb_docs` table (sync enabled)
   - Index the `body` column with metadata: `kind`, `table_name`, `column_name`

3. **Model Serving**:
   - Deploy Llama (or DBRX) via Model Serving
   - Get the endpoint URL and ensure token has access

4. **Run Notebook**: Open `assistant/assistant_orchestrator.ipynb` and run cells 1-3 to validate setup

## REST API

Start the FastAPI service:
```bash
cd services
pip install -r requirements.txt
uvicorn assistant_api:app --reload --port 8000
```

Test:
```bash
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "What was GMV in the last 15 minutes?"}'
```

## Demo Flow

1. **Setup question**: "What was GMV over the last 15 minutes?"
2. **Drill down**: "Break that by conversion rate trends"  
3. **Investigate**: "Show 20 sample purchases from the peak minute"
4. **Governance**: Show generated SQL, guardrails, and logs table
