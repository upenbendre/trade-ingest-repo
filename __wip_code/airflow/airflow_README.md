# Configuration notes:
'''
Airflow Connections required:
1. flink_api — Flink REST base host (host, port, schema, optional basic auth).
2. ingest_controller_api — optional ingestion controller endpoint and auth.
3. _postgres — Postgres-style connection pointing at Yugabyte.

Environment variables:
1. USE_INGEST_CONTROLLER (default "true") — choose ingest controller path.
2. USE_DIRECT_FLINK (optional) — use direct Flink REST submission.
3. UPLOAD_JAR (default "false") — set to "true" to upload jar from worker.
4. FLINK_JAR_FILE — path to jar on Airflow worker (when UPLOAD_JAR=true).
5. FLINK_JAR_ID — if using USE_DIRECT_FLINK without upload, set jar id.
6. BULK_JOB_TIMEOUT_MIN, BULK_JOB_POLL_SEC — monitoring tuning.
'''

# Ingest Audit table DDL used in the _finalize step of the DAG:
-- create audit table for bulk ingest DAG (YugabyteDB - Postgres API)
CREATE TABLE IF NOT EXISTS ingest_audit (
  id BIGSERIAL PRIMARY KEY,
  correlation_id TEXT NOT NULL,
  job_id TEXT,
  manifest_path TEXT,
  status TEXT,
  details JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- index to support lookups by correlation_id
CREATE INDEX IF NOT EXISTS idx_ingest_audit_correlation_id ON ingest_audit (correlation_id);

-- optional index by created_at if you query recent jobs often
CREATE INDEX IF NOT EXISTS idx_ingest_audit_created_at ON ingest_audit (created_at DESC);

-- example grant (adjust role name 'app_user' to your application user)
GRANT INSERT, SELECT ON ingest_audit TO app_user;

# Connection imports:
# 1. For flink_api (REST API call):

{
  "conn_id": "flink_api",
  "conn_type": "http",
  "host": "flink-jobmanager.example.internal",
  "port": 8081,
  "schema": "http",
  "login": "flink_user",
  "password": "FLINK_PASSWORD",
  "extra": "{\"verify\": true}"
}

# 2. For ingestion_controller_api:

{
  "conn_id": "ingest_controller_api",
  "conn_type": "http",
  "host": "ingest-controller.example.internal",
  "port": 8080,
  "schema": "https",
  "password": "YOUR_INGEST_CONTROLLER_TOKEN",
  "extra": "{\"verify\": true}"
}

# 3. Postgres connection

{
  "conn_id": "yugabyte_postgres",
  "conn_type": "postgres",
  "host": "yugabyte-primary.example.internal",
  "port": 5433,
  "schema": "your_db",
  "login": "app_user",
  "password": "APP_USER_PASSWORD",
  "extra": "{\"sslmode\":\"require\"}"
}

# Out of Scope for now:
# Configuration of HashiCorp Vault secrets backend or K8S secrets backend in Airflow, along with storage of p8 key files on k8s nodes.

# Testing steps:
'''
Create ingest_audit table in Yugabyte (run the DDL above).
Add Airflow connections with the commands above (or use your secrets backend).
Set environment variables in Airflow (via Kubernetes env, systemd unit, or Airflow variables):
USE_INGEST_CONTROLLER (true/false)
USE_DIRECT_FLINK (true/false)
UPLOAD_JAR (true/false)
FLINK_JAR_FILE (if uploading)
FLINK_JAR_ID (if using direct submit without upload)
BULK_JOB_TIMEOUT_MIN, BULK_JOB_POLL_SEC (monitoring tune)
Deploy the DAG file (airflow_bulk_ingest_dag_full.py) to your Airflow DAGs folder and verify the DAG appears in the UI.
Run a manual test DAG run using airflow dags trigger --conf '{"manifest_path":"s3://...","correlation_id":"test-1"}' bulk_ingest.
Verify audit row is written to Yugabyte after DAG completes.
'''