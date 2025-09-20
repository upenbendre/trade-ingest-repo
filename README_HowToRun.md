# ############ TRADE INGEST REPO  - PyFlink (Pulsar -> Flink -> Postgres) ############

# This repository contains a production-ready PyFlink-based ingestion pipeline for trade messages on Pulsar.

# Features
# 1 Pulsar source (partitioned topic)
# 2 Pre-filtering in Flink keyed state to reject lower-version / invalid maturity trades
# 3 Postgres idempotent UPSERT sink (INSERT ... ON CONFLICT DO UPDATE)
# 4 Postgres DB-level guardrail triggers to enforce rejection rules (DDL provided)
# 5 Dead-letter queue (DLQ) published to Pulsar for rejected messages
# 6 Vault integration: credentials sourced from HashiCorp Vault (or environment variables)
# 6 Docker Compose for local dev: Pulsar standalone, Vault dev, Flink JobManager/TaskManager
# 7 PyTest integration tests (disabled by default unless RUN_INTEGRATION=true)
# 8 GitHub Actions CI: lint + bandit security scan + docker build + manual integration job

# -----------------------------------------------------------------------------------------------
##  A. LOCAL DEPLOYMENT 
# -----------------------------------------------------------------------------------------------

# 1. Clone this repo (or download the ZIP) and enter folder.

# 2. Prepare connectors and build Flink image (downloads connector jars)
```bash
chmod +x flink/plugins-download.sh
./flink/plugins-download.sh
docker build -f Dockerfile.flink -t local/flink-with-pulsar:1.16 .
```

# 3. Start local services (Postgres is external; if you want local Postgres use your own instance)
```bash
docker-compose up -d
```

# 4. Bootstrap Vault (writes sample secrets for local dev)
```bash
chmod +x vault/bootstrap_vault.sh
./vault/bootstrap_vault.sh
```
# Login to Vault localhost:8200 with option Token, and value 'root' to see and edit the secrets. In production, Vault token generation is a two step process.

# 5. Apply SQL DDL to Postgres
```bash
psql -h <pg_host> -U <pg_user> -d tradedb -f setup/sql/trades_hash_partition.sql
```

# 6. Submit the Flink job (example)
# You can run the job inside the Flink cluster (ensure connectors in lib/ and Python requirements installed in the image).
# See `jobs/trade_ingest_job.py` for the job code.

# ------------------------------------------------------------------------------------------------------------

## Notes

# Replace secrets in Vault with production values.
# Ensure Pulsar topic `persistent://front-office/trades/trades.incoming.normal` exists with 32 partitions.
# Ensure DLQ topic `persistent://front-office/trades/ingest.dlq` exists (partitioning optional).
# The Flink connector versions are selected for Flink 1.16.x. If you upgrade Flink, update `flink/plugins-download.sh` accordingly.

# ------------------------------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------------
## B. LOCAL ENVIRONMENT SETUP
# -----------------------------------------------------------------------------------------------

# 1) Download connectors into ./flink/plugins
chmod +x flink/plugins-download.sh
./flink/plugins-download.sh

# 2) Build images (no-cache to ensure fresh test)
docker compose build --no-cache

# 3) Start services
docker compose up -d

# 4) Tail logs if needed
docker compose logs -f flink-jobmanager
docker compose logs -f vault

# 5) Application set up to be run from local
cd setup
chmod +x pulsar_setup.sh vault_setup.sh postgres_check.sh run_all.sh

# Container names
export PULSAR_CONTAINER="pulsar"
export VAULT_CONTAINER="vault-1"
# Leave PG_CONTAINER empty if Postgres is on host, otherwise set to container name (e.g. "postgres")
export PG_CONTAINER=""

# Vault token (REPLACE with your token)
export VAULT_TOKEN="REPLACE_WITH_YOUR_VAULT_TOKEN"

# optional overrides for DB values if different from defaults (edit if needed)
export PG_HOST="127.0.0.1"
export PG_PORT="5432"
export PG_DB="trades"
export PG_USER="tradeuser"
export PG_PASS="tradeuser"

# optional Pulsar values (defaults used by scripts are fine)
export PULSAR_SERVICE_URL="pulsar://127.0.0.1:6650"
export PULSAR_ADMIN_URL="http://127.0.0.1:8080"
export PULSAR_DLQ="persistent://front-office/trades/ingest.dlq"

# Replace REPLACE_WITH_YOUR_VAULT_TOKEN with your actual Vault token (the token must have permissions to write/read secret/trade/ingest

   # Run Pulsar setup (tenant/namespace/topics + smoke publish/consume) 
   # Run Vault setup (writes secrets to KV v2 at secret/trade/ingest)
   # Run Postgres check (create user/db idempotently and test connection)

# Run all set up scripts
./run_all.sh

# -----------------------------------------------------------------------------------------------
##  C. RUN TESTS :
# -----------------------------------------------------------------------------------------------

# 1. Run Unit Tests:
python3 -m pip install --user pytest  # optional, but unittest is stdlib
# No external libs required for the provided tests (uses unittest.mock, stdlib)
python3 -m unittest test_trade_job.py -v

# 2. Run integration tests (optional - local)
```bash
RUN_INTEGRATION=true pytest -q tests/test_integration.py -s
```
# 3. For additional random test data generation, use the Polars based gen_trades.py . It can produce json lines files with 100k messages in few seconds.