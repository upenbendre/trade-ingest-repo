#!/usr/bin/env bash
set -euo pipefail

# === Edit these ===
VAULT_CONTAINER="${VAULT_CONTAINER:-vault}"  # name of Vault container (if empty, uses host vault CLI)
VAULT_TOKEN="${VAULT_TOKEN:-}"               # set to your Vault token or export in environment
VAULT_ADDR="${VAULT_ADDR:-http://127.0.0.1:8200}"
# secret values to write (change if needed)
PULSAR_SERVICE_URL="${PULSAR_SERVICE_URL:-pulsar://127.0.0.1:6650}"
PULSAR_ADMIN_URL="${PULSAR_ADMIN_URL:-http://127.0.0.1:8080}"
PULSAR_DLQ_TOPIC="${PULSAR_DLQ_TOPIC:-persistent://front-office/trades/ingest.dlq}"
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-trades}"
PG_USER="${PG_USER:-tradeuser}"
PG_PASS="${PG_PASS:-tradeuser}"

# Vault path to write to (KV v2). The PyFlink code expects secret/data/trade/ingest form;
# 'vault kv put secret/trade/ingest' is the CLI way for kv v2 at mount 'secret'.
SECRET_PATH="secret/trade/ingest"

# helper to run vault CLI either in container or host
vault_cmd() {
  if [ -n "${VAULT_CONTAINER:-}" ]; then
    # run inside container ensuring VAULT_ADDR and token env are set for that process
    docker exec -i "${VAULT_CONTAINER}" bash -lc "export VAULT_ADDR='${VAULT_ADDR}'; export VAULT_TOKEN='${VAULT_TOKEN}'; vault $*"
  else
    export VAULT_ADDR="${VAULT_ADDR}"
    export VAULT_TOKEN="${VAULT_TOKEN}"
    vault "$@"
  fi
}

if [ -z "${VAULT_TOKEN}" ]; then
  echo "WARNING: VAULT_TOKEN is empty. Ensure Vault token available in VAULT_TOKEN env var or set VAULT_CONTAINER to run interactive vault login."
fi

echo ">>> Writing secrets to Vault at ${SECRET_PATH}"
vault_cmd kv put "${SECRET_PATH}" \
  pulsar_service_url="${PULSAR_SERVICE_URL}" \
  pulsar_admin_url="${PULSAR_ADMIN_URL}" \
  pulsar_dql_topic="${PULSAR_DLQ_TOPIC}" \
  pg_host="${PG_HOST}" \
  pg_port="${PG_PORT}" \
  pg_database="${PG_DB}" \
  pg_user="${PG_USER}" \
  pg_password="${PG_PASS}"

echo ">>> Reading back secret (kv get) for verification"
vault_cmd kv get "${SECRET_PATH}"

echo "Vault secret write complete."
