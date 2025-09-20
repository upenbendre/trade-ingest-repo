#!/usr/bin/env bash
set -euo pipefail
VAULT_ADDR=${VAULT_ADDR:-http://127.0.0.1:8200}
VAULT_TOKEN=${VAULT_TOKEN:-root}
export VAULT_ADDR
export VAULT_TOKEN
curl -s --header "X-Vault-Token: $VAULT_TOKEN" --request POST \
  --data '{"data": {"pulsar_service_url": "pulsar://pulsar:6650", "pulsar_admin_url": "http://pulsar:8080", "pulsar_dql_topic": "persistent://front-office/trades/ingest.dlq", "pg_host": "host.docker.internal", "pg_port": "5432", "pg_database": "tradedb", "pg_user": "trader", "pg_password": "changeme"}}' \
  $VAULT_ADDR/v1/secret/data/trade/ingest

echo "Wrote sample secrets to Vault at secret/data/trade/ingest (dev only)."
