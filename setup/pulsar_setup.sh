#!/usr/bin/env bash
set -euo pipefail

# === Edit these to match your environment ===
PULSAR_CONTAINER="${PULSAR_CONTAINER:-pulsar}"        # docker container name for Pulsar
PULSAR_ADMIN_CMD="${PULSAR_ADMIN_CMD:-pulsar-admin}" # CLI command inside container (usually pulsar-admin)
PULSAR_CLIENT_CMD="${PULSAR_CLIENT_CMD:-pulsar-client}"
TENANT="${TENANT:-front-office}"
NAMESPACE="${NAMESPACE:-trades}"
INCOMING_TOPIC="${INCOMING_TOPIC:-persistent://front-office/trades/trades.incoming.normal}"
DLQ_TOPIC="${DLQ_TOPIC:-persistent://front-office/trades/ingest.dlq}"
PARTITIONS="${PARTITIONS:-32}"

# Helper to run a command inside Pulsar container
run_in_pulsar() {
  docker exec -i "${PULSAR_CONTAINER}" bash -lc "$*"
}

echo ">>> Creating tenant ${TENANT} (idempotent)"
run_in_pulsar "${PULSAR_ADMIN_CMD} tenants create ${TENANT} --allowed-clusters standalone --admin-roles admin || true"

echo ">>> Creating namespace ${TENANT}/${NAMESPACE}"
run_in_pulsar "${PULSAR_ADMIN_CMD} namespaces create ${TENANT}/${NAMESPACE} || true"

echo ">>> Setting optional retention for ${TENANT}/${NAMESPACE}"
run_in_pulsar "${PULSAR_ADMIN_CMD} namespaces set-retention ${TENANT}/${NAMESPACE} --size 10M --time 72h || true"

echo ">>> Creating partitioned topic ${INCOMING_TOPIC} with ${PARTITIONS} partitions"
run_in_pulsar "${PULSAR_ADMIN_CMD} topics create-partitioned-topic ${INCOMING_TOPIC} -p ${PARTITIONS} || true"

echo ">>> Creating DLQ topic ${DLQ_TOPIC} (non-partitioned)"
run_in_pulsar "${PULSAR_ADMIN_CMD} topics create ${DLQ_TOPIC} || true"

echo ">>> Producing a smoke message to incoming topic"
run_in_pulsar "${PULSAR_CLIENT_CMD} produce ${INCOMING_TOPIC} -m '{\"trade_id\":\"T-0001\",\"version\":1,\"maturity_date\":\"2026-01-01\",\"payload\":{}}' || true"

echo ">>> Producing a smoke message to DLQ topic"
run_in_pulsar "${PULSAR_CLIENT_CMD} produce ${DLQ_TOPIC} -m '{\"test\":\"dlq\"}' || true"

echo ">>> Consuming one message from incoming topic (smoke read)"
# uses -n 1 to read one message, subscription name smoke-check
run_in_pulsar "${PULSAR_CLIENT_CMD} consume ${INCOMING_TOPIC} -s smoke-check -n 1 || true"

echo "Pulsar setup complete."
