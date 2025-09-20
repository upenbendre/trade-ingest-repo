#!/usr/bin/env bash
set -euo pipefail

# optionally override container names here
export PULSAR_CONTAINER="${PULSAR_CONTAINER:-pulsar}"
export VAULT_CONTAINER="${VAULT_CONTAINER:-vault}"
export PG_CONTAINER="${PG_CONTAINER:-postgres}"   # set to '' if using host Postgres

./pulsar_setup.sh
./vault_setup.sh

# If Postgres running on docker container on local
# export PG_CONTAINER="postgres"   # or your container name
# ./postgres_check.sh

# If Postgres running on a laptop.
 export PG_DB=mydb
./postgres_check-local.sh   

echo "All steps executed."
