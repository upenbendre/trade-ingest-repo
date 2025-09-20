#!/usr/bin/env bash
set -euo pipefail

# === Edit these ===
PG_CONTAINER="${PG_CONTAINER:-}"  # set to postgres container name if Postgres is a container, leave empty to use host psql
PG_SUPERUSER="${PG_SUPERUSER:-postgres}"   # superuser to run create user/db (if using host psql)
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-trades}"
PG_USER="${PG_USER:-tradeuser}"
PG_PASS="${PG_PASS:-tradeuser}"

# Run arbitrary psql in container or host
run_psql() {
  if [ -n "${PG_CONTAINER}" ]; then
    docker exec -i "${PG_CONTAINER}" psql -U "${PG_SUPERUSER}" -d "${PG_DB}" -c "$1"
  else
    PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -d "${PG_DB}" -c "$1"
  fi
}

echo ">>> Creating role ${PG_USER} (idempotent)"
if [ -n "${PG_CONTAINER}" ]; then
  docker exec -i "${PG_CONTAINER}" bash -lc "psql -U ${PG_SUPERUSER} -c \"CREATE ROLE ${PG_USER} WITH LOGIN PASSWORD '${PG_PASS}';\" || true"
  docker exec -i "${PG_CONTAINER}" bash -lc "psql -U ${PG_SUPERUSER} -c \"ALTER ROLE ${PG_USER} WITH SUPERUSER;\" || true" || true
else
  # try via host psql (requires superuser access)
  PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -c "CREATE ROLE ${PG_USER} WITH LOGIN PASSWORD '${PG_PASS}';" || true
fi

echo ">>> Creating database ${PG_DB} owned by ${PG_USER} (idempotent)"
if [ -n "${PG_CONTAINER}" ]; then
  docker exec -i "${PG_CONTAINER}" bash -lc "psql -U ${PG_SUPERUSER} -c \"CREATE DATABASE ${PG_DB} OWNER ${PG_USER};\" || true"
else
  PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -c "CREATE DATABASE ${PG_DB} OWNER ${PG_USER};" || true
fi

echo ">>> Granting privileges to ${PG_USER} on ${PG_DB}"
if [ -n "${PG_CONTAINER}" ]; then
  docker exec -i "${PG_CONTAINER}" bash -lc "psql -U ${PG_SUPERUSER} -d ${PG_DB} -c \"GRANT ALL PRIVILEGES ON DATABASE ${PG_DB} TO ${PG_USER};\" || true"
else
  PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -d "${PG_DB}" -c "GRANT ALL PRIVILEGES ON DATABASE ${PG_DB} TO ${PG_USER};" || true
fi

echo ">>> Testing connection as ${PG_USER}"
# test using psql client (connect as the user)
if [ -n "${PG_CONTAINER}" ]; then
  docker exec -i "${PG_CONTAINER}" bash -lc "PGPASSWORD='${PG_PASS}' psql -U ${PG_USER} -d ${PG_DB} -c \"SELECT current_database(), current_user, now();\""
else
  PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -c "SELECT current_database(), current_user, now();"
fi

echo ">>> Optional Python psycopg2 check (host must have python & psycopg2)"
python3 - <<PY || true
import os, sys
try:
    import psycopg2
    conn = psycopg2.connect(host=os.getenv('PG_HOST','${PG_HOST}'), port=${PG_PORT}, dbname='${PG_DB}', user='${PG_USER}', password='${PG_PASS}', connect_timeout=5)
    cur = conn.cursor()
    cur.execute('SELECT version(), current_database(), current_user;')
    print(cur.fetchone())
    conn.close()
except Exception as e:
    print('Python PG check failed:', e)
    sys.exit(0)
PY

echo "Postgres check complete."
