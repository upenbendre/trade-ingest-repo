#!/usr/bin/env bash
set -euo pipefail

# === Config (edit as needed) ===
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-trades}"
PG_USER="${PG_USER:-tradeuser}"
PG_PASS="${PG_PASS:-tradeuser}"
PG_SUPERUSER="${PG_SUPERUSER:-postgres}"   # system superuser to create roles/db

# === 1. Create user (if not exists) ===
echo ">>> Ensuring role ${PG_USER} exists"
PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -tc "SELECT 1 FROM pg_roles WHERE rolname='${PG_USER}';" | grep -q 1 \
  || PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -c "CREATE ROLE ${PG_USER} WITH LOGIN PASSWORD '${PG_PASS}';"

# === 2. Create database (if not exists) ===
echo ">>> Ensuring database ${PG_DB} exists"
PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -tc "SELECT 1 FROM pg_database WHERE datname='${PG_DB}';" | grep -q 1 \
  || PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -c "CREATE DATABASE ${PG_DB} OWNER ${PG_USER};"

# === 3. Grant privileges ===
echo ">>> Granting privileges on ${PG_DB} to ${PG_USER}"
PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -d "${PG_DB}" -c "GRANT ALL PRIVILEGES ON DATABASE ${PG_DB} TO ${PG_USER};" || true

# === 4. Test connection as tradeuser ===
echo ">>> Testing connection as ${PG_USER}"
PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -c "SELECT current_database(), current_user, now();"

# === 5. Optional: Python psycopg2 check (requires psycopg2 installed locally) ===
if command -v python3 >/dev/null 2>&1; then
  echo ">>> Running optional psycopg2 connection test"
  python3 - <<PY || true
import psycopg2, os
try:
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST","${PG_HOST}"),
        port=${PG_PORT},
        dbname="${PG_DB}",
        user="${PG_USER}",
        password="${PG_PASS}",
        connect_timeout=5
    )
    cur = conn.cursor()
    cur.execute("SELECT version(), current_database(), current_user;")
    print(cur.fetchone())
    conn.close()
except Exception as e:
    print("Python PG check failed:", e)
PY
fi

echo "Postgres check complete."
