#!/usr/bin/env bash
set -euo pipefail

### -----------------------------------------
### Config (override via env vars if you like)
### -----------------------------------------
DB_USER="${DB_USER:-airflow}"
DB_PASS="${DB_PASS:-airflow}"
DB_NAME="${DB_NAME:-airflow}"
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"   # change only if you alter postgresql.conf
NONINTERACTIVE_TEST="${NONINTERACTIVE_TEST:-1}"  # 1 = run test query

echo "==> Installing PostgreSQL..."
sudo apt update
sudo apt install -y postgresql postgresql-contrib
psql --version || true

echo "==> Starting PostgreSQL service..."
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  sudo systemctl enable --now postgresql
else
  # WSL without systemd
  sudo service postgresql start
fi

echo "==> Ensuring role '${DB_USER}' exists..."
sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASS}'"

echo "==> Ensuring database '${DB_NAME}' exists (owned by ${DB_USER})..."
sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1 || \
  sudo -u postgres createdb -O "${DB_USER}" "${DB_NAME}"

echo "==> Granting privileges on database '${DB_NAME}' to ${DB_USER}..."
sudo -u postgres psql -d "${DB_NAME}" -c "GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};" >/dev/null

echo "==> Locating pg_hba.conf to ensure password auth on localhost..."
HBA_FILE="$(sudo -u postgres psql -tA -c "SHOW hba_file;")"
echo "    hba_file: ${HBA_FILE}"

# Ensure IPv4 localhost md5 line
if ! sudo grep -Eq '^[[:space:]]*host[[:space:]]+all[[:space:]]+all[[:space:]]+127\.0\.0\.1/32[[:space:]]+md5' "${HBA_FILE}"; then
  echo "    Adding IPv4 md5 auth to ${HBA_FILE}"
  sudo tee -a "${HBA_FILE}" >/dev/null <<'EOF'

# Added by install_postgres.sh for local password auth
host    all             all             127.0.0.1/32            md5
EOF
fi

# Ensure IPv6 localhost md5 line
if ! sudo grep -Eq '^[[:space:]]*host[[:space:]]+all[[:space:]]+all[[:space:]]+::1/128[[:space:]]+md5' "${HBA_FILE}"; then
  echo "    Adding IPv6 md5 auth to ${HBA_FILE}"
  sudo tee -a "${HBA_FILE}" >/dev/null <<'EOF'
host    all             all             ::1/128                 md5
EOF
fi

echo "==> Reloading PostgreSQL to apply pg_hba.conf changes..."
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  sudo systemctl reload postgresql
else
  sudo service postgresql reload
fi

if [ "${NONINTERACTIVE_TEST}" = "1" ]; then
  echo "==> Testing connection as ${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}..."
  # non-interactive: use PGPASSWORD env var
  PGPASSWORD="${DB_PASS}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT version();" || {
    echo "❌ Connection test failed. Check pg_hba.conf, password, or port (${DB_PORT})." >&2
    exit 1
  }
fi

echo
echo "✅ PostgreSQL is ready."
echo "   Role:       ${DB_USER}"
echo "   Database:   ${DB_NAME}"
echo "   Host:       ${DB_HOST}"
echo "   Port:       ${DB_PORT}"
echo
echo "Airflow SQLAlchemy URL:"
echo "postgresql+psycopg2://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_NAME}"