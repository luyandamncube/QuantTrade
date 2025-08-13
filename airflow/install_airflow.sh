#!/usr/bin/env bash
set -euo pipefail

### -----------------------------
### Config (override via env vars)
### -----------------------------
: "${AIRFLOW_HOME:=$HOME/airflow}"
: "${VENV_DIR:=airflow_venv}"

# Airflow & Python compat: 2.9.x supports Python 3.12+
: "${AIRFLOW_VERSION:=2.9.2}"

# Concurrency knobs
: "${AIRFLOW_PARALLELISM:=16}"                 # max running tasks across all DAGs
: "${AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG:=4}"      # DAG runs at once per DAG
: "${AIRFLOW_MAX_ACTIVE_TASKS_PER_DAG:=16}"    # tasks from a single DAG at once

# DB toggle & creds
: "${USE_POSTGRES:=1}"                          # 1=LocalExecutor+Postgres, 0=Sequential+SQLite
: "${AIRFLOW_DB_HOST:=localhost}"
: "${AIRFLOW_DB_PORT:=5432}"
: "${AIRFLOW_DB_NAME:=airflow}"
: "${AIRFLOW_DB_USER:=airflow}"
: "${AIRFLOW_DB_PASS:=airflow}"

### -----------------------------
### 0) Create & activate venv
### -----------------------------
python3 -m venv "$VENV_DIR"
# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"
python -m pip install --upgrade pip setuptools wheel

### -----------------------------
### 1) Install Airflow (+psycopg2 if Postgres)
### -----------------------------
PYTHON_VERSION="$(python -c 'import sys;print(".".join(map(str, sys.version_info[:2])))')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Installing Apache Airflow ${AIRFLOW_VERSION} for Python ${PYTHON_VERSION}..."
if curl -fsI "$CONSTRAINT_URL" >/dev/null 2>&1; then
  pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "$CONSTRAINT_URL"
else
  echo "⚠️  No constraints file for ${AIRFLOW_VERSION} / Python ${PYTHON_VERSION}. Installing without constraints."
  pip install "apache-airflow==${AIRFLOW_VERSION}"
fi

if [ "$USE_POSTGRES" -eq 1 ]; then
  pip install "psycopg2-binary>=2.9"
fi

### -----------------------------
### 2) Environment & first init
### -----------------------------
export AIRFLOW_HOME
mkdir -p "$AIRFLOW_HOME"

# Generate default config & fernet key (SQLite/Sequential on first pass)
echo "Initializing Airflow database (first pass) to create airflow.cfg..."
airflow db init

CFG_PATH="$AIRFLOW_HOME/airflow.cfg"

### -----------------------------
### 3) Optional Postgres bootstrap
### -----------------------------
if [ "$USE_POSTGRES" -eq 1 ]; then
  echo "Attempting to ensure Postgres DB/user exist (best-effort)..."
  if command -v psql >/dev/null 2>&1 && [ "$AIRFLOW_DB_HOST" = "localhost" ]; then
    # Try via local postgres superuser if available
    if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
      sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='${AIRFLOW_DB_USER}'" | grep -q 1 || \
        sudo -u postgres psql -c "CREATE ROLE ${AIRFLOW_DB_USER} LOGIN PASSWORD '${AIRFLOW_DB_PASS}';"
      sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='${AIRFLOW_DB_NAME}'" | grep -q 1 || \
        sudo -u postgres psql -c "CREATE DATABASE ${AIRFLOW_DB_NAME} OWNER ${AIRFLOW_DB_USER};"
      echo "✅ Postgres role/db ensured."
    else
      echo "ℹ️  Skipping Postgres bootstrap (no sudo passwordless or postgres superuser). Ensure DB & user manually if needed."
    fi
  else
    echo "ℹ️  Skipping Postgres bootstrap (psql not found or remote host)."
  fi
fi

### -----------------------------
### 4) Set config for parallelism
### -----------------------------
if [ "$USE_POSTGRES" -eq 1 ]; then
  EXECUTOR="LocalExecutor"
  SQL_CONN="postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASS}@${AIRFLOW_DB_HOST}:${AIRFLOW_DB_PORT}/${AIRFLOW_DB_NAME}"
else
  EXECUTOR="SequentialExecutor"
  SQL_CONN="sqlite:///${AIRFLOW_HOME}/airflow.db"
fi

echo "Writing concurrency & executor settings to airflow.cfg..."
airflow config set core load_examples False
airflow config set core executor "$EXECUTOR"
airflow config set core parallelism "$AIRFLOW_PARALLELISM"
airflow config set scheduler max_active_runs_per_dag "$AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG"
airflow config set scheduler max_active_tasks_per_dag "$AIRFLOW_MAX_ACTIVE_TASKS_PER_DAG"
airflow config set database sql_alchemy_conn "$SQL_CONN"

### -----------------------------
### 5) Re-init DB using final settings
### -----------------------------
echo "Re-initializing Airflow DB to apply final settings..."
airflow db reset --yes >/dev/null 2>&1 || true   # ok if fresh
airflow db init

### -----------------------------
### 6) Create admin user (idempotent)
### -----------------------------
echo "Creating admin user (if missing)..."
airflow users create \
  --username admin \
  --firstname Luyanda \
  --lastname Mncube \
  --role Admin \
  --email admin@example.com \
  --password admin \
  >/dev/null 2>&1 || echo "ℹ️  Admin user may already exist."

### -----------------------------
### 7) Summary & how to run
### -----------------------------
echo
echo "✅ Airflow installed and configured!"
echo "   AIRFLOW_HOME:            $AIRFLOW_HOME"
echo "   Executor:                $EXECUTOR"
echo "   DB connection:           $SQL_CONN"
echo "   [core] parallelism:      $AIRFLOW_PARALLELISM"
echo "   [scheduler] max_active_runs_per_dag:  $AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG"
echo "   [scheduler] max_active_tasks_per_dag: $AIRFLOW_MAX_ACTIVE_TASKS_PER_DAG"
echo
echo "Run the following in separate terminals:"
echo "1) source \"$VENV_DIR/bin/activate\""
echo "2) airflow webserver --port 8080"
echo "3) airflow scheduler"
echo
if [ "$USE_POSTGRES" -eq 0 ]; then
  echo "⚠️  Running SequentialExecutor (SQLite) -> single-task execution. Set USE_POSTGRES=1 to enable parallelism."
fi
