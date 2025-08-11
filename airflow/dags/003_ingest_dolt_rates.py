import os
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from dotenv import load_dotenv

# # project root for imports
# import sys
# PROJECT_ROOT = os.getenv("QT_ROOT", "/mnt/c/Users/luyanda/workspace/QuantTrade")
# if PROJECT_ROOT not in sys.path:
#     sys.path.append(PROJECT_ROOT)

# from utils.dolt_to_duckdb import dolt_dump_repo, load_many_csvs_to_duckdb

# # ---- Config ----
# DOLT_BIN   = os.getenv("DOLT_BIN", "/usr/local/bin/dolt")
# EXTRA_PATH = "/usr/local/bin:/usr/bin:/bin"

# DOLT_REMOTE = os.getenv("DOLT_RATES_REMOTE", "post-no-preference/rates")
# REPO_NAME   = os.getenv("DOLT_RATES_REPO", "rates")

# RAW_DIR   = os.path.join(PROJECT_ROOT, "data", "raw", "dolt", REPO_NAME)
# DB_PATH   = os.path.join(PROJECT_ROOT, "data", "processed", "dolt", f"{REPO_NAME}.duckdb")
# REPO_ROOT = os.path.join(PROJECT_ROOT, "data", "raw", "dolt")

# STEP 1: Log DAG python script to airflow
import sys,os
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("dag_logger")

try:
    current_file = os.path.abspath(__file__)
except NameError:
    current_file = os.path.abspath(os.getcwd())

logger.info(f"[DAG INIT] __file__: {current_file}")
logger.info(f"[DAG INIT] sys.path: {sys.path}")

print(f"[DEBUG] __file__: {current_file}")

# STEP 2: Inject Project root safely into sys.path
original_sys_path = sys.path.copy()
PROJECT_ROOT = os.getcwd()

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Try import manually to catch errors
try:
    from utils.dolt_to_duckdb import dolt_dump_repo, load_many_csvs_to_duckdb
    print("✅ Import succeeded: dolt_to_duckdb")
except Exception as e:
    print(f"❌ Import failed: {e}")
    raise

# STEP 2:  Load environment variables ---
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

DOLT_BIN = os.getenv("DOLT_BIN", "/usr/local/bin/dolt")
DOLT_EXTRA_PATH = os.getenv("DOLT_EXTRA_PATH", "/usr/local/bin/dolt")

DOLT_RATES_REMOTE   = os.getenv("DOLT_RATES_REMOTE", "post-no-preference/rates")
DOLT_RAW_RATES_DIR   = os.getenv("DOLT_RAW_RATES_DIR", "data/raw/dolt/rates")
DOLT_PROC_DIR   = os.getenv("DOLT_PROC_DIR", "data/processed/dolt")
RAW_DOLT_DIR  = os.getenv("RAW_DOLT_DIR",   f"{PROJECT_ROOT}/{DOLT_RAW_RATES_DIR}")
DOLT_PROC_DIR = os.getenv("DOLT_PROC_DIR", f"{PROJECT_ROOT}/data/processed/dolt")
DUCKDB_PATH = f"{DOLT_PROC_DIR}/rates.duckdb"

# ---- Config ----
REPO_NAME   = os.getenv("DOLT_RATES_REPO", "rates")

RAW_DIR  = os.path.join(PROJECT_ROOT, "data", "raw",  "dolt", REPO_NAME)
DB_PATH  = os.path.join(PROJECT_ROOT, "data", "processed", "dolt", f"{REPO_NAME}.duckdb")
REPO_ROOT = os.path.join(PROJECT_ROOT, "data", "raw", "dolt")

print(f"[DEBUG] DOLT_RATES_REMOTE: {DOLT_RATES_REMOTE}")
print(f"[DEBUG] DOLT_RAW_RATES_DIR: {DOLT_RAW_RATES_DIR}")
print(f"[DEBUG] DOLT_PROC_DIR: {DOLT_PROC_DIR}")
print(f"[DEBUG] RAW_DOLT_DIR: {RAW_DOLT_DIR}")
print(f"[DEBUG] DUCKDB_PATH: {DUCKDB_PATH}")

EXPECTED_TABLES = ["us_treasury"]

# Primary key guess for rates (adjust if headers differ after first run)
# Common in these datasets: date + tenor/term (e.g., 'term' or 'maturity' or 'tenor')
PK_MAP = {
    "us_treasury": ["date", "term"],  # change 'term' to your actual column name if needed
}

default_args = dict(
    owner="quant", 
    retries=1, 
    retry_delay=timedelta(minutes=5)
)

with DAG(
    dag_id="ingest_dolt_rates",
    start_date=datetime(2025, 8, 1),
    schedule_interval="0 7 * * 1-5",  # weekdays 07:00 UTC
    catchup=False,
    default_args=default_args,
    description="Dump Dolt 'rates' CSVs and load directly into DuckDB (UPSERT).",
    max_active_runs=1,
) as dag:

    check_dolt = BashOperator(
        task_id="check_dolt",
        bash_command=f'export PATH="{DOLT_EXTRA_PATH}:$PATH"; {DOLT_BIN} version',
    )

    # Branch: reuse today's CSV if fresh
    def _branch_on_fresh_csv(**_):
        os.makedirs(RAW_DIR, exist_ok=True)
        def is_today(path):
            if not os.path.exists(path): return False
            d = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc).date()
            return d == datetime.now(timezone.utc).date()

        ok = all(is_today(os.path.join(RAW_DIR, f"{t}.csv")) for t in EXPECTED_TABLES)
        return "use_existing_csv" if ok else "dolt_dump_csv"

    branch = BranchPythonOperator(
        task_id="branch_on_fresh_csv",
        python_callable=_branch_on_fresh_csv,
    )

    def _use_existing():
        mapping = {t: os.path.join(RAW_DIR, f"{t}.csv") for t in EXPECTED_TABLES}
        missing = [t for t, p in mapping.items() if not os.path.exists(p)]
        if missing:
            raise FileNotFoundError(f"Missing CSV(s): {missing}")
        return mapping

    use_existing_csv = PythonOperator(
        task_id="use_existing_csv",
        python_callable=_use_existing,
        do_xcom_push=True,
    )

    def _dump():
        os.makedirs(RAW_DIR, exist_ok=True)
        mapping = dolt_dump_repo(
            repo_root=REPO_ROOT,
            remote=DOLT_RATES_REMOTE,
            repo_name=REPO_NAME,
            out_dir=RAW_DIR,
            dolt_bin=DOLT_BIN,
        )
        # filter to expected tables (repo may ship extras later)
        mapping = {t: p for t, p in mapping.items() if t in EXPECTED_TABLES}
        if len(mapping) != len(EXPECTED_TABLES):
            print(f"[WARN] Missing after dump: {sorted(set(EXPECTED_TABLES) - set(mapping.keys()))}")
        return mapping

    dolt_dump_csv = PythonOperator(
        task_id="dolt_dump_csv",
        python_callable=_dump,
        do_xcom_push=True,
    )

    join = EmptyOperator(task_id="join_branches", trigger_rule="none_failed_min_one_success")

    def _load_duckdb(**kwargs):
        ti = kwargs["ti"]
        candidates = ti.xcom_pull(task_ids=["use_existing_csv", "dolt_dump_csv"])
        table_to_csv = next((m for m in candidates if isinstance(m, dict)), None)
        if not table_to_csv:
            raise ValueError(f"No CSV mapping from branches: {candidates}")

        load_many_csvs_to_duckdb(db_path=DB_PATH, table_to_csv=table_to_csv, pk_map=PK_MAP)

        # Small sanity: log row count + staged columns
        import duckdb
        with duckdb.connect(DB_PATH) as con:
            for t in sorted(table_to_csv.keys()):
                # Count
                cnt = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
                print(f'[DuckDB] {t}: {cnt:,} rows')
                # Schema
                cols = [r[0] for r in con.execute(f"PRAGMA table_info('{t}')").fetchall()]
                print(f'[DuckDB] {t} columns: {cols}')

    load_duckdb = PythonOperator(
        task_id="load_duckdb",
        python_callable=_load_duckdb,
        provide_context=True,
        trigger_rule="none_failed_min_one_success",
    )

    check_dolt >> branch
    branch >> use_existing_csv >> join
    branch >> dolt_dump_csv     >> join
    join >> load_duckdb
