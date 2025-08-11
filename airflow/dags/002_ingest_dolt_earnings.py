import os
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from dotenv import load_dotenv

# # wiring for project root imports
# import sys
# PROJECT_ROOT = os.getenv("QT_ROOT", "/mnt/c/Users/luyanda/workspace/QuantTrade")
# if PROJECT_ROOT not in sys.path:
#     sys.path.append(PROJECT_ROOT)

# from utils.dolt_to_duckdb import dolt_dump_repo, load_many_csvs_to_duckdb

# # ---- Config ----
# DOLT_BIN   = os.getenv("DOLT_BIN", "/usr/local/bin/dolt")
# EXTRA_PATH = "/usr/local/bin:/usr/bin:/bin"

# DOLT_REMOTE = os.getenv("DOLT_EARNINGS_REMOTE", "post-no-preference/rates")
# REPO_NAME   = os.getenv("DOLT_RATES_REPO", "rates")

# RAW_DIR  = os.path.join(PROJECT_ROOT, "data", "raw",  "dolt", REPO_NAME)
# DB_PATH  = os.path.join(PROJECT_ROOT, "data", "processed", "dolt", f"{REPO_NAME}.duckdb")
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

DOLT_EARNINGS_REMOTE   = os.getenv("DOLT_EARNINGS_REMOTE", "post-no-preference/earnings")
DOLT_RAW_EARNINGS_DIR   = os.getenv("DOLT_RAW_EARNINGS_DIR", "data/raw/dolt/earnings")
DOLT_PROC_DIR   = os.getenv("DOLT_PROC_DIR", "data/processed/dolt")
RAW_DOLT_DIR  = os.getenv("RAW_DOLT_DIR",   f"{PROJECT_ROOT}/{DOLT_RAW_EARNINGS_DIR}")
DOLT_PROC_DIR = os.getenv("DOLT_PROC_DIR", f"{PROJECT_ROOT}/data/processed/dolt")
DUCKDB_PATH = f"{DOLT_PROC_DIR}/earnings.duckdb"

# ---- Config ----
REPO_NAME   = os.getenv("DOLT_EARNINGS_REPO", "earnings")

RAW_DIR  = os.path.join(PROJECT_ROOT, "data", "raw",  "dolt", REPO_NAME)
DB_PATH  = os.path.join(PROJECT_ROOT, "data", "processed", "dolt", f"{REPO_NAME}.duckdb")
REPO_ROOT = os.path.join(PROJECT_ROOT, "data", "raw", "dolt")

print(f"[DEBUG] DOLT_EARNINGS_REMOTE: {DOLT_EARNINGS_REMOTE}")
print(f"[DEBUG] DOLT_RAW_EARNINGS_DIR: {DOLT_RAW_EARNINGS_DIR}")
print(f"[DEBUG] DOLT_PROC_DIR: {DOLT_PROC_DIR}")
print(f"[DEBUG] RAW_DOLT_DIR: {RAW_DOLT_DIR}")
print(f"[DEBUG] DUCKDB_PATH: {DUCKDB_PATH}")

EXPECTED_TABLES = [
    "balance_sheet_assets",
    "balance_sheet_equity",
    "balance_sheet_liabilities",
    "cash_flow_statement",
    "earnings_calendar",
    "eps_estimate",
    "eps_history",
    "income_statement",
    "rank_score",
    "sales_estimate",
]

# Primary keys
PK_MAP = {
    # Common patterns; tweak to match actual CSV headers you see in __stg log
    "balance_sheet_assets":      ["act_symbol", "period", "date"],
    "balance_sheet_equity":      ["act_symbol", "period", "date"],
    "balance_sheet_liabilities": ["act_symbol", "period", "date"],
    "cash_flow_statement":       ["act_symbol", "period", "date"],
    "income_statement":          ["act_symbol", "period", "date"],
    # calendars / estimates often by date
    "earnings_calendar":         ["act_symbol", "date"],
    "eps_estimate":              ["act_symbol", "period", "date"],
    "eps_history":              ["act_symbol", "period_end_date"],
    "sales_estimate":            ["act_symbol", "period", "date"],
    "rank_score":                ["act_symbol", "as_of_date"]
}

default_args = dict(
    owner="quant",
    retries=1,
    retry_delay=timedelta(minutes=5)
)

with DAG(
    dag_id="ingest_dolt_earnings",
    start_date=datetime(2025, 8, 1),
    schedule_interval="0 7 * * 1-5",    # weekdays 07:00
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    description="Dump Dolt 'rates' CSVs and load directly into DuckDB (UPSERT).",
) as dag:

    check_dolt = BashOperator(
        task_id="check_dolt",
        bash_command=f'export PATH="{DOLT_EXTRA_PATH}:$PATH"; {DOLT_BIN} version',
    )

    # Branch: reuse today's CSVs if present and fresh for ALL expected tables
    def _branch_on_fresh_csv(**_):
        os.makedirs(RAW_DIR, exist_ok=True)
        def is_today(p):
            if not os.path.exists(p): return False
            mdate = datetime.fromtimestamp(os.path.getmtime(p), tz=timezone.utc).date()
            return mdate == datetime.now(timezone.utc).date()

        all_ok = True
        for t in EXPECTED_TABLES:
            if not is_today(os.path.join(RAW_DIR, f"{t}.csv")):
                all_ok = False
                break
        return "use_existing_csv" if all_ok else "dolt_dump_csv"

    branch = BranchPythonOperator(
        task_id="branch_on_fresh_csv",
        python_callable=_branch_on_fresh_csv,
    )

    def _use_existing():
        paths = {t: os.path.join(RAW_DIR, f"{t}.csv") for t in EXPECTED_TABLES}
        missing = [t for t, p in paths.items() if not os.path.exists(p)]
        if missing:
            raise FileNotFoundError(f"Missing CSVs for tables: {missing}")
        return paths

    use_existing_csv = PythonOperator(
        task_id="use_existing_csv",
        python_callable=_use_existing,
        do_xcom_push=True,
    )

    def _dump():
        os.makedirs(RAW_DIR, exist_ok=True)
        mapping = dolt_dump_repo(
            repo_root=REPO_ROOT,
            remote=DOLT_EARNINGS_REMOTE,
            repo_name=REPO_NAME,
            out_dir=RAW_DIR,
            dolt_bin=DOLT_BIN,
        )
        # Keep only the tables we care about (if repo has extras)
        filtered = {t: p for t, p in mapping.items() if t in EXPECTED_TABLES}
        if len(filtered) != len(EXPECTED_TABLES):
            missing = sorted(set(EXPECTED_TABLES) - set(filtered.keys()))
            if missing:
                print(f"[WARN] Missing in dump: {missing}")
        return filtered

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

        # Load each table (upsert if PK present; otherwise append+dedupe)
        load_many_csvs_to_duckdb(db_path=DB_PATH, table_to_csv=table_to_csv, pk_map=PK_MAP)

        # quick sanity
        import duckdb
        with duckdb.connect(DB_PATH) as con:
            for t in sorted(table_to_csv.keys()):
                cnt = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
                print(f'[DuckDB] {t:28s} rows: {cnt:,}')

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
