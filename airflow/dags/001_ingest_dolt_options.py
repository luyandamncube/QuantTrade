import os
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from dotenv import load_dotenv

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
    from utils.dolt_to_duckdb import dolt_dump, load_csv_to_duckdb
    print("✅ Import succeeded: dolt_to_duckdb")
except Exception as e:
    print(f"❌ Import failed: {e}")
    raise

# STEP 2:  Load environment variables ---
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

DOLT_BIN = os.getenv("DOLT_BIN", "/usr/local/bin/dolt")
DOLT_EXTRA_PATH = os.getenv("DOLT_EXTRA_PATH", "/usr/local/bin/dolt")

DOLT_OPTIONS_REMOTE   = os.getenv("DOLT_OPTIONS_REMOTE", "post-no-preference/options")
DOLT_RAW_OPTIONS_DIR   = os.getenv("DOLT_RAW_OPTIONS_DIR", "data/raw/dolt/options")
DOLT_PROC_DIR   = os.getenv("DOLT_PROC_DIR", "data/processed/dolt")
RAW_DOLT_DIR  = os.getenv("RAW_DOLT_DIR",   f"{PROJECT_ROOT}/{DOLT_RAW_OPTIONS_DIR}")
DOLT_PROC_DIR = os.getenv("DOLT_PROC_DIR", f"{PROJECT_ROOT}/data/processed/dolt")
DUCKDB_PATH = f"{DOLT_PROC_DIR}/options.duckdb"

# print(f"[DEBUG] DOLT_OPTIONS_REMOTE: {DOLT_OPTIONS_REMOTE}")
# print(f"[DEBUG] DOLT_RAW_OPTIONS_DIR: {DOLT_RAW_OPTIONS_DIR}")
# print(f"[DEBUG] DOLT_PROC_DIR: {DOLT_PROC_DIR}")
# print(f"[DEBUG] RAW_DOLT_DIR: {RAW_DOLT_DIR}")
# print(f"[DEBUG] DUCKDB_PATH: {DUCKDB_PATH}")

default_args = dict(
    owner="quant",
    depends_on_past=False,
    retries=1,
    retry_delay=timedelta(minutes=5),
)
with DAG(
    dag_id="ingest_dolt_options",
    start_date=datetime(2025, 8, 1),
    schedule_interval="0 6 * * 1-5",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    description="Dump Dolt CSVs and load directly into DuckDB (UPSERT).",
) as dag:

    check_dolt = BashOperator(
        task_id="check_dolt",
        bash_command=f'export PATH="{DOLT_EXTRA_PATH}:$PATH"; {DOLT_BIN} version',
    )

    # --- Branch: decide whether to reuse today's CSVs ---
    def _branch_on_fresh_csv(**kwargs):
        os.makedirs(RAW_DOLT_DIR, exist_ok=True)
        chain_csv = os.path.join(RAW_DOLT_DIR, "option_chain.csv")
        vol_csv   = os.path.join(RAW_DOLT_DIR, "volatility_history.csv")

        def is_today(path: str) -> bool:
            if not os.path.exists(path):
                return False
            mtime_utc_date = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc).date()
            today_utc_date = datetime.now(timezone.utc).date()
            return mtime_utc_date == today_utc_date

        return "use_existing_csv" if (is_today(chain_csv) and is_today(vol_csv)) else "dolt_dump_csv"


    branch = BranchPythonOperator(
        task_id="branch_on_fresh_csv",
        python_callable=_branch_on_fresh_csv,
        provide_context=True,
    )

    # If fresh → push XCom with paths (so load_duckdb can consume)
    def _use_existing():
        chain_csv = os.path.join(RAW_DOLT_DIR, "option_chain.csv")
        vol_csv   = os.path.join(RAW_DOLT_DIR, "volatility_history.csv")
        if not (os.path.exists(chain_csv) and os.path.exists(vol_csv)):
            raise FileNotFoundError("Expected CSVs not found in RAW_DOLT_DIR")
        return {"chain_csv": chain_csv, "vol_csv": vol_csv}

    use_existing_csv = PythonOperator(
        task_id="use_existing_csv",
        python_callable=_use_existing,
        do_xcom_push=True,
    )

    # Else → perform Dolt dump and return paths
    def _dump():
        os.makedirs(RAW_DOLT_DIR, exist_ok=True)
        chain_csv, vol_csv = dolt_dump(
            repo_root=os.path.join(PROJECT_ROOT, "data", "raw", "dolt"),
            remote=DOLT_OPTIONS_REMOTE,
            out_dir=RAW_DOLT_DIR,
            dolt_bin=DOLT_BIN,
        )
        return {"chain_csv": chain_csv, "vol_csv": vol_csv}

    dolt_dump_csv = PythonOperator(
        task_id="dolt_dump_csv",
        python_callable=_dump,
        do_xcom_push=True,
    )

    # Converge branches (optional visual no-op)
    join = EmptyOperator(task_id="join_branches", trigger_rule="none_failed_min_one_success")

    # Load into DuckDB (pull XCom from either branch)
    def _load_duckdb(**kwargs):
        ti = kwargs["ti"]
        x = ti.xcom_pull(task_ids=["use_existing_csv", "dolt_dump_csv"])
        # x can be [dict, None] or [None, dict]; pick the dict
        payload = next((i for i in x if isinstance(i, dict)), None)
        if not payload:
            raise ValueError(f"No XCom payload from branches: {x}")
        chain_csv, vol_csv = payload["chain_csv"], payload["vol_csv"]

        # Adjust PKs to your schema
        load_csv_to_duckdb(DUCKDB_PATH, chain_csv, "option_chain",
                           pk_cols=['symbol','expiry','strike','right','quote_date'])
        load_csv_to_duckdb(DUCKDB_PATH, vol_csv, "volatility_history",
                           pk_cols=['symbol','date'])

        # quick sanity log
        import duckdb
        with duckdb.connect(DUCKDB_PATH) as con:
            c1 = con.execute("SELECT COUNT(*) FROM option_chain").fetchone()[0]
            c2 = con.execute("SELECT COUNT(*) FROM volatility_history").fetchone()[0]
            print(f"[DuckDB] option_chain rows: {c1}, volatility_history rows: {c2}")

    load_duckdb = PythonOperator(
        task_id="load_duckdb",
        python_callable=_load_duckdb,
        provide_context=True,
        trigger_rule="none_failed_min_one_success",  # run if either branch succeeds
    )

    check_dolt >> branch
    branch >> use_existing_csv >> join
    branch >> dolt_dump_csv     >> join
    join >> load_duckdb