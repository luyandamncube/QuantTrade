# QuantTrade/airflow/dags/000_ingest_alpaca_minute.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os, sys, logging
from pathlib import Path
from dotenv import load_dotenv

# ----------------------------
# Init logging + project paths
# ----------------------------
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("dag_logger")

try:
    current_file = os.path.abspath(__file__)
except NameError:
    current_file = os.path.abspath(os.getcwd())

logger.info(f"[DAG INIT] __file__: {current_file}")
logger.info(f"[DAG INIT] sys.path: {sys.path}")
print(f"[DEBUG] __file__: {current_file}")    

PROJECT_ROOT = os.getcwd()
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# ----------------------------
# Imports from local utilities
# ----------------------------
try:
    from utils.ingest_alpaca_minute import ingest_symbol 
    print("✅ Import succeeded: ingest_alpaca_minute")
    from utils.alpaca_duckdb_utils import (
        merge_minute_csvs_to_duckdb,
        generate_mermaid_erd,
        render_mermaid_svg
    )
    print("✅ Import succeeded: alpaca_duckdb_utils")
except Exception as e:
    print(f"❌ Import failed: {e}")
    raise

# ----------------------------
# Environment & config
# ----------------------------
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# Paths (match your utils)
RAW_DIR   = os.path.join(PROJECT_ROOT, "data", "raw", "alpaca", "price")
DB_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "alpaca", "price_minute_alpaca.duckdb")
ERD_DIR   = os.path.join(PROJECT_ROOT, "docs", "erd", "price")
MMD_PATH  = os.path.join(ERD_DIR, "price_minute_alpaca.mmd")
SVG_PATH  = os.path.join(ERD_DIR, "price_minute_alpaca.svg")

# print(f"[DEBUG] PROJECT_ROOT: {PROJECT_ROOT}")
# print(f"[DEBUG] DB_PATH: {DB_PATH}")
# print(f"[DEBUG] ERD_DIR: {ERD_DIR}")
# print(f"[DEBUG] MMD_PATH: {MMD_PATH}")
# print(f"[DEBUG] SVG_PATH: {SVG_PATH}")

default_args = {
    "owner": "quant",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="ingest_alpaca_minute",
    default_args=default_args,
    description="Ingest 1-min Alpaca data to CSV, then merge → DuckDB, then generate ERD.",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    SYMBOLS = ["SPY", "QQQ", "SPLV", "SHY"]



    # 1) Per-symbol CSV pull (existing pattern)
    ingest_tasks = []
    for sym in SYMBOLS:
        t = PythonOperator(
            task_id=f"ingest_{sym}",
            python_callable=ingest_symbol,
            op_args=[sym],
        )
        ingest_tasks.append(t)

    join_ingest = EmptyOperator(task_id="join_ingest")

    # 2) Merge CSVs → DuckDB table (upsert on timestamp+symbol)
    def _merge():
        merge_minute_csvs_to_duckdb(
            db_path=DB_PATH,
            raw_dir=RAW_DIR,
            table="alpaca_minute",
            pk_cols=["timestamp", "symbol"],
        )
    merge_duckdb = PythonOperator(
        task_id="merge_duckdb",
        python_callable=_merge,
    )

    # 3) Generate Mermaid ERD (.mmd)
    def _gen_mmd():
        os.makedirs(ERD_DIR, exist_ok=True)
        mmd = generate_mermaid_erd(DB_PATH, MMD_PATH, tables=["alpaca_minute"])
        print("[ERD] Mermaid file:", mmd)
    gen_mermaid = PythonOperator(
        task_id="generate_mermaid",
        python_callable=_gen_mmd,
    )

    # 4) Render Mermaid SVG (requires mermaid-cli `mmdc`; safe no-op if missing)
    def _render():
        ok = render_mermaid_svg(MMD_PATH, SVG_PATH)
        if not ok:
            print("[ERD] Skipped SVG render (mmdc not available). Mermaid file is ready:", MMD_PATH)
    render_svg = PythonOperator(
        task_id="render_mermaid_svg",
        python_callable=_render,
    )

    # Wire it up
    for t in ingest_tasks:
        t >> join_ingest
    join_ingest >> merge_duckdb >> gen_mermaid >> render_svg
