from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

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
    # sys.path.insert(0, PROJECT_ROOT)
    sys.path.append(PROJECT_ROOT)
# print(f'[DEBUG] PROJECT_ROOT: {PROJECT_ROOT}')

# Try import manually to catch errors
try:
    from utils.ingest_alpaca_daily import ingest_daily_symbol
    print("✅ Import succeeded: ingest_daily_symbol")
except Exception as e:
    print(f"❌ Import failed: {e}")
    raise

default_args = {
    "owner": "quant",
    "start_date": datetime(2023, 1, 1),
    "retries": 0
}

dag = DAG(
    dag_id="ingest_alpaca_daily",
    default_args=default_args,
    description="Ingest daily Alpaca data and write to CSV and DuckDB",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual trigger for now
    catchup=False
)

# --- Parameters ---
SYMBOLS = ["SPY", "QQQ", "SPLV", "SHY"]

# with DAG("ingest_alpaca_daily", default_args=default_args, schedule_interval=None, catchup=False) as dag:
for symbol in SYMBOLS:
    PythonOperator(
        task_id=f"ingest_{symbol}",
        python_callable=ingest_daily_symbol,
        op_args=[symbol],
        dag=dag
    )