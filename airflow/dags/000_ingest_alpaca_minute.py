from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import os

import sys
import os

# Add QuantTrade project root to Python path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

print(PROJECT_ROOT)

from utils.ingest_alpaca_minute import fetch_minute_data, ingest_symbol

# --- DAG Config ---
default_args = {
    "owner": "quant",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="ingest_alpaca_minute",
    default_args=default_args,
    description="Ingest 1-min Alpaca data and write to CSV, Parquet, and DuckDB",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual trigger for now
    catchup=False
)

# --- Parameters ---
SYMBOLS = ["SPY", "QQQ", "SPLV", "SHY"]

for symbol in SYMBOLS:
    PythonOperator(
        task_id=f"ingest_{symbol}",
        python_callable=ingest_symbol,
        op_args=[symbol],
        dag=dag
    )
