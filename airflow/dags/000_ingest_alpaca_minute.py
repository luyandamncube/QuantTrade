# QuantTrade/airflow/dags/000_ingest_alpaca_minute.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os, sys, logging
from pathlib import Path
from dotenv import load_dotenv

import re
import importlib
import subprocess
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import BranchPythonOperator  # for isinstance checks (ok if not used)

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

PIPELINE_NAME = "price_minute"  # file/folder name for pipeline diagram

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

    def _safe_node_id(s: str) -> str:
        nid = re.sub(r"[^A-Za-z0-9_]", "_", s)
        if not re.match(r"[A-Za-z_]", nid):
            nid = f"n_{nid}"
        return nid

    def generate_mermaid_pipeline_flowchart(
        dag, out_dir: str, name: str,
        edge_labels: dict[tuple[str, str], str] | None = None,
        direction: str = "LR",
    ) -> str:
        """
        Build a Mermaid flowchart from the DAG's current graph.
        - BranchPythonOperator → diamond; others → rectangles
        - Edges reflect upstream→downstream dependencies
        - Optional edge labels for branches
        """
        os.makedirs(out_dir, exist_ok=True)
        out_mmd = os.path.join(out_dir, f"{name}.mmd")

        tasks = dag.topological_sort()
        id_map = {t.task_id: _safe_node_id(t.task_id) for t in tasks}

        lines = [
            '%% Auto-generated from Airflow DAG wiring',
            '%%{init: {"flowchart": {"curve": "linear"}}}%%',
            f"flowchart {direction}",
        ]

        # Nodes
        for t in tasks:
            nid = id_map[t.task_id]
            label = t.task_id.replace('"', r"\"")
            if isinstance(t, BranchPythonOperator):
                lines.append(f'{nid}{{"{label}"}}')   # diamond
            else:
                lines.append(f'{nid}["{label}"]')     # rectangle

        # Edges
        edge_labels = edge_labels or {}
        for t in tasks:
            src = id_map[t.task_id]
            for d in t.get_direct_relatives(upstream=False):
                dst = id_map[d.task_id]
                lbl = edge_labels.get((t.task_id, d.task_id))
                if lbl:
                    lbl = lbl.replace('"', r"\"")
                    lines.append(f"{src} -->|{lbl}| {dst}")
                else:
                    lines.append(f"{src} --> {dst}")

        with open(out_mmd, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return out_mmd

    def _generate_pipeline_mermaid(**_):
        # store under docs/pipeline/<name>/<name>.*
        pipe_dir = os.path.join(PROJECT_ROOT, "docs", "pipeline", PIPELINE_NAME)

        # (No branches in this DAG; labels dict is optional and empty)
        path = generate_mermaid_pipeline_flowchart(
            dag, out_dir=pipe_dir, name=PIPELINE_NAME, edge_labels={}, direction="LR"
        )
        print(f"[PIPELINE] Mermaid flowchart written: {path}")
        return path

    generate_pipeline_mermaid = PythonOperator(
        task_id="generate_pipeline_mermaid",
        python_callable=_generate_pipeline_mermaid,
        provide_context=True,
    )

    def _render_pipeline_mermaid_svg(**kwargs):
        ti = kwargs["ti"]
        mmd_path = ti.xcom_pull(task_ids="generate_pipeline_mermaid")
        if not mmd_path or not os.path.exists(mmd_path):
            raise FileNotFoundError(f"[PIPELINE] Mermaid file not found: {mmd_path}")
        svg_path = mmd_path.replace(".mmd", ".svg")

        def ensure(pkg: str):
            try:
                return importlib.import_module(pkg)
            except ImportError:
                print(f"[PIPELINE] Installing {pkg} ...")
                subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
                return importlib.import_module(pkg)

        try:
            ensure("mermaid_cli")
            ensure("playwright")
            try:
                subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
            except Exception as e:
                print(f"[PIPELINE] playwright chromium install warning: {e}")

            from mermaid_cli import render_mermaid_file_sync
            render_mermaid_file_sync(input_file=mmd_path, output_file=svg_path, output_format="svg")
            print(f"[PIPELINE] SVG rendered: {svg_path}")
            return svg_path

        except subprocess.CalledProcessError as e:
            print(f"[PIPELINE] Dependency install failed, skipping SVG: {e}")
            raise AirflowSkipException("Skipped: mermaid-cli unavailable")
        except Exception as e:
            print(f"[PIPELINE] Render failed, skipping SVG: {e}")
            raise AirflowSkipException("Skipped: could not render pipeline Mermaid to SVG")

    render_pipeline_mermaid_svg = PythonOperator(
        task_id="render_pipeline_mermaid_svg",
        python_callable=_render_pipeline_mermaid_svg,
        provide_context=True,
    )

    # Wire it up
    for t in ingest_tasks:
        t >> join_ingest
    join_ingest >> merge_duckdb >> gen_mermaid >> render_svg
    render_svg >> generate_pipeline_mermaid >> render_pipeline_mermaid_svg

