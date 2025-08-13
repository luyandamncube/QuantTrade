import os
import re
import sys
import logging
import importlib
import subprocess
from datetime import datetime, timedelta, timezone

import duckdb
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
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
    from utils.dolt_to_duckdb import dolt_dump_repo, load_many_csvs_to_duckdb
    print("✅ Import succeeded: dolt_to_duckdb")
except Exception as e:
    print(f"❌ Import failed: {e}")
    raise

# ----------------------------
# Environment & config
# ----------------------------
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

DOLT_BIN = os.getenv("DOLT_BIN", "/usr/local/bin/dolt")
DOLT_EXTRA_PATH = os.getenv("DOLT_EXTRA_PATH", "/usr/local/bin")

DOLT_EARNINGS_REMOTE = os.getenv("DOLT_EARNINGS_REMOTE", "post-no-preference/earnings")
REPO_NAME = os.getenv("DOLT_EARNINGS_REPO", "earnings")

RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "dolt", REPO_NAME)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "dolt", f"{REPO_NAME}.duckdb")
REPO_ROOT = os.path.join(PROJECT_ROOT, "data", "raw", "dolt")

# print(f"[DEBUG] DOLT_EARNINGS_REMOTE: {DOLT_EARNINGS_REMOTE}")
# print(f"[DEBUG] RAW_DIR: {RAW_DIR}")
# print(f"[DEBUG] DB_PATH: {DB_PATH}")

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

# Primary keys (tweak if your CSV headers differ)
PK_MAP = {
    "balance_sheet_assets":      ["act_symbol", "period", "date"],
    "balance_sheet_equity":      ["act_symbol", "period", "date"],
    "balance_sheet_liabilities": ["act_symbol", "period", "date"],
    "cash_flow_statement":       ["act_symbol", "period", "date"],
    "income_statement":          ["act_symbol", "period", "date"],
    "earnings_calendar":         ["act_symbol", "date"],
    "eps_estimate":              ["act_symbol", "period", "date"],
    "eps_history":               ["act_symbol", "period_end_date"],
    "sales_estimate":            ["act_symbol", "period", "date"],
    "rank_score":                ["act_symbol", "as_of_date"],
}

default_args = dict(owner="quant", retries=1, retry_delay=timedelta(minutes=5))

# ----------------------------
# Mermaid helpers (pure Python)
# ----------------------------
def _norm_type(dt: str) -> str:
    dt = dt.upper()
    if "INT" in dt: return "int"
    if any(k in dt for k in ["DOUBLE", "REAL", "FLOAT", "DECIMAL", "NUMERIC"]): return "float"
    if "DATE" in dt: return "date"
    if "TIMESTAMP" in dt: return "timestamp"
    if any(k in dt for k in ["CHAR", "TEXT", "STRING", "VARCHAR"]): return "string"
    return "string"

def _safe_ident(s: str) -> str:
    # Mermaid entity identifiers: [A-Za-z_][A-Za-z0-9_]*
    x = re.sub(r"[^A-Za-z0-9_]", "_", s)
    if not x or not re.match(r"[A-Za-z_]", x):
        x = f"t_{x}" if x else "t_"
    return x

def _safe_attr(name: str) -> str:
    # Mermaid attributes must start with letter/underscore; map '10_qtr' -> 'c_10_qtr'
    y = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not re.match(r"[A-Za-z_]", y):
        y = f"c_{y}"
    return y

def _infer_fk_edges_from_pk_map(tables, cols_by_table, pk_map):
    """
    Infer edges where a table contains ALL PK columns of another table (case-insensitive).
    Returns:
      - inferred: (child_schema, child_table, [child_cols], parent_schema, parent_table, [parent_pk_cols])
      - peers: ((s1, t1), (s2, t2), [shared_pk_cols])
    """
    table_keys = [(s, t) for (s, t) in tables]

    # lowercase column lookup per table
    lc_cols = {}
    for k in table_keys:
        existing = cols_by_table.get(k, [])
        lc_cols[k] = {c.lower(): c for (c, _dt) in existing}

    # actual PKs with real casing
    pk_actual = {}
    for (_, t) in table_keys:
        if t in pk_map:
            want = [c.lower() for c in pk_map[t]]
            k = next(k for k in table_keys if k[1] == t)
            actual_cols = [lc_cols[k][c] for c in want if c in lc_cols[k]]
            if len(actual_cols) == len(pk_map[t]):
                pk_actual[k] = actual_cols

    # child contains parent's full PK set → inferred edge
    inferred_edges, seen_pairs = [], set()
    for parent_k, parent_pk_cols in pk_actual.items():
        ps, pt = parent_k
        for child_k in table_keys:
            if child_k == parent_k:
                continue
            cs, ct = child_k
            if all(c.lower() in lc_cols[child_k] for c in parent_pk_cols):
                child_cols_actual = [lc_cols[child_k][c.lower()] for c in parent_pk_cols]
                pair = (cs, ct, ps, pt)
                if pair not in seen_pairs:
                    inferred_edges.append((cs, ct, child_cols_actual, ps, pt, parent_pk_cols))
                    seen_pairs.add(pair)

    # peer links (same PK set)
    peers, done = [], set()
    items = list(pk_actual.items())
    for i in range(len(items)):
        (s1, t1), pk1 = items[i]
        set1 = tuple(sorted([c.lower() for c in pk1]))
        for j in range(i + 1, len(items)):
            (s2, t2), pk2 = items[j]
            set2 = tuple(sorted([c.lower() for c in pk2]))
            if set1 == set2 and (s1, t1) != (s2, t2):
                key = tuple(sorted([(s1, t1), (s2, t2)]))
                if key not in done:
                    peers.append(((s1, t1), (s2, t2), [lc_cols[(s1, t1)][c] for c in set1]))
                    done.add(key)

    return inferred_edges, peers

def generate_mermaid_erd(
    db_path: str,
    out_dir: str,
    erd_name: str,
    pk_map: dict | None = None,
    include_relationships: bool = True,
) -> str:
    """DuckDB → Mermaid erDiagram
       - Sanitizes identifiers/attributes
       - Draws declared FKs (and optional PK inference) if include_relationships=True
    """
    os.makedirs(out_dir, exist_ok=True)
    out_mmd = os.path.join(out_dir, f"{erd_name}.mmd")

    with duckdb.connect(db_path) as con:
        tables = con.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type='BASE TABLE'
            ORDER BY table_schema, table_name
        """).fetchall()

        cols = con.execute("""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            ORDER BY table_schema, table_name, ordinal_position
        """).fetchall()

        pks = con.execute("""
            SELECT tc.table_schema, tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
        """).fetchall()

        # Only fetch FK metadata if we might use it
        if include_relationships:
            fks = con.execute("""
                SELECT
                  src.table_schema AS fk_schema,
                  src.table_name   AS fk_table,
                  src.column_name  AS fk_column,
                  tgt.table_schema AS pk_schema,
                  tgt.table_name   AS pk_table,
                  tgt.column_name  AS pk_column
                FROM information_schema.referential_constraints rc
                JOIN information_schema.key_column_usage src
                  ON rc.constraint_name = src.constraint_name
                JOIN information_schema.key_column_usage tgt
                  ON rc.unique_constraint_name = tgt.constraint_name
                 AND src.ordinal_position = tgt.ordinal_position
                ORDER BY fk_schema, fk_table, src.ordinal_position
            """).fetchall()
        else:
            fks = []
    from collections import defaultdict
    cols_by_table = defaultdict(list)
    pk_by_table   = defaultdict(set)
    for s, t, c, dt in cols:
        cols_by_table[(s, t)].append((c, dt))
    for s, t, c in pks:
        pk_by_table[(s, t)].add(c)

    lines = ["erDiagram"]
    alias_notes = []

    # Entities only
    for s, t in tables:
        ent = _safe_ident(f"{s}__{t}")
        lines.append(f"{ent} {{")
        for c, dt in cols_by_table[(s, t)]:
            typ = _norm_type(dt)
            safe_c = _safe_attr(c)
            if safe_c != c:
                alias_notes.append(f"%% {s}.{t}.{c} → {safe_c}")
            suffix = " PK" if c in pk_by_table[(s, t)] else ""
            lines.append(f"  {typ} {safe_c}{suffix}")
        lines.append("}")

    # Relationships (only if requested)
    if include_relationships:
        edges_declared = set()
        for fs, ft, fc, ps, pt, pc in fks:
            a = _safe_ident(f"{fs}__{ft}")
            b = _safe_ident(f"{ps}__{pt}")
            label = f'{fc}→{pc}'.replace('"', r"\"")
            lines.append(f'{a} }}o--|| {b} : "{label}"')
            edges_declared.add(((fs, ft), (ps, pt)))

        if pk_map:
            inferred, peers = _infer_fk_edges_from_pk_map(tables, cols_by_table, pk_map)
            for cs, ct, child_cols, ps, pt, parent_pk_cols in inferred:
                if ((cs, ct), (ps, pt)) in edges_declared:
                    continue
                a = _safe_ident(f"{cs}__{ct}")
                b = _safe_ident(f"{ps}__{pt}")
                label = ", ".join([f"{cc}→{pc}" for cc, pc in zip(child_cols, parent_pk_cols)]).replace('"', r"\"")
                lines.append(f'{a} }}o--|| {b} : "{label} (inferred)"')

            for (s1, t1), (s2, t2), shared_actual in peers:
                a = _safe_ident(f"{s1}__{t1}")
                b = _safe_ident(f"{s2}__{t2}")
                label = ", ".join(shared_actual).replace('"', r"\"")
                lines.append(f'{a} }}o--o{{ {b} : "shared keys: {label}"')

    if alias_notes:
        lines = ["%% Column alias mapping for Mermaid-safe names"] + alias_notes + [""] + lines

    with open(out_mmd, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # defensive validation: no attribute starts with a digit
    txt = open(out_mmd, "r", encoding="utf-8").read()
    if re.search(r"^\s*(int|float|string|date|timestamp)\s+[0-9]", txt, flags=re.M):
        raise RuntimeError(f"[ERD] Unsafe attribute slipped into {out_mmd}. Check sanitization.")
    return out_mmd

# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="ingest_dolt_earnings",
    start_date=datetime(2025, 8, 1),
    schedule_interval="0 7 * * 1-5",  # weekdays 07:00
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    description="Dump Dolt 'earnings' CSVs and load directly into DuckDB (UPSERT).",
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
        all_ok = all(is_today(os.path.join(RAW_DIR, f"{t}.csv")) for t in EXPECTED_TABLES)
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

    # --- Mermaid generation + render (Python-only) ---
    def _make_erd(**_):
        # store under docs/erd/<repo>/<repo>.*
        erd_dir = os.path.join(PROJECT_ROOT, "docs", "erd", REPO_NAME)
        path = generate_mermaid_erd(
            db_path=DB_PATH,
            out_dir=erd_dir,
            erd_name=REPO_NAME,
            pk_map=None,                 # ignore PK inference
            include_relationships=False  # ✅ entities only
        )
        print(f"[ERD] Mermaid written: {path}")
        return path


    generate_mermaid_erd_task = PythonOperator(
        task_id="generate_mermaid_erd",
        python_callable=_make_erd,
        provide_context=True,
    )

    def _render_mermaid_svg_py(**kwargs):
        ti = kwargs["ti"]
        mmd_path = ti.xcom_pull(task_ids="generate_mermaid_erd")
        if not mmd_path or not os.path.exists(mmd_path):
            raise FileNotFoundError(f"[ERD] Mermaid file not found: {mmd_path}")
        svg_path = mmd_path.replace(".mmd", ".svg")

        def ensure(pkg: str):
            try:
                return importlib.import_module(pkg)
            except ImportError:
                print(f"[ERD] Installing {pkg} ...")
                subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
                return importlib.import_module(pkg)

        try:
            # Defensive sanitize pass on the .mmd (guards against stale files)
            with open(mmd_path, "r", encoding="utf-8") as f:
                txt = f.read()

            def _fix_line(m):
                t = m.group(1)
                name = m.group(2)
                fixed = name
                if not re.match(r"[A-Za-z_]", name):
                    fixed = "c_" + re.sub(r"[^A-Za-z0-9_]", "_", name)
                return f"{t} {fixed}"

            txt = re.sub(r"^(int|float|string|date|timestamp)\s+([^\s]+)", _fix_line, txt, flags=re.M)

            with open(mmd_path, "w", encoding="utf-8") as f:
                f.write(txt)

            print("[ERD] First lines of Mermaid file:")
            for i, line in enumerate(txt.splitlines()[:30], 1):
                print(f"{i:02d}: {line}")

            # Render via Python package mermaid_cli + Playwright
            ensure("mermaid_cli")
            ensure("playwright")
            try:
                subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
            except Exception as e:
                print(f"[ERD] playwright chromium install warning: {e}")

            from mermaid_cli import render_mermaid_file_sync
            render_mermaid_file_sync(input_file=mmd_path, output_file=svg_path, output_format="svg")
            print(f"[ERD] SVG rendered: {svg_path}")
            return svg_path

        except subprocess.CalledProcessError as e:
            print(f"[ERD] Dependency install failed, skipping SVG: {e}")
            raise AirflowSkipException("Skipped: mermaid-cli unavailable")
        except Exception as e:
            print(f"[ERD] Render failed, skipping SVG: {e}")
            raise AirflowSkipException("Skipped: could not render Mermaid to SVG")

    render_mermaid_svg_py = PythonOperator(
        task_id="render_mermaid_svg_py",
        python_callable=_render_mermaid_svg_py,
        provide_context=True,
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
        # store under docs/pipeline/<repo>/<repo>.*
        pipe_dir = os.path.join(PROJECT_ROOT, "docs", "pipeline", REPO_NAME)

        # Cosmetic labels for the branch edges
        edge_labels = {
            ("branch_on_fresh_csv", "use_existing_csv"): "fresh",
            ("branch_on_fresh_csv", "dolt_dump_csv"): "stale",
        }

        path = generate_mermaid_pipeline_flowchart(
            dag, out_dir=pipe_dir, name=REPO_NAME,
            edge_labels=edge_labels, direction="LR",
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

        # reuse the same renderer pattern
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

    # Wiring
    check_dolt >> branch
    branch >> use_existing_csv >> join
    branch >> dolt_dump_csv     >> join
    join >> load_duckdb
    # load_duckdb >> generate_mermaid_erd_task >> render_mermaid_svg_py
    load_duckdb >> generate_mermaid_erd_task >> render_mermaid_svg_py >> generate_pipeline_mermaid >> render_pipeline_mermaid_svg

