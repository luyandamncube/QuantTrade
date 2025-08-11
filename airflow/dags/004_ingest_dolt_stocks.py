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
GRAPHVIZ_BIN = os.getenv("GRAPHVIZ_BIN", "/usr/bin/dot")

DOLT_STOCKS_REMOTE = os.getenv("DOLT_STOCKS_REMOTE", "post-no-preference/stocks")
REPO_NAME = os.getenv("DOLT_STOCKS_REPO", "stocks")  # << used for folder + file names

RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "dolt", REPO_NAME)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "dolt", f"{REPO_NAME}.duckdb")
REPO_ROOT = os.path.join(PROJECT_ROOT, "data", "raw", "dolt")

# print(f"[DEBUG] DOLT_STOCKS_REMOTE: {DOLT_STOCKS_REMOTE}")
# print(f"[DEBUG] RAW_DIR: {RAW_DIR}")
# print(f"[DEBUG] DB_PATH: {DB_PATH}")

EXPECTED_TABLES = ["symbol", "ohlcv", "split", "dividend"]

# Primary keys for UPSERT and for ERD inference
PK_MAP = {
    "symbol":   ["act_symbol"],            # parent
    "ohlcv":    ["act_symbol", "date"],    # child referencing symbol.act_symbol
    "split":    ["act_symbol", "ex_date"], # child referencing symbol.act_symbol
    "dividend": ["act_symbol", "ex_date"], # child referencing symbol.act_symbol
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
    # Mermaid attributes must start with letter/underscore; e.g., '52_week_high' -> 'c_52_week_high'
    y = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not re.match(r"[A-Za-z_]", y):
        y = f"c_{y}"
    return y

def _infer_fk_edges_from_pk_map(tables, cols_by_table, pk_map):
    """
    Infer edges where a table contains ALL PK columns of another table (case-insensitive match).
    Returns list of (child_schema, child_table, [child_cols], parent_schema, parent_table, [parent_pk_cols]).
    Also returns 'peer links' for tables that share exactly the same PK set.
    """
    # Normalize structures
    table_keys = [(s, t) for (s, t) in tables]
    # Build lowercase name → actual name maps per table
    lc_cols = {}
    for k in table_keys:
        existing = cols_by_table.get(k, [])
        lc_cols[k] = {c.lower(): c for (c, _dt) in existing}

    # Build pk sets with actual casing
    pk_actual = {}
    for (_, t) in table_keys:
        if t in pk_map:
            want = [c.lower() for c in pk_map[t]]
            # we don't know schema yet; assume any schema name for lookup
            # fetch from the first (schema, table) matching that 't'
            k = next(k for k in table_keys if k[1] == t)
            actual_cols = []
            for c in want:
                if c in lc_cols[k]:
                    actual_cols.append(lc_cols[k][c])
            if len(actual_cols) == len(pk_map[t]):
                pk_actual[k] = actual_cols

    # Parent-child edges: child contains all PKs of parent
    inferred_edges = []
    seen_pairs = set()
    for parent_k, parent_pk_cols in pk_actual.items():
        ps, pt = parent_k
        for child_k in table_keys:
            if child_k == parent_k:
                continue
            cs, ct = child_k
            child_cols_lc = lc_cols[child_k]
            if all(c.lower() in child_cols_lc for c in parent_pk_cols):
                child_cols_actual = [child_cols_lc[c.lower()] for c in parent_pk_cols]
                pair = (cs, ct, ps, pt)
                if pair not in seen_pairs:
                    inferred_edges.append((cs, ct, child_cols_actual, ps, pt, parent_pk_cols))
                    seen_pairs.add(pair)

    # Peer links: same PK set exactly (e.g., dividend & split both have (act_symbol, ex_date))
    peer_edges = []
    done = set()
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
                    peer_edges.append(((s1, t1), (s2, t2), [lc_cols[(s1, t1)][c] for c in set1]))
                    done.add(key)

    return inferred_edges, peer_edges

def generate_mermaid_erd(db_path: str, out_dir: str, erd_name: str, pk_map: dict | None = None) -> str:
    """Introspect DuckDB and emit Mermaid erDiagram text with safe identifiers.
       - Sanitizes attribute names
       - Uses declared FKs if present
       - Optionally infers edges from PK_MAP (child contains parent's PK columns)
       - Adds peer links for tables sharing the exact same PK set
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

    from collections import defaultdict
    cols_by_table = defaultdict(list)
    pk_by_table   = defaultdict(set)
    for s, t, c, dt in cols:
        cols_by_table[(s, t)].append((c, dt))
    for s, t, c in pks:
        pk_by_table[(s, t)].add(c)

    lines = ["erDiagram"]
    alias_notes = []

    # Entities
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

    # Declared FK relationships (labels keep original col names; entity ids are safe)
    edges_declared = set()
    for fs, ft, fc, ps, pt, pc in fks:
        a = _safe_ident(f"{fs}__{ft}")
        b = _safe_ident(f"{ps}__{pt}")
        label = f'{fc}→{pc}'.replace('"', r"\"")
        lines.append(f'{a} }}o--|| {b} : "{label}"')
        edges_declared.add(((fs, ft), (ps, pt)))

    # Inferred edges from PK_MAP
    if pk_map:
        inferred, peers = _infer_fk_edges_from_pk_map(tables, cols_by_table, pk_map)
        for cs, ct, child_cols, ps, pt, parent_pk_cols in inferred:
            if ((cs, ct), (ps, pt)) in edges_declared:
                continue  # already drawn
            a = _safe_ident(f"{cs}__{ct}")
            b = _safe_ident(f"{ps}__{pt}")
            label = ", ".join([f"{cc}→{pc}" for cc, pc in zip(child_cols, parent_pk_cols)]).replace('"', r"\"")
            lines.append(f'{a} }}o--|| {b} : "{label} (inferred)"')

        # Peer links for matching composite keys (e.g., dividend <> split on (act_symbol, ex_date))
        for (s1, t1), (s2, t2), shared_actual in peers:
            a = _safe_ident(f"{s1}__{t1}")
            b = _safe_ident(f"{s2}__{t2}")
            label = ", ".join(shared_actual).replace('"', r"\"")
            lines.append(f'{a} }}o--o{{ {b} : "shared keys: {label}"')

    if alias_notes:
        lines = ["%% Column alias mapping for Mermaid-safe names"] + alias_notes + [""] + lines

    with open(out_mmd, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # Hard validation: no attribute may start with a digit
    txt = open(out_mmd, "r", encoding="utf-8").read()
    if re.search(r"^\s*(int|float|string|date|timestamp)\s+[0-9]", txt, flags=re.M):
        raise RuntimeError(f"[ERD] Unsafe attribute slipped into {out_mmd}. Check sanitization.")

    return out_mmd

# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="ingest_dolt_stocks",
    start_date=datetime(2025, 8, 1),
    schedule_interval="0 7 * * 1-5",  # weekdays 07:00 UTC
    catchup=False,
    default_args=default_args,
    description="Dump Dolt 'stocks' CSVs and load directly into DuckDB (UPSERT).",
    max_active_runs=1,
) as dag:

    # If you're on Windows without bash, swap this for a PythonOperator
    check_dolt = BashOperator(
        task_id="check_dolt",
        bash_command=f'export PATH="{DOLT_EXTRA_PATH}:$PATH"; {DOLT_BIN} version',
    )

    # Branch: reuse today's CSV if fresh
    def _branch_on_fresh_csv(**_):
        os.makedirs(RAW_DIR, exist_ok=True)

        def is_today(path):
            if not os.path.exists(path):
                return False
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
            remote=DOLT_STOCKS_REMOTE,
            repo_name=REPO_NAME,
            out_dir=RAW_DIR,
            dolt_bin=DOLT_BIN,
        )
        # Filter to expected tables (repo may ship extras later)
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

        # Sanity: log row count + columns
        with duckdb.connect(DB_PATH) as con:
            for t in sorted(table_to_csv.keys()):
                cnt = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
                print(f'[DuckDB] {t}: {cnt:,} rows')
                cols = [r[0] for r in con.execute(f"PRAGMA table_info('{t}')").fetchall()]
                print(f'[DuckDB] {t} columns: {cols}')

    load_duckdb = PythonOperator(
        task_id="load_duckdb",
        python_callable=_load_duckdb,
        provide_context=True,
        trigger_rule="none_failed_min_one_success",
    )

    # --- Mermaid generation + render (Python-only) ---
    def _make_erd(**_):
        # Store under docs/erd/<repo>/<repo>.*  (e.g., docs/erd/stocks/stocks.svg)
        erd_dir = os.path.join(PROJECT_ROOT, "docs", "erd", REPO_NAME)
        path = generate_mermaid_erd(db_path=DB_PATH, out_dir=erd_dir, erd_name=REPO_NAME, pk_map=PK_MAP)
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

    def _port_id(name: str) -> str:
        # Safe port id for Graphviz record labels (used on edges)
        pid = re.sub(r"[^A-Za-z0-9_]", "_", name)
        if not re.match(r"[A-Za-z_]", pid):
            pid = f"c_{pid}"
        return pid

    def generate_graphviz_erd(db_path: str, out_dir: str, name: str, pk_map: dict | None = None) -> str:
        """
        Build a DOT file with orthogonal edges. Uses declared FKs, plus infers edges from pk_map
        (child contains all parent PK columns) and adds peer links when PK sets match.
        """
        os.makedirs(out_dir, exist_ok=True)
        dot_path = os.path.join(out_dir, f"{name}_ortho.dot")

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

            fks = con.execute("""
                SELECT
                src.table_schema AS fk_schema,
                src.table_name   AS fk_table,
                src.column_name  AS fk_column,
                tgt.table_schema AS pk_schema,
                tgt.table_name   AS pk_table,
                tgt.column_name  AS pk_column,
                src.constraint_name AS c_name,
                src.ordinal_position AS ordpos
                FROM information_schema.referential_constraints rc
                JOIN information_schema.key_column_usage src
                ON rc.constraint_name = src.constraint_name
                JOIN information_schema.key_column_usage tgt
                ON rc.unique_constraint_name = tgt.constraint_name
                AND src.ordinal_position = tgt.ordinal_position
                ORDER BY fk_schema, fk_table, c_name, ordpos
            """).fetchall()

        from collections import defaultdict
        cols_by_table = defaultdict(list)
        for s,t,c,dt in cols:
            cols_by_table[(s,t)].append((c, dt))

        def qname(s,t): return f"{s}.{t}"

        # Record label per table with ports for columns
        out = [
            'digraph ERD {',
            'rankdir=LR;',
            'splines=ortho;',
            'nodesep=0.5;',
            'ranksep=0.9;',
            'pad=0.3;',
            'edge [arrowsize=0.7];',
            'node [shape=record, fontsize=10, fontname="Inter,Arial"];'
        ]

        # Build nodes
        port_maps = {}  # (s,t) -> {orig_col -> port_id}
        for s,t in tables:
            pm = {}
            fields = []
            for c,dt in cols_by_table[(s,t)]:
                pid = _port_id(c)
                pm[c] = pid
                fields.append(f'<{pid}> {c}: {dt}')
            port_maps[(s,t)] = pm
            label = "{{" + qname(s,t) + "|" + r'\l'.join(fields) + r'\l' + "}}"
            out.append(f'"{qname(s,t)}" [label="{label}"];')

        # Declared FK edges (per column)
        for fs,ft,fc, ps,pt,pc, cname, _ in fks:
            a = qname(fs,ft); b = qname(ps,pt)
            sp = port_maps[(fs,ft)].get(fc, _port_id(fc))
            tp = port_maps[(ps,pt)].get(pc, _port_id(pc))
            out.append(f'"{a}":"{sp}":e -> "{b}":"{tp}":w [label="{fc}→{pc}", fontsize=9];')

        # Inferred edges & peer links (reuse the pk inference you already have)
        if pk_map:
            inferred, peers = _infer_fk_edges_from_pk_map(tables, cols_by_table, pk_map)

            # For each inferred mapping, draw edges per column
            for cs, ct, child_cols, ps, pt, parent_pk_cols in inferred:
                a = qname(cs,ct); b = qname(ps,pt)
                for cc, pc in zip(child_cols, parent_pk_cols):
                    sp = port_maps[(cs,ct)].get(cc, _port_id(cc))
                    tp = port_maps[(ps,pt)].get(pc, _port_id(pc))
                    out.append(
                        f'"{a}":"{sp}":e -> "{b}":"{tp}":w '
                        f'[label="{cc}→{pc} (inferred)", style=dashed, fontsize=9, color="#7a7a7a"];'
                    )

            # Peer link (no direction) when two tables share identical PK set
            for (s1, t1), (s2, t2), shared_actual in peers:
                a = qname(s1,t1); b = qname(s2,t2)
                label = ", ".join(shared_actual).replace('"', r"\"")
                out.append(
                    f'"{a}" -> "{b}" [dir=both, arrowhead=none, arrowtail=none, '
                    f'style=dotted, color="#8a5cff", label="shared: {label}", fontsize=9];'
                )

        out.append('}')
        with open(dot_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(out))
        return dot_path

    def render_dot_to_svg_or_skip(dot_path: str) -> str:
        """Render DOT to SVG using Graphviz.
        Prefers Linux 'dot' in PATH (WSL). Optionally falls back to Windows dot.exe if configured.
        """
        import shutil, subprocess, os, re
        from airflow.exceptions import AirflowSkipException

        if not os.path.exists(dot_path):
            raise FileNotFoundError(dot_path)

        def _is_wsl() -> bool:
            try:
                with open("/proc/version", "r") as f:
                    return "microsoft" in f.read().lower()
            except Exception:
                return False

        # 1) Try regular PATH first (works if you apt-installed graphviz in WSL)
        dot_bin = shutil.which("dot")
        if not dot_bin and _is_wsl():
            # 2) Optional fallback to Windows Graphviz if user opted in
            win_dot = os.getenv("GRAPHVIZ_BIN_WIN")  # e.g. r"C:\Program Files\Graphviz\bin\dot.exe"
            if win_dot:
                # Convert Windows path to WSL mount (/mnt/c/...):
                drive = win_dot[0].lower()
                wsl_path = f"/mnt/{drive}{win_dot[2:]}".replace("\\", "/")
                if os.path.exists(wsl_path):
                    dot_bin = wsl_path

        if not dot_bin:
            raise AirflowSkipException("Graphviz 'dot' not found. In WSL, run: sudo apt install graphviz, or set GRAPHVIZ_BIN_WIN to a Windows dot.exe path.")

        svg_path = dot_path.replace(".dot", ".svg")
        subprocess.check_call([dot_bin, "-Tsvg", dot_path, "-o", svg_path])
        print(f"[ERD] Graphviz SVG rendered: {svg_path}")
        return svg_path

    # Wiring
    check_dolt >> branch
    branch >> use_existing_csv >> join
    branch >> dolt_dump_csv >> join
    join >> load_duckdb
    load_duckdb >> generate_mermaid_erd_task >> render_mermaid_svg_py

