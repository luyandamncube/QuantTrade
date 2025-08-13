import os
from pathlib import Path
import shutil
import duckdb
import subprocess

def _qid(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'

def merge_minute_csvs_to_duckdb(
    db_path: str,
    raw_dir: str,
    table: str = "alpaca_minute",
    pk_cols: list[str] = ["timestamp", "symbol"]
) -> None:
    """
    Merge all CSVs matching minute_*.csv from raw_dir into a single DuckDB table.
    - Idempotent upsert on (timestamp, symbol).
    - Assumes CSVs have columns: timestamp,symbol,open,high,low,close,volume,trade_count,vwap
    """
    dbp = Path(db_path)
    dbp.parent.mkdir(parents=True, exist_ok=True)
    raw = Path(raw_dir)
    if not raw.exists():
        raise FileNotFoundError(f"raw_dir not found: {raw_dir}")

    csv_glob = (raw / "minute_*.csv").as_posix()

    with duckdb.connect(str(dbp)) as con:
        con.execute("PRAGMA threads=4;")

        con.execute("DROP TABLE IF EXISTS __stg;")
        con.execute(f"""
            CREATE TABLE __stg AS
            SELECT * FROM read_csv_auto('{csv_glob}', filename=true)
        """)

        cnt = con.execute("SELECT COUNT(*) FROM __stg").fetchone()[0]
        if cnt == 0:
            print("[merge] __stg is empty; nothing to merge.")
            con.execute("DROP TABLE __stg;")
            return

        # ✅ column names are at index 1 (not 0)
        cols_info  = con.execute("PRAGMA table_info('__stg')").fetchall()
        col_names  = [str(r[1]) for r in cols_info]        # r[1] = name
        has_cols   = {c.lower() for c in col_names}

        required = {"timestamp","symbol","open","high","low","close","volume"}
        if not required.issubset(has_cols):
            raise ValueError(
                f"Missing required columns in CSVs. Found={sorted(has_cols)}; "
                f"require>={sorted(required)}"
            )

        # Build stg2 with explicit casts; include optionals only if present
        trade_count_sql = "CAST(trade_count AS BIGINT)" if "trade_count" in has_cols else "NULL::BIGINT"
        vwap_sql        = "CAST(vwap AS DOUBLE)"        if "vwap"        in has_cols else "NULL::DOUBLE"

        con.execute("DROP TABLE IF EXISTS __stg2;")
        con.execute(f"""
            CREATE TABLE __stg2 AS
            SELECT
            CAST(timestamp AS TIMESTAMP) AS timestamp,
            CAST(symbol    AS VARCHAR)   AS symbol,
            CAST(open      AS DOUBLE)    AS open,
            CAST(high      AS DOUBLE)    AS high,
            CAST(low       AS DOUBLE)    AS low,
            CAST(close     AS DOUBLE)    AS close,
            CAST(volume    AS BIGINT)    AS volume,
            {trade_count_sql}            AS trade_count,
            {vwap_sql}                   AS vwap
            FROM __stg
        """)
        con.execute("DROP TABLE __stg;")
        # Create target with same schema (if missing)
        con.execute(f"CREATE TABLE IF NOT EXISTS {_qid(table)} AS SELECT * FROM __stg2 WHERE 1=0;")

        # Upsert (insert only new PKs)
        pk_pred = " AND ".join([f"t.{_qid(c)} = s.{_qid(c)}" for c in pk_cols])
        cols_info2 = con.execute("PRAGMA table_info('__stg2')").fetchall()
        cols2 = [str(r[1]) for r in cols_info2]  # r[1] = column name

        # Build quoted column lists
        tgt_cols = ", ".join([_qid(c) for c in cols2])
        sel_cols = ", ".join([f"s.{_qid(c)}" for c in cols2])

        # Upsert (insert only new PKs)
        pk_pred = " AND ".join([f"t.{_qid(c)} = s.{_qid(c)}" for c in pk_cols])

        con.execute(f"""
            INSERT INTO {_qid(table)} ({tgt_cols})
            SELECT {sel_cols}
            FROM __stg2 s
            WHERE NOT EXISTS (
                SELECT 1 FROM {_qid(table)} t WHERE {pk_pred}
            );
        """)
        con.execute("DROP TABLE __stg2;")

        # Optional: index (DuckDB creates ZoneMaps automatically; this is a hint if you later switch engines)
        print(f"[merge] Upsert complete into table `{table}`.")

def generate_mermaid_erd(db_path: str, out_mmd_path: str, tables: list[str] | None = None) -> str:
    """
    Generate a simple Mermaid ER diagram for DuckDB schema.
    Returns the .mmd path.
    """
    dbp = Path(db_path)
    outp = Path(out_mmd_path)
    outp.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(dbp)) as con:
        all_tbls = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
        tbls = [t for t in tables if t in all_tbls] if tables else all_tbls

        schema = {}
        for t in tbls:
            info = con.execute(f"PRAGMA table_info('{t}')").fetchall()
            # r[1] = name, r[2] = type
            schema[t] = [{"name": str(r[1]), "type": str(r[2])} for r in info]

    # Build Mermaid text
    lines = ["erDiagram"]
    for t, cols in schema.items():
        lines.append(f"  {t} {{")
        for c in cols:
            # Mermaid expects: TYPE name
            mtype = (c["type"] or "TEXT").upper()
            lines.append(f"    {mtype} {c['name']}")
        lines.append("  }")

    # Try to infer a simple link: if a table has 'symbol' and there is a 'symbol' dimension
    # (You can extend to explicit FK map if you want)
    if "alpaca_minute" in schema:
        if "symbol" in [c["name"] for c in schema["alpaca_minute"]]:
            # If you later create a reference table “symbol” you can enable the link:
            if "symbol" in schema:
                lines.append("  symbol ||--o{ alpaca_minute : has")

    outp.write_text("\n".join(lines), encoding="utf-8")
    return str(outp)

def render_mermaid_svg(mmd_path: str, svg_path: str) -> bool:
    """
    Render Mermaid .mmd to .svg using mermaid-cli (mmdc).
    Returns True if rendered, False if mmdc missing (leaves .mmd on disk).
    """
    svgp = Path(svg_path)
    svgp.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Check availability
        subprocess.run(["mmdc", "-h"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    except FileNotFoundError:
        print("[mermaid] mmdc not found; skipping SVG render. Mermaid file left at:", mmd_path)
        return False

    cmd = f"mmdc -i {mmd_path} -o {svg_path}"
    proc = subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if proc.returncode != 0:
        print("[mermaid] Render failed:\n", proc.stdout)
        return False
    print("[mermaid] SVG written:", svg_path)
    return True