#!/usr/bin/env python3
import os
import sys
import argparse
import duckdb
from pathlib import Path

# DEFAULT_ROOT = os.getenv("QT_ROOT") or "/mnt/c/Users/luyanda/workspace/QuantTrade"
# DEFAULT_DB   = Path(DEFAULT_ROOT) / "data" / "processed" / "dolt" / "rates.duckdb"
DUCKDB_PATH = Path("data/processed/dolt/rates.duckdb")
DEFAULT_TBL  = "us_treasury"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DUCKDB_PATH))
    ap.add_argument("--table", default=DEFAULT_TBL)
    args = ap.parse_args()

    dbp = Path(args.db)
    if not dbp.exists():
        print(f"❌ DuckDB not found: {dbp}")
        sys.exit(1)

    con = duckdb.connect(str(dbp))

    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    print("📋 Tables:", tables)

    if args.table not in tables:
        print(f"❌ Table '{args.table}' missing")
        sys.exit(2)

    # Schema
    sch = con.execute(f"PRAGMA table_info('{args.table}')").fetchdf()
    print("\n🔬 Schema:")
    print(sch)

    # Row count
    n = con.execute(f'SELECT COUNT(*) FROM "{args.table}"').fetchone()[0]
    print(f"\n#️⃣  Rows: {n:,}")

    # Inspect candidate columns
    cols = sch["name"].tolist()
    date_col = next((c for c in ["date", "as_of_date"] if c in cols), None)
    term_col = next((c for c in ["term", "tenor", "maturity", "years"] if c in cols), None)
    rate_col = next((c for c in ["rate", "yield", "value"] if c in cols), None)

    if date_col:
        mm = con.execute(f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM "{args.table}"').fetchone()
        print(f"\n🗓️  {date_col} range: {mm[0]} → {mm[1]}")

    # Latest snapshot
    if date_col:
        snap = con.execute(
            f'''
            WITH mx AS (
              SELECT MAX("{date_col}") AS mx_dt FROM "{args.table}"
            )
            SELECT *
            FROM "{args.table}", mx
            WHERE "{date_col}" = mx.mx_dt
            ORDER BY 1
            LIMIT 20
            '''
        ).fetchdf()
        print("\n📅 Latest date snapshot (top 20 rows):")
        print(snap)

    # Simple pivot (term vs rate) if columns detected
    if date_col and term_col and rate_col:
        last_curve = con.execute(
            f'''
            WITH mx AS (SELECT MAX("{date_col}") AS mx_dt FROM "{args.table}")
            SELECT "{term_col}", "{rate_col}"
            FROM "{args.table}", mx
            WHERE "{date_col}" = mx.mx_dt
            ORDER BY 1
            '''
        ).fetchdf()
        print("\n🔧 Latest curve (term, rate):")
        print(last_curve.head(12))

    con.close()
    print("\n✅ Done.")

if __name__ == "__main__":
    main()
