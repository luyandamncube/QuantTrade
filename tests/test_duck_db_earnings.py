#!/usr/bin/env python3
"""
Quick sanity checks for fundamentals DuckDB (earnings table).
- Lists tables
- Prints schema
- Row counts, date range
- Sample rows (latest)
- Duplicate check by PK
- Upcoming events (next 30 days)

Usage:
  python tests/test_duck_db_earnings.py
  python tests/test_duck_db_earnings.py --db /path/to/earnings.duckdb --table earnings_calendar
"""

import os
import sys
import argparse
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone

# DEFAULT_PROJECT_ROOT = os.getenv("QT_ROOT") or "/mnt/c/Users/luyanda/workspace/QuantTrade"
# DEFAULT_DB = Path(DEFAULT_PROJECT_ROOT) / "data" / "processed" / "dolt" / "earnings.duckdb"
# Path to your DuckDB file
DUCKDB_PATH = Path("data/processed/dolt/earnings.duckdb")
DEFAULT_TABLE = "earnings_calendar"

def fmt_int(n):
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, default=str(DUCKDB_PATH), help="Path to DuckDB file")
    parser.add_argument("--table", type=str, default=DEFAULT_TABLE, help="Earnings table name")
    args = parser.parse_args()

    db_path = Path(args.db)
    table = args.table

    if not db_path.exists():
        print(f"❌ DuckDB file not found: {db_path}")
        sys.exit(1)

    print(f"🔌 Connecting to: {db_path}")
    con = duckdb.connect(str(db_path))

    # ---- List tables
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    print("\n📋 Tables:")
    for t in tables:
        print(f"  - {t}")

    if table not in tables:
        print(f"\n❌ Table '{table}' not found. (Pick one of: {tables[:10]}{' ...' if len(tables)>10 else ''})")
        sys.exit(2)

    # ---- Schema
    print(f"\n🔬 Schema for '{table}':")
    schema_df = con.execute(f"PRAGMA table_info('{table}')").fetchdf()
    print(schema_df)

    # Best guess columns for date and symbol (adjust if your headers differ)
    # Try common alternatives automatically
    date_cols = [c for c in ["event_date", "date", "earnings_date", "report_date"] if c in schema_df["name"].tolist()]
    sym_cols  = [c for c in ["symbol", "ticker"] if c in schema_df["name"].tolist()]
    date_col = date_cols[0] if date_cols else None
    sym_col  = sym_cols[0] if sym_cols else None

    # ---- Row count
    total = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    print(f"\n#️⃣  Total rows: {fmt_int(total)}")

    # ---- Date range (if date column exists)
    if date_col:
        min_max = con.execute(f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM "{table}"').fetchone()
        print(f"🗓️  {date_col} range: {min_max[0]} → {min_max[1]}")
    else:
        print("⚠️  No date-like column detected (checked: event_date/date/earnings_date/report_date).")

    # ---- Sample latest 5 rows by date if possible
    print("\n🔍 Sample (latest 5):")
    if date_col:
        print(con.execute(
            f'SELECT * FROM "{table}" ORDER BY "{date_col}" DESC LIMIT 5'
        ).fetchdf())
    else:
        print(con.execute(f'SELECT * FROM "{table}" LIMIT 5').fetchdf())

    # ---- Duplicate check by PK guess
    # Common PK guess for earnings calendar: (symbol, event_date)
    pk_guess = []
    if sym_col: pk_guess.append(sym_col)
    if date_col: pk_guess.append(date_col)

    if len(pk_guess) >= 1:
        pk_list = ", ".join(f'"{c}"' for c in pk_guess)
        dupes = con.execute(
            f'SELECT {pk_list}, COUNT(*) AS cnt '
            f'FROM "{table}" GROUP BY {pk_list} HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 10'
        ).fetchdf()
        n_dupes = len(dupes)
        print(f"\n🧹 Duplicate check on PK guess ({', '.join(pk_guess)}):")
        if n_dupes == 0:
            print("   ✅ No duplicates (top 10).")
        else:
            print(f"   ⚠️ Found {n_dupes} duplicate key groups (showing up to 10):")
            print(dupes)
    else:
        print("\n⚠️  Could not form a PK guess (missing symbol/date columns).")

    # ---- Upcoming events (next 30 days)
    if date_col:
        today = datetime.now(timezone.utc).date()
        horizon = today + timedelta(days=30)
        upcoming = con.execute(
            f'''
            SELECT *
            FROM "{table}"
            WHERE "{date_col}" >= DATE '{today.isoformat()}'
              AND "{date_col}" <= DATE '{horizon.isoformat()}'
            ORDER BY "{date_col}" ASC
            LIMIT 20
            '''
        ).fetchdf()
        print(f"\n📅 Upcoming events (next 30 days, top 20):")
        if len(upcoming) == 0:
            print("   (none found)")
        else:
            print(upcoming)
    else:
        print("\n📅 Upcoming events: skipped (no date column).")

    # ---- Top symbols by count
    if sym_col:
        top_syms = con.execute(
            f'SELECT "{sym_col}", COUNT(*) AS n '
            f'FROM "{table}" GROUP BY 1 ORDER BY n DESC LIMIT 10'
        ).fetchdf()
        print(f"\n🏷️  Top symbols by rows:")
        print(top_syms)

    con.close()
    print("\n✅ Done.")

if __name__ == "__main__":
    main()
