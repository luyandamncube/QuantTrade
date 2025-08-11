#!/usr/bin/env python3
import os, sys, argparse
import duckdb
from pathlib import Path

# ROOT = os.getenv("QT_ROOT") or "/mnt/c/Users/luyanda/workspace/QuantTrade"
# DB   = Path(ROOT) / "data" / "processed" / "dolt" / "stocks.duckdb"
DUCKDB_PATH = Path("data/processed/dolt/stocks.duckdb")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DUCKDB_PATH))
    args = ap.parse_args()

    dbp = Path(args.db)
    if not dbp.exists():
        print(f"❌ DuckDB not found: {dbp}")
        sys.exit(1)

    con = duckdb.connect(str(dbp))
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    print("📋 Tables:", tables)

    def has(t): return t in tables

    # symbol
    if has("symbol"):
        print("\n[symbol]")
        print(con.execute('SELECT COUNT(*) FROM "symbol"').fetchone()[0], "rows")
        print(con.execute('SELECT * FROM "symbol" LIMIT 5').fetchdf())

    # ohlcv overview
    if has("ohlcv"):
        print("\n[ohlcv]")
        n = con.execute('SELECT COUNT(*) FROM "ohlcv"').fetchone()[0]
        print(n, "rows")
        cols = [r[0] for r in con.execute("PRAGMA table_info('ohlcv')").fetchall()]
        print("columns:", cols)
        date_col = "date" if "date" in cols else None
        if date_col:
            mm = con.execute(f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM "ohlcv"').fetchone()
            print("date range:", mm)
            # latest day snapshot for few large symbols
            snap = con.execute(f"""
                WITH mx AS (SELECT MAX("{date_col}") AS mx_dt FROM "ohlcv")
                SELECT symbol, open, high, low, close, volume
                FROM "ohlcv", mx
                WHERE "{date_col}" = mx.mx_dt
                AND symbol IN ('AAPL','MSFT','SPY','QQQ')
                ORDER BY symbol
                LIMIT 20
            """).fetchdf()
            print("latest snapshot:")
            print(snap)

    # split
    if has("split"):
        print("\n[split]")
        print(con.execute('SELECT COUNT(*) FROM "split"').fetchone()[0], "rows")
        print(con.execute('SELECT * FROM "split" ORDER BY ex_date DESC LIMIT 5').fetchdf())

    # dividend
    if has("dividend"):
        print("\n[dividend]")
        print(con.execute('SELECT COUNT(*) FROM "dividend"').fetchone()[0], "rows")
        print(con.execute('SELECT * FROM "dividend" ORDER BY ex_date DESC LIMIT 5').fetchdf())

    con.close()
    print("\n✅ Done.")

if __name__ == "__main__":
    main()
