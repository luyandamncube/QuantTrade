# QuantTrade/tests/test_duck_db_options.py
import duckdb
from pathlib import Path

# Path to your DuckDB file
DUCKDB_PATH = Path("data/processed/dolt/options.duckdb")

def main():
    if not DUCKDB_PATH.exists():
        print(f"❌ DuckDB file not found: {DUCKDB_PATH}")
        return

    with duckdb.connect(str(DUCKDB_PATH)) as con:
        # List all tables
        print("📋 Tables in DB:")
        tables = con.execute("SHOW TABLES").fetchall()
        for t in tables:
            print("  -", t[0])

        # Check row counts
        for t in tables:
            table_name = t[0]
            cnt = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"✅ {table_name}: {cnt:,} rows")

        # Sample data from option_chain
        if ("option_chain",) in tables:
            print("\n🔍 Sample option_chain rows:")
            print(con.execute("SELECT * FROM option_chain LIMIT 5").fetchdf())

        # Sample data from volatility_history
        if ("volatility_history",) in tables:
            print("\n🔍 Sample volatility_history rows:")
            print(con.execute("SELECT * FROM volatility_history LIMIT 5").fetchdf())

if __name__ == "__main__":
    main()
