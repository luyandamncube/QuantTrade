import os
import duckdb
from pathlib import Path

def test_duckdb_connection():
    # Dynamically construct the DB path from the current working directory
    project_root = Path(__file__).resolve().parent.parent  # Adjust if needed
    db_path = project_root / "data" / "processed" / "alpaca" / "minute.duckdb"

    if not db_path.exists():
        print(f"❌ DuckDB file not found at: {db_path}")
        return

    print(f"✅ Found DuckDB at: {db_path}")

    # Connect and check for tables
    try:
        con = duckdb.connect(str(db_path))
        tables = con.execute("SHOW TABLES").fetchall()

        if not tables:
            print("⚠️ No tables found in the DuckDB file.")
        else:
            print("📊 Tables in DuckDB:")
            for (table_name,) in tables:
                print(f"  - {table_name}")
                # Uncomment to preview contents:
                # df = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
                # print(df)

    except Exception as e:
        print(f"❌ Error connecting to DuckDB: {e}")

if __name__ == "__main__":
    test_duckdb_connection()
