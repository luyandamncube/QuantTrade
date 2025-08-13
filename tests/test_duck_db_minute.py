import os, sys
import duckdb
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

# --- Load environment variables ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "alpaca", "minute")
PROCESSED_DB = os.path.join(PROJECT_ROOT, "data", "processed", "alpaca", "price_minute_alpaca.duckdb")

def test_duckdb_connection():

    # Step 1: Define DB path
    project_root = Path(__file__).resolve().parent.parent  # Adjust if needed
    db_path = project_root / "data" / "processed" / "alpaca" / "price_minute_alpaca.duckdb"

    if not db_path.exists():
        print(f"DuckDB file not found at: {db_path}")
        return

    print(f"✅ Found DuckDB at: {db_path}")

    try:
        # Step 2: Connect to DuckDB
        con = duckdb.connect(str(db_path))

        # Step 3: List all tables
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        if tables:
            print("\nAvailable tables:")
            for name in table_names:
                print(f"  - {name}")
        else: 
            print(f"No tables found in {db_path}")

        # Step 4: Print schema for each table
        for table in table_names:
            print(f"\n📄 Schema for table `{table}`:")
            schema_df = con.execute(f"PRAGMA table_info('{table}')").fetchdf()
            print(schema_df)

        # Step 5: Preview each table (first 5 rows)
        for table in table_names:
            print(f"\nPreview of `{table}`:")
            try:
                preview = con.execute(f"SELECT * FROM {table} ORDER BY timestamp DESC LIMIT 5").fetchdf()
                print(preview)
            except Exception as e:
                print(f"Error querying {table}: {e}")

    except Exception as e:
        print(f"Error connecting to DuckDB: {e}")

if __name__ == "__main__":
    test_duckdb_connection()
