import duckdb
import os

# Root directory (adjust if needed)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "processed", "dolt")

# List of Parquet files to preview
PARQUET_FILES = [
    # Options
    os.path.join(DATA_DIR, "options", "option_chain.parquet"),
    os.path.join(DATA_DIR, "options", "volatility_history.parquet"),

    # Rates
    os.path.join(DATA_DIR, "rates", "us_treasury.parquet"),

    # Earnings (balance sheets & financials)
    os.path.join(DATA_DIR, "earnings", "balance_sheet_assets.parquet"),
    os.path.join(DATA_DIR, "earnings", "balance_sheet_equity.parquet"),
    os.path.join(DATA_DIR, "earnings", "balance_sheet_liabilities.parquet"),
    os.path.join(DATA_DIR, "earnings", "cash_flow_statement.parquet"),
    os.path.join(DATA_DIR, "earnings", "earnings_calendar.parquet"),
    os.path.join(DATA_DIR, "earnings", "eps_estimate.parquet"),
    os.path.join(DATA_DIR, "earnings", "eps_history.parquet"),
    os.path.join(DATA_DIR, "earnings", "income_statement.parquet"),
    os.path.join(DATA_DIR, "earnings", "rank_score.parquet"),
    os.path.join(DATA_DIR, "earnings", "sales_estimate.parquet"),
]

def peek_parquet(file_path: str, limit: int = 10):
    """Preview a Parquet file with DuckDB."""
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    try:
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM '{file_path}' LIMIT {limit}").df()
        print(f"\n=== Preview: {file_path} ===")
        print(df)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

if __name__ == "__main__":
    for file in PARQUET_FILES:
        peek_parquet(file, limit=10)
