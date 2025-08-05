import os, sys
from datetime import datetime, timedelta
import pandas as pd
import duckdb
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from dotenv import load_dotenv
from pathlib import Path

# # Load .env
# PROJECT_ROOT = Path(__file__).resolve().parents[1]
# load_dotenv(PROJECT_ROOT / ".env")

# ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
# ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

# RAW_DIR = PROJECT_ROOT / "data" / "raw" / "alpaca" / "price"
# DUCKDB_PATH = PROJECT_ROOT / "data" / "processed" / "alpaca" / "price.duckdb"

# client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

# --- Load environment variables ---
PROJECT_ROOT = Path.cwd()
CWD = Path(os.getcwd())
DUCKDB_PATH = PROJECT_ROOT / "data" / "processed" / "alpaca" / "price.duckdb"

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# print(f"Project Root: {PROJECT_ROOT}")
# print(f"Working directory: {CWD}")
# print(f"duckdb path: {DUCKDB_PATH}")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
# RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "alpaca", "price")
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "alpaca" / "price"
PROCESSED_DB = os.path.join(PROJECT_ROOT, "data", "processed", "alpaca", "price.duckdb")

client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

# --- Create duckdb file if not created  ---
DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)


def fetch_daily_data(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    req = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Day,
        start=start,
        end=end,
        feed="iex"
    )
    bars = client.get_stock_bars(req).df
    if bars.empty:
        return bars
    bars = bars[bars.index.get_level_values("symbol") == symbol]
    bars.index.name = "timestamp"
    bars.reset_index(inplace=True)
    return bars.set_index("timestamp").sort_index()

def ingest_daily_symbol(symbol: str):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RAW_DIR / f"{symbol}.csv"

    # Load existing
    if csv_path.exists():
        existing = pd.read_csv(csv_path, parse_dates=["timestamp"], index_col="timestamp")
        start = existing.index[-1] + timedelta(days=1)
    else:
        existing = pd.DataFrame()
        start = datetime.utcnow() - timedelta(days=730)

    end = datetime.utcnow()
    print(f"[{symbol}] Fetching from {start} to {end}")

    try:
        new_data = fetch_daily_data(symbol, start, end)
    except Exception as e:
        print(f"[{symbol}] Fetch failed: {e}")
        return

    if not new_data.empty:
        combined = pd.concat([existing, new_data]).sort_index().drop_duplicates()
        combined.to_csv(csv_path)
        print(f"[{symbol}] Saved to {csv_path}")

        with duckdb.connect(str(DUCKDB_PATH)) as con:
            combined_reset = combined.reset_index()
            con.execute(f"CREATE TABLE IF NOT EXISTS daily_{symbol} AS SELECT * FROM combined_reset LIMIT 0")
            con.execute(f"INSERT INTO daily_{symbol} SELECT * FROM combined_reset")
            print(f"[{symbol}] Written to DuckDB: table daily_{symbol}")
    else:
        print(f"[{symbol}] No new data")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("symbol", type=str)
    args = parser.parse_args()
    ingest_daily_symbol(args.symbol.upper())
