import os,sys
import time
import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta
from pathlib import Path
import duckdb
from dotenv import load_dotenv

# --- Load environment variables ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "alpaca", "minute")
PROCESSED_DB = os.path.join(PROJECT_ROOT, "data", "processed", "alpaca", "minute.duckdb")

client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

# --- Create duckdb file if not created  ---
cwd = Path(os.getcwd())
duckdb_path = cwd / "data" / "processed" / "alpaca" / "minute.duckdb"
duckdb_path.parent.mkdir(parents=True, exist_ok=True)

def fetch_minute_data(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    req = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Minute,
        start=start,
        end=end,
        feed='iex'
    )
    bars = client.get_stock_bars(req).df
    if bars.empty:
        return bars
    bars = bars[bars.index.get_level_values("symbol") == symbol]
    bars.index.name = "timestamp"
    bars.reset_index(inplace=True)
    bars = bars.set_index("timestamp").sort_index()
    return bars

def ingest_symbol(symbol: str):
    os.makedirs(RAW_DIR, exist_ok=True)
    raw_csv_path = os.path.join(RAW_DIR, f"minute_{symbol}.csv")

    # Load existing data if any
    if os.path.exists(raw_csv_path):
        existing = pd.read_csv(raw_csv_path, parse_dates=["timestamp"], index_col="timestamp")
        last_ts = existing.index[-1]
        start = last_ts + timedelta(minutes=1)
    else:
        existing = pd.DataFrame()
        start = datetime.utcnow() - timedelta(days=730)  # ~2 years

    end = datetime.utcnow()
    print(f"Fetching {symbol}: {start} to {end}")

    try:
        new_data = fetch_minute_data(symbol, start=start, end=end)
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    if not new_data.empty:
        combined = pd.concat([existing, new_data]).sort_index().drop_duplicates()
        combined.to_csv(raw_csv_path)
        print(f"Saved to {raw_csv_path}")

        # Save to DuckDB
        with duckdb.connect(PROCESSED_DB) as con:
            con.execute(f"CREATE TABLE IF NOT EXISTS minute_{symbol} AS SELECT * FROM combined LIMIT 0")
            con.execute(f"INSERT INTO minute_{symbol} SELECT * FROM combined")
            print(f"Written to DuckDB: table minute_{symbol}")
    else:
        print("No new data")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("symbol", type=str, help="Ticker symbol to ingest")
    args = parser.parse_args()
    ingest_symbol(args.symbol.upper())
