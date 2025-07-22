import os
import time
import pandas as pd
from tiingo import TiingoClient
from dotenv import load_dotenv
from datetime import datetime

# --- Tiingo API Configuration ---
load_dotenv()

# Resolve project root (two levels up from this file)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Get .env values
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")
CACHE_DIR_ENV = os.getenv("CACHE_DIR", "data/raw/tiingo")

if not TIINGO_API_KEY:
    raise ValueError("Missing TIINGO_API_KEY in .env file or environment variables.")
 
TIINGO_CONFIG = {
    'session': True,
    'api_key': TIINGO_API_KEY
}
client = TiingoClient(TIINGO_CONFIG)


def get_price_data(symbols, start, end):
    """
    Fetch and cache adjusted close prices for symbols via Tiingo.
    Cache directory is defined in .env but resolved relative to project root.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    all_data = pd.DataFrame()

    for sym in symbols:
        cache_file = os.path.join(CACHE_DIR, f"{sym}.csv")

        if os.path.exists(cache_file):
            df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
        else:
            print(f"Fetching {sym} from Tiingo...")
            try:
                df = client.get_dataframe(sym, startDate=start, endDate=end)
                if 'adjClose' in df.columns:
                    df = df[['adjClose']]
                elif 'close' in df.columns:
                    df = df[['close']]
                else:
                    raise ValueError(f"No close data for {sym}")
                df.columns = [sym]
                df.to_csv(cache_file)
                time.sleep(1)
            except Exception as e:
                print(f"Error fetching {sym}: {e}")
                continue

        all_data = pd.concat([all_data, df], axis=1)

    return all_data.ffill().dropna()
