import sys, os
import time
import pandas as pd
from tiingo import TiingoClient
from dotenv import load_dotenv
from datetime import datetime

# --- Ensure Project Root is Always on sys.path ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# --- Load .env from Project Root ---
env_path = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(dotenv_path=env_path)  # Explicitly load from root

# --- Get Variables ---
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")
CACHE_DIR_ENV = os.getenv("CACHE_DIR", "data/raw/tiingo")  # fallback if not in .env
CACHE_DIR = os.path.join(PROJECT_ROOT, CACHE_DIR_ENV)      # make absolute

if not TIINGO_API_KEY:
    raise ValueError("TIINGO_API_KEY not set in .env or environment.")

# --- Configure Tiingo Client ---
TIINGO_CONFIG = {'session': True, 'api_key': TIINGO_API_KEY}
client = TiingoClient(TIINGO_CONFIG)


def get_price_data(symbols, start, end):
    """
    Fetch and cache adjusted close prices for symbols via Tiingo.
    Cache directory is defined in .env but resolved relative to project root.
    """
    os.makedirs(CACHE_DIR_ENV, exist_ok=True)
    all_data = pd.DataFrame()

    for sym in symbols:
        cache_file = os.path.join(CACHE_DIR_ENV, f"{sym}.csv")

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