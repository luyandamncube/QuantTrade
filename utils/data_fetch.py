import sys, os
import time
import pandas as pd
from datetime import datetime
import
from tiingo import TiingoClient
from dotenv import load_dotenv

# --- Ensure Project Root is Always on sys.path ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# --- Load .env from Project Root ---
env_path = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(dotenv_path=env_path)

# --- Get Variables ---
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")

PRICE_DIR_ENV = os.getenv("TIINGO_PRICE_DIR", "data/raw/tiingo/price")
OHLCV_DIR_ENV = os.getenv("TIINGO_OHLCV_DIR", "data/raw/tiingo/ohlcv")
OPTIONS_DIR_ENV = os.getenv("TIINGO_OPTIONS_DIR", "data/raw/tiingo/options")

PRICE_DIR = os.path.join(PROJECT_ROOT, PRICE_DIR_ENV)
OHLCV_DIR = os.path.join(PROJECT_ROOT, OHLCV_DIR_ENV)
OPTIONS_DIR = os.path.join(PROJECT_ROOT, OPTIONS_DIR_ENV)

if not TIINGO_API_KEY:
    raise ValueError("TIINGO_API_KEY not set in .env or environment.")

# --- Configure Tiingo Client ---
TIINGO_CONFIG = {'session': True, 'api_key': TIINGO_API_KEY}
client = TiingoClient(TIINGO_CONFIG)


def get_price_data(symbols, start, end, ohlcv=False):
    """
    Fetch and cache price data (adjusted close or full OHLCV) for symbols via Tiingo.
    - If ohlcv=False (default): Returns a DataFrame of adjClose for all symbols.
                                Cached in TIINGO_PRICE_DIR as SYMBOL.csv.
    - If ohlcv=True: Returns a dict of DataFrames (one per symbol) with full OHLCV.
                     Cached in TIINGO_OHLCV_DIR as SYMBOL-OHLCV.csv.
    """
    # Use correct directory based on mode
    target_dir = OHLCV_DIR if ohlcv else PRICE_DIR
    os.makedirs(target_dir, exist_ok=True)

    all_data = {}

    for sym in symbols:
        filename = f"{sym}-OHLCV.csv" if ohlcv else f"{sym}.csv"
        cache_file = os.path.join(target_dir, filename)

        if os.path.exists(cache_file):
            df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
        else:
            print(f"Fetching {sym} from Tiingo...")
            try:
                df = client.get_dataframe(sym, startDate=start, endDate=end)
                if not ohlcv:
                    # Only keep adjusted close
                    if 'adjClose' in df.columns:
                        df = df[['adjClose']]
                    elif 'close' in df.columns:
                        df = df[['close']]
                    else:
                        raise ValueError(f"No close data for {sym}")
                    df.columns = [sym]

                # Cache file
                df.to_csv(cache_file)
                time.sleep(1)  # Avoid Tiingo API rate limit
            except Exception as e:
                print(f"Error fetching {sym}: {e}")
                continue

        all_data[sym] = df

    # For price-only mode, combine into one DataFrame
    if not ohlcv:
        return pd.concat(all_data.values(), axis=1).ffill().dropna()

    # For OHLCV, return a dict (one DataFrame per symbol)
    return all_data

def get_options_chain(symbol, fetch_new=True):
    """
    Fetch the *current* options chain for a given symbol from Tiingo (no date filters).
    Caches the chain in:
        data/raw/tiingo/options/YYYY-MM-DD/SYMBOL_options.csv

    If fetch_new=False, loads from the most recent cached date folder.

    Args:
        symbol (str): Underlying ticker (e.g., "AAPL").
        fetch_new (bool): If True, pulls fresh data and caches it. If False, loads last cache.

    Returns:
        pd.DataFrame: Flattened options chain (calls & puts).
    """
    os.makedirs(OPTIONS_DIR, exist_ok=True)

    # If not fetching, load most recent cached chain
    # if not fetch_new:
    #     date_folders = [d for d in os.listdir(OPTIONS_DIR) if os.path.isdir(os.path.join(OPTIONS_DIR, d))]
    #     if not date_folders:
    #         raise FileNotFoundError("No cached options data found.")
    #     latest_date = max(date_folders)
    #     cache_file = os.path.join(OPTIONS_DIR, latest_date, f"{symbol}_options.csv")
    #     if not os.path.exists(cache_file):
    #         raise FileNotFoundError(f"No cached data for {symbol} in {latest_date}.")
    #     print(f"Loading cached chain for {symbol} from {latest_date}.")
    #     return pd.read_csv(cache_file, index_col=0, parse_dates=['expirationDate'])

    # Fetch current chain
    today = datetime.today().strftime("%Y-%m-%d")
    # today = '2025-07-01'
    date_dir = os.path.join(OPTIONS_DIR, today)
    os.makedirs(date_dir, exist_ok=True)

    cache_file = os.path.join(date_dir, f"{symbol}_options.csv")
    base_url = f"https://api.tiingo.com/tiingo/options/{symbol}/chains"
    params = {"token": TIINGO_API_KEY}

    print(f"Fetching live options chain for {symbol}...")
    try:
        resp = requests.get(base_url, params=params)
        resp.raise_for_status()
        data = resp.json()

        rows = []
        for chain in data:
            for opt_type in ["call", "put"]:
                for option in chain.get(opt_type + "s", []):
                    row = option.copy()
                    row["underlying"] = symbol
                    row["type"] = opt_type
                    row["expirationDate"] = pd.to_datetime(option["expirationDate"])
                    rows.append(row)

        df = pd.DataFrame(rows)
        if not df.empty:
            df.to_csv(cache_file)
        else:
            print(f"No options data returned for {symbol}.")
        time.sleep(1)  # Avoid API throttling
        return df

    except Exception as e:
        print(f"Error fetching options for {symbol}: {e}")
        return pd.DataFrame()