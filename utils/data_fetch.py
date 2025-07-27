import sys, os
import time
import pandas as pd
from datetime import datetime
import requests
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
# OPTIONS_DIR_ENV = os.getenv("TIINGO_OPTIONS_DIR", "data/raw/tiingo/options")
SYMBOLS_DIR_ENV = os.getenv("TIINGO_SYMBOLS_DIR", "data/raw/tiingo/symbols.csv")

PRICE_DIR = os.path.join(PROJECT_ROOT, PRICE_DIR_ENV)
OHLCV_DIR = os.path.join(PROJECT_ROOT, OHLCV_DIR_ENV)
# OPTIONS_DIR = os.path.join(PROJECT_ROOT, OPTIONS_DIR_ENV)
SYMBOLS_DIR = os.path.join(PROJECT_ROOT, SYMBOLS_DIR_ENV)

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
OPTIONS_DIR_ENV = os.getenv("POLYGON_OPTIONS_DIR", "data/raw/polygon/options")
OPTIONS_DIR = os.path.join(PROJECT_ROOT, OPTIONS_DIR_ENV)

if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY not set in .env (required for Polygon API).")

if not TIINGO_API_KEY:
    raise ValueError("TIINGO_API_KEY not set in .env or environment.")


# --- Configure Tiingo Client ---
TIINGO_CONFIG = {'session': True, 'api_key': TIINGO_API_KEY}
client = TiingoClient(TIINGO_CONFIG)

def get_available_symbols(active_only=True, force_refresh=False):
    """
    Fetch all available stock/ETF tickers from Tiingo using TiingoClient.
    
    Args:
        active_only (bool): Filter out delisted/inactive tickers.
        force_refresh (bool): Skip cache and always pull from Tiingo.
    
    Returns:
        pd.DataFrame: DataFrame of tickers with metadata.
    """
    # Return from cache unless forced
    if os.path.exists(SYMBOLS_DIR) and not force_refresh:
        return pd.read_csv(SYMBOLS_DIR)

    print("Fetching all stock/ETF tickers from Tiingo via client...")
    try:
        tickers = client.list_stock_tickers()
        # print(tickers)clear; pytho    
        df = pd.DataFrame(tickers)
        print(df.head())
        # Filter for active symbols
        # if active_only and "endDate" in df.columns:
        #     df = df[df["endDate"].isna()]

        # Cache the result
        os.makedirs(os.path.dirname('data/raw/tiingo/symbols.csv'), exist_ok=True)
        df.to_csv('data/raw/tiingo/symbols.csv', index=False)
        return df
    except Exception as e:
        print(f"Error fetching symbols via TiingoClient: {e}")
        return pd.DataFrame()
    
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

def get_options_chain(symbol, expiration=None, per_page=250, force_refresh=False):
    """
    Fetch the full options chain for a symbol from Polygon.io with pagination.
    
    Saves snapshot to: data/raw/polygon/options/YYYY-MM-DD/SYMBOL_options_YYYY-MM-DD.csv
    
    Args:
        symbol (str): Underlying ticker (e.g., "AAPL").
        expiration (str, optional): Filter by expiration date ("YYYY-MM-DD").
        per_page (int): Results per API call (max 250 for free tier).
        force_refresh (bool): Re-fetch even if cached.

    Returns:
        pd.DataFrame: Full options chain (all strikes, expiries, calls & puts).
    """
    today = datetime.today().strftime("%Y-%m-%d")
    date_dir = os.path.join(OPTIONS_DIR, today)
    os.makedirs(date_dir, exist_ok=True)

    cache_file = os.path.join(date_dir, f"{symbol}_options_{today}.csv")

    # Load cached result if exists
    if os.path.exists(cache_file) and not force_refresh:
        return pd.read_csv(cache_file, index_col=0, parse_dates=["expiration_date"])

    print(f"Fetching full options chain for {symbol} from Polygon.io (paginated)...")

    base_url = "https://api.polygon.io/v3/reference/options/contracts"
    params = {
        "underlying_ticker": symbol,
        "limit": per_page,
        "apiKey": POLYGON_API_KEY
    }
    if expiration:
        params["expiration_date"] = expiration

    all_results = []
    next_url = base_url

    while next_url:
        resp = requests.get(next_url, params=params if next_url == base_url else {}, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        all_results.extend(results)

        # Pagination: follow 'next_url' if present
        next_url = data.get("next_url", None)

    # Convert to DataFrame
    if not all_results:
        print(f"No options contracts found for {symbol}.")
        return pd.DataFrame()

    df = pd.DataFrame(all_results)

    # Normalize columns
    if "expiration_date" in df.columns:
        df["expiration_date"] = pd.to_datetime(df["expiration_date"])

    # Save snapshot
    df.to_csv(cache_file)
    return df

# def get_options_chain(symbol):
#     """
#     Fetch and cache today's live options chain for the given symbol from Tiingo.
    
#     Saves to: data/raw/tiingo/options/YYYY-MM-DD/SYMBOL_options_YYYY-MM-DD.csv
#     """
#     # Today's date for folder + file
#     date = datetime.today().strftime("%Y-%m-%d")
#     date_dir = os.path.join(OPTIONS_DIR, date)
#     os.makedirs(date_dir, exist_ok=True)

#     cache_file = os.path.join(date_dir, f"{symbol}_options_{date}.csv")

#     if os.path.exists(cache_file):
#         return pd.read_csv(cache_file, index_col=0, parse_dates=['expirationDate'])

#     print(f"Fetching live options chain for {symbol}...")
#     base_url = f"https://api.tiingo.com/tiingo/options/{symbol}/chains"
#     params = {"token": TIINGO_API_KEY}

#     try:
#         resp = requests.get(base_url, params=params)
#         resp.raise_for_status()
#         data = resp.json()

#         rows = []
#         for chain in data:
#             for opt_type in ["call", "put"]:
#                 for option in chain.get(opt_type + "s", []):
#                     row = option.copy()
#                     row["underlying"] = symbol
#                     row["type"] = opt_type
#                     row["expirationDate"] = pd.to_datetime(option["expirationDate"])
#                     rows.append(row)

#         df = pd.DataFrame(rows)
#         if not df.empty:
#             df.to_csv(cache_file)
#         time.sleep(1)
#         return df

#     except Exception as e:
#         print(f"Error fetching live chain for {symbol}: {e}")
#         return pd.DataFrame()

# def get_options_chain(symbol, expiration=None, force_refresh=False):
#     """
#     Fetches the live options chain for a symbol using Tradier API (sandbox by default).
#     Saves CSV snapshots by date for historical archiving.

#     Args:
#         symbol (str): Underlying ticker (e.g., "AAPL").
#         expiration (str, optional): Specific expiration date ("YYYY-MM-DD"). Defaults to nearest expiry.
#         force_refresh (bool): If True, fetch fresh even if cached.

#     Returns:
#         pd.DataFrame: Options chain DataFrame (calls + puts).
#     """
#     today = datetime.today().strftime("%Y-%m-%d")
#     date_dir = os.path.join(OPTIONS_DIR, today)
#     os.makedirs(date_dir, exist_ok=True)

#     cache_file = os.path.join(date_dir, f"{symbol}_options_{today}.csv")

#     # Return cached copy if available
#     if os.path.exists(cache_file) and not force_refresh:
#         return pd.read_csv(cache_file, index_col=0, parse_dates=["expiration_date"])

#     print(f"Fetching options chain for {symbol} from Tradier...")

#     # Step 1: Get available expirations if not provided
#     exp_url = f"{TRADIER_BASE}/markets/options/expirations"
#     exp_params = {"symbol": symbol, "includeAllRoots": "true", "strikes": "false"}
#     exp_headers = {"Authorization": f"Bearer {TRADIER_API_KEY}", "Accept": "application/json"}

#     resp = requests.get(exp_url, params=exp_params, headers=exp_headers)
#     resp.raise_for_status()
#     expirations = resp.json().get("expirations", {}).get("date", [])
#     if not expirations:
#         print(f"No options expirations found for {symbol}.")
#         return pd.DataFrame()

#     # Default to nearest expiry if none specified
#     expiration = expiration or expirations[0]

#     # Step 2: Fetch full chain for that expiration
#     chain_url = f"{TRADIER_BASE}/markets/options/chains"
#     chain_params = {"symbol": symbol, "expiration": expiration, "greeks": "true"}

#     resp = requests.get(chain_url, params=chain_params, headers=exp_headers)
#     resp.raise_for_status()
#     options = resp.json().get("options", {}).get("option", [])
#     if not options:
#         print(f"No chain data for {symbol} (exp: {expiration})")
#         return pd.DataFrame()

#     # Convert to DataFrame
#     df = pd.DataFrame(options)

#     # Clean & Normalize Column Names
#     df.rename(columns={"expiration_date": "expiration_date"}, inplace=True)
#     df["expiration_date"] = pd.to_datetime(df["expiration_date"])

#     # Cache for reuse
#     df.to_csv(cache_file)
#     return df