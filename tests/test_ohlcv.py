import os
print(f"Working directory: {os.getcwd()}")

from utils.data_fetch import get_price_data

symbols = ["SPY", "AGG", "VWO"]
ohlc_data = get_price_data(symbols, "2018-01-01", "2024-12-31", ohlcv=True)

# Print keys (symbols)
print(f"Symbols fetched: {list(ohlc_data.keys())}")

# Look at SPY’s OHLCV data
# print("SPY OHLCV sample:")
# print(ohlc_data["SPY"].head())

# Or loop through all symbols
for sym, df in ohlc_data.items():
    print(f"\n{sym} OHLCV last 5 rows:")
    print(df.tail())
