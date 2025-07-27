from utils.data_fetch import get_available_symbols

# Get cached or live symbol list
symbols = get_available_symbols()
print(symbols.head())

# # Force a fresh fetch (ignores cache)
# fresh_symbols = get_available_symbols(force_refresh=True)
# print(fresh_symbols.shape)