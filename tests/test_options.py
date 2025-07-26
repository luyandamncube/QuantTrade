from utils.data_fetch import get_options_chain

# Fetch live options chain (will cache under today's date)
chain = get_options_chain("AAPL", fetch_new=True)
print(chain.head())

# Load the latest cached chain (no new fetch)
cached_chain = get_options_chain("AAPL", fetch_new=False)
print(cached_chain.tail())
