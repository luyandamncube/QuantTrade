import os
print(f"Working directory: {os.getcwd()}")
from utils.data_fetch import get_price_data

symbols = ["SPY", "AGG", "VWO"]
prices = get_price_data(symbols, "2018-01-01", "2024-12-31")

print(prices.tail())