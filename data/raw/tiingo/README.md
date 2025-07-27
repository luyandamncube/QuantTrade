
## Tiingo API Integration

This project uses [Tiingo](https://api.tiingo.com/) for price, fundamentals, crypto, and options data.  
Make sure you set up the required environment variables to enable API access and control data caching.

### Environment Setup

Add the following to your `.env` file (stored at the project root):

```dotenv
TIINGO_API_KEY='YOUR_API_KEY'
TIINGO_PRICE_DIR='data/raw/tiingo/price'
TIINGO_OHLCV_DIR='data/raw/tiingo/ohlcv'
TIINGO_OPTIONS_DIR='data/raw/tiingo/options'
```

## Supported Tiingo API Endpoints

Below is a summary of the main Tiingo API endpoints supported, grouped by data type.  
For most endpoints, you can use the official [`tiingo` Python client](https://tiingo-python.readthedocs.io/en/latest/readme.html) — others (like options chains) require direct HTTP requests.

| Data Category              | Endpoint URL Pattern                              | Access Notes                                     |
|----------------------------|----------------------------------------------------|--------------------------------------------------|
| **Daily Stock Prices**     | `/tiingo/daily/{ticker}/prices`                   | End-of-day OHLCV data, adjusted closes, etc.     |
| **Intraday / Real-Time**   | `/iex`                                             | 1-minute & real-time data (via IEX integration)  |
| **Ticker Metadata**        | `/tiingo/{ticker}` (metadata)                      | Company info, exchange, currency, listing dates  |
| **Available Symbols List** | `/tiingo/utilities/search`                         | Query tickers/assets by keyword                  |
| **Curated News**           | `/tiingo/news`                                     | Finance-related news tagged with tickers         |
| **Fundamentals (Daily)**   | `/fundamentals/daily/{ticker}`                     | Daily fundamental metrics                        |
| **Fundamentals (Statements)** | `/fundamentals/statements/{ticker}`             | Quarterly/annual income, balance sheet, cashflow |
| **Cryptocurrency Prices**  | `/tiingo/crypto/prices`                            | Intraday & historical crypto OHLCV               |
| **Options Chains (Live)**  | `/tiingo/options/{symbol}/chains`                  | Live options chain snapshots (calls & puts)      |
| **Options Historical (Pro)** | `/tiingo/options/prices` (Pro Tier only)         | Historical options prices, requires paid plan    |

### Usage Notes
- **Most endpoints** (daily prices, fundamentals, news, crypto) can be accessed via the `TiingoClient`:
  ```python
  from tiingo import TiingoClient
  client = TiingoClient({'api_key': 'YOUR_API_KEY'})
  df = client.get_dataframe("AAPL", startDate="2020-01-01", endDate="2025-01-01")
