# QuantTrade
Learning how to apply quantiative methods & thinking to real world trading scenarios 

## Thought process 
1. Screen - Find trade idea
2. Research - Develop Trade Hypothesis
3. Validate - Build straegy/model
4. Validate - Calculate performance

## Data source(s)
- [Tiingo](https://www.tiingo.com/products/stock-api) provides a free tier for pulling financial data (symbols, day resolution)
- [Dolthub](https://www.dolthub.com/repositories/post-no-preference/options/data/master) has a larg ~6.GB public options database

 ```
  brew install dolt
  dolt clone post-no-preference/options
  cd ./options 
  dolt dump -r csv
  ```
- The above script on mac will create a csv dump of options data (`option_chain.csv`)

## Repo directory

```
QuantTrade
│
├── notebooks/                 # All Jupyter notebooks for research
│   ├── etf_backtesting.ipynb  # The ETF DCA research notebook
│   ├── strategy_tests.ipynb   # Placeholder for future strategy prototypes
│   └── utils_demo.ipynb       # Example usage of common functions
│
├── strategies/                # Standalone strategy scripts (Python)
│   ├── etf_dca.py             # Python script version (for Lean / live deploy)
│   └── custom_strategies/     # Additional trading models
│
├── data/                      # Raw or processed market data
│   ├── raw/                   # Unprocessed data (downloads from Yahoo, etc.)
│   │   ├── tiingo/price/          # Daily adjClose
│   │   ├── tiingo/ohlcv/          # Full OHLCV
│   │   ├── polygon/options/       # Options chains (CSV snapshots)
│   │   ├── price/minute/          # Parquet (minute bars)
│   │   └── options/tick/          # Parquet (tick chains if needed)
│   └── processed/             # Cleaned time series (used for backtests)
│
├── results/                   # Output from backtests
│   ├── summaries/             # CSV/JSON summary stats for each run
│   └── charts/                # Plots (portfolio growth curves, comparisons)
│
├── sql/                        # SQL migrations
│   └── SQL_table_setup.sql
│
├── utils/                     # Helper modules (shared functions)
│   ├── data_fetch.py          # Functions to get data (Yahoo, QC, etc.)
│   ├── portfolio_tools.py     # DCA, rebalancing, drawdowns, stats
│   └── plotting.py            # Common plotting utilities
│
├── requirements.txt           # Dependencies for running everything locally
├── README.md                  # Overview of the repo and usage instructions
└── .gitignore                 # Ignore data dumps, logs, cache
```

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
