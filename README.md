# QuantTrade
Learning how to apply quantiative methods & thinking to real world trading scenarios 

## Thought process 
1. Screen - Find trade idea
2. Research - Develop Trade Hypothesis
3. Validate - Build straegy/model
4. Validate - Calculate performance

## Data source(s)
- [Tiingo](https://www.tiingo.com/products/stock-api) provides a free tier for pulling financial data 

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
│   └── processed/             # Cleaned time series (used for backtests)
│
├── results/                   # Output from backtests
│   ├── summaries/             # CSV/JSON summary stats for each run
│   └── charts/                # Plots (portfolio growth curves, comparisons)
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
