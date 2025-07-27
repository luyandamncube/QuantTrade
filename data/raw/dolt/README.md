# Dolt Market Data Workflow

This folder contains **raw datasets** cloned from [DoltHub](https://www.dolthub.com/), including **earnings**, **options**, and **rates** data.

The steps below document how to **set up Dolt, pull the latest data, dump CSVs, and convert to Parquet** for easier use in research or pipelines.

---

## 1. Install Dolt

```bash
brew install dolt
```

## 2. Clone and Dump Each Dataset
Clone the repositories into data/raw/dolt/ and dump all tables as CSV files.

### Earnings Data

```bash
cd data/raw/dolt
dolt clone post-no-preference/earnings
cd earnings
dolt dump -r csv
```
This creates:
```bash
balance_sheet_assets.csv
balance_sheet_equity.csv
balance_sheet_liabilities.csv
cash_flow_statement.csv
earnings_calendar.csv
eps_estimate.csv
eps_history.csv
income_statement.csv
rank_score.csv
sales_estimate.csv
```

### Options Data
```bash
cd data/raw/dolt
dolt clone post-no-preference/options
cd options
dolt dump -r csv
```

This creates:
```bash
option_chain.csv
volatility_history.csv
```

### Rates Data
```bash
cd data/raw/dolt
dolt clone post-no-preference/rates
cd rates
dolt dump -r csv
```

This creates:
```bash
us_treasury.csv
```

## 3. Convert CSVs to Parquet

```bash
python utils/csv_to_parquet.py --src data/raw/dolt/earnings/doltdump --dest data/processed/earnings
python utils/csv_to_parquet.py --src data/raw/dolt/options/doltdump --dest data/processed/options
python utils/csv_to_parquet.py --src data/raw/dolt/rates/doltdump --dest data/processed/rates
```
