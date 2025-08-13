# QuantTrade/utils/duck.py
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

__all__ = [
    "to_bt_daily_duckdb",
    "to_bt_minute_duckdb",
    "build_1m",  # optional: materialize minute bars in DuckDB
]

def to_bt_daily_duckdb(con, symbol: str,
                       table: str = "ohlcv",
                       date_col: str = "date",
                       symbol_col: str = "act_symbol",
                       start=None, end=None):
    """
    Build clean daily OHLCV from DuckDB:
    - tz-naive DatetimeIndex named 'datetime'
    - one row per calendar day per symbol
    - O=first, H=max, L=min, C=last, V=sum
    - optional start/end (inclusive) on the date column
    """
    where = [f"{symbol_col} = ?"]
    params = [symbol]

    if start is not None:
        where.append(f"{date_col} >= ?")
        params.append(pd.to_datetime(start).date())
    if end is not None:
        where.append(f"{date_col} <= ?")  # inclusive end
        params.append(pd.to_datetime(end).date())

    where_sql = "WHERE " + " AND ".join(where)

    # If there are multiple rows per (symbol, date), aggregate to true daily bars.
    q = f"""
    WITH d AS (
      SELECT
        {symbol_col} AS symbol,
        CAST({date_col} AS TIMESTAMP) AS datetime,
        first(open)  AS open,     -- 'first' within the day (if duplicates)
        max(high)    AS high,
        min(low)     AS low,
        last(close)  AS close,    -- 'last' within the day (if duplicates)
        sum(volume)  AS volume
      FROM {table}
      {where_sql}
      GROUP BY 1, 2
    )
    SELECT symbol, datetime, open, high, low, close, volume
    FROM d
    WHERE open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
    ORDER BY datetime
    """

    df = con.execute(q, params).df()
    if df.empty:
        # Return a properly-shaped empty frame
        return pd.DataFrame(columns=["open","high","low","close","volume"]).astype({
            "open":"float64","high":"float64","low":"float64","close":"float64","volume":"float64"
        }).set_index(pd.DatetimeIndex([], name="datetime"))

    df["datetime"] = pd.to_datetime(df["datetime"])  # tz-naive at midnight
    df.columns = [c.lower() for c in df.columns]
    out = df.drop(columns=["symbol"]).set_index("datetime").sort_index()
    return out


def to_bt_minute_duckdb(con, table, symbol):
    """
    Build *true* 1-minute OHLCV bars for a single symbol using DuckDB and return
    a tidy Pandas DataFrame indexed by tz-naive 'datetime'.

    Requirements (columns in `table`):
      - timestamp (TIMESTAMP)
      - open, high, low, close (NUMERIC)
      - volume (NUMERIC)
      - trade_count (optional but used below)
      - vwap (optional but used below)

    How it works:
      - date_trunc('minute', timestamp) bins rows into 1-minute buckets.
      - open  = first price in the minute  → min_by(open,  timestamp)
      - high  = max price in the minute    → max(high)
      - low   = min price in the minute    → min(low)
      - close = last price in the minute   → max_by(close, timestamp)
      - volume      = sum(volume) across the minute
      - trade_count = sum(trade_count) across the minute
      - vwap        = volume-weighted average across the minute:
                      sum(vwap * volume) / sum(volume)
                      (this correctly aggregates sub-minute VWAPs if each row’s
                       vwap is itself volume-weighted for that sub-interval)

    Notes:
      - GROUP BY 1 groups by the first selected column (datetime).
      - ORDER BY 1 sorts by that same column.
      - If your source table lacks `trade_count` or `vwap`, remove those lines.
    """
    
    q = f"""
    SELECT
      date_trunc('minute', timestamp) AS datetime,
      min_by(open, timestamp)  AS open,
      max(high)                 AS high,
      min(low)                  AS low,
      max_by(close, timestamp)  AS close,
      sum(volume)               AS volume,
      sum(trade_count)          AS trade_count,
      CASE WHEN sum(volume) > 0
           THEN sum(vwap * volume) / sum(volume)
           ELSE NULL END        AS vwap
    FROM {table}
    WHERE symbol = ?
    GROUP BY 1
    ORDER BY 1
    """
    df = con.execute(q, [symbol]).df()
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.set_index("datetime")

def build_1m(
    con: duckdb.DuckDBPyConnection,
    *,
    src_table: str = "alpaca_minute",
    symbol: str,
    out_table: str | None = None,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
) -> pd.DataFrame | str:
    """
    Build true 1-minute bars *in DuckDB* from a raw minute/tick table.
    If `out_table` is given, materializes into DuckDB and returns the table name;
    otherwise returns a Pandas DataFrame.
    """
    where = ["symbol = ?"]
    params: list = [symbol]
    if start:
        where.append("timestamp >= ?"); params.append(pd.to_datetime(start))
    if end:
        where.append("timestamp < ?");  params.append(pd.to_datetime(end))
    where_sql = "WHERE " + " AND ".join(where)

    q = f"""
    WITH m AS (
      SELECT
        symbol,
        date_trunc('minute', timestamp) AS datetime,
        min_by(open,  timestamp) AS open,
        max(high)                 AS high,
        min(low)                  AS low,
        max_by(close, timestamp)  AS close,
        sum(volume)               AS volume,
        sum(trade_count)          AS trade_count,
        CASE WHEN sum(volume) > 0
             THEN sum(vwap * volume) / sum(volume)
             ELSE NULL END        AS vwap
      FROM {src_table}
      {where_sql}
      GROUP BY 1,2
    )
    SELECT * FROM m ORDER BY symbol, datetime
    """
    if out_table:
        con.execute(f"CREATE OR REPLACE TABLE {out_table} AS {q}", params)
        return out_table
    df = con.execute(q, params).df()
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.set_index("datetime")
