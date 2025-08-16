from __future__ import annotations

import duckdb
import pandas as pd
from typing import Dict, List

__all__ = [
    "add_mas_duckdb",
    "add_rsi_duckdb",
    "add_emas_duckdb",
    "ema_crossover_signals_duckdb"
]

def _stack_for_duck(data_by_sym: dict[str, pd.DataFrame], *, required: list[str]) -> pd.DataFrame:
    frames = []
    for sym, df in data_by_sym.items():
        if df.empty: 
            continue
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise KeyError(f"{sym}: missing columns {missing}")
        tmp = df.sort_index().reset_index()
        tmp = tmp.rename(columns={tmp.columns[0]: "datetime"})
        tmp["symbol"] = sym
        frames.append(tmp)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _prep_for_duckdb(df: pd.DataFrame, order_col: str) -> tuple[pd.DataFrame, str | None, object | None]:
    idx_name = df.index.name or order_col
    tzinfo = df.index.tz if isinstance(df.index, pd.DatetimeIndex) else None
    tdf = df.reset_index()
    # standardize the order column name
    if tdf.columns[0] != order_col:
        tdf = tdf.rename(columns={tdf.columns[0]: order_col})
    tdf = tdf.sort_values(order_col).reset_index(drop=True)
    return tdf, idx_name, tzinfo

def ensure_ema_cols(df: pd.DataFrame, periods: list[int], prefix="ema"):
    missing = [p for p in periods if f"{prefix}{p}" not in df.columns]
    if missing:
        raise KeyError(f"Missing EMA columns: {missing}. "
                       f"Run add_emas_duckdb(..., windows={periods}) first.")
def add_mas_duckdb(
    data_by_sym: dict[str, pd.DataFrame],
    con: duckdb.DuckDBPyConnection,
    windows: list[int],
    *,
    price_col: str = "close",
    prefix: str = "ma",
) -> dict[str, pd.DataFrame]:
    """
    Add SMA columns (e.g., ma20, ma50, …) to each df using DuckDB window fns.
    """
    if not data_by_sym or not windows:
        return data_by_sym
    if any(w <= 0 for w in windows):
        raise ValueError("All MA windows must be positive.")
    all_bars = _stack_for_duck(data_by_sym, required=[price_col])
    if all_bars.empty:
        return data_by_sym

    view = "_bars_for_ma"
    con.register(view, all_bars)

    ma_cols_sql = ",\n".join(
        [
            f"avg({price_col}) OVER (PARTITION BY symbol ORDER BY datetime "
            f"ROWS BETWEEN {w-1} PRECEDING AND CURRENT ROW) AS {prefix}{w}"
            for w in windows
        ]
    )
    sql = f"SELECT *, {ma_cols_sql} FROM {view} ORDER BY symbol, datetime"
    result = con.execute(sql).df()
    con.unregister(view)

    result["datetime"] = pd.to_datetime(result["datetime"])
    out: dict[str, pd.DataFrame] = {}
    for sym, g in result.groupby("symbol", sort=False):
        out[sym] = g.drop(columns=["symbol"]).set_index("datetime").sort_index()
    return out

def add_rsi_duckdb(
    data_by_sym: dict[str, pd.DataFrame],
    con: duckdb.DuckDBPyConnection,
    *,
    period: int = 14,
    price_col: str = "close",
    colname: str | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Add SMA-style RSI (non-recursive) as column `rsi{period}` (or custom name).
    Good, fast approximation; for Wilder’s exact RSI we can switch to recursive CTE.
    """
    if not data_by_sym:
        return data_by_sym
    if period <= 0:
        raise ValueError("`period` must be positive.")
    colname = colname or f"rsi{period}"

    all_bars = _stack_for_duck(data_by_sym, required=[price_col])
    if all_bars.empty:
        return data_by_sym

    view = "_bars_for_rsi"
    con.register(view, all_bars)

    N = period
    sql = f"""
    WITH base AS (
      SELECT
        symbol,
        datetime,
        {price_col}::DOUBLE AS close,
        LAG({price_col}) OVER (PARTITION BY symbol ORDER BY datetime) AS prev_close
      FROM {view}
    ),
    deltas AS (
      SELECT
        symbol, datetime,
        CASE WHEN prev_close IS NULL THEN NULL
             WHEN close - prev_close > 0 THEN (close - prev_close) ELSE 0 END AS gain,
        CASE WHEN prev_close IS NULL THEN NULL
             WHEN close - prev_close < 0 THEN (prev_close - close) ELSE 0 END AS loss
      FROM base
    ),
    win AS (
      SELECT
        symbol, datetime,
        AVG(gain) OVER (
          PARTITION BY symbol ORDER BY datetime
          ROWS BETWEEN {N-1} PRECEDING AND CURRENT ROW
        ) AS avg_gain,
        AVG(loss) OVER (
          PARTITION BY symbol ORDER BY datetime
          ROWS BETWEEN {N-1} PRECEDING AND CURRENT ROW
        ) AS avg_loss,
        COUNT(*) FILTER (WHERE gain IS NOT NULL) OVER (
          PARTITION BY symbol ORDER BY datetime
          ROWS BETWEEN {N-1} PRECEDING AND CURRENT ROW
        ) AS cnt_valid
      FROM deltas
    ),
    rsi_calc AS (
      SELECT
        symbol, datetime,
        CASE
          WHEN cnt_valid < {N} THEN NULL
          WHEN avg_loss = 0 THEN 100.0
          ELSE 100.0 - 100.0 / (1.0 + (avg_gain / NULLIF(avg_loss, 0)))
        END AS {colname}
      FROM win
    )
    SELECT v.*, r.{colname}
    FROM {view} AS v
    LEFT JOIN rsi_calc AS r USING (symbol, datetime)
    ORDER BY symbol, datetime
    """
    result = con.execute(sql).df()
    con.unregister(view)

    result["datetime"] = pd.to_datetime(result["datetime"])
    out: dict[str, pd.DataFrame] = {}
    for sym, g in result.groupby("symbol", sort=False):
        out[sym] = g.drop(columns=["symbol"]).set_index("datetime").sort_index()
    return out

def add_emas_duckdb(
    data_by_sym: Dict[str, pd.DataFrame],
    con: duckdb.DuckDBPyConnection,
    windows: List[int],
    *,
    price_col: str = "close",
    prefix: str = "ema",
) -> Dict[str, pd.DataFrame]:
    """
    Add EMA columns (e.g., ema20, ema50, …) to each df.

    Mirrors `add_mas_duckdb`:
      1) stack dict -> single DF
      2) ORDER BY symbol, datetime via DuckDB
      3) compute EMAs (vectorized pandas .ewm)
      4) split back to dict with datetime index

    Notes:
    - DuckDB has no built-in ema() function; we compute EMA in pandas after ordering.
    - Returns new per-symbol DataFrames with EMA columns appended.
    """
    if not data_by_sym or not windows:
        return data_by_sym
    if any(w <= 0 for w in windows):
        raise ValueError("All EMA windows must be positive.")

    # 1) Stack to a single table (reuses your helper & lowercase convention)
    all_bars = _stack_for_duck(data_by_sym, required=[price_col])
    if all_bars.empty:
        return data_by_sym

    # 2) Use DuckDB to guarantee global ordering by symbol, datetime
    view = "_bars_for_ema"
    con.register(view, all_bars)
    ordered = con.execute(f"SELECT * FROM {view} ORDER BY symbol, datetime").df()
    con.unregister(view)

    # 3) Vectorized EMA per symbol for each requested window
    ordered["datetime"] = pd.to_datetime(ordered["datetime"])
    for w in windows:
        col = f"{prefix}{w}"
        # groupby-transform keeps original row order and aligns output
        ordered[col] = (
            ordered
            .groupby("symbol", sort=False)[price_col]
            .transform(lambda s: s.ewm(span=w, adjust=False).mean())
        )

    # 4) Split back into dict, drop 'symbol', set datetime index, sort index
    out: Dict[str, pd.DataFrame] = {}
    for sym, g in ordered.groupby("symbol", sort=False):
        out[sym] = (
            g.drop(columns=["symbol"])
             .set_index("datetime")
             .sort_index()
        )
    return out

def ema_crossover_signals_duckdb(
    data_by_sym: dict,
    con,
    fast: int = 12,
    slow: int = 26,
    order_col: str = "datetime",
    restore_index: bool = True,
):
    """
    Adds signal_raw, long_cross, flat_cross in DuckDB and (optionally) restores the original datetime index.
    Requires ema{fast} & ema{slow} columns to exist.
    """
    out = {}

    for sym, df in data_by_sym.items():
        # sanity: make sure EMAs exist
        for p in (fast, slow):
            if f"ema{p}" not in df.columns:
                raise KeyError(f"Missing column ema{p} for {sym}. Run add_emas_duckdb(...) first.")

        # expose index for SQL, remember original index name & tz
        tdf, idx_name, tzinfo = _prep_for_duckdb(df, order_col)

        con.register("t", tdf)
        q = f"""
        SELECT
            t.*,
            CASE WHEN ema{fast} > ema{slow} THEN 1 ELSE 0 END AS signal_raw,
            CASE
                WHEN LAG(CASE WHEN ema{fast} > ema{slow} THEN 1 ELSE 0 END)
                     OVER (ORDER BY {order_col}) = 0
                 AND (ema{fast} > ema{slow})
                THEN 1 ELSE 0
            END AS long_cross,
            CASE
                WHEN LAG(CASE WHEN ema{fast} > ema{slow} THEN 1 ELSE 0 END)
                     OVER (ORDER BY {order_col}) = 1
                 AND (ema{fast} <= ema{slow})
                THEN 1 ELSE 0
            END AS flat_cross
        FROM t
        ORDER BY {order_col}
        """
        res = con.execute(q).df()
        con.unregister("t")

        if restore_index:
            # ensure datetime dtype, then restore as index and tz
            res[order_col] = pd.to_datetime(res[order_col], utc=tzinfo is not None)
            res = res.set_index(order_col).sort_index()
            res.index.name = idx_name or order_col
            if tzinfo is not None:
                # If we parsed with utc=True above, convert to original tz
                res.index = res.index.tz_convert(tzinfo)

        out[sym] = res

    return out

