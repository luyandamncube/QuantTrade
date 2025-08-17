from __future__ import annotations

import duckdb
import pandas as pd
import numpy as np
from typing import Dict, List
import backtrader as bt
import math

__all__ = [
    "add_mas_duckdb",
    "add_rsi_duckdb",
    "add_emas_duckdb",
    "add_macd_duckdb",
    "ema_crossover_signals_duckdb",
    # "add_atr_duckdb",
    # "add_keltner_duckdb",
    # "add_hhll_duckdb",
    # "add_atr_trailing_stops_duckdb",
    # "add_choppiness_duckdb",
    # "add_bollinger_bw_duckdb",
    "add_atr_feature_pack_duckdb"
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

def add_macd_duckdb(
    data_by_sym: Dict[str, pd.DataFrame],
    con: duckdb.DuckDBPyConnection,
    *,
    price_col: str = "close",
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    prefix: str = "macd",
) -> Dict[str, pd.DataFrame]:
    """
    Add MACD columns to each per-symbol DataFrame in `data_by_sym`.

    Columns added:
      - f"{prefix}"          : MACD line (EMA(fast) - EMA(slow))
      - f"{prefix}_signal"   : Signal line (EMA of MACD line, span=signal)
      - f"{prefix}_hist"     : Histogram (MACD - Signal)

    Approach mirrors `add_emas_duckdb`:
      1) Stack dict -> single DF (expects columns: symbol, datetime, <price_col>)
      2) Use DuckDB to ORDER BY symbol, datetime (canonical ordering)
      3) Compute EMAs via pandas groupby-transform (DuckDB has no EMA)
      4) Split back into dict with datetime index sorted ascending

    Notes:
    - Requires an existing `_stack_for_duck(data_by_sym, required=[price_col])`
      that returns columns ['symbol','datetime',..., price_col] with lowercase names.
    - All input DataFrames should have lowercase column names and a DatetimeIndex
      (or at least a 'datetime' column the stacker creates).
    """
    if not data_by_sym:
        return data_by_sym
    if fast <= 0 or slow <= 0 or signal <= 0:
        raise ValueError("MACD parameters must be positive integers.")
    if slow <= fast:
        raise ValueError("`slow` must be greater than `fast`.")
    # 1) Stack to a single table
    all_bars = _stack_for_duck(data_by_sym, required=[price_col])
    if all_bars.empty:
        return data_by_sym

    # 2) Enforce global ordering via DuckDB
    view = "_bars_for_macd"
    con.register(view, all_bars)
    ordered = con.execute(f"SELECT * FROM {view} ORDER BY symbol, datetime").df()
    con.unregister(view)

    # 3) Compute EMAs and MACD per symbol
    ordered["datetime"] = pd.to_datetime(ordered["datetime"])
    g = ordered.groupby("symbol", sort=False)

    fast_ema = g[price_col].transform(lambda s: s.ewm(span=fast, adjust=False).mean())
    slow_ema = g[price_col].transform(lambda s: s.ewm(span=slow, adjust=False).mean())
    macd_line = fast_ema - slow_ema
    macd_signal = macd_line.groupby(ordered["symbol"], sort=False).transform(
        lambda s: s.ewm(span=signal, adjust=False).mean()
    )
    macd_hist = macd_line - macd_signal

    ordered[f"{prefix}"] = macd_line
    ordered[f"{prefix}_signal"] = macd_signal
    ordered[f"{prefix}_hist"] = macd_hist

    # 4) Split back to dict, index by datetime
    out: Dict[str, pd.DataFrame] = {}
    for sym, gdf in ordered.groupby("symbol", sort=False):
        out[sym] = (
            gdf.drop(columns=["symbol"])
               .set_index("datetime")
               .sort_index()
        )
    return out

def add_atr_duckdb(
    data_by_sym: Dict[str, pd.DataFrame],
    con: duckdb.DuckDBPyConnection,
    n: int = 14,
    *,
    price_cols: Tuple[str,str,str] = ("high","low","close"),
    wilder: bool = True,          # Wilder ATR via EWM(alpha=1/n)
    add_tr: bool = True,
    add_natr: bool = True,
    prefix: str = "atr",
) -> Dict[str, pd.DataFrame]:
    """
    Adds TR (optional), ATR (atr{n}) and NATR (natr{n}) per symbol.
    Requires columns: high, low, close.
    """
    if not data_by_sym:
        return data_by_sym
    h,l,c = price_cols
    all_bars = _stack_for_duck(data_by_sym, required=[h,l,c])
    if all_bars.empty:
        return data_by_sym

    view = "_bars_for_atr"
    con.register(view, all_bars)
    ordered = con.execute(f"SELECT * FROM {view} ORDER BY symbol, datetime").df()
    con.unregister(view)

    ordered["datetime"] = pd.to_datetime(ordered["datetime"])
    def _true_range(g: pd.DataFrame) -> pd.Series:
        pc = g[c].shift(1)
        tr = pd.concat([(g[h]-g[l]).abs(),
                        (g[h]-pc).abs(),
                        (g[l]-pc).abs()], axis=1).max(axis=1)
        return tr

    # compute TR and ATR per symbol
    if add_tr:
        # ordered["tr"] = (
        #     ordered.groupby("symbol", sort=False)
        #         .apply(_true_range, include_groups=False)
        # )
        ordered["tr"] = (
            ordered.groupby("symbol", sort=False, group_keys=False)
                .apply(_true_range)
        )

    else:
        tr = ordered.groupby("symbol", sort=False, group_keys=False).apply(_true_range)
        ordered["_tr_tmp"] = tr  # ephemeral

    tr_col = "tr" if add_tr else "_tr_tmp"

    if wilder:
        ordered[f"{prefix}{n}"] = (
            ordered.groupby("symbol", sort=False)[tr_col]
                   .transform(lambda s: s.ewm(alpha=1.0/n, adjust=False, min_periods=n).mean())
        )
    else:
        ordered[f"{prefix}{n}"] = (
            ordered.groupby("symbol", sort=False)[tr_col]
                   .transform(lambda s: s.rolling(n, min_periods=n).mean())
        )

    if add_natr:
        ordered[f"n{prefix}{n}"] = ordered[f"{prefix}{n}"] / ordered[c]  # e.g., natr14

    if not add_tr:
        ordered.drop(columns=["_tr_tmp"], inplace=True, errors="ignore")

    out: Dict[str, pd.DataFrame] = {}
    for sym, g in ordered.groupby("symbol", sort=False):
        out[sym] = (g.drop(columns=["symbol"])
                      .set_index("datetime")
                      .sort_index())
    return out

def add_keltner_duckdb(
    data_by_sym: Dict[str, pd.DataFrame],
    con: duckdb.DuckDBPyConnection,
    *,
    n_basis: int = 20,
    n_atr: int = 20,
    k: float = 2.0,
    basis: str = "ema",           # "ema" or "sma"
    price_col: str = "close",
    atr_prefix: str = "atr",
) -> Dict[str, pd.DataFrame]:
    """
    Adds kc{n_basis}_basis, kc{n_basis}_u, kc{n_basis}_l using ATR(n_atr).
    Computes basis internally (EMA/SMA) per symbol.
    """
    if not data_by_sym:
        return data_by_sym
    all_bars = _stack_for_duck(data_by_sym, required=[price_col])
    view = "_bars_for_kc"
    con.register(view, all_bars)
    ordered = con.execute(f"SELECT * FROM {view} ORDER BY symbol, datetime").df()
    con.unregister(view)
    ordered["datetime"] = pd.to_datetime(ordered["datetime"])

    # Ensure ATR(n_atr) exists; compute on the fly if missing
    need_atr = f"{atr_prefix}{n_atr}"
    if need_atr not in ordered.columns:
        # inject high/low/close if available from source dict; _stack_for_duck usually includes all cols
        out_tmp = add_atr_duckdb({k:v for k,v in data_by_sym.items()}, con, n=n_atr, add_tr=False, add_natr=False, prefix=atr_prefix)
        # restack to ordered again to align (simplest path to keep pattern consistent)
        all_bars = _stack_for_duck(out_tmp, required=[price_col, need_atr])
        con.register(view, all_bars)
        ordered = con.execute(f"SELECT * FROM {view} ORDER BY symbol, datetime").df()
        con.unregister(view)
        ordered["datetime"] = pd.to_datetime(ordered["datetime"])

    # compute basis
    if basis == "ema":
        basis_ser = (ordered.groupby("symbol", sort=False)[price_col]
                             .transform(lambda s: s.ewm(span=n_basis, adjust=False, min_periods=n_basis).mean()))
    else:
        basis_ser = (ordered.groupby("symbol", sort=False)[price_col]
                             .transform(lambda s: s.rolling(n_basis, min_periods=n_basis).mean()))

    kc_b_name = f"kc{n_basis}_basis"
    ordered[kc_b_name] = basis_ser
    ordered[f"kc{n_basis}_u"] = ordered[kc_b_name] + k * ordered[need_atr]
    ordered[f"kc{n_basis}_l"] = ordered[kc_b_name] - k * ordered[need_atr]

    out: Dict[str, pd.DataFrame] = {}
    for sym, g in ordered.groupby("symbol", sort=False):
        out[sym] = (g.drop(columns=["symbol"])
                      .set_index("datetime")
                      .sort_index())
    return out

def add_hhll_duckdb(
    data_by_sym: Dict[str, pd.DataFrame],
    con: duckdb.DuckDBPyConnection,
    window: int = 20,
) -> Dict[str, pd.DataFrame]:
    """
    Adds hh_{window} (rolling max high) and ll_{window} (rolling min low).
    """
    all_bars = _stack_for_duck(data_by_sym, required=["high","low"])
    view = "_bars_for_hhll"
    con.register(view, all_bars)
    ordered = con.execute(f"SELECT * FROM {view} ORDER BY symbol, datetime").df()
    con.unregister(view)
    ordered["datetime"] = pd.to_datetime(ordered["datetime"])

    ordered[f"hh_{window}"] = (ordered.groupby("symbol", sort=False)["high"]
                                      .transform(lambda s: s.rolling(window, min_periods=window).max()))
    ordered[f"ll_{window}"] = (ordered.groupby("symbol", sort=False)["low"]
                                      .transform(lambda s: s.rolling(window, min_periods=window).min()))

    out = {}
    for sym, g in ordered.groupby("symbol", sort=False):
        out[sym] = (g.drop(columns=["symbol"])
                      .set_index("datetime")
                      .sort_index())
    return out

def add_atr_trailing_stops_duckdb(
    data_by_sym: Dict[str, pd.DataFrame],
    con: duckdb.DuckDBPyConnection,
    *,
    atr_col: str = "atr14",
    m_atr: float = 3.0,
    price_col: str = "close",
) -> Dict[str, pd.DataFrame]:
    """
    Adds ratcheting stops:
      atr_stop_long_{m} = cummax(close - m*ATR)
      atr_stop_short_{m} = cummin(close + m*ATR)
    per symbol, respecting chronological order.
    """
    needed = [price_col, atr_col]
    all_bars = _stack_for_duck(data_by_sym, required=needed)
    view = "_bars_for_atrstops"
    con.register(view, all_bars)
    ordered = con.execute(f"SELECT * FROM {view} ORDER BY symbol, datetime").df()
    con.unregister(view)
    ordered["datetime"] = pd.to_datetime(ordered["datetime"])

    base_long = ordered[price_col] - m_atr * ordered[atr_col]
    base_short = ordered[price_col] + m_atr * ordered[atr_col]

    # group-wise ratchets
    ordered[f"atr_stop_long_{m_atr:g}"] = (
        ordered.assign(_b=base_long)
               .groupby("symbol", sort=False)["_b"].cummax()
    )
    ordered[f"atr_stop_short_{m_atr:g}"] = (
        ordered.assign(_b=base_short)
               .groupby("symbol", sort=False)["_b"].cummin()
    )
    ordered.drop(columns=["_b"], inplace=True, errors="ignore")

    out = {}
    for sym, g in ordered.groupby("symbol", sort=False):
        out[sym] = (g.drop(columns=["symbol"])
                      .set_index("datetime")
                      .sort_index())
    return out

def add_choppiness_duckdb(
    data_by_sym: Dict[str, pd.DataFrame],
    con: duckdb.DuckDBPyConnection,
    n: int = 14,
) -> Dict[str, pd.DataFrame]:
    """
    Adds chop{n}. Computes TR internally if missing (not persisted).
    """
    all_bars = _stack_for_duck(data_by_sym, required=["high","low","close"])
    view = "_bars_for_chop"
    con.register(view, all_bars)
    ordered = con.execute(f"SELECT * FROM {view} ORDER BY symbol, datetime").df()
    con.unregister(view)
    ordered["datetime"] = pd.to_datetime(ordered["datetime"])

    # TR (temp)
    pc = ordered["close"].shift(1)
    tr = pd.concat([(ordered["high"]-ordered["low"]).abs(),
                    (ordered["high"]-pc).abs(),
                    (ordered["low"]-pc).abs()], axis=1).max(axis=1)

    tr_sum = (ordered.assign(_tr=tr)
                      .groupby("symbol", sort=False)["_tr"]
                      .transform(lambda s: s.rolling(n, min_periods=n).sum()))

    high_n = (ordered.groupby("symbol", sort=False)["high"]
                     .transform(lambda s: s.rolling(n, min_periods=n).max()))
    low_n  = (ordered.groupby("symbol", sort=False)["low"]
                     .transform(lambda s: s.rolling(n, min_periods=n).min()))
    rng = (high_n - low_n).replace(0, np.nan)

    ordered[f"chop{n}"] = 100 * np.log10(tr_sum / rng) / np.log10(n)

    out = {}
    for sym, g in ordered.groupby("symbol", sort=False):
        out[sym] = (g.drop(columns=["symbol"])
                      .set_index("datetime")
                      .sort_index())
    return out


def add_bollinger_bw_duckdb(
    data_by_sym: Dict[str, pd.DataFrame],
    con: duckdb.DuckDBPyConnection,
    n: int = 20,
    k: float = 2.0,
    *,
    price_col: str = "close",
) -> Dict[str, pd.DataFrame]:
    """
    Adds bb_bw{n} = (upper-lower)/MA where upper/lower are MA ± k*std.
    """
    all_bars = _stack_for_duck(data_by_sym, required=[price_col])
    view = "_bars_for_bbbw"
    con.register(view, all_bars)
    ordered = con.execute(f"SELECT * FROM {view} ORDER BY symbol, datetime").df()
    con.unregister(view)
    ordered["datetime"] = pd.to_datetime(ordered["datetime"])

    ma = (ordered.groupby("symbol", sort=False)[price_col]
                 .transform(lambda s: s.rolling(n, min_periods=n).mean()))
    sd = (ordered.groupby("symbol", sort=False)[price_col]
                 .transform(lambda s: s.rolling(n, min_periods=n).std(ddof=0)))
    upper, lower = ma + k*sd, ma - k*sd
    ordered[f"bb_bw{n}"] = (upper - lower) / ma

    out = {}
    for sym, g in ordered.groupby("symbol", sort=False):
        out[sym] = (g.drop(columns=["symbol"])
                      .set_index("datetime")
                      .sort_index())
    return out

def add_atr_feature_pack_duckdb(
    data_by_sym: Dict[str, pd.DataFrame],
    con: duckdb.DuckDBPyConnection,
    *,
    n_atr: int = 14,
    kc_basis: str = "ema",
    kc_n_basis: int = 20,
    kc_n_atr: int = 20,
    kc_k: float = 2.0,
    hhll_window: int = 20,
    m_atr_stop: float = 3.0,
    add_regime: bool = True,
    regime_fixed: Tuple[float,float] = (0.008, 0.02),  # 0.8% / 2.0%
) -> Dict[str, pd.DataFrame]:
    """
    Composes: TR/ATR/NATR, Keltner, HH/LL, ATR trailing stops, CHOP, BB bandwidth,
    and (optionally) a simple NATR-based regime label.
    """
    out = add_atr_duckdb(data_by_sym, con, n=n_atr, add_tr=True, add_natr=True)
    out = add_keltner_duckdb(out, con, n_basis=kc_n_basis, n_atr=kc_n_atr, k=kc_k, basis=kc_basis)
    out = add_hhll_duckdb(out, con, window=hhll_window)
    out = add_atr_trailing_stops_duckdb(out, con, atr_col=f"atr{n_atr}", m_atr=m_atr_stop)
    out = add_choppiness_duckdb(out, con, n=14)
    out = add_bollinger_bw_duckdb(out, con, n=20, k=2.0)

    if add_regime:
        # add 'atr_vol_regime' via fixed thresholds on natr
        packed = _stack_for_duck(out, required=[f"natr{n_atr}"])
        view = "_bars_for_regime"
        con.register(view, packed)
        ordered = con.execute(f"SELECT * FROM {view} ORDER BY symbol, datetime").df()
        con.unregister(view)
        lo, hi = regime_fixed
        x = ordered[f"natr{n_atr}"]
        ordered["atr_vol_regime"] = pd.Categorical(
            np.where(x < lo, "low", np.where(x < hi, "med", "high")),
            categories=["low","med","high"],
            ordered=True
        )
        # split back
        res = {}
        for sym, g in ordered.groupby("symbol", sort=False):
            # merge regime back into existing dict entry for that sym
            base = out[sym].copy()
            base = base.join(g.set_index("datetime")["atr_vol_regime"])
            res[sym] = base
        out = res

    return out

# ---------------------------------------------------------------------------
# ATR helpers
# ---------------------------------------------------------------------------

class RegimeFilterMixin:
    # params must be a tuple of (name, default)
    def regime_allows(self):
        lo = getattr(self.p, 'natr_min', 0.0)
        hi = getattr(self.p, 'natr_max', 10.0)
        try:
            natr = float(self.data.natr14[0])
        except Exception:
            return True
        return (natr >= lo) and (natr <= hi)

class ATRTrailingStopMixin:
    _stop_order = None

    def _stop_line_name(self):
        return getattr(self.p, 'stop_line', 'atr_stop_long_3')

    def update_trailing_stop(self):
        if not self.position:
            return
        stop_line = self._stop_line_name()
        if not hasattr(self.data, stop_line):
            return
        trail = float(getattr(self.data, stop_line)[0])
        if math.isnan(trail):
            return
        if self._stop_order:
            try:
                self.cancel(self._stop_order)
            except Exception:
                pass
            self._stop_order = None
        self._stop_order = self.sell(exectype=bt.Order.Stop, price=trail)

class ATRRiskSizer(bt.Sizer):
    params = (
        ('risk_cash', 200.0),
        ('m_atr', 3.0),
        ('min_shares', 1),
        ('max_shares', None),
        ('fallback_stake', 0),
    )

    def _getsizing(self, comminfo, cash, data, isbuy):
        if not isbuy:
            return self.broker.getposition(data).size
        try:
            atr = float(data.atr14[0])
            px  = float(data.close[0])
            risk_per_share = max(atr * self.p.m_atr, 1e-6)
            shares = int(cash // risk_per_share)
            shares = max(shares, self.p.min_shares)
            if self.p.max_shares: 
                shares = min(shares, self.p.max_shares)
            # ensure affordability
            if shares * px > cash:
                shares = int(cash // px)
            return max(shares, self.p.fallback_stake)
        except Exception:
            return self.p.fallback_stake
