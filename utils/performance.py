"""
QuantTrade.utils.performance
----------------------------
Reusable helpers for running Backtrader strategies on daily bars and
computing/ exporting performance metrics.

Usage (in a notebook):

    from QuantTrade.utils.performance import (
        run_backtest_daily,
        summarize_performance,
        export_performance_csvs,
        equity_curves_vs_bh,
    )

    res = run_backtest_daily(
        df=daily_data['SPY'],
        strategy_cls=EMACrossStrategy,
        strategy_params=dict(fast=12, slow=26, use_trend_filter=True, stake_pct=0.99),
        start_cash=10_000,
        commission_bps=0.5,   # 0.5 bps
        datafeed_cls=PandasDataExt,  # your extended feed with ema/ma fields
        symbol='SPY',
    )

    perf = summarize_performance(res)
    perf['summary']        # dict of headline metrics
    perf['equity_strat']   # pd.Series equity curve
    perf['equity_bh']      # pd.Series buy&hold curve
    perf['daily_returns']  # pd.Series daily returns
    perf['trade_stats']    # dict from TradeAnalyzer

    # Optional exports
    export_performance_csvs('SPY', perf)

    # Plot (matplotlib)
    ax = equity_curves_vs_bh(perf)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Type

import backtrader as bt
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import plotly.graph_objects as go
from plotly.subplots import make_subplots


# ---------------------------------------------------------------------------
# Backtest runner
# ---------------------------------------------------------------------------

@dataclass
class BacktestResult:
    symbol: str
    start_cash: float
    cerebro: bt.Cerebro
    strategy: bt.Strategy
    analyzers: Dict[str, Any]


def run_backtest_daily(
    *,
    df: pd.DataFrame,
    strategy_cls: Type[bt.Strategy],
    strategy_params: Optional[Dict[str, Any]] = None,
    start_cash: float = 10_000.0,
    commission_bps: float = 0.0,
    datafeed_cls: Type[bt.feeds.PandasData] = bt.feeds.PandasData,
    symbol: str = "SYMBOL",
) -> BacktestResult:
    """Run a Backtrader backtest on **daily** bars and attach common analyzers.

    Parameters
    ----------
    df : DataFrame
        Must contain at least columns: open, high, low, close, volume.
        Index must be a DatetimeIndex (tz-naive).

    strategy_cls : bt.Strategy
        Your strategy class (e.g., EMACrossStrategy).

    strategy_params : dict
        Parameters passed to the strategy constructor.

    start_cash : float
        Initial broker cash.

    commission_bps : float
        Commission in basis points (e.g., 0.5 means 0.5 bps = 0.00005).

    datafeed_cls : bt.feeds.PandasData (or subclass)
        Data feed class to map DataFrame -> Backtrader data (use your PandasDataExt if you
        have extra lines like ema12/ema26/ma200).

    symbol : str
        Symbol used for labeling.

    Returns
    -------
    BacktestResult
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("df.index must be a DatetimeIndex")
    # Ensure tz-naive for Backtrader
    if getattr(df.index, "tz", None) is not None:
        df = df.tz_localize(None)

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(float(start_cash))
    if commission_bps:
        cerebro.broker.setcommission(commission=float(commission_bps) / 10_000.0)

    # Data feed
    feed = datafeed_cls(dataname=df)
    cerebro.adddata(feed, name=symbol)

    # Strategy
    cerebro.addstrategy(strategy_cls, **(strategy_params or {}))

    # Analyzers
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="timereturn", timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days, riskfreerate=0.0)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="dd")
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name="annual")

    # extract executions
    cerebro.addanalyzer(bt.analyzers.Transactions, _name='tx')

    res = cerebro.run()
    strat = res[0]
    analyzers = {
        "timereturn": strat.analyzers.timereturn,
        "sharpe": strat.analyzers.sharpe,
        "dd": strat.analyzers.dd,
        "trades": strat.analyzers.trades,
        "annual": strat.analyzers.annual,
        "tx":         strat.analyzers.tx,
    }
    return BacktestResult(symbol=symbol, start_cash=start_cash, cerebro=cerebro, strategy=strat, analyzers=analyzers)


# ---------------------------------------------------------------------------
# Performance metrics
# ---------------------------------------------------------------------------

def _equity_from_timereturn(timereturn_an: bt.Analyzer) -> Tuple[pd.Series, pd.Series]:
    """Analyzer -> (daily_returns, equity_curve)"""
    rets = pd.Series(timereturn_an.get_analysis())
    rets.index = pd.to_datetime(rets.index)
    rets = rets.sort_index()
    equity = (1 + rets).cumprod()
    return rets, equity


def _buyhold_equity(df_close: pd.Series, start_cash: float) -> pd.Series:
    df_close = df_close.dropna()
    start = df_close.iloc[0]
    units = start_cash / float(start)
    eq = units * df_close
    eq.name = "Buy&Hold"
    return eq


def cagr(equity: pd.Series, periods_per_year: int = 252) -> float:
    if equity.empty:
        return float("nan")
    start_val, end_val = float(equity.iloc[0]), float(equity.iloc[-1])
    n_periods = len(equity) - 1
    years = n_periods / periods_per_year if periods_per_year else max(1e-9, (equity.index[-1] - equity.index[0]).days / 365.25)
    if years <= 0:
        return float("nan")
    return (end_val / start_val) ** (1 / years) - 1.0


def sharpe(rets: pd.Series, rf: float = 0.0, periods_per_year: int = 252) -> float:
    if len(rets) < 2:
        return float("nan")
    er = rets.mean() - (rf / periods_per_year)
    vol = rets.std(ddof=0)
    return float("nan") if vol == 0 else float(er / vol * math.sqrt(periods_per_year))


def sortino(rets: pd.Series, rf: float = 0.0, periods_per_year: int = 252) -> float:
    dn = rets[rets < 0]
    if len(dn) == 0:
        return float("nan")
    dr = dn.std(ddof=0)
    er = rets.mean() - (rf / periods_per_year)
    return float("nan") if dr == 0 else float(er / dr * math.sqrt(periods_per_year))


def max_drawdown_stats(equity: pd.Series) -> Tuple[float, Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    if equity.empty:
        return float("nan"), None, None
    roll_max = equity.cummax()
    dd = equity / roll_max - 1.0
    mdd = float(dd.min())
    end = dd.idxmin()
    start = equity.loc[:end].idxmax()
    return mdd, start, end


def summarize_performance(btres: BacktestResult) -> Dict[str, Any]:
    """Produce headline metrics + curves from BacktestResult.

    Returns dict with keys:
      summary: dict of headline numbers
      equity_strat: Series (currency, based on start_cash)
      equity_bh:    Series (currency)
      daily_returns: Series (strategy daily return)
      trade_stats:   dict from TradeAnalyzer
      annual_returns: Series of calendar-year returns (from analyzer)
    """
    cerebro = btres.cerebro
    strat = btres.strategy
    start_cash = btres.start_cash
    symbol = btres.symbol

    df_feed = strat.datas[0]  # Backtrader data wrapper
    # Build close series aligned to TimeReturn output
    rets_series, eq_rel = _equity_from_timereturn(btres.analyzers["timereturn"])
    eq_strat = eq_rel * float(start_cash)
    eq_strat.name = "Strategy"

    # rebuild close prices for buy&hold on same index
    # We try to fetch from the original pandas, else fallback reading from data lines
    try:
        closes = df_feed.p.dataname["close"].copy()  # type: ignore[attr-defined]
        closes = closes.reindex(rets_series.index).ffill()
    except Exception:
        # Fallback: iterate over data
        dates = []
        vals = []
        for i in range(len(df_feed)):
            dt = bt.num2date(df_feed.datetime[i])
            dates.append(pd.Timestamp(dt))
            vals.append(float(df_feed.close[i]))
        closes = pd.Series(vals, index=pd.to_datetime(dates)).reindex(rets_series.index).ffill()

    eq_bh = _buyhold_equity(closes, start_cash=start_cash)
    eq_bh = eq_bh.reindex(eq_strat.index).ffill()

    # daily returns from equity (aligned)
    daily_rets = eq_strat.pct_change().fillna(0.0)

    # metrics
    mdd, mdd_start, mdd_end = max_drawdown_stats(eq_strat)
    summary = {
        "Symbol": symbol,
        "Start": eq_strat.index[0].date().isoformat(),
        "End": eq_strat.index[-1].date().isoformat(),
        "Start Value": float(start_cash),
        "End Value": float(eq_strat.iloc[-1]),
        "Total Return %": 100.0 * (float(eq_strat.iloc[-1]) / float(start_cash) - 1.0),
        "CAGR %": 100.0 * cagr(eq_strat / float(start_cash)),
        "Sharpe": sharpe(daily_rets),
        "Sortino": sortino(daily_rets),
        "MaxDD %": 100.0 * mdd,
        "MaxDD start": mdd_start,
        "MaxDD end": mdd_end,
    }

    # Backtrader's own Sharpe / DD (for cross-check)
    try:
        summary["Sharpe (BT)"] = btres.analyzers["sharpe"].get_analysis().get("sharperatio", float("nan"))
    except Exception:
        summary["Sharpe (BT)"] = float("nan")
    try:
        dd_an = btres.analyzers["dd"].get_analysis()
        summary["MaxDD % (BT)"] = dd_an.get("maxdrawdown", float("nan"))
        summary["MaxDD Money (BT)"] = dd_an.get("maxmoneydown", float("nan"))
    except Exception:
        pass

    # Trade analyzer
    trade_stats = {}
    try:
        a = btres.analyzers["trades"].get_analysis()
        total = a.get("total", {})
        won   = a.get("won", {})
        lost  = a.get("lost", {})
        pnls  = a.get("pnl", {})

        trade_stats = {
            "Total trades": total.get("total", 0),
            "Won": won.get("total", 0),
            "Lost": lost.get("total", 0),
            "Win rate %": 100.0 * (won.get("total", 0) / max(1, total.get("total", 1))),
            "Gross PnL": pnls.get("gross", float("nan")),
            "Net PnL": pnls.get("net", float("nan")),
            "Longest win streak": won.get("streak", {}).get("longest", float("nan")),
            "Longest loss streak": lost.get("streak", {}).get("longest", float("nan")),
        }
    except Exception:
        pass

    # Annual returns (calendar-year) from analyzer
    try:
        annual = btres.analyzers["annual"].get_analysis()
        annual_returns = pd.Series(annual, name="AnnualReturn").sort_index()
    except Exception:
        annual_returns = pd.Series(dtype=float)

    return dict(
        summary=summary,
        equity_strat=eq_strat,
        equity_bh=eq_bh,
        daily_returns=daily_rets,
        trade_stats=trade_stats,
        annual_returns=annual_returns,
    )


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

def export_performance_csvs(tag: str, perf: Dict[str, Any], *, directory: str | None = None) -> Dict[str, str]:
    """Write CSVs for later analysis. Returns dict of file paths."""
    directory = directory or "."
    paths = {}

    # Summary
    df_summary = pd.DataFrame([perf["summary"]])
    p_sum = f"{directory}/{tag}_summary.csv"
    df_summary.to_csv(p_sum, index=False)
    paths["summary"] = p_sum

    # Trade stats
    df_tr = pd.DataFrame([perf.get("trade_stats", {})])
    p_tr = f"{directory}/{tag}_trade_stats.csv"
    df_tr.to_csv(p_tr, index=False)
    paths["trade_stats"] = p_tr

    # Equity curves
    curves = pd.concat([
        perf["equity_strat"].rename("strategy"),
        perf["equity_bh"].rename("buyhold"),
    ], axis=1)
    p_eq = f"{directory}/{tag}_equity_curves.csv"
    curves.to_csv(p_eq)
    paths["equity_curves"] = p_eq

    # Daily returns
    p_rets = f"{directory}/{tag}_daily_returns.csv"
    perf["daily_returns"].rename("return").to_csv(p_rets)
    paths["daily_returns"] = p_rets

    # Annual returns
    if isinstance(perf.get("annual_returns"), pd.Series) and len(perf["annual_returns"]):
        p_ann = f"{directory}/{tag}_annual_returns.csv"
        perf["annual_returns"].to_csv(p_ann)
        paths["annual_returns"] = p_ann

    return paths


# ---------------------------------------------------------------------------
# Quick plot
# ---------------------------------------------------------------------------

# def equity_curves_vs_bh(perf: Dict[str, Any], *, figsize=(10, 5)):
#     """Plot Strategy vs Buy&Hold equity (normalized to start). Returns Matplotlib Axes."""
#     eq_s = perf["equity_strat"]
#     eq_b = perf["equity_bh"]
#     start_val = float(eq_s.iloc[0])
#     fig, ax = plt.subplots(figsize=figsize)
#     (eq_s / start_val).plot(ax=ax, lw=2, label="Strategy")
#     (eq_b / start_val).plot(ax=ax, lw=2, label="Buy & Hold", alpha=0.85)
#     ax.set_title("Equity Curve (normalized)")
#     ax.set_ylabel("Multiple of Start")
#     ax.grid(True, alpha=0.25)
#     ax.legend()
#     return ax


def plot_equity_curves_plotly(perf: dict, symbol: str = "", dark: bool = True):
    """Interactive equity curve comparison (Strategy vs Buy & Hold)."""

    eq_s = perf["equity_strat"]
    eq_b = perf["equity_bh"]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=eq_s.index, y=eq_s / eq_s.iloc[0],
        mode="lines", name="Strategy", line=dict(width=2, color="#1f77b4")
    ))

    fig.add_trace(go.Scatter(
        x=eq_b.index, y=eq_b / eq_b.iloc[0],
        mode="lines", name="Buy & Hold", line=dict(width=2, color="#ff7f0e")
    ))

    fig.update_layout(
        template="plotly_dark" if dark else "plotly_white",
        title=f"Equity Curve Comparison {symbol}",
        yaxis_title="Multiple of Start",
        hovermode="x unified",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
    )
    return fig


def plot_returns_histogram(perf: dict, dark: bool = True):
    """Histogram of daily returns."""
    rets = perf["daily_returns"].dropna()

    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=rets * 100, nbinsx=50, name="Daily Returns",
        marker_color="#1f77b4", opacity=0.75
    ))

    fig.update_layout(
        template="plotly_dark" if dark else "plotly_white",
        title="Distribution of Daily Returns",
        xaxis_title="Return (%)",
        yaxis_title="Frequency",
        bargap=0.05
    )
    return fig


def plot_drawdown_curve(perf: dict, dark: bool = True):
    """Cumulative drawdown curve."""
    eq = perf["equity_strat"]
    dd = eq / eq.cummax() - 1

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dd.index, y=dd * 100,
        mode="lines", name="Drawdown (%)", line=dict(width=1.8, color="#d62728")
    ))

    fig.update_layout(
        template="plotly_dark" if dark else "plotly_white",
        title="Drawdown Curve",
        yaxis_title="Drawdown (%)",
        hovermode="x unified"
    )
    return fig


# ---------------------------------------------------------------------------
# Markers (BUY/SELL)
# ---------------------------------------------------------------------------

def extract_exec_df_from_strategy(strat) -> pd.DataFrame:
    """Return exec log DataFrame if strategy implements ExecLoggerMixin."""
    data = getattr(strat, 'exec_log', None)
    if not data:
        return pd.DataFrame(columns=['dt','side','price','size','value','commission'])
    df = pd.DataFrame(data).sort_values('dt')
    return df

def transactions_to_df(tx_analyzer) -> pd.DataFrame:
    """
    Convert Backtrader Transactions analyzer to a tidy DataFrame.
    Robust to tuple length/shape variations across BT versions.
    Output columns: dt, side, size, price, value, commission
    """
    tx = tx_analyzer.get_analysis()
    rows = []

    for dt, items in tx.items():
        if not items:
            continue
        for it in items:
            size = 0.0
            price = float("nan")
            value = float("nan")
            comm = 0.0

            if isinstance(it, dict):
                size  = float(it.get("size", 0.0))
                price = float(it.get("price", float("nan")))
                if "value" in it and it["value"] is not None:
                    value = float(it["value"])
                elif price == price:
                    value = size * price
                comm  = float(it.get("commission", 0.0))
            else:
                t = list(it)
                # common minimal
                if len(t) >= 1: size  = float(t[0])
                if len(t) >= 2: price = float(t[1])

                # your 5-tuple: [size, price, sid, symbol, value]
                if len(t) == 5 and isinstance(t[3], str):
                    try:
                        value = float(t[4])
                    except Exception:
                        value = size * price if price == price else float("nan")
                    comm = 0.0

                # len=4: often value at idx 3
                elif len(t) >= 4 and t[3] is not None and not isinstance(t[3], (list, tuple, dict, str)):
                    try:
                        value = float(t[3])
                    except Exception:
                        value = size * price if price == price else float("nan")

                # len=6: [size, price, sid, value, pnl, commission]
                if len(t) >= 6 and t[5] is not None:
                    try:
                        comm = float(t[5])
                    except Exception:
                        comm = 0.0
                # some 5-tuples use index 4 for commission (rare)
                elif len(t) == 5 and not isinstance(t[4], str):
                    try:
                        # if not already set by symbol/value branch
                        if not (len(t) == 5 and isinstance(t[3], str)):
                            comm = float(t[4])
                    except Exception:
                        pass

                # fallbacks
                if value != value and price == price:
                    value = size * price

            side = "BUY" if size > 0 else "SELL"
            rows.append({
                "dt": pd.Timestamp(dt).tz_localize(None),
                "side": side,
                "size": abs(size),
                "price": price,
                "value": value,
                "commission": comm,
            })

    cols = ["dt","side","size","price","value","commission"]
    return (pd.DataFrame(rows, columns=cols)
              .sort_values("dt")
              .reset_index(drop=True))

def execs_to_lw_markers(exec_df: pd.DataFrame):
    """DataFrame -> Lightweight-Charts markers for price pane."""
    markers = []
    if exec_df is None or exec_df.empty:
        return markers
    for r in exec_df.itertuples():
        try:
            ts = int(pd.Timestamp(r.dt).tz_localize('UTC').timestamp())
        except Exception:
            continue
        is_buy = (r.side == 'BUY')
        markers.append({
            'time': ts,
            'position': 'belowBar' if is_buy else 'aboveBar',
            'shape': 'arrowUp' if is_buy else 'arrowDown',
            'color': '#26a69a' if is_buy else '#ef5350',
            'text': f"{r.side} {getattr(r, 'size', 0):g} @ {getattr(r, 'price', float('nan')):.2f}",
        })
    return markers
