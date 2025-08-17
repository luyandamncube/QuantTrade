"""
Reusable MACD Strategies
========================

This module provides three Backtrader strategies built around MACD:

1) MACDSignalCrossStrategy
   - BUY  when MACD crosses *above* Signal
   - SELL when MACD crosses *below* Signal
   - Optional trend filter: close > SMA(trend_ma)

2) MACDZeroLineStrategy
   - BUY  when MACD crosses *above* 0
   - SELL when MACD crosses *below* 0
   - Optional trend filter: close > SMA(trend_ma)

3) MACDHistogramMomentumStrategy
   - BUY  when MACD Histogram turns positive (and rising)
   - SELL when Histogram turns negative
   - Optional trend filter: close > SMA(trend_ma)

Common parameters:
- fast, slow, signal: MACD settings (default 12/26/9)
- use_trend_filter: gate longs on price > SMA(trend_ma)
- trend_ma: SMA window used for trend filter (default 200)
- stake_pct: use this fraction of *portfolio value* for entries
- printlog: if True, logs entries/exits and stop() value
"""

from __future__ import annotations
import backtrader as bt
import pandas as pd


class ExecLoggerMixin:
    """Records executions to `self.exec_log` inside `notify_order`."""
    def __init__(self):
        # list of dicts: dt, side, price, size, value, commission
        self.exec_log = []
        super().__init__()

    def notify_order(self, order):
        if order.status != order.Completed:
            return
        side = 'BUY' if order.isbuy() else 'SELL'
        try:
            dt = bt.num2date(order.executed.dt)
        except Exception:
            dt = self.datas[0].datetime.datetime(0)

        self.exec_log.append({
            'dt': pd.Timestamp(dt).tz_localize(None),
            'side': side,
            'price': float(order.executed.price),
            'size':  float(order.executed.size),
            'value': float(getattr(order.executed, 'value',
                                   order.executed.price * order.executed.size)),
            'commission': float(order.executed.comm),
        })


class _MACDBase(ExecLoggerMixin, bt.Strategy):
    """
    Base class providing MACD lines, trend filter and logging utilities.
    Subclasses should implement `entry_cond()` and `exit_cond()`.
    """
    params = dict(
        fast=12,
        slow=26,
        signal=9,
        use_trend_filter=True,
        trend_ma=200,
        stake_pct=0.99,
        printlog=False,
    )

    def log(self, txt):
        if self.p.printlog:
            dt = self.datas[0].datetime.datetime(0)
            print(f'{dt:%Y-%m-%d} | {txt}')

    def __init__(self):
        ExecLoggerMixin.__init__(self)

        d = self.datas[0]
        self.close = d.close

        # MACD with histogram
        self.macd = bt.ind.MACD(
            d.close,
            period_me1=self.p.fast,
            period_me2=self.p.slow,
            period_signal=self.p.signal
        )
        self.macd_line = self.macd.macd
        self.signal_line = self.macd.signal
        # self.hist = self.macd.histo
        try:
            self.hist = self.macd.histo
        except AttributeError:
            self.hist = self.macd_line - self.signal_line

        # Optional trend filter (SMA)
        self.trend_ma = bt.ind.SMA(d.close, period=self.p.trend_ma)

        # Reusable crossover helper (for signal-strategy)
        self._cross_macd_signal = bt.ind.CrossOver(self.macd_line, self.signal_line)

    # --- hooks for subclasses ---
    def entry_cond(self) -> bool:
        raise NotImplementedError

    def exit_cond(self) -> bool:
        raise NotImplementedError

    # --- utilities ---
    def _long_ok(self) -> bool:
        if not self.p.use_trend_filter:
            return True
        return self.close[0] > self.trend_ma[0]

    def _buy_size(self) -> int:
        value = self.broker.getvalue()
        stake_cash = value * self.p.stake_pct
        size = int(stake_cash / max(self.close[0], 1e-12))
        return max(size, 0)

    def next(self):
        if not self.position:
            if self.entry_cond() and self._long_ok():
                size = self._buy_size()
                if size > 0:
                    self.buy(size=size)
                    self.log(f'BUY  size={size} @ {self.close[0]:.2f}')
        else:
            if self.exit_cond():
                self.sell(size=self.position.size)
                self.log(f'SELL size={self.position.size} @ {self.close[0]:.2f}')

    def stop(self):
        if self.p.printlog:
            self.log(f'Final Value: {self.broker.getvalue():.2f}')


class MACDSignalCrossStrategy(_MACDBase):
    """
    Enter long when MACD crosses above Signal; exit when MACD crosses below Signal.
    """
    def entry_cond(self) -> bool:
        # CrossOver returns +1 on a cross up, -1 on cross down, 0 otherwise
        return self._cross_macd_signal[0] > 0

    def exit_cond(self) -> bool:
        return self._cross_macd_signal[0] < 0


class MACDZeroLineStrategy(_MACDBase):
    """
    Enter long when MACD crosses above zero; exit when MACD crosses below zero.
    """
    def entry_cond(self) -> bool:
        # emulate cross above 0: prev <= 0 and now > 0
        return (self.macd_line[-1] <= 0.0) and (self.macd_line[0] > 0.0)

    def exit_cond(self) -> bool:
        # emulate cross below 0: prev >= 0 and now < 0
        return (self.macd_line[-1] >= 0.0) and (self.macd_line[0] < 0.0)


class MACDHistogramMomentumStrategy(_MACDBase):
    """
    Enter when histogram turns positive *and* is rising; exit when histogram turns negative.
    """
    def entry_cond(self) -> bool:
        # Turned positive this bar AND rising vs previous
        return (self.hist[-1] <= 0.0) and (self.hist[0] > 0.0) and (self.hist[0] > self.hist[-1])

    def exit_cond(self) -> bool:
        # Turned negative this bar
        return (self.hist[-1] >= 0.0) and (self.hist[0] < 0.0)
