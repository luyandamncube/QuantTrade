"""
EMA Crossover Strategy (reusable)
=================================

Signals:
- BUY  when EMA(fast) crosses *above* EMA(slow)
- SELL when EMA(fast) crosses *below* EMA(slow)

Optional filter:
- `use_trend_filter=True` enforces `close > SMA(200)` for longs.

Position sizing:
- `stake_pct` fraction of available cash is used for each entry.

This strategy also logs filled orders into `self.exec_log` so you can
build chart markers without attaching extra analyzers.
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


class EMACrossStrategy(ExecLoggerMixin, bt.Strategy):
    params = dict(
        fast=12,
        slow=26,
        use_trend_filter=True,
        trend_ma=200,       # for SMA trend filter
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

        # Indicators (use data's close by default)
        self.ema_fast = bt.ind.EMA(d.close, period=self.p.fast)
        self.ema_slow = bt.ind.EMA(d.close, period=self.p.slow)
        self.crossover = bt.ind.CrossOver(self.ema_fast, self.ema_slow)

        # Trend filter
        self.trend_ma = bt.ind.SMA(d.close, period=self.p.trend_ma)

    def next(self):
        cash = self.broker.getcash()
        value = self.broker.getvalue()

        # Calculate target stake size (integer shares/units)
        if not self.position:
            # Entry condition
            long_ok = True
            if self.p.use_trend_filter:
                long_ok = self.close[0] > self.trend_ma[0]

            if self.crossover[0] > 0 and long_ok:
                stake_cash = value * self.p.stake_pct
                size = int(stake_cash / self.close[0])
                if size > 0:
                    self.buy(size=size)
                    self.log(f'BUY  size={size} @ {self.close[0]:.2f}')
        else:
            # Exit condition
            if self.crossover[0] < 0:
                self.sell(size=self.position.size)
                self.log(f'SELL size={self.position.size} @ {self.close[0]:.2f}')

    def stop(self):
        if self.p.printlog:
            self.log(f'Final Value: {self.broker.getvalue():.2f}')
