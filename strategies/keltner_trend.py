import backtrader as bt
from utils.bt_utils import  RegimeFilterMixin, ATRTrailingStopMixin

class KeltnerTrend(RegimeFilterMixin, ATRTrailingStopMixin, bt.Strategy):
    params = (
        ('printlog', False),
        # mixin-consumed
        ('natr_min', 0.0),
        ('natr_max', 10.0),
        ('stop_line', 'atr_stop_long_3'),
        # strategy-specific
        ('kc_u', 'kc20_u'),
        ('kc_l', 'kc20_l'),
        ('basis', 'kc20_basis'),
        ('trend_ema_fast', 20),
        ('trend_ema_slow', 50),
        ('use_trend_filter', True),
    )

    def __init__(self):
        self.order = None
        self.ema_fast = bt.ind.EMA(self.data.close, period=self.p.trend_ema_fast)
        self.ema_slow = bt.ind.EMA(self.data.close, period=self.p.trend_ema_slow)

    def next(self):
        if self.order:
            return
        if not self.regime_allows():
            return

        close = float(self.data.close[0])
        kc_u = float(getattr(self.data, self.p.kc_u)[0])
        kc_l = float(getattr(self.data, self.p.kc_l)[0])

        trend_ok = True
        if self.p.use_trend_filter:
            trend_ok = self.ema_fast[0] > self.ema_slow[0]

        if not self.position:
            if trend_ok and close > kc_u:
                self.order = self.buy()
        else:
            if close < kc_l:
                self.order = self.sell(size=self.position.size)
            else:
                self.update_trailing_stop()

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
            self.order = None