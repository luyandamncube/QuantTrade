import backtrader as bt
from utils.bt_utils import  RegimeFilterMixin, ATRTrailingStopMixin
import math

class ATRPullback(RegimeFilterMixin, ATRTrailingStopMixin, bt.Strategy):
    params = (
        ('printlog', False),
        # mixin-consumed
        ('natr_min', 0.0),
        ('natr_max', 10.0),
        ('stop_line', 'atr_stop_long_3'),
        # strategy-specific
        ('base_ema', 20),
        ('trend_sma', 200),
        ('pullback_atr', 1.5),
        ('confirm_above_ema', True),
    )

    def __init__(self):
        self.order = None
        self.ema = bt.ind.EMA(self.data.close, period=self.p.base_ema)
        self.sma200 = bt.ind.SMA(self.data.close, period=self.p.trend_sma)
        self.recent_high = bt.ind.Highest(self.data.close, period=self.p.base_ema)
        self._atr_line = getattr(self.data, 'atr14', None)
        if self._atr_line is None:
            self._atr_line = bt.ind.ATR(self.data, period=14)

    def _pulled_back_enough(self) -> bool:
        if len(self.data) < self.p.base_ema:
            return False
        atr_v = float(self._atr_line[0])
        if math.isnan(atr_v) or atr_v <= 0:
            return False
        depth = float(self.recent_high[0] - self.data.low[0])
        return depth >= self.p.pullback_atr * atr_v

    def next(self):
        if self.order:
            return
        if not self.regime_allows():
            return

        close = float(self.data.close[0])
        in_uptrend = close > float(self.sma200[0])

        if not self.position:
            if in_uptrend and self._pulled_back_enough():
                if (not self.p.confirm_above_ema) or (close > float(self.ema[0])):
                    self.order = self.buy()
        else:
            if close < float(self.ema[0]):
                self.order = self.sell(size=self.position.size)
            else:
                self.update_trailing_stop()

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
            self.order = None
