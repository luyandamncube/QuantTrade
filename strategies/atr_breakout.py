import backtrader as bt
import math
from utils.bt_utils import  RegimeFilterMixin, ATRTrailingStopMixin

class ATRBreakout(RegimeFilterMixin, ATRTrailingStopMixin, bt.Strategy):
    params = (
        ('printlog', False),
        # mixin-consumed
        ('natr_min', 0.0),
        ('natr_max', 10.0),
        ('stop_line', 'atr_stop_long_3'),
        # strategy-specific
        ('hh_line', 'hh_20'),
        ('ll_line', 'll_10'),
        ('m_atr', 3.0),
    )

    def __init__(self):
        self.order = None

    def next(self):
        if self.order:
            return
        if not self.regime_allows():
            return

        close = float(self.data.close[0])
        hh = float(getattr(self.data, self.p.hh_line)[0]) if hasattr(self.data, self.p.hh_line) else float('nan')
        ll = float(getattr(self.data, self.p.ll_line)[0]) if hasattr(self.data, self.p.ll_line) else float('nan')

        if not self.position:
            if not math.isnan(hh) and close > hh:
                self.order = self.buy()
        else:
            if not math.isnan(ll) and close < ll:
                self.order = self.sell(size=self.position.size)
            else:
                self.update_trailing_stop()

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
            self.order = None