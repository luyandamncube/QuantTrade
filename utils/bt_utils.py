import backtrader as bt
import math

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