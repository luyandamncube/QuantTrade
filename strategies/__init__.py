"""
QuantTrade.strategies
---------------------
Collection of reusable Backtrader strategies.
"""
from .ema_cross import EMACrossStrategy
from .macd import MACDSignalCrossStrategy, MACDZeroLineStrategy, MACDHistogramMomentumStrategy
from .atr_breakout import ATRBreakout
from .atr_pullback import ATRPullback
from .keltner_trend import KeltnerTrend

__all__ = [
    "EMACrossStrategy",
    "MACDSignalCrossStrategy",
    "MACDZeroLineStrategy",
    "MACDHistogramMomentumStrategy",
    "ATRBreakout",
    "ATRPullback",
    "KeltnerTrend"
]
