"""
QuantTrade.strategies
---------------------
Collection of reusable Backtrader strategies.
"""
from .ema_cross import EMACrossStrategy
from .macd import MACDSignalCrossStrategy, MACDZeroLineStrategy, MACDHistogramMomentumStrategy

__all__ = [
    "EMACrossStrategy",
    "MACDSignalCrossStrategy",
    "MACDZeroLineStrategy",
    "MACDHistogramMomentumStrategy"
]
