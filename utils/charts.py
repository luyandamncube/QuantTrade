from __future__ import annotations
import json
import pandas as pd
from pathlib import Path
import json

# def render_lightweight_chart(
#     df: pd.DataFrame,
#     *,
#     symbol: str = "SYMBOL",
#     out_html: str | Path = "QuantTrade/charts/output/chart.html",
#     theme: str = "dark",
#     height: int = 700,
#     title: str | None = None,
#     ma_windows: list[int] | None = None,
#     ema_windows: list[int] | None = None,
#     rsi_period: int | None = None,
#     rsi_bounds: tuple[int, int] | None = None,
#     timeframes: list[str] = ("1m","5m","15m","1h","1d"),
#     default_tf: str = "1m",
#     digits: int = 2,
#     watermark_text: str | None = None,
#     watermark_opacity: float = 0.08,
#     assets_rel: str = "../static",
#     markers: list[dict] | None = None
# ) -> Path:
#     """
#     Write an HTML shell that loads static/lw_chart.css and static/lw_chart.js.
#     Performs error checks for asset presence and initLightweightChart() function.
#     """
#     # --- Check static assets ---
#     out_html = Path(out_html)
#     static_dir = (out_html.parent / assets_rel).resolve()

#     if not static_dir.exists():
#         raise FileNotFoundError(f"Static assets directory not found: {static_dir}")

#     css_file = static_dir / "lw_chart.css"
#     js_file = static_dir / "lw_chart.js"

#     if not css_file.exists():
#         raise FileNotFoundError(f"CSS file not found: {css_file}")
#     if not js_file.exists():
#         raise FileNotFoundError(f"JavaScript file not found: {js_file}")

#     # Check for initLightweightChart function in JS
#     js_text = js_file.read_text(encoding="utf-8", errors="ignore")
#     if "function initLightweightChart" not in js_text:
#         raise RuntimeError(
#             f"'initLightweightChart' function not found in {js_file}.\n"
#             "Make sure you copied the correct lw_chart.js file."
#         )

#     # --- DataFrame validation ---
#     req = {"open","high","low","close","volume"}
#     missing = req - set(df.columns)
#     if missing:
#         raise KeyError(f"Missing required columns: {sorted(missing)}")
#     tmp = df.copy()
#     idx = pd.to_datetime(tmp.index)
#     tmp.index = (idx.tz_convert("UTC") if idx.tz is not None else idx.tz_localize("UTC"))
#     tmp = tmp.dropna(subset=["open","high","low","close"])
#     if tmp.empty:
#         raise ValueError("No rows with complete OHLC to plot.")

#     def to_ts(x): return int(pd.Timestamp(x).timestamp())

#     candles = [
#         {"time": to_ts(t), "open": float(r.open), "high": float(r.high),
#          "low": float(r.low), "close": float(r.close)}
#         for t, r in tmp[["open","high","low","close"]].iterrows()
#     ]
#     volumes = []
#     for t, r in tmp[["open","close","volume"]].iterrows():
#         up = (r.close >= r.open)
#         volumes.append({"time": to_ts(t), "value": float(r.volume or 0.0),
#                         "color": "#26a69a" if up else "#ef5350"})

#     ma_windows = ma_windows or []
#     ema_windows = ema_windows or []

#     ma_palette  = ["#ff9800","#42a5f5","#ab47bc","#26a69a","#ec407a",
#                    "#8d6e63","#66bb6a","#ffa726","#29b6f6","#ef5350"]
#     ema_palette = ["#fdd835","#1de9b6","#b388ff","#ff7043","#7e57c2",
#                    "#4db6ac","#ba68c8","#9ccc65","#ef5350","#42a5f5"]
#     ma_colors  = {f"ma{w}": ma_palette[i % len(ma_palette)]   for i, w in enumerate(ma_windows)}
#     ema_colors = {f"ema{w}": ema_palette[i % len(ema_palette)] for i, w in enumerate(ema_windows)}

#     title = title or f"{symbol} • Lightweight Charts"
#     config = {
#         "candles": candles,
#         "volumes": volumes,
#         "ma_windows": ma_windows,
#         "ema_windows": ema_windows,
#         "ma_colors": ma_colors,
#         "ema_colors": ema_colors,
#         "rsi_period": rsi_period,
#         "rsi_bounds": rsi_bounds,
#         "timeframes": list(timeframes),
#         "default_tf": default_tf,
#         "digits": digits,
#         "symbol": symbol,
#         "watermark_text": watermark_text or "",
#         "watermark_opacity": watermark_opacity,
#         "theme": theme,
#         "height": height,
#         "title": title,
#         "markers": markers or []
#     }

#     css_href = f"{assets_rel}/lw_chart.css"
#     js_src   = f"{assets_rel}/lw_chart.js"

#     html = f"""<!doctype html>
# <html>
# <head>
#   <meta charset="utf-8"/>
#   <title>{title}</title>
#   <link rel="stylesheet" href="{css_href}">
#   <script src="https://unpkg.com/lightweight-charts@4.2.1/dist/lightweight-charts.standalone.production.js"></script>
#   <script src="{js_src}"></script>
# </head>
# <body>
# <div id="wrap">
#   <div id="toolbar"></div>
#   <div id="chart-price"></div>
#   <div id="chart-vol"></div>     <!-- NEW: volume pane -->
#   <div id="chart-rsi"></div>
#   <div id="legend" class="legend"></div>
#   <div id="wm" class="watermark" style="display:none;"></div>
# </div>
#   <script>
#     window.__LW_CHART_CONFIG__ = {json.dumps(config)};
#     initLightweightChart(window.__LW_CHART_CONFIG__);
#   </script>
# </body>
# </html>"""

#     out_html.parent.mkdir(parents=True, exist_ok=True)
#     out_html.write_text(html, encoding="utf-8")
#     return out_html

def render_lightweight_chart(
    df: pd.DataFrame,
    *,
    symbol: str = "SYMBOL",
    out_html: str | Path = "QuantTrade/charts/output/chart.html",
    theme: str = "dark",
    height: int = 700,
    title: str | None = None,
    # Precomputed feature selectors (use these windows/periods to pick DF columns)
    ma_windows: list[int] | None = None,     # expects df['ma{w}']
    ema_windows: list[int] | None = None,    # expects df['ema{w}']
    rsi_period: int | None = None,           # expects df[f'rsi{rsi_period}']
    rsi_bounds: tuple[int, int] | None = None,
    # MACD precomputed column names
    macd_cols: tuple[str, str, str] = ("macd", "macd_signal", "macd_hist"),
    macd_colors: dict | None = None,         # optional override colors
    # Chart plumbing
    timeframes: list[str] = ("1m","5m","15m","1h","1d"),
    default_tf: str = "1m",
    digits: int = 2,
    watermark_text: str | None = None,
    watermark_opacity: float = 0.08,
    assets_rel: str = "../static",
    markers: list[dict] | None = None,
) -> Path:
    """
    Render a compute-free Lightweight Charts HTML:
    - Assumes *all* indicators are precomputed in `df`.
    - Packs MA/EMA/RSI/MACD series for lw_chart.js (which only aggregates + draws).
    """
    # --- Check static assets ---
    out_html = Path(out_html)
    static_dir = (out_html.parent / assets_rel).resolve()
    if not static_dir.exists():
        raise FileNotFoundError(f"Static assets directory not found: {static_dir}")
    css_file = static_dir / "lw_chart.css"
    js_file  = static_dir / "lw_chart.js"
    if not css_file.exists():
        raise FileNotFoundError(f"CSS file not found: {css_file}")
    if not js_file.exists():
        raise FileNotFoundError(f"JavaScript file not found: {js_file}")
    js_text = js_file.read_text(encoding="utf-8", errors="ignore")
    if "function initLightweightChart" not in js_text:
        raise RuntimeError(
            f"'initLightweightChart' function not found in {js_file}.\n"
            "Make sure you copied the correct lw_chart.js file."
        )

    # --- DataFrame validation ---
    req = {"open","high","low","close","volume"}
    missing = req - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {sorted(missing)}")

    tmp = df.copy()
    idx = pd.to_datetime(tmp.index)
    tmp.index = (idx.tz_convert("UTC") if idx.tz is not None else idx.tz_localize("UTC"))
    tmp = tmp.dropna(subset=["open","high","low","close"])
    if tmp.empty:
        raise ValueError("No rows with complete OHLC to plot.")

    def to_ts(x) -> int:
        return int(pd.Timestamp(x).timestamp())

    # --- Price / Volume series ---
    candles = [
        {"time": to_ts(t), "open": float(r.open), "high": float(r.high),
         "low": float(r.low), "close": float(r.close)}
        for t, r in tmp[["open","high","low","close"]].iterrows()
    ]
    volumes = []
    for t, r in tmp[["open","close","volume"]].iterrows():
        up = (r.close >= r.open)
        volumes.append({"time": to_ts(t), "value": float(r.volume or 0.0),
                        "color": "#26a69a" if up else "#ef5350"})

    # --- Prepack MA/EMA series from DF columns ---
    ma_windows  = ma_windows or []
    ema_windows = ema_windows or []

    def pack_line(col: str) -> list[dict]:
        s = tmp[col].astype(float)
        return [{"time": to_ts(t), "value": float(v)} for t, v in s.items()]

    ma_series = {}
    for w in ma_windows:
        col = f"ma{w}"
        if col not in tmp.columns:
            raise KeyError(f"MA column missing: {col}")
        ma_series[col] = pack_line(col)

    ema_series = {}
    for w in ema_windows:
        col = f"ema{w}"
        if col not in tmp.columns:
            raise KeyError(f"EMA column missing: {col}")
        ema_series[col] = pack_line(col)

    # --- RSI series (precomputed) ---
    rsi_series = []
    if rsi_period is not None:
        rcol = f"rsi{int(rsi_period)}"
        if rcol not in tmp.columns:
            raise KeyError(f"RSI column missing: {rcol}")
        rsi_series = pack_line(rcol)

    # --- Colors for MA/EMA lines (legend uses these names) ---
    ma_palette  = ["#ff9800","#42a5f5","#ab47bc","#26a69a","#ec407a",
                   "#8d6e63","#66bb6a","#ffa726","#29b6f6","#ef5350"]
    ema_palette = ["#fdd835","#1de9b6","#b388ff","#ff7043","#7e57c2",
                   "#4db6ac","#ba68c8","#9ccc65","#ef5350","#42a5f5"]
    ma_colors  = {f"ma{w}": ma_palette[i % len(ma_palette)]   for i, w in enumerate(ma_windows)}
    ema_colors = {f"ema{w}": ema_palette[i % len(ema_palette)] for i, w in enumerate(ema_windows)}

    # --- MACD (precomputed) ---
    macd_key, sig_key, hist_key = macd_cols
    have_macd = all(k in tmp.columns for k in (macd_key, sig_key, hist_key))
    if have_macd:
        macd_cfg = {
            "enabled": True,
            "series": {
                "macd":  [{"time": to_ts(t), "value": float(v)} for t, v in tmp[macd_key].astype(float).items()],
                "signal":[{"time": to_ts(t), "value": float(v)} for t, v in tmp[sig_key].astype(float).items()],
                "hist":  [{"time": to_ts(t), "value": float(v)} for t, v in tmp[hist_key].astype(float).items()],
            },
            "colors": macd_colors or {
                "macd": "#26c6da",
                "signal": "#ffa726",
                "hist_pos": "#66bb6a",
                "hist_neg": "#ef5350",
            },
        }
    else:
        macd_cfg = {"enabled": False}

    # --- Final config blob for JS ---
    title = title or f"{symbol} • Lightweight Charts"
    config = {
        "candles": candles,
        "volumes": volumes,
        # compute-free prepacked indicator series:
        "ma_series": ma_series,          # { 'ma20': [{time,value}], ... }
        "ema_series": ema_series,        # { 'ema12': [...], ... }
        "rsi_series": rsi_series,        # [ {time,value}, ... ]
        "macd": macd_cfg,                # { enabled, series{macd,signal,hist}, colors }
        # legacy color + window hints (legend + styling)
        "ma_windows": ma_windows,
        "ema_windows": ema_windows,
        "ma_colors": ma_colors,
        "ema_colors": ema_colors,
        # rsi label/bounds
        "rsi_period": rsi_period,
        "rsi_bounds": rsi_bounds,
        # chart plumbing
        "timeframes": list(timeframes),
        "default_tf": default_tf,
        "digits": digits,
        "symbol": symbol,
        "watermark_text": watermark_text or "",
        "watermark_opacity": watermark_opacity,
        "theme": theme,
        "height": height,
        "title": title,
        "markers": markers or [],
    }

    css_href = f"{assets_rel}/lw_chart.css"
    js_src   = f"{assets_rel}/lw_chart.js"

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>{title}</title>
  <link rel="stylesheet" href="{css_href}">
  <script src="https://unpkg.com/lightweight-charts@4.2.1/dist/lightweight-charts.standalone.production.js"></script>
  <script src="{js_src}"></script>
</head>
<body>
<div id="wrap">
  <div id="toolbar"></div>
  <div id="chart-price"></div>
  <div id="chart-vol"></div>
  <div id="chart-macd"></div>  <!-- MACD pane (auto-hidden if macd.enabled=false) -->
  <div id="chart-rsi"></div>
  <div id="legend" class="legend"></div>
  <div id="wm" class="watermark" style="display:none;"></div>
</div>
  <script>
    window.__LW_CHART_CONFIG__ = {json.dumps(config)};
    initLightweightChart(window.__LW_CHART_CONFIG__);
  </script>
</body>
</html>"""

    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8")
    return out_html
