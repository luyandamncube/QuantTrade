from __future__ import annotations
import json
import pandas as pd
from pathlib import Path
import json

def render_lightweight_chart(
    df: pd.DataFrame,
    *,
    symbol: str = "SYMBOL",
    out_html: str | Path = "QuantTrade/charts/output/chart.html",
    theme: str = "dark",
    height: int = 700,
    title: str | None = None,
    ma_windows: list[int] | None = None,
    ema_windows: list[int] | None = None,
    rsi_period: int | None = None,
    rsi_bounds: tuple[int, int] | None = (30, 70),
    timeframes: list[str] = ("1m","5m","15m","1h","1d"),
    default_tf: str = "1m",
    digits: int = 2,
    watermark_text: str | None = None,
    watermark_opacity: float = 0.08,
    assets_rel: str = "../static",  # relative path from out_html to static dir
) -> Path:
    """
    Write an HTML shell that loads static/lw_chart.css and static/lw_chart.js.
    Performs error checks for asset presence and initLightweightChart() function.
    """
    # --- Check static assets ---
    out_html = Path(out_html)
    static_dir = (out_html.parent / assets_rel).resolve()

    if not static_dir.exists():
        raise FileNotFoundError(f"Static assets directory not found: {static_dir}")

    css_file = static_dir / "lw_chart.css"
    js_file = static_dir / "lw_chart.js"

    if not css_file.exists():
        raise FileNotFoundError(f"CSS file not found: {css_file}")
    if not js_file.exists():
        raise FileNotFoundError(f"JavaScript file not found: {js_file}")

    # Check for initLightweightChart function in JS
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

    def to_ts(x): return int(pd.Timestamp(x).timestamp())

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

    ma_windows = ma_windows or []
    ema_windows = ema_windows or []

    ma_palette  = ["#ff9800","#42a5f5","#ab47bc","#26a69a","#ec407a",
                   "#8d6e63","#66bb6a","#ffa726","#29b6f6","#ef5350"]
    ema_palette = ["#fdd835","#1de9b6","#b388ff","#ff7043","#7e57c2",
                   "#4db6ac","#ba68c8","#9ccc65","#ef5350","#42a5f5"]
    ma_colors  = {f"ma{w}": ma_palette[i % len(ma_palette)]   for i, w in enumerate(ma_windows)}
    ema_colors = {f"ema{w}": ema_palette[i % len(ema_palette)] for i, w in enumerate(ema_windows)}

    title = title or f"{symbol} • Lightweight Charts"
    config = {
        "candles": candles,
        "volumes": volumes,
        "ma_windows": ma_windows,
        "ema_windows": ema_windows,
        "ma_colors": ma_colors,
        "ema_colors": ema_colors,
        "rsi_period": rsi_period,
        "rsi_bounds": rsi_bounds,
        "timeframes": list(timeframes),
        "default_tf": default_tf,
        "digits": digits,
        "symbol": symbol,
        "watermark_text": watermark_text or "",
        "watermark_opacity": watermark_opacity,
        "theme": theme,
        "height": height,
        "title": title,
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
    <div id="chart"></div>
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
