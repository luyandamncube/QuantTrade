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
    # ✨ NEW:
    extra_lines: dict[str, str] | None = None,         # {"kc_basis":"kc20_basis","atr_stop":"atr_stop_long_3",...}
    bands: list[dict] | None = None,                   # [{"name":"Keltner 20,2x","upper":"kc20_u","lower":"kc20_l"}]
) -> Path:
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
    tmp.index = (idx.tz_convert("UTC") if getattr(idx, "tz", None) is not None else idx.tz_localize("UTC"))
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

    # --- helpers ---
    ma_windows  = ma_windows or []
    ema_windows = ema_windows or []
    def pack_line(col: str) -> list[dict]:
        if col not in tmp.columns:
            raise KeyError(f"Column missing for chart: {col}")
        s = tmp[col].astype(float)
        return [{"time": to_ts(t), "value": float(v)} for t, v in s.items()]

    # --- MAs / EMAs ---
    ma_series = {f"ma{w}": pack_line(f"ma{w}") for i, w in enumerate(ma_windows)}
    ema_series = {f"ema{w}": pack_line(f"ema{w}") for i, w in enumerate(ema_windows)}

    # --- RSI ---
    rsi_series = []
    if rsi_period is not None:
        rcol = f"rsi{int(rsi_period)}"
        rsi_series = pack_line(rcol)

    # --- Colors for legend ---
    ma_palette  = ["#ff9800","#42a5f5","#ab47bc","#26a69a","#ec407a",
                   "#8d6e63","#66bb6a","#ffa726","#29b6f6","#ef5350"]
    ema_palette = ["#fdd835","#1de9b6","#b388ff","#ff7043","#7e57c2",
                   "#4db6ac","#ba68c8","#9ccc65","#ef5350","#42a5f5"]
    ma_colors  = {f"ma{w}": ma_palette[i % len(ma_palette)]   for i, w in enumerate(ma_windows)}
    ema_colors = {f"ema{w}": ema_palette[i % len(ema_palette)] for i, w in enumerate(ema_windows)}

    # --- MACD ---
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

    # --- ✨ Extra price-pane lines (e.g., kc basis, hh/ll, atr stop) ---
    extra_lines = extra_lines or {}
    extra_lines_series: dict[str, list[dict]] = {}
    for label, col in extra_lines.items():
        if col in tmp.columns:
            extra_lines_series[label] = pack_line(col)

    # --- ✨ Bands (e.g., Keltner upper/lower) ---
    bands = bands or []
    bands_series: list[dict[str, Any]] = []
    for b in bands:
        up_col = b.get("upper"); lo_col = b.get("lower")
        if not up_col or not lo_col:
            continue
        if up_col not in tmp.columns or lo_col not in tmp.columns:
            continue
        bands_series.append({
            "name": b.get("name", f"{up_col}/{lo_col}"),
            "upper": pack_line(up_col),
            "lower": pack_line(lo_col),
        })

    # --- Final config blob for JS ---
    title = title or f"{symbol} • Lightweight Charts"
    config = {
        "candles": candles,
        "volumes": volumes,
        # compute-free prepacked indicator series:
        "ma_series": ma_series,
        "ema_series": ema_series,
        "rsi_series": rsi_series,
        "macd": macd_cfg,
        # legend hints
        "ma_windows": ma_windows,
        "ema_windows": ema_windows,
        "ma_colors": ma_colors,
        "ema_colors": ema_colors,
        # rsi label/bounds
        "rsi_period": rsi_period,
        "rsi_bounds": rsi_bounds,
        # ✨ new payloads
        "extra_lines": extra_lines_series,    # {"kc_basis":[{t,v},...], "atr_stop":[{t,v},...]}
        "bands": bands_series,                # [{"name":..., "upper":[...], "lower":[...]}]
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
  <div id="chart-macd"></div>
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
