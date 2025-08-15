# QuantTrade/utils/charts.py
from __future__ import annotations
import json
import pandas as pd
from pathlib import Path

# def render_lightweight_chart(
#     df: pd.DataFrame,
#     *,
#     symbol: str = "SYMBOL",
#     out_html: str | Path = "chart.html",
#     theme: str = "dark",
#     height: int = 700,
#     title: str | None = None,
#     # indicators (recomputed per TF)
#     ma_windows: list[int] | None = None,   # e.g., [20, 50, 200]
#     rsi_period: int | None = 14,           # None to disable RSI pane
#     rsi_bounds: tuple[int,int] = (30, 70),
#     # timeframe switcher
#     timeframes: list[str] = ("1m","5m","15m","1h","1d"),
#     default_tf: str = "1m",
#     digits: int = 2,
#     # watermark
#     watermark_text: str | None = None,     # e.g. "SPY — {tf}"
#     watermark_opacity: float = 0.08,
# ) -> Path:
#     """
#     Candles + volume (+ optional MAs + optional RSI pane) with a timeframe switcher and watermark.
#     Writes a self-contained HTML file and returns its Path.
#     """
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
#     ma_names = [f"ma{w}" for w in ma_windows]
#     palette = ["#ff9800","#42a5f5","#ab47bc","#26a69a","#ec407a",
#                "#8d6e63","#66bb6a","#ffa726","#29b6f6","#ef5350"]
#     color_map = {name: palette[i % len(palette)] for i, name in enumerate(ma_names)}

#     title = title or f"{symbol} • Lightweight Charts"
#     wm_color = "#ffffff" if theme=="dark" else "#111827"
#     wm_text  = watermark_text or ""
#     wm_display = "" if wm_text else "display:none;"

#     html = f"""<!doctype html>
# <html>
# <head>
#   <meta charset="utf-8"/>
#   <title>{title}</title>
#   <script src="https://unpkg.com/lightweight-charts@4.2.1/dist/lightweight-charts.standalone.production.js"></script>
#   <style>
#     html, body {{ margin:0; padding:0; background:{("#0e1117" if theme=="dark" else "#ffffff")}; }}
#     #wrap {{ position:relative; }}
#     #toolbar {{
#       display:flex; gap:8px; padding:8px 12px;
#       background:{("rgba(14,17,23,0.65)" if theme=="dark" else "rgba(255,255,255,0.9)")};
#       position:sticky; top:0; z-index:10;
#       font:13px -apple-system, Segoe UI, Roboto, sans-serif;
#       color:{("#e5e7eb" if theme=="dark" else "#111827")}; backdrop-filter: blur(4px);
#     }}
#     .btn {{ padding:4px 10px; border-radius:6px; cursor:pointer; user-select:none;
#       border:1px solid {("#374151" if theme=="dark" else "#d1d5db")};
#       background:{("#111827" if theme=="dark" else "#ffffff")}; }}
#     .btn.active {{ background:{("#1f2937" if theme=="dark" else "#f3f4f6")};
#       border-color:{("#6b7280" if theme=="dark" else "#9ca3af")}; font-weight:600; }}
#     #chart {{ height:{height}px; }}
#     .legend {{
#       position:absolute; left:12px; top:54px; padding:6px 8px;
#       background:{("rgba(14,17,23,0.65)" if theme=="dark" else "rgba(255,255,255,0.9)")};
#       border-radius:8px; font:12px/1.25 -apple-system, Segoe UI, Roboto, sans-serif;
#       color:{("#e5e7eb" if theme=="dark" else "#111827")}; box-shadow:0 2px 6px rgba(0,0,0,.15);
#       z-index:5;
#     }}
#     .legend .row {{ margin-right:10px; display:inline-block; }}
#     .legend .sym {{ font-weight:600; }}
#     .dot {{ display:inline-block; width:8px; height:8px; border-radius:50%; margin:0 4px; vertical-align:middle; }}
#     .watermark {{
#       position:absolute; inset:0; display:flex; align-items:center; justify-content:center;
#       pointer-events:none; user-select:none; z-index:0;
#       transform: rotate(-18deg); opacity: {watermark_opacity};
#       color: {wm_color}; font: 900 11vw/1 -apple-system, Segoe UI, Roboto, sans-serif;
#       letter-spacing: .08em; text-transform: uppercase;
#       mix-blend-mode: {"screen" if theme=="dark" else "multiply"};
#     }}
#   </style>
# </head>
# <body>
#   <div id="wrap">
#     <div id="toolbar"></div>
#     <div id="chart"></div>
#     <div id="legend" class="legend"></div>
#     <div id="wm" class="watermark" style="{wm_display}"></div>
#   </div>
#   <script>
#     // ---- Data from Python ----
#     const baseCandles = {json.dumps(candles)};
#     const baseVolumes = {json.dumps(volumes)};
#     const TF_OPTIONS  = {json.dumps(list(timeframes))};
#     let   currentTF   = "{default_tf}";
#     const MA_WINDOWS  = {json.dumps(ma_windows)};
#     const MA_COLORS   = {json.dumps(color_map)};
#     const RSI_PERIOD  = {json.dumps(rsi_period)};
#     const RSI_BOUNDS  = {json.dumps(rsi_bounds)};
#     const DIGITS      = {digits};
#     const SYMBOL      = {json.dumps(symbol)};
#     const WM_TEMPLATE = {json.dumps(wm_text)};

#     // ---- Single element handles (✅ declare once) ----
#     const toolbarEl = document.getElementById('toolbar');
#     const legendEl  = document.getElementById('legend');
#     const wmEl      = document.getElementById('wm');

#     // ---- Helpers ----
#     function tfToMinutes(tf) {{
#       const m = tf.toLowerCase();
#       if (m.endsWith('m')) return parseInt(m);
#       if (m.endsWith('h')) return parseInt(m) * 60;
#       if (m.endsWith('d')) return 'day';
#       return 1;
#     }}
#     function bucketTimeSec(ts, tf) {{
#       const kind = tfToMinutes(tf);
#       if (kind === 'day') {{
#         const d = new Date(ts * 1000);
#         return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000;
#       }} else {{
#         const mins   = Math.floor(ts / 60);
#         const bstart = mins - (mins % kind);
#         return bstart * 60;
#       }}
#     }}
#     function aggregate(candles, volumes, tf) {{
#       if (!candles.length) return {{candles: [], volumes: []}};
#       const map = new Map();
#       for (const c of candles) {{
#         const ts = bucketTimeSec(c.time, tf);
#         let b = map.get(ts);
#         if (!b) {{
#           b = {{ time: ts, open: c.open, high: c.high, low: c.low, close: c.close, volume: 0 }};
#           map.set(ts, b);
#         }} else {{
#           b.high = Math.max(b.high, c.high);
#           b.low  = Math.min(b.low,  c.low);
#           b.close = c.close;
#         }}
#       }}
#       for (const v of baseVolumes) {{
#         const ts = bucketTimeSec(v.time, tf);
#         const b = map.get(ts);
#         if (b) b.volume += (v.value || 0);
#       }}
#       const outCandles = Array.from(map.values()).sort((a,b)=>a.time-b.time);
#       const outVolumes = outCandles.map(b => ({{
#         time: b.time, value: b.volume, color: (b.close >= b.open) ? '#26a69a' : '#ef5350'
#       }}));
#       return {{candles: outCandles, volumes: outVolumes}};
#     }}
#     function computeSMA(candles, window) {{
#       const out = []; let sum = 0;
#       for (let i=0; i<candles.length; i++) {{
#         sum += candles[i].close;
#         if (i >= window) sum -= candles[i - window].close;
#         const val = (i >= window-1) ? (sum / window) : null;
#         out.push({{ time: candles[i].time, value: (val===null? null : +val) }});
#       }}
#       return out;
#     }}
#     function computeRSI(candles, period) {{
#       const out = [];
#       let gainSum = 0, lossSum = 0;
#       for (let i=0; i<candles.length; i++) {{
#         if (i === 0) {{ out.push({{ time: candles[i].time, value: null }}); continue; }}
#         const ch = candles[i].close - candles[i-1].close;
#         const gain = Math.max(ch, 0), loss = Math.max(-ch, 0);
#         if (i <= period) {{
#           gainSum += gain; lossSum += loss;
#           out.push({{ time: candles[i].time, value: (i === period ? 100 - 100/(1 + (gainSum/period) / (lossSum/period || 1e-12)) : null) }});
#         }} else {{
#           const chOld = candles[i-period].close - candles[i-period-1].close;
#           const gOld = Math.max(chOld, 0), lOld = Math.max(-chOld, 0);
#           gainSum += gain - gOld; lossSum += loss - lOld;
#           const avgGain = gainSum / period, avgLoss = lossSum / period;
#           const rs = avgGain / (avgLoss || 1e-12);
#           out.push({{ time: candles[i].time, value: 100 - 100 / (1 + rs) }});
#         }}
#       }}
#       return out;
#     }}

#     // ---- Chart ----
#     const chart = LightweightCharts.createChart(document.getElementById('chart'), {{
#       layout: {{
#         background: {{ type:'solid', color:'{("#0e1117" if theme=="dark" else "#ffffff")}' }},
#         textColor: '{("#d1d5db" if theme=="dark" else "#111827")}'
#       }},
#       grid: {{
#         vertLines: {{ color:'{("#1f2937" if theme=="dark" else "#e5e7eb")}' }},
#         horzLines: {{ color:'{("#1f2937" if theme=="dark" else "#e5e7eb")}' }}
#       }},
#       timeScale: {{ timeVisible:true, secondsVisible:false, rightOffset:6, barSpacing:6 }},
#       rightPriceScale: {{ borderVisible:false }}
#     }});

#     // scale margins (top price, middle rsi, bottom vol)
#     chart.priceScale('right').applyOptions({{ scaleMargins: {{ top: 0.0, bottom: {0.40 if rsi_period else 0.20} }} }});
#     chart.priceScale('rsi').applyOptions({{   scaleMargins: {{ top: 0.60, bottom: 0.20 }} }});
#     chart.priceScale('vol').applyOptions({{   scaleMargins: {{ top: {0.80 if rsi_period else 0.80}, bottom: 0.00 }} }});

#     const candleSeries = chart.addCandlestickSeries({{
#       upColor:'#26a69a', downColor:'#ef5350',
#       borderUpColor:'#26a69a', borderDownColor:'#ef5350',
#       wickUpColor:'#26a69a', wickDownColor:'#ef5350'
#     }});
#     const volumeSeries = chart.addHistogramSeries({{
#       priceScaleId:'vol', priceFormat:{{ type:'volume' }},
#       priceLineVisible:false, base:0
#     }});

#     // MAs
#     const maSeries = {{}};
#     for (const w of MA_WINDOWS) {{
#       const name = 'ma' + w;
#       maSeries[name] = chart.addLineSeries({{
#         color: MA_COLORS[name] || '#888', lineWidth:2, priceLineVisible:false
#       }});
#     }}

#     // RSI
#     let rsiSeries = null;
#     if (RSI_PERIOD) {{
#       rsiSeries = chart.addLineSeries({{
#         priceScaleId:'rsi', lineWidth:2, color:'#ffd54f', priceLineVisible:false
#       }});
#       rsiSeries.createPriceLine({{ price: RSI_BOUNDS[1], color:'#9ca3af', lineWidth:1, lineStyle: LightweightCharts.LineStyle.Dotted, title:'70' }});
#       rsiSeries.createPriceLine({{ price: RSI_BOUNDS[0], color:'#9ca3af', lineWidth:1, lineStyle: LightweightCharts.LineStyle.Dotted, title:'30' }});
#     }}

#     // Legend
#     const fmt = n => (n==null || isNaN(n)) ? '' : Number(n).toFixed({digits});
#     const fmtVol = n => {{
#       if (n==null || isNaN(n)) return '';
#       if (n >= 1e9) return (n/1e9).toFixed(2)+'B';
#       if (n >= 1e6) return (n/1e6).toFixed(2)+'M';
#       if (n >= 1e3) return (n/1e3).toFixed(2)+'K';
#       return Number(n).toFixed(0);
#     }};
#     function updateLegend(param) {{
#       let c = param.seriesData ? param.seriesData.get(candleSeries) : null;
#       let v = param.seriesData ? param.seriesData.get(volumeSeries) : null;
#       if (!c) {{
#         const lastIdx = currentCandles.length - 1;
#         c = currentCandles[lastIdx]; v = currentVolumes[lastIdx];
#       }}
#       const ts = c.time;
#       const tstr = new Date(ts*1000).toISOString().replace('T',' ').slice(0,19) + ' UTC';
#       let html = `<span class="row sym">{symbol}</span><span class="row">${{tstr}}</span>`;
#       html += `<span class="row">O:${{fmt(c.open)}} H:${{fmt(c.high)}} L:${{fmt(c.low)}} C:${{fmt(c.close)}}</span>`;
#       if (v && v.value != null) html += `<span class="row">Vol:${{fmtVol(v.value)}}</span>`;
#       for (const [name, series] of Object.entries(maSeries)) {{
#         const sd = param.seriesData ? param.seriesData.get(series) : null;
#         html += `<span class="row"><span class="dot" style="background:${{MA_COLORS[name]||'#888'}}"></span>${{name}}: ${{fmt(sd?sd.value:null)}}</span>`;
#       }}
#       if (rsiSeries) {{
#         const rsd = param.seriesData ? param.seriesData.get(rsiSeries) : null;
#         html += `<span class="row">RSI({json.dumps(rsi_period)}): ${{fmt(rsd?rsd.value:null)}}</span>`;
#       }}
#       legendEl.innerHTML = html;
#     }}
#     chart.subscribeCrosshairMove(updateLegend);

#     // Watermark
#     function setWatermarkText(tf) {{
#       if (!WM_TEMPLATE) return;
#       wmEl.textContent = WM_TEMPLATE.replace('{{tf}}', tf);
#       wmEl.style.display = '';
#     }}

#     // Build toolbar once (✅ no duplicate const)
#     function buildToolbar() {{
#       toolbarEl.innerHTML = '';
#       for (const tf of TF_OPTIONS) {{
#         const btn = document.createElement('div');
#         btn.className = 'btn' + (tf === currentTF ? ' active' : '');
#         btn.textContent = tf;
#         btn.onclick = () => setTimeframe(tf);
#         toolbarEl.appendChild(btn);
#       }}
#     }}

#     // Apply timeframe
#     let currentCandles = [], currentVolumes = [];
#     function setTimeframe(tf) {{
#       currentTF = tf;
#       const agg = aggregate(baseCandles, baseVolumes, tf);
#       currentCandles = agg.candles; currentVolumes = agg.volumes;
#       candleSeries.setData(currentCandles);
#       volumeSeries.setData(currentVolumes);
#       for (const w of MA_WINDOWS) {{
#         const name = 'ma' + w;
#         maSeries[name].setData(computeSMA(currentCandles, w));
#       }}
#       if (rsiSeries) {{
#         rsiSeries.setData(computeRSI(currentCandles, RSI_PERIOD));
#       }}
#       // update active button
#       for (const el of toolbarEl.querySelectorAll('.btn')) {{
#         el.classList.toggle('active', el.textContent === tf);
#       }}
#       setWatermarkText(tf);
#       chart.timeScale().fitContent();
#       if (currentCandles.length) {{
#         const lastIdx = currentCandles.length - 1;
#         updateLegend({{ seriesData: new Map([
#           [candleSeries, currentCandles[lastIdx]],
#           [volumeSeries, currentVolumes[lastIdx]]
#         ]) }});
#       }}
#     }}

#     // Init UI + chart
#     buildToolbar();
#     setTimeframe(currentTF);

#     // Responsive
#     new ResizeObserver(e => {{
#       const w = e[0].contentRect.width;
#       chart.applyOptions({{ width: Math.max(320, Math.floor(w)) }});
#     }}).observe(document.getElementById('wrap'));
#   </script>
# </body>
# </html>"""

#     out_html = Path(out_html)
#     out_html.parent.mkdir(parents=True, exist_ok=True)
#     out_html.write_text(html, encoding="utf-8")
#     return out_html
#     raise NotImplementedError("Paste the tested function body here")

def render_lightweight_chart(
    df: pd.DataFrame,
    *,
    symbol: str = "SYMBOL",
    out_html: str | Path = "chart.html",
    theme: str = "dark",
    height: int = 700,
    title: str | None = None,
    # indicators (recomputed per TF)
    ma_windows: list[int] | None = None,   # e.g., [20, 50, 200]
    ema_windows: list[int] | None = None,  # e.g., [20, 50]
    rsi_period: int | None = 14,           # None to disable RSI pane
    rsi_bounds: tuple[int,int] = (30, 70),
    # timeframe switcher
    timeframes: list[str] = ("1m","5m","15m","1h","1d"),
    default_tf: str = "1m",
    digits: int = 2,
    # watermark
    watermark_text: str | None = None,     # e.g. "SPY — {tf}"
    watermark_opacity: float = 0.08,
) -> Path:
    """
    Candles + volume (+ optional MAs + optional EMAs + optional RSI pane)
    with a timeframe switcher and watermark. Writes a self-contained HTML file
    and returns its Path.
    """
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

    ma_names = [f"ma{w}" for w in ma_windows]
    ema_names = [f"ema{w}" for w in ema_windows]

    # Separate palettes so MA/EMA are visually distinct
    ma_palette  = ["#ff9800","#42a5f5","#ab47bc","#26a69a","#ec407a",
                   "#8d6e63","#66bb6a","#ffa726","#29b6f6","#ef5350"]
    ema_palette = ["#fdd835","#1de9b6","#b388ff","#ff7043","#7e57c2",
                   "#4db6ac","#ba68c8","#9ccc65","#ef5350","#42a5f5"]

    ma_color_map  = {name: ma_palette[i % len(ma_palette)] for i, name in enumerate(ma_names)}
    ema_color_map = {name: ema_palette[i % len(ema_palette)] for i, name in enumerate(ema_names)}

    title = title or f"{symbol} • Lightweight Charts"
    wm_color = "#ffffff" if theme=="dark" else "#111827"
    wm_text  = watermark_text or ""
    wm_display = "" if wm_text else "display:none;"

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>{title}</title>
  <script src="https://unpkg.com/lightweight-charts@4.2.1/dist/lightweight-charts.standalone.production.js"></script>
  <style>
    html, body {{ margin:0; padding:0; background:{("#0e1117" if theme=="dark" else "#ffffff")}; }}
    #wrap {{ position:relative; }}
    #toolbar {{
      display:flex; gap:8px; padding:8px 12px;
      background:{("rgba(14,17,23,0.65)" if theme=="dark" else "rgba(255,255,255,0.9)")};
      position:sticky; top:0; z-index:10;
      font:13px -apple-system, Segoe UI, Roboto, sans-serif;
      color:{("#e5e7eb" if theme=="dark" else "#111827")}; backdrop-filter: blur(4px);
    }}
    .btn {{ padding:4px 10px; border-radius:6px; cursor:pointer; user-select:none;
      border:1px solid {("#374151" if theme=="dark" else "#d1d5db")};
      background:{("#111827" if theme=="dark" else "#ffffff")}; }}
    .btn.active {{ background:{("#1f2937" if theme=="dark" else "#f3f4f6")};
      border-color:{("#6b7280" if theme=="dark" else "#9ca3af")}; font-weight:600; }}
    #chart {{ height:{height}px; }}
    .legend {{
      position:absolute; left:12px; top:54px; padding:6px 8px;
      background:{("rgba(14,17,23,0.65)" if theme=="dark" else "rgba(255,255,255,0.9)")};
      border-radius:8px; font:12px/1.25 -apple-system, Segoe UI, Roboto, sans-serif;
      color:{("#e5e7eb" if theme=="dark" else "#111827")}; box-shadow:0 2px 6px rgba(0,0,0,.15);
      z-index:5;
    }}
    .legend .row {{ margin-right:10px; display:inline-block; }}
    .legend .sym {{ font-weight:600; }}
    .dot {{ display:inline-block; width:8px; height:8px; border-radius:50%; margin:0 4px; vertical-align:middle; }}
    .watermark {{
      position:absolute; inset:0; display:flex; align-items:center; justify-content:center;
      pointer-events:none; user-select:none; z-index:0;
      transform: rotate(-18deg); opacity: {watermark_opacity};
      color: {wm_color}; font: 900 11vw/1 -apple-system, Segoe UI, Roboto, sans-serif;
      letter-spacing: .08em; text-transform: uppercase;
      mix-blend-mode: {"screen" if theme=="dark" else "multiply"};
    }}
  </style>
</head>
<body>
  <div id="wrap">
    <div id="toolbar"></div>
    <div id="chart"></div>
    <div id="legend" class="legend"></div>
    <div id="wm" class="watermark" style="{wm_display}"></div>
  </div>
  <script>
    // ---- Data from Python ----
    const baseCandles = {json.dumps(candles)};
    const baseVolumes = {json.dumps(volumes)};
    const TF_OPTIONS  = {json.dumps(list(timeframes))};
    let   currentTF   = "{default_tf}";
    const MA_WINDOWS  = {json.dumps(ma_windows)};
    const EMA_WINDOWS = {json.dumps(ema_windows)};
    const MA_COLORS   = {json.dumps(ma_color_map)};
    const EMA_COLORS  = {json.dumps(ema_color_map)};
    const RSI_PERIOD  = {json.dumps(rsi_period)};
    const RSI_BOUNDS  = {json.dumps(rsi_bounds)};
    const DIGITS      = {digits};
    const SYMBOL      = {json.dumps(symbol)};
    const WM_TEMPLATE = {json.dumps(wm_text)};

    // ---- Single element handles ----
    const toolbarEl = document.getElementById('toolbar');
    const legendEl  = document.getElementById('legend');
    const wmEl      = document.getElementById('wm');

    // ---- Helpers ----
    function tfToMinutes(tf) {{
      const m = tf.toLowerCase();
      if (m.endsWith('m')) return parseInt(m);
      if (m.endsWith('h')) return parseInt(m) * 60;
      if (m.endsWith('d')) return 'day';
      return 1;
    }}
    function bucketTimeSec(ts, tf) {{
      const kind = tfToMinutes(tf);
      if (kind === 'day') {{
        const d = new Date(ts * 1000);
        return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000;
      }} else {{
        const mins   = Math.floor(ts / 60);
        const bstart = mins - (mins % kind);
        return bstart * 60;
      }}
    }}
    function aggregate(candles, volumes, tf) {{
      if (!candles.length) return {{candles: [], volumes: []}};
      const map = new Map();
      for (const c of candles) {{
        const ts = bucketTimeSec(c.time, tf);
        let b = map.get(ts);
        if (!b) {{
          b = {{ time: ts, open: c.open, high: c.high, low: c.low, close: c.close, volume: 0 }};
          map.set(ts, b);
        }} else {{
          b.high  = Math.max(b.high, c.high);
          b.low   = Math.min(b.low,  c.low);
          b.close = c.close;
        }}
      }}
      for (const v of baseVolumes) {{
        const ts = bucketTimeSec(v.time, tf);
        const b = map.get(ts);
        if (b) b.volume += (v.value || 0);
      }}
      const outCandles = Array.from(map.values()).sort((a,b)=>a.time-b.time);
      const outVolumes = outCandles.map(b => ({{
        time: b.time, value: b.volume, color: (b.close >= b.open) ? '#26a69a' : '#ef5350'
      }}));
      return {{candles: outCandles, volumes: outVolumes}};
    }}
    function computeSMA(candles, window) {{
      const out = []; let sum = 0;
      for (let i=0; i<candles.length; i++) {{
        sum += candles[i].close;
        if (i >= window) sum -= candles[i - window].close;
        const val = (i >= window-1) ? (sum / window) : null;
        out.push({{ time: candles[i].time, value: (val===null? null : +val) }});
      }}
      return out;
    }}
    function computeEMA(candles, window) {{
      const out = [];
      if (window <= 1) {{
        for (const c of candles) out.push({{ time: c.time, value: c.close }});
        return out;
      }}
      // seed with SMA of first N closes
      let sum = 0;
      for (let i=0; i<candles.length; i++) {{
        const close = candles[i].close;
        if (i < window) {{
          sum += close;
          out.push({{ time: candles[i].time, value: null }});
          if (i === window - 1) {{
            const sma = sum / window;
            out[i] = {{ time: candles[i].time, value: sma }};
            // continue with EMA recurrence
            let prev = sma;
            const alpha = 2 / (window + 1);
            for (let j=i+1; j<candles.length; j++) {{
              const ema = alpha * candles[j].close + (1 - alpha) * prev;
              out.push({{ time: candles[j].time, value: ema }});
              prev = ema;
            }}
            return out;
          }}
        }}
      }}
      return out;
    }}
    function computeRSI(candles, period) {{
      if (!period) return [];
      const out = [];
      let gains = 0, losses = 0;
      for (let i=0; i<candles.length; i++) {{
        if (i === 0) {{ out.push({{ time: candles[i].time, value: null }}); continue; }}
        const ch = candles[i].close - candles[i-1].close;
        const gain = Math.max(ch, 0), loss = Math.max(-ch, 0);
        if (i <= period) {{
          gains += gain; losses += loss;
          out.push({{ time: candles[i].time, value: (i === period ? 100 - 100/(1 + (gains/period) / (losses/period || 1e-12)) : null) }});
        }} else {{
          const chOld = candles[i-period].close - candles[i-period-1].close;
          const gOld = Math.max(chOld, 0), lOld = Math.max(-chOld, 0);
          gains += gain - gOld; losses += loss - lOld;
          const avgGain = gains / period, avgLoss = losses / period;
          const rs = avgGain / (avgLoss || 1e-12);
          out.push({{ time: candles[i].time, value: 100 - 100 / (1 + rs) }});
        }}
      }}
      return out;
    }}

    // ---- Chart ----
    const chart = LightweightCharts.createChart(document.getElementById('chart'), {{
      layout: {{
        background: {{ type:'solid', color:'{("#0e1117" if theme=="dark" else "#ffffff")}' }},
        textColor: '{("#d1d5db" if theme=="dark" else "#111827")}'
      }},
      grid: {{
        vertLines: {{ color:'{("#1f2937" if theme=="dark" else "#e5e7eb")}' }},
        horzLines: {{ color:'{("#1f2937" if theme=="dark" else "#e5e7eb")}' }}
      }},
      timeScale: {{ timeVisible:true, secondsVisible:false, rightOffset:6, barSpacing:6 }},
      rightPriceScale: {{ borderVisible:false }}
    }});

    // scale margins (top price, middle rsi, bottom vol)
    chart.priceScale('right').applyOptions({{ scaleMargins: {{ top: 0.0, bottom: {0.40 if rsi_period else 0.20} }} }});
    chart.priceScale('rsi').applyOptions({{   scaleMargins: {{ top: 0.60, bottom: 0.20 }} }});
    chart.priceScale('vol').applyOptions({{   scaleMargins: {{ top: {0.80 if rsi_period else 0.80}, bottom: 0.00 }} }});

    const candleSeries = chart.addCandlestickSeries({{
      upColor:'#26a69a', downColor:'#ef5350',
      borderUpColor:'#26a69a', borderDownColor:'#ef5350',
      wickUpColor:'#26a69a', wickDownColor:'#ef5350'
    }});
    const volumeSeries = chart.addHistogramSeries({{
      priceScaleId:'vol', priceFormat:{{ type:'volume' }},
      priceLineVisible:false, base:0
    }});

    // MAs
    const maSeries = {{}};
    for (const w of MA_WINDOWS) {{
      const name = 'ma' + w;
      maSeries[name] = chart.addLineSeries({{
        color: MA_COLORS[name] || '#888', lineWidth:2, priceLineVisible:false
      }});
    }}

    // EMAs
    const emaSeries = {{}};
    for (const w of EMA_WINDOWS) {{
      const name = 'ema' + w;
      emaSeries[name] = chart.addLineSeries({{
        color: EMA_COLORS[name] || '#aaa', lineWidth:2, priceLineVisible:false
      }});
    }}

    // RSI
    let rsiSeries = null;
    if (RSI_PERIOD) {{
      rsiSeries = chart.addLineSeries({{
        priceScaleId:'rsi', lineWidth:2, color:'#ffd54f', priceLineVisible:false
      }});
      rsiSeries.createPriceLine({{ price: RSI_BOUNDS[1], color:'#9ca3af', lineWidth:1, lineStyle: LightweightCharts.LineStyle.Dotted, title:'70' }});
      rsiSeries.createPriceLine({{ price: RSI_BOUNDS[0], color:'#9ca3af', lineWidth:1, lineStyle: LightweightCharts.LineStyle.Dotted, title:'30' }});
    }}

    // Legend
    const fmt = n => (n==null || isNaN(n)) ? '' : Number(n).toFixed({digits});
    const fmtVol = n => {{
      if (n==null || isNaN(n)) return '';
      if (n >= 1e9) return (n/1e9).toFixed(2)+'B';
      if (n >= 1e6) return (n/1e6).toFixed(2)+'M';
      if (n >= 1e3) return (n/1e3).toFixed(2)+'K';
      return Number(n).toFixed(0);
    }};
    function updateLegend(param) {{
      let c = param.seriesData ? param.seriesData.get(candleSeries) : null;
      let v = param.seriesData ? param.seriesData.get(volumeSeries) : null;
      if (!c) {{
        const lastIdx = currentCandles.length - 1;
        c = currentCandles[lastIdx]; v = currentVolumes[lastIdx];
      }}
      const ts = c.time;
      const tstr = new Date(ts*1000).toISOString().replace('T',' ').slice(0,19) + ' UTC';
      let html = `<span class="row sym">{symbol}</span><span class="row">${{tstr}}</span>`;
      html += `<span class="row">O:${{fmt(c.open)}} H:${{fmt(c.high)}} L:${{fmt(c.low)}} C:${{fmt(c.close)}}</span>`;
      if (v && v.value != null) html += `<span class="row">Vol:${{fmtVol(v.value)}}</span>`;

      for (const [name, series] of Object.entries(maSeries)) {{
        const sd = param.seriesData ? param.seriesData.get(series) : null;
        html += `<span class="row"><span class="dot" style="background:${{MA_COLORS[name]||'#888'}}"></span>${{name}}: ${{fmt(sd?sd.value:null)}}</span>`;
      }}
      for (const [name, series] of Object.entries(emaSeries)) {{
        const sd = param.seriesData ? param.seriesData.get(series) : null;
        html += `<span class="row"><span class="dot" style="background:${{EMA_COLORS[name]||'#aaa'}}"></span>${{name}}: ${{fmt(sd?sd.value:null)}}</span>`;
      }}

      if (rsiSeries) {{
        const rsd = param.seriesData ? param.seriesData.get(rsiSeries) : null;
        html += `<span class="row">RSI({json.dumps(rsi_period)}): ${{fmt(rsd?rsd.value:null)}}</span>`;
      }}
      legendEl.innerHTML = html;
    }}
    chart.subscribeCrosshairMove(updateLegend);

    // Watermark
    function setWatermarkText(tf) {{
      if (!WM_TEMPLATE) return;
      wmEl.textContent = WM_TEMPLATE.replace('{{tf}}', tf);
      wmEl.style.display = '';
    }}

    // Build toolbar once
    function buildToolbar() {{
      toolbarEl.innerHTML = '';
      for (const tf of TF_OPTIONS) {{
        const btn = document.createElement('div');
        btn.className = 'btn' + (tf === currentTF ? ' active' : '');
        btn.textContent = tf;
        btn.onclick = () => setTimeframe(tf);
        toolbarEl.appendChild(btn);
      }}
    }}

    // Apply timeframe
    let currentCandles = [], currentVolumes = [];
    function setTimeframe(tf) {{
      currentTF = tf;
      const agg = aggregate(baseCandles, baseVolumes, tf);
      currentCandles = agg.candles; currentVolumes = agg.volumes;

      candleSeries.setData(currentCandles);
      volumeSeries.setData(currentVolumes);

      for (const w of MA_WINDOWS) {{
        const name = 'ma' + w;
        maSeries[name].setData(computeSMA(currentCandles, w));
      }}
      for (const w of EMA_WINDOWS) {{
        const name = 'ema' + w;
        emaSeries[name].setData(computeEMA(currentCandles, w));
      }}
      if (rsiSeries) {{
        rsiSeries.setData(computeRSI(currentCandles, RSI_PERIOD));
      }}

      // update active button
      for (const el of toolbarEl.querySelectorAll('.btn')) {{
        el.classList.toggle('active', el.textContent === tf);
      }}
      setWatermarkText(tf);
      chart.timeScale().fitContent();

      if (currentCandles.length) {{
        const lastIdx = currentCandles.length - 1;
        updateLegend({{ seriesData: new Map([
          [candleSeries, currentCandles[lastIdx]],
          [volumeSeries, currentVolumes[lastIdx]],
          ...Object.values(maSeries).map(s => [s, null]),
          ...Object.values(emaSeries).map(s => [s, null]),
          ...(rsiSeries ? [[rsiSeries, null]] : [])
        ]) }});
      }}
    }}

    // Init UI + chart
    buildToolbar();
    setTimeframe(currentTF);

    // Responsive
    new ResizeObserver(e => {{
      const w = e[0].contentRect.width;
      chart.applyOptions({{ width: Math.max(320, Math.floor(w)) }});
    }}).observe(document.getElementById('wrap'));
  </script>
</body>
</html>"""

    out_html = Path(out_html)
    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8")
    return out_html