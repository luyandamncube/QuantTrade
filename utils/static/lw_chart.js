/* global LightweightCharts */
function initLightweightChart(config){
  const {
    candles, volumes, ma_windows, ema_windows, ma_colors, ema_colors,
    rsi_period, rsi_bounds, timeframes, default_tf, digits,
    symbol, watermark_text, watermark_opacity, theme, height, title,
    markers = [],
    // precomputed series
    ma_series = {},            // { name -> [{time,value}] }
    ema_series = {},           // { name -> [{time,value}] }
    rsi_series = [],           // [{time,value}]  (legacy input)
    macd: macdCfg = { enabled:false }, // { enabled, series:{macd,signal,hist}, colors? }
    rsi: rsiCfgRaw = null      // NEW preferred: { enabled, series, bounds, period }
  } = config;

  // Resolve RSI config (supports new "rsi" and legacy separate keys)
  const rsiCfg = rsiCfgRaw ?? {
    enabled: Array.isArray(rsi_series) && rsi_series.length > 0,
    series:  rsi_series || [],
    bounds:  rsi_bounds,
    period:  rsi_period
  };

  // ---- theming ----
  const isDark = (theme || 'dark') === 'dark';
  document.body.style.background = isDark ? '#0e1117' : '#ffffff';
  const bg   = isDark ? '#0e1117' : '#ffffff';
  const grid = isDark ? '#1f2937' : '#e5e7eb';
  const text = isDark ? '#d1d5db' : '#111827';

  const toolbarEl = document.getElementById('toolbar');
  const legendEl  = document.getElementById('legend');
  const wmEl      = document.getElementById('wm');

  const priceEl = document.getElementById('chart-price');
  const volEl   = document.getElementById('chart-vol');
  const macdEl  = document.getElementById('chart-macd');
  const rsiEl   = document.getElementById('chart-rsi');

  if (!priceEl || !volEl){
    console.error('Missing containers. Need #chart-price and #chart-vol.');
    return;
  }
  if (macdCfg.enabled && !macdEl){
    console.error('MACD enabled but missing container #chart-macd.');
    return;
  }
  if (rsiCfg.enabled && !rsiEl){
    console.error('RSI enabled but missing container #chart-rsi.');
    return;
  }

  const totalH = Math.max(420, (height || 700));
  const rsiH   = rsiCfg.enabled ? 160 : 0;
  const volH   = 120;
  const macdH  = macdCfg.enabled ? 140 : 0;

  // Heights + show/hide panes
  priceEl.style.height = `${totalH - volH - rsiH - macdH}px`;
  volEl.style.height   = `${volH}px`;
  if (macdEl){
    macdEl.style.height = macdCfg.enabled ? `${macdH}px` : '0px';
    macdEl.style.display = macdCfg.enabled ? '' : 'none';
  }
  if (rsiEl){
    rsiEl.style.height = rsiCfg.enabled ? `${rsiH}px` : '0px';
    rsiEl.style.display = rsiCfg.enabled ? '' : 'none';
  }

  if (toolbarEl){
    toolbarEl.style.background = isDark ? 'rgba(14,17,23,0.65)' : 'rgba(255,255,255,0.9)';
    toolbarEl.style.color      = isDark ? '#e5e7eb' : '#111827';
  }
  if (legendEl){
    legendEl.style.background  = isDark ? 'rgba(14,17,23,0.65)' : 'rgba(255,255,255,0.9)';
    legendEl.style.color       = isDark ? '#e5e7eb' : '#111827';
  }
  if (wmEl){
    wmEl.style.color        = isDark ? '#ffffff' : '#111827';
    wmEl.style.opacity      = (watermark_opacity ?? 0.08);
    wmEl.style.mixBlendMode = isDark ? 'screen' : 'multiply';
  }

  // ---- helpers (no indicator math here) ----
  function tfToMinutes(tf){ const m=String(tf).toLowerCase();
    if (m.endsWith('m')) return parseInt(m, 10);
    if (m.endsWith('h')) return parseInt(m, 10)*60;
    if (m.endsWith('d')) return 'day';
    return 1;
  }
  function bucketTimeSec(ts, tf){
    const kind = tfToMinutes(tf);
    if (kind === 'day'){
      const d = new Date(ts * 1000);
      return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000;
    }
    const mins = Math.floor(ts / 60);
    return (mins - (mins % kind)) * 60;
  }
  function aggregate(cs, vs, tf){
    if (!cs || !cs.length) return { candles: [], volumes: [] };
    const map = new Map();
    for (const c of cs){
      const ts = bucketTimeSec(c.time, tf);
      const b = map.get(ts) || { time: ts, open:c.open, high:c.high, low:c.low, close:c.close, volume:0 };
      if (map.has(ts)){
        b.high = Math.max(b.high, c.high);
        b.low  = Math.min(b.low,  c.low);
        b.close = c.close;
      }
      map.set(ts, b);
    }
    for (const v of (vs || [])){
      const ts = bucketTimeSec(v.time, tf);
      const b = map.get(ts);
      if (b) b.volume += (v.value || 0);
    }
    const outC = Array.from(map.values()).sort((a,b)=>a.time-b.time);
    const outV = outC.map(b => ({ time:b.time, value:b.volume, color:(b.close>=b.open)?'#26a69a':'#ef5350' }));
    return { candles: outC, volumes: outV };
  }
  function aggregateMarkers(mks, tf){
    if (!mks || !mks.length) return [];
    const out=[];
    for (const m of mks){
      if (!m || !Number.isFinite(m.time)) continue;
      const t = bucketTimeSec(m.time, tf);
      if (!Number.isFinite(t)) continue;
      out.push({ ...m, time:t });
    }
    return out;
  }
  function aggregateLineSeries(series, tf){
    if (!series || !series.length) return [];
    const map=new Map();
    for (const pt of series){
      const ts = bucketTimeSec(pt.time, tf);
      map.set(ts, { time: ts, value: pt.value }); // keep last in bucket
    }
    return Array.from(map.values()).sort((a,b)=>a.time-b.time);
  }
  function aggregateHistSeries(series, tf){
    if (!series || !series.length) return [];
    const map=new Map();
    for (const pt of series){
      const ts = bucketTimeSec(pt.time, tf);
      const rec = map.get(ts) || { time: ts, sum:0, count:0 };
      rec.sum += (pt.value ?? 0);
      rec.count += 1;
      map.set(ts, rec);
    }
    const arr = Array.from(map.values()).map(r => ({ time:r.time, value: r.count ? r.sum/r.count : null }));
    arr.sort((a,b)=>a.time-b.time);
    return arr;
  }
  function lastNonNull(arr){
    for (let i=arr.length-1;i>=0;i--){ const v=arr[i]?.value; if (v!=null && !Number.isNaN(v)) return {time:arr[i].time,value:v}; }
    return null;
  }
  const fmt = n => (n==null || isNaN(n)) ? '' : Number(n).toFixed(digits ?? 2);
  function fmtVol(n){ if (n==null || isNaN(n)) return '';
    if (n>=1e9) return (n/1e9).toFixed(2)+'B';
    if (n>=1e6) return (n/1e6).toFixed(2)+'M';
    if (n>=1e3) return (n/1e3).toFixed(2)+'K';
    return Number(n).toFixed(0);
  }
  function valueAtTime(arr, t){
    let lo=0, hi=arr.length-1, ans=null;
    while (lo<=hi){
      const mid=(lo+hi)>>1; const mt=arr[mid].time;
      if (mt===t){ ans=arr[mid]; break; }
      if (mt<t){ ans=arr[mid]; lo=mid+1; } else { hi=mid-1; }
    }
    return ans;
  }

  // NEW: remove price lines via series API
  function clearPriceLines(dict, seriesMap){
    for (const [name, pl] of Object.entries(dict)){
      const seriesLine = seriesMap[name];
      if (seriesLine && pl){
        try { seriesLine.removePriceLine(pl); } catch (e) {}
      }
    }
  }

  // ---- build charts ----
  const chartPrice = LightweightCharts.createChart(priceEl, {
    layout:{ background:{type:'solid',color:bg}, textColor:text },
    grid:{ vertLines:{color:grid}, horzLines:{color:grid} },
    timeScale:{ timeVisible:true, secondsVisible:false, rightOffset:6, barSpacing:6 },
    rightPriceScale:{ borderVisible:false }
  });
  const chartVol = LightweightCharts.createChart(volEl, {
    layout:{ background:{type:'solid',color:bg}, textColor:text },
    grid:{ vertLines:{color:grid}, horzLines:{color:grid} },
    timeScale:{ timeVisible:true, secondsVisible:false, rightOffset:6, barSpacing:6 },
    rightPriceScale:{ borderVisible:false }
  });
  const chartRSI = (rsiCfg.enabled && rsiEl)
    ? LightweightCharts.createChart(rsiEl, {
        layout:{ background:{type:'solid',color:bg}, textColor:text },
        grid:{ vertLines:{color:grid}, horzLines:{color:grid} },
        timeScale:{ timeVisible:true, secondsVisible:false, rightOffset:6, barSpacing:6 },
        rightPriceScale:{ borderVisible:false }
      })
    : null;
  const chartMACD = (macdCfg.enabled && macdEl)
    ? LightweightCharts.createChart(macdEl, {
        layout:{ background:{type:'solid',color:bg}, textColor:text },
        grid:{ vertLines:{color:grid}, horzLines:{color:grid} },
        timeScale:{ timeVisible:true, secondsVisible:false, rightOffset:6, barSpacing:6 },
        rightPriceScale:{ borderVisible:false }
      })
    : null;

  // base series
  const candleSeries = chartPrice.addCandlestickSeries({
    upColor:'#26a69a', downColor:'#ef5350',
    borderUpColor:'#26a69a', borderDownColor:'#ef5350',
    wickUpColor:'#26a69a', wickDownColor:'#ef5350'
  });
  const volumeSeries = chartVol.addHistogramSeries({
    priceFormat:{ type:'volume' }, priceLineVisible:false, base:0
  });

  // MA / EMA line series
  const maSeriesLines = {};
  for (const name of Object.keys(ma_series || {})){
    maSeriesLines[name] = chartPrice.addLineSeries({
      color:(ma_colors && ma_colors[name]) || '#888', lineWidth:2, priceLineVisible:false
    });
  }
  const emaSeriesLines = {};
  for (const name of Object.keys(ema_series || {})){
    emaSeriesLines[name] = chartPrice.addLineSeries({
      color:(ema_colors && ema_colors[name]) || '#aaa', lineWidth:2, priceLineVisible:false
    });
  }
  let maPriceLines={}, emaPriceLines={}, lastPriceLine=null;

  // RSI (precomputed)
  let rsiSeriesLine=null, currentRSI=[];
  const rsiBounds = rsiCfg.bounds;
  const rsiPeriodLabel = rsiCfg.period;
  if (chartRSI && Array.isArray(rsiCfg.series) && rsiCfg.series.length){
    rsiSeriesLine = chartRSI.addLineSeries({ lineWidth:2, color:'#ffd54f', priceLineVisible:false });
    if (Array.isArray(rsiBounds) && rsiBounds.length===2
        && Number.isFinite(+rsiBounds[0]) && Number.isFinite(+rsiBounds[1])){
      rsiSeriesLine.createPriceLine({ price:+rsiBounds[1], color:'#9ca3af', lineWidth:1, lineStyle:LightweightCharts.LineStyle.Dotted, title:String(rsiBounds[1]) });
      rsiSeriesLine.createPriceLine({ price:+rsiBounds[0], color:'#9ca3af', lineWidth:1, lineStyle:LightweightCharts.LineStyle.Dotted, title:String(rsiBounds[0]) });
    }
  }

  // MACD (precomputed)
  const macdColors = {
    macd: (macdCfg.colors && macdCfg.colors.macd) || '#26c6da',
    signal: (macdCfg.colors && macdCfg.colors.signal) || '#ffa726',
    hist_pos: (macdCfg.colors && macdCfg.colors.hist_pos) || '#66bb6a',
    hist_neg: (macdCfg.colors && macdCfg.colors.hist_neg) || '#ef5350',
  };
  let macdLineSeries=null, macdSignalSeries=null, macdHistSeries=null;
  let currentMACD=[], currentMACDSignal=[], currentMACDHist=[];
  if (chartMACD){
    macdLineSeries   = chartMACD.addLineSeries({ title:'MACD',   color: macdColors.macd, lineWidth:2, priceLineVisible:false });
    macdSignalSeries = chartMACD.addLineSeries({ title:'Signal', color: macdColors.signal, lineWidth:2, priceLineVisible:false });
    macdHistSeries   = chartMACD.addHistogramSeries({ priceFormat: { type:'price', precision:5, minMove:0.00001 }, priceLineVisible:false, base:0 });
  }

  // ---- dynamic indicator labels (follow crosshair / last visible) ----
  let currentCandles=[], currentVolumes=[];
  let currentMAs = {}, currentEMAs = {};

  function ensurePriceLine(dict, seriesLine, name, color){
    if (dict[name]) return dict[name];
    dict[name] = seriesLine.createPriceLine({
      price: 0,
      color,
      lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Solid,
      axisLabelVisible: true,
      title: name,
    });
    return dict[name];
  }

  function updateIndicatorPriceLines(ts){
    // MA labels
    for (const name of Object.keys(maSeriesLines)){
      const data = currentMAs[name] || [];
      const pt = valueAtTime(data, ts) || lastNonNull(data);
      if (!pt || pt.value == null) continue;
      const color = (ma_colors && ma_colors[name]) || '#888';
      const pl = ensurePriceLine(maPriceLines, maSeriesLines[name], name, color);
      pl.applyOptions({ price: pt.value, color, title: `${name} ${fmt(pt.value)}` });
    }
    // EMA labels
    for (const name of Object.keys(emaSeriesLines)){
      const data = currentEMAs[name] || [];
      const pt = valueAtTime(data, ts) || lastNonNull(data);
      if (!pt || pt.value == null) continue;
      const color = (ema_colors && ema_colors[name]) || '#aaa';
      const pl = ensurePriceLine(emaPriceLines, emaSeriesLines[name], name, color);
      pl.applyOptions({ price: pt.value, color, title: `${name} ${fmt(pt.value)}` });
    }
  }

  // legend
  function updateLegend(ts){
    if (!legendEl || !currentCandles.length) return;
    const c = valueAtTime(currentCandles, ts) || currentCandles[currentCandles.length-1];
    const v = valueAtTime(currentVolumes, ts);
    let html = `<span class="row sym">${symbol}</span><span class="row">${new Date(c.time*1000).toISOString().replace('T',' ').slice(0,19)} UTC</span>`;
    html += `<span class="row">O:${fmt(c.open)} H:${fmt(c.high)} L:${fmt(c.low)} C:${fmt(c.close)}</span>`;
    if (v && v.value!=null) html += `<span class="row">Vol:${fmtVol(v.value)}</span>`;

    for (const name of Object.keys(maSeriesLines)){
      const last = lastNonNull(currentMAs[name] || []);
      html += `<span class="row"><span class="dot" style="background:${(ma_colors && ma_colors[name])||'#888'}"></span>${name}: ${fmt(last?.value)}</span>`;
    }
    for (const name of Object.keys(emaSeriesLines)){
      const last = lastNonNull(currentEMAs[name] || []);
      html += `<span class="row"><span class="dot" style="background:${(ema_colors && ema_colors[name])||'#aaa'}"></span>${name}: ${fmt(last?.value)}</span>`;
    }
    if (rsiSeriesLine && currentRSI.length){
      const r = valueAtTime(currentRSI, c.time);
      const label = rsiPeriodLabel ? `RSI(${rsiPeriodLabel})` : 'RSI';
      html += `<span class="row">${label}: ${fmt(r ? r.value : null)}</span>`;
    }
    if (chartMACD){
      const m = valueAtTime(currentMACD, c.time);
      const s = valueAtTime(currentMACDSignal, c.time);
      const h = valueAtTime(currentMACDHist, c.time);
      html += `<span class="row">MACD: ${fmt(m?.value)} Sig: ${fmt(s?.value)} Hist: ${fmt(h?.value)}</span>`;
    }
    legendEl.innerHTML = html;
  }

  function setWatermarkText(tf){
    const wmText = watermark_text;
    if (!wmEl || !wmText) return;
    wmEl.textContent = wmText.replace('{tf}', tf);
    wmEl.style.display = '';
  }

  function buildToolbar(){
    if (!toolbarEl) return;
    toolbarEl.innerHTML = '';
    for (const tf of (timeframes || [])){
      const btn = document.createElement('div');
      btn.className = 'btn';
      btn.textContent = tf;
      btn.onclick = () => setTimeframe(tf);
      btn.style.border = `1px solid ${isDark ? '#374151' : '#d1d5db'}`;
      btn.style.background = isDark ? '#111827' : '#ffffff';
      toolbarEl.appendChild(btn);
    }
  }

  // ---- TF switch (only aggregation & paint) ----
  let currentTF = default_tf || '1d';

  function setTimeframe(tf){
    currentTF = tf;
    const agg = aggregate(candles, volumes, tf);
    currentCandles = agg.candles;
    currentVolumes = agg.volumes;

    candleSeries.setData(currentCandles);
    volumeSeries.setData(currentVolumes);
    candleSeries.setMarkers(aggregateMarkers(markers, tf));

    // remove price lines via the series API
    if (lastPriceLine) {
      try { candleSeries.removePriceLine(lastPriceLine); } catch (e) {}
      lastPriceLine = null;
    }
    clearPriceLines(maPriceLines,  maSeriesLines);  maPriceLines  = {};
    clearPriceLines(emaPriceLines, emaSeriesLines); emaPriceLines = {};

    // Paint precomputed MA/EMA series
    currentMAs = {};
    for (const name of Object.keys(maSeriesLines)){
      const data = aggregateLineSeries(ma_series[name] || [], tf);
      currentMAs[name] = data;
      maSeriesLines[name].setData(data);
    }
    currentEMAs = {};
    for (const name of Object.keys(emaSeriesLines)){
      const data = aggregateLineSeries(ema_series[name] || [], tf);
      currentEMAs[name] = data;
      emaSeriesLines[name].setData(data);
    }

    // RSI
    if (chartRSI && rsiSeriesLine){
      currentRSI = aggregateLineSeries(rsiCfg.series || [], tf);
      rsiSeriesLine.setData(currentRSI);
    }

    // MACD
    if (chartMACD && macdCfg && macdCfg.series){
      const macdA = aggregateLineSeries(macdCfg.series.macd || [], tf);
      const sigA  = aggregateLineSeries(macdCfg.series.signal || [], tf);
      const histA = aggregateHistSeries(macdCfg.series.hist || [], tf);
      currentMACD = macdA;
      currentMACDSignal = sigA;
      currentMACDHist = histA;

      macdLineSeries.setData(macdA);
      macdSignalSeries.setData(sigA);
      macdHistSeries.setData(histA.map(pt => ({
        time: pt.time,
        value: pt.value,
        color: (pt.value >= 0 ? macdColors.hist_pos : macdColors.hist_neg)
      })));
    }

    if (currentCandles.length){
      const lastC=currentCandles[currentCandles.length-1];
      lastPriceLine = candleSeries.createPriceLine({
        price:lastC.close, color:'#9ca3af', lineWidth:1,
        lineStyle:LightweightCharts.LineStyle.Dotted, axisLabelVisible:true,
        title:`C ${lastC.close.toFixed(digits||2)}`
      });
      // make indicator labels match the last bar initially
      updateIndicatorPriceLines(lastC.time);
    }

    // button state + watermark
    if (toolbarEl){
      for (const el of toolbarEl.querySelectorAll('.btn')){
        const active = (el.textContent === tf);
        el.classList.toggle('active', active);
        el.style.background = active ? (isDark ? '#1f2937' : '#f3f4f6') : (isDark ? '#111827' : '#ffffff');
        el.style.borderColor = active ? (isDark ? '#6b7280' : '#9ca3af') : (isDark ? '#374151' : '#d1d5db');
      }
    }
    setWatermarkText(tf);

    // INITIAL ALIGNMENT: sync panes by logical range
    const lr = chartPrice.timeScale().getVisibleLogicalRange();
    if (lr){
      chartVol.timeScale().setVisibleLogicalRange(lr);
      if (chartRSI)  chartRSI.timeScale().setVisibleLogicalRange(lr);
      if (chartMACD) chartMACD.timeScale().setVisibleLogicalRange(lr);
    }
    // refresh labels using the last visible time (not dataset end)
    const tr = chartPrice.timeScale().getVisibleRange();
    const tvis = tr?.to ?? currentCandles.at(-1)?.time;
    if (tvis != null) updateIndicatorPriceLines(tvis);

    if (currentCandles.length){
      updateLegend(currentCandles[currentCandles.length-1].time);
    }
  }

  // sync panes by LOGICAL range (prevents drift) + keep labels updated
  let syncing = false;
  function syncLogical(from, others){
    if (syncing) return;
    const lr = from.timeScale().getVisibleLogicalRange();
    if (!lr) return;
    syncing = true;
    for (const ch of others) ch.timeScale().setVisibleLogicalRange(lr);
    syncing = false;

    const tr = from.timeScale().getVisibleRange();
    const t = tr?.to ?? currentCandles.at(-1)?.time;
    if (t != null) updateIndicatorPriceLines(t);
  }

  function wireSync(){
    const group = [chartPrice, chartVol]
      .concat(chartRSI ? [chartRSI] : [])
      .concat(chartMACD ? [chartMACD] : []);
    for (const a of group){
      const others = group.filter(x => x!==a);
      a.timeScale().subscribeVisibleLogicalRangeChange(() => syncLogical(a, others));
    }
    chartPrice.subscribeCrosshairMove((param) => {
      const t = param?.time ?? (chartPrice.timeScale().getVisibleRange()?.to ?? currentCandles.at(-1)?.time);
      if (t != null) {
        updateLegend(t);
        updateIndicatorPriceLines(t);
      }
    });
  }

  // init
  buildToolbar();
  requestAnimationFrame(() => {
    setTimeframe(default_tf || '1d');
    requestAnimationFrame(wireSync);
  });

  // responsive widths
  const ro = new ResizeObserver(e => {
    const w = Math.max(320, Math.floor(e[0].contentRect.width));
    chartPrice.applyOptions({ width: w });
    chartVol.applyOptions({ width: w });
    if (chartRSI)  chartRSI.applyOptions({ width: w });
    if (chartMACD) chartMACD.applyOptions({ width: w });
  });
  ro.observe(document.getElementById('wrap'));
}
