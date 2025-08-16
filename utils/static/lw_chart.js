/* global LightweightCharts */
function initLightweightChart(config){
  const {
    candles, volumes, ma_windows, ema_windows, ma_colors, ema_colors,
    rsi_period, rsi_bounds, timeframes, default_tf, digits,
    symbol, watermark_text, watermark_opacity, theme, height, title
  } = config;

  // -------- Theming --------
  const isDark = (theme || 'dark') === 'dark';
  document.body.style.background = isDark ? '#0e1117' : '#ffffff';
  const toolbarEl = document.getElementById('toolbar');
  const legendEl  = document.getElementById('legend');
  const wmEl      = document.getElementById('wm');
  const chartEl   = document.getElementById('chart');
  chartEl.style.height = `${height || 700}px`;

  toolbarEl.style.background = isDark ? 'rgba(14,17,23,0.65)' : 'rgba(255,255,255,0.9)';
  toolbarEl.style.color      = isDark ? '#e5e7eb' : '#111827';
  legendEl.style.background  = isDark ? 'rgba(14,17,23,0.65)' : 'rgba(255,255,255,0.9)';
  legendEl.style.color       = isDark ? '#e5e7eb' : '#111827';
  wmEl.style.color           = isDark ? '#ffffff' : '#111827';
  wmEl.style.opacity         = (watermark_opacity ?? 0.08);
  wmEl.style.mixBlendMode    = isDark ? 'screen' : 'multiply';

  const bg     = isDark ? '#0e1117' : '#ffffff';
  const grid   = isDark ? '#1f2937' : '#e5e7eb';
  const text   = isDark ? '#d1d5db' : '#111827';

  // -------- Helpers --------
  function tfToMinutes(tf){
    const m = String(tf).toLowerCase();
    if (m.endsWith('m')) return parseInt(m);
    if (m.endsWith('h')) return parseInt(m) * 60;
    if (m.endsWith('d')) return 'day';
    return 1;
  }
  function bucketTimeSec(ts, tf){
    const kind = tfToMinutes(tf);
    if (kind === 'day'){
      const d = new Date(ts * 1000);
      return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000;
    } else {
      const mins = Math.floor(ts / 60);
      const bstart = mins - (mins % kind);
      return bstart * 60;
    }
  }
  function aggregate(candles, volumes, tf){
    if (!candles.length) return {candles: [], volumes: []};
    const map = new Map();
    for (const c of candles){
      const ts = bucketTimeSec(c.time, tf);
      let b = map.get(ts);
      if (!b){
        b = { time: ts, open: c.open, high: c.high, low: c.low, close: c.close, volume: 0 };
        map.set(ts, b);
      } else {
        b.high  = Math.max(b.high, c.high);
        b.low   = Math.min(b.low,  c.low);
        b.close = c.close;
      }
    }
    for (const v of volumes){
      const ts = bucketTimeSec(v.time, tf);
      const b = map.get(ts);
      if (b) b.volume += (v.value || 0);
    }
    const outC = Array.from(map.values()).sort((a,b)=>a.time-b.time);
    const outV = outC.map(b => ({ time: b.time, value: b.volume, color: (b.close >= b.open) ? '#26a69a' : '#ef5350' }));
    return {candles: outC, volumes: outV};
  }
  function computeSMA(c, w){
    const out = []; let sum = 0;
    for (let i=0;i<c.length;i++){
      sum += c[i].close;
      if (i >= w) sum -= c[i-w].close;
      const val = (i >= w-1) ? (sum / w) : null;
      out.push({ time: c[i].time, value: (val==null? null : +val) });
    }
    return out;
  }
  function computeEMA(c, w){
    const out = [];
    if (w <= 1) { for (const x of c) out.push({ time:x.time, value:x.close }); return out; }
    let sum = 0;
    for (let i=0;i<c.length;i++){
      const close = c[i].close;
      if (i < w){
        sum += close; out.push({ time:c[i].time, value:null });
        if (i === w-1){
          const sma = sum / w; out[i] = { time:c[i].time, value:sma };
          let prev = sma; const alpha = 2 / (w+1);
          for (let j=i+1;j<c.length;j++){ const ema = alpha*c[j].close + (1-alpha)*prev; out.push({ time:c[j].time, value:ema }); prev = ema; }
          return out;
        }
      }
    }
    return out;
  }
  function computeRSI(c, period){
    if (!period) return [];
    const out=[]; let gains=0, losses=0;
    for (let i=0;i<c.length;i++){
      if (i===0){ out.push({time:c[i].time, value:null}); continue; }
      const ch = c[i].close - c[i-1].close;
      const g = Math.max(ch,0), l = Math.max(-ch,0);
      if (i <= period){
        gains += g; losses += l;
        out.push({ time:c[i].time, value: (i===period ? 100 - 100/(1 + (gains/period) / (losses/period || 1e-12)) : null) });
      } else {
        const chOld = c[i-period].close - c[i-period-1].close;
        const gOld = Math.max(chOld,0), lOld = Math.max(-chOld,0);
        gains += g - gOld; losses += l - lOld;
        const avgGain = gains/period, avgLoss = losses/period;
        const rs = avgGain / (avgLoss || 1e-12);
        out.push({ time:c[i].time, value: 100 - 100/(1+rs) });
      }
    }
    return out;
  }
  function lastNonNull(arr){
    for (let i = arr.length - 1; i >= 0; i--){
      const v = arr[i]?.value;
      if (v != null && !Number.isNaN(v)) return { time: arr[i].time, value: v };
    }
    return null;
  }
  const fmt = n => (n==null || isNaN(n)) ? '' : Number(n).toFixed(digits ?? 2);
  function fmtVol(n){
    if (n==null || isNaN(n)) return '';
    if (n >= 1e9) return (n/1e9).toFixed(2)+'B';
    if (n >= 1e6) return (n/1e6).toFixed(2)+'M';
    if (n >= 1e3) return (n/1e3).toFixed(2)+'K';
    return Number(n).toFixed(0);
  }

  // -------- Chart --------
  const chart = LightweightCharts.createChart(chartEl, {
    layout: { background: { type:'solid', color:bg }, textColor: text },
    grid: { vertLines:{color:grid}, horzLines:{color:grid} },
    timeScale: { timeVisible:true, secondsVisible:false, rightOffset:6, barSpacing:6 },
    rightPriceScale: { borderVisible:false }
  });
  chart.priceScale('right').applyOptions({ scaleMargins: { top: 0.0, bottom: (rsi_period ? 0.40 : 0.20) } });
  chart.priceScale('rsi').applyOptions({   scaleMargins: { top: 0.60, bottom: 0.20 } });
  chart.priceScale('vol').applyOptions({   scaleMargins: { top: 0.80, bottom: 0.00 } });

  const candleSeries = chart.addCandlestickSeries({
    upColor:'#26a69a', downColor:'#ef5350',
    borderUpColor:'#26a69a', borderDownColor:'#ef5350',
    wickUpColor:'#26a69a', wickDownColor:'#ef5350'
  });
  const volumeSeries = chart.addHistogramSeries({ priceScaleId:'vol', priceFormat:{ type:'volume' }, priceLineVisible:false, base:0 });

  const maSeries = {};
  for (const w of (ma_windows || [])){
    const name = 'ma' + w;
    maSeries[name] = chart.addLineSeries({ color: ma_colors[name] || '#888', lineWidth:2, priceLineVisible:false });
  }
  const emaSeries = {};
  for (const w of (ema_windows || [])){
    const name = 'ema' + w;
    emaSeries[name] = chart.addLineSeries({ color: ema_colors[name] || '#aaa', lineWidth:2, priceLineVisible:false });
  }
  let maPriceLines  = {};
  let emaPriceLines = {};
  let lastPriceLine = null;

  let rsiSeries = null;
  if (rsi_period){
    rsiSeries = chart.addLineSeries({ priceScaleId:'rsi', lineWidth:2, color:'#ffd54f', priceLineVisible:false });
    if (Array.isArray(rsi_bounds) && rsi_bounds.length === 2){
      rsiSeries.createPriceLine({ price: rsi_bounds[1], color:'#9ca3af', lineWidth:1, lineStyle: LightweightCharts.LineStyle.Dotted, title:'70' });
      rsiSeries.createPriceLine({ price: rsi_bounds[0], color:'#9ca3af', lineWidth:1, lineStyle: LightweightCharts.LineStyle.Dotted, title:'30' });
    }
  }

  function updateLegend(param){
    let c = param.seriesData ? param.seriesData.get(candleSeries) : null;
    let v = param.seriesData ? param.seriesData.get(volumeSeries) : null;
    if (!c && currentCandles.length){
      const lastIdx = currentCandles.length - 1;
      c = currentCandles[lastIdx]; v = currentVolumes[lastIdx];
    }
    if (!c) return;
    const ts = c.time;
    const tstr = new Date(ts*1000).toISOString().replace('T',' ').slice(0,19) + ' UTC';
    let html = `<span class="row sym">${symbol}</span><span class="row">${tstr}</span>`;
    html += `<span class="row">O:${fmt(c.open)} H:${fmt(c.high)} L:${fmt(c.low)} C:${fmt(c.close)}</span>`;
    if (v && v.value != null) html += `<span class="row">Vol:${fmtVol(v.value)}</span>`;
    for (const [name, series] of Object.entries(maSeries)){
      const sd = param.seriesData ? param.seriesData.get(series) : null;
      html += `<span class="row"><span class="dot" style="background:${ma_colors[name]||'#888'}"></span>${name}: ${fmt(sd?sd.value:null)}</span>`;
    }
    for (const [name, series] of Object.entries(emaSeries)){
      const sd = param.seriesData ? param.seriesData.get(series) : null;
      html += `<span class="row"><span class="dot" style="background:${ema_colors[name]||'#aaa'}"></span>${name}: ${fmt(sd?sd.value:null)}</span>`;
    }
    if (rsiSeries){
      const rsd = param.seriesData ? param.seriesData.get(rsiSeries) : null;
      html += `<span class="row">RSI(${rsi_period}): ${fmt(rsd?rsd.value:null)}</span>`;
    }
    legendEl.innerHTML = html;
  }
  chart.subscribeCrosshairMove(updateLegend);

  function setWatermarkText(tf){
    if (!watermark_text) return;
    wmEl.textContent = watermark_text.replace('{tf}', tf);
    wmEl.style.display = '';
  }

  function buildToolbar(){
    toolbarEl.innerHTML = '';
    for (const tf of (timeframes || [])){
      const btn = document.createElement('div');
      btn.className = 'btn' + (tf === currentTF ? ' active' : '');
      btn.textContent = tf;
      btn.style.border = `1px solid ${isDark ? '#374151' : '#d1d5db'}`;
      btn.style.background = isDark ? '#111827' : '#ffffff';
      btn.onclick = () => setTimeframe(tf);
      toolbarEl.appendChild(btn);
    }
  }

  // ---- timeframe switching
  let currentTF = default_tf || '1m';
  let currentCandles = [], currentVolumes = [];

  function setTimeframe(tf){
    currentTF = tf;
    const agg = aggregate(candles, volumes, tf);
    currentCandles = agg.candles; currentVolumes = agg.volumes;
    candleSeries.setData(currentCandles);
    volumeSeries.setData(currentVolumes);

    if (lastPriceLine) { lastPriceLine.remove(); lastPriceLine = null; }

    // compute + set series
    for (const w of (ma_windows || [])){
      const name = 'ma' + w;
      maSeries[name].setData(computeSMA(currentCandles, w));
    }
    for (const w of (ema_windows || [])){
      const name = 'ema' + w;
      emaSeries[name].setData(computeEMA(currentCandles, w));
    }

    // rebuild right‑scale price lines
    for (const [, pl] of Object.entries(maPriceLines))  { pl.remove(); }
    for (const [, pl] of Object.entries(emaPriceLines)) { pl.remove(); }
    maPriceLines  = {};
    emaPriceLines = {};

    for (const w of (ma_windows || [])){
      const name = 'ma' + w;
      const data = maSeries[name].data || computeSMA(currentCandles, w);
      const last = lastNonNull(data);
      if (last){
        maPriceLines[name] = maSeries[name].createPriceLine({
          price: last.value, color: ma_colors[name] || '#888',
          lineWidth: 1, lineStyle: LightweightCharts.LineStyle.Solid,
          axisLabelVisible: true, title: `${name} ${last.value.toFixed(digits||2)}`
        });
      }
    }
    for (const w of (ema_windows || [])){
      const name = 'ema' + w;
      const data = emaSeries[name].data || computeEMA(currentCandles, w);
      const last = lastNonNull(data);
      if (last){
        emaPriceLines[name] = emaSeries[name].createPriceLine({
          price: last.value, color: ema_colors[name] || '#aaa',
          lineWidth: 1, lineStyle: LightweightCharts.LineStyle.Solid,
          axisLabelVisible: true, title: `${name} ${last.value.toFixed(digits||2)}`
        });
      }
    }

    if (rsiSeries){
      rsiSeries.setData(computeRSI(currentCandles, rsi_period));
    }

    // optional last close label
    if (currentCandles.length){
      const lastC = currentCandles[currentCandles.length - 1];
      lastPriceLine = candleSeries.createPriceLine({
        price: lastC.close, color: '#9ca3af',
        lineWidth: 1, lineStyle: LightweightCharts.LineStyle.Dotted,
        axisLabelVisible: true, title: `C ${lastC.close.toFixed(digits||2)}`
      });
    }

    // active button + watermark + view
    for (const el of toolbarEl.querySelectorAll('.btn')){
      el.classList.toggle('active', el.textContent === tf);
      if (el.textContent === tf){
        el.style.background = isDark ? '#1f2937' : '#f3f4f6';
        el.style.borderColor = isDark ? '#6b7280' : '#9ca3af';
      } else {
        el.style.background = isDark ? '#111827' : '#ffffff';
        el.style.borderColor = isDark ? '#374151' : '#d1d5db';
      }
    }
    setWatermarkText(tf);
    chart.timeScale().fitContent();

    // seed legend with last bar
    if (currentCandles.length){
      const lastIdx = currentCandles.length - 1;
      updateLegend({ seriesData: new Map([
        [candleSeries, currentCandles[lastIdx]],
        [volumeSeries, currentVolumes[lastIdx]],
        ...Object.values(maSeries).map(s => [s, null]),
        ...Object.values(emaSeries).map(s => [s, null]),
        ...(rsiSeries ? [[rsiSeries, null]] : [])
      ]) });
    }
  }

  // init
  buildToolbar();
  setTimeframe(currentTF);

  // responsive
  new ResizeObserver(e => {
    const w = e[0].contentRect.width;
    chart.applyOptions({ width: Math.max(320, Math.floor(w)) });
  }).observe(document.getElementById('wrap'));
}
