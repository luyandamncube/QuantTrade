/* global LightweightCharts */
function initLightweightChart(config){
  const {
    candles, volumes, ma_windows, ema_windows, ma_colors, ema_colors,
    rsi_period, rsi_bounds, timeframes, default_tf, digits,
    symbol, watermark_text, watermark_opacity, theme, height, title,
    markers = []
  } = config;

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
  const rsiEl   = document.getElementById('chart-rsi');

  if (!priceEl || !volEl || !rsiEl){
    console.error('Missing containers. Need #chart-price, #chart-vol, #chart-rsi.');
    return;
  }

  const totalH = Math.max(420, (height || 700));
  priceEl.style.height = `${totalH - 120 - 160}px`;  // price gets the rest
  volEl.style.height   = `120px`;
  rsiEl.style.height   = `160px`;

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

  // ---- helpers ----
  function tfToMinutes(tf){ const m=String(tf).toLowerCase();
    if (m.endsWith('m')) return parseInt(m);
    if (m.endsWith('h')) return parseInt(m)*60;
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
  function computeSMA(c, w){
    const out=[]; let sum=0;
    for (let i=0;i<c.length;i++){
      sum += c[i].close;
      if (i >= w) sum -= c[i-w].close;
      out.push({ time:c[i].time, value: (i>=w-1) ? sum/w : null });
    }
    return out;
  }
  function computeEMA(c, w){
    const out=[]; if (w<=1){ for (const x of c) out.push({time:x.time,value:x.close}); return out; }
    let sum=0;
    for (let i=0;i<c.length;i++){
      const close=c[i].close;
      if (i < w){ sum+=close; out.push({time:c[i].time,value:null});
        if (i===w-1){ const sma=sum/w; out[i]={time:c[i].time,value:sma};
          let prev=sma, a=2/(w+1);
          for (let j=i+1;j<c.length;j++){ const ema=a*c[j].close+(1-a)*prev; out.push({time:c[j].time,value:ema}); prev=ema; }
          return out;
        }
      }
    }
    return out;
  }
  function computeRSI(c,p){
    if (!p) return [];
    const out=[]; let g=0,l=0;
    for (let i=0;i<c.length;i++){
      if (i===0){ out.push({time:c[i].time,value:null}); continue; }
      const ch=c[i].close-c[i-1].close, gn=Math.max(ch,0), ln=Math.max(-ch,0);
      if (i<=p){ g+=gn; l+=ln; out.push({time:c[i].time,value:(i===p?100-100/(1+(g/p)/(l/p||1e-12)):null)}); }
      else { const chOld=c[i-p].close-c[i-p-1].close, gOld=Math.max(chOld,0), lOld=Math.max(-chOld,0);
        g+=gn-gOld; l+=ln-lOld; const rs=(g/p)/(l/p||1e-12); out.push({time:c[i].time,value:100-100/(1+rs)}); }
    }
    return out;
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
  const chartRSI = LightweightCharts.createChart(rsiEl, {
    layout:{ background:{type:'solid',color:bg}, textColor:text },
    grid:{ vertLines:{color:grid}, horzLines:{color:grid} },
    timeScale:{ timeVisible:true, secondsVisible:false, rightOffset:6, barSpacing:6 },
    rightPriceScale:{ borderVisible:false }
  });

  // series
  const candleSeries = chartPrice.addCandlestickSeries({
    upColor:'#26a69a', downColor:'#ef5350',
    borderUpColor:'#26a69a', borderDownColor:'#ef5350',
    wickUpColor:'#26a69a', wickDownColor:'#ef5350'
  });
  const volumeSeries = chartVol.addHistogramSeries({
    priceFormat:{ type:'volume' }, priceLineVisible:false, base:0
  });

  const maSeries = {};
  for (const w of (ma_windows || [])){
    const name='ma'+w;
    maSeries[name] = chartPrice.addLineSeries({ color:(ma_colors && ma_colors[name])||'#888', lineWidth:2, priceLineVisible:false });
  }
  const emaSeries = {};
  for (const w of (ema_windows || [])){
    const name='ema'+w;
    emaSeries[name] = chartPrice.addLineSeries({ color:(ema_colors && ema_colors[name])||'#aaa', lineWidth:2, priceLineVisible:false });
  }
  let maPriceLines={}, emaPriceLines={}, lastPriceLine=null;

  let rsiSeries=null;
  if (rsi_period && Number.isFinite(+rsi_period)){
    rsiSeries = chartRSI.addLineSeries({ lineWidth:2, color:'#ffd54f', priceLineVisible:false });
    if (Array.isArray(rsi_bounds) && rsi_bounds.length===2
        && Number.isFinite(+rsi_bounds[0]) && Number.isFinite(+rsi_bounds[1])){
      rsiSeries.createPriceLine({ price:+rsi_bounds[1], color:'#9ca3af', lineWidth:1, lineStyle:LightweightCharts.LineStyle.Dotted, title:'70' });
      rsiSeries.createPriceLine({ price:+rsi_bounds[0], color:'#9ca3af', lineWidth:1, lineStyle:LightweightCharts.LineStyle.Dotted, title:'30' });
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
    for (const [name, series] of Object.entries(maSeries)){
      // we can't read series data directly; show last label only
      const last = maPriceLines[name] ? maPriceLines[name].options().price : null;
      html += `<span class="row"><span class="dot" style="background:${(ma_colors && ma_colors[name])||'#888'}"></span>${name}: ${fmt(last)}</span>`;
    }
    for (const [name, series] of Object.entries(emaSeries)){
      const last = emaPriceLines[name] ? emaPriceLines[name].options().price : null;
      html += `<span class="row"><span class="dot" style="background:${(ema_colors && ema_colors[name])||'#aaa'}"></span>${name}: ${fmt(last)}</span>`;
    }
    if (rsiSeries && currentRSI.length){
      const r = valueAtTime(currentRSI, c.time);
      html += `<span class="row">RSI(${rsi_period}): ${fmt(r ? r.value : null)}</span>`;
    }
    legendEl.innerHTML = html;
  }

  function setWatermarkText(tf){
    if (!wmEl || !watermark_text) return;
    wmEl.textContent = watermark_text.replace('{tf}', tf);
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

  // ---- state + TF switch ----
  let currentTF = default_tf || '1d';
  let currentCandles=[], currentVolumes=[], currentRSI=[];

  function setTimeframe(tf){
    currentTF = tf;
    const agg = aggregate(candles, volumes, tf);
    currentCandles = agg.candles;
    currentVolumes = agg.volumes;

    candleSeries.setData(currentCandles);
    volumeSeries.setData(currentVolumes);
    candleSeries.setMarkers(aggregateMarkers(markers, tf));

    if (lastPriceLine) { lastPriceLine.remove(); lastPriceLine=null; }
    for (const [,pl] of Object.entries(maPriceLines)) pl.remove();
    for (const [,pl] of Object.entries(emaPriceLines)) pl.remove();
    maPriceLines={}; emaPriceLines={};

    for (const w of (ma_windows || [])){
      const name='ma'+w, data=computeSMA(currentCandles, w);
      maSeries[name].setData(data);
      const last=lastNonNull(data); if (last){
        maPriceLines[name]=maSeries[name].createPriceLine({
          price:last.value, color:(ma_colors && ma_colors[name])||'#888',
          lineWidth:1, lineStyle:LightweightCharts.LineStyle.Solid, axisLabelVisible:true,
          title:`${name} ${last.value.toFixed(digits||2)}`
        });
      }
    }
    for (const w of (ema_windows || [])){
      const name='ema'+w, data=computeEMA(currentCandles, w);
      emaSeries[name].setData(data);
      const last=lastNonNull(data); if (last){
        emaPriceLines[name]=emaSeries[name].createPriceLine({
          price:last.value, color:(ema_colors && ema_colors[name])||'#aaa',
          lineWidth:1, lineStyle:LightweightCharts.LineStyle.Solid, axisLabelVisible:true,
          title:`${name} ${last.value.toFixed(digits||2)}`
        });
      }
    }

    if (rsiSeries){
      currentRSI = computeRSI(currentCandles, +rsi_period);
      rsiSeries.setData(currentRSI);
    }

    if (currentCandles.length){
      const lastC=currentCandles[currentCandles.length-1];
      lastPriceLine = candleSeries.createPriceLine({
        price:lastC.close, color:'#9ca3af', lineWidth:1,
        lineStyle:LightweightCharts.LineStyle.Dotted, axisLabelVisible:true,
        title:`C ${lastC.close.toFixed(digits||2)}`
      });
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

    chartPrice.timeScale().fitContent();
    const rangeNow = chartPrice.timeScale().getVisibleRange();
    if (rangeNow){
      chartVol.timeScale().setVisibleRange(rangeNow);
      chartRSI.timeScale().setVisibleRange(rangeNow);
    } else {
      requestAnimationFrame(() => {
        const r = chartPrice.timeScale().getVisibleRange();
        if (r){ chartVol.timeScale().setVisibleRange(r); chartRSI.timeScale().setVisibleRange(r); }
      });
    }

    if (currentCandles.length){
      updateLegend(currentCandles[currentCandles.length-1].time);
    }
  }

  // sync ranges across all three (after first paint)
  let syncing = false;
  function sync(from, others){
    if (syncing) return;
    const r = from.timeScale().getVisibleRange();
    if (!r) return;
    syncing = true;
    for (const ch of others) ch.timeScale().setVisibleRange(r);
    syncing = false;
  }

  function wireSync(){
    chartPrice.timeScale().subscribeVisibleTimeRangeChange(() => sync(chartPrice, [chartVol, chartRSI]));
    chartVol.timeScale().subscribeVisibleTimeRangeChange(() => sync(chartVol, [chartPrice, chartRSI]));
    chartRSI.timeScale().subscribeVisibleTimeRangeChange(() => sync(chartRSI, [chartPrice, chartVol]));

    // legend on price crosshair; use time to lookup volume/RSI
    chartPrice.subscribeCrosshairMove((param) => {
      const t = param?.time ?? (currentCandles.at(-1)?.time);
      if (t != null) updateLegend(t);
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
    chartRSI.applyOptions({ width: w });
  });
  ro.observe(document.getElementById('wrap'));
}
