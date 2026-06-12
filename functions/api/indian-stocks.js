export async function onRequest() {
  const TD_KEY = 'c0f820df7be2436d88d2e2092d1066fc';
  const ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
  const cors = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'public, max-age=60'
  };

  const results = [];

  // 1. Twelve Data for NSE stocks (proven to work from Cloudflare Workers)
  try {
    const symbols = 'RELIANCE:NSE,TCS:NSE,HDFCBANK:NSE,INFY:NSE,WIPRO:NSE,ICICIBANK:NSE';
    const r = await fetch(`https://api.twelvedata.com/quote?symbol=${encodeURIComponent(symbols)}&apikey=${TD_KEY}`);
    const data = await r.json();
    for (const [key, d] of Object.entries(data)) {
      if (!d || d.status === 'error' || !d.close) continue;
      const baseSym = key.split(':')[0];
      results.push({
        symbol: baseSym + '.NS',
        regularMarketPrice: parseFloat(d.close),
        regularMarketChange: parseFloat(d.change) || 0,
        regularMarketChangePercent: parseFloat(d.percent_change) || 0
      });
    }
  } catch(e) {}

  // 2. Stooq for Indian indices (server-side fetch, no CORS restriction)
  const stooqIndices = [
    ['%5ensei', '^NSEI'],
    ['%5ebsesn', '^BSESN'],
    ['%5ensebank', '^NSEBANK']
  ];

  const idxResults = await Promise.allSettled(stooqIndices.map(async ([s, sym]) => {
    try {
      const r = await fetch(`https://stooq.com/q/d/l/?s=${s}&i=d`, { headers: { 'User-Agent': ua } });
      const text = await r.text();
      const lines = text.trim().split('\n').filter(l => l && !l.startsWith('Date'));
      if (!lines.length) return null;
      const last = lines[lines.length - 1].split(',');
      const prev = lines.length > 1 ? lines[lines.length - 2].split(',') : last;
      const close = parseFloat(last[4]);
      const prevClose = parseFloat(prev[4]);
      if (!close || isNaN(close)) return null;
      const change = close - prevClose;
      return { symbol: sym, regularMarketPrice: close, regularMarketChange: change, regularMarketChangePercent: prevClose ? (change / prevClose) * 100 : 0 };
    } catch(e) { return null; }
  }));

  for (const r of idxResults) {
    if (r.status === 'fulfilled' && r.value) results.push(r.value);
  }

  // 3. If Stooq indices failed, fall back to Twelve Data for NIFTY/SENSEX
  const hasNifty = results.some(r => r.symbol === '^NSEI');
  const hasSensex = results.some(r => r.symbol === '^BSESN');
  if (!hasNifty || !hasSensex) {
    try {
      const idxSyms = [];
      if (!hasNifty) idxSyms.push('NIFTY 50:NSE');
      if (!hasSensex) idxSyms.push('SENSEX:BSE');
      if (idxSyms.length) {
        const r = await fetch(`https://api.twelvedata.com/quote?symbol=${encodeURIComponent(idxSyms.join(','))}&apikey=${TD_KEY}`);
        const data = await r.json();
        const tryAdd = (key, sym) => {
          const d = data[key] || (data.symbol && data.symbol === key.split(':')[0] ? data : null);
          if (!d || d.status === 'error' || !d.close) return;
          results.push({ symbol: sym, regularMarketPrice: parseFloat(d.close), regularMarketChange: parseFloat(d.change) || 0, regularMarketChangePercent: parseFloat(d.percent_change) || 0 });
        };
        tryAdd('NIFTY 50:NSE', '^NSEI');
        tryAdd('SENSEX:BSE', '^BSESN');
      }
    } catch(e) {}
  }

  return new Response(JSON.stringify({ quoteResponse: { result: results, error: null } }), { headers: cors });
}
