#!/usr/bin/env python3
"""
fetch_market_data.py
Daily market data fetcher for thebusinessledger.theapurva.com

Run: python3 scripts/fetch_market_data.py
Output: public/data/*.json  (served as static files by the website)

Schedule: GitHub Actions runs this after Indian market close (10:00 UTC = 3:30 PM IST)
          and again after US market close (21:30 UTC = 4:30 PM EST)
"""

import json, os, sys
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import yfinance as yf
except ImportError:
    print("Installing yfinance...")
    os.system(f"{sys.executable} -m pip install yfinance -q")
    import yfinance as yf

# ── Output directory ──────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT  = os.path.join(ROOT, 'public', 'data')
os.makedirs(OUT, exist_ok=True)

NOW = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

# ── Symbol lists ──────────────────────────────────────────────────────────────

INDIA_INDICES = ['^NSEI', '^BSESN', '^NSEBANK', '^CNX100', '^NSEMDCP50', '^CNXSC', '^CNXIT', '^CNXAUTO', '^CNXPHARMA', '^CNXFMCG', '^INDIAVIX']
INDEX_NAMES   = {
    '^NSEI':      'NIFTY 50',
    '^BSESN':     'SENSEX',
    '^NSEBANK':   'NIFTY BANK',
    '^CNX100':    'NIFTY 100',
    '^NSEMDCP50': 'NIFTY MIDCAP 50',
    '^CNXSC':     'NIFTY SMALLCAP',
    '^CNXIT':     'NIFTY IT',
    '^CNXAUTO':   'NIFTY AUTO',
    '^CNXPHARMA': 'NIFTY PHARMA',
    '^CNXFMCG':   'NIFTY FMCG',
    '^INDIAVIX':  'INDIA VIX',
}

NSE_STOCKS = [
    ('RELIANCE.NS',   'Reliance Industries'),
    ('TCS.NS',        'Tata Consultancy Services'),
    ('HDFCBANK.NS',   'HDFC Bank'),
    ('INFY.NS',       'Infosys'),
    ('ICICIBANK.NS',  'ICICI Bank'),
    ('HINDUNILVR.NS', 'Hindustan Unilever'),
    ('BAJFINANCE.NS', 'Bajaj Finance'),
    ('KOTAKBANK.NS',  'Kotak Mahindra Bank'),
    ('LT.NS',         'Larsen & Toubro'),
    ('SBIN.NS',       'State Bank of India'),
    ('WIPRO.NS',      'Wipro'),
    ('TMCV.NS',       'Tata Motors (CV)'),
    ('BHARTIARTL.NS', 'Bharti Airtel'),
    ('MARUTI.NS',     'Maruti Suzuki'),
    ('ADANIENT.NS',   'Adani Enterprises'),
    ('HCLTECH.NS',    'HCL Technologies'),
    ('TITAN.NS',      'Titan Company'),
    ('ASIANPAINT.NS', 'Asian Paints'),
    # Mid Cap leaders
    ('PERSISTENT.NS', 'Persistent Systems'),
    ('POLYCAB.NS',    'Polycab India'),
    ('MPHASIS.NS',    'Mphasis'),
    ('DIXON.NS',      'Dixon Technologies'),
    ('COFORGE.NS',    'Coforge'),
    # Small Cap leaders
    ('LATENTVIEW.NS', 'LatentView Analytics'),
    ('KAYNES.NS',     'Kaynes Technology'),
    ('BIKAJI.NS',     'Bikaji Foods'),
    ('IRCTC.NS',      'IRCTC'),
    ('CAMPUS.NS',     'Campus Activewear'),
]

BSE_STOCKS = [
    ('RELIANCE.BO',   'Reliance Industries'),
    ('TCS.BO',        'Tata Consultancy Services'),
    ('HDFCBANK.BO',   'HDFC Bank'),
    ('INFY.BO',       'Infosys'),
    ('ICICIBANK.BO',  'ICICI Bank'),
    ('TATASTEEL.BO',  'Tata Steel'),
    ('SUNPHARMA.BO',  'Sun Pharmaceutical'),
    ('ONGC.BO',       'ONGC'),
    ('BAJAJFINSV.BO', 'Bajaj Finserv'),
    ('POWERGRID.BO',  'Power Grid Corp'),
]

# All Indian sector stocks (for markets/index.astro)
SECTOR_STOCKS = [
    # Banking
    'HDFCBANK.NS','ICICIBANK.NS','KOTAKBANK.NS','AXISBANK.NS','SBIN.NS',
    'BANKBARODA.NS','PNB.NS','INDUSINDBK.NS','FEDERALBNK.NS','IDFCFIRSTB.NS','BANDHANBNK.NS','AUBANK.NS',
    # IT
    'TCS.NS','INFY.NS','WIPRO.NS','HCLTECH.NS','TECHM.NS','LTTS.NS','MPHASIS.NS','PERSISTENT.NS','COFORGE.NS','KPITTECH.NS',
    # Finance & NBFC
    'BAJFINANCE.NS','BAJAJFINSV.NS','HDFCAMC.NS','HDFCLIFE.NS','SBILIFE.NS','ICICIGI.NS','ICICIPRULI.NS','JIOFIN.NS','CHOLAFIN.NS','MUTHOOTFIN.NS',
    # Auto
    'TMCV.NS','MARUTI.NS','M&M.NS','BAJAJ-AUTO.NS','HEROMOTOCO.NS','EICHERMOT.NS','TVSMOTOR.NS','ASHOKLEY.NS','BOSCHLTD.NS','MOTHERSON.NS',
    # Pharma
    'SUNPHARMA.NS','DRREDDY.NS','CIPLA.NS','DIVISLAB.NS','AUROPHARMA.NS','BIOCON.NS','LUPIN.NS','TORNTPHARM.NS','APOLLOHOSP.NS','MAXHEALTH.NS',
    # FMCG
    'HINDUNILVR.NS','ITC.NS','NESTLEIND.NS','BRITANNIA.NS','DABUR.NS','MARICO.NS','GODREJCP.NS','ASIANPAINT.NS','BERGEPAINT.NS','TATACONSUM.NS',
    # Energy
    'RELIANCE.NS','ONGC.NS','IOC.NS','BPCL.NS','HINDPETRO.NS','GAIL.NS','NTPC.NS','TATAPOWER.NS','ADANIGREEN.NS','ADANIPORTS.NS',
    # Infra
    'LT.NS','SIEMENS.NS','ABB.NS','BEL.NS','HAL.NS','RVNL.NS','IRFC.NS','CUMMINSIND.NS','THERMAX.NS','KEC.NS',
    # Metals
    'TATASTEEL.NS','JSWSTEEL.NS','HINDALCO.NS','VEDL.NS','SAIL.NS','NMDC.NS','COALINDIA.NS','MOIL.NS','JSWENERGY.NS','NATIONALUM.NS',
    # Telecom
    'BHARTIARTL.NS','IDEA.NS','HFCL.NS','TEJASNET.NS','NETWORK18.NS','ZEEL.NS','SUNTV.NS','INDIAMART.NS','NAUKRI.NS','TATACOMM.NS',
]

# All symbols that get a /company/ detail page — fundamentals + history fetched daily
COMPANY_SYMBOLS = list(dict.fromkeys([
    # Banking
    'HDFCBANK.NS','ICICIBANK.NS','KOTAKBANK.NS','AXISBANK.NS','SBIN.NS','BANKBARODA.NS','PNB.NS',
    'INDUSINDBK.NS','FEDERALBNK.NS','IDFCFIRSTB.NS','BANDHANBNK.NS','AUBANK.NS',
    # IT
    'TCS.NS','INFY.NS','WIPRO.NS','HCLTECH.NS','TECHM.NS','LTTS.NS','MPHASIS.NS',
    'PERSISTENT.NS','COFORGE.NS','KPITTECH.NS',
    # Finance & NBFC
    'BAJFINANCE.NS','BAJAJFINSV.NS','HDFCAMC.NS','HDFCLIFE.NS','SBILIFE.NS',
    'ICICIGI.NS','ICICIPRULI.NS','JIOFIN.NS','CHOLAFIN.NS','MUTHOOTFIN.NS',
    # Auto
    'TMCV.NS','MARUTI.NS','BAJAJ-AUTO.NS','HEROMOTOCO.NS','EICHERMOT.NS',
    'TVSMOTOR.NS','ASHOKLEY.NS','BOSCHLTD.NS','MOTHERSON.NS',
    # Pharma
    'SUNPHARMA.NS','DRREDDY.NS','CIPLA.NS','DIVISLAB.NS','AUROPHARMA.NS',
    'BIOCON.NS','LUPIN.NS','TORNTPHARM.NS','APOLLOHOSP.NS','MAXHEALTH.NS',
    # FMCG
    'HINDUNILVR.NS','ITC.NS','NESTLEIND.NS','BRITANNIA.NS','DABUR.NS',
    'MARICO.NS','GODREJCP.NS','ASIANPAINT.NS','BERGEPAINT.NS','TATACONSUM.NS',
    # Energy
    'RELIANCE.NS','ONGC.NS','IOC.NS','BPCL.NS','HINDPETRO.NS',
    'GAIL.NS','NTPC.NS','TATAPOWER.NS','ADANIGREEN.NS','ADANIPORTS.NS',
    # Infra
    'LT.NS','SIEMENS.NS','ABB.NS','BEL.NS','HAL.NS',
    'RVNL.NS','IRFC.NS','CUMMINSIND.NS','THERMAX.NS','KEC.NS',
    # Metals
    'TATASTEEL.NS','JSWSTEEL.NS','HINDALCO.NS','VEDL.NS','SAIL.NS',
    'NMDC.NS','COALINDIA.NS','MOIL.NS','JSWENERGY.NS','NATIONALUM.NS',
    # Telecom / Media / Others
    'BHARTIARTL.NS','IDEA.NS','HFCL.NS','NETWORK18.NS','ZEEL.NS',
    'SUNTV.NS','INDIAMART.NS','NAUKRI.NS','TATACOMM.NS',
    # Misc
    'TITAN.NS','M&M.NS','ADANIENT.NS',
    # Adani Group (extended)
    'ADANIPOWER.NS','ADANIENSOL.NS','ADANIWILMAR.NS','AMBUJACEMENT.NS','ACC.NS',
    # Consumer & New-age
    'ZOMATO.NS','DMART.NS','TRENT.NS','NYKAA.NS','PAYTM.NS','POLICYBZR.NS',
    # Industrials / Materials
    'PIDILITIND.NS','HAVELLS.NS','POLYCAB.NS','DIXON.NS','IRCTC.NS',
    # Small/Mid cap additions
    'LATENTVIEW.NS','KAYNES.NS','BIKAJI.NS','CAMPUS.NS',
]))

US_INDICES = ['^GSPC', '^DJI', '^IXIC', '^RUT']
US_INDEX_NAMES = {'^GSPC': 'S&P 500', '^DJI': 'Dow Jones', '^IXIC': 'NASDAQ', '^RUT': 'Russell 2000'}

US_TECH = [
    ('AAPL','Apple Inc'), ('MSFT','Microsoft'), ('GOOGL','Alphabet'),
    ('AMZN','Amazon'), ('META','Meta Platforms'), ('NVDA','NVIDIA'),
    ('TSLA','Tesla'), ('AMD','AMD'), ('INTC','Intel'), ('CRM','Salesforce'),
    ('ORCL','Oracle'), ('ADBE','Adobe'), ('NFLX','Netflix'), ('PYPL','PayPal'), ('QCOM','Qualcomm'),
]

US_FINANCE = [
    ('JPM','JPMorgan Chase'), ('BAC','Bank of America'), ('WFC','Wells Fargo'),
    ('GS','Goldman Sachs'), ('MS','Morgan Stanley'), ('C','Citigroup'),
    ('BLK','BlackRock'), ('V','Visa'), ('MA','Mastercard'),
    ('AXP','American Express'), ('BRK-B','Berkshire Hathaway'), ('SCHW','Charles Schwab'),
]

# ── Core fetch function ───────────────────────────────────────────────────────

def fetch_batch(symbols: list) -> dict:
    """
    Fetch latest close price + daily change for a list of symbols.
    Returns dict keyed by symbol.
    Uses yfinance batch download (single HTTP request for all symbols).
    """
    if not symbols:
        return {}

    print(f"  Fetching {len(symbols)} symbols...")
    try:
        raw = yf.download(
            symbols if len(symbols) > 1 else symbols[0],
            period='5d',
            interval='1d',
            progress=False,
            auto_adjust=True,
        )
        close = raw['Close'] if len(symbols) > 1 else raw['Close'].rename(symbols[0]).to_frame()
        if hasattr(close, 'to_frame'):
            close = close.to_frame()
    except Exception as e:
        print(f"  Batch download failed: {e}")
        return {}

    result = {}
    for sym in symbols:
        try:
            col = sym if sym in close.columns else close.columns[0] if len(symbols) == 1 else None
            if col is None:
                result[sym] = _empty(sym)
                continue
            series = close[col].dropna()
            if len(series) < 1:
                result[sym] = _empty(sym)
                continue
            price = float(series.iloc[-1])
            prev  = float(series.iloc[-2]) if len(series) >= 2 else price
            chg   = price - prev
            pct   = (chg / prev * 100) if prev else 0
            result[sym] = {
                'symbol': sym,
                'regularMarketPrice':         round(price, 2),
                'regularMarketChange':        round(chg,   2),
                'regularMarketChangePercent': round(pct,   2),
            }
        except Exception as e:
            print(f"  Error processing {sym}: {e}")
            result[sym] = _empty(sym)

    return result

def _empty(sym):
    return {'symbol': sym, 'regularMarketPrice': 0, 'regularMarketChange': 0, 'regularMarketChangePercent': 0}

ARCHIVE_DAYS = 90   # how many days of snapshots to keep

def save(filename, data):
    # 1. Always write the live file (current behaviour)
    path = os.path.join(OUT, filename)
    with open(path, 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    size = os.path.getsize(path)

    # 2. Also write a dated copy: public/data/archive/YYYY-MM-DD/<filename>
    #    Skip per-company files — they already carry 1Y history inside them
    if not filename.startswith('company/'):
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        archive_dir = os.path.join(OUT, 'archive', today)
        os.makedirs(archive_dir, exist_ok=True)
        archive_path = os.path.join(archive_dir, filename)
        with open(archive_path, 'w') as f:
            json.dump(data, f, separators=(',', ':'))

    print(f"  Saved {filename} ({size/1024:.1f} KB)")

def prune_archive():
    """Delete archive folders older than ARCHIVE_DAYS days."""
    from datetime import timedelta, date
    archive_root = os.path.join(OUT, 'archive')
    if not os.path.isdir(archive_root):
        return
    cutoff = date.today() - timedelta(days=ARCHIVE_DAYS)
    removed = 0
    for folder in os.listdir(archive_root):
        try:
            folder_date = date.fromisoformat(folder)
            if folder_date < cutoff:
                import shutil
                shutil.rmtree(os.path.join(archive_root, folder))
                removed += 1
        except ValueError:
            pass  # skip non-date folders
    if removed:
        print(f"  Pruned {removed} archive folder(s) older than {ARCHIVE_DAYS} days")

def fetch_company_history_batch(symbols):
    """Batch-download 1Y of daily closes for all company symbols (single HTTP request)."""
    print(f"  Batch downloading 1Y history for {len(symbols)} symbols...")
    try:
        raw = yf.download(
            symbols if len(symbols) > 1 else symbols[0],
            period='1y', interval='1d', progress=False, auto_adjust=True,
        )
        close = raw['Close'] if len(symbols) > 1 else raw['Close'].rename(symbols[0]).to_frame()
        if hasattr(close, 'to_frame'):
            close = close.to_frame()
        result = {}
        for sym in symbols:
            col = sym if sym in close.columns else (close.columns[0] if len(symbols)==1 else None)
            if col is None:
                result[sym] = []
                continue
            series = close[col].dropna()
            result[sym] = [
                {'t': int(ts.timestamp()), 'c': round(float(v), 2)}
                for ts, v in series.items()
            ]
        return result
    except Exception as e:
        print(f"  History batch failed: {e}")
        return {sym: [] for sym in symbols}

def fetch_one_info(sym):
    """Fetch fundamentals for one symbol via yfinance.Ticker.info."""
    try:
        info = yf.Ticker(sym).info
        return sym, {
            'longName':           info.get('longName') or info.get('shortName') or sym,
            'sector':             info.get('sector') or '',
            'industry':           info.get('industry') or '',
            'website':            info.get('website') or '',
            'marketCap':          info.get('marketCap'),
            'trailingPE':         info.get('trailingPE'),
            'priceToBook':        info.get('priceToBook'),
            'trailingEps':        info.get('trailingEps'),
            'returnOnEquity':     info.get('returnOnEquity'),
            'dividendYield':      info.get('dividendYield'),
            'beta':               info.get('beta'),
            'fiftyTwoWeekHigh':   info.get('fiftyTwoWeekHigh'),
            'fiftyTwoWeekLow':    info.get('fiftyTwoWeekLow'),
            'averageVolume':      info.get('averageVolume'),
            'regularMarketPrice': info.get('regularMarketPrice') or info.get('currentPrice'),
        }
    except Exception as e:
        print(f"  Info failed for {sym}: {e}")
        return sym, {}

def fetch_all_info_parallel(symbols, workers=12):
    """Fetch .info for all symbols in parallel."""
    print(f"  Fetching fundamentals for {len(symbols)} symbols ({workers} workers)...")
    results = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_one_info, sym): sym for sym in symbols}
        done = 0
        for future in as_completed(futures):
            sym, info = future.result()
            results[sym] = info
            done += 1
            if done % 10 == 0:
                print(f"    {done}/{len(symbols)} done...")
    return results

# ── Conglomerate Stats ───────────────────────────────────────────────────────

# Map: which yfinance symbol to use as the "revenue proxy" for each group entry
# and how to format it into a human-readable stat string.
CONGLOMERATE_SYMBOLS = {
    'RELIANCE.NS': 'RELIANCE.NS',
    'TMCV.NS':     'TMCV.NS',
    'TCS.NS':      'TCS.NS',
    'HDFCBANK.NS': 'HDFCBANK.NS',
}

def fmt_inr_cr(val):
    """Format a raw INR value (from yfinance, in rupees) into ₹X.XL Cr."""
    if not val or val <= 0:
        return None
    cr = val / 1e7          # 1 Cr = 10^7
    if cr >= 100000:
        return f'₹{cr/100000:.1f}L Cr'
    elif cr >= 1000:
        return f'₹{cr/1000:.0f}K Cr'
    else:
        return f'₹{cr:.0f} Cr'

def fmt_inr_mcap(val):
    """Format market cap from yfinance (INR rupees) to ₹X.X L Cr."""
    return fmt_inr_cr(val)

def fetch_conglomerate_stats():
    """
    Read public/data/conglomerate.json, refresh autoStats (marketCap, revenue)
    from yfinance for each group's primary symbol, then save the file back.
    This runs as part of the daily data fetch so stats stay current.
    """
    path = os.path.join(OUT, 'conglomerate.json')
    if not os.path.exists(path):
        print("  conglomerate.json not found — skipping")
        return

    with open(path) as f:
        data = json.load(f)

    updated = 0
    for group_sym, yf_sym in CONGLOMERATE_SYMBOLS.items():
        if group_sym not in data:
            continue
        try:
            info = yf.Ticker(yf_sym).info
            mcap = info.get('marketCap')
            rev  = info.get('totalRevenue')
            data[group_sym]['autoStats'] = {
                'marketCap': fmt_inr_mcap(mcap),
                'revenue':   fmt_inr_cr(rev),
            }
            print(f"  {group_sym}: mcap={fmt_inr_mcap(mcap)}, rev={fmt_inr_cr(rev)}")
            updated += 1
        except Exception as e:
            print(f"  {group_sym} autoStats failed: {e}")

    data['lastUpdated'] = NOW

    with open(path, 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    size = os.path.getsize(path)
    print(f"  Saved conglomerate.json ({size/1024:.1f} KB) — {updated} groups refreshed")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*55}")
    print(f"  Market Data Fetch  —  {NOW}")
    print(f"{'='*55}")

    # 1. India indices
    print("\n[1/5] India Indices")
    idx_q = fetch_batch(INDIA_INDICES)
    save('india-indices.json', {
        'lastUpdated': NOW,
        'indices': [
            {**idx_q.get(s, _empty(s)), 'name': INDEX_NAMES.get(s, s)}
            for s in INDIA_INDICES
        ]
    })

    # 2. NSE + BSE stocks (india.astro page)
    print("\n[2/5] India NSE + BSE Stocks")
    all_india_syms = [s for s,_ in NSE_STOCKS] + [s for s,_ in BSE_STOCKS]
    india_q = fetch_batch(all_india_syms)
    save('india-stocks.json', {
        'lastUpdated': NOW,
        'nse': [
            {**india_q.get(s, _empty(s)), 'name': name}
            for s, name in NSE_STOCKS
        ],
        'bse': [
            {**india_q.get(s, _empty(s)), 'name': name}
            for s, name in BSE_STOCKS
        ],
    })

    # 3. India sector stocks (markets/index.astro page)
    print("\n[3/5] India Sector Stocks")
    unique_sector = list(dict.fromkeys(SECTOR_STOCKS))  # deduplicate, preserve order
    sector_q = fetch_batch(unique_sector)
    # Merge with already-fetched India data to avoid re-fetching
    sector_q.update(india_q)
    save('india-sectors.json', {
        'lastUpdated': NOW,
        'quotes': sector_q,
    })

    # 4. US Tech
    print("\n[4/5] US Tech Stocks")
    tech_syms = [s for s,_ in US_TECH]
    tech_q = fetch_batch(tech_syms)
    save('us-tech.json', {
        'lastUpdated': NOW,
        'stocks': [
            {**tech_q.get(s, _empty(s)), 'name': name}
            for s, name in US_TECH
        ],
    })

    # 5. US Finance + indices
    print("\n[5/5] US Finance Stocks + Indices")
    fin_syms = [s for s,_ in US_FINANCE]
    us_all = fetch_batch(fin_syms + US_INDICES)
    save('us-finance.json', {
        'lastUpdated': NOW,
        'stocks': [
            {**us_all.get(s, _empty(s)), 'name': name}
            for s, name in US_FINANCE
        ],
    })
    save('us-indices.json', {
        'lastUpdated': NOW,
        'indices': [
            {**us_all.get(s, _empty(s)), 'name': US_INDEX_NAMES.get(s, s)}
            for s in US_INDICES
        ],
    })

    # 6. Per-company detail pages (price + fundamentals + 1Y history)
    print("\n[6/6] Company Detail Data (fundamentals + history)")
    company_dir = os.path.join(OUT, 'company')
    os.makedirs(company_dir, exist_ok=True)

    # 1Y history for all company symbols (one batch request)
    history_map = fetch_company_history_batch(COMPANY_SYMBOLS)

    # Fundamentals in parallel (uses Ticker.info — one call per symbol, parallelised)
    info_map = fetch_all_info_parallel(COMPANY_SYMBOLS, workers=12)

    # Merge price/change from already-fetched sector data where available
    # (sector_q already has today's price+change from the earlier batch)
    saved_count = 0
    for sym in COMPANY_SYMBOLS:
        info  = info_map.get(sym, {})
        hist  = history_map.get(sym, [])
        sq    = sector_q.get(sym, {})   # from earlier india-sectors fetch

        # Prefer sector_q price (fresher batch download) over info price
        price  = sq.get('regularMarketPrice') or info.get('regularMarketPrice') or 0
        change = sq.get('regularMarketChange', 0)
        pct    = sq.get('regularMarketChangePercent', 0)

        record = {
            'lastUpdated':           NOW,
            'symbol':                sym,
            'longName':              info.get('longName') or sym,
            'shortName':             info.get('longName') or sym,
            'sector':                info.get('sector', ''),
            'industry':              info.get('industry', ''),
            'website':               info.get('website', ''),
            'country':               'India',
            'regularMarketPrice':    round(price,  2) if price  else None,
            'regularMarketChange':   round(change, 2) if change else None,
            'regularMarketChangePercent': round(pct, 2) if pct else None,
            'marketCap':             info.get('marketCap'),
            'trailingPE':            info.get('trailingPE'),
            'priceToBook':           info.get('priceToBook'),
            'trailingEps':           info.get('trailingEps'),
            'returnOnEquity':        info.get('returnOnEquity'),
            'dividendYield':         info.get('dividendYield'),
            'beta':                  info.get('beta'),
            'fiftyTwoWeekHigh':      info.get('fiftyTwoWeekHigh'),
            'fiftyTwoWeekLow':       info.get('fiftyTwoWeekLow'),
            'averageVolume':         info.get('averageVolume'),
            'history':               hist,
        }

        fname = sym.replace('.', '-') + '.json'
        path  = os.path.join(company_dir, fname)
        with open(path, 'w') as f:
            json.dump(record, f, separators=(',', ':'))
        saved_count += 1

    total_kb = sum(os.path.getsize(os.path.join(company_dir, f)) for f in os.listdir(company_dir)) / 1024
    print(f"  Saved {saved_count} company files ({total_kb:.0f} KB total) → public/data/company/")

    # 7. FII / DII flow — uses curl_cffi to bypass Cloudflare (works from GitHub Actions)
    print("\n[7/7] FII / DII Flow")
    try:
        try:
            from curl_cffi import requests as cffi_requests
            _resp = cffi_requests.get(
                'https://www.nseindia.com/api/fiidiiTradeReact',
                impersonate='chrome120',
                headers={
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': 'https://www.nseindia.com/market-data/fii-dii-data',
                },
                timeout=20,
            )
            raw = _resp.json()
        except ImportError:
            # Fallback: session cookie approach via urllib
            import urllib.request, http.cookiejar, json as _json, time as _time
            _cj = http.cookiejar.CookieJar()
            _opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_cj))
            _opener.addheaders = [('User-Agent','Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36')]
            try: _opener.open('https://www.nseindia.com/', timeout=12); _time.sleep(1.5)
            except: pass
            _req = urllib.request.Request('https://www.nseindia.com/api/fiidiiTradeReact',
                headers={'User-Agent':'Mozilla/5.0','Accept':'application/json','Referer':'https://www.nseindia.com/'})
            with _opener.open(_req, timeout=15) as resp:
                raw = _json.loads(resp.read().decode())

        if raw and isinstance(raw, list) and len(raw) > 0:
            by_date = {}
            for row in raw:
                date = row.get('date', '')
                cat  = row.get('category', '').upper()
                raw_net = row.get('netValue', row.get('fiiNet', row.get('fii_net', 0))) or 0
                net  = float(str(raw_net).replace(',', '')) if raw_net else 0.0
                if date not in by_date:
                    by_date[date] = {'date': date, 'fiiNet': 0, 'diiNet': 0}
                if cat.startswith('FII'):
                    by_date[date]['fiiNet'] = net
                elif cat == 'DII':
                    by_date[date]['diiNet'] = net
                elif not cat:
                    by_date[date]['fiiNet'] = float(str(row.get('fiiNet', 0) or 0).replace(',', ''))
                    by_date[date]['diiNet'] = float(str(row.get('diiNet', 0) or 0).replace(',', ''))
            rows = sorted(by_date.values(), key=lambda x: x['date'], reverse=True)[:10]
            if not rows: raise ValueError('no usable rows')
            save('fii-dii.json', {'lastUpdated': NOW, 'data': rows})
            print(f"  Saved fii-dii.json — latest: FII={rows[0]['fiiNet']:.0f}Cr DII={rows[0]['diiNet']:.0f}Cr")
        else:
            raise ValueError('empty response')
    except Exception as e:
        print(f"  FII/DII fetch failed: {e} — saving empty fallback")
        save('fii-dii.json', {'lastUpdated': NOW, 'data': [], 'error': str(e)})

    # 8. India IPO data from NSE
    print("\n[8/9] India IPO Data (NSE)")
    try:
        import urllib.request as _ur, json as _j
        _h = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.nseindia.com/',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        req = _ur.Request('https://www.nseindia.com/api/allIpo', headers=_h)
        with _ur.urlopen(req, timeout=15) as resp:
            raw = _j.loads(resp.read().decode())
        upcoming = raw.get('upcoming', [])
        ongoing  = raw.get('ongoing', [])
        listed   = raw.get('listed', [])[:20]  # last 20 listed

        def parse_ipo(item, status):
            return {
                'company':     item.get('companyName', item.get('symbol', '')),
                'symbol':      item.get('symbol', ''),
                'openDate':    item.get('ipoOpenDate', item.get('openDate', '')),
                'closeDate':   item.get('ipoCloseDate', item.get('closeDate', '')),
                'listingDate': item.get('listingDate', ''),
                'priceMin':    item.get('issuePrice', item.get('minPrice', '')),
                'priceMax':    item.get('issuePrice', item.get('maxPrice', '')),
                'lotSize':     item.get('lotSize', ''),
                'issueSize':   item.get('issueSize', ''),
                'status':      status,
            }

        ipo_data = {
            'lastUpdated': NOW,
            'upcoming': [parse_ipo(i, 'upcoming') for i in upcoming],
            'ongoing':  [parse_ipo(i, 'ongoing')  for i in ongoing],
            'listed':   [parse_ipo(i, 'listed')   for i in listed],
        }
        save('ipo.json', ipo_data)
        print(f"  Saved ipo.json — {len(upcoming)} upcoming, {len(ongoing)} ongoing, {len(listed)} listed")
    except Exception as e:
        print(f"  IPO fetch failed: {e} — saving empty fallback")
        save('ipo.json', {'lastUpdated': NOW, 'upcoming': [], 'ongoing': [], 'listed': [], 'error': str(e)})

    # 9. India Earnings Calendar (next earnings date per company via yfinance)
    print("\n[9/9] India Earnings Calendar (yfinance)")
    EARNINGS_STOCKS = [
        'TCS.NS','INFY.NS','WIPRO.NS','HCLTECH.NS','TECHM.NS',
        'RELIANCE.NS','HDFCBANK.NS','ICICIBANK.NS','KOTAKBANK.NS','SBIN.NS','AXISBANK.NS',
        'BAJFINANCE.NS','BAJAJFINSV.NS','LT.NS','BHARTIARTL.NS',
        'SUNPHARMA.NS','DRREDDY.NS','CIPLA.NS','DIVISLAB.NS',
        'HINDUNILVR.NS','ITC.NS','NESTLEIND.NS','BRITANNIA.NS',
        'MARUTI.NS','TMCV.NS','TITAN.NS','ADANIENT.NS','ASIANPAINT.NS',
        'NTPC.NS','ONGC.NS','COALINDIA.NS','TATASTEEL.NS',
    ]
    earnings_entries = []
    def fetch_earnings_date(sym):
        try:
            cal = yf.Ticker(sym).calendar
            if cal is None or cal.empty:
                return None
            ed = cal.get('Earnings Date')
            if ed is not None and len(ed) > 0:
                date_val = ed[0]
                date_str = str(date_val)[:10] if date_val else None
                if date_str:
                    return {
                        'symbol': sym,
                        'name': sym.replace('.NS','').replace('.BO',''),
                        'earningsDate': date_str,
                    }
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_earnings_date, sym): sym for sym in EARNINGS_STOCKS}
        for future in as_completed(futures):
            result = future.result()
            if result:
                earnings_entries.append(result)

    earnings_entries.sort(key=lambda x: x['earningsDate'])
    save('earnings.json', {'lastUpdated': NOW, 'earnings': earnings_entries})
    print(f"  Saved earnings.json — {len(earnings_entries)} companies with upcoming dates")

    # 10. Conglomerate stats (refreshes autoStats fields from yfinance)
    print("\n[10/10] Conglomerate Stats (yfinance refresh)")
    fetch_conglomerate_stats()

    # Auto-generate tickers.json from COMPANY_SYMBOLS + fetched names/sectors
    # This replaces the old static file — any symbol added to COMPANY_SYMBOLS
    # automatically appears in the Company Spotlight search on the next run.
    print("\n[Auto] Regenerating tickers.json from COMPANY_SYMBOLS…")
    ticker_list = []
    for sym in COMPANY_SYMBOLS:
        info  = info_map.get(sym, {})
        fname = sym.replace('.', '-')          # e.g. ADANIPOWER-NS
        symbol_key = sym.replace('.NS', '').replace('.BO', '')
        long_name  = info.get('longName') or info.get('shortName') or symbol_key
        sector     = info.get('sector', 'Equity')
        ticker_list.append({
            'symbol': symbol_key,
            'name':   long_name,
            'sector': sector,
            'file':   fname,
        })
    # Sort alphabetically by symbol
    ticker_list.sort(key=lambda t: t['symbol'])
    save('tickers.json', ticker_list)
    print(f"  tickers.json: {len(ticker_list)} companies")

    # Meta file (for "last updated" display on site)
    save('meta.json', {'lastUpdated': NOW, 'status': 'ok'})

    # Archive housekeeping — remove snapshots older than 90 days
    print("\n[Archive] Pruning old snapshots…")
    prune_archive()

    print(f"\n✓ All data saved to public/data/")
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    archive_dir = os.path.join(OUT, 'archive', today)
    if os.path.isdir(archive_dir):
        archived = os.listdir(archive_dir)
        print(f"  Archive for today ({today}): {', '.join(archived)}")
    print(f"  Live files: {', '.join(f for f in os.listdir(OUT) if not os.path.isdir(os.path.join(OUT, f)))}\n")

if __name__ == '__main__':
    main()
