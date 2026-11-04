#!/usr/bin/env python3
"""
Two-scanner crypto screener + paper-trading tracker.
  A) DEX scanner  : Solana tokens from DEX Screener -> safety/liquidity filters -> BREAKOUT ENGINE
                    (hourly-bar range breakout confirmed on bar close, invalidation level,
                     R:R targets, 10,000-run "time machine" simulation, real-history replay,
                     0-100 quantitative score, PASS/BLOCK risk gate, risk-based position size)
  B) Majors scanner: established coins on Binance (volume surge, vol/mcap, squeeze,
                     EMA trend, RSI, depth/spread, open interest) - free public data only

Each run (both scanners):
 1. Pulls candidate tokens from DEX Screener's public API
 2. Applies hard filters, then an optional RugCheck safety check
 3. Scores survivors and picks the top few
 4. Shows rupee/dollar position size, stop and target for each pick
 5. Logs picks as PAPER trades and updates earlier paper trades
    (stop-loss, sell half at 2x, trailing stop on the rest, time-expiry)
 6. Saves CSVs and optionally sends a Telegram summary

SCREENING + PAPER TRACKING ONLY. It never places real trades.
Nothing here is financial advice.

Install:  pip install requests
Run:      python dex_screener_bot.py
"""
import csv, json, math, os, random, sys, time, datetime as dt
import requests

# ---------------- CONFIG ----------------
CHAIN = "solana"

# Capital and risk plan (INR 10,000 budget)
CAPITAL_INR = 10_000
POSITION_INR = 1_500        # per slot
MAX_OPEN = 2                # max simultaneous DEX paper positions
STOP_PCT = 25               # stop-loss below entry
TAKE_PROFIT_MULT = 2.0      # sell half at 2x
TRAIL_PCT = 25              # trailing stop on remaining half, from peak
HOLD_DAYS = 7               # time exit for the paper tracker (change to your holding period)
USD_INR_FALLBACK = 88.0     # used only if the live rate can't be fetched

FILTERS = dict(
    min_liquidity=100_000,
    min_mcap=1_000_000,
    max_mcap=50_000_000,
    min_age_hours=72,
    max_age_days=60,
    min_vol24=500_000,
    min_txns24=2_000,
    min_chg24=10, max_chg24=150,
    min_chg6=0,
    min_chg1=-10,
    min_vol_liq=1.0, max_vol_liq=10.0,
    min_buy_sell_ratio=0.95,
)
TOP_N = 3
USE_RUGCHECK = True
RUGCHECK_MAX_SCORE = 5000   # lower raw score = safer; tune after looking at a few reports
TELEGRAM_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TG_CHAT_ID", "")
NOTIFY_ONLY_ON_NEWS = True  # message only for new picks / paper-trade events, plus a daily heartbeat
HEARTBEAT_HOURS = 24        # send a "still running" message at least this often
OUT_DIR = os.getenv("BOT_DATA_DIR") or os.path.dirname(os.path.abspath(__file__))
os.makedirs(OUT_DIR, exist_ok=True)
HEARTBEAT_FILE = os.path.join(OUT_DIR, "last_telegram.txt")
ALERTED_FILE = os.path.join(OUT_DIR, "alerted_picks.json")
ALERT_COOLDOWN_HOURS = 24   # don't re-alert the same token within this window

# ---- Breakout engine for DEX tokens (method adapted from the Day Trader Skill guide) ----
BO_ENABLED = True
GECKO = "https://api.geckoterminal.com/api/v2"      # free hourly candles (about 30 calls/min)
FILTERS_BO = dict(FILTERS, min_chg24=-15, min_chg6=-10,
                  max_mcap=300_000_000,     # run_log: 14-16 liquid tokens/run were rejected as 'mcap too high'
                  max_age_days=180,         # ...and 5-6 as 'age too old'; older tokens also have more history
                  min_vol_liq=0.5)          # mid-caps often trade 0.5-1x their liquidity per day
GT_DISCOVERY = True                         # add trending / top-volume Solana pools from GeckoTerminal   # momentum filters relaxed: a breakout
                                                          # often starts from a quiet range
BO = dict(
    range_bars=24,          # consolidation window = last 24 closed hourly bars
    vol_confirm=1.5,        # breakout bar volume vs average of the window
    near_pct=2.0,           # watchlist: price within this % below the range high
    max_chase_pct=5.0,      # BLOCK if price is already this far above the trigger close
    target_r=2.0,           # main reward target in R (1R and 3R are shown too)
    horizon_hours=48,       # time-machine horizon and paper time-exit
    min_tm_bars=120,        # minimum hourly bars needed for the simulation
    sims=10_000, seed=19,
    candidates=12,          # tokens whose candles are fetched per run
    min_score=60,           # minimum 0-100 score for a signal
)
# ---- Risk manager (applies to paper trades; BLOCK is final for that signal) ----
MAX_RISK_PCT = 20            # invalidation may be at most this far below entry
MAX_LOSS_PER_TRADE_INR = 200 # position is sized so a stop-out loses about this much at most
MAX_DAILY_LOSS_INR = 400
MAX_TRADES_PER_DAY = 2
MIN_RR = 1.5
MIN_RISK_PCT = 2             # invalidation closer than this is inside normal noise (breakout already given back)
MIN_POSITION_INR = 500       # smaller than this = range too wide for the loss rule

# ---- Majors scanner (established coins, Binance public data, no API key) ----
MAJORS_ENABLED = True
MAJORS_MAX_OPEN = 2         # max simultaneous majors paper positions
MAJORS_HOLD_DAYS = 14       # swing horizon for majors (daily-candle signals)
BINANCE = os.getenv("BINANCE_BASE", "https://api.binance.com")
BINANCE_FUT = "https://fapi.binance.com"
COINGECKO = "https://api.coingecko.com/api/v3"
MJ = dict(
    min_quote_vol=10_000_000,   # min 24h USDT volume (liquidity)
    max_spread_pct=0.10,        # max bid-ask spread %
    min_depth_usd=50_000,       # min bid-side depth within 1% of price
    min_history_days=210,       # needed for the 200-day EMA
    vol_surge_min=2.0,          # volume vs 30-day average (hard filter)
    volmcap_min=0.15, volmcap_max=1.5,   # volume / market cap band
    rsi_min=50, rsi_max=75,
    max_candidates=150,
    depth_check_top=15,         # order-book / open-interest checks only for the best N
    min_score=60,               # minimum final score (max about 110)
    top_n=3,
    min_range60=0.05,           # skip pegged/flat assets: 60-day high-low range under 5% (stablecoins)
    rsi_hard_max=75,            # skip overheated coins (RSI above this) instead of just lowering the score
    max_stop_pct=18,            # skip if 2 x ATR exceeds this: a stop capped below normal noise is no real invalidation
)
STABLES = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "USDD", "EUR", "AEUR", "PYUSD",
           "USD1", "XUSD", "USDE", "RLUSD", "BFUSD", "WBTC", "WBETH", "U", "USDG", "USDS", "USD0"}
API = "https://api.dexscreener.com"
HEAD = {"User-Agent": "Mozilla/5.0 (screener-script)"}
PICKS_CSV = os.path.join(OUT_DIR, "picks_log.csv")
TRADES_CSV = os.path.join(OUT_DIR, "paper_trades.csv")
TRADE_FIELDS = ["opened_ts", "opened", "symbol", "mint", "entry_price", "stop_price",
                "target_price", "size_inr", "status", "half_sold", "realized_frac",
                "peak_price", "exit_price", "exit_time", "pnl_pct", "url",
                "kind", "hold_days", "trail_pct", "pool", "checked_ts"]
# ----------------------------------------


_BN_MAIN = "https://api.binance.com"
_BN_MIRROR = "https://data-api.binance.vision"      # market-data-only mirror
_BN_FALLBACK = [False]


def _bn(url):
    if _BN_FALLBACK[0] and url.startswith(_BN_MAIN):
        return _BN_MIRROR + url[len(_BN_MAIN):]
    return url


def get(url, **kw):
    err = None
    for _ in range(3):
        try:
            r = requests.get(_bn(url), headers=HEAD, timeout=20, **kw)
            if r.status_code in (451, 403) and url.startswith(_BN_MAIN) and not _BN_FALLBACK[0]:
                _BN_FALLBACK[0] = True
                print("binance: main host refused this region, switching to the market-data mirror")
                continue
            if r.status_code == 429:
                time.sleep(5); continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            err = e; time.sleep(2)
    print("request failed:", url, err)
    return None


def usd_inr():
    d = get("https://open.er-api.com/v6/latest/USD")
    try:
        return float(d["rates"]["INR"])
    except Exception:
        return USD_INR_FALLBACK


_GT_LAST = [0.0]


def _gt_pace(min_gap=3.0):
    """Keep at least min_gap seconds between ANY two GeckoTerminal calls (free tier is rate-limited)."""
    wait = _GT_LAST[0] + min_gap - time.time()
    if wait > 0:
        time.sleep(wait)
    _GT_LAST[0] = time.time()


def gt_get(url, **kw):
    """GET with pacing, and up to 3 attempts that honour Retry-After on HTTP 429."""
    head = dict(HEAD, Accept="application/json;version=20230302")
    for attempt in range(3):
        _gt_pace()
        r = requests.get(url, headers=head, timeout=20, **kw)
        if r.status_code == 429 and attempt < 2:
            try:
                wait = float(r.headers.get("Retry-After", 0))
            except (TypeError, ValueError):
                wait = 0
            time.sleep(min(max(wait, 15 * (attempt + 1)), 60))
            continue
        r.raise_for_status()
        return r.json()


def gt_discovery():
    """Extra token addresses: GeckoTerminal trending pools and top pools by 24h volume.
    Only the token addresses are used; full pair data still comes from DEX Screener."""
    addrs, ok = set(), 0
    calls = [("trending_pools", {"page": 1}), ("trending_pools", {"page": 2}),
             ("pools", {"page": 1, "sort": "h24_volume_usd_desc"}),
             ("pools", {"page": 2, "sort": "h24_volume_usd_desc"}),
             ("pools", {"page": 3, "sort": "h24_volume_usd_desc"})]
    for name, prm in calls:
        try:
            data = gt_get(f"{GECKO}/networks/{CHAIN}/{name}", params=prm)
            for d in data.get("data", []):
                tid = (((d.get("relationships") or {}).get("base_token") or {}).get("data") or {}).get("id", "")
                if tid.startswith(CHAIN + "_"):
                    addrs.add(tid[len(CHAIN) + 1:])
            ok += 1
        except Exception as e:
            code = getattr(getattr(e, "response", None), "status_code", "")
            print("discovery failed:", name, type(e).__name__, code)
    print(f"discovery: {len(addrs)} extra tokens from GeckoTerminal ({ok}/{len(calls)} calls ok)")
    return addrs


def candidate_tokens():
    addrs = set()
    for ep in ("/token-boosts/top/v1", "/token-boosts/latest/v1", "/token-profiles/latest/v1"):
        for d in (get(API + ep) or []):
            if d.get("chainId") == CHAIN and d.get("tokenAddress"):
                addrs.add(d["tokenAddress"])
    for q in ("SOL", "USDC", "meme"):
        data = get(f"{API}/latest/dex/search", params={"q": q}) or {}
        for p in data.get("pairs", []):
            if p.get("chainId") == CHAIN:
                addrs.add(p["baseToken"]["address"])
    if GT_DISCOVERY:
        addrs |= gt_discovery()
    return list(addrs)


def fetch_pairs(addrs):
    pairs = []
    for i in range(0, len(addrs), 30):
        chunk = ",".join(addrs[i:i + 30])
        data = get(f"{API}/tokens/v1/{CHAIN}/{chunk}")
        if isinstance(data, list):
            pairs += data
        time.sleep(0.3)
    best = {}
    for p in pairs:
        a = p["baseToken"]["address"]
        liq = (p.get("liquidity") or {}).get("usd", 0) or 0
        if a not in best or liq > ((best[a].get("liquidity") or {}).get("usd", 0) or 0):
            best[a] = p
    return list(best.values())


def passes(p, f=FILTERS):
    liq = (p.get("liquidity") or {}).get("usd", 0) or 0
    mcap = p.get("marketCap") or p.get("fdv") or 0
    created = p.get("pairCreatedAt")
    if not created:
        return False, "no age"
    age_h = (time.time() * 1000 - created) / 3.6e6
    vol24 = (p.get("volume") or {}).get("h24", 0) or 0
    tx = (p.get("txns") or {}).get("h24", {})
    buys, sells = tx.get("buys", 0), tx.get("sells", 0)
    pc = p.get("priceChange") or {}
    c24, c6, c1 = pc.get("h24", 0) or 0, pc.get("h6", 0) or 0, pc.get("h1", 0) or 0
    vl = vol24 / liq if liq else 0
    bs = buys / sells if sells else 9

    checks = [
        (liq >= f["min_liquidity"], "liquidity"),
        (mcap >= f["min_mcap"], "mcap too low"),
        (mcap <= f["max_mcap"], "mcap too high"),
        (age_h >= f["min_age_hours"], "age too young"),
        (age_h <= f["max_age_days"] * 24, "age too old"),
        (vol24 >= f["min_vol24"], "volume"),
        (buys + sells >= f["min_txns24"], "txns"),
        (c24 >= f["min_chg24"], "24h change too low"),
        (c24 <= f["max_chg24"], "24h change too high"),
        (c6 >= f["min_chg6"], "6h change"),
        (c1 >= f["min_chg1"], "1h change"),
        (vl >= f["min_vol_liq"], "vol/liq too low"),
        (vl <= f["max_vol_liq"], "vol/liq too high"),
        (bs >= f["min_buy_sell_ratio"], "buy/sell"),
    ]
    for ok, name in checks:
        if not ok:
            return False, name
    p["_m"] = dict(liq=liq, mcap=mcap, age_h=age_h, vol24=vol24, txns=buys + sells,
                   c24=c24, c6=c6, c1=c1, vl=vl, bs=bs)
    return True, ""


def rugcheck_ok(mint):
    """Best-effort safety check. Fails open with a warning note if the service is unavailable."""
    try:
        r = requests.get(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary",
                         headers=HEAD, timeout=15)
        if r.status_code != 200:
            return True, "rugcheck unavailable - CHECK MANUALLY"
        d = r.json()
        score = d.get("score", 0)
        risks = [x.get("name") for x in d.get("risks", []) if x.get("level") in ("danger", "high")]
        if risks or score > RUGCHECK_MAX_SCORE:
            return False, f"rugcheck risks: {risks or score}"
        return True, "rugcheck ok"
    except Exception:
        return True, "rugcheck error - CHECK MANUALLY"


def score(m):
    s = 0
    s += min(m["c6"], 40) * 0.5
    s += min(m["liq"] / 100_000, 10) * 2
    s += min(m["bs"], 1.5) * 10
    s -= abs(m["vl"] - 3) * 1.5
    s -= max(0, m["c24"] - 100) * 0.2
    return round(s, 2)


# ---------- paper trading ----------
def load_trades():
    if not os.path.exists(TRADES_CSV):
        return []
    with open(TRADES_CSV, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def save_trades(trades):
    with open(TRADES_CSV, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=TRADE_FIELDS)
        w.writeheader()
        w.writerows(trades)


def update_trades(trades, prices):
    """Apply stop / take-profit / trailing / expiry rules using the latest prices.
    Runs only when the script runs, so intra-interval spikes and dips are missed."""
    now = time.time()
    events = []
    for t in trades:
        if t["status"] != "OPEN":
            continue
        px = prices.get(t["mint"])
        if not px:
            continue
        hold = float(t.get("hold_days") or HOLD_DAYS)
        trail = float(t.get("trail_pct") or TRAIL_PCT)
        entry = float(t["entry_price"])
        half = t["half_sold"] == "1"
        realized = float(t["realized_frac"] or 0)
        peak = max(float(t["peak_price"] or entry), px)
        t["peak_price"] = peak
        remaining = 0.5 if half else 1.0
        ret = px / entry - 1

        reason = None
        if not half and px <= float(t["stop_price"]):
            reason = "STOP"
        elif not half and px >= float(t["target_price"]):
            t["half_sold"] = "1"
            t["realized_frac"] = round(0.5 * ret, 4)
            events.append(f"{t['symbol']}: hit target ${float(t['target_price']):.6g} - paper-sold half, trailing the rest")
        elif half and px <= peak * (1 - trail / 100):
            reason = "TRAIL"
        elif now - float(t["opened_ts"]) > hold * 86400:
            reason = "EXPIRED"

        if reason:
            pnl = realized + remaining * ret
            t["status"] = reason
            t["exit_price"] = px
            t["exit_time"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
            t["pnl_pct"] = round(pnl * 100, 1)
            events.append(f"{t['symbol']}: closed ({reason}) at {t['pnl_pct']}%")
    return events


def open_paper_trades(trades, picks, stamp, kind="dex"):
    open_rows = [t for t in trades if t["status"] == "OPEN"]
    open_mints = {t["mint"] for t in open_rows}
    cap = MAJORS_MAX_OPEN if kind == "major" else MAX_OPEN
    slots = cap - sum(1 for t in open_rows if (t.get("kind") or "dex") == kind)
    new = []
    for r in picks:
        if slots <= 0:
            break
        if r["mint"] in open_mints:
            continue
        entry = float(r["price"])
        if "stop" in r:                      # engine-built plan (breakout / majors)
            stop, target = r["stop"], r["target"]
            hold = r.get("hold_days") or (MAJORS_HOLD_DAYS if kind == "major" else HOLD_DAYS)
            trail = r["trail_pct"]
            size = r.get("size_inr", POSITION_INR)
        else:                                # legacy fixed-percent plan
            stop, target = entry * (1 - STOP_PCT / 100), entry * TAKE_PROFIT_MULT
            hold, trail, size = HOLD_DAYS, TRAIL_PCT, POSITION_INR
        trades.append(dict(
            opened_ts=time.time(), opened=stamp, symbol=r["symbol"], mint=r["mint"],
            entry_price=entry, stop_price=stop, target_price=target,
            size_inr=size, status="OPEN", half_sold="0", realized_frac="0",
            peak_price=entry, exit_price="", exit_time="", pnl_pct="", url=r["url"],
            kind=kind, hold_days=hold, trail_pct=trail, pool=r.get("pool", ""),
            checked_ts=math.ceil(time.time() / 3600) * 3600))
        new.append(r["symbol"])
        slots -= 1
    return new


def replay_trade(t, bars):
    """Apply stop / target / trailing stop / time-exit using candle HIGHS and LOWS since the last
    check, so hours when the PC was off can't hide a stop or target hit. Inside one bar the low is
    assumed to come before the high (pessimistic). Gaps fill at the bar's open, not at the level."""
    entry = float(t["entry_price"])
    stop, target = float(t["stop_price"]), float(t["target_price"])
    hold = float(t.get("hold_days") or HOLD_DAYS)
    trail = float(t.get("trail_pct") or TRAIL_PCT)
    half = t["half_sold"] == "1"
    realized = float(t["realized_frac"] or 0)
    peak = float(t["peak_price"] or entry)
    opened = float(t["opened_ts"])
    start = float(t.get("checked_ts") or 0) or math.ceil(opened / 3600) * 3600
    events, last_ts = [], None

    def close(reason, px, ts):
        pnl = realized + (0.5 if half else 1.0) * (px / entry - 1)
        t.update(status=reason, exit_price=px, pnl_pct=round(pnl * 100, 1),
                 exit_time=dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M"))
        events.append(f"{t['symbol']}: closed ({reason}) at {t['pnl_pct']}% [from candle data, "
                      f"{t['exit_time']}]")

    for b in bars:
        ts, o, h, l = b[0], b[1], b[2], b[3]
        if ts < start:
            continue
        last_ts = ts
        if ts >= opened + hold * 86400:
            close("EXPIRED", o, ts)
            break
        if not half:
            if l <= stop:
                close("STOP", min(stop, o), ts)
                break
            if h >= target:
                half, realized = True, 0.5 * (max(target, o) / entry - 1)
                peak = max(peak, h)
                events.append(f"{t['symbol']}: hit target ${target:.6g} at "
                              f"{dt.datetime.fromtimestamp(ts):%Y-%m-%d %H:%M} (candle high) - paper-sold half, trailing the rest")
        else:
            trig = peak * (1 - trail / 100)
            if l <= trig:
                close("TRAIL", min(trig, o), ts)
                break
            peak = max(peak, h)
    t["half_sold"] = "1" if half else "0"
    t["realized_frac"] = round(realized, 4)
    t["peak_price"] = peak
    if t["status"] == "OPEN" and last_ts is not None:
        t["checked_ts"] = last_ts + 3600
    return events


def binance_hourly(symbol):
    k = get(BINANCE + "/api/v3/klines", params=dict(symbol=symbol, interval="1h", limit=500))
    if not isinstance(k, list):
        return None
    now = time.time() * 1000
    return [[x[0] / 1000, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])]
            for x in k if x[6] < now]


def paper_summary(trades):
    closed = [t for t in trades if t["status"] != "OPEN"]
    open_n = len(trades) - len(closed)
    if not closed:
        return f"Paper trades: {open_n} open, none closed yet."
    pnls = [float(t["pnl_pct"]) for t in closed]
    wins = sum(1 for p in pnls if p > 0)
    return (f"Paper results: {len(closed)} closed, win rate {wins / len(closed):.0%}, "
            f"avg {sum(pnls) / len(pnls):.1f}% per trade, {open_n} open.")


def load_alerted():
    try:
        with open(ALERTED_FILE, encoding="utf-8") as fh:
            return {k: float(v) for k, v in json.load(fh).items()}
    except Exception:
        return {}


def save_alerted(d):
    cutoff = time.time() - 7 * 86400
    d = {k: v for k, v in d.items() if v > cutoff}
    with open(ALERTED_FILE, "w", encoding="utf-8") as fh:
        json.dump(d, fh)


def holding_lines(trades, prices):
    """One status block per open paper position: entry, now, P/L, stop, target."""
    out = []
    now = time.time()
    for t in trades:
        if t["status"] != "OPEN":
            continue
        entry = float(t["entry_price"])
        px = prices.get(t["mint"])
        age_d = (now - float(t["opened_ts"])) / 86400
        hold = float(t.get("hold_days") or HOLD_DAYS)
        trail = float(t.get("trail_pct") or TRAIL_PCT)
        tag = "major" if (t.get("kind") or "dex") == "major" else "DEX"
        head = f"{t['symbol']} (HELD, paper, {tag}) day {age_d:.1f}/{hold:g}"
        if px is None:
            out.append(f"{head}\nentry ${entry:.6g} | current price unavailable")
            continue
        pnl = (px / entry - 1) * 100
        target = float(t["target_price"])
        if t["half_sold"] == "1":
            peak = float(t["peak_price"])
            plan = (f"half already sold at target | trailing stop "
                    f"${peak * (1 - trail / 100):.6g} (peak ${peak:.6g})")
        else:
            plan = (f"stop ${float(t['stop_price']):.6g} | sell half at "
                    f"${target:.6g} (+{(target / entry - 1) * 100:.0f}%)")
        out.append(f"{head}\nentry ${entry:.6g} -> now ${px:.6g} ({pnl:+.1f}%)\n{plan}")
    return out


# ======================= MAJORS SCANNER (Binance public data) =======================
def get_once(url, **kw):
    """Single quiet attempt (used where failure is normal, e.g. coins without futures)."""
    try:
        r = requests.get(_bn(url), headers=HEAD, timeout=15, **kw)
        if r.status_code in (451, 403) and url.startswith(_BN_MAIN) and not _BN_FALLBACK[0]:
            _BN_FALLBACK[0] = True
            r = requests.get(_bn(url), headers=HEAD, timeout=15, **kw)
        return r.json() if r.ok else None
    except Exception:
        return None


def ema(vals, n):
    k = 2 / (n + 1)
    e = sum(vals[:n]) / n
    out = [None] * (n - 1) + [e]
    for v in vals[n:]:
        e = v * k + e * (1 - k)
        out.append(e)
    return out


def rsi_series(closes, n=14):
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0); losses += max(-d, 0)
    ag, al = gains / n, losses / n
    out = [None] * n + [100.0 if al == 0 else 100 - 100 / (1 + ag / al)]
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n
        al = (al * (n - 1) + max(-d, 0)) / n
        out.append(100.0 if al == 0 else 100 - 100 / (1 + ag / al))
    return out


def bb_width_series(closes, n=20):
    out = [None] * (n - 1)
    for i in range(n - 1, len(closes)):
        w = closes[i - n + 1:i + 1]
        m = sum(w) / n
        sd = (sum((x - m) ** 2 for x in w) / n) ** 0.5
        out.append(4 * sd / m if m else None)   # (upper - lower) / middle band
    return out


def atr_value(h, l, c, n=14):
    trs = [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, len(c))]
    a = sum(trs[:n]) / n
    for x in trs[n:]:
        a = (a * (n - 1) + x) / n
    return a


def bearish_divergence(c, r):
    """Rough check: price makes a higher high over the last 10 days vs the 10 before,
    while RSI at that high is clearly lower. A heuristic, not a guarantee."""
    a, b = c[-10:], c[-20:-10]
    ia = max(range(len(a)), key=lambda i: a[i])
    ib = max(range(len(b)), key=lambda i: b[i])
    ra, rb = r[-10 + ia], r[-20 + ib]
    if ra is None or rb is None:
        return False
    return a[ia] > b[ib] * 1.005 and ra < rb - 3


def majors_universe():
    tick = get(BINANCE + "/api/v3/ticker/24hr")
    book = get(BINANCE + "/api/v3/ticker/bookTicker")
    if not isinstance(tick, list) or not isinstance(book, list):
        return None
    bk = {b["symbol"]: b for b in book}
    out = []
    for t in tick:
        sym = t["symbol"]
        if not sym.endswith("USDT"):
            continue
        base = sym[:-4]
        if base in STABLES:
            continue
        try:
            qv = float(t["quoteVolume"])
            b = bk.get(sym)
            if qv < MJ["min_quote_vol"] or not b:
                continue
            bid, ask = float(b["bidPrice"]), float(b["askPrice"])
            if bid <= 0 or ask <= 0:
                continue
            spread = (ask - bid) / ((ask + bid) / 2) * 100
            if spread > MJ["max_spread_pct"]:
                continue
            out.append(dict(symbol=sym, base=base, price=float(t["lastPrice"]),
                            qv=qv, spread=spread))
        except (KeyError, ValueError):
            continue
    out.sort(key=lambda x: x["qv"], reverse=True)
    return out[:MJ["max_candidates"]]


def coingecko_caps():
    """symbol -> (market cap, global 24h volume). Highest market cap wins symbol clashes."""
    caps = {}
    for page in (1, 2):
        data = get_once(COINGECKO + "/coins/markets",
                        params=dict(vs_currency="usd", order="market_cap_desc",
                                    per_page=250, page=page, sparkline="false"))
        if not isinstance(data, list):
            break
        for d in data:
            sym = str(d.get("symbol", "")).upper()
            if sym and sym not in caps and d.get("market_cap"):
                caps[sym] = (float(d["market_cap"]), float(d.get("total_volume") or 0))
        time.sleep(1.5)
    return caps


def analyze_major(u, caps):
    """Daily-candle analysis. Returns None if a hard filter fails."""
    k = get(BINANCE + "/api/v3/klines",
            params=dict(symbol=u["symbol"], interval="1d", limit=250))
    if not isinstance(k, list) or len(k) < MJ["min_history_days"]:
        return None
    h = [float(x[2]) for x in k]
    l = [float(x[3]) for x in k]
    c = [float(x[4]) for x in k]
    qv = [float(x[7]) for x in k]

    if (max(c[-60:]) - min(c[-60:])) / c[-1] < MJ["min_range60"]:      # pegged or flat: cannot reach a 2R target
        return None

    # volume surge vs 30-day average (baseline excludes the last 4 completed days + today)
    base = qv[-35:-5]
    avg30 = sum(base) / len(base)
    if avg30 <= 0:
        return None
    frac = (time.time() * 1000 - k[-1][0]) / 86_400_000       # share of today's candle elapsed
    ratio = max(qv[-2] / avg30, (qv[-1] / frac) / avg30 if frac > 0.25 else 0)
    if ratio < MJ["vol_surge_min"]:
        return None
    exp3 = (sum(qv[-4:-1]) / 3) / avg30                         # multi-day expansion

    e20, e50, e200 = ema(c, 20), ema(c, 50), ema(c, 200)
    if e200[-1] is None or c[-1] <= e200[-1]:                   # hard: above 200 EMA
        return None
    rs = rsi_series(c)
    rsi_now = rs[-1]
    if rsi_now is None or rsi_now < MJ["rsi_min"] or rsi_now > MJ["rsi_hard_max"]:   # hard: 50 <= RSI <= 75
        return None

    pts, notes = 0, []
    pts += 30 if ratio >= 3 else 22
    notes.append(f"volume {ratio:.1f}x 30d avg")
    if exp3 >= 1.5:
        pts += 10; notes.append(f"3-day volume expansion {exp3:.1f}x")

    mcap, gvol = caps.get(u["base"], (None, None))
    vm = (gvol / mcap) if mcap and gvol else None
    if vm is not None:
        if MJ["volmcap_min"] <= vm <= MJ["volmcap_max"]:
            pts += 15; notes.append(f"vol/mcap {vm:.2f}")
        elif vm > MJ["volmcap_max"]:
            pts -= 10
            notes.append(f"vol/mcap {vm:.2f} very high (hype/wash risk)")

    bw = bb_width_series(c)
    hist = [x for x in bw[-120:] if x is not None]
    recent_min = min(x for x in bw[-10:] if x is not None)
    rank = sum(1 for x in hist if x <= recent_min) / len(hist)
    if rank <= 0.20:
        pts += 15; notes.append("recent Bollinger squeeze (breakout from compression)")

    if e20[-1] > e50[-1]:
        pts += 10; notes.append("20 EMA above 50 EMA")
        if any(e20[i - 1] <= e50[i - 1] and e20[i] > e50[i] for i in range(len(c) - 5, len(c))):
            pts += 5; notes.append("fresh 20/50 cross")
    if rsi_now <= MJ["rsi_max"]:
        pts += 10
    else:
        pts -= 5; notes.append(f"RSI {rsi_now:.0f} overheated")
    if bearish_divergence(c, rs):
        pts -= 15; notes.append("possible bearish RSI divergence")

    price = u["price"]
    raw_sd = 2 * atr_value(h, l, c) / c[-1]
    if raw_sd > MJ["max_stop_pct"] / 100:                        # too volatile for a meaningful stop
        return None
    sd = min(max(raw_sd, 0.06), 0.18)                            # stop distance: 2 x ATR, 6-18%
    return dict(
        symbol=u["base"], mint=u["symbol"], price=price, score=pts, notes=notes,
        ratio=ratio, vm=vm, mcap=mcap, rsi=rsi_now, spread=u["spread"],
        chg3=(c[-1] / c[-4] - 1) * 100, stop=price * (1 - sd), target=price * (1 + 2 * sd),
        chg7=(c[-1] / c[-8] - 1) * 100, ext20=(c[-1] / e20[-1] - 1) * 100,
        ext200=(c[-1] / e200[-1] - 1) * 100, stop_pct=sd * 100,
        up_day=bool(c[-2] > k_open(k, -2)),
        trail_pct=round(sd * 100, 1), depth=None, oi=None,
        url=f"https://www.tradingview.com/chart/?symbol=BINANCE:{u['symbol']}")


def depth_stats(symbol, mid):
    d = get_once(BINANCE + "/api/v3/depth", params=dict(symbol=symbol, limit=500))
    if not d or "bids" not in d:
        return None
    lo, hi = mid * 0.99, mid * 1.01
    bid = sum(float(p) * float(q) for p, q in d["bids"] if float(p) >= lo)
    ask = sum(float(p) * float(q) for p, q in d["asks"] if float(p) <= hi)
    return bid, ask


def oi_change(symbol, days=3):
    d = get_once(BINANCE_FUT + "/futures/data/openInterestHist",
                 params=dict(symbol=symbol, period="1d", limit=days + 2))
    if not isinstance(d, list) or len(d) < days + 1:
        return None
    a, b = float(d[-1 - days]["sumOpenInterestValue"]), float(d[-1]["sumOpenInterestValue"])
    return (b / a - 1) * 100 if a else None


def major_prices(symbols):
    out = {}
    if not symbols:
        return out
    d = get(BINANCE + "/api/v3/ticker/price",
            params={"symbols": json.dumps(symbols, separators=(",", ":"))})
    for x in d if isinstance(d, list) else []:
        try:
            out[x["symbol"]] = float(x["price"])
        except (KeyError, ValueError):
            pass
    return out


def scan_majors():
    """Returns a list of picks (maybe empty), or None if Binance data is unreachable."""
    uni = majors_universe()
    if uni is None:
        return None
    caps = coingecko_caps()
    if not caps:
        print("majors: CoinGecko unavailable - vol/mcap criterion skipped")
    cands = []
    for u in uni:
        try:
            r = analyze_major(u, caps)
        except Exception as e:
            print("majors: skip", u["symbol"], type(e).__name__)
            continue
        if r:
            cands.append(r)
        time.sleep(0.12)
    print(f"majors: {len(uni)} liquid coins checked, {len(cands)} passed trend/volume filters")
    cands.sort(key=lambda x: x["score"], reverse=True)

    final, oi_fail = [], 0
    for r in cands[:MJ["depth_check_top"]]:
        ds = depth_stats(r["mint"], r["price"])
        if ds is None:
            continue
        bid, ask = ds
        if bid < MJ["min_depth_usd"]:
            continue
        r["depth"] = bid
        if ask > 0 and bid >= 1.2 * ask:
            r["score"] += 5; r["notes"].append("buy-side depth heavier than sell-side")
        if oi_fail < 5:
            oc = oi_change(r["mint"])
            if oc is None:
                oi_fail += 1
            else:
                oi_fail = 0
                r["oi"] = oc
                if oc >= 5 and r["chg3"] > 0:
                    r["score"] += 10; r["notes"].append(f"open interest +{oc:.0f}% with rising price")
        if r["score"] >= MJ["min_score"]:
            final.append(r)
    final.sort(key=lambda x: x["score"], reverse=True)
    return final[:MJ["top_n"]]


MAJORS_CSV = os.path.join(OUT_DIR, "majors_picks_log.csv")


def k_open(klines, i):
    """Open price of the i-th daily candle from the end."""
    return float(klines[i][1])


def log_majors(picks, stamp):
    if not picks:
        return
    new_cols = ["time", "symbol", "price", "score", "vol_ratio", "vol_mcap", "rsi",
                "spread_pct", "bid_depth_usd", "oi_change_pct", "chg3_pct", "chg7_pct",
                "ext20_pct", "ext200_pct", "stop_pct", "yday_up", "notes"]
    if os.path.exists(MAJORS_CSV):               # keep an older-format log instead of mixing columns
        with open(MAJORS_CSV, encoding="utf-8") as fh:
            first = fh.readline()
        if "ext20_pct" not in first:
            os.replace(MAJORS_CSV, MAJORS_CSV.replace(".csv", "_old.csv"))
    new_file = not os.path.exists(MAJORS_CSV)
    with open(MAJORS_CSV, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if new_file:
            w.writerow(new_cols)
        for r in picks:
            w.writerow([stamp, r["symbol"], r["price"], r["score"], round(r["ratio"], 2),
                        "" if r["vm"] is None else round(r["vm"], 3), round(r["rsi"], 1),
                        round(r["spread"], 3), "" if r["depth"] is None else round(r["depth"]),
                        "" if r["oi"] is None else round(r["oi"], 1), round(r["chg3"], 1),
                        round(r.get("chg7", 0), 1), round(r.get("ext20", 0), 1),
                        round(r.get("ext200", 0), 1), round(r.get("stop_pct", 0), 1),
                        int(bool(r.get("up_day"))), "; ".join(r["notes"])])


# ======================= BREAKOUT ENGINE (DEX tokens) =======================
def fill_gaps(bars):
    """GeckoTerminal omits hours with no trades. Insert flat, zero-volume bars (flag 1 in slot 6)
    so that 24 bars = 24 hours. A gap longer than 72 hours is treated as a break in history."""
    if not bars:
        return bars
    out = [bars[0]]
    for b in bars[1:]:
        prev = out[-1]
        if (b[0] - prev[0]) / 3600 > 72:
            out = [b]
            continue
        t = prev[0] + 3600
        while t < b[0]:
            pc = prev[4]
            out.append([t, pc, pc, pc, pc, 0.0, 1])
            prev = out[-1]
            t += 3600
        out.append(b)
    return out


def gt_ohlcv(pool, limit=500):
    """Closed hourly candles [ts, o, h, l, c, v], oldest first, from GeckoTerminal."""
    try:
        data = gt_get(f"{GECKO}/networks/{CHAIN}/pools/{pool}/ohlcv/hour",
                      params=dict(aggregate=1, limit=limit, currency="usd"))
        lst = data["data"]["attributes"]["ohlcv_list"]
        bars = sorted([float(x[0]), float(x[1]), float(x[2]), float(x[3]),
                       float(x[4]), float(x[5])] for x in lst)
        now = time.time()
        return fill_gaps([b for b in bars if b[0] + 3600 <= now])       # closed bars only
    except Exception as e:
        code = getattr(getattr(e, "response", None), "status_code", "")
        print("candles failed:", str(pool)[:8], type(e).__name__, code)
        return None


def analyze_breakout(bars):
    """Range breakout on the latest CLOSED hourly bar. A poke above the line does not count:
    the bar must close above the range high, on confirming volume, as the first such close."""
    n = BO["range_bars"]
    if not bars or len(bars) < n + 15:
        return None
    L = len(bars) - 1
    win = bars[L - n:L]
    if sum(1 for b in win if len(b) < 7 or not b[6]) < 14:      # traded in fewer than 14 of 24 hours
        return None
    hi, lo = max(b[2] for b in win), min(b[3] for b in win)
    mid = (hi + lo) / 2
    close = bars[L][4]
    avgv = sum(b[5] for b in win) / n
    volx = bars[L][5] / avgv if avgv > 0 else 0
    tail = bars[-40:]
    atr = atr_value([b[2] for b in tail], [b[3] for b in tail], [b[4] for b in tail])
    closes = [b[4] for b in bars[-72:]]
    e20 = ema(closes, 20) if len(closes) >= 20 else [None]
    a = dict(hi=hi, lo=lo, mid=mid, close=close, volx=volx, atr=atr,
             width_pct=(hi - lo) / mid * 100 if mid else 0,
             ema20=e20[-1], ema20_prev=e20[-4] if len(e20) > 4 else None,
             last_ts=bars[L][0], state=None)
    a["age"] = 0
    if close > hi and bars[L - 1][4] <= hi:
        a["state"] = "TRIGGERED" if volx >= BO["vol_confirm"] else "WEAK"
    else:
        k = L - 1                        # breakout that fired one bar ago and is still holding
        if k - n >= 0:
            win2 = bars[k - n:k]
            hi2, lo2 = max(b[2] for b in win2), min(b[3] for b in win2)
            mid2 = (hi2 + lo2) / 2
            avg2 = sum(b[5] for b in win2) / n
            vx2 = bars[k][5] / avg2 if avg2 > 0 else 0
            if (sum(1 for b in win2 if len(b) < 7 or not b[6]) >= 14 and bars[k][4] > hi2
                    and bars[k - 1][4] <= hi2 and vx2 >= BO["vol_confirm"] and close > hi2 and close > mid2):
                a.update(hi=hi2, lo=lo2, mid=mid2, volx=vx2, state="TRIGGERED", age=1,
                         width_pct=(hi2 - lo2) / mid2 * 100 if mid2 else 0)
        if a["state"] is None and hi - min(hi * BO["near_pct"] / 100, 0.25 * (hi - lo)) <= close <= hi:
            a["state"] = "WATCH"         # within 2% of the high, and in the top quarter of the range
    return a


def tm_verdict(p_hit, r_mult):
    """Same reading rules as the guide: break-even share of target-first = 1/(1+R)."""
    diff = (p_hit - 1 / (1 + r_mult)) * 100
    ad = abs(diff)
    if ad <= 5:
        return ("close to a coin flip" if r_mult == 1 else "close to break-even"), "even", "even"
    side = "target" if diff > 0 else "stop"
    return (f"leans toward the {side}" if ad <= 15 else f"strongly toward the {side}"), side, \
        ("lean" if ad <= 15 else "strong")


def time_machine(bars, entry, stop, target, r_mult, start_hour):
    """Replays the next horizon_hours 10,000 times. Each bar is copied from a random past bar at
    the same hour of day, with the token's historical drift removed so a past pump doesn't
    flatter the odds. If stop and target fall in the same bar, the stop counts first."""
    H = BO["horizon_hours"]
    rel = [(int((b[0] // 3600) % 24), b[2] / b[1], b[3] / b[1], b[4] / b[1])
           for b in bars if b[1] > 0 and b[2] >= b[3] > 0 and b[4] > 0]
    if len(rel) < BO["min_tm_bars"]:
        return None
    adj = math.exp(-sum(math.log(x[3]) for x in rel) / len(rel))   # remove average drift
    pool, allp = {h: [] for h in range(24)}, []
    for hh, rh, rl, rc in rel:
        c2 = rc * adj
        t = (max(rh * adj, c2, 1.0), min(rl * adj, c2, 1.0), c2)
        pool[hh].append(t)
        allp.append(t)
    rnd = random.Random(BO["seed"])
    risk = entry - stop
    T = S = O = 0
    sum_r = 0.0
    ends = []
    for _ in range(BO["sims"]):
        cur, res = entry, None
        for j in range(H):
            lst = pool[(start_hour + j) % 24] or allp
            rh, rl, rc = lst[int(rnd.random() * len(lst))]
            if cur * rl <= stop:
                res = "S"; cur = stop; break
            if cur * rh >= target:
                res = "T"; cur = target; break
            cur *= rc
        if res == "S":
            S += 1; sum_r -= 1
        elif res == "T":
            T += 1; sum_r += r_mult
        else:
            O += 1; sum_r += (cur - entry) / risk
        ends.append(cur)
    N = BO["sims"]
    ends.sort()
    p_hit = T / (T + S) if (T + S) else 0.0
    resolved = (T + S) / N
    if resolved < 0.25:      # almost no replay reaches the stop or the target: odds are meaningless
        text, side, level = (f"inconclusive: only {resolved * 100:.0f}% of replays reach the stop "
                             f"or target within {H}h", "even", "even")
    else:
        text, side, level = tm_verdict(p_hit, r_mult)
    return dict(p_t=T / N, p_s=S / N, p_o=O / N, exp_r=sum_r / N, p_hit=p_hit, resolved=resolved,
                verdict=text, side=side, level=level,
                p05=ends[int(N * 0.05)], p50=ends[N // 2], p95=ends[int(N * 0.95)],
                sessions=len(rel))


def history_replay(bars, r_mult):
    """Real (not simulated) history: every past time the same rule fired on this token."""
    n, H = BO["range_bars"], BO["horizon_hours"]
    fired = T = S = O = 0
    i = n + 1
    while i < len(bars) - H - 1:
        win = bars[i - n:i]
        hi, lo = max(b[2] for b in win), min(b[3] for b in win)
        mid = (hi + lo) / 2
        avgv = sum(b[5] for b in win) / n
        b0 = bars[i]
        if (avgv > 0 and b0[4] > hi and bars[i - 1][4] <= hi and b0[5] / avgv >= BO["vol_confirm"]
                and b0[4] > mid and (b0[4] - mid) / b0[4] * 100 <= MAX_RISK_PCT):
            entry, stop = b0[4], mid
            target = entry + r_mult * (entry - stop)
            res = "O"
            for fb in bars[i + 1:i + 1 + H]:
                if fb[3] <= stop:
                    res = "S"; break
                if fb[2] >= target:
                    res = "T"; break
            fired += 1
            T += res == "T"; S += res == "S"; O += res == "O"
            i += 6
            continue
        i += 1
    return dict(fired=fired, T=T, S=S, O=O)


def size_for(risk_frac):
    """Risk-based sizing: never lose more than MAX_LOSS_PER_TRADE_INR at the stop."""
    if risk_frac <= 0:
        return 0
    return int(min(POSITION_INR, MAX_LOSS_PER_TRADE_INR / risk_frac) // 50 * 50)


def today_stats(trades):
    today = dt.date.today().strftime("%Y-%m-%d")
    opened = sum(1 for t in trades if str(t.get("opened", "")).startswith(today))
    lost = 0.0
    for t in trades:
        if t["status"] != "OPEN" and str(t.get("exit_time", "")).startswith(today) and t.get("pnl_pct"):
            pnl = float(t["size_inr"]) * float(t["pnl_pct"]) / 100
            if pnl < 0:
                lost += -pnl
    return opened, lost


def risk_check(sig, trades):
    """PASS or BLOCK with reasons. A BLOCK is final for that signal."""
    blocks, warns = [], []
    if not sig["stop"] < sig["entry"]:
        blocks.append("no valid stop below entry")
    if sig["risk_frac"] * 100 > MAX_RISK_PCT:
        blocks.append(f"invalidation {sig['risk_frac'] * 100:.1f}% below entry exceeds the {MAX_RISK_PCT}% limit")
    if sig["risk_frac"] * 100 < MIN_RISK_PCT:
        blocks.append(f"invalidation only {sig['risk_frac'] * 100:.1f}% away: inside normal noise, price has given back the breakout")
    if sig.get("failed"):
        blocks.append("price has fallen back below the range high: the breakout is not holding")
    if sig["rr"] < MIN_RR:
        blocks.append(f"R:R {sig['rr']:.1f} below minimum {MIN_RR}")
    if sig["size_inr"] < MIN_POSITION_INR:
        blocks.append(f"range too wide: max-loss rule would shrink the position below Rs {MIN_POSITION_INR}")
    opened_today, lost_today = today_stats(trades)
    if opened_today >= MAX_TRADES_PER_DAY:
        blocks.append(f"already {opened_today} trades today (limit {MAX_TRADES_PER_DAY})")
    if lost_today >= MAX_DAILY_LOSS_INR:
        blocks.append(f"daily loss limit reached (Rs {lost_today:,.0f} of Rs {MAX_DAILY_LOSS_INR})")
    deployed = sum(float(t["size_inr"]) for t in trades if t["status"] == "OPEN")
    deployable = (MAX_OPEN + MAJORS_MAX_OPEN) * POSITION_INR
    if deployed + sig["size_inr"] > deployable:
        blocks.append(f"cash rule: Rs {deployed:,.0f} already deployed of Rs {deployable:,} allowed")
    if sig.get("chase"):
        blocks.append(f"price already >{BO['max_chase_pct']:.0f}% above the trigger close (chasing)")
    tm = sig.get("tm")
    if tm is None:
        if sig.get("kind") != "major":
            warns.append("no simulation (token history too short)")
    elif tm["side"] == "stop" and tm["level"] == "strong":
        blocks.append(f"time machine {tm['verdict']}")
    elif tm["side"] == "stop":
        warns.append(f"time machine {tm['verdict']}")
    if sig.get("no_socials"):
        warns.append("no website/socials on DEX Screener profile")
    return ("BLOCK" if blocks else "PASS"), blocks, warns


def bo_score(a, m, entry, risk_frac, tm):
    """0-100 quantitative score with visible components."""
    parts = {}
    atr = a["atr"] or 1e-12
    parts["Brk"] = round(min(max((a["close"] - a["hi"]) / atr, 0), 1.0) * 15, 1)
    parts["Vol"] = round(min(max((a["volx"] - 1.0) / 2.0, 0), 1.0) * 20, 1)
    w = a["width_pct"] / 100 * a["mid"] / atr                      # range width in ATRs
    parts["Cmp"] = 10 if w <= 6 else 6 if w <= 10 else 3 if w <= 15 else 0
    e, ep = a.get("ema20"), a.get("ema20_prev")
    parts["Trend"] = 10 if (e and ep and a["close"] > e and e > ep) else 5 if (e and a["close"] > e) else 0
    liq = m["liq"]
    lq = 10 if liq >= 250_000 else 7 if liq >= 150_000 else 4
    parts["Liq"] = max(lq - (3 if liq and m["mcap"] / liq > 20 else 0), 0)
    bs = m["bs"]
    parts["Flow"] = 10 if bs >= 1.2 else 7 if bs >= 1.05 else 4 if bs >= 0.95 else 0
    rp = risk_frac * 100
    parts["Risk"] = 10 if rp <= 8 else 7 if rp <= 12 else 3 if rp <= 20 else 0
    if tm and tm.get("resolved", 1) < 0.25:
        parts["TM"] = 5                       # inconclusive simulation: neutral points
    elif tm:
        d = (tm["p_hit"] - 1 / (1 + BO["target_r"])) * 100
        parts["TM"] = 15 if d >= 10 else 10 if d >= 0 else 6 if d >= -5 else 2 if d >= -15 else 0
    else:
        parts["TM"] = 0
    return round(sum(parts.values())), parts


def socials_of(p):
    info = p.get("info") or {}
    got = [x.get("type", "link") for x in (info.get("socials") or [])]
    if info.get("websites"):
        got.append("website")
    return got


def scan_breakouts(universe, trades, stamp):
    """Returns (picks, blocked, watch, note)."""
    picks, blocked, watch, fails = [], [], [], 0
    R = BO["target_r"]
    log_rows = []
    for u in universe:
        p, m = u["pair"], u["m"]
        bars = gt_ohlcv(p.get("pairAddress", ""))
        if bars is None:
            fails += 1
            continue
        a = analyze_breakout(bars)
        if a is None:
            print(f"  breakout check {u['symbol']}: skipped (too little or too sparse candle data)")
            continue
        print(f"  breakout check {u['symbol']}: close ${a['close']:.6g} is "
              f"{(a['close'] / a['hi'] - 1) * 100:+.1f}% vs 24h high ${a['hi']:.6g}, "
              f"volume {a['volx']:.1f}x, setup: {a['state'] or 'none'}")
        if a["state"] is None:
            continue
        if a["state"] == "WATCH":
            watch.append(f"{u['symbol']}: ${a['close']:.6g} is {(1 - a['close'] / a['hi']) * 100:.1f}% below "
                         f"24h range high ${a['hi']:.6g} (range {a['width_pct']:.0f}% wide)")
            continue
        if a["state"] == "WEAK":
            watch.append(f"{u['symbol']}: closed above range but volume only {a['volx']:.1f}x "
                         f"(need {BO['vol_confirm']}x) - not confirmed")
            continue

        entry = u["price"]
        stop = a["mid"]                                    # invalidation: middle of the range
        if entry <= stop:
            print(f"  -> {u['symbol']}: skipped, live price ${entry:.6g} is at or below the invalidation level ${stop:.6g}")
            continue
        risk = entry - stop
        risk_frac = risk / entry
        target = entry + R * risk
        size = size_for(risk_frac)
        start_hour = int(((a["last_ts"] + 3600) // 3600) % 24)
        tm = time_machine(bars, entry, stop, target, R, start_hour)
        hist = history_replay(bars, R)
        total, parts = bo_score(a, m, entry, risk_frac, tm)
        soc = socials_of(p)
        sig = dict(symbol=u["symbol"], mint=u["mint"], price=entry, entry=entry, stop=stop,
                   target=target, risk_frac=risk_frac, rr=R, size_inr=size, tm=tm, hist=hist,
                   score=total, parts=parts, a=a, m=m, url=u["url"], note=u["note"],
                   chase=entry > a["close"] * (1 + BO["max_chase_pct"] / 100),
                   failed=entry < a["hi"],
                   no_socials=not soc, socials=soc,
                   trail_pct=round(max(risk_frac * 100, 8.0), 1),
                   hold_days=BO["horizon_hours"] / 24, kind="dex", pool=p.get("pairAddress", ""))
        status, blocks, warns = risk_check(sig, trades)
        if status == "PASS" and total < BO["min_score"]:
            status, blocks = "BLOCK", [f"score {total} below minimum {BO['min_score']}"]
        sig.update(status=status, blocks=blocks, warns=warns)
        print(f"  -> {sig['symbol']}: {status} | score {total}/100 | risk {risk_frac * 100:.1f}% | "
              + ("; ".join(blocks) if blocks else "all risk checks passed")
              + (f" | warnings: {'; '.join(warns)}" if warns else ""))
        (picks if status == "PASS" else blocked).append(sig)
        log_rows.append([stamp, sig["symbol"], status, total, round(risk_frac * 100, 1), R,
                         "" if not tm else round(tm["p_t"], 3), "" if not tm else round(tm["p_s"], 3),
                         "" if not tm else round(tm["exp_r"], 3), hist["fired"], hist["T"], hist["S"],
                         "; ".join(blocks + warns)])
    if log_rows:
        path = os.path.join(OUT_DIR, "signals_log.csv")
        new_file = not os.path.exists(path)
        with open(path, "a", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            if new_file:
                w.writerow(["time", "symbol", "status", "score", "risk_pct", "target_r",
                            "tm_p_target", "tm_p_stop", "tm_exp_r", "hist_fired", "hist_target",
                            "hist_stop", "notes"])
            w.writerows(log_rows)
    note = f"candle data unavailable for {fails} of {len(universe)} tokens" if fails else ""
    picks.sort(key=lambda x: x["score"], reverse=True)
    return picks, blocked, watch[:4], note


def fmt_bo_pick(r, fx):
    a, tm, h = r["a"], r["tm"], r["hist"]
    e, st, rf, sz = r["entry"], r["stop"], r["risk_frac"], r["size_inr"]
    R = r["rr"]
    t1, t2, t3 = e + (e - st), e + R * (e - st), e + 3 * (e - st)
    pc = lambda x: f"{(x / e - 1) * 100:+.1f}%"
    lines = [
        f"BREAKOUT SIGNAL: {r['symbol']} | SCORE {r['score']}/100 | RISK CHECK {r['status']}",
        f"Trigger: 1h bar closed ${a['close']:.6g} above the 24h range high ${a['hi']:.6g} "
        f"on {a['volx']:.1f}x average volume (range ${a['lo']:.6g} to ${a['hi']:.6g}, {a['width_pct']:.0f}% wide)",
        f"Entry ~${e:.6g} | INVALIDATION (stop) ${st:.6g} ({pc(st)}) = 1R risk per token",
        f"Targets: 1R ${t1:.6g} ({pc(t1)}) | {R:g}R ${t2:.6g} ({pc(t2)}) | 3R ${t3:.6g} ({pc(t3)})",
        f"R:R 1:{R:g} | Size Rs {sz:,} (~${sz / fx:,.0f}) | loss at stop ~Rs {sz * rf:,.0f} | gain at {R:g}R ~Rs {sz * rf * R:,.0f}",
    ]
    if tm:
        lines.append(
            f"Time machine ({BO['sims']:,} replays, {BO['horizon_hours']}h, drift removed): target first "
            f"{tm['p_t'] * 100:.0f}% | stop first {tm['p_s'] * 100:.0f}% | neither {tm['p_o'] * 100:.0f}% | "
            f"avg {tm['exp_r']:+.2f}R | {tm['verdict']}")
    else:
        lines.append("Time machine: not enough hourly history for this token")
    lines.append(f"Real history, same rule on this token: fired {h['fired']}x -> {h['T']} target, "
                 f"{h['S']} stop, {h['O']} neither" + (" (too few to trust)" if h["fired"] < 5 else ""))
    lines.append("Score parts: " + " | ".join(f"{k} {v:g}" for k, v in r["parts"].items()))
    lines.append(f"mcap ${r['m']['mcap']:,.0f} | liq ${r['m']['liq']:,.0f} | 24h {r['m']['c24']}% | "
                 f"socials: {', '.join(r['socials']) if r['socials'] else 'NONE'} | {r['note']}")
    if a.get("age"):
        lines[1] += " [breakout bar was 1 hour earlier and is still holding]"
    if r["warns"]:
        lines.append("Warnings: " + "; ".join(r["warns"]))
    lines.append(r["url"])
    return "\n".join(lines)


def main():
    stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    fx = usd_inr()
    addrs = candidate_tokens()
    pairs = fetch_pairs(addrs)
    print(f"{stamp}: {len(addrs)} candidates, {len(pairs)} pairs fetched, USD/INR {fx:.2f}")

    # If the API returned nothing, don't pretend the market is quiet and don't touch paper trades
    if len(addrs) < 5 or len(pairs) < 5:
        warn = (f"{stamp}: DEX Screener data unavailable ({len(addrs)} candidates, "
                f"{len(pairs)} pairs). Run skipped - paper trades NOT updated. Check internet/DNS.")
        print(warn)
        send_telegram(warn)
        return

    # update existing paper trades first (DEX and majors priced from their own sources)
    trades = load_trades()
    kind_of = lambda t: t.get("kind") or "dex"
    dex_open = [t["mint"] for t in trades if t["status"] == "OPEN" and kind_of(t) == "dex"]
    maj_open = [t["mint"] for t in trades if t["status"] == "OPEN" and kind_of(t) == "major"]
    prices, pools = {}, {}
    if dex_open:
        for p in fetch_pairs(dex_open):
            try:
                prices[p["baseToken"]["address"]] = float(p["priceUsd"])
                pools[p["baseToken"]["address"]] = p.get("pairAddress", "")
            except Exception:
                pass
    if maj_open:
        prices.update(major_prices(maj_open))
    # replay candle highs/lows since each position's last check (covers hours the PC was off)
    events = []
    for t in trades:
        if t["status"] != "OPEN":
            continue
        if kind_of(t) == "major":
            bars = binance_hourly(t["mint"])
        else:
            pool = pools.get(t["mint"]) or t.get("pool")
            bars = gt_ohlcv(pool) if pool else None
        if bars:
            events += replay_trade(t, bars)
        else:
            print(f"replay: no candles for {t['symbol']} this run (live price check only)")
    events += update_trades(trades, prices)

    # ---- DEX: safety/liquidity universe -> breakout engine ----
    universe = []
    rejections = {}
    for p in pairs:
        ok, why = passes(p, FILTERS_BO)
        if not ok:
            rejections[why] = rejections.get(why, 0) + 1
            continue
        note = "rugcheck skipped"
        if USE_RUGCHECK:
            ok, note = rugcheck_ok(p["baseToken"]["address"])
            time.sleep(0.5)
            if not ok:
                continue
        m = p["_m"]
        try:
            price = float(p["priceUsd"])
        except Exception:
            continue
        universe.append(dict(pair=p, symbol=p["baseToken"]["symbol"], mint=p["baseToken"]["address"],
                             price=price, note=note, m=m, url=p.get("url"), lscore=score(m)))
    universe.sort(key=lambda x: x["lscore"], reverse=True)
    print(f"dex: {len(universe)} tokens passed liquidity/safety filters")
    if rejections:
        print("dex: rejected by first failing filter -> " +
              ", ".join(f"{k} {v}" for k, v in sorted(rejections.items(), key=lambda kv: -kv[1])))

    bo_picks, bo_blocked, bo_watch, bo_note = [], [], [], ""
    if BO_ENABLED and universe:
        bo_picks, bo_blocked, bo_watch, bo_note = scan_breakouts(universe[:BO["candidates"]], trades, stamp)

    held = {t["mint"] for t in trades if t["status"] == "OPEN"}
    alerted = load_alerted()
    cooldown = ALERT_COOLDOWN_HOURS * 3600
    fresh = lambda mint: mint not in held and time.time() - alerted.get(mint, 0) > cooldown
    fresh_key = lambda key: time.time() - alerted.get(key, 0) > cooldown
    new_picks = [r for r in bo_picks if fresh(r["mint"])]
    new_blocked = [r for r in bo_blocked if fresh_key(r["mint"] + "|blk")]
    suppressed = [r["symbol"] for r in bo_picks if r not in new_picks]
    quiet = suppressed + [r["symbol"] for r in bo_blocked if r not in new_blocked]
    if quiet:
        print("dex: signal(s) found but not re-sent (already alerted in the last "
              f"{ALERT_COOLDOWN_HOURS}h or already held): {', '.join(quiet)}")

    # ---- Majors scan (+ same risk gate and risk-based sizing) ----
    m_picks, m_new, m_blocked, m_suppressed, m_down = [], [], [], [], False
    if MAJORS_ENABLED:
        try:
            m_picks = scan_majors()
        except Exception as e:
            print("majors scan error:", type(e).__name__, e)
            m_picks = None
        if m_picks is None:
            m_down, m_picks = True, []
        for r in m_picks:
            if not fresh(r["mint"]):
                m_suppressed.append(r["symbol"])
                continue
            rf = 1 - r["stop"] / r["price"]
            r.update(entry=r["price"], risk_frac=rf, rr=2.0, size_inr=size_for(rf), kind="major",
                     tm=None, hold_days=MAJORS_HOLD_DAYS)
            status, blocks, warns = risk_check(r, trades)
            r.update(status=status, blocks=blocks, warns=warns)
            (m_new if status == "PASS" else m_blocked).append(r)
        log_majors(m_new, stamp)

    opened = open_paper_trades(trades, new_picks, stamp, "dex") if new_picks else []
    opened += open_paper_trades(trades, m_new, stamp, "major") if m_new else []
    save_trades(trades)

    lines = [f"Crypto screener update {stamp} (screening only - do your own checks)"]
    if new_picks:
        lines.append("DEX BREAKOUT SIGNALS (hourly-bar range breakouts, Solana)")
        lines += [fmt_bo_pick(r, fx) for r in new_picks]
    else:
        lines.append("DEX: no confirmed breakout signals this run."
                     + (f" Still active/tracked: {', '.join(suppressed)}." if suppressed else ""))
    for r in new_blocked:
        lines.append(f"BLOCKED: {r['symbol']} broke out (score {r['score']}) but the risk check says BLOCK:\n- "
                     + "\n- ".join(r["blocks"]))
    if bo_watch:
        lines.append("WATCHLIST (not signals):\n" + "\n".join(bo_watch))
    if bo_note:
        lines.append("Note: " + bo_note)

    if MAJORS_ENABLED:
        if m_down:
            lines.append("MAJORS: Binance data not reachable this run.")
        elif m_new:
            lines.append(f"MAJORS PICKS (established coins, daily-chart swing, up to {MAJORS_HOLD_DAYS} days)")
        else:
            lines.append("MAJORS: no new picks this run."
                         + (f" Still passing but already tracked/alerted: {', '.join(m_suppressed)}." if m_suppressed else ""))
        for r in m_new:
            mc = f"${r['mcap'] / 1e6:,.0f}M" if r["mcap"] else "n/a"
            oi = "n/a" if r["oi"] is None else f"{r['oi']:+.0f}%"
            e, st = r["price"], r["stop"]
            t1, t3 = e + (e - st), e + 3 * (e - st)
            lines.append(
                f"{r['symbol']} | SCORE {r['score']} | RISK CHECK {r['status']} | price ${e:.6g} | mcap {mc}\n"
                f"RSI {r['rsi']:.0f} | spread {r['spread']:.3f}% | bid depth(1%) ${r['depth']:,.0f} | OI 3d {oi}\n"
                f"Why: {'; '.join(r['notes'])}\n"
                f"INVALIDATION (stop) ${st:.6g} ({(st / e - 1) * 100:+.1f}%) = 1R | targets: 1R ${t1:.6g} | "
                f"2R ${r['target']:.6g} ({(r['target'] / e - 1) * 100:+.1f}%) | 3R ${t3:.6g}\n"
                f"R:R 1:2 | Size Rs {r['size_inr']:,} (~${r['size_inr'] / fx:,.0f}) | loss at stop ~Rs "
                f"{r['size_inr'] * r['risk_frac']:,.0f} | trail rest {r['trail_pct']}%\n"
                f"{r['url']}  (check it is available on your exchange)")
        for r in m_blocked:
            lines.append(f"BLOCKED: {r['symbol']} (majors score {r['score']}) - " + "; ".join(r["blocks"]))

    if opened:
        lines.append("Paper trades opened: " + ", ".join(opened))
    if events:
        lines.append("Paper updates:\n" + "\n".join(events))
    for r in m_new + new_picks:
        prices.setdefault(r["mint"], float(r["price"]))
    hl = holding_lines(trades, prices)
    if hl:
        lines.append("OPEN POSITIONS\n" + "\n\n".join(hl))
    lines.append(paper_summary(trades))
    reserve = CAPITAL_INR - (MAX_OPEN + MAJORS_MAX_OPEN) * POSITION_INR
    lines.append(f"Reserve: keep at least Rs {reserve:,} uninvested. Paper trading only - the bot never places orders.")
    msg = "\n\n".join(lines)
    print(msg)

    has_news = bool(new_picks or new_blocked or m_new or m_blocked or opened or events)
    if NOTIFY_ONLY_ON_NEWS and not has_news and not heartbeat_due():
        print("Telegram: nothing new, not sent")
    elif send_telegram(msg):
        for r in new_picks + m_new:
            alerted[r["mint"]] = time.time()
        for r in new_blocked:
            alerted[r["mint"] + "|blk"] = time.time()
        for r in m_blocked:
            alerted[r["mint"]] = time.time()
        save_alerted(alerted)

def heartbeat_due():
    try:
        with open(HEARTBEAT_FILE) as fh:
            return time.time() - float(fh.read().strip()) > HEARTBEAT_HOURS * 3600
    except Exception:
        return True


def send_telegram(text):
    """Send a message and print clearly whether it worked."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram: NOT CONFIGURED - TG_BOT_TOKEN / TG_CHAT_ID not visible to this process")
        return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          data={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000],
                                "disable_web_page_preview": True}, timeout=15)
        if r.ok:
            print("Telegram: sent")
            with open(HEARTBEAT_FILE, "w") as fh:
                fh.write(str(time.time()))
            return True
        print("Telegram: FAILED", r.status_code, r.text[:200])
    except Exception as e:
        print("Telegram: FAILED", type(e).__name__)
    return False


def probe():
    """--probe: checks the breakout engine on the 3 most liquid DEX pairs, ignoring all filters.
    Shows whether hourly candles load and what the engine would say. Places nothing, sends nothing."""
    addrs = candidate_tokens()
    pairs = fetch_pairs(addrs)
    pairs.sort(key=lambda x: (x.get("liquidity") or {}).get("usd", 0) or 0, reverse=True)
    print(f"probe: {len(pairs)} pairs found, testing the 3 most liquid")
    for p in pairs[:3]:
        sym = p["baseToken"]["symbol"]
        bars = gt_ohlcv(p.get("pairAddress", ""))
        print(f"\n{sym}: {0 if bars is None else len(bars)} closed hourly bars loaded")
        if not bars:
            print("  no candles -> check the 'candles failed' line above for the reason")
            continue
        a = analyze_breakout(bars)
        if a is None:
            print("  too little history for analysis")
            continue
        print(f"  last closed bar ${a['close']:.6g} | 24h range ${a['lo']:.6g} to ${a['hi']:.6g} | "
              f"volume {a['volx']:.1f}x average | setup: {a['state'] or 'none'}")
        entry, stop = a["close"], a["mid"]
        label = "example plan, stop at range midpoint"
        if entry <= stop:        # price sits inside the lower half of the range: use a lower example stop
            stop = min(a["lo"], entry * 0.97)
            label = "example plan, stop at range low"
        if entry > stop:
            R = BO["target_r"]
            target = entry + R * (entry - stop)
            start_hour = int(((a["last_ts"] + 3600) // 3600) % 24)
            tm = time_machine(bars, entry, stop, target, R, start_hour)
            h = history_replay(bars, R)
            if tm:
                print(f"  time machine ({label}): target first {tm['p_t'] * 100:.0f}% | "
                      f"stop first {tm['p_s'] * 100:.0f}% | {tm['verdict']}")
            else:
                print("  time machine: not enough history")
            print(f"  real history of this rule: fired {h['fired']}x -> {h['T']} target, {h['S']} stop")
    print("\nprobe finished - engine components ran" if pairs else "\nprobe: no pairs found")


class _Tee:
    """Send everything printed to the screen AND to run_log.txt (works under pythonw too)."""
    def __init__(self, *streams):
        self.streams = [x for x in streams if x]

    def write(self, text):
        for x in self.streams:
            try:
                x.write(text)
                x.flush()
            except Exception:
                pass

    def flush(self):
        pass


def _open_run_log():
    path = os.path.join(OUT_DIR, "run_log.txt")
    try:                                   # keep the log small: trim when above ~500 KB
        if os.path.exists(path) and os.path.getsize(path) > 500_000:
            with open(path, "rb") as fh:
                fh.seek(-200_000, os.SEEK_END)
                tail = fh.read()
            with open(path, "wb") as fh:
                fh.write(tail)
        return open(path, "a", encoding="utf-8")
    except Exception:
        return None


if __name__ == "__main__":
    import traceback
    _log = _open_run_log()
    sys.stdout = _Tee(sys.stdout, _log)
    sys.stderr = _Tee(sys.stderr, _log)
    print(f"\n===== run started {dt.datetime.now():%Y-%m-%d %H:%M:%S} =====")
    try:
        if "--test" in sys.argv:
            send_telegram("Test message from Crypto_By_SPG: Telegram alerts are working.")
        elif "--probe" in sys.argv:
            probe()
        else:
            main()
        print(f"===== run finished {dt.datetime.now():%H:%M:%S} =====")
    except Exception:
        traceback.print_exc()
        print("===== run FAILED =====")
        raise SystemExit(1)
