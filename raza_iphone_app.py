import os
import time
import csv
import json
import threading
import secrets
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import psycopg
from psycopg.rows import dict_row

from flask import (
    Flask,
    jsonify,
    render_template,
    send_from_directory,
    request,
    session as flask_session,
    redirect,
    url_for,
    Response,
)

# ============================================================
# RAZA SHAH SIGNAL
# COINGECKO AGGREGATED CRYPTO / DERIVATIVES DATA
# SIGNAL ONLY — NO AUTO ORDERS
# ============================================================

CG_BASE_URL = "https://api.coingecko.com/api/v3"
CG_API_KEY = (os.getenv("COINGECKO_API_KEY") or "").strip()
DATABASE_URL = os.getenv("DATABASE_URL")

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "900"))
TOP_COINS = int(os.getenv("TOP_COINS", "100"))
DEEP_CHECK = int(os.getenv("DEEP_CHECK", "25"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "85"))

TP_PCT = float(os.getenv("TP_PCT", "0.006"))
SL_PCT = float(os.getenv("SL_PCT", "0.004"))

LIGHT_SCAN_WORKERS = int(os.getenv("LIGHT_SCAN_WORKERS", "8"))
DEEP_SCAN_WORKERS = int(os.getenv("DEEP_SCAN_WORKERS", "4"))
SCAN_HTTP_TIMEOUT = float(os.getenv("SCAN_HTTP_TIMEOUT", "15"))
SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "14400"))

# Paper tester
TEST_START_CAPITAL = float(os.getenv("TEST_START_CAPITAL", "100"))
TEST_LEVERAGE = float(os.getenv("TEST_LEVERAGE", "20"))

# ============================================================
# TELEGRAM / WEB
# ============================================================

TELEGRAM_BOT_TOKEN = (
    os.getenv("RAZA_TELEGRAM_BOT_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or ""
).strip()

TELEGRAM_CHAT_ID = (
    os.getenv("RAZA_TELEGRAM_CHAT_ID")
    or os.getenv("TELEGRAM_CHAT_ID")
    or ""
).strip()

APP_URL = os.getenv(
    "APP_URL",
    "https://raza-shah-signal.onrender.com"
).rstrip("/")

APP_SECRET_KEY = os.getenv(
    "APP_SECRET_KEY",
    "raza-signal-change-this-secret"
)

OTP_TTL_SECONDS = int(os.getenv("OTP_TTL_SECONDS", "300"))
OTP_MAX_ATTEMPTS = int(os.getenv("OTP_MAX_ATTEMPTS", "5"))
ACCESS_TTL_SECONDS = int(os.getenv("ACCESS_TTL_SECONDS", "86400"))
TELEGRAM_STATUS_INTERVAL = int(os.getenv("TELEGRAM_STATUS_INTERVAL", "3600"))
TRADE_MONITOR_INTERVAL = int(os.getenv("TRADE_MONITOR_INTERVAL", "20"))

# ============================================================
# FILES
# ============================================================

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)

LIVE_FILE = DATA_DIR / "live_signals.csv"
STATE_FILE = DATA_DIR / "scanner_state.json"

CSV_COLUMNS = [
    "time_utc", "symbol", "signal", "score", "price",
    "flow_delta", "buy_usd_60s", "sell_usd_60s",
    "spread_bps", "book_imb", "trend_5m", "oi_change_pct",
    "tp", "sl",
]

# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()
session.headers.update({
    "User-Agent": "RAZA-SHAH-SIGNAL-AGGREGATED/3.0",
    "Accept": "application/json",
})

# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)
app.secret_key = APP_SECRET_KEY
app.permanent_session_lifetime = timedelta(seconds=ACCESS_TTL_SECONDS)

# ============================================================
# DATABASE
# ============================================================

def get_db():
    if not DATABASE_URL:
        return None
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db():
    if not DATABASE_URL:
        print("[DB] DATABASE_URL missing", flush=True)
        return

    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trade_results (
                        trade_id TEXT PRIMARY KEY,
                        time_utc TEXT,
                        closed_time_utc TEXT,
                        symbol TEXT,
                        signal TEXT,
                        score DOUBLE PRECISION,
                        entry DOUBLE PRECISION,
                        tp DOUBLE PRECISION,
                        sl DOUBLE PRECISION,
                        status TEXT,
                        exit_price DOUBLE PRECISION
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS forming_results (
                        setup_id TEXT PRIMARY KEY,
                        time_utc TEXT,
                        closed_time_utc TEXT,
                        symbol TEXT,
                        signal TEXT,
                        score DOUBLE PRECISION,
                        risk_label TEXT,
                        entry DOUBLE PRECISION,
                        tp DOUBLE PRECISION,
                        sl DOUBLE PRECISION,
                        status TEXT,
                        exit_price DOUBLE PRECISION
                    )
                """)

            conn.commit()

        print("[DB] PostgreSQL tables ready", flush=True)

    except Exception as e:
        print(f"[DB] INIT ERROR: {type(e).__name__}: {e}", flush=True)

# ============================================================
# STATE
# ============================================================

state_lock = threading.Lock()

state = {
    "running": False,
    "exchange": "AGGREGATED",
    "data_source": "CoinGecko",
    "status": "Starting...",
    "last_scan": None,
    "next_scan": None,
    "alerts_last_scan": 0,
    "latest_signal": None,
    "best_candidate": None,
    "scan_progress": "0/0",
    "last_scan_seconds": None,
    "last_error": None,
    "rsi_watchlist": {"oversold_long": [], "overbought_short": []},
    "oi_snapshot": {},
}


def save_state_snapshot():
    try:
        with state_lock:
            payload = dict(state)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp.replace(STATE_FILE)
    except Exception as e:
        print(f"[SCANNER] STATE SAVE ERROR: {e}", flush=True)


def load_state_snapshot():
    try:
        if not STATE_FILE.exists():
            return None
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception as e:
        print(f"[SCANNER] STATE LOAD ERROR: {e}", flush=True)
        return None


_old_state = load_state_snapshot()
if _old_state and isinstance(_old_state.get("oi_snapshot"), dict):
    state["oi_snapshot"] = _old_state["oi_snapshot"]

# ============================================================
# LOGGING / TELEGRAM
# ============================================================

def scan_log(message):
    print(f"[SCANNER] {message}", flush=True)


def telegram_log(message):
    print(f"[TELEGRAM] {message}", flush=True)


def telegram(text):
    if not TELEGRAM_BOT_TOKEN:
        telegram_log("BOT TOKEN missing")
        return False
    if not TELEGRAM_CHAT_ID:
        telegram_log("CHAT ID missing")
        return False

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        r = requests.post(
            url,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if not r.ok:
            telegram_log(f"FAILED HTTP {r.status_code} | {r.text[:300]}")
            return False
        telegram_log("Message sent OK")
        return True
    except Exception as e:
        telegram_log(f"ERROR {type(e).__name__}: {e}")
        return False


def telegram_async(text):
    threading.Thread(target=telegram, args=(text,), daemon=True).start()

# ============================================================
# COINGECKO AGGREGATED DATA
# ============================================================

cg_cache_lock = threading.Lock()
cg_cache = {
    "derivatives": {"time": 0.0, "data": []},
    "markets": {"time": 0.0, "data": []},
    "coin_map": {"time": 0.0, "data": {}},
    "chart": {},
}


def cg_headers():
    h = {}
    if CG_API_KEY:
        h["x-cg-demo-api-key"] = CG_API_KEY
    return h


def cg_get(path, params=None, timeout=None, retries=4):
    if timeout is None:
        timeout = SCAN_HTTP_TIMEOUT

    url = CG_BASE_URL + path
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            r = session.get(
                url,
                params=params or {},
                headers=cg_headers(),
                timeout=timeout,
            )

            if r.status_code == 429:
                wait = min(2 ** attempt, 12)
                scan_log(f"CoinGecko rate limit — retry in {wait}s")
                time.sleep(wait)
                continue

            if r.status_code in (401, 403):
                msg = (
                    f"CoinGecko HTTP {r.status_code}. "
                    f"Add COINGECKO_API_KEY in Render Environment if required."
                )
                raise RuntimeError(msg)

            r.raise_for_status()
            return r.json()

        except Exception as e:
            last_error = e
            if attempt < retries:
                time.sleep(min(attempt * 1.5, 5))

    raise RuntimeError(f"CoinGecko request failed: {last_error}")


def derivatives_snapshot(force=False):
    now = time.time()
    with cg_cache_lock:
        cached = cg_cache["derivatives"]
        if not force and cached["data"] and now - cached["time"] < 45:
            return cached["data"]

    data = cg_get("/derivatives")
    if not isinstance(data, list):
        data = []

    rows = []
    for x in data:
        try:
            if str(x.get("contract_type", "")).lower() != "perpetual":
                continue
            symbol = str(x.get("symbol", "")).upper().replace("-", "")
            index_id = str(x.get("index_id", "")).upper().strip()
            price = float(x.get("price") or 0)
            volume = float(x.get("volume_24h") or 0)
            oi = float(x.get("open_interest") or 0)
            spread = float(x.get("spread") or 0)
            funding = float(x.get("funding_rate") or 0)
            basis = float(x.get("basis") or 0)
            change24 = float(x.get("price_percentage_change_24h") or 0)

            if price <= 0 or volume <= 0:
                continue
            if not index_id:
                continue

            rows.append({
                "market": str(x.get("market") or ""),
                "raw_symbol": symbol,
                "base": index_id,
                "symbol": index_id + "USDT",
                "price": price,
                "volume_24h": volume,
                "open_interest": oi,
                "spread_pct": spread,
                "funding_rate": funding,
                "basis": basis,
                "change_24h": change24,
                "last_traded_at": int(x.get("last_traded_at") or 0),
            })
        except Exception:
            pass

    with cg_cache_lock:
        cg_cache["derivatives"] = {"time": now, "data": rows}

    return rows


def market_snapshot(force=False):
    now = time.time()
    with cg_cache_lock:
        cached = cg_cache["markets"]
        if not force and cached["data"] and now - cached["time"] < 60:
            return cached["data"]

    data = cg_get(
        "/coins/markets",
        {
            "vs_currency": "usd",
            "order": "volume_desc",
            "per_page": 250,
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "1h,24h",
        },
    )
    if not isinstance(data, list):
        data = []

    with cg_cache_lock:
        cg_cache["markets"] = {"time": now, "data": data}

    return data


def aggregate_derivatives():
    rows = derivatives_snapshot()

    grouped = {}
    for x in rows:
        g = grouped.setdefault(x["base"], {
            "base": x["base"],
            "symbol": x["base"] + "USDT",
            "volume_24h": 0.0,
            "open_interest": 0.0,
            "weighted_price": 0.0,
            "weight": 0.0,
            "funding_weighted": 0.0,
            "basis_weighted": 0.0,
            "change_weighted": 0.0,
            "spread_weighted": 0.0,
            "markets": [],
        })

        w = max(x["volume_24h"], 1.0)
        g["volume_24h"] += x["volume_24h"]
        g["open_interest"] += x["open_interest"]
        g["weighted_price"] += x["price"] * w
        g["funding_weighted"] += x["funding_rate"] * w
        g["basis_weighted"] += x["basis"] * w
        g["change_weighted"] += x["change_24h"] * w
        g["spread_weighted"] += x["spread_pct"] * w
        g["weight"] += w
        g["markets"].append(x)

    out = []
    for g in grouped.values():
        if not g["weight"]:
            continue
        g["price"] = g["weighted_price"] / g["weight"]
        g["funding_rate"] = g["funding_weighted"] / g["weight"]
        g["basis"] = g["basis_weighted"] / g["weight"]
        g["change_24h"] = g["change_weighted"] / g["weight"]
        g["spread_pct"] = g["spread_weighted"] / g["weight"]
        out.append(g)

    out.sort(key=lambda z: z["volume_24h"], reverse=True)
    return out


def fallback_market_aggregates():
    out = []
    for x in market_snapshot():
        try:
            sym = str(x.get("symbol") or "").upper()
            if not sym or len(sym) > 12:
                continue
            price = float(x.get("current_price") or 0)
            volume = float(x.get("total_volume") or 0)
            if price <= 0 or volume <= 0:
                continue
            out.append({
                "base": sym,
                "symbol": sym + "USDT",
                "price": price,
                "volume_24h": volume,
                "open_interest": 0.0,
                "funding_rate": 0.0,
                "basis": 0.0,
                "change_24h": float(x.get("price_change_percentage_24h") or 0),
                "spread_pct": 0.10,
                "markets": [],
                "coin_id": x.get("id"),
            })
        except Exception:
            pass
    return out


def all_aggregates():
    try:
        agg = aggregate_derivatives()
        if agg:
            return agg
    except Exception as e:
        scan_log(f"DERIVATIVES DATA FALLBACK: {e}")

    return fallback_market_aggregates()


def top_symbols():
    agg = all_aggregates()
    symbols = [x["symbol"] for x in agg[:TOP_COINS]]
    if not symbols:
        raise RuntimeError("No aggregated crypto symbols found")
    scan_log(f"TOP SYMBOLS LOADED: {len(symbols)}")
    return symbols


def aggregate_for_symbol(symbol):
    base = str(symbol).upper().replace("USDT", "")
    for x in all_aggregates():
        if x["base"] == base:
            return x
    return None


def coin_id_for_symbol(symbol):
    base = str(symbol).upper().replace("USDT", "").lower()

    now = time.time()
    with cg_cache_lock:
        cm = cg_cache["coin_map"]
        if cm["data"] and now - cm["time"] < 3600:
            found = cm["data"].get(base)
            if found:
                return found

    mapping = {}
    for x in market_snapshot():
        try:
            sym = str(x.get("symbol") or "").lower()
            cid = str(x.get("id") or "")
            if sym and cid and sym not in mapping:
                mapping[sym] = cid
        except Exception:
            pass

    with cg_cache_lock:
        cg_cache["coin_map"] = {"time": now, "data": mapping}

    return mapping.get(base)


def market_chart(symbol, days=1, interval=None):
    cid = coin_id_for_symbol(symbol)
    if not cid:
        raise RuntimeError(f"CoinGecko ID not found for {symbol}")

    key = f"{cid}:{days}:{interval or 'auto'}"
    now = time.time()

    with cg_cache_lock:
        cached = cg_cache["chart"].get(key)
        if cached and now - cached["time"] < (45 if days == 1 else 600):
            return cached["data"]

    params = {"vs_currency": "usd", "days": days}
    if interval:
        params["interval"] = interval

    data = cg_get(f"/coins/{cid}/market_chart", params)

    with cg_cache_lock:
        cg_cache["chart"][key] = {"time": now, "data": data}

    return data


def resample_series(points, bucket_minutes):
    if not points:
        return []

    bucket_ms = bucket_minutes * 60 * 1000
    buckets = {}

    for ts, value in points:
        try:
            ts = int(ts)
            value = float(value)
        except Exception:
            continue
        b = ts - (ts % bucket_ms)
        buckets[b] = value

    return [v for _, v in sorted(buckets.items())]


def rsi(values, period=14):
    if len(values) < period + 1:
        return 50.0

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def ema(values, n):
    if not values:
        return 0.0
    a = 2 / (n + 1)
    e = values[0]
    for v in values[1:]:
        e = a * v + (1 - a) * e
    return e


def historical_metrics(symbol):
    one_day = market_chart(symbol, days=1)
    seven_day = market_chart(symbol, days=7, interval="hourly")

    p5 = one_day.get("prices", []) or []
    v5 = one_day.get("total_volumes", []) or []
    ph = seven_day.get("prices", []) or []

    closes_5m = [float(x[1]) for x in p5 if len(x) >= 2]
    volumes_5m = [float(x[1]) for x in v5 if len(x) >= 2]

    closes_15m = resample_series(p5, 15)
    closes_1h = resample_series(ph, 60)
    closes_4h = resample_series(ph, 240)

    if len(closes_5m) < 25:
        raise RuntimeError(f"Not enough live chart points for {symbol}")

    r15 = rsi(closes_15m, 14)
    r1h = rsi(closes_1h, 14)
    r4h = rsi(closes_4h, 14)

    e9 = ema(closes_5m[-40:], 9)
    e21 = ema(closes_5m[-50:], 21)
    trend = "BULL" if e9 > e21 else "BEAR"

    momentum = (
        closes_5m[-1] / closes_5m[-4] - 1
        if len(closes_5m) >= 4 and closes_5m[-4]
        else 0.0
    )

    if len(volumes_5m) >= 21:
        avg_vol = sum(volumes_5m[-21:-1]) / 20
        vol_ratio = volumes_5m[-1] / avg_vol if avg_vol else 0.0
    else:
        vol_ratio = 1.0

    # Signed recent volume pressure from CoinGecko aggregate price/volume chart.
    buy = 0.0
    sell = 0.0
    recent_n = min(12, len(closes_5m) - 1, len(volumes_5m) - 1)

    for i in range(len(closes_5m) - recent_n, len(closes_5m)):
        if i <= 0 or i >= len(volumes_5m):
            continue
        dv = max(volumes_5m[i] - volumes_5m[i - 1], 0.0)
        if closes_5m[i] >= closes_5m[i - 1]:
            buy += dv
        else:
            sell += dv

    total = buy + sell
    flow_delta = (buy - sell) / total if total else 0.0

    buy_rank = (
        max(0.0, 50.0 - r15) * 1.5
        + max(0.0, r1h - 45.0)
        + max(0.0, r4h - 45.0)
    )

    sell_rank = (
        max(0.0, r15 - 50.0) * 1.5
        + max(0.0, 55.0 - r1h)
        + max(0.0, 55.0 - r4h)
    )

    bias = "BUY" if buy_rank >= sell_rank else "SELL"

    return {
        "price": closes_5m[-1],
        "trend": trend,
        "momentum": momentum,
        "vol_ratio": vol_ratio,
        "rsi_15m": round(r15, 2),
        "rsi_1h": round(r1h, 2),
        "rsi_4h": round(r4h, 2),
        "rsi_bias": bias,
        "rsi_rank": max(buy_rank, sell_rank),
        "flow_delta": flow_delta,
        "buy_usd": buy,
        "sell_usd": sell,
    }


def current_price(symbol):
    agg = aggregate_for_symbol(symbol)
    if agg and float(agg.get("price") or 0) > 0:
        return float(agg["price"])

    cid = coin_id_for_symbol(symbol)
    if not cid:
        raise RuntimeError(f"No price mapping for {symbol}")

    data = cg_get(
        "/simple/price",
        {"ids": cid, "vs_currencies": "usd"},
    )
    px = float((data.get(cid) or {}).get("usd") or 0)
    if px <= 0:
        raise RuntimeError(f"No CoinGecko price for {symbol}")
    return px


def oi_change_pct(symbol):
    agg = aggregate_for_symbol(symbol)
    if not agg:
        return 0.0

    current_oi = float(agg.get("open_interest") or 0)
    if current_oi <= 0:
        return 0.0

    base = str(symbol).upper()
    with state_lock:
        previous = float((state.get("oi_snapshot") or {}).get(base) or 0)
        state.setdefault("oi_snapshot", {})[base] = current_oi

    if previous <= 0:
        return 0.0

    return ((current_oi / previous) - 1.0) * 100.0


def aggregate_liquidity_metrics(symbol, hm):
    agg = aggregate_for_symbol(symbol) or {}
    markets = list(agg.get("markets") or [])

    # Cross-market liquidity pressure from reported futures price, basis,
    # funding and 24h change. This is aggregated data, not a direct exchange book.
    funding = float(agg.get("funding_rate") or 0)
    basis = float(agg.get("basis") or 0)
    change = float(agg.get("change_24h") or 0)
    spread_pct = abs(float(agg.get("spread_pct") or 0.10))

    pressure = (
        max(-1.0, min(1.0, hm.get("flow_delta", 0.0))) * 0.50
        + max(-1.0, min(1.0, change / 5.0)) * 0.20
        + max(-1.0, min(1.0, basis / 1.0)) * 0.15
        + max(-1.0, min(1.0, funding / 0.05)) * 0.15
    )
    pressure = max(-1.0, min(1.0, pressure))

    spread_bps = max(0.01, spread_pct * 100.0)

    # Build an aggregated best-quote ladder from CoinGecko derivative tickers.
    bids = []
    asks = []

    for m in sorted(markets, key=lambda x: x["volume_24h"], reverse=True)[:12]:
        px = float(m["price"])
        sp = max(abs(float(m.get("spread_pct") or spread_pct)), 0.001)
        half = px * (sp / 100.0) / 2.0
        weight = max(float(m.get("volume_24h") or 0), 1.0)
        bids.append({"price": px - half, "qty": 0.0, "usd": weight, "market": m.get("market")})
        asks.append({"price": px + half, "qty": 0.0, "usd": weight, "market": m.get("market")})

    return spread_bps, pressure, bids, asks


def flow_metrics(symbol):
    hm = historical_metrics(symbol)
    return (
        hm["flow_delta"],
        hm["buy_usd"],
        hm["sell_usd"],
    )


def depth_metrics(symbol, hm=None):
    if hm is None:
        hm = historical_metrics(symbol)
    spread, pressure, _, _ = aggregate_liquidity_metrics(symbol, hm)
    return spread, pressure

# ============================================================
# TRADE DATABASE
# ============================================================

def trade_rows():
    if not DATABASE_URL:
        return []
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        trade_id, time_utc, closed_time_utc, symbol,
                        signal, score, entry, tp, sl, status, exit_price
                    FROM trade_results
                    ORDER BY time_utc ASC
                """)
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"[DB] TRADE READ ERROR: {e}", flush=True)
        return []


def write_trade_rows(rows):
    if not DATABASE_URL:
        return
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute("""
                        INSERT INTO trade_results (
                            trade_id, time_utc, closed_time_utc, symbol,
                            signal, score, entry, tp, sl, status, exit_price
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (trade_id)
                        DO UPDATE SET
                            closed_time_utc = EXCLUDED.closed_time_utc,
                            score = EXCLUDED.score,
                            entry = EXCLUDED.entry,
                            tp = EXCLUDED.tp,
                            sl = EXCLUDED.sl,
                            status = EXCLUDED.status,
                            exit_price = EXCLUDED.exit_price
                    """, (
                        r.get("trade_id"),
                        r.get("time_utc"),
                        r.get("closed_time_utc") or "",
                        r.get("symbol"),
                        r.get("signal"),
                        float(r.get("score") or 0),
                        float(r.get("entry") or 0),
                        float(r.get("tp") or 0),
                        float(r.get("sl") or 0),
                        r.get("status") or "OPEN",
                        float(r.get("exit_price")) if r.get("exit_price") not in ("", None) else None,
                    ))
            conn.commit()
    except Exception as e:
        print(f"[DB] TRADE WRITE ERROR: {e}", flush=True)


def add_open_trade(row):
    trade_id = f'{row["symbol"]}-{int(time.time()*1000)}'
    rows = trade_rows()
    rows.append({
        "trade_id": trade_id,
        "time_utc": row["time_utc"],
        "closed_time_utc": "",
        "symbol": row["symbol"],
        "signal": row["signal"],
        "score": row["score"],
        "entry": row["price"],
        "tp": row["tp"],
        "sl": row["sl"],
        "status": "OPEN",
        "exit_price": "",
    })
    write_trade_rows(rows)
    return trade_id


def forming_rows():
    if not DATABASE_URL:
        return []
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        setup_id, time_utc, closed_time_utc, symbol,
                        signal, score, risk_label, entry, tp, sl,
                        status, exit_price
                    FROM forming_results
                    ORDER BY time_utc ASC
                """)
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"[DB] FORMING READ ERROR: {e}", flush=True)
        return []


def has_recent_forming(symbol, side):
    now = datetime.now(timezone.utc)
    for r in forming_rows():
        if r.get("symbol") != symbol or r.get("signal") != side:
            continue
        if r.get("status") == "OPEN":
            return True
        try:
            t = datetime.fromisoformat(r.get("time_utc") or "")
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            if (now - t).total_seconds() < SIGNAL_COOLDOWN_SECONDS:
                return True
        except Exception:
            pass
    return False


def add_forming_setup(candidate):
    score = int(candidate.get("score", 0) or 0)
    if score < 60 or score >= MIN_SCORE:
        return False

    symbol = candidate.get("symbol")
    side = candidate.get("signal")

    if not symbol or not side or has_recent_forming(symbol, side):
        return False

    setup_id = f"{symbol}-{int(time.time()*1000)}"

    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO forming_results (
                        setup_id, time_utc, closed_time_utc, symbol,
                        signal, score, risk_label, entry, tp, sl,
                        status, exit_price
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (setup_id) DO NOTHING
                """, (
                    setup_id,
                    candidate.get("time_utc") or datetime.now(timezone.utc).isoformat(),
                    "",
                    symbol,
                    side,
                    score,
                    candidate.get("risk_label") or risk_label(score),
                    float(candidate.get("price") or 0),
                    float(candidate.get("tp") or 0),
                    float(candidate.get("sl") or 0),
                    "OPEN",
                    None,
                ))
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] ADD FORMING ERROR: {e}", flush=True)
        return False

# ============================================================
# PERFORMANCE / CAPITAL
# ============================================================

def _trade_return_pct(r):
    try:
        entry = float(r.get("entry") or 0)
        exit_price = float(r.get("exit_price") or 0)
        side = str(r.get("signal") or "").upper()

        if entry <= 0 or exit_price <= 0:
            return 0.0

        raw = (
            (exit_price - entry) / entry
            if side == "BUY"
            else (entry - exit_price) / entry
        )

        return raw * TEST_LEVERAGE
    except Exception:
        return 0.0


def performance():
    rows = trade_rows()
    closed = [r for r in rows if r.get("status") in ("WIN", "LOSS")]
    wins = sum(1 for r in closed if r.get("status") == "WIN")
    losses = sum(1 for r in closed if r.get("status") == "LOSS")
    total = wins + losses

    profit = 0.0
    loss = 0.0

    for r in closed:
        pl = TEST_START_CAPITAL * _trade_return_pct(r)
        if pl >= 0:
            profit += pl
        else:
            loss += abs(pl)

    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": round((wins / total) * 100, 2) if total else 0.0,
        "open_trades": sum(1 for r in rows if r.get("status") == "OPEN"),
        "total_profit": round(profit, 2),
        "total_loss": round(loss, 2),
        "net_pl": round(profit - loss, 2),
    }


def forming_performance():
    rows = forming_rows()
    closed = [r for r in rows if r.get("status") in ("WIN", "LOSS")]
    wins = sum(1 for r in closed if r.get("status") == "WIN")
    losses = sum(1 for r in closed if r.get("status") == "LOSS")
    total = wins + losses
    scores = []

    profit = 0.0
    loss = 0.0

    for r in rows:
        try:
            scores.append(float(r.get("score") or 0))
        except Exception:
            pass

    for r in closed:
        pl = TEST_START_CAPITAL * _trade_return_pct(r)
        if pl >= 0:
            profit += pl
        else:
            loss += abs(pl)

    return {
        "total_setups": len(rows),
        "closed_setups": total,
        "wins": wins,
        "losses": losses,
        "open": sum(1 for r in rows if r.get("status") == "OPEN"),
        "win_rate": round((wins / total) * 100, 2) if total else 0.0,
        "avg_score": round(sum(scores) / len(scores), 2) if scores else 0.0,
        "total_profit": round(profit, 2),
        "total_loss": round(loss, 2),
        "net_pl": round(profit - loss, 2),
    }


def capital_summary():
    rows = [
        r for r in trade_rows()
        if r.get("status") in ("WIN", "LOSS")
    ]

    # Daily summary uses KSA calendar date.
    ksa = timezone(timedelta(hours=3))
    today_ksa = datetime.now(ksa).date()

    daily = []
    for r in rows:
        try:
            dt = datetime.fromisoformat(r.get("closed_time_utc") or r.get("time_utc") or "")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt.astimezone(ksa).date() == today_ksa:
                daily.append(r)
        except Exception:
            pass

    profit = 0.0
    loss = 0.0

    for r in daily:
        pl = TEST_START_CAPITAL * _trade_return_pct(r)
        if pl >= 0:
            profit += pl
        else:
            loss += abs(pl)

    net = profit - loss
    ending = TEST_START_CAPITAL + net

    return {
        "starting_capital": round(TEST_START_CAPITAL, 2),
        "leverage": TEST_LEVERAGE,
        "daily_profit": round(profit, 2),
        "daily_loss": round(loss, 2),
        "net_pl": round(net, 2),
        "ending_capital": round(ending, 2),
        "net_pl_pct": round((net / TEST_START_CAPITAL) * 100, 2) if TEST_START_CAPITAL else 0.0,
        "closed_trades_today": len(daily),
    }

# ============================================================
# TP / SL CHECKS
# ============================================================

def check_open_trades():
    rows = trade_rows()
    changed = False
    closed_messages = []

    for r in rows:
        if r.get("status") != "OPEN":
            continue

        try:
            symbol = r["symbol"]
            side = r["signal"]
            entry = float(r["entry"])
            tp = float(r["tp"])
            sl = float(r["sl"])
            px = current_price(symbol)

            result = None

            if side == "BUY":
                if px >= tp:
                    result = "WIN"
                elif px <= sl:
                    result = "LOSS"
            else:
                if px <= tp:
                    result = "WIN"
                elif px >= sl:
                    result = "LOSS"

            if result:
                r["status"] = result
                r["exit_price"] = px
                r["closed_time_utc"] = datetime.now(timezone.utc).isoformat()
                changed = True
                closed_messages.append((symbol, side, result, entry, tp, sl, px))

        except Exception as e:
            scan_log(f"OPEN TRADE CHECK ERROR {r.get('symbol')}: {e}")

    if changed:
        write_trade_rows(rows)
        p = performance()

        for symbol, side, result, entry, tp, sl, px in closed_messages:
            icon = "✅" if result == "WIN" else "❌"
            label = "TP HIT / WIN" if result == "WIN" else "SL HIT / LOSS"

            telegram_async(
                f"{icon} {label} — {symbol}\n"
                f"Data: CoinGecko Aggregated\n"
                f"Side: {side}\n"
                f"Entry: {entry:.8g}\n"
                f"Exit: {px:.8g}\n"
                f"TP: {tp:.8g}\n"
                f"SL: {sl:.8g}\n\n"
                f"📊 PERFORMANCE\n"
                f"Trades: {p['total_trades']}\n"
                f"Wins: {p['wins']}\n"
                f"Losses: {p['losses']}\n"
                f"Winning: {p['win_rate']:.2f}%\n"
                f"Net P/L: ${p['net_pl']:.2f}\n"
                f"🔗 {APP_URL}"
            )


def check_forming_setups():
    if not DATABASE_URL:
        return

    try:
        rows = forming_rows()

        for r in rows:
            if r.get("status") != "OPEN":
                continue

            try:
                symbol = r["symbol"]
                side = r["signal"]
                tp = float(r["tp"])
                sl = float(r["sl"])
                px = current_price(symbol)

                result = None

                if side == "BUY":
                    if px >= tp:
                        result = "WIN"
                    elif px <= sl:
                        result = "LOSS"
                else:
                    if px <= tp:
                        result = "WIN"
                    elif px >= sl:
                        result = "LOSS"

                if result:
                    with get_db() as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                UPDATE forming_results
                                SET status=%s, exit_price=%s, closed_time_utc=%s
                                WHERE setup_id=%s
                            """, (
                                result,
                                px,
                                datetime.now(timezone.utc).isoformat(),
                                r["setup_id"],
                            ))
                        conn.commit()

            except Exception as e:
                scan_log(f"FORMING CHECK ERROR {r.get('symbol')}: {e}")

    except Exception as e:
        print(f"[DB] FORMING CHECK ERROR: {e}", flush=True)

# ============================================================
# SCORING
# ============================================================

def risk_label(score):
    try:
        score = int(score)
    except (TypeError, ValueError):
        score = 0

    if score >= 85:
        return "STRONG"
    if score >= 70:
        return "MEDIUM"
    return "RISKY"


def risk_level(score):
    return {"RISKY": 1, "MEDIUM": 2, "STRONG": 3}[risk_label(score)]


def build_score(light, delta, spread, book, oi):
    if light["trend"] == "BULL" and delta > 0 and book > 0:
        side = "BUY"
    elif light["trend"] == "BEAR" and delta < 0 and book < 0:
        side = "SELL"
    else:
        return None, 0

    score = 15

    if (
        (side == "BUY" and light["momentum"] > 0)
        or (side == "SELL" and light["momentum"] < 0)
    ):
        score += 5

    vr = light["vol_ratio"]
    if vr >= 1.5:
        score += 15
    elif vr >= 1.2:
        score += 10
    elif vr >= 1.0:
        score += 5

    ad = abs(delta)
    if ad >= 0.50:
        score += 25
    elif ad >= 0.35:
        score += 20
    elif ad >= 0.20:
        score += 15
    elif ad >= 0.10:
        score += 8

    ab = abs(book)
    if ab >= 0.55:
        score += 25
    elif ab >= 0.40:
        score += 20
    elif ab >= 0.25:
        score += 15
    elif ab >= 0.15:
        score += 8

    if spread <= 0.5:
        score += 10
    elif spread <= 1.0:
        score += 7
    elif spread <= 2.0:
        score += 4

    if oi > 0:
        score += 5

    if light.get("rsi_bias") == side:
        score += 5

    return side, min(score, 100)


def hard_confirm(side, delta, book, spread):
    # CoinGecko aggregate spread is cross-market data, so allow wider spread
    # while still requiring directional flow + liquidity pressure.
    if spread > 10.0:
        return False

    return (
        (side == "BUY" and delta >= 0.20 and book >= 0.20)
        or
        (side == "SELL" and delta <= -0.20 and book <= -0.20)
    )


def candidate_payload(symbol, lm, side, score, delta, buy, sell, spread, book, oi):
    price = lm["price"]

    if side == "BUY":
        tp = price * (1 + TP_PCT)
        sl = price * (1 - SL_PCT)
    else:
        tp = price * (1 - TP_PCT)
        sl = price * (1 + SL_PCT)

    confirmed = hard_confirm(side, delta, book, spread)

    return {
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "exchange": "AGGREGATED",
        "data_source": "CoinGecko",
        "symbol": symbol,
        "signal": side,
        "score": score,
        "risk_label": risk_label(score),
        "risk_level": risk_level(score),
        "price": price,
        "flow_delta": delta,
        "buy_usd_60s": buy,
        "sell_usd_60s": sell,
        "spread_bps": spread,
        "book_imb": book,
        "trend_5m": lm["trend"],
        "rsi_15m": lm.get("rsi_15m"),
        "rsi_1h": lm.get("rsi_1h"),
        "rsi_4h": lm.get("rsi_4h"),
        "rsi_bias": lm.get("rsi_bias"),
        "oi_change_pct": oi,
        "tp": tp,
        "sl": sl,
        "points_to_85": max(0, MIN_SCORE - score),
        "hard_confirm": confirmed,
        "status": (
            "TRADE READY"
            if score >= MIN_SCORE and confirmed
            else "FORMING"
        ),
    }

# ============================================================
# SIGNAL FILE
# ============================================================

def save_signal(row):
    new_file = not LIVE_FILE.exists()
    with LIVE_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if new_file:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in CSV_COLUMNS})


def recent_signals(limit=20):
    if not LIVE_FILE.exists():
        return []
    try:
        with LIVE_FILE.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        return list(reversed(rows[-limit:]))
    except Exception:
        return []

# ============================================================
# COOLDOWN
# ============================================================

def has_recent_or_open_trade(symbol, side):
    now = datetime.now(timezone.utc)

    for r in trade_rows():
        if r.get("symbol") != symbol or r.get("signal") != side:
            continue

        if r.get("status") == "OPEN":
            return True

        try:
            t = datetime.fromisoformat(r.get("time_utc") or "")
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            if (now - t).total_seconds() < SIGNAL_COOLDOWN_SECONDS:
                return True
        except Exception:
            pass

    return False

# ============================================================
# LIGHT / DEEP SCAN
# ============================================================

_scan_aggregate_map = {}


def _light_scan_one(symbol):
    try:
        agg = _scan_aggregate_map.get(symbol)
        if not agg:
            return None

        price = float(agg.get("price") or 0)
        change = float(agg.get("change_24h") or 0)
        vol = float(agg.get("volume_24h") or 0)
        oi = float(agg.get("open_interest") or 0)

        if price <= 0 or vol <= 0:
            return None

        rank = (
            min(abs(change), 20) * 5
            + (1 if oi > 0 else 0) * 10
            + min(vol / 100_000_000, 50)
        )

        lm = {
            "price": price,
            "trend": "BULL" if change >= 0 else "BEAR",
            "momentum": change / 100.0,
            "vol_ratio": 1.0,
            "rsi_15m": None,
            "rsi_1h": None,
            "rsi_4h": None,
            "rsi_bias": "BUY" if change >= 0 else "SELL",
            "rsi_rank": rank,
        }

        return rank, symbol, lm, None

    except Exception as e:
        return None, symbol, None, str(e)


def _deep_scan_one(item):
    _, symbol, _ = item

    try:
        hm = historical_metrics(symbol)

        spread, book = depth_metrics(symbol, hm)
        delta = hm["flow_delta"]
        buy = hm["buy_usd"]
        sell = hm["sell_usd"]
        oi = oi_change_pct(symbol)

        side, score = build_score(
            hm,
            delta,
            spread,
            book,
            oi,
        )

        if not side:
            return {
                "symbol": symbol,
                "side": None,
                "score": 0,
                "lm": hm,
                "error": None,
            }

        c = candidate_payload(
            symbol,
            hm,
            side,
            score,
            delta,
            buy,
            sell,
            spread,
            book,
            oi,
        )

        return {
            "symbol": symbol,
            "side": side,
            "score": score,
            "candidate": c,
            "delta": delta,
            "buy": buy,
            "sell": sell,
            "spread": spread,
            "book": book,
            "oi": oi,
            "lm": hm,
            "error": None,
        }

    except Exception as e:
        return {
            "symbol": symbol,
            "error": str(e),
        }

# ============================================================
# MAIN SCAN
# ============================================================

def scan_once():
    global _scan_aggregate_map

    scan_start = time.time()
    scan_log("================================")
    scan_log("COINGECKO AGGREGATED SCAN START")

    with state_lock:
        state["status"] = f"Scanning Top {TOP_COINS} aggregated crypto markets..."
        state["last_error"] = None
        state["scan_progress"] = "0/0"

    scan_log("Checking open trades...")
    check_open_trades()
    check_forming_setups()

    scan_log("Loading aggregated crypto / derivatives data...")

    try:
        aggregates = all_aggregates()
        _scan_aggregate_map = {x["symbol"]: x for x in aggregates}
        symbols = [x["symbol"] for x in aggregates[:TOP_COINS]]
        if not symbols:
            raise RuntimeError("No aggregate symbols available")
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        scan_log(f"TOP SYMBOL LOAD ERROR: {error}")

        with state_lock:
            state["last_error"] = error
            state["status"] = "Aggregate data load error — retrying"
            state["last_scan"] = datetime.now(timezone.utc).isoformat()
            state["last_scan_seconds"] = round(time.time() - scan_start, 2)

        save_state_snapshot()
        return

    with state_lock:
        state["scan_progress"] = f"0/{len(symbols)}"

    # LIGHT SCAN — bulk snapshot only; no per-coin API calls.
    light_candidates = []
    light_errors = []

    with ThreadPoolExecutor(max_workers=max(1, LIGHT_SCAN_WORKERS)) as pool:
        futures = {pool.submit(_light_scan_one, s): s for s in symbols}
        done = 0

        for future in as_completed(futures):
            done += 1
            result = future.result()

            if result:
                rank, symbol, lm, err = result
                if rank is not None and lm is not None:
                    light_candidates.append((rank, symbol, lm))
                elif err:
                    light_errors.append(f"{symbol}: {err}")

            if done == 1 or done % 10 == 0 or done == len(symbols):
                scan_log(f"LIGHT SCAN: {done}/{len(symbols)}")

            with state_lock:
                state["scan_progress"] = f"{done}/{len(symbols)}"

    light_candidates.sort(reverse=True, key=lambda x: x[0])
    deep_items = light_candidates[:DEEP_CHECK]

    scan_log(f"DEEP SCAN START: {len(deep_items)} candidates")

    alerts = 0
    best = None
    deep_errors = []
    deep_scored = []

    with ThreadPoolExecutor(max_workers=max(1, DEEP_SCAN_WORKERS)) as pool:
        futures = {pool.submit(_deep_scan_one, item): item[1] for item in deep_items}

        for future in as_completed(futures):
            result = future.result()
            symbol = result.get("symbol")

            if result.get("error"):
                deep_errors.append(f"{symbol}: {result['error']}")
                continue

            lm = result.get("lm") or {}
            side = result.get("side")
            score = int(result.get("score") or 0)

            watch_item = {
                "symbol": symbol,
                "rsi_15m": lm.get("rsi_15m"),
                "rsi_1h": lm.get("rsi_1h"),
                "rsi_4h": lm.get("rsi_4h"),
                "rsi_bias": lm.get("rsi_bias"),
                "rsi_rank": round(float(lm.get("rsi_rank") or 0), 2),
                "score": score,
                "risk_label": risk_label(score),
                "book_imb": result.get("book", 0),
                "flow_delta": result.get("delta", 0),
            }
            deep_scored.append(watch_item)

            c = result.get("candidate")
            if not c:
                continue

            if best is None or c["score"] > best["score"]:
                best = c

            if 60 <= c["score"] < MIN_SCORE:
                add_forming_setup(c)

            if (
                c["score"] >= MIN_SCORE
                and c["hard_confirm"]
                and not has_recent_or_open_trade(c["symbol"], c["signal"])
            ):
                save_signal(c)
                add_open_trade(c)

                with state_lock:
                    state["latest_signal"] = c

                p = performance()

                telegram_async(
                    f"🚨 RAZA SHAH SIGNAL — TRADE READY\n\n"
                    f"Data: CoinGecko Aggregated\n"
                    f"Coin: {c['symbol']}\n"
                    f"Side: {'LONG' if c['signal']=='BUY' else 'SHORT'}\n"
                    f"Score: {c['score']}/100\n"
                    f"Risk: {c['risk_label']}\n\n"
                    f"Entry: {c['price']:.8g}\n"
                    f"TP: {c['tp']:.8g}\n"
                    f"SL: {c['sl']:.8g}\n\n"
                    f"Flow: {c['flow_delta']:+.3f}\n"
                    f"Liquidity Pressure: {c['book_imb']:+.3f}\n"
                    f"Aggregate Spread: {c['spread_bps']:.2f} bps\n"
                    f"OI Change: {c['oi_change_pct']:+.3f}%\n\n"
                    f"Closed Trades: {p['total_trades']}\n"
                    f"Win Rate: {p['win_rate']:.2f}%\n"
                    f"🔗 {APP_URL}"
                )

                alerts += 1

    long_watch = [x for x in deep_scored if x.get("rsi_bias") == "BUY"]
    short_watch = [x for x in deep_scored if x.get("rsi_bias") == "SELL"]

    long_watch.sort(
        key=lambda x: (
            x.get("score", 0),
            -(x.get("rsi_15m") if x.get("rsi_15m") is not None else 999)
        ),
        reverse=True,
    )

    short_watch.sort(
        key=lambda x: (
            x.get("score", 0),
            x.get("rsi_15m") if x.get("rsi_15m") is not None else -1
        ),
        reverse=True,
    )

    now = datetime.now(timezone.utc)
    elapsed = round(time.time() - scan_start, 2)

    with state_lock:
        state["rsi_watchlist"] = {
            "oversold_long": long_watch[:10],
            "overbought_short": short_watch[:10],
        }
        state["best_candidate"] = best
        state["last_scan"] = now.isoformat()
        state["alerts_last_scan"] = alerts
        state["last_scan_seconds"] = elapsed

        if alerts:
            state["status"] = f"{alerts} verified 85+ trade ready"
        elif best:
            state["status"] = (
                f"Best forming: {best['symbol']} "
                f"{'LONG' if best['signal']=='BUY' else 'SHORT'} "
                f"{best['score']}/100"
            )
        else:
            state["status"] = "Waiting for 85+ setup"

        if not best and (light_errors or deep_errors):
            state["last_error"] = " | ".join((light_errors + deep_errors)[:3])

    save_state_snapshot()

    scan_log(
        f"SCAN COMPLETE in {elapsed}s | alerts={alerts} "
        f"| best={best['symbol'] if best else 'NONE'}"
    )

# ============================================================
# SECURITY / OTP
# ============================================================

otp_lock = threading.Lock()
otp_by_ip = {}
authorized_ips = {}


def client_ip():
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return (request.remote_addr or "unknown").strip()


def cleanup_access():
    now = time.time()
    with otp_lock:
        for ip in list(otp_by_ip):
            if otp_by_ip[ip].get("expires", 0) <= now:
                otp_by_ip.pop(ip, None)

        for ip in list(authorized_ips):
            if authorized_ips[ip] <= now:
                authorized_ips.pop(ip, None)


def ensure_otp_for_ip(ip):
    cleanup_access()
    now = time.time()

    with otp_lock:
        existing = otp_by_ip.get(ip)
        if existing and existing.get("expires", 0) > now:
            return existing["code"], False

        code = f"{secrets.randbelow(1000000):06d}"
        otp_by_ip[ip] = {
            "code": code,
            "expires": now + OTP_TTL_SECONDS,
            "attempts": 0,
        }

    telegram_async(
        f"🔐 RAZA SHAH SIGNAL\n"
        f"ACCESS REQUEST\n\n"
        f"Permission Code: {code}\n"
        f"IP: {ip}\n"
        f"Valid: {OTP_TTL_SECONDS//60} minutes\n"
        f"Access after approval: 24 hours\n\n"
        f"🔗 {APP_URL}"
    )

    return code, True


def is_authorized():
    cleanup_access()
    ip = client_ip()
    now = time.time()

    with otp_lock:
        if authorized_ips.get(ip, 0) > now:
            return True

    auth_until = float(flask_session.get("authorized_until", 0) or 0)
    auth_ip = flask_session.get("authorized_ip")

    return auth_ip == ip and auth_until > now


def login_html(message=""):
    return f"""
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RAZA SHAH SIGNAL — Secure Access</title>
<style>
body{{font-family:Arial;background:#07111f;color:#fff;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}}
.box{{width:min(92%,420px);background:#101d2e;padding:28px;border-radius:18px;text-align:center}}
input{{font-size:24px;letter-spacing:6px;width:85%;padding:14px;text-align:center;border-radius:10px;border:1px solid #445;background:#07111f;color:#fff}}
button{{margin-top:14px;padding:13px 24px;border:0;border-radius:10px;font-weight:bold;cursor:pointer}}
.msg{{margin:12px;color:#ffcc66}}.small{{opacity:.7;font-size:13px}}
</style>
</head>
<body>
<div class="box">
<h2>🔐 RAZA SHAH SIGNAL</h2>
<p><b>Waiting for Admin Permission</b></p>
<p>Enter 6-Digit Permission Code</p>
<div class="msg">{message}</div>
<form method="post" action="/verify">
<input name="code" inputmode="numeric" maxlength="6" placeholder="000000" required>
<br>
<button type="submit">REQUEST ACCESS</button>
</form>
<p class="small">One code per IP. After approval, this IP stays authorized for 24 hours.</p>
</div>
</body>
</html>
"""

# ============================================================
# BACKGROUND LOOPS
# ============================================================

def telegram_hourly_status_loop():
    while True:
        time.sleep(TELEGRAM_STATUS_INTERVAL)

        try:
            p = performance()
            c = capital_summary()

            with state_lock:
                st = dict(state)

            best = st.get("best_candidate")
            best_line = "Best Candidate: none"

            if best:
                best_line = (
                    f"Best Candidate: {best.get('symbol')} | "
                    f"{'LONG' if best.get('signal')=='BUY' else 'SHORT'} | "
                    f"{best.get('score',0)}/100"
                )

            telegram_async(
                f"🟢 RAZA SHAH SIGNAL\n"
                f"1 HOUR STATUS\n\n"
                f"Data: CoinGecko Aggregated\n"
                f"Scanner: {'LIVE' if st.get('running') else 'OFFLINE'}\n"
                f"Status: {st.get('status') or '—'}\n"
                f"{best_line}\n"
                f"Last scan alerts: {st.get('alerts_last_scan',0)}\n"
                f"Last scan time: {st.get('last_scan_seconds','—')}s\n"
                f"Progress: {st.get('scan_progress','—')}\n\n"
                f"Trades: {p['total_trades']}\n"
                f"Wins: {p['wins']}\n"
                f"Losses: {p['losses']}\n"
                f"Winning: {p['win_rate']:.2f}%\n"
                f"Open: {p['open_trades']}\n\n"
                f"Today P/L: ${c['net_pl']:.2f}\n"
                f"Capital: ${c['ending_capital']:.2f}\n"
                f"🔗 {APP_URL}"
            )

        except Exception as e:
            scan_log(f"HOURLY STATUS ERROR: {e}")


def trade_monitor_loop():
    scan_log("LIVE TP/SL MONITOR STARTED")

    while True:
        try:
            check_open_trades()
            check_forming_setups()
        except Exception as e:
            scan_log(f"TP/SL MONITOR ERROR: {type(e).__name__}: {e}")

        time.sleep(TRADE_MONITOR_INTERVAL)


def scanner_loop():
    with state_lock:
        state["running"] = True
        state["exchange"] = "AGGREGATED"
        state["data_source"] = "CoinGecko"

    save_state_snapshot()

    p = performance()

    telegram_async(
        f"🟢 RAZA SHAH SIGNAL\n"
        f"LIVE ACTIVE\n\n"
        f"Data Source: CoinGecko Aggregated\n"
        f"Top Coins: {TOP_COINS}\n"
        f"Deep Scan: {DEEP_CHECK}\n"
        f"Verified Score: {MIN_SCORE}+\n"
        f"Scan interval: {SCAN_INTERVAL//60} minutes\n"
        f"Paper Tester: ${TEST_START_CAPITAL:.0f} @ {TEST_LEVERAGE:.0f}x\n"
        f"Hourly status: ON\n\n"
        f"All-Time: {p['total_trades']} trades\n"
        f"Winning: {p['win_rate']:.2f}%\n\n"
        f"🔗 {APP_URL}"
    )

    while True:
        start = time.time()

        try:
            scan_once()

        except Exception as e:
            scan_log(f"SCAN LOOP ERROR: {type(e).__name__}: {e}")

            with state_lock:
                state["last_error"] = str(e)
                state["status"] = "Scan error — retrying"

        wait = max(10, SCAN_INTERVAL - (time.time() - start))

        with state_lock:
            state["next_scan"] = datetime.fromtimestamp(
                time.time() + wait,
                timezone.utc,
            ).isoformat()

        save_state_snapshot()
        time.sleep(wait)

# ============================================================
# WEB ROUTES
# ============================================================

@app.route("/", methods=["GET", "HEAD"])
def home():
    if request.method == "HEAD":
        return Response(status=200)

    if not is_authorized():
        ip = client_ip()
        _, created = ensure_otp_for_ip(ip)

        msg = (
            "Permission code sent to Telegram."
            if created
            else "Use the active Telegram code already sent for this IP."
        )

        return login_html(msg)

    return render_template("index.html")


@app.route("/verify", methods=["POST"])
def verify():
    ip = client_ip()
    code = (request.form.get("code") or "").strip()

    cleanup_access()
    now = time.time()

    with otp_lock:
        rec = otp_by_ip.get(ip)

        if not rec or rec.get("expires", 0) <= now:
            otp_by_ip.pop(ip, None)
            return login_html(
                "Code expired. Reload once for a new code."
            ), 401

        if rec.get("attempts", 0) >= OTP_MAX_ATTEMPTS:
            otp_by_ip.pop(ip, None)
            return login_html(
                "Too many attempts. Reload once."
            ), 401

        rec["attempts"] = rec.get("attempts", 0) + 1

        if code != rec.get("code"):
            return login_html("Wrong permission code."), 401

        auth_until = now + ACCESS_TTL_SECONDS
        authorized_ips[ip] = auth_until
        otp_by_ip.pop(ip, None)

    flask_session.permanent = True
    flask_session["authorized_ip"] = ip
    flask_session["authorized_until"] = auth_until

    telegram_async(
        f"✅ RAZA SHAH SIGNAL — ACCESS GRANTED\n"
        f"IP: {ip}\n"
        f"Access valid: 24 hours\n"
        f"🔗 {APP_URL}"
    )

    return redirect(url_for("home"))


@app.route("/logout")
def logout():
    ip = client_ip()

    with otp_lock:
        authorized_ips.pop(ip, None)
        otp_by_ip.pop(ip, None)

    flask_session.clear()
    return redirect(url_for("home"))


@app.route("/api/status")
def api_status():
    if not is_authorized():
        return jsonify({"error": "unauthorized"}), 401

    with state_lock:
        x = dict(state)

    persisted = load_state_snapshot()
    if persisted:
        if (
            not x.get("last_scan")
            or (
                persisted.get("last_scan")
                and str(persisted.get("last_scan")) > str(x.get("last_scan") or "")
            )
        ):
            x.update(persisted)

    x["signals"] = recent_signals(20)
    x["telegram"] = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    x["min_score"] = MIN_SCORE
    x["performance"] = performance()
    x["forming_performance"] = forming_performance()
    x["forming_history"] = list(reversed(forming_rows()[-20:]))
    x["capital_summary"] = capital_summary()

    x["dashboard"] = {
        "risk_rules": {
            "risky": "60-69",
            "medium": "70-84",
            "strong": "85+",
            "trade_ready": "85+ AND hard confirmation",
        },
        "market_data": (
            "CoinGecko aggregated market and derivatives data. "
            "No direct Binance or Bybit API connection."
        ),
        "disclaimer": (
            "Trading involves significant risk. Signals are informational only. "
            "No profit or performance is guaranteed."
        ),
    }

    return jsonify(x)


@app.route("/api/signal")
def api_signal():
    if not is_authorized():
        return jsonify({"error": "unauthorized"}), 401

    with state_lock:
        x = dict(state)

    x["performance"] = performance()
    x["forming_performance"] = forming_performance()
    x["forming_history"] = list(reversed(forming_rows()[-20:]))
    x["capital_summary"] = capital_summary()
    return jsonify(x)


@app.route("/api/live-market/<symbol>")
def api_live_market(symbol):
    if not is_authorized():
        return jsonify({"error": "unauthorized"}), 401

    symbol = "".join(ch for ch in str(symbol or "").upper() if ch.isalnum())
    if not symbol.endswith("USDT"):
        return jsonify({"error": "invalid symbol"}), 400

    try:
        hm = historical_metrics(symbol)
        agg = aggregate_for_symbol(symbol) or {}

        spread, pressure, bids, asks = aggregate_liquidity_metrics(symbol, hm)

        buy_pct = max(0.0, min(100.0, (pressure + 1.0) * 50.0))
        sell_pct = 100.0 - buy_pct

        return jsonify({
            "symbol": symbol,
            "data_source": "CoinGecko Aggregated",
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "price": current_price(symbol),
            "rsi_15m": hm["rsi_15m"],
            "rsi_1h": hm["rsi_1h"],
            "rsi_4h": hm["rsi_4h"],
            "bids": bids,
            "asks": asks,
            "bid_usd": sum(float(x.get("usd") or 0) for x in bids),
            "ask_usd": sum(float(x.get("usd") or 0) for x in asks),
            "buy_pct": round(buy_pct, 2),
            "sell_pct": round(sell_pct, 2),
            "book_imb": round(pressure, 6),
            "spread_bps": round(spread, 4),
            "flow_delta": round(hm["flow_delta"], 6),
            "buy_usd_60s": hm["buy_usd"],
            "sell_usd_60s": hm["sell_usd"],
            "oi_change_pct": round(oi_change_pct(symbol), 6),
            "aggregate_open_interest": float(agg.get("open_interest") or 0),
            "aggregate_volume_24h": float(agg.get("volume_24h") or 0),
            "funding_rate": float(agg.get("funding_rate") or 0),
            "basis": float(agg.get("basis") or 0),
        })

    except Exception as e:
        return jsonify({
            "error": f"{type(e).__name__}: {e}"
        }), 500


@app.route("/manifest.webmanifest")
def manifest():
    return send_from_directory(
        "static",
        "manifest.webmanifest",
        mimetype="application/manifest+json",
    )


@app.route("/sw.js")
def sw():
    return send_from_directory(
        "static",
        "sw.js",
        mimetype="application/javascript",
    )

# ============================================================
# START THREADS
# ============================================================

init_db()

threading.Thread(target=scanner_loop, daemon=True).start()
threading.Thread(target=telegram_hourly_status_loop, daemon=True).start()
threading.Thread(target=trade_monitor_loop, daemon=True).start()

# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
    )
