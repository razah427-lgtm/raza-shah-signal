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
# RAZA SHAH SIGNAL — V4 REFINED MARKET-LOGIC ENGINE
# BITGET USDT-M FUTURES
# AUTO PAPER-TRADE SIGNAL ENGINE — NO REAL ORDERS
#
# CORE DECISION FLOW (hard gates, not prediction):
# TIME
# -> 4H REGIME
# -> 1H BIAS / STRUCTURE
# -> 15M LOCATION
# -> LIQUIDITY EVENT / BREAKOUT ACCEPTANCE
# -> DISPLACEMENT / REACTION
# -> 5M STRUCTURE CONFIRMATION
# -> LIVE FLOW / ORDER BOOK / OI
# -> STRUCTURAL + ATR RISK
# -> QUALITY SCORE (ranking only)
# -> PAPER SIGNAL
#
# Philosophy:
# Location tells us WHERE to watch.
# Liquidity tells us WHAT happened.
# Reaction tells us WHO responded.
# Structure tells us whether response gained control.
# Risk decides whether we are allowed to participate.
# ============================================================

STRATEGY_VERSION = "V4_LOGIC"

BITGET_BASE = "https://api.bitget.com"
BITGET_PRODUCT_TYPE = "usdt-futures"
DATABASE_URL = os.getenv("DATABASE_URL")

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "300"))
# Scan every eligible Bitget USDT-M futures symbol by default.
# Set SCAN_ALL_COINS=false to fall back to TOP_COINS / DEEP_CHECK limits.
SCAN_ALL_COINS = (
    os.getenv("SCAN_ALL_COINS", "true").strip().lower()
    in ("1", "true", "yes", "on")
)
TOP_COINS = int(os.getenv("TOP_COINS", "20"))
DEEP_CHECK = int(os.getenv("DEEP_CHECK", "20"))
WATCHLIST_LIMIT = int(os.getenv("WATCHLIST_LIMIT", "20"))

LIGHT_SCAN_WORKERS = int(os.getenv("LIGHT_SCAN_WORKERS", "8"))
DEEP_SCAN_WORKERS = int(os.getenv("DEEP_SCAN_WORKERS", "4"))
SCAN_HTTP_TIMEOUT = float(os.getenv("SCAN_HTTP_TIMEOUT", "12"))
SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "14400"))
TRADE_MONITOR_INTERVAL = int(os.getenv("TRADE_MONITOR_INTERVAL", "20"))

# ----------------------------
# HARD MARKET-MICROSTRUCTURE FILTERS
# ----------------------------
MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "3.0"))
MIN_FLOW_DELTA = float(os.getenv("MIN_FLOW_DELTA", "0.08"))
MIN_BOOK_IMB = float(os.getenv("MIN_BOOK_IMB", "0.08"))
MIN_OI_CHANGE_PCT = float(os.getenv("MIN_OI_CHANGE_PCT", "-0.20"))

# ----------------------------
# VOLATILITY / PARTICIPATION
# ----------------------------
MIN_VOL_RATIO = float(os.getenv("MIN_VOL_RATIO", "0.85"))
TREND_MIN_VOL_RATIO = float(os.getenv("TREND_MIN_VOL_RATIO", "0.90"))
BREAKOUT_MIN_VOL_RATIO = float(os.getenv("BREAKOUT_MIN_VOL_RATIO", "1.15"))
REVERSAL_MIN_VOL_RATIO = float(os.getenv("REVERSAL_MIN_VOL_RATIO", "1.00"))

MIN_ATR_PCT = float(os.getenv("MIN_ATR_PCT", "0.0010"))   # 0.10%
MAX_ATR_PCT = float(os.getenv("MAX_ATR_PCT", "0.0300"))   # 3.00%

# ----------------------------
# OBJECTIVE REGIME LOGIC
# ----------------------------
REGIME_TREND_EMA_SEP_MIN = float(os.getenv("REGIME_TREND_EMA_SEP_MIN", "0.0010"))
REGIME_TREND_SLOPE_MIN = float(os.getenv("REGIME_TREND_SLOPE_MIN", "0.0005"))
REGIME_CHOP_EMA_SEP_MAX = float(os.getenv("REGIME_CHOP_EMA_SEP_MAX", "0.0007"))
REGIME_EXPANSION_ATR_MULT = float(os.getenv("REGIME_EXPANSION_ATR_MULT", "1.35"))
REGIME_EXPANSION_RANGE_MULT = float(os.getenv("REGIME_EXPANSION_RANGE_MULT", "1.40"))

# ----------------------------
# LOCATION / LIQUIDITY / ACCEPTANCE
# ----------------------------
LIQ_LOOKBACK = int(os.getenv("LIQ_LOOKBACK", "24"))
LOCATION_ATR_DISTANCE = float(os.getenv("LOCATION_ATR_DISTANCE", "0.75"))
MAX_EXTENSION_ATR = float(os.getenv("MAX_EXTENSION_ATR", "1.75"))

SWEEP_MIN_ATR = float(os.getenv("SWEEP_MIN_ATR", "0.05"))
SWEEP_MAX_ATR = float(os.getenv("SWEEP_MAX_ATR", "0.70"))
RECLAIM_BUFFER_ATR = float(os.getenv("RECLAIM_BUFFER_ATR", "0.03"))
BREAKOUT_BUFFER_ATR = float(os.getenv("BREAKOUT_BUFFER_ATR", "0.10"))
RETEST_MAX_ATR = float(os.getenv("RETEST_MAX_ATR", "0.45"))

# ----------------------------
# DISPLACEMENT / 5M STRUCTURE CONFIRMATION
# ----------------------------
DISPLACEMENT_BODY_ATR = float(os.getenv("DISPLACEMENT_BODY_ATR", "0.55"))
DISPLACEMENT_RANGE_ATR = float(os.getenv("DISPLACEMENT_RANGE_ATR", "0.80"))
DISPLACEMENT_CLOSE_LOC = float(os.getenv("DISPLACEMENT_CLOSE_LOC", "0.68"))
STRUCTURE_LOOKBACK_5M = int(os.getenv("STRUCTURE_LOOKBACK_5M", "10"))
STRUCTURE_BREAK_BUFFER_ATR = float(os.getenv("STRUCTURE_BREAK_BUFFER_ATR", "0.03"))

# ----------------------------
# RSI IS SUPPORTIVE ONLY — NEVER THE PRIMARY DIRECTION ENGINE
# ----------------------------
RSI_OVERBOUGHT = float(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "30"))
RSI_EXTREME_OVERBOUGHT = float(os.getenv("RSI_EXTREME_OVERBOUGHT", "80"))
RSI_EXTREME_OVERSOLD = float(os.getenv("RSI_EXTREME_OVERSOLD", "20"))

# ----------------------------
# DYNAMIC RISK
# ----------------------------
RR_TARGET = float(os.getenv("RR_TARGET", "1.80"))
MIN_STOP_PCT = float(os.getenv("MIN_STOP_PCT", "0.0025"))  # 0.25%
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "0.0080"))  # 0.80%
ATR_STOP_MULT = float(os.getenv("ATR_STOP_MULT", "1.10"))
STRUCTURAL_STOP_BUFFER_ATR = float(os.getenv("STRUCTURAL_STOP_BUFFER_ATR", "0.12"))

# ----------------------------
# BTC MARKET FILTER
# ----------------------------
BTC_FILTER_ENABLED = (
    os.getenv("BTC_FILTER_ENABLED", "true").strip().lower()
    in ("1", "true", "yes", "on")
)

# ----------------------------
# TIME GATE — OPTIONAL KSA SESSION
# ----------------------------
SESSION_FILTER_ENABLED = (
    os.getenv("SESSION_FILTER_ENABLED", "false").strip().lower()
    in ("1", "true", "yes", "on")
)
KSA_SESSION_START = int(os.getenv("KSA_SESSION_START", "15"))
KSA_SESSION_END = int(os.getenv("KSA_SESSION_END", "22"))

# Quality score is DISPLAY / RANKING ONLY. Hard gates create trades.
STRONG_QUALITY_SCORE = int(os.getenv("STRONG_QUALITY_SCORE", "85"))
MIN_READY_SCORE = int(os.getenv("MIN_READY_SCORE", "70"))

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

# ============================================================
# FILES
# ============================================================

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)

LIVE_FILE = DATA_DIR / "live_signals_v4.csv"
STATE_FILE = DATA_DIR / "scanner_state_v4.json"

CSV_COLUMNS = [
    "time_utc",
    "strategy_version",
    "symbol",
    "signal",
    "score",
    "price",
    "strategy_type",
    "regime_4h",
    "structure_1h",
    "bias_1h",
    "setup_15m",
    "location_15m",
    "liquidity_event_15m",
    "liquidity_tier",
    "acceptance_15m",
    "rejection_15m",
    "reaction_15m",
    "trigger_5m",
    "flow_delta",
    "buy_usd_60s",
    "sell_usd_60s",
    "spread_bps",
    "book_imb",
    "oi_change_pct",
    "atr_15m_pct",
    "vol_ratio",
    "risk_pct",
    "rr",
    "structural_level",
    "tp",
    "sl",
]

# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()
session.headers.update({
    "User-Agent": "RAZA-SHAH-SIGNAL-BITGET-V4/1.0",
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

                # Safe migration: preserve old rows and mark only new versioned rows.
                cur.execute("""
                    ALTER TABLE trade_results
                    ADD COLUMN IF NOT EXISTS strategy_version TEXT
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

                cur.execute("""
                    ALTER TABLE forming_results
                    ADD COLUMN IF NOT EXISTS strategy_version TEXT
                """)

            conn.commit()

        print("[DB] PostgreSQL tables ready for V4", flush=True)

    except Exception as e:
        print(f"[DB] INIT ERROR: {type(e).__name__}: {e}", flush=True)

# ============================================================
# STATE
# ============================================================

state_lock = threading.Lock()

state = {
    "running": False,
    "strategy_version": STRATEGY_VERSION,
    "exchange": "BITGET",
    "data_source": "Bitget USDT-M Futures",
    "status": "Starting V4 refined market logic...",
    "last_scan": None,
    "next_scan": None,
    "alerts_last_scan": 0,
    "latest_signal": None,
    "best_candidate": None,
    "scan_progress": "0/0",
    "last_scan_seconds": None,
    "last_error": None,
    "market_regime": None,
    "blocked_counts": {},
    "watchlist": [],
    "oi_snapshot": {},
}


def save_state_snapshot():
    try:
        with state_lock:
            payload = dict(state)

        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, default=str),
            encoding="utf-8"
        )
        tmp.replace(STATE_FILE)

    except Exception as e:
        print(f"[SCANNER] STATE SAVE ERROR: {e}", flush=True)


def load_state_snapshot():
    try:
        if not STATE_FILE.exists():
            return None

        data = json.loads(
            STATE_FILE.read_text(encoding="utf-8")
        )
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
            telegram_log(
                f"FAILED HTTP {r.status_code} | {r.text[:300]}"
            )
            return False

        telegram_log("Message sent OK")
        return True

    except Exception as e:
        telegram_log(f"ERROR {type(e).__name__}: {e}")
        return False


def telegram_async(text):
    threading.Thread(
        target=telegram,
        args=(text,),
        daemon=True,
    ).start()

# ============================================================
# BITGET FUTURES PUBLIC API
# ============================================================

def bitget_get(path, params=None, timeout=None, retries=3):
    if timeout is None:
        timeout = SCAN_HTTP_TIMEOUT

    url = BITGET_BASE + path
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            r = session.get(
                url,
                params=params or {},
                timeout=timeout,
            )

            if r.status_code == 429:
                wait = min(2 ** attempt, 8)
                scan_log(
                    f"BITGET RATE LIMIT HTTP 429 — retry in {wait}s"
                )
                time.sleep(wait)
                continue

            r.raise_for_status()
            payload = r.json()

            if not isinstance(payload, dict):
                raise RuntimeError("Bitget returned non-JSON object")

            code = str(payload.get("code") or "")
            if code and code != "00000":
                raise RuntimeError(
                    f"Bitget API code={code} msg={payload.get('msg')}"
                )

            return payload.get("data")

        except Exception as e:
            last_error = e

            if attempt < retries:
                time.sleep(min(attempt, 3))

    raise RuntimeError(
        f"Bitget request failed {path}: "
        f"{type(last_error).__name__}: {last_error}"
    )


def top_symbols():
    scan_log("TOP SYMBOLS: loading Bitget USDT-M Futures tickers...")

    tickers = bitget_get(
        "/api/v2/mix/market/tickers",
        {"productType": BITGET_PRODUCT_TYPE},
    )

    if not isinstance(tickers, list):
        raise RuntimeError("Bitget ticker list unavailable")

    rows = []

    for x in tickers:
        try:
            symbol = str(x.get("symbol") or "").upper()

            last_price = float(
                x.get("lastPr")
                or x.get("last")
                or x.get("close")
                or 0
            )

            quote_volume = float(
                x.get("quoteVolume")
                or x.get("usdtVolume")
                or x.get("turnover24h")
                or 0
            )

            if not symbol.endswith("USDT"):
                continue

            if last_price <= 0 or quote_volume <= 0:
                continue

            rows.append((symbol, quote_volume))

        except Exception:
            pass

    rows.sort(key=lambda z: z[1], reverse=True)

    # ALL-COINS mode ignores TOP_COINS, including any old Render env value.
    # We still sort by 24h quote volume so the busiest markets are submitted first.
    if SCAN_ALL_COINS:
        symbols = [symbol for symbol, _ in rows]
    else:
        symbols = [symbol for symbol, _ in rows[:max(1, TOP_COINS)]]

    if not symbols:
        raise RuntimeError("No Bitget top futures symbols loaded")

    scan_log(f"SYMBOLS LOADED: {len(symbols)} ({'ALL' if SCAN_ALL_COINS else f'TOP {TOP_COINS}'})")
    return symbols


def _bitget_granularity(interval):
    mapping = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "6h": "6H",
        "12h": "12H",
        "1d": "1D",
    }
    return mapping.get(str(interval), str(interval))


def klines(symbol, interval="5m", limit=100):
    data = bitget_get(
        "/api/v2/mix/market/candles",
        {
            "symbol": symbol,
            "productType": BITGET_PRODUCT_TYPE,
            "granularity": _bitget_granularity(interval),
            "limit": str(limit),
        },
    )

    if not isinstance(data, list):
        return []

    return sorted(data, key=lambda x: int(x[0]))


def candle_dicts(symbol, interval, limit=100, drop_live=True):
    rows = klines(symbol, interval, limit)

    out = []
    for x in rows:
        try:
            if len(x) < 7:
                continue

            out.append({
                "ts": int(x[0]),
                "open": float(x[1]),
                "high": float(x[2]),
                "low": float(x[3]),
                "close": float(x[4]),
                "base_volume": float(x[5]),
                "quote_volume": float(x[6]),
            })
        except Exception:
            pass

    # Public REST candle can include the currently forming candle.
    # V4 uses completed bars only for HTF / setup decisions.
    if drop_live and len(out) >= 3:
        out = out[:-1]

    return out


def current_price(symbol):
    data = bitget_get(
        "/api/v2/mix/market/ticker",
        {
            "symbol": symbol,
            "productType": BITGET_PRODUCT_TYPE,
        },
    )

    row = None

    if isinstance(data, list) and data:
        row = data[0]
    elif isinstance(data, dict):
        row = data

    if not isinstance(row, dict):
        raise RuntimeError(f"No Bitget ticker for {symbol}")

    px = float(
        row.get("lastPr")
        or row.get("last")
        or row.get("close")
        or 0
    )

    if px <= 0:
        raise RuntimeError(f"No Bitget ticker price for {symbol}")

    return px

# ============================================================
# TECHNICAL HELPERS
# ============================================================

def ema(values, n):
    if not values:
        return 0.0

    a = 2 / (n + 1)
    e = values[0]

    for v in values[1:]:
        e = a * v + (1 - a) * e

    return e


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


def true_range_series(candles):
    out = []

    for i, c in enumerate(candles):
        if i == 0:
            tr = c["high"] - c["low"]
        else:
            pc = candles[i - 1]["close"]
            tr = max(
                c["high"] - c["low"],
                abs(c["high"] - pc),
                abs(c["low"] - pc),
            )

        out.append(max(0.0, tr))

    return out


def atr_value(candles, period=14):
    trs = true_range_series(candles)

    if len(trs) < period:
        return 0.0

    a = sum(trs[:period]) / period

    for tr in trs[period:]:
        a = ((a * (period - 1)) + tr) / period

    return a


def volume_ratio(candles, lookback=20):
    if len(candles) < lookback + 1:
        return 1.0

    current = candles[-1]["quote_volume"]
    prior = [c["quote_volume"] for c in candles[-lookback - 1:-1]]

    avg = sum(prior) / len(prior) if prior else 0.0

    return current / avg if avg > 0 else 1.0


def timeframe_rsi(symbol, interval, limit=80):
    candles = candle_dicts(symbol, interval, limit)

    closes = [c["close"] for c in candles]

    if len(closes) < 20:
        raise RuntimeError(
            f"Not enough {interval} candles for {symbol}"
        )

    return rsi(closes, 14)

# ============================================================
# V4 LAYER 1 — 4H MARKET REGIME
# ============================================================

def _candle_shape(c):
    rng = max(float(c["high"]) - float(c["low"]), 1e-12)
    body = abs(float(c["close"]) - float(c["open"]))
    close_loc = (float(c["close"]) - float(c["low"])) / rng
    return {
        "range": rng,
        "body": body,
        "body_frac": body / rng,
        "close_loc": close_loc,
    }


def _directional_displacement(candle, atr, side):
    """Objective reaction candle: meaningful body/range + close near directional extreme."""
    if atr <= 0:
        return False
    m = _candle_shape(candle)
    body_ok = m["body"] >= DISPLACEMENT_BODY_ATR * atr
    range_ok = m["range"] >= DISPLACEMENT_RANGE_ATR * atr
    if side == "BUY":
        direction_ok = candle["close"] > candle["open"] and m["close_loc"] >= DISPLACEMENT_CLOSE_LOC
    else:
        direction_ok = candle["close"] < candle["open"] and m["close_loc"] <= (1.0 - DISPLACEMENT_CLOSE_LOC)
    return bool(direction_ok and (body_ok or range_ok))


def regime_4h(symbol):
    c = candle_dicts(symbol, "4h", 140)
    if len(c) < 80:
        raise RuntimeError(f"Not enough 4H candles for {symbol}")

    closes = [x["close"] for x in c]
    price = closes[-1]
    e20 = ema(closes[-100:], 20)
    e50 = ema(closes[-120:], 50)
    e20_old = ema(closes[-100:-4], 20)
    slope = ((e20 / e20_old) - 1.0) if e20_old > 0 else 0.0
    separation = abs(e20 - e50) / price if price > 0 else 0.0

    atr_now = atr_value(c[-50:], 14)
    atr_old = atr_value(c[-70:-10], 14)
    atr_pct = atr_now / price if price > 0 else 0.0
    atr_expansion = (atr_now / atr_old) if atr_old > 0 else 1.0

    recent_ranges = [x["high"] - x["low"] for x in c[-12:-1]]
    avg_recent_range = sum(recent_ranges) / len(recent_ranges) if recent_ranges else atr_now
    last_range = c[-1]["high"] - c[-1]["low"]
    range_expansion = (last_range / avg_recent_range) if avg_recent_range > 0 else 1.0

    bull_trend = (
        e20 > e50 and price > e20
        and slope >= REGIME_TREND_SLOPE_MIN
        and separation >= REGIME_TREND_EMA_SEP_MIN
    )
    bear_trend = (
        e20 < e50 and price < e20
        and slope <= -REGIME_TREND_SLOPE_MIN
        and separation >= REGIME_TREND_EMA_SEP_MIN
    )

    expansion = (
        atr_expansion >= REGIME_EXPANSION_ATR_MULT
        or range_expansion >= REGIME_EXPANSION_RANGE_MULT
    )

    if expansion and c[-1]["close"] > c[-1]["open"] and price > e20:
        regime = "EXPANSION_BULL"
    elif expansion and c[-1]["close"] < c[-1]["open"] and price < e20:
        regime = "EXPANSION_BEAR"
    elif bull_trend:
        regime = "BULL_TREND"
    elif bear_trend:
        regime = "BEAR_TREND"
    elif separation <= REGIME_CHOP_EMA_SEP_MAX and atr_expansion < 0.95:
        regime = "CHOP"
    else:
        regime = "RANGE"

    return {
        "regime": regime,
        "price": price,
        "ema20": e20,
        "ema50": e50,
        "slope": slope,
        "ema_separation_pct": separation,
        "atr": atr_now,
        "atr_pct": atr_pct,
        "atr_expansion": atr_expansion,
        "range_expansion": range_expansion,
    }

# ============================================================
# V4 LAYER 2 — 1H STRUCTURE / BIAS
# ============================================================

def _pivot_points(candles, left=2, right=2):
    highs, lows = [], []
    for i in range(left, len(candles) - right):
        h = candles[i]["high"]
        l = candles[i]["low"]
        if all(h >= candles[j]["high"] for j in range(i-left, i+right+1) if j != i):
            highs.append((i, h))
        if all(l <= candles[j]["low"] for j in range(i-left, i+right+1) if j != i):
            lows.append((i, l))
    return highs, lows


def structure_1h(symbol):
    c = candle_dicts(symbol, "1h", 140)
    if len(c) < 70:
        raise RuntimeError(f"Not enough 1H candles for {symbol}")

    recent_c = c[-90:]
    highs, lows = _pivot_points(recent_c, 2, 2)
    closes = [x["close"] for x in recent_c]
    e20 = ema(closes[-60:], 20)
    e50 = ema(closes[-80:], 50)

    last_highs = highs[-2:]
    last_lows = lows[-2:]
    structure = "RANGE"

    if len(last_highs) >= 2 and len(last_lows) >= 2:
        h1, h2 = last_highs[-2][1], last_highs[-1][1]
        l1, l2 = last_lows[-2][1], last_lows[-1][1]
        if h2 > h1 and l2 > l1 and closes[-1] >= e20:
            structure = "HH_HL"
        elif h2 < h1 and l2 < l1 and closes[-1] <= e20:
            structure = "LH_LL"
        elif h2 > h1 and l2 < l1:
            structure = "EXPANDING_RANGE"
        else:
            structure = "RANGE"

    recent_high = max(x["high"] for x in recent_c[-24:])
    recent_low = min(x["low"] for x in recent_c[-24:])
    mid = (recent_high + recent_low) / 2.0
    price = closes[-1]

    if structure == "HH_HL":
        bias = "BUY"
    elif structure == "LH_LL":
        bias = "SELL"
    else:
        bias = "NEUTRAL"

    return {
        "structure": structure,
        "bias": bias,
        "recent_high": recent_high,
        "recent_low": recent_low,
        "range_mid": mid,
        "ema20": e20,
        "ema50": e50,
        "last_swing_high": last_highs[-1][1] if last_highs else recent_high,
        "prev_swing_high": last_highs[-2][1] if len(last_highs) >= 2 else recent_high,
        "last_swing_low": last_lows[-1][1] if last_lows else recent_low,
        "prev_swing_low": last_lows[-2][1] if len(last_lows) >= 2 else recent_low,
    }

# ============================================================
# V4 LAYER 3 — 15M LOCATION / LIQUIDITY / ACCEPTANCE / REACTION
# ============================================================

def _liquidity_context_15m(c):
    history = c[-(LIQ_LOOKBACK + 4):-4]
    if len(history) < max(10, LIQ_LOOKBACK // 2):
        history = c[-(LIQ_LOOKBACK + 2):-2]
    prior_high = max(x["high"] for x in history)
    prior_low = min(x["low"] for x in history)

    # Tier B: approximate equal highs/lows in the same history.
    tolerance = max((prior_high - prior_low) * 0.0015, 1e-12)
    highs = sorted([x["high"] for x in history], reverse=True)
    lows = sorted([x["low"] for x in history])
    equal_high = highs[1] if len(highs) > 1 and abs(highs[0]-highs[1]) <= tolerance else None
    equal_low = lows[1] if len(lows) > 1 and abs(lows[0]-lows[1]) <= tolerance else None

    return {
        "prior_high": prior_high,
        "prior_low": prior_low,
        "equal_high": equal_high,
        "equal_low": equal_low,
    }


def _sweep_reclaim(candle, level, atr, side):
    if atr <= 0 or level <= 0:
        return False, 0.0
    if side == "BUY":
        penetration = level - candle["low"]
        valid = (
            penetration >= SWEEP_MIN_ATR * atr
            and penetration <= SWEEP_MAX_ATR * atr
            and candle["close"] >= level + RECLAIM_BUFFER_ATR * atr
        )
    else:
        penetration = candle["high"] - level
        valid = (
            penetration >= SWEEP_MIN_ATR * atr
            and penetration <= SWEEP_MAX_ATR * atr
            and candle["close"] <= level - RECLAIM_BUFFER_ATR * atr
        )
    return bool(valid), max(0.0, penetration)


def _retest_hold(candle, level, atr, side):
    if atr <= 0 or level <= 0:
        return False
    if side == "BUY":
        distance = abs(candle["low"] - level)
        return bool(distance <= RETEST_MAX_ATR * atr and candle["close"] > level)
    distance = abs(candle["high"] - level)
    return bool(distance <= RETEST_MAX_ATR * atr and candle["close"] < level)


def liquidity_setup_15m(symbol, side):
    """Backward-compatible liquidity reader, now using ATR-normalized sweep rules."""
    c = candle_dicts(symbol, "15m", max(100, LIQ_LOOKBACK + 40))
    if len(c) < LIQ_LOOKBACK + 10:
        raise RuntimeError(f"Not enough 15M candles for {symbol}")

    ctx = _liquidity_context_15m(c)
    last = c[-1]
    prev = c[-2]
    price = last["close"]
    atr15 = atr_value(c[-50:], 14)
    atr_pct = atr15 / price if price > 0 else 0.0
    vr = volume_ratio(c, 20)
    volatility_ok = MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT

    level = ctx["prior_low"] if side == "BUY" else ctx["prior_high"]
    swept_last, _ = _sweep_reclaim(last, level, atr15, side)
    swept_prev, _ = _sweep_reclaim(prev, level, atr15, side)
    swept = swept_last or swept_prev
    retest = _retest_hold(last, level, atr15, side)
    reaction = _directional_displacement(last, atr15, side)

    valid = volatility_ok and (swept or retest) and reaction
    if swept:
        setup_name = "LOW_SWEEP_RECLAIM" if side == "BUY" else "HIGH_SWEEP_REJECT"
    elif retest:
        setup_name = "SUPPORT_RETEST_HOLD" if side == "BUY" else "RESISTANCE_RETEST_HOLD"
    else:
        setup_name = "NONE"

    sweep_extreme = min(last["low"], prev["low"]) if side == "BUY" else max(last["high"], prev["high"])
    return {
        "valid": bool(valid),
        "setup": setup_name,
        "price": price,
        "prior_high": ctx["prior_high"],
        "prior_low": ctx["prior_low"],
        "liquidity_level": level,
        "liquidity_tier": "A",
        "sweep_extreme": sweep_extreme,
        "event": "SWEEP" if swept else "RETEST" if retest else "NONE",
        "reaction": "DISPLACEMENT" if reaction else "WEAK",
        "atr": atr15,
        "atr_pct": atr_pct,
        "vol_ratio": vr,
        "volatility_ok": volatility_ok,
        "location_ok": True,
        "acceptance": False,
        "rejection": bool(swept),
    }

# ============================================================
# V4 LAYER 4 — 5M STRUCTURE CONFIRMATION / ENTRY TRIGGER
# ============================================================

def entry_trigger_5m(symbol, side):
    c = candle_dicts(symbol, "5m", 100)
    if len(c) < 40:
        raise RuntimeError(f"Not enough 5M candles for {symbol}")

    closes = [x["close"] for x in c]
    e9 = ema(closes[-50:], 9)
    e21 = ema(closes[-70:], 21)
    atr5 = atr_value(c[-45:], 14)
    last = c[-1]

    history = c[-(STRUCTURE_LOOKBACK_5M + 2):-2]
    recent_high = max(x["high"] for x in history)
    recent_low = min(x["low"] for x in history)
    buffer = STRUCTURE_BREAK_BUFFER_ATR * atr5
    displacement = _directional_displacement(last, atr5, side)

    if side == "BUY":
        structure_break = last["close"] > recent_high + buffer
        ema_ok = last["close"] > e9 and e9 >= e21
        trigger = structure_break and displacement and ema_ok
        name = "BULL_5M_STRUCTURE_BREAK" if trigger else "WAIT"
        invalidation = recent_low
    else:
        structure_break = last["close"] < recent_low - buffer
        ema_ok = last["close"] < e9 and e9 <= e21
        trigger = structure_break and displacement and ema_ok
        name = "BEAR_5M_STRUCTURE_BREAK" if trigger else "WAIT"
        invalidation = recent_high

    return {
        "valid": bool(trigger),
        "trigger": name,
        "structure_break": bool(structure_break),
        "displacement": bool(displacement),
        "ema_ok": bool(ema_ok),
        "recent_high": recent_high,
        "recent_low": recent_low,
        "invalidation": invalidation,
        "atr": atr5,
        "ema9": e9,
        "ema21": e21,
        "price": last["close"],
    }

# ============================================================
# ORDER BOOK
# ============================================================

def raw_order_book(symbol, limit=100):
    data = bitget_get(
        "/api/v2/mix/market/merge-depth",
        {
            "symbol": symbol,
            "productType": BITGET_PRODUCT_TYPE,
            "limit": str(limit),
            "precision": "scale0",
        },
    )

    return data if isinstance(data, dict) else {}


def depth_metrics(symbol, limit=100):
    data = raw_order_book(symbol, limit)

    bids = data.get("bids", [])
    asks = data.get("asks", [])

    if not bids or not asks:
        return None

    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    mid = (best_bid + best_ask) / 2

    spread_bps = (
        ((best_ask - best_bid) / mid) * 10000
        if mid
        else 999.0
    )

    # Use top part of book instead of letting far-away orders dominate.
    bids = bids[:30]
    asks = asks[:30]

    bid_usd = sum(
        float(price) * float(qty)
        for price, qty in bids
    )

    ask_usd = sum(
        float(price) * float(qty)
        for price, qty in asks
    )

    total = bid_usd + ask_usd

    book_imb = (
        (bid_usd - ask_usd) / total
        if total
        else 0.0
    )

    return spread_bps, book_imb

# ============================================================
# AGGRESSIVE TRADE FLOW
# ============================================================

def flow_metrics(symbol):
    trades = bitget_get(
        "/api/v2/mix/market/fills",
        {
            "symbol": symbol,
            "productType": BITGET_PRODUCT_TYPE,
            "limit": "100",
        },
    )

    if not isinstance(trades, list):
        trades = []

    cutoff = int(time.time() * 1000) - 60000

    buy = 0.0
    sell = 0.0

    for t in trades:
        try:
            trade_time = int(
                t.get("ts")
                or t.get("timestamp")
                or 0
            )

            if trade_time < cutoff:
                continue

            price = float(t.get("price") or 0)
            qty = float(
                t.get("size")
                or t.get("qty")
                or 0
            )

            usd = price * qty
            side = str(t.get("side") or "").lower()

            if side == "buy":
                buy += usd
            elif side == "sell":
                sell += usd

        except Exception:
            pass

    total = buy + sell

    delta = (
        (buy - sell) / total
        if total
        else 0.0
    )

    return delta, buy, sell

# ============================================================
# OPEN INTEREST CHANGE
# ============================================================

def oi_change_pct(symbol):
    data = bitget_get(
        "/api/v2/mix/market/open-interest",
        {
            "symbol": symbol,
            "productType": BITGET_PRODUCT_TYPE,
        },
    )

    current_oi = 0.0

    if isinstance(data, dict):
        oi_list = data.get("openInterestList")

        if isinstance(oi_list, list) and oi_list:
            row = oi_list[0]

            if isinstance(row, dict):
                current_oi = float(
                    row.get("size")
                    or row.get("openInterest")
                    or row.get("amount")
                    or 0
                )
        else:
            current_oi = float(
                data.get("size")
                or data.get("openInterest")
                or data.get("amount")
                or 0
            )

    elif isinstance(data, list) and data:
        row = data[0]

        if isinstance(row, dict):
            current_oi = float(
                row.get("size")
                or row.get("openInterest")
                or row.get("amount")
                or 0
            )

    if current_oi <= 0:
        return 0.0

    with state_lock:
        previous = float(
            state.setdefault("oi_snapshot", {}).get(symbol)
            or 0
        )

        state["oi_snapshot"][symbol] = current_oi

    if previous <= 0:
        return 0.0

    return ((current_oi / previous) - 1.0) * 100.0

# ============================================================
# SESSION / MARKET FILTERS
# ============================================================

def ksa_hour_now():
    return (datetime.now(timezone.utc) + timedelta(hours=3)).hour


def in_ksa_session():
    if not SESSION_FILTER_ENABLED:
        return True

    h = ksa_hour_now()

    if KSA_SESSION_START <= KSA_SESSION_END:
        return KSA_SESSION_START <= h < KSA_SESSION_END

    # Supports overnight windows.
    return h >= KSA_SESSION_START or h < KSA_SESSION_END


def btc_market_allows(side, btc_regime):
    if not BTC_FILTER_ENABLED:
        return True

    regime = str((btc_regime or {}).get("regime") or "RANGE")
    bearish = regime in ("BEAR_TREND", "EXPANSION_BEAR")
    bullish = regime in ("BULL_TREND", "EXPANSION_BULL")

    if side == "BUY":
        return not bearish
    return not bullish

# ============================================================
# V4 DYNAMIC RISK
# ============================================================

def dynamic_risk(entry, side, setup, trigger=None):
    """Structural stop first, ATR buffer second. Reject if stop becomes too wide."""
    atr15 = float(setup.get("atr") or 0)
    extreme = float(setup.get("sweep_extreme") or entry)
    trigger = trigger or {}
    trigger_invalid = float(trigger.get("invalidation") or 0)

    if entry <= 0 or atr15 <= 0:
        return None

    buffer = STRUCTURAL_STOP_BUFFER_ATR * atr15

    if side == "BUY":
        structural_level = min(x for x in [extreme, trigger_invalid or extreme] if x > 0)
        structural_sl = structural_level - buffer
        structural_dist = max(0.0, entry - structural_sl)
    else:
        structural_level = max(x for x in [extreme, trigger_invalid or extreme] if x > 0)
        structural_sl = structural_level + buffer
        structural_dist = max(0.0, structural_sl - entry)

    atr_dist = ATR_STOP_MULT * atr15
    raw_dist = max(atr_dist, structural_dist)
    raw_pct = raw_dist / entry

    if raw_pct > MAX_STOP_PCT:
        return None

    risk_pct = max(raw_pct, MIN_STOP_PCT)
    if risk_pct > MAX_STOP_PCT:
        return None

    risk_dist = entry * risk_pct
    if side == "BUY":
        sl = entry - risk_dist
        tp = entry + risk_dist * RR_TARGET
    else:
        sl = entry + risk_dist
        tp = entry - risk_dist * RR_TARGET

    return {
        "risk_pct": risk_pct,
        "risk_dist": risk_dist,
        "sl": sl,
        "tp": tp,
        "rr": RR_TARGET,
        "structural_level": structural_level,
    }

# ============================================================
# V4 QUALITY SCORE — RANKING ONLY
# ============================================================

def quality_score(
    side,
    regime,
    structure,
    setup,
    trigger,
    delta,
    book,
    spread,
    oi,
):
    score = 0

    reg = regime.get("regime")
    st = structure.get("structure")
    if side == "BUY" and reg in ("BULL_TREND", "EXPANSION_BULL"):
        score += 14
    elif side == "SELL" and reg in ("BEAR_TREND", "EXPANSION_BEAR"):
        score += 14
    elif reg == "RANGE" and setup.get("strategy_type") in ("REVERSAL", "BREAKOUT"):
        score += 10

    if (side == "BUY" and st == "HH_HL") or (side == "SELL" and st == "LH_LL"):
        score += 12
    elif setup.get("strategy_type") in ("REVERSAL", "BREAKOUT"):
        score += 6

    if setup.get("location_ok"):
        score += 10
    if setup.get("event") not in (None, "NONE"):
        score += 12
    if setup.get("acceptance") or setup.get("rejection"):
        score += 8
    if setup.get("reaction") == "DISPLACEMENT":
        score += 12
    if trigger.get("valid"):
        score += 12

    ad = abs(delta)
    if ad >= 0.50:
        score += 7
    elif ad >= 0.30:
        score += 6
    elif ad >= MIN_FLOW_DELTA:
        score += 4

    ab = abs(book)
    if ab >= 0.50:
        score += 7
    elif ab >= 0.30:
        score += 6
    elif ab >= MIN_BOOK_IMB:
        score += 4

    if spread <= 0.5:
        score += 4
    elif spread <= 1.0:
        score += 3
    elif spread <= MAX_SPREAD_BPS:
        score += 2

    vr = float(setup.get("vol_ratio") or 0)
    if vr >= 1.5:
        score += 5
    elif vr >= 1.1:
        score += 4
    elif vr >= MIN_VOL_RATIO:
        score += 2

    if oi > 0:
        score += 3
    elif oi >= MIN_OI_CHANGE_PCT:
        score += 1

    return min(100, int(round(score)))


def risk_label(score):
    if score >= 90:
        return "A+"
    if score >= STRONG_QUALITY_SCORE:
        return "STRONG"
    if score >= 75:
        return "GOOD"
    return "VALID"

# ============================================================
# V4 HARD LIVE CONFIRMATION
# ============================================================

def live_confirmation(side, delta, book, spread, oi):
    """
    V4 live confirmation. Spread and OI remain safety gates, while flow/book
    use a 1-of-2 directional vote. This avoids rejecting a valid setup just
    because one microstructure feed is temporarily neutral.
    """
    reasons = []

    if spread > MAX_SPREAD_BPS:
        reasons.append("SPREAD")

    if oi < MIN_OI_CHANGE_PCT:
        reasons.append("OI")

    if side == "BUY":
        flow_ok = delta >= MIN_FLOW_DELTA
        book_ok = book >= MIN_BOOK_IMB
    else:
        flow_ok = delta <= -MIN_FLOW_DELTA
        book_ok = book <= -MIN_BOOK_IMB

    if not (flow_ok or book_ok):
        reasons.append("FLOW_BOOK")

    return len(reasons) == 0, reasons

# ============================================================
# TRADE DATABASE
# ============================================================

def trade_rows(v2_only=True):
    if not DATABASE_URL:
        return []

    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                if v2_only:
                    cur.execute("""
                        SELECT
                            trade_id,
                            time_utc,
                            closed_time_utc,
                            symbol,
                            signal,
                            score,
                            entry,
                            tp,
                            sl,
                            status,
                            exit_price,
                            strategy_version
                        FROM trade_results
                        WHERE strategy_version = %s
                        ORDER BY time_utc ASC
                    """, (STRATEGY_VERSION,))
                else:
                    cur.execute("""
                        SELECT
                            trade_id,
                            time_utc,
                            closed_time_utc,
                            symbol,
                            signal,
                            score,
                            entry,
                            tp,
                            sl,
                            status,
                            exit_price,
                            strategy_version
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
                            trade_id,
                            time_utc,
                            closed_time_utc,
                            symbol,
                            signal,
                            score,
                            entry,
                            tp,
                            sl,
                            status,
                            exit_price,
                            strategy_version
                        )
                        VALUES (
                            %s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s
                        )
                        ON CONFLICT (trade_id)
                        DO UPDATE SET
                            closed_time_utc = EXCLUDED.closed_time_utc,
                            score = EXCLUDED.score,
                            entry = EXCLUDED.entry,
                            tp = EXCLUDED.tp,
                            sl = EXCLUDED.sl,
                            status = EXCLUDED.status,
                            exit_price = EXCLUDED.exit_price,
                            strategy_version = EXCLUDED.strategy_version
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
                        (
                            float(r.get("exit_price"))
                            if r.get("exit_price") not in ("", None)
                            else None
                        ),
                        r.get("strategy_version") or STRATEGY_VERSION,
                    ))

            conn.commit()

    except Exception as e:
        print(f"[DB] TRADE WRITE ERROR: {e}", flush=True)


def add_open_trade(row):
    trade_id = (
        f'{STRATEGY_VERSION}-'
        f'{row["symbol"]}-'
        f'{int(time.time()*1000)}'
    )

    rows = trade_rows(v2_only=True)

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
        "strategy_version": STRATEGY_VERSION,
    })

    write_trade_rows(rows)
    return trade_id

# ============================================================
# PERFORMANCE / CAPITAL — CURRENT STRATEGY ONLY
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
    rows = trade_rows(v2_only=True)

    closed = [
        r for r in rows
        if r.get("status") in ("WIN", "LOSS")
    ]

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
        "strategy_version": STRATEGY_VERSION,
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": (
            round((wins / total) * 100, 2)
            if total
            else 0.0
        ),
        "open_trades": sum(
            1 for r in rows
            if r.get("status") == "OPEN"
        ),
        "total_profit": round(profit, 2),
        "total_loss": round(loss, 2),
        "net_pl": round(profit - loss, 2),
    }


def forming_performance():
    # A forming candidate is NOT treated as a paper trade.
    # This avoids fake performance from non-entered setups.
    return {
        "strategy_version": STRATEGY_VERSION,
        "total_setups": 0,
        "closed_setups": 0,
        "wins": 0,
        "losses": 0,
        "open": 0,
        "win_rate": 0.0,
        "avg_score": 0.0,
        "total_profit": 0.0,
        "total_loss": 0.0,
        "net_pl": 0.0,
        "note": "V4 measures performance only after a fully validated trade entry.",
    }


def score_performance():
    rows = [
        r for r in trade_rows(v2_only=True)
        if r.get("status") in ("WIN", "LOSS")
    ]

    buckets_def = [
        (0, 74, "VALID <75"),
        (75, 84, "GOOD 75-84"),
        (85, 89, "STRONG 85-89"),
        (90, None, "A+ 90+"),
    ]

    buckets = []

    for low, high, label in buckets_def:
        selected = []

        for r in rows:
            try:
                sc = float(r.get("score") or 0)
            except Exception:
                continue

            if sc < low:
                continue

            if high is not None and sc > high:
                continue

            selected.append(r)

        wins = sum(
            1 for r in selected
            if r.get("status") == "WIN"
        )
        losses = sum(
            1 for r in selected
            if r.get("status") == "LOSS"
        )
        total = wins + losses

        buckets.append({
            "range": label,
            "closed": total,
            "wins": wins,
            "losses": losses,
            "win_rate": (
                round((wins / total) * 100, 2)
                if total
                else 0.0
            ),
        })

    return {
        "strategy_version": STRATEGY_VERSION,
        "closed_total": len(rows),
        "exact": [],
        "buckets": buckets,
        "best_exact": None,
        "best_bucket": (
            max(
                [x for x in buckets if x["closed"] >= 5],
                key=lambda x: (x["win_rate"], x["closed"]),
            )
            if any(x["closed"] >= 5 for x in buckets)
            else None
        ),
        "note": (
            "V4 score is a quality/ranking metric only. "
            "Market logic creates the trade."
        ),
    }


def capital_summary():
    rows = [
        r for r in trade_rows(v2_only=True)
        if r.get("status") in ("WIN", "LOSS")
    ]

    profit = 0.0
    loss = 0.0

    for r in rows:
        pl = TEST_START_CAPITAL * _trade_return_pct(r)

        if pl >= 0:
            profit += pl
        else:
            loss += abs(pl)

    net = profit - loss
    ending = TEST_START_CAPITAL + net

    wins = sum(1 for r in rows if r.get("status") == "WIN")
    losses = sum(1 for r in rows if r.get("status") == "LOSS")

    return {
        "strategy_version": STRATEGY_VERSION,
        "starting_capital": round(TEST_START_CAPITAL, 2),
        "leverage": TEST_LEVERAGE,
        "daily_profit": round(profit, 2),
        "daily_loss": round(loss, 2),
        "net_pl": round(net, 2),
        "ending_capital": round(ending, 2),
        "net_pl_pct": (
            round((net / TEST_START_CAPITAL) * 100, 2)
            if TEST_START_CAPITAL
            else 0.0
        ),
        "closed_trades_today": len(rows),
        "source": "V4 VALIDATED TRADES ONLY",
        "closed_setups": len(rows),
        "wins": wins,
        "losses": losses,
    }

# ============================================================
# TP / SL CHECKS
# ============================================================

def check_open_trades():
    rows = trade_rows(v2_only=True)

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
                r["closed_time_utc"] = (
                    datetime.now(timezone.utc).isoformat()
                )

                changed = True

                closed_messages.append(
                    (symbol, side, result, entry, tp, sl, px)
                )

        except Exception as e:
            scan_log(
                f"OPEN TRADE CHECK ERROR {r.get('symbol')}: {e}"
            )

    if changed:
        write_trade_rows(rows)

        p = performance()

        for (
            symbol,
            side,
            result,
            entry,
            tp,
            sl,
            px,
        ) in closed_messages:

            icon = "✅" if result == "WIN" else "❌"
            label = "TP HIT / WIN" if result == "WIN" else "SL HIT / LOSS"

            telegram_async(
                f"{icon} V4 {label} — {symbol}\n"
                f"Exchange: BITGET FUTURES\n"
                f"Side: {side}\n"
                f"Entry: {entry:.8g}\n"
                f"Exit: {px:.8g}\n"
                f"TP: {tp:.8g}\n"
                f"SL: {sl:.8g}\n\n"
                f"📊 V4 PERFORMANCE\n"
                f"Trades: {p['total_trades']}\n"
                f"Wins: {p['wins']}\n"
                f"Losses: {p['losses']}\n"
                f"Winning: {p['win_rate']:.2f}%\n"
                f"Net P/L: ${p['net_pl']:.2f}\n"
                f"🔗 {APP_URL}"
            )


def check_forming_setups():
    # Intentionally disabled: forming candidates are not paper trades.
    # A forming candidate is NOT counted as a trade.
    return

# ============================================================
# SIGNAL FILE
# ============================================================

def save_signal(row):
    new_file = not LIVE_FILE.exists()

    with LIVE_FILE.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as f:
        w = csv.DictWriter(
            f,
            fieldnames=CSV_COLUMNS,
        )

        if new_file:
            w.writeheader()

        w.writerow({
            k: row.get(k, "")
            for k in CSV_COLUMNS
        })


def recent_signals(limit=20):
    if not LIVE_FILE.exists():
        return []

    try:
        with LIVE_FILE.open(
            "r",
            newline="",
            encoding="utf-8",
        ) as f:
            rows = list(csv.DictReader(f))

        return list(reversed(rows[-limit:]))

    except Exception:
        return []

# ============================================================
# COOLDOWN
# ============================================================

def has_recent_or_open_trade(symbol, side):
    now = datetime.now(timezone.utc)

    for r in trade_rows(v2_only=True):
        if (
            r.get("symbol") != symbol
            or r.get("signal") != side
        ):
            continue

        if r.get("status") == "OPEN":
            return True

        try:
            t = datetime.fromisoformat(
                r.get("time_utc") or ""
            )

            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)

            if (
                (now - t).total_seconds()
                < SIGNAL_COOLDOWN_SECONDS
            ):
                return True

        except Exception:
            pass

    return False

# ============================================================
# LIGHT SCAN
# ============================================================

def light_metrics(symbol):
    """Activity ranking only; no RSI-extreme directional bias."""
    c15 = candle_dicts(symbol, "15m", 80)
    if len(c15) < 40:
        return None

    closes = [x["close"] for x in c15]
    price = closes[-1]
    vr = volume_ratio(c15, 20)
    a = atr_value(c15[-50:], 14)
    atr_pct = a / price if price > 0 else 0.0
    r15 = rsi(closes, 14)

    e20 = ema(closes[-60:], 20)
    e50 = ema(closes[-70:], 50)
    trend_sep = abs(e20 - e50) / price if price > 0 else 0.0

    recent = c15[-24:-2]
    rh = max(x["high"] for x in recent)
    rl = min(x["low"] for x in recent)
    edge_dist = min(abs(price-rh), abs(price-rl)) / a if a > 0 else 99.0
    edge_bonus = max(0.0, 12.0 - min(edge_dist, 3.0) * 4.0)

    rank = (
        min(vr, 3.0) * 12.0
        + min(atr_pct * 10000, 120.0)
        + min(trend_sep * 10000, 25.0)
        + edge_bonus
    )

    return {
        "price": price,
        "rsi_15m": round(r15, 2),
        "vol_ratio_15m": vr,
        "atr_15m_pct": atr_pct,
        "rank": rank,
    }


def _light_scan_one(symbol):
    try:
        lm = light_metrics(symbol)

        if not lm:
            return None

        return (
            lm["rank"],
            symbol,
            lm,
            None,
        )

    except Exception as e:
        return (
            None,
            symbol,
            None,
            str(e),
        )

# ============================================================
# V4 RSI SUPPORTIVE CONTEXT (NOT DIRECTION SELECTOR)
# ============================================================

def rsi_context_15m(symbol):
    c = candle_dicts(symbol, "15m", 100)
    if len(c) < 50:
        raise RuntimeError(f"Not enough 15M candles for RSI context {symbol}")
    closes = [x["close"] for x in c]
    r_now = rsi(closes, 14)
    r_prev = rsi(closes[:-1], 14)
    return {
        "rsi_now": r_now,
        "rsi_prev": r_prev,
        "rsi_turn": r_now - r_prev,
        "oversold": r_now <= RSI_OVERSOLD,
        "overbought": r_now >= RSI_OVERBOUGHT,
        "extreme_oversold": r_now <= RSI_EXTREME_OVERSOLD,
        "extreme_overbought": r_now >= RSI_EXTREME_OVERBOUGHT,
    }

# ============================================================
# V4 MULTI-STRATEGY 15M SETUP SELECTOR — HARD GATE LOGIC
# ============================================================

def _bias_from_context(reg, st):
    reg_name = reg.get("regime")
    st_name = st.get("structure")
    if reg_name in ("BULL_TREND", "EXPANSION_BULL") and st_name in ("HH_HL", "RANGE"):
        return "BUY"
    if reg_name in ("BEAR_TREND", "EXPANSION_BEAR") and st_name in ("LH_LL", "RANGE"):
        return "SELL"
    if st_name == "HH_HL" and reg_name != "BEAR_TREND":
        return "BUY"
    if st_name == "LH_LL" and reg_name != "BULL_TREND":
        return "SELL"
    return "NEUTRAL"


def multi_strategy_setup_15m(symbol, reg, st):
    c = candle_dicts(symbol, "15m", 140)
    if len(c) < 80:
        raise RuntimeError(f"Not enough 15M candles for V4 setup {symbol}")

    last, prev, prev2 = c[-1], c[-2], c[-3]
    closes = [x["close"] for x in c]
    price = float(last["close"])
    e20 = ema(closes[-80:], 20)
    e50 = ema(closes[-110:], 50)
    atr15 = atr_value(c[-60:], 14)
    atr_pct = atr15 / price if price > 0 else 0.0
    vr = volume_ratio(c, 20)
    volatility_ok = MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT
    ctx = _liquidity_context_15m(c)
    prior_high, prior_low = ctx["prior_high"], ctx["prior_low"]
    rctx = rsi_context_15m(symbol)
    bias = _bias_from_context(reg, st)
    regime_name = reg.get("regime")

    base = {
        "price": price,
        "prior_high": prior_high,
        "prior_low": prior_low,
        "atr": atr15,
        "atr_pct": atr_pct,
        "vol_ratio": vr,
        "volatility_ok": volatility_ok,
        "bias": bias,
        "rsi_15m": rctx["rsi_now"],
        "rsi_context": rctx,
    }

    if regime_name == "CHOP" or not volatility_ok:
        return {
            **base, "valid": False, "side": None, "strategy_type": "NONE", "setup": "NONE",
            "reason": "CHOP" if regime_name == "CHOP" else "VOLATILITY",
            "liquidity_level": price, "liquidity_tier": "NONE", "sweep_extreme": price,
            "location_ok": False, "event": "NONE", "acceptance": False,
            "rejection": False, "reaction": "WEAK",
        }

    # --------------------------------------------------------
    # 1) TREND PULLBACK — primary setup, only in established trend regime.
    # Context -> correct location -> retest hold -> displacement.
    # --------------------------------------------------------
    if (
        bias in ("BUY", "SELL")
        and ((bias == "BUY" and regime_name == "BULL_TREND")
             or (bias == "SELL" and regime_name == "BEAR_TREND"))
    ):
        side = bias
        one_h_level = st.get("last_swing_low") if side == "BUY" else st.get("last_swing_high")
        levels = [x for x in (e20, one_h_level) if isinstance(x, (int, float)) and x > 0]
        level = min(levels, key=lambda x: abs(price - x)) if levels else e20

        if side == "BUY":
            touched = min(last["low"], prev["low"]) <= level + LOCATION_ATR_DISTANCE * atr15
            held = last["close"] > level
            extension = max(0.0, price - e20) / atr15 if atr15 > 0 else 99.0
            ema_ok = e20 > e50 and price > e20
            extreme = min(last["low"], prev["low"], level)
        else:
            touched = max(last["high"], prev["high"]) >= level - LOCATION_ATR_DISTANCE * atr15
            held = last["close"] < level
            extension = max(0.0, e20 - price) / atr15 if atr15 > 0 else 99.0
            ema_ok = e20 < e50 and price < e20
            extreme = max(last["high"], prev["high"], level)

        location_ok = touched and held and extension <= MAX_EXTENSION_ATR
        reaction = _directional_displacement(last, atr15, side)
        if location_ok and ema_ok and reaction and vr >= TREND_MIN_VOL_RATIO:
            return {
                **base, "valid": True, "side": side, "strategy_type": "TREND",
                "setup": "15M_TREND_PULLBACK_RESUME", "reason": "LOCATION_RETEST_DISPLACEMENT",
                "liquidity_level": level, "liquidity_tier": "B", "sweep_extreme": extreme,
                "location_ok": True, "event": "RETEST", "acceptance": False,
                "rejection": False, "reaction": "DISPLACEMENT",
            }

    # --------------------------------------------------------
    # 2) BREAKOUT — secondary setup for range/expansion or clear HTF alignment.
    # Level break -> acceptance/retest -> directional displacement.
    # --------------------------------------------------------
    breakout_candidates = []
    for side, level in (("BUY", prior_high), ("SELL", prior_low)):
        buffer = BREAKOUT_BUFFER_ATR * atr15
        if side == "BUY":
            prev_broke = prev["close"] > level + buffer
            prev2_broke = prev2["close"] > level + buffer
            last_beyond = last["close"] > level + buffer
            accepted = last_beyond and (prev_broke or prev2_broke)
            retest = prev_broke and last["low"] <= level + RETEST_MAX_ATR * atr15 and last["close"] > level
            extreme = min(last["low"], prev["low"], level)
            same_expansion = regime_name == "EXPANSION_BULL"
        else:
            prev_broke = prev["close"] < level - buffer
            prev2_broke = prev2["close"] < level - buffer
            last_beyond = last["close"] < level - buffer
            accepted = last_beyond and (prev_broke or prev2_broke)
            retest = prev_broke and last["high"] >= level - RETEST_MAX_ATR * atr15 and last["close"] < level
            extreme = max(last["high"], prev["high"], level)
            same_expansion = regime_name == "EXPANSION_BEAR"

        reaction = _directional_displacement(last, atr15, side)
        context_ok = (
            regime_name == "RANGE"
            or same_expansion
            or side == bias
        )
        if context_ok and vr >= BREAKOUT_MIN_VOL_RATIO and reaction and (accepted or retest):
            breakout_candidates.append({
                **base, "valid": True, "side": side, "strategy_type": "BREAKOUT",
                "setup": "15M_BREAKOUT_ACCEPT" if accepted else "15M_BREAKOUT_RETEST",
                "reason": "BREAK_ACCEPTANCE_DISPLACEMENT" if accepted else "BREAK_RETEST_DISPLACEMENT",
                "liquidity_level": level, "liquidity_tier": "A", "sweep_extreme": extreme,
                "location_ok": True, "event": "BREAKOUT", "acceptance": bool(accepted),
                "rejection": False, "reaction": "DISPLACEMENT",
            })
    if breakout_candidates:
        breakout_candidates.sort(key=lambda x: (x["side"] == bias, x["acceptance"]), reverse=True)
        return breakout_candidates[0]

    # --------------------------------------------------------
    # 3) REVERSAL — advanced setup. Never RSI-only and never blind countertrend.
    # Tier-A sweep -> reclaim/reject -> displacement; context must permit reversal.
    # --------------------------------------------------------
    for side, level in (("BUY", prior_low), ("SELL", prior_high)):
        swept_last, _ = _sweep_reclaim(last, level, atr15, side)
        swept_prev, _ = _sweep_reclaim(prev, level, atr15, side)
        swept = swept_last or swept_prev
        reaction = _directional_displacement(last, atr15, side)
        location_ok = (
            abs((last["low"] if side == "BUY" else last["high"]) - level)
            <= (SWEEP_MAX_ATR + RETEST_MAX_ATR) * atr15
        )
        # Range is ideal. In a trend, only allow reversal if 1H structure already agrees.
        reversal_context_ok = (
            regime_name == "RANGE"
            or st.get("bias") == side
            or (side == "BUY" and regime_name == "EXPANSION_BULL")
            or (side == "SELL" and regime_name == "EXPANSION_BEAR")
        )
        if swept and reaction and location_ok and reversal_context_ok and vr >= REVERSAL_MIN_VOL_RATIO:
            return {
                **base, "valid": True, "side": side, "strategy_type": "REVERSAL",
                "setup": "15M_LOW_SWEEP_RECLAIM" if side == "BUY" else "15M_HIGH_SWEEP_REJECT",
                "reason": "LIQUIDITY_REJECTION_DISPLACEMENT",
                "liquidity_level": level, "liquidity_tier": "A",
                "sweep_extreme": min(last["low"], prev["low"]) if side == "BUY" else max(last["high"], prev["high"]),
                "location_ok": True, "event": "SWEEP", "acceptance": False,
                "rejection": True, "reaction": "DISPLACEMENT",
            }

    return {
        **base, "valid": False, "side": None, "strategy_type": "NONE", "setup": "NONE",
        "reason": "NO_COMPLETE_LOGIC_CHAIN", "liquidity_level": price,
        "liquidity_tier": "NONE", "sweep_extreme": price, "location_ok": False,
        "event": "NONE", "acceptance": False, "rejection": False, "reaction": "WEAK",
    }

# ============================================================
# V4 DEEP SCAN — COMPLETE HARD-GATE DECISION CHAIN
# ============================================================

def _deep_scan_one(item):
    _, symbol, lm, btc_regime = item

    try:
        reg = regime_4h(symbol)
        st = structure_1h(symbol)
        setup = multi_strategy_setup_15m(symbol, reg, st)

        if reg.get("regime") == "CHOP":
            return {
                "symbol": symbol, "blocked": "CHOP_REGIME", "score": 0,
                "lm": lm, "regime": reg, "structure": st, "setup": setup,
                "error": None,
            }

        if not setup.get("valid"):
            return {
                "symbol": symbol,
                "blocked": setup.get("reason") or "NO_V4_SETUP",
                "score": 0,
                "lm": lm,
                "regime": reg,
                "structure": st,
                "setup": setup,
                "error": None,
            }

        side = setup["side"]
        strategy_type = setup.get("strategy_type", "V4")

        # Hard gate: optional BTC context cannot directly oppose the trade.
        if not btc_market_allows(side, btc_regime):
            return {
                "symbol": symbol, "side": side, "blocked": "BTC_REGIME",
                "score": 0, "lm": lm, "regime": reg, "structure": st,
                "setup": setup, "error": None,
            }

        # Hard gate: 5m must confirm a real local structure break with displacement.
        trigger = entry_trigger_5m(symbol, side)
        if not trigger.get("valid"):
            q = quality_score(side, reg, st, setup, trigger, 0.0, 0.0, 999.0, 0.0)
            return {
                "symbol": symbol, "side": side, "blocked": "5M_STRUCTURE_WAIT",
                "score": q, "lm": lm, "regime": reg, "structure": st,
                "setup": setup, "trigger": trigger, "error": None,
            }

        # Hard gate: tradable spread + directional live confirmation.
        dm = depth_metrics(symbol)
        if not dm:
            return {
                "symbol": symbol, "side": side, "blocked": "NO_ORDER_BOOK",
                "score": 0, "lm": lm, "regime": reg, "structure": st,
                "setup": setup, "trigger": trigger, "error": None,
            }

        spread, book = dm
        delta, buy, sell = flow_metrics(symbol)
        oi = oi_change_pct(symbol)
        live_ok, live_reasons = live_confirmation(side, delta, book, spread, oi)

        q = quality_score(side, reg, st, setup, trigger, delta, book, spread, oi)
        price = current_price(symbol)
        risk = dynamic_risk(price, side, setup, trigger)

        if risk is None:
            return {
                "symbol": symbol, "side": side, "blocked": "RISK_TOO_WIDE",
                "score": q, "lm": lm, "regime": reg, "structure": st,
                "setup": setup, "trigger": trigger, "delta": delta,
                "book": book, "spread": spread, "oi": oi, "error": None,
            }

        score_ok = q >= MIN_READY_SCORE
        hard_confirm = bool(live_ok and score_ok)
        blocked_reasons = list(live_reasons)
        if not score_ok:
            blocked_reasons.append("SCORE")

        rctx = setup.get("rsi_context") or {}
        candidate = {
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "strategy_version": STRATEGY_VERSION,
            "exchange": "BITGET",
            "data_source": "Bitget USDT-M Futures",
            "symbol": symbol,
            "signal": side,
            "score": q,
            "risk_label": risk_label(q),
            "price": price,
            "strategy_type": strategy_type,

            "regime_4h": reg["regime"],
            "structure_1h": st["structure"],
            "bias_1h": setup.get("bias", st.get("bias", "NEUTRAL")),
            "setup_15m": f"{strategy_type}:{setup['setup']}",
            "location_15m": "VALID" if setup.get("location_ok") else "INVALID",
            "liquidity_event_15m": setup.get("event", "NONE"),
            "liquidity_tier": setup.get("liquidity_tier", "NONE"),
            "acceptance_15m": bool(setup.get("acceptance")),
            "rejection_15m": bool(setup.get("rejection")),
            "reaction_15m": setup.get("reaction", "WEAK"),
            "trigger_5m": trigger["trigger"],

            "rsi_15m": round(float(setup.get("rsi_15m") or 0), 2),
            "rsi_prev_15m": round(float(rctx.get("rsi_prev") or 0), 2),
            "rsi_turn_15m": round(float(rctx.get("rsi_turn") or 0), 2),
            "reversal_strength": "SUPPORTIVE_ONLY",

            "flow_delta": delta,
            "buy_usd_60s": buy,
            "sell_usd_60s": sell,
            "spread_bps": spread,
            "book_imb": book,
            "oi_change_pct": oi,
            "atr_15m_pct": setup["atr_pct"],
            "vol_ratio": setup["vol_ratio"],
            "risk_pct": risk["risk_pct"],
            "rr": risk["rr"],
            "tp": risk["tp"],
            "sl": risk["sl"],
            "structural_level": risk.get("structural_level"),
            "hard_confirm": hard_confirm,
            "blocked_reasons": blocked_reasons,
            "status": "TRADE READY" if hard_confirm else "WAIT LIVE CONFIRMATION",
        }

        return {
            "symbol": symbol, "side": side, "score": q, "candidate": candidate,
            "blocked": None if hard_confirm else "LIVE_CONFIRM",
            "lm": lm, "regime": reg, "structure": st, "setup": setup,
            "trigger": trigger, "delta": delta, "buy": buy, "sell": sell,
            "spread": spread, "book": book, "oi": oi, "error": None,
        }

    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

# ============================================================
# MAIN SCAN
# ============================================================

def scan_once():
    scan_start = time.time()

    scan_log("================================")
    scan_log("RAZA V4 REFINED LOGIC BITGET FUTURES SCAN START")

    with state_lock:
        state["status"] = (
            "V4 24/7 scanning ALL Bitget USDT-M Futures..."
            if SCAN_ALL_COINS
            else f"V4 24/7 scanning Top {TOP_COINS} Bitget Futures..."
        )
        state["last_error"] = None
        state["scan_progress"] = "0/0"
        state["blocked_counts"] = {}

    check_open_trades()

    # ----------------------------
    # KSA SESSION HARD FILTER
    # ----------------------------

    if not in_ksa_session():
        now = datetime.now(timezone.utc)
        elapsed = round(time.time() - scan_start, 2)

        with state_lock:
            state["last_scan"] = now.isoformat()
            state["alerts_last_scan"] = 0
            state["last_scan_seconds"] = elapsed
            state["best_candidate"] = None
            state["status"] = (
                f"V4 waiting for optional KSA session "
                f"{KSA_SESSION_START}:00-{KSA_SESSION_END}:00"
            )

        save_state_snapshot()

        scan_log(
            f"OUTSIDE KSA SESSION "
            f"{KSA_SESSION_START}:00-{KSA_SESSION_END}:00"
        )
        return

    # ----------------------------
    # BTC REGIME
    # ----------------------------

    try:
        btc_reg = regime_4h("BTCUSDT")
    except Exception as e:
        btc_reg = {"regime": "SIDEWAYS", "error": str(e)}

    with state_lock:
        state["market_regime"] = {
            "symbol": "BTCUSDT",
            **btc_reg,
        }

    scan_log(
        f"BTC 4H REGIME: {btc_reg.get('regime')}"
    )

    # ----------------------------
    # SYMBOLS
    # ----------------------------

    try:
        symbols = top_symbols()

    except Exception as e:
        error = f"{type(e).__name__}: {e}"

        scan_log(f"TOP SYMBOL LOAD ERROR: {error}")

        with state_lock:
            state["last_error"] = error
            state["status"] = "Bitget symbol load error — retrying"
            state["last_scan"] = datetime.now(timezone.utc).isoformat()
            state["last_scan_seconds"] = round(
                time.time() - scan_start,
                2,
            )

        save_state_snapshot()
        return

    with state_lock:
        state["scan_progress"] = f"0/{len(symbols)}"

    # ----------------------------
    # LIGHT SCAN
    # ----------------------------

    light_candidates = []
    light_errors = []

    with ThreadPoolExecutor(
        max_workers=max(1, LIGHT_SCAN_WORKERS)
    ) as pool:

        futures = {
            pool.submit(
                _light_scan_one,
                symbol,
            ): symbol
            for symbol in symbols
        }

        done = 0

        for future in as_completed(futures):
            done += 1
            result = future.result()

            if result:
                rank, symbol, lm, err = result

                if rank is not None and lm is not None:
                    light_candidates.append(
                        (rank, symbol, lm)
                    )
                elif err:
                    light_errors.append(
                        f"{symbol}: {err}"
                    )

            if (
                done == 1
                or done % 10 == 0
                or done == len(symbols)
            ):
                scan_log(
                    f"LIGHT SCAN: {done}/{len(symbols)}"
                )

            with state_lock:
                state["scan_progress"] = f"{done}/{len(symbols)}"

    light_candidates.sort(
        reverse=True,
        key=lambda x: x[0],
    )

    with state_lock:
        state["watchlist"] = [
            {
                "symbol": symbol,
                "rank": round(float(rank), 2),
                "rsi_15m": lm.get("rsi_15m"),
                "vol_ratio_15m": round(
                    float(lm.get("vol_ratio_15m") or 0),
                    2,
                ),
                "atr_15m_pct": round(
                    float(lm.get("atr_15m_pct") or 0) * 100,
                    3,
                ),
            }
            for rank, symbol, lm
            in light_candidates[:max(1, WATCHLIST_LIMIT)]
        ]

    scan_log(
        f"LIGHT SCAN COMPLETE: "
        f"{len(light_candidates)} candidates"
    )

    # ----------------------------
    # DEEP SCAN
    # ----------------------------

    # In ALL-COINS mode, every LIGHT-SCAN candidate receives the full V4 deep scan.
    # Worker count remains bounded by DEEP_SCAN_WORKERS to avoid an unbounded burst.
    selected_for_deep = (
        light_candidates
        if SCAN_ALL_COINS
        else light_candidates[:max(1, DEEP_CHECK)]
    )

    deep_items = [
        (rank, symbol, lm, btc_reg)
        for rank, symbol, lm
        in selected_for_deep
    ]

    scan_log(
        f"V4 DEEP SCAN START: {len(deep_items)} candidates"
    )

    alerts = 0
    best = None
    deep_errors = []
    blocked_counts = {}

    with ThreadPoolExecutor(
        max_workers=max(1, DEEP_SCAN_WORKERS)
    ) as pool:

        futures = {
            pool.submit(
                _deep_scan_one,
                item,
            ): item[1]
            for item in deep_items
        }

        for future in as_completed(futures):
            result = future.result()
            symbol = result.get("symbol")

            if result.get("error"):
                deep_errors.append(
                    f"{symbol}: {result['error']}"
                )
                continue

            blocked = result.get("blocked")
            if blocked:
                blocked_counts[blocked] = (
                    blocked_counts.get(blocked, 0) + 1
                )

            c = result.get("candidate")

            # Best candidate can be READY or waiting only on live confirmation.
            if c:
                if (
                    best is None
                    or c["score"] > best["score"]
                ):
                    best = c

            if not c:
                continue

            # IMPORTANT:
            # Score does NOT trigger the trade.
            # All hard market + liquidity + live confirmations must pass.
            if (
                c["status"] == "TRADE READY"
                and c["hard_confirm"]
                and not has_recent_or_open_trade(
                    c["symbol"],
                    c["signal"],
                )
            ):
                save_signal(c)
                add_open_trade(c)

                with state_lock:
                    state["latest_signal"] = c

                p = performance()

                telegram_async(
                    f"🚨 RAZA SHAH SIGNAL V4 — TRADE READY\n\n"
                    f"Exchange: BITGET FUTURES\n"
                    f"Coin: {c['symbol']}\n"
                    f"Side: "
                    f"{'LONG' if c['signal']=='BUY' else 'SHORT'}\n"
                    f"Quality: {c['score']}/100 ({c['risk_label']})\n\n"
                    f"4H Regime: {c['regime_4h']}\n"
                    f"1H Structure: {c['structure_1h']} | Bias: {c['bias_1h']}\n"
                    f"15M Setup: {c['setup_15m']}\n"
                    f"Location: {c['location_15m']} | Liquidity: {c['liquidity_event_15m']} ({c['liquidity_tier']})\n"
                    f"Accept: {c['acceptance_15m']} | Reject: {c['rejection_15m']} | Reaction: {c['reaction_15m']}\n"
                    f"5M Structure: {c['trigger_5m']}\n\n"
                    f"Entry: {c['price']:.8g}\n"
                    f"TP: {c['tp']:.8g}\n"
                    f"SL: {c['sl']:.8g}\n"
                    f"Risk: {c['risk_pct']*100:.3f}%\n"
                    f"R:R: 1:{c['rr']:.2f}\n\n"
                    f"Flow: {c['flow_delta']:+.3f}\n"
                    f"Book: {c['book_imb']:+.3f}\n"
                    f"Spread: {c['spread_bps']:.2f} bps\n"
                    f"OI Change: {c['oi_change_pct']:+.3f}%\n"
                    f"15M Volume: {c['vol_ratio']:.2f}x\n\n"
                    f"V4 Closed: {p['total_trades']}\n"
                    f"V4 Win Rate: {p['win_rate']:.2f}%\n"
                    f"🔗 {APP_URL}"
                )

                alerts += 1

    now = datetime.now(timezone.utc)
    elapsed = round(time.time() - scan_start, 2)

    with state_lock:
        state["best_candidate"] = best
        state["last_scan"] = now.isoformat()
        state["alerts_last_scan"] = alerts
        state["last_scan_seconds"] = elapsed
        state["blocked_counts"] = blocked_counts

        if alerts:
            state["status"] = (
                f"V4: {alerts} auto paper trade(s) added"
            )

        elif best:
            state["status"] = (
                f"V4 best: {best['symbol']} "
                f"{'LONG' if best['signal']=='BUY' else 'SHORT'} "
                f"{best['score']}/100 — {best['status']}"
            )

        else:
            top_block = (
                max(
                    blocked_counts,
                    key=blocked_counts.get,
                )
                if blocked_counts
                else "NO_SETUP"
            )

            state["status"] = (
                f"V4 scanning — main block: {top_block}"
            )

        if (
            not best
            and (light_errors or deep_errors)
        ):
            state["last_error"] = " | ".join(
                (light_errors + deep_errors)[:3]
            )

    save_state_snapshot()

    scan_log(
        f"V4 SCAN COMPLETE in {elapsed}s "
        f"| alerts={alerts} "
        f"| best={best['symbol'] if best else 'NONE'} "
        f"| blocked={blocked_counts}"
    )

# ============================================================
# SECURITY / OTP
# ============================================================

otp_lock = threading.Lock()
otp_by_ip = {}
authorized_ips = {}


def client_ip():
    xff = request.headers.get(
        "X-Forwarded-For",
        "",
    )

    if xff:
        return xff.split(",")[0].strip()

    return (
        request.remote_addr
        or "unknown"
    ).strip()


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

        if (
            existing
            and existing.get("expires", 0) > now
        ):
            return existing["code"], False

        code = f"{secrets.randbelow(1000000):06d}"

        otp_by_ip[ip] = {
            "code": code,
            "expires": now + OTP_TTL_SECONDS,
            "attempts": 0,
        }

    telegram_async(
        f"🔐 RAZA SHAH SIGNAL V4\n"
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

    auth_until = float(
        flask_session.get(
            "authorized_until",
            0,
        )
        or 0
    )

    auth_ip = flask_session.get("authorized_ip")

    return (
        auth_ip == ip
        and auth_until > now
    )


def login_html(message=""):
    return f"""
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RAZA SHAH SIGNAL — Secure Access</title>
<style>
body{{
    font-family:Arial;
    background:#07111f;
    color:#fff;
    display:flex;
    align-items:center;
    justify-content:center;
    min-height:100vh;
    margin:0;
}}
.box{{
    width:min(92%,420px);
    background:#101d2e;
    padding:28px;
    border-radius:18px;
    text-align:center;
}}
input{{
    font-size:24px;
    letter-spacing:6px;
    width:85%;
    padding:14px;
    text-align:center;
    border-radius:10px;
    border:1px solid #445;
    background:#07111f;
    color:#fff;
}}
button{{
    margin-top:14px;
    padding:13px 24px;
    border:0;
    border-radius:10px;
    font-weight:bold;
    cursor:pointer;
}}
.msg{{
    margin:12px;
    color:#ffcc66;
}}
.small{{
    opacity:.7;
    font-size:13px;
}}
</style>
</head>
<body>
<div class="box">
<h2>🔐 RAZA SHAH SIGNAL V4</h2>
<p><b>Waiting for Admin Permission</b></p>
<p>Enter 6-Digit Permission Code</p>
<div class="msg">{message}</div>
<form method="post" action="/verify">
<input
    name="code"
    inputmode="numeric"
    maxlength="6"
    placeholder="000000"
    required
>
<br>
<button type="submit">REQUEST ACCESS</button>
</form>
<p class="small">
One code per IP. After approval,
this IP stays authorized for 24 hours.
</p>
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

            if best:
                best_line = (
                    f"Best: {best.get('symbol')} | "
                    f"{'LONG' if best.get('signal')=='BUY' else 'SHORT'} | "
                    f"{best.get('score',0)}/100 | "
                    f"{best.get('status')}"
                )
            else:
                best_line = "Best: none"

            telegram_async(
                f"🟢 RAZA SHAH SIGNAL V4\n"
                f"1 HOUR STATUS\n\n"
                f"Exchange: BITGET FUTURES\n"
                f"Scanner: {'LIVE' if st.get('running') else 'OFFLINE'}\n"
                f"Status: {st.get('status') or '—'}\n"
                f"BTC 4H: "
                f"{(st.get('market_regime') or {}).get('regime','—')}\n"
                f"{best_line}\n"
                f"Last alerts: {st.get('alerts_last_scan',0)}\n"
                f"Scan time: {st.get('last_scan_seconds','—')}s\n\n"
                f"V4 Trades: {p['total_trades']}\n"
                f"Wins: {p['wins']}\n"
                f"Losses: {p['losses']}\n"
                f"Winning: {p['win_rate']:.2f}%\n"
                f"Open: {p['open_trades']}\n\n"
                f"Paper P/L: ${c['net_pl']:.2f}\n"
                f"Capital: ${c['ending_capital']:.2f}\n"
                f"🔗 {APP_URL}"
            )

        except Exception as e:
            scan_log(f"HOURLY STATUS ERROR: {e}")


def trade_monitor_loop():
    scan_log("V4 LIVE TP/SL MONITOR STARTED")

    while True:
        try:
            check_open_trades()

        except Exception as e:
            scan_log(
                f"TP/SL MONITOR ERROR: "
                f"{type(e).__name__}: {e}"
            )

        time.sleep(TRADE_MONITOR_INTERVAL)


def scanner_loop():
    with state_lock:
        state["running"] = True
        state["strategy_version"] = STRATEGY_VERSION
        state["exchange"] = "BITGET"
        state["data_source"] = "Bitget USDT-M Futures"

    save_state_snapshot()

    p = performance()

    telegram_async(
        f"🟢 RAZA SHAH SIGNAL V4\n"
        f"LIVE PAPER TEST ACTIVE\n\n"
        f"Logic: Time → 4H Regime → 1H Bias → 15M Location/Liquidity → "
        f"Displacement → 5M Structure → Flow/Book/OI → Risk\n"
        f"Exchange: BITGET FUTURES\n"
        f"Coin Universe: {'ALL eligible USDT-M futures' if SCAN_ALL_COINS else f'Top {TOP_COINS}'}\n"
        f"Deep Scan: {'ALL light-scan candidates' if SCAN_ALL_COINS else DEEP_CHECK}\n"
        f"Scan interval: {SCAN_INTERVAL//60} minutes\n"
        f"KSA Session: "
        f"{KSA_SESSION_START}:00-{KSA_SESSION_END}:00 "
        f"({'ON' if SESSION_FILTER_ENABLED else 'OFF'})\n"
        f"BTC Filter: {'ON' if BTC_FILTER_ENABLED else 'OFF'}\n"
        f"Dynamic R:R: 1:{RR_TARGET:.2f}\n"
        f"Paper Tester: "
        f"${TEST_START_CAPITAL:.0f} @ {TEST_LEVERAGE:.0f}x\n\n"
        f"V4 history: {p['total_trades']} trades\n"
        f"Winning: {p['win_rate']:.2f}%\n\n"
        f"Score = quality only, not entry trigger.\n"
        f"🔗 {APP_URL}"
    )

    while True:
        start = time.time()

        try:
            scan_once()

        except Exception as e:
            scan_log(
                f"SCAN LOOP ERROR: "
                f"{type(e).__name__}: {e}"
            )

            with state_lock:
                state["last_error"] = str(e)
                state["status"] = "V4 scan error — retrying"

        wait = max(
            10,
            SCAN_INTERVAL - (time.time() - start),
        )

        with state_lock:
            state["next_scan"] = (
                datetime.fromtimestamp(
                    time.time() + wait,
                    timezone.utc,
                ).isoformat()
            )

        save_state_snapshot()
        time.sleep(wait)

# ============================================================
# WEB ROUTES
# ============================================================

@app.route("/", methods=["GET", "HEAD"])
def home():
    if request.method == "HEAD":
        return Response(status=200)

    # Telegram OTP/login gate removed.
    # Existing dashboard remains unchanged.
    return render_template("index.html")


@app.route("/verify", methods=["POST"])
def verify():
    # Legacy endpoint retained only so old bookmarks/forms do not error.
    return redirect(url_for("home"))


@app.route("/logout")
def logout():
    # No login session exists anymore.
    return redirect(url_for("home"))


@app.route("/api/status")
def api_status():
    with state_lock:
        x = dict(state)

    persisted = load_state_snapshot()

    if persisted:
        if (
            not x.get("last_scan")
            or (
                persisted.get("last_scan")
                and str(persisted.get("last_scan"))
                > str(x.get("last_scan") or "")
            )
        ):
            x.update(persisted)

    x["signals"] = recent_signals(20)
    x["telegram"] = bool(
        TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
    )

    # Frontend compatibility:
    x["min_score"] = STRONG_QUALITY_SCORE

    x["performance"] = performance()
    x["forming_performance"] = forming_performance()
    x["score_performance"] = score_performance()
    x["forming_history"] = []
    x["capital_summary"] = capital_summary()

    x["dashboard"] = {
        "strategy_version": STRATEGY_VERSION,
        "logic": (
            "Time → 4H regime → 1H bias/structure → 15m location/liquidity → "
            "accept/reject + displacement → 5m structure confirmation → "
            "live flow/book/OI → structural ATR risk"
        ),
        "risk_rules": {
            "score_role": "QUALITY ONLY",
            "trade_ready": (
                "All V4 hard filters must pass. "
                "Score alone cannot create a trade."
            ),
            "rr_target": RR_TARGET,
            "max_stop_pct": MAX_STOP_PCT * 100,
            "session": (
                f"KSA {KSA_SESSION_START}:00-"
                f"{KSA_SESSION_END}:00"
                if SESSION_FILTER_ENABLED
                else "OFF"
            ),
        },
        "market_data": "Bitget USDT-M Futures public market data.",
        "disclaimer": (
            "Paper-testing / informational signal system only. "
            "No profit or performance is guaranteed."
        ),
    }

    return jsonify(x)


@app.route("/api/signal")
def api_signal():
    with state_lock:
        x = dict(state)

    x["performance"] = performance()
    x["forming_performance"] = forming_performance()
    x["score_performance"] = score_performance()
    x["forming_history"] = []
    x["capital_summary"] = capital_summary()

    return jsonify(x)


@app.route("/api/live-market/<symbol>")
def api_live_market(symbol):
    symbol = "".join(
        ch
        for ch in str(symbol or "").upper()
        if ch.isalnum()
    )

    if not symbol.endswith("USDT"):
        return jsonify({"error": "invalid symbol"}), 400

    try:
        price = current_price(symbol)

        rsi15 = timeframe_rsi(symbol, "15m")
        rsi1h = timeframe_rsi(symbol, "1h")
        rsi4h = timeframe_rsi(symbol, "4h")

        reg = regime_4h(symbol)
        st = structure_1h(symbol)

        ob = raw_order_book(symbol, 100)

        bids_raw = ob.get("bids", [])[:12]
        asks_raw = ob.get("asks", [])[:12]

        bids = []
        asks = []

        bid_usd = 0.0
        ask_usd = 0.0

        for p, q in bids_raw:
            px = float(p)
            qty = float(q)
            usd = px * qty
            bid_usd += usd

            bids.append({
                "price": px,
                "qty": qty,
                "usd": usd,
            })

        for p, q in asks_raw:
            px = float(p)
            qty = float(q)
            usd = px * qty
            ask_usd += usd

            asks.append({
                "price": px,
                "qty": qty,
                "usd": usd,
            })

        total = bid_usd + ask_usd

        book_imb = (
            (bid_usd - ask_usd) / total
            if total
            else 0.0
        )

        buy_pct = (
            (bid_usd / total) * 100
            if total
            else 50.0
        )

        sell_pct = 100.0 - buy_pct

        spread_bps = 999.0

        if bids and asks:
            best_bid = bids[0]["price"]
            best_ask = asks[0]["price"]
            mid = (best_bid + best_ask) / 2

            if mid:
                spread_bps = (
                    (best_ask - best_bid)
                    / mid
                    * 10000
                )

        delta, buy, sell = flow_metrics(symbol)
        oi = oi_change_pct(symbol)

        setup = multi_strategy_setup_15m(symbol, reg, st)
        side = setup.get("side") if setup.get("valid") else None
        trigger = entry_trigger_5m(symbol, side) if side else None

        return jsonify({
            "symbol": symbol,
            "strategy_version": STRATEGY_VERSION,
            "data_source": "Bitget USDT-M Futures",
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "price": price,

            "regime_4h": reg,
            "structure_1h": st,
            "setup_15m": setup,
            "trigger_5m": trigger,

            "rsi_15m": round(rsi15, 2),
            "rsi_1h": round(rsi1h, 2),
            "rsi_4h": round(rsi4h, 2),

            "bids": bids,
            "asks": asks,
            "bid_usd": bid_usd,
            "ask_usd": ask_usd,
            "buy_pct": round(buy_pct, 2),
            "sell_pct": round(sell_pct, 2),
            "book_imb": round(book_imb, 6),
            "spread_bps": round(spread_bps, 4),

            "flow_delta": round(delta, 6),
            "buy_usd_60s": buy,
            "sell_usd_60s": sell,
            "oi_change_pct": round(oi, 6),
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

threading.Thread(
    target=scanner_loop,
    daemon=True,
).start()

threading.Thread(
    target=telegram_hourly_status_loop,
    daemon=True,
).start()

threading.Thread(
    target=trade_monitor_loop,
    daemon=True,
).start()

# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "10000",
            )
        ),
    )
