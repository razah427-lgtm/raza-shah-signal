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
# RAZA SHAH SIGNAL — V3 SMART MONEY TP/SL
# BITGET USDT-M FUTURES
# SIGNAL ONLY — NO AUTO ORDERS
#
# CORE FLOW:
# 4H REGIME
# -> 1H STRUCTURE
# -> 15M CONTINUATION / BREAKOUT / LIQUIDITY SETUP
# -> 5M EXECUTION TIMING (BONUS, NOT HARD VETO)
# -> LIVE FLOW / ORDER BOOK / OI
# -> DYNAMIC RISK
# -> QUALITY SCORE + BTC CONTEXT (RANKING ONLY)
# -> PAPER SIGNAL
# ============================================================

STRATEGY_VERSION = "V2R2_RESEARCH_STABLE"
BUILD_VERSION = "V3_SMART_MONEY_TPSL"

BITGET_BASE = "https://api.bitget.com"
BITGET_PRODUCT_TYPE = "usdt-futures"
DATABASE_URL = os.getenv("DATABASE_URL")

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "300"))
TOP_COINS = int(os.getenv("TOP_COINS", "100"))
DEEP_CHECK = int(os.getenv("DEEP_CHECK", "50"))

LIGHT_SCAN_WORKERS = int(os.getenv("LIGHT_SCAN_WORKERS", "8"))
DEEP_SCAN_WORKERS = int(os.getenv("DEEP_SCAN_WORKERS", "4"))
SCAN_HTTP_TIMEOUT = float(os.getenv("SCAN_HTTP_TIMEOUT", "12"))
SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "14400"))
TRADE_MONITOR_INTERVAL = int(os.getenv("TRADE_MONITOR_INTERVAL", "20"))

# ----------------------------
# V2 HARD FILTERS
# ----------------------------

MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "2.0"))

MIN_FLOW_DELTA = float(os.getenv("MIN_FLOW_DELTA", "0.08"))
MIN_BOOK_IMB = float(os.getenv("MIN_BOOK_IMB", "0.08"))
MIN_SECONDARY_MICRO = float(os.getenv("MIN_SECONDARY_MICRO", "0.03"))

MIN_VOL_RATIO = float(os.getenv("MIN_VOL_RATIO", "0.95"))

# OI is supportive, but first snapshot may be 0.0.
# Reject only if OI is strongly against the move.
MIN_OI_CHANGE_PCT = float(os.getenv("MIN_OI_CHANGE_PCT", "-0.10"))

# 15m volatility filter
MIN_ATR_PCT = float(os.getenv("MIN_ATR_PCT", "0.0010"))   # 0.10%
MAX_ATR_PCT = float(os.getenv("MAX_ATR_PCT", "0.0300"))   # 3.00%

# Liquidity sweep / retest
LIQ_LOOKBACK = int(os.getenv("LIQ_LOOKBACK", "24"))
SWEEP_BUFFER_PCT = float(os.getenv("SWEEP_BUFFER_PCT", "0.0015"))
RETEST_DISTANCE_PCT = float(os.getenv("RETEST_DISTANCE_PCT", "0.0075"))

# Dynamic risk
RR_TARGET = float(os.getenv("RR_TARGET", "1.80"))
MIN_STOP_PCT = float(os.getenv("MIN_STOP_PCT", "0.0040"))  # 0.40%
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "0.0100"))  # 1.00%
ATR_STOP_MULT = float(os.getenv("ATR_STOP_MULT", "1.10"))

# Smart Money / order-book heat TP-SL layer.
# This uses public-market proxies (liquidity walls + aggressive large prints);
# it does not claim to identify a real-world whale wallet or guarantee an order is genuine.
SMART_MIN_RR = float(os.getenv("SMART_MIN_RR", "1.25"))
SMART_MAX_RR = float(os.getenv("SMART_MAX_RR", "2.40"))
SMART_STRONG_RR = float(os.getenv("SMART_STRONG_RR", "2.20"))
SMART_WEAK_RR = float(os.getenv("SMART_WEAK_RR", "1.55"))
SMART_ATR_BUFFER_MULT = float(os.getenv("SMART_ATR_BUFFER_MULT", "0.15"))
SMART_PRICE_BUFFER_PCT = float(os.getenv("SMART_PRICE_BUFFER_PCT", "0.0004"))
ORDERBOOK_WALL_MIN_RATIO = float(os.getenv("ORDERBOOK_WALL_MIN_RATIO", "3.0"))
ORDERBOOK_HEAT_MAX_DIST_PCT = float(os.getenv("ORDERBOOK_HEAT_MAX_DIST_PCT", "0.025"))
ORDERBOOK_SL_WALL_MAX_DIST_PCT = float(os.getenv("ORDERBOOK_SL_WALL_MAX_DIST_PCT", "0.012"))
WHALE_TRADE_MIN_USD = float(os.getenv("WHALE_TRADE_MIN_USD", "5000"))
WHALE_TRADE_MEDIAN_MULT = float(os.getenv("WHALE_TRADE_MEDIAN_MULT", "5.0"))

# BTC market regime is CONTEXT ONLY in V2R1.
# It is recorded and scored, but it never hard-rejects an otherwise valid altcoin setup.
BTC_FILTER_ENABLED = False

# KSA session filter
SESSION_FILTER_ENABLED = False
KSA_SESSION_START = int(os.getenv("KSA_SESSION_START", "15"))
KSA_SESSION_END = int(os.getenv("KSA_SESSION_END", "22"))

# Quality score is DISPLAY / RANKING ONLY.
# It does NOT independently create a trade.
MIN_TRADE_SCORE = int(os.getenv("MIN_TRADE_SCORE", "85"))
STRONG_QUALITY_SCORE = int(os.getenv("STRONG_QUALITY_SCORE", "85"))

# Paper tester
TEST_START_CAPITAL = float(os.getenv("TEST_START_CAPITAL", "100"))
TEST_LEVERAGE = float(os.getenv("TEST_LEVERAGE", "20"))

# Cost-aware paper tester. Bitget standard taker fee is commonly 0.06% per side.
# Slippage is a conservative paper-test assumption and can be overridden in Render env.
PAPER_TAKER_FEE_RATE = float(os.getenv("PAPER_TAKER_FEE_RATE", "0.0006"))
PAPER_SLIPPAGE_PER_SIDE = float(os.getenv("PAPER_SLIPPAGE_PER_SIDE", "0.0002"))

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

LIVE_FILE = DATA_DIR / "live_signals_v2.csv"
STATE_FILE = DATA_DIR / "scanner_state_v2.json"

CSV_COLUMNS = [
    "time_utc",
    "strategy_version",
    "symbol",
    "signal",
    "score",
    "price",
    "regime_4h",
    "structure_1h",
    "setup_15m",
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
    "tp",
    "sl",
]

# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()
session.headers.update({
    "User-Agent": "RAZA-SHAH-SIGNAL-BITGET-V2/6.0",
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

                # Safe migration: preserve old rows and mark only new V2 rows.
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

        print("[DB] PostgreSQL tables ready for V2", flush=True)

    except Exception as e:
        print(f"[DB] INIT ERROR: {type(e).__name__}: {e}", flush=True)

# ============================================================
# STATE
# ============================================================

state_lock = threading.Lock()

state = {
    "running": False,
    "strategy_version": STRATEGY_VERSION,
    "build_version": BUILD_VERSION,
    "exchange": "BITGET",
    "data_source": "Bitget USDT-M Futures",
    "status": "Starting V3...",
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
    symbols = [symbol for symbol, _ in rows[:TOP_COINS]]

    if not symbols:
        raise RuntimeError("No Bitget top futures symbols loaded")

    scan_log(f"TOP SYMBOLS LOADED: {len(symbols)}")
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
    # V2 uses completed bars only for HTF / setup decisions.
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
# V2 LAYER 1 — 4H REGIME
# ============================================================

def regime_4h(symbol):
    c = candle_dicts(symbol, "4h", 120)

    if len(c) < 60:
        raise RuntimeError(f"Not enough 4H candles for {symbol}")

    closes = [x["close"] for x in c]

    e20 = ema(closes[-80:], 20)
    e50 = ema(closes[-100:], 50)

    recent_e20 = ema(closes[-70:-4], 20)
    slope = (
        (e20 / recent_e20) - 1
        if recent_e20 > 0
        else 0.0
    )

    price = closes[-1]
    a = atr_value(c[-40:], 14)
    atr_pct = a / price if price > 0 else 0.0

    # Avoid calling tiny EMA differences a real trend.
    separation = abs(e20 - e50) / price if price > 0 else 0.0

    if (
        e20 > e50
        and price > e20
        and slope > 0
        and separation >= 0.0007
    ):
        regime = "BULL"

    elif (
        e20 < e50
        and price < e20
        and slope < 0
        and separation >= 0.0007
    ):
        regime = "BEAR"

    else:
        regime = "SIDEWAYS"

    return {
        "regime": regime,
        "price": price,
        "ema20": e20,
        "ema50": e50,
        "slope": slope,
        "atr_pct": atr_pct,
    }

# ============================================================
# V3 LAYER 2 — 1H MOMENTUM / TREND CONFIRMATION
# ============================================================

def structure_1h(symbol):
    c = candle_dicts(symbol, "1h", 120)

    if len(c) < 60:
        raise RuntimeError(f"Not enough 1H candles for {symbol}")

    closes = [x["close"] for x in c]
    price = closes[-1]
    e20 = ema(closes[-80:], 20)
    e50 = ema(closes[-100:], 50)
    e20_prev = ema(closes[-86:-6], 20)
    ema_slope = ((e20 / e20_prev) - 1.0) if e20_prev > 0 else 0.0
    r = rsi(closes[-80:], 14)
    a = atr_value(c[-50:], 14)
    atr_pct = a / price if price > 0 else 0.0

    # 12-hour price impulse normalised by ATR. This is more robust than
    # requiring a textbook HH/HL or LH/LL pattern on every rolling block.
    base = closes[-13]
    impulse = ((price / base) - 1.0) if base > 0 else 0.0
    impulse_atr = impulse / max(atr_pct, 1e-6)

    bull_points = 0
    bear_points = 0

    if e20 > e50: bull_points += 2
    if price > e20: bull_points += 2
    if ema_slope > 0: bull_points += 2
    if r >= 52: bull_points += 1
    if impulse_atr >= 0.35: bull_points += 1

    if e20 < e50: bear_points += 2
    if price < e20: bear_points += 2
    if ema_slope < 0: bear_points += 2
    if r <= 48: bear_points += 1
    if impulse_atr <= -0.35: bear_points += 1

    if bull_points >= 6 and bull_points >= bear_points + 2:
        structure = "BULL_MOMENTUM"
        strength = int(round((bull_points / 8.0) * 100))
    elif bear_points >= 6 and bear_points >= bull_points + 2:
        structure = "BEAR_MOMENTUM"
        strength = int(round((bear_points / 8.0) * 100))
    else:
        structure = "RANGE"
        strength = int(round((max(bull_points, bear_points) / 8.0) * 100))

    return {
        "structure": structure,
        "strength": strength,
        "bull_points": bull_points,
        "bear_points": bear_points,
        "ema20": e20,
        "ema50": e50,
        "ema20_slope": ema_slope,
        "rsi": r,
        "impulse_12h": impulse,
        "impulse_atr": impulse_atr,
        "atr_pct": atr_pct,
    }

# ============================================================
# V3 LAYER 3 — 15M CONTINUATION / BREAKOUT / LIQUIDITY SETUP
# ============================================================

def liquidity_setup_15m(symbol, side):
    c = candle_dicts(symbol, "15m", max(120, LIQ_LOOKBACK + 50))

    if len(c) < 70:
        raise RuntimeError(f"Not enough 15M candles for {symbol}")

    last = c[-1]
    prev = c[-2]
    closes = [x["close"] for x in c]
    e20 = ema(closes[-70:], 20)
    e50 = ema(closes[-90:], 50)
    price = last["close"]

    atr15 = atr_value(c[-50:], 14)
    atr_pct = atr15 / price if price > 0 else 0.0
    vr = volume_ratio(c, 20)
    volatility_ok = MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT

    # Completed-bar levels only.
    recent = c[-23:-3]
    prior_high = max(x["high"] for x in recent)
    prior_low = min(x["low"] for x in recent)

    # Wider external-liquidity pool used only for smart TP placement.
    external = c[-55:-3]
    external_high = max(x["high"] for x in external)
    external_low = min(x["low"] for x in external)

    rng = max(last["high"] - last["low"], 1e-12)
    close_loc = (last["close"] - last["low"]) / rng
    lower_wick = min(last["open"], last["close"]) - last["low"]
    upper_wick = last["high"] - max(last["open"], last["close"])

    bullish_bar = (last["close"] > last["open"] and close_loc >= 0.55) or (lower_wick >= 0.30 * rng and close_loc >= 0.60)
    bearish_bar = (last["close"] < last["open"] and close_loc <= 0.45) or (upper_wick >= 0.30 * rng and close_loc <= 0.40)

    if side == "BUY":
        trend_aligned = e20 >= e50 * 0.999 and price >= e20 * 0.997
        pullback = (
            trend_aligned
            and last["low"] <= e20 * 1.004
            and last["close"] > e20
            and bullish_bar
        )
        breakout = (
            prev["close"] > prior_high
            and last["low"] <= prior_high * (1 + RETEST_DISTANCE_PCT)
            and last["close"] > prior_high
            and close_loc >= 0.50
        )
        sweep = (
            last["low"] <= prior_low * (1 + SWEEP_BUFFER_PCT)
            and last["close"] > prior_low
            and bullish_bar
        )

        valid = volatility_ok and (pullback or breakout or sweep)
        setup_name = (
            "BREAKOUT_RETEST" if breakout
            else "LOW_SWEEP_RECLAIM" if sweep
            else "PULLBACK_CONTINUATION" if pullback
            else "NONE"
        )
        liquidity_level = prior_high if breakout else prior_low if sweep else e20
        sweep_extreme = min(last["low"], prev["low"], e20)

    else:
        trend_aligned = e20 <= e50 * 1.001 and price <= e20 * 1.003
        pullback = (
            trend_aligned
            and last["high"] >= e20 * 0.996
            and last["close"] < e20
            and bearish_bar
        )
        breakout = (
            prev["close"] < prior_low
            and last["high"] >= prior_low * (1 - RETEST_DISTANCE_PCT)
            and last["close"] < prior_low
            and close_loc <= 0.50
        )
        sweep = (
            last["high"] >= prior_high * (1 - SWEEP_BUFFER_PCT)
            and last["close"] < prior_high
            and bearish_bar
        )

        valid = volatility_ok and (pullback or breakout or sweep)
        setup_name = (
            "BREAKOUT_RETEST" if breakout
            else "HIGH_SWEEP_REJECT" if sweep
            else "PULLBACK_CONTINUATION" if pullback
            else "NONE"
        )
        liquidity_level = prior_low if breakout else prior_high if sweep else e20
        sweep_extreme = max(last["high"], prev["high"], e20)

    return {
        "valid": bool(valid),
        "setup": setup_name,
        "price": price,
        "prior_high": prior_high,
        "prior_low": prior_low,
        "external_high": external_high,
        "external_low": external_low,
        "liquidity_level": liquidity_level,
        "sweep_extreme": sweep_extreme,
        "ema20": e20,
        "ema50": e50,
        "atr": atr15,
        "atr_pct": atr_pct,
        "vol_ratio": vr,
        "volatility_ok": volatility_ok,
        "pullback": bool(pullback),
        "breakout": bool(breakout),
        "sweep": bool(sweep),
    }

# ============================================================
# V2 LAYER 4 — 5M ENTRY TRIGGER
# ============================================================

def entry_trigger_5m(symbol, side):
    c = candle_dicts(symbol, "5m", 80)
    if len(c) < 30:
        raise RuntimeError(f"Not enough 5M candles for {symbol}")

    closes = [x["close"] for x in c]
    e9 = ema(closes[-40:], 9)
    e21 = ema(closes[-50:], 21)
    last, prev = c[-1], c[-2]
    rng = max(last["high"] - last["low"], 1e-12)
    close_loc = (last["close"] - last["low"]) / rng

    if side == "BUY":
        trend_ok = e9 >= e21 * 0.9995 and last["close"] > e9
        momentum = last["close"] > prev["high"]
        reclaim = last["low"] <= e9 * 1.0025 and last["close"] > prev["close"] and close_loc >= 0.55
        trigger = trend_ok and (momentum or reclaim)
        name = "BULL_5M_TRIGGER" if trigger else "WAIT"
    else:
        trend_ok = e9 <= e21 * 1.0005 and last["close"] < e9
        momentum = last["close"] < prev["low"]
        reclaim = last["high"] >= e9 * 0.9975 and last["close"] < prev["close"] and close_loc <= 0.45
        trigger = trend_ok and (momentum or reclaim)
        name = "BEAR_5M_TRIGGER" if trigger else "WAIT"

    return {"valid": bool(trigger), "trigger": name, "ema9": e9, "ema21": e21, "price": last["close"]}

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


def _median_value(values):
    vals = sorted(float(v) for v in values if float(v) > 0)
    if not vals:
        return 0.0
    n = len(vals)
    m = n // 2
    if n % 2:
        return vals[m]
    return (vals[m - 1] + vals[m]) / 2.0


def _strongest_book_wall(levels, mid, is_bid):
    """Return the strongest nearby resting-liquidity wall as a market proxy."""
    rows = []

    for price, qty in levels:
        try:
            px = float(price)
            q = float(qty)
            if px <= 0 or q <= 0 or mid <= 0:
                continue

            dist_pct = abs(px - mid) / mid
            if dist_pct > ORDERBOOK_HEAT_MAX_DIST_PCT:
                continue

            # A bid wall must sit below/at mid; an ask wall above/at mid.
            if is_bid and px > mid:
                continue
            if (not is_bid) and px < mid:
                continue

            usd = px * q
            rows.append((px, q, usd, dist_pct))
        except Exception:
            continue

    if not rows:
        return None

    median_usd = _median_value([r[2] for r in rows])
    if median_usd <= 0:
        return None

    candidates = []
    for px, q, usd, dist_pct in rows:
        ratio = usd / median_usd
        if ratio < ORDERBOOK_WALL_MIN_RATIO:
            continue

        # Favor genuinely large walls but slightly penalize far-away levels.
        proximity = max(0.15, 1.0 - (dist_pct / max(ORDERBOOK_HEAT_MAX_DIST_PCT, 1e-9)))
        score = min(ratio, 20.0) * proximity
        candidates.append((score, px, q, usd, dist_pct, ratio))

    if not candidates:
        return None

    _, px, q, usd, dist_pct, ratio = max(candidates, key=lambda x: x[0])
    return {
        "price": px,
        "qty": q,
        "usd": usd,
        "distance_pct": dist_pct,
        "ratio_to_median": ratio,
    }


def orderbook_heat_metrics(symbol, limit=100):
    data = raw_order_book(symbol, limit)

    bids_raw = data.get("bids", [])
    asks_raw = data.get("asks", [])

    if not bids_raw or not asks_raw:
        return None

    best_bid = float(bids_raw[0][0])
    best_ask = float(asks_raw[0][0])
    mid = (best_bid + best_ask) / 2.0

    if mid <= 0:
        return None

    spread_bps = ((best_ask - best_bid) / mid) * 10000.0

    # Keep the same top-of-book behavior used by the approved build.
    bids = bids_raw[:30]
    asks = asks_raw[:30]

    bid_usd = sum(float(price) * float(qty) for price, qty in bids)
    ask_usd = sum(float(price) * float(qty) for price, qty in asks)
    total = bid_usd + ask_usd
    book_imb = ((bid_usd - ask_usd) / total) if total else 0.0

    # Heat walls scan a wider slice of the public book, still capped by distance.
    bid_wall = _strongest_book_wall(bids_raw[:80], mid, True)
    ask_wall = _strongest_book_wall(asks_raw[:80], mid, False)

    return {
        "spread_bps": spread_bps,
        "book_imb": book_imb,
        "mid": mid,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_wall": bid_wall,
        "ask_wall": ask_wall,
    }


def depth_metrics(symbol, limit=100):
    """Backward-compatible tuple for existing dashboard/API code."""
    heat = orderbook_heat_metrics(symbol, limit)
    if not heat:
        return None
    return heat["spread_bps"], heat["book_imb"]

# ============================================================
# AGGRESSIVE TRADE FLOW
# ============================================================

def flow_metrics_detailed(symbol):
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
    prints = []

    for t in trades:
        try:
            trade_time = int(t.get("ts") or t.get("timestamp") or 0)
            if trade_time < cutoff:
                continue

            price = float(t.get("price") or 0)
            qty = float(t.get("size") or t.get("qty") or 0)
            usd = price * qty
            side = str(t.get("side") or "").lower()

            if usd <= 0 or side not in ("buy", "sell"):
                continue

            prints.append((side, usd))

            if side == "buy":
                buy += usd
            else:
                sell += usd
        except Exception:
            pass

    total = buy + sell
    delta = ((buy - sell) / total) if total else 0.0

    median_print = _median_value([usd for _, usd in prints])
    whale_threshold = max(
        WHALE_TRADE_MIN_USD,
        median_print * WHALE_TRADE_MEDIAN_MULT,
    )

    whale_buy = sum(usd for s, usd in prints if s == "buy" and usd >= whale_threshold)
    whale_sell = sum(usd for s, usd in prints if s == "sell" and usd >= whale_threshold)
    whale_total = whale_buy + whale_sell
    whale_delta = ((whale_buy - whale_sell) / whale_total) if whale_total else 0.0

    largest_buy = max((usd for s, usd in prints if s == "buy"), default=0.0)
    largest_sell = max((usd for s, usd in prints if s == "sell"), default=0.0)

    return {
        "delta": delta,
        "buy": buy,
        "sell": sell,
        "whale_delta": whale_delta,
        "whale_buy": whale_buy,
        "whale_sell": whale_sell,
        "whale_threshold": whale_threshold,
        "largest_buy": largest_buy,
        "largest_sell": largest_sell,
    }


def flow_metrics(symbol):
    """Backward-compatible aggregate flow tuple."""
    m = flow_metrics_detailed(symbol)
    return m["delta"], m["buy"], m["sell"]

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
    # V2R1: BTC is context only, never a hard rejection gate.
    # Keeping this function preserves compatibility with the existing scan flow.
    return True


def btc_context_points(side, btc_regime):
    regime = str((btc_regime or {}).get("regime") or "SIDEWAYS")
    aligned = (
        (side == "BUY" and regime == "BULL")
        or (side == "SELL" and regime == "BEAR")
    )
    conflict = (
        (side == "BUY" and regime == "BEAR")
        or (side == "SELL" and regime == "BULL")
    )
    if aligned:
        return 3
    if conflict:
        return 0
    return 1

# ============================================================
# V2 DYNAMIC RISK
# ============================================================

def dynamic_risk(entry, side, setup, heat=None, whale=None):
    """
    Smart TP/SL model:
    - SL sits beyond SMC invalidation (sweep/structure) with an ATR buffer.
    - A strong same-side order-book wall can widen the invalidation point, but
      only when it is close enough and still inside MAX_STOP_PCT.
    - TP is pulled toward the first meaningful opposing liquidity pool/wall.
    - Strong aligned large-print flow + book pressure can extend RR; weak flow
      reduces RR. Static walls are treated as proxies because they can be pulled.
    """
    atr15 = float(setup.get("atr") or 0)
    extreme = float(setup.get("sweep_extreme") or entry)

    if entry <= 0 or atr15 <= 0:
        return None

    heat = heat or {}
    whale = whale or {}

    atr_dist = ATR_STOP_MULT * atr15
    buffer_dist = max(
        SMART_ATR_BUFFER_MULT * atr15,
        entry * SMART_PRICE_BUFFER_PCT,
    )

    bid_wall = heat.get("bid_wall") or {}
    ask_wall = heat.get("ask_wall") or {}

    # ----------------------------
    # SMART-MONEY INVALIDATION SL
    # ----------------------------
    if side == "BUY":
        structural_sl = min(
            extreme - buffer_dist,
            entry - atr_dist,
        )
        sl = structural_sl

        wall_px = float(bid_wall.get("price") or 0)
        wall_ratio = float(bid_wall.get("ratio_to_median") or 0)
        wall_dist = float(bid_wall.get("distance_pct") or 999)

        if (
            wall_px > 0
            and wall_px < entry
            and wall_ratio >= ORDERBOOK_WALL_MIN_RATIO
            and wall_dist <= ORDERBOOK_SL_WALL_MAX_DIST_PCT
        ):
            wall_sl = wall_px - buffer_dist
            wall_risk_pct = (entry - wall_sl) / entry
            if 0 < wall_risk_pct <= MAX_STOP_PCT:
                sl = min(sl, wall_sl)

        raw_dist = entry - sl

    else:
        structural_sl = max(
            extreme + buffer_dist,
            entry + atr_dist,
        )
        sl = structural_sl

        wall_px = float(ask_wall.get("price") or 0)
        wall_ratio = float(ask_wall.get("ratio_to_median") or 0)
        wall_dist = float(ask_wall.get("distance_pct") or 999)

        if (
            wall_px > entry
            and wall_ratio >= ORDERBOOK_WALL_MIN_RATIO
            and wall_dist <= ORDERBOOK_SL_WALL_MAX_DIST_PCT
        ):
            wall_sl = wall_px + buffer_dist
            wall_risk_pct = (wall_sl - entry) / entry
            if 0 < wall_risk_pct <= MAX_STOP_PCT:
                sl = max(sl, wall_sl)

        raw_dist = sl - entry

    raw_pct = raw_dist / entry

    if raw_pct > MAX_STOP_PCT:
        return None

    # Preserve the approved minimum stop floor.
    risk_pct = max(raw_pct, MIN_STOP_PCT)
    if risk_pct > MAX_STOP_PCT:
        return None

    risk_dist = entry * risk_pct
    if side == "BUY":
        sl = entry - risk_dist
    else:
        sl = entry + risk_dist

    # ----------------------------
    # WHALE / HEAT ADAPTIVE RR
    # ----------------------------
    book_dir = float(heat.get("book_imb") or 0)
    whale_dir = float(whale.get("whale_delta") or 0)
    if side == "SELL":
        book_dir = -book_dir
        whale_dir = -whale_dir

    target_rr = RR_TARGET

    if whale_dir >= 0.25 and book_dir >= 0.15:
        target_rr = max(target_rr, SMART_STRONG_RR)
    elif whale_dir <= -0.15 or book_dir <= -0.10:
        target_rr = min(target_rr, SMART_WEAK_RR)

    target_rr = min(SMART_MAX_RR, max(SMART_MIN_RR, target_rr))

    if side == "BUY":
        base_tp = entry + (risk_dist * target_rr)
    else:
        base_tp = entry - (risk_dist * target_rr)

    # ----------------------------
    # SMC + OPPOSING HEAT TP
    # ----------------------------
    liquidity_candidates = []

    if side == "BUY":
        for key in ("prior_high", "external_high"):
            level = float(setup.get(key) or 0)
            if level > entry:
                liquidity_candidates.append((level, f"SMC_{key.upper()}"))

        wall_px = float(ask_wall.get("price") or 0)
        wall_ratio = float(ask_wall.get("ratio_to_median") or 0)
        if wall_px > entry and wall_ratio >= ORDERBOOK_WALL_MIN_RATIO:
            liquidity_candidates.append((wall_px, "ORDERBOOK_ASK_WALL"))

        tp = base_tp
        tp_source = "ADAPTIVE_RR"

        for level, source in sorted(liquidity_candidates, key=lambda x: x[0]):
            # Exit just before visible opposing liquidity rather than inside it.
            candidate_tp = level - max(buffer_dist * 0.35, entry * 0.0002)
            rr = (candidate_tp - entry) / risk_dist
            if rr >= SMART_MIN_RR and candidate_tp < tp:
                tp = candidate_tp
                tp_source = source
                break

        effective_rr = (tp - entry) / risk_dist

    else:
        for key in ("prior_low", "external_low"):
            level = float(setup.get(key) or 0)
            if 0 < level < entry:
                liquidity_candidates.append((level, f"SMC_{key.upper()}"))

        wall_px = float(bid_wall.get("price") or 0)
        wall_ratio = float(bid_wall.get("ratio_to_median") or 0)
        if 0 < wall_px < entry and wall_ratio >= ORDERBOOK_WALL_MIN_RATIO:
            liquidity_candidates.append((wall_px, "ORDERBOOK_BID_WALL"))

        tp = base_tp
        tp_source = "ADAPTIVE_RR"

        for level, source in sorted(liquidity_candidates, key=lambda x: x[0], reverse=True):
            candidate_tp = level + max(buffer_dist * 0.35, entry * 0.0002)
            rr = (entry - candidate_tp) / risk_dist
            if rr >= SMART_MIN_RR and candidate_tp > tp:
                tp = candidate_tp
                tp_source = source
                break

        effective_rr = (entry - tp) / risk_dist

    if effective_rr < SMART_MIN_RR:
        return None

    return {
        "risk_pct": risk_pct,
        "risk_dist": risk_dist,
        "sl": sl,
        "tp": tp,
        "rr": effective_rr,
        "target_rr": target_rr,
        "tp_source": tp_source,
        "sl_source": "SMC_STRUCTURE_PLUS_HEAT",
        "whale_delta": float(whale.get("whale_delta") or 0),
        "bid_wall_price": float(bid_wall.get("price") or 0),
        "ask_wall_price": float(ask_wall.get("price") or 0),
        "bid_wall_ratio": float(bid_wall.get("ratio_to_median") or 0),
        "ask_wall_ratio": float(ask_wall.get("ratio_to_median") or 0),
    }

# ============================================================
# V2 QUALITY SCORE — RANKING ONLY
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
    btc_regime=None,
):
    score = 0

    # HTF regime
    if (
        (side == "BUY" and regime.get("regime") == "BULL")
        or
        (side == "SELL" and regime.get("regime") == "BEAR")
    ):
        score += 20
    else:
        score += 12

    # 1H structure
    if (
        (side == "BUY" and structure.get("structure") == "BULL_MOMENTUM")
        or (side == "SELL" and structure.get("structure") == "BEAR_MOMENTUM")
    ):
        score += 20

    # 15m setup quality
    if setup.get("valid"):
        score += 20
        if str(setup.get("setup")) in ("BREAKOUT_RETEST", "LOW_SWEEP_RECLAIM", "HIGH_SWEEP_REJECT"):
            score += 5

    # 5m trigger
    if trigger.get("valid"):
        score += 10

    # Direction-aware live flow. Opposite flow never earns quality points.
    flow_dir = delta if side == "BUY" else -delta
    if flow_dir >= 0.50:
        score += 10
    elif flow_dir >= 0.30:
        score += 8
    elif flow_dir >= MIN_FLOW_DELTA:
        score += 6

    # Direction-aware order book.
    book_dir = book if side == "BUY" else -book
    if book_dir >= 0.50:
        score += 10
    elif book_dir >= 0.30:
        score += 8
    elif book_dir >= MIN_BOOK_IMB:
        score += 6

    # Spread
    if spread <= 0.5:
        score += 5
    elif spread <= 1.0:
        score += 4
    elif spread <= MAX_SPREAD_BPS:
        score += 3

    # Volume
    vr = float(setup.get("vol_ratio") or 0)
    if vr >= 1.5:
        score += 5
    elif vr >= 1.1:
        score += 4
    elif vr >= MIN_VOL_RATIO:
        score += 2

    # OI is participation/support only; it is not directional by itself.
    if oi > 0:
        score += 5
    elif oi >= MIN_OI_CHANGE_PCT:
        score += 2

    # BTC is a small context bonus only, never a trade veto.
    score += btc_context_points(side, btc_regime)

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
# V3 LIVE MICROSTRUCTURE CONFIRMATION
# ============================================================

def live_confirmation(side, delta, book, spread, oi):
    reasons = []

    if spread > MAX_SPREAD_BPS:
        reasons.append("SPREAD")

    flow_dir = delta if side == "BUY" else -delta
    book_dir = book if side == "BUY" else -book

    # Require one strong live confirmation plus at least non-hostile support
    # from the second source. This avoids demanding two noisy snapshots to
    # simultaneously exceed the same threshold.
    micro_ok = (
        (flow_dir >= MIN_FLOW_DELTA and book_dir >= MIN_SECONDARY_MICRO)
        or
        (book_dir >= MIN_BOOK_IMB and flow_dir >= MIN_SECONDARY_MICRO)
    )

    if not micro_ok:
        if flow_dir < MIN_SECONDARY_MICRO:
            reasons.append("FLOW")
        if book_dir < MIN_SECONDARY_MICRO:
            reasons.append("BOOK")
        if not reasons:
            reasons.append("MICROSTRUCTURE")

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
# PERFORMANCE / CAPITAL — V2 ONLY
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

        # Round-trip taker fees + conservative slippage, charged on leveraged notional.
        round_trip_cost = 2.0 * (PAPER_TAKER_FEE_RATE + PAPER_SLIPPAGE_PER_SIDE)
        return (raw - round_trip_cost) * TEST_LEVERAGE

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
    # V2 does NOT treat a "forming" candidate as a paper trade.
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
        "note": "V2R3 measures performance only after a fully validated trade entry.",
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
            "V2R3 score is a quality/ranking metric only. "
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
        "source": "V2R1 VALIDATED TRADES ONLY",
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
                f"{icon} V2 {label} — {symbol}\n"
                f"Exchange: BITGET FUTURES\n"
                f"Side: {side}\n"
                f"Entry: {entry:.8g}\n"
                f"Exit: {px:.8g}\n"
                f"TP: {tp:.8g}\n"
                f"SL: {sl:.8g}\n\n"
                f"📊 V2 PERFORMANCE\n"
                f"Trades: {p['total_trades']}\n"
                f"Wins: {p['wins']}\n"
                f"Losses: {p['losses']}\n"
                f"Winning: {p['win_rate']:.2f}%\n"
                f"Net P/L: ${p['net_pl']:.2f}\n"
                f"🔗 {APP_URL}"
            )


def check_forming_setups():
    # Intentionally disabled in V2.
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
    c15 = candle_dicts(symbol, "15m", 60)

    if len(c15) < 30:
        return None

    closes = [x["close"] for x in c15]

    r15 = rsi(closes, 14)
    vr = volume_ratio(c15, 20)

    a = atr_value(c15[-40:], 14)
    price = closes[-1]
    atr_pct = a / price if price > 0 else 0.0

    # Light ranking only: unusual RSI + volume + tradable volatility.
    rsi_extreme = abs(r15 - 50.0)

    rank = (
        rsi_extreme * 1.5
        + min(vr, 3.0) * 8
        + min(atr_pct * 10000, 100)
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
# V2 DEEP SCAN
# ============================================================

def _deep_scan_one(item):
    _, symbol, lm, btc_regime = item

    try:
        reg = regime_4h(symbol)

        if reg["regime"] == "SIDEWAYS":
            return {
                "symbol": symbol,
                "blocked": "4H_SIDEWAYS",
                "score": 0,
                "lm": lm,
                "error": None,
            }

        side = "BUY" if reg["regime"] == "BULL" else "SELL"

        if not btc_market_allows(side, btc_regime):
            return {
                "symbol": symbol,
                "blocked": "BTC_CONFLICT",
                "score": 0,
                "lm": lm,
                "regime": reg,
                "error": None,
            }

        st = structure_1h(symbol)

        structure_ok = (
            (side == "BUY" and st["structure"] == "BULL_MOMENTUM")
            or
            (side == "SELL" and st["structure"] == "BEAR_MOMENTUM")
        )

        if not structure_ok:
            return {
                "symbol": symbol,
                "side": side,
                "blocked": "1H_STRUCTURE",
                "score": 0,
                "lm": lm,
                "regime": reg,
                "structure": st,
                "error": None,
            }

        setup = liquidity_setup_15m(symbol, side)

        if not setup["valid"]:
            return {
                "symbol": symbol,
                "side": side,
                "blocked": "15M_LIQUIDITY",
                "score": 0,
                "lm": lm,
                "regime": reg,
                "structure": st,
                "setup": setup,
                "error": None,
            }

        # Participation filter.
        if setup["vol_ratio"] < MIN_VOL_RATIO:
            return {
                "symbol": symbol,
                "side": side,
                "blocked": "LOW_VOLUME",
                "score": 0,
                "lm": lm,
                "regime": reg,
                "structure": st,
                "setup": setup,
                "error": None,
            }

        trigger = entry_trigger_5m(symbol, side)

        # V3: 5M is an execution-quality bonus, not a mandatory veto.
        # Strong 4H/1H/15M + live microstructure can still qualify without it.

        heat = orderbook_heat_metrics(symbol)

        if not heat:
            return {
                "symbol": symbol,
                "side": side,
                "blocked": "NO_ORDER_BOOK",
                "score": 0,
                "error": None,
            }

        spread = heat["spread_bps"]
        book = heat["book_imb"]
        whale = flow_metrics_detailed(symbol)
        delta = whale["delta"]
        buy = whale["buy"]
        sell = whale["sell"]
        oi = oi_change_pct(symbol)

        live_ok, live_reasons = live_confirmation(
            side,
            delta,
            book,
            spread,
            oi,
        )

        q = quality_score(
            side,
            reg,
            st,
            setup,
            trigger,
            delta,
            book,
            spread,
            oi,
            btc_regime,
        )

        score_ok = q >= MIN_TRADE_SCORE
        trade_ready = live_ok and score_ok
        if not score_ok:
            live_reasons = list(live_reasons) + ["QUALITY_SCORE"]

        price = current_price(symbol)

        risk = dynamic_risk(
            price,
            side,
            setup,
            heat=heat,
            whale=whale,
        )

        if risk is None:
            return {
                "symbol": symbol,
                "side": side,
                "blocked": "SMART_RISK_NO_ROOM",
                "score": q,
                "lm": lm,
                "regime": reg,
                "structure": st,
                "setup": setup,
                "trigger": trigger,
                "delta": delta,
                "book": book,
                "spread": spread,
                "oi": oi,
                "error": None,
            }

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

            "regime_4h": reg["regime"],
            "structure_1h": st["structure"],
            "setup_15m": setup["setup"],
            "trigger_5m": trigger["trigger"],

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
            "tp_source": risk.get("tp_source"),
            "sl_source": risk.get("sl_source"),
            "whale_delta": risk.get("whale_delta", 0.0),
            "bid_wall_price": risk.get("bid_wall_price", 0.0),
            "ask_wall_price": risk.get("ask_wall_price", 0.0),
            "bid_wall_ratio": risk.get("bid_wall_ratio", 0.0),
            "ask_wall_ratio": risk.get("ask_wall_ratio", 0.0),

            "hard_confirm": trade_ready,
            "blocked_reasons": live_reasons,
            "status": (
                "TRADE READY"
                if trade_ready
                else "WAIT LIVE CONFIRMATION"
            ),
        }

        return {
            "symbol": symbol,
            "side": side,
            "score": q,
            "candidate": candidate,
            "blocked": None if trade_ready else ("QUALITY_SCORE" if not score_ok else "LIVE_CONFIRM"),
            "lm": lm,
            "regime": reg,
            "structure": st,
            "setup": setup,
            "trigger": trigger,
            "delta": delta,
            "buy": buy,
            "sell": sell,
            "spread": spread,
            "book": book,
            "oi": oi,
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
    scan_start = time.time()

    scan_log("================================")
    scan_log("RAZA V2R3 BALANCED BITGET FUTURES SCAN START")

    with state_lock:
        state["status"] = (
            f"V2R3 scanning Top {TOP_COINS} Bitget Futures..."
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
                f"V2 waiting for KSA session "
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
            in light_candidates[:20]
        ]

    scan_log(
        f"LIGHT SCAN COMPLETE: "
        f"{len(light_candidates)} candidates"
    )

    # ----------------------------
    # DEEP SCAN
    # ----------------------------

    deep_items = [
        (rank, symbol, lm, btc_reg)
        for rank, symbol, lm
        in light_candidates[:DEEP_CHECK]
    ]

    scan_log(
        f"V3 DEEP SCAN START: {len(deep_items)} candidates"
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
                    f"🚨 RAZA SHAH SIGNAL V3 — TRADE READY\n\n"
                    f"Exchange: BITGET FUTURES\n"
                    f"Coin: {c['symbol']}\n"
                    f"Side: "
                    f"{'LONG' if c['signal']=='BUY' else 'SHORT'}\n"
                    f"Quality: {c['score']}/100 ({c['risk_label']})\n\n"
                    f"4H: {c['regime_4h']}\n"
                    f"1H: {c['structure_1h']}\n"
                    f"15M: {c['setup_15m']}\n"
                    f"5M: {c['trigger_5m']}\n\n"
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
                    f"V2 Closed: {p['total_trades']}\n"
                    f"V2 Win Rate: {p['win_rate']:.2f}%\n"
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
                f"V3: {alerts} fully validated trade ready"
            )

        elif best:
            state["status"] = (
                f"V3 best: {best['symbol']} "
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
                f"V3 waiting — main block: {top_block}"
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
        f"V3 SCAN COMPLETE in {elapsed}s "
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
        f"🔐 RAZA SHAH SIGNAL V2\n"
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
<h2>🔐 RAZA SHAH SIGNAL V2</h2>
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
                f"🟢 RAZA SHAH SIGNAL V2\n"
                f"1 HOUR STATUS\n\n"
                f"Exchange: BITGET FUTURES\n"
                f"Scanner: {'LIVE' if st.get('running') else 'OFFLINE'}\n"
                f"Status: {st.get('status') or '—'}\n"
                f"BTC 4H: "
                f"{(st.get('market_regime') or {}).get('regime','—')}\n"
                f"{best_line}\n"
                f"Last alerts: {st.get('alerts_last_scan',0)}\n"
                f"Scan time: {st.get('last_scan_seconds','—')}s\n\n"
                f"V2 Trades: {p['total_trades']}\n"
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
    scan_log("V2 LIVE TP/SL MONITOR STARTED")

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
        state["build_version"] = BUILD_VERSION
        state["exchange"] = "BITGET"
        state["data_source"] = "Bitget USDT-M Futures"

    save_state_snapshot()

    p = performance()

    telegram_async(
        f"🟢 RAZA SHAH SIGNAL V2\n"
        f"LIVE PAPER TEST ACTIVE\n\n"
        f"Logic: 4H → 1H → 15M Liquidity → "
        f"5M Trigger → Flow/Book/OI\n"
        f"Exchange: BITGET FUTURES\n"
        f"Top Coins: {TOP_COINS}\n"
        f"Deep Scan: {DEEP_CHECK}\n"
        f"Scan interval: {SCAN_INTERVAL//60} minutes\n"
        f"KSA Session: "
        f"{KSA_SESSION_START}:00-{KSA_SESSION_END}:00 "
        f"({'ON' if SESSION_FILTER_ENABLED else 'OFF'})\n"
        f"BTC: CONTEXT ONLY (no hard block)\n"
        f"Dynamic R:R: 1:{RR_TARGET:.2f}\n"
        f"Paper Tester: "
        f"${TEST_START_CAPITAL:.0f} @ {TEST_LEVERAGE:.0f}x\n\n"
        f"V2 history: {p['total_trades']} trades\n"
        f"Winning: {p['win_rate']:.2f}%\n\n"
        f"Trade score gate: {MIN_TRADE_SCORE}+ after market confirmation.\n"
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
                state["status"] = "V2 scan error — retrying"

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

    if not is_authorized():
        ip = client_ip()
        _, created = ensure_otp_for_ip(ip)

        msg = (
            "Permission code sent to Telegram."
            if created
            else
            "Use the active Telegram code already sent for this IP."
        )

        return login_html(msg)

    return render_template("index.html")


@app.route("/verify", methods=["POST"])
def verify():
    ip = client_ip()

    code = (
        request.form.get("code")
        or ""
    ).strip()

    cleanup_access()
    now = time.time()

    with otp_lock:
        rec = otp_by_ip.get(ip)

        if (
            not rec
            or rec.get("expires", 0) <= now
        ):
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
            return login_html(
                "Wrong permission code."
            ), 401

        auth_until = now + ACCESS_TTL_SECONDS

        authorized_ips[ip] = auth_until
        otp_by_ip.pop(ip, None)

    flask_session.permanent = True
    flask_session["authorized_ip"] = ip
    flask_session["authorized_until"] = auth_until

    telegram_async(
        f"✅ RAZA SHAH SIGNAL V2 — ACCESS GRANTED\n"
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
        "build_version": BUILD_VERSION,
        "logic": (
            "4H regime → 1H structure → 15m liquidity "
            "→ 5m trigger → live flow/book/OI → dynamic risk"
        ),
        "risk_rules": {
            "score_role": "QUALITY ONLY",
            "trade_ready": (
                f"All V2R1 hard filters must pass and score must be >= {MIN_TRADE_SCORE}. "
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
    if not is_authorized():
        return jsonify({"error": "unauthorized"}), 401

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
    if not is_authorized():
        return jsonify({"error": "unauthorized"}), 401

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

        side = (
            "BUY"
            if reg["regime"] == "BULL"
            else "SELL"
            if reg["regime"] == "BEAR"
            else None
        )

        setup = (
            liquidity_setup_15m(symbol, side)
            if side
            else None
        )

        trigger = (
            entry_trigger_5m(symbol, side)
            if side
            else None
        )

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
