import os
import time
import json
import hmac
import base64
import hashlib
import threading
import secrets
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify

# ============================================================
# RAZA SHAH SIGNAL — V11 RSI VOLUME AUTO EXECUTION
# BITGET USDT-M FUTURES
#
# CORE:
# 15M RSI EXTREME + TURN
# -> REVERSAL VOLUME EXPANSION
# -> 15M LIQUIDITY REJECTION / RECLAIM
# -> 5M CHOCH
# -> LIVE FLOW + ORDER BOOK
# -> DYNAMIC STRUCTURAL SL
# -> COST-AWARE TP
# -> $100 STRATEGY CAPITAL / ~2% PLANNED RISK
# -> AUTO TRADE + EXCHANGE-SIDE TP/SL
#
# TELEGRAM:
# Notifications only. No OTP / no website auth.
# ============================================================

STRATEGY_VERSION = "V11_RSI_VOLUME_AUTO_20260817"
BITGET_BASE = "https://api.bitget.com"
PRODUCT_TYPE = "usdt-futures"
MARGIN_COIN = "USDT"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "300"))
TOP_COINS = int(os.getenv("TOP_COINS", "80"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "12"))

AUTO_TRADING = os.getenv("AUTO_TRADING", "false").lower() in ("1","true","yes","on")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() in ("1","true","yes","on")

# Dedicated strategy capital. Even if wallet has more, sizing uses at most this.
STRATEGY_CAPITAL_USDT = float(os.getenv("STRATEGY_CAPITAL_USDT", "100"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.02"))
MAX_LEVERAGE = float(os.getenv("MAX_LEVERAGE", "20"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "1"))
MAX_DAILY_LOSS_USDT = float(os.getenv("MAX_DAILY_LOSS_USDT", "6"))
SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "10800"))

MARGIN_MODE = os.getenv("MARGIN_MODE", "isolated").strip().lower()
if MARGIN_MODE not in ("isolated", "crossed"):
    MARGIN_MODE = "isolated"

# Entry logic
RSI_OVERBOUGHT = float(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "30"))
RSI_STRONG_OVERBOUGHT = float(os.getenv("RSI_STRONG_OVERBOUGHT", "75"))
RSI_STRONG_OVERSOLD = float(os.getenv("RSI_STRONG_OVERSOLD", "25"))
RSI_EXTREME_OVERBOUGHT = float(os.getenv("RSI_EXTREME_OVERBOUGHT", "80"))
RSI_EXTREME_OVERSOLD = float(os.getenv("RSI_EXTREME_OVERSOLD", "20"))
RSI_TURN_MIN = float(os.getenv("RSI_TURN_MIN", "1.0"))

MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.30"))
STRONG_VOLUME_RATIO = float(os.getenv("STRONG_VOLUME_RATIO", "1.70"))

MIN_FLOW_DELTA = float(os.getenv("MIN_FLOW_DELTA", "0.10"))
MIN_BOOK_IMB = float(os.getenv("MIN_BOOK_IMB", "0.10"))
MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "2.0"))
MIN_OI_CHANGE_PCT = float(os.getenv("MIN_OI_CHANGE_PCT", "-0.10"))

MIN_ATR_PCT = float(os.getenv("MIN_ATR_PCT", "0.0010"))
MAX_ATR_PCT = float(os.getenv("MAX_ATR_PCT", "0.0300"))

MIN_STOP_PCT = float(os.getenv("MIN_STOP_PCT", "0.0040"))
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "0.0100"))
ATR_STOP_MULT = float(os.getenv("ATR_STOP_MULT", "1.10"))

NET_RR_TARGET = float(os.getenv("NET_RR_TARGET", "1.80"))
NET_RR_MIN = float(os.getenv("NET_RR_MIN", "1.50"))
PAPER_TAKER_FEE_RATE = float(os.getenv("PAPER_TAKER_FEE_RATE", "0.0006"))
SLIPPAGE_PER_SIDE = float(os.getenv("SLIPPAGE_PER_SIDE", "0.0002"))

# Prevent reversal entries against a very strong higher-timeframe trend
HTF_EXTREME_VETO = os.getenv("HTF_EXTREME_VETO", "true").lower() in ("1","true","yes","on")

BITGET_API_KEY = os.getenv("BITGET_API_KEY", "").strip()
BITGET_API_SECRET = os.getenv("BITGET_API_SECRET", "").strip()
BITGET_API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "").strip()

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

session = requests.Session()
session.headers.update({
    "User-Agent": "RAZA-SHAH-V11-RSI-VOLUME-AUTO",
    "Accept": "application/json",
})

app = Flask(__name__)

state_lock = threading.Lock()
state = {
    "running": False,
    "strategy_version": STRATEGY_VERSION,
    "auto_trading": AUTO_TRADING,
    "dry_run": DRY_RUN,
    "last_scan": None,
    "last_error": None,
    "latest_signal": None,
    "watchlist": [],
    "cooldowns": {},
    "oi_snapshot": {},
    "daily_realized_loss": 0.0,
    "daily_key": datetime.now(timezone.utc).date().isoformat(),
}

# ============================================================
# BASIC HELPERS
# ============================================================

def log(msg):
    print(f"[V11] {msg}", flush=True)

def telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True},
            timeout=10,
        )
        return bool(r.ok)
    except Exception:
        return False

def telegram_async(text):
    threading.Thread(target=telegram, args=(text,), daemon=True).start()

def utc_now():
    return datetime.now(timezone.utc)

def daily_reset_if_needed():
    today = utc_now().date().isoformat()
    with state_lock:
        if state["daily_key"] != today:
            state["daily_key"] = today
            state["daily_realized_loss"] = 0.0

def bitget_public_get(path, params=None, retries=3):
    last = None
    for attempt in range(retries):
        try:
            r = session.get(BITGET_BASE + path, params=params or {}, timeout=HTTP_TIMEOUT)
            if r.status_code == 429:
                time.sleep(min(2 ** (attempt + 1), 8))
                continue
            r.raise_for_status()
            payload = r.json()
            if str(payload.get("code") or "") not in ("", "00000"):
                raise RuntimeError(f"{payload.get('code')} {payload.get('msg')}")
            return payload.get("data")
        except Exception as e:
            last = e
            if attempt + 1 < retries:
                time.sleep(attempt + 1)
    raise RuntimeError(f"Bitget public GET failed {path}: {last}")

def _sign(ts, method, request_path, query_string="", body=""):
    prehash = f"{ts}{method.upper()}{request_path}"
    if query_string:
        prehash += f"?{query_string}"
    prehash += body
    digest = hmac.new(
        BITGET_API_SECRET.encode(),
        prehash.encode(),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode()

def bitget_private_request(method, path, params=None, body=None):
    if not BITGET_API_KEY or not BITGET_API_SECRET or not BITGET_API_PASSPHRASE:
        raise RuntimeError("Bitget private API credentials missing")

    method = method.upper()
    params = params or {}
    body = body or {}

    query = ""
    if params:
        from urllib.parse import urlencode
        query = urlencode(params)

    body_text = ""
    if method != "GET":
        body_text = json.dumps(body, separators=(",", ":"), ensure_ascii=False)

    ts = str(int(time.time() * 1000))
    headers = {
        "ACCESS-KEY": BITGET_API_KEY,
        "ACCESS-SIGN": _sign(ts, method, path, query, body_text),
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": BITGET_API_PASSPHRASE,
        "Content-Type": "application/json",
        "locale": "en-US",
    }

    url = BITGET_BASE + path + (f"?{query}" if query else "")
    r = session.request(
        method,
        url,
        headers=headers,
        data=body_text if body_text else None,
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    payload = r.json()
    if str(payload.get("code") or "") != "00000":
        raise RuntimeError(f"Bitget private API error {payload.get('code')}: {payload.get('msg')}")
    return payload.get("data")

# ============================================================
# MARKET DATA
# ============================================================

def _granularity(interval):
    return {
        "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
        "1h":"1H","2h":"2H","4h":"4H","6h":"6H","12h":"12H","1d":"1D",
    }.get(interval, interval)

def klines(symbol, interval="15m", limit=100):
    data = bitget_public_get(
        "/api/v2/mix/market/candles",
        {
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
            "granularity": _granularity(interval),
            "limit": str(min(max(int(limit), 2), 1000)),
        },
    )
    if not isinstance(data, list):
        return []
    rows = [x for x in data if isinstance(x, (list, tuple)) and len(x) >= 7]
    return sorted(rows, key=lambda x: int(x[0]))

def candle_dicts(symbol, interval, limit=100, drop_live=True):
    out = []
    for x in klines(symbol, interval, limit):
        try:
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
    if drop_live and len(out) >= 3:
        out = out[:-1]
    return out

def top_symbols():
    tickers = bitget_public_get(
        "/api/v2/mix/market/tickers",
        {"productType": PRODUCT_TYPE},
    )
    contracts = bitget_public_get(
        "/api/v2/mix/market/contracts",
        {"productType": PRODUCT_TYPE},
    )
    contract_map = {}
    for c in contracts or []:
        if isinstance(c, dict):
            contract_map[str(c.get("symbol") or "").upper()] = c

    rows = []
    for x in tickers or []:
        try:
            sym = str(x.get("symbol") or "").upper()
            cfg = contract_map.get(sym, {})
            if not sym.endswith("USDT"):
                continue
            if str(cfg.get("isRwa") or "NO").upper() == "YES":
                continue
            if str(cfg.get("symbolStatus") or "normal").lower() not in ("normal","listed",""):
                continue
            if str(cfg.get("symbolType") or "perpetual").lower() != "perpetual":
                continue
            vol = float(x.get("quoteVolume") or x.get("usdtVolume") or x.get("turnover24h") or 0)
            px = float(x.get("lastPr") or x.get("last") or 0)
            if vol > 0 and px > 0:
                rows.append((sym, vol))
        except Exception:
            pass

    rows.sort(key=lambda z: z[1], reverse=True)
    return [s for s, _ in rows[:TOP_COINS]]

def current_price(symbol):
    data = bitget_public_get(
        "/api/v2/mix/market/ticker",
        {"symbol": symbol, "productType": PRODUCT_TYPE},
    )
    row = data[0] if isinstance(data, list) and data else data
    if not isinstance(row, dict):
        raise RuntimeError("ticker unavailable")
    px = float(row.get("lastPr") or row.get("last") or row.get("close") or 0)
    if px <= 0:
        raise RuntimeError("ticker price unavailable")
    return px

# ============================================================
# INDICATORS
# ============================================================

def ema(values, n):
    if not values:
        return 0.0
    a = 2.0 / (n + 1.0)
    e = float(values[0])
    for v in values[1:]:
        e = a * float(v) + (1.0 - a) * e
    return e

def rsi_series(values, period=14):
    if len(values) < period + 2:
        return []
    gains, losses = [], []
    for i in range(1, len(values)):
        ch = values[i] - values[i-1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    out = [None] * period
    rs = avg_gain / avg_loss if avg_loss else float("inf")
    out.append(100.0 - 100.0 / (1.0 + rs) if avg_loss else (100.0 if avg_gain else 50.0))

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
        if avg_loss == 0:
            val = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            val = 100.0 - (100.0 / (1.0 + rs))
        out.append(val)
    return out

def true_range_series(candles):
    out = []
    for i, c in enumerate(candles):
        if i == 0:
            tr = c["high"] - c["low"]
        else:
            pc = candles[i-1]["close"]
            tr = max(c["high"]-c["low"], abs(c["high"]-pc), abs(c["low"]-pc))
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
    cur = candles[-1]["quote_volume"]
    prior = [x["quote_volume"] for x in candles[-lookback-1:-1]]
    avg = sum(prior) / len(prior) if prior else 0
    return cur / avg if avg > 0 else 1.0

# ============================================================
# 4H CONTEXT — VETO ONLY
# ============================================================

def htf_context(symbol):
    c = candle_dicts(symbol, "4h", 120)
    if len(c) < 60:
        raise RuntimeError("not enough 4h candles")
    closes = [x["close"] for x in c]
    e20 = ema(closes[-80:], 20)
    e50 = ema(closes[-100:], 50)
    price = closes[-1]
    r = [x for x in rsi_series(closes[-90:], 14) if x is not None][-1]
    sep = abs(e20-e50) / price if price else 0
    if e20 > e50 and price > e20 and sep >= 0.001:
        trend = "BULL"
    elif e20 < e50 and price < e20 and sep >= 0.001:
        trend = "BEAR"
    else:
        trend = "RANGE"
    return {"trend": trend, "rsi": r, "ema20": e20, "ema50": e50}

# ============================================================
# RSI + VOLUME REVERSAL SETUP
# ============================================================

def reversal_setup_15m(symbol):
    c = candle_dicts(symbol, "15m", 140)
    if len(c) < 80:
        raise RuntimeError("not enough 15m candles")

    closes = [x["close"] for x in c]
    rs = [x for x in rsi_series(closes, 14)]
    valid_rs = [(i, x) for i, x in enumerate(rs) if x is not None]
    if len(valid_rs) < 4:
        return {"valid": False, "reason": "RSI_UNAVAILABLE"}

    r_now = valid_rs[-1][1]
    r_prev = valid_rs[-2][1]
    r_prev2 = valid_rs[-3][1]
    turn = r_now - r_prev

    last = c[-1]
    prev = c[-2]
    vr = volume_ratio(c, 20)
    atr15 = atr_value(c[-50:], 14)
    atr_pct = atr15 / last["close"] if last["close"] > 0 else 0

    recent = c[-26:-2]
    prior_high = max(x["high"] for x in recent)
    prior_low = min(x["low"] for x in recent)

    rng = max(last["high"] - last["low"], 1e-12)
    close_loc = (last["close"] - last["low"]) / rng
    lower_wick = min(last["open"], last["close"]) - last["low"]
    upper_wick = last["high"] - max(last["open"], last["close"])

    bull_reject = (
        last["close"] > last["open"]
        and close_loc >= 0.58
        and (
            last["low"] < prior_low
            or lower_wick >= 0.28 * rng
            or last["close"] > prev["high"]
        )
    )
    bear_reject = (
        last["close"] < last["open"]
        and close_loc <= 0.42
        and (
            last["high"] > prior_high
            or upper_wick >= 0.28 * rng
            or last["close"] < prev["low"]
        )
    )

    long_extreme = min(r_prev2, r_prev) <= RSI_OVERSOLD
    short_extreme = max(r_prev2, r_prev) >= RSI_OVERBOUGHT

    long_turn = turn >= RSI_TURN_MIN
    short_turn = turn <= -RSI_TURN_MIN

    vol_ok = vr >= MIN_VOLUME_RATIO
    volatility_ok = MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT

    side = None
    valid = False
    strength = "NORMAL"

    if long_extreme and long_turn and bull_reject and vol_ok and volatility_ok:
        side = "BUY"
        valid = True
        if min(r_prev2, r_prev) <= RSI_STRONG_OVERSOLD or vr >= STRONG_VOLUME_RATIO:
            strength = "STRONG"
        if min(r_prev2, r_prev) <= RSI_EXTREME_OVERSOLD and vr >= STRONG_VOLUME_RATIO:
            strength = "EXTREME"

    elif short_extreme and short_turn and bear_reject and vol_ok and volatility_ok:
        side = "SELL"
        valid = True
        if max(r_prev2, r_prev) >= RSI_STRONG_OVERBOUGHT or vr >= STRONG_VOLUME_RATIO:
            strength = "STRONG"
        if max(r_prev2, r_prev) >= RSI_EXTREME_OVERBOUGHT and vr >= STRONG_VOLUME_RATIO:
            strength = "EXTREME"

    return {
        "valid": valid,
        "side": side,
        "strength": strength,
        "rsi_now": r_now,
        "rsi_prev": r_prev,
        "rsi_prev2": r_prev2,
        "rsi_turn": turn,
        "volume_ratio": vr,
        "atr": atr15,
        "atr_pct": atr_pct,
        "price": last["close"],
        "prior_high": prior_high,
        "prior_low": prior_low,
        "bull_reject": bull_reject,
        "bear_reject": bear_reject,
        "sweep_extreme": min(last["low"], prev["low"]) if side == "BUY" else max(last["high"], prev["high"]),
    }

# ============================================================
# 5M EXECUTION TRIGGER
# ============================================================

def trigger_5m(symbol, side):
    c = candle_dicts(symbol, "5m", 90)
    if len(c) < 40:
        raise RuntimeError("not enough 5m candles")

    closes = [x["close"] for x in c]
    e9 = ema(closes[-50:], 9)
    e21 = ema(closes[-60:], 21)
    last = c[-1]
    prior = c[-5:-1]
    prior_high = max(x["high"] for x in prior)
    prior_low = min(x["low"] for x in prior)
    rng = max(last["high"] - last["low"], 1e-12)
    close_loc = (last["close"] - last["low"]) / rng

    if side == "BUY":
        valid = (
            last["close"] > prior_high
            and last["close"] > e9
            and e9 >= e21 * 0.9995
            and last["close"] > last["open"]
            and close_loc >= 0.60
        )
    else:
        valid = (
            last["close"] < prior_low
            and last["close"] < e9
            and e9 <= e21 * 1.0005
            and last["close"] < last["open"]
            and close_loc <= 0.40
        )

    return {
        "valid": bool(valid),
        "price": last["close"],
        "ema9": e9,
        "ema21": e21,
        "prior_high": prior_high,
        "prior_low": prior_low,
        "close_loc": close_loc,
    }

# ============================================================
# LIVE FLOW / BOOK / OI
# ============================================================

def orderbook_metrics(symbol):
    data = bitget_public_get(
        "/api/v2/mix/market/merge-depth",
        {
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
            "limit": "100",
            "precision": "scale0",
        },
    )
    if not isinstance(data, dict):
        return None
    bids = data.get("bids") or []
    asks = data.get("asks") or []
    if not bids or not asks:
        return None
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    mid = (best_bid + best_ask) / 2
    spread = ((best_ask-best_bid)/mid)*10000 if mid else 999
    bid_usd = sum(float(p)*float(q) for p,q in bids[:30])
    ask_usd = sum(float(p)*float(q) for p,q in asks[:30])
    total = bid_usd + ask_usd
    imb = (bid_usd - ask_usd) / total if total else 0
    return {"spread_bps": spread, "book_imb": imb, "mid": mid}

def flow_metrics(symbol):
    trades = bitget_public_get(
        "/api/v2/mix/market/fills",
        {"symbol": symbol, "productType": PRODUCT_TYPE, "limit": "100"},
    )
    cutoff = int(time.time()*1000) - 60000
    buy = sell = 0.0
    for t in trades or []:
        try:
            ts = int(t.get("ts") or t.get("timestamp") or 0)
            if ts < cutoff:
                continue
            px = float(t.get("price") or 0)
            qty = float(t.get("size") or t.get("qty") or 0)
            usd = px * qty
            side = str(t.get("side") or "").lower()
            if side == "buy":
                buy += usd
            elif side == "sell":
                sell += usd
        except Exception:
            pass
    total = buy + sell
    delta = (buy-sell)/total if total else 0
    return {"delta": delta, "buy": buy, "sell": sell}

def oi_change_pct(symbol):
    data = bitget_public_get(
        "/api/v2/mix/market/open-interest",
        {"symbol": symbol, "productType": PRODUCT_TYPE},
    )
    cur = 0.0
    if isinstance(data, dict):
        rows = data.get("openInterestList")
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            cur = float(rows[0].get("size") or rows[0].get("openInterest") or 0)
        else:
            cur = float(data.get("size") or data.get("openInterest") or 0)

    if cur <= 0:
        return 0.0

    with state_lock:
        old = float(state["oi_snapshot"].get(symbol) or 0)
        state["oi_snapshot"][symbol] = cur

    if old <= 0:
        return 0.0
    return ((cur/old)-1.0)*100.0

def live_confirmation(side, flow, book, spread, oi):
    flow_dir = flow if side == "BUY" else -flow
    book_dir = book if side == "BUY" else -book
    reasons = []
    if spread > MAX_SPREAD_BPS:
        reasons.append("SPREAD")
    if flow_dir < MIN_FLOW_DELTA:
        reasons.append("FLOW")
    if book_dir < MIN_BOOK_IMB:
        reasons.append("BOOK")
    if oi < MIN_OI_CHANGE_PCT:
        reasons.append("OI")
    return len(reasons) == 0, reasons

# ============================================================
# COST-AWARE RISK
# ============================================================

def round_trip_cost():
    return 2.0 * (PAPER_TAKER_FEE_RATE + SLIPPAGE_PER_SIDE)

def required_tp_move(stop_pct, net_rr):
    cost = round_trip_cost()
    return net_rr * (stop_pct + cost) + cost

def risk_plan(entry, side, setup):
    atr = float(setup["atr"])
    extreme = float(setup["sweep_extreme"])
    if entry <= 0 or atr <= 0:
        return None

    buffer_dist = max(0.15*atr, entry*0.0004)
    atr_dist = ATR_STOP_MULT*atr

    if side == "BUY":
        structural_sl = min(extreme-buffer_dist, entry-atr_dist)
        raw_pct = (entry-structural_sl)/entry
    else:
        structural_sl = max(extreme+buffer_dist, entry+atr_dist)
        raw_pct = (structural_sl-entry)/entry

    if raw_pct > MAX_STOP_PCT:
        return None

    stop_pct = max(raw_pct, MIN_STOP_PCT)
    if stop_pct > MAX_STOP_PCT:
        return None

    sl = entry*(1-stop_pct) if side == "BUY" else entry*(1+stop_pct)
    tp_move = required_tp_move(stop_pct, NET_RR_TARGET)
    tp = entry*(1+tp_move) if side == "BUY" else entry*(1-tp_move)

    # Target loss in USDT using at most $100 strategy capital
    capital_risk_usdt = STRATEGY_CAPITAL_USDT * RISK_PER_TRADE
    per_1x_loss_pct = stop_pct + round_trip_cost()
    notional_by_risk = capital_risk_usdt / per_1x_loss_pct if per_1x_loss_pct > 0 else 0
    max_notional = STRATEGY_CAPITAL_USDT * MAX_LEVERAGE
    notional = min(notional_by_risk, max_notional)
    leverage = notional / STRATEGY_CAPITAL_USDT if STRATEGY_CAPITAL_USDT > 0 else 0

    if notional <= 0 or leverage <= 0:
        return None

    return {
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "stop_pct": stop_pct,
        "notional_usdt": notional,
        "leverage": max(1.0, min(MAX_LEVERAGE, leverage)),
        "planned_loss_usdt": min(capital_risk_usdt, notional*per_1x_loss_pct),
        "net_rr": NET_RR_TARGET,
    }

# ============================================================
# BITGET PRIVATE ACCOUNT / EXECUTION
# ============================================================

def account_info():
    return bitget_private_request(
        "GET",
        "/api/v2/mix/account/account",
        params={"symbol": "BTCUSDT", "productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN},
    )

def available_balance():
    data = account_info()
    if not isinstance(data, dict):
        return 0.0
    for key in ("available", "availableBalance", "maxTransferOut", "accountEquity", "usdtEquity"):
        try:
            v = float(data.get(key) or 0)
            if v > 0:
                return v
        except Exception:
            pass
    return 0.0

def contract_config(symbol):
    rows = bitget_public_get(
        "/api/v2/mix/market/contracts",
        {"productType": PRODUCT_TYPE, "symbol": symbol},
    )
    if isinstance(rows, list) and rows:
        return rows[0]
    if isinstance(rows, dict):
        return rows
    raise RuntimeError(f"contract config unavailable for {symbol}")

def floor_to_step(value, step):
    d = Decimal(str(value))
    s = Decimal(str(step))
    if s <= 0:
        return d
    return (d / s).to_integral_value(rounding=ROUND_DOWN) * s

def format_qty(symbol, base_qty):
    cfg = contract_config(symbol)
    min_qty = Decimal(str(cfg.get("minTradeNum") or "0"))
    step = Decimal(str(cfg.get("sizeMultiplier") or "0.00000001"))
    qty = floor_to_step(base_qty, step)
    if qty < min_qty:
        return None
    return format(qty, "f")

def set_leverage(symbol, leverage):
    lev = str(max(1, int(leverage)))
    return bitget_private_request(
        "POST",
        "/api/v2/mix/account/set-leverage",
        body={
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
            "marginCoin": MARGIN_COIN,
            "leverage": lev,
            "marginMode": MARGIN_MODE,
        },
    )

def open_positions():
    data = bitget_private_request(
        "GET",
        "/api/v2/mix/position/all-position",
        params={"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN},
    )
    rows = data if isinstance(data, list) else []
    active = []
    for x in rows:
        try:
            total = abs(float(x.get("total") or x.get("available") or x.get("holdSize") or 0))
            if total > 0:
                active.append(x)
        except Exception:
            pass
    return active

def place_market_order(symbol, side, qty, client_oid):
    return bitget_private_request(
        "POST",
        "/api/v2/mix/order/place-order",
        body={
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
            "marginMode": MARGIN_MODE,
            "marginCoin": MARGIN_COIN,
            "size": qty,
            "side": "buy" if side == "BUY" else "sell",
            "orderType": "market",
            "clientOid": client_oid,
        },
    )

def place_position_tpsl(symbol, side, tp, sl):
    # Exchange-side TP/SL for current position.
    # holdSide is required by position TPSL interface.
    hold_side = "long" if side == "BUY" else "short"
    return bitget_private_request(
        "POST",
        "/api/v2/mix/order/place-pos-tpsl",
        body={
            "marginCoin": MARGIN_COIN,
            "productType": PRODUCT_TYPE,
            "symbol": symbol,
            "holdSide": hold_side,
            "stopSurplusTriggerPrice": str(tp),
            "stopSurplusTriggerType": "mark_price",
            "stopSurplusExecutePrice": "0",
            "stopLossTriggerPrice": str(sl),
            "stopLossTriggerType": "mark_price",
            "stopLossExecutePrice": "0",
        },
    )

def execute_trade(symbol, side, plan):
    daily_reset_if_needed()

    if not AUTO_TRADING:
        return {"executed": False, "reason": "AUTO_TRADING_DISABLED"}

    if DRY_RUN:
        return {
            "executed": False,
            "reason": "DRY_RUN",
            "symbol": symbol,
            "side": side,
            "plan": plan,
        }

    with state_lock:
        if state["daily_realized_loss"] >= MAX_DAILY_LOSS_USDT:
            return {"executed": False, "reason": "DAILY_LOSS_LIMIT"}

    positions = open_positions()
    if len(positions) >= MAX_OPEN_POSITIONS:
        return {"executed": False, "reason": "MAX_OPEN_POSITIONS"}

    bal = available_balance()
    required_margin = plan["notional_usdt"] / max(plan["leverage"], 1)
    if bal <= 0 or required_margin > bal:
        return {
            "executed": False,
            "reason": "INSUFFICIENT_AVAILABLE_BALANCE",
            "available": bal,
            "required_margin": required_margin,
        }

    px = current_price(symbol)
    base_qty = plan["notional_usdt"] / px
    qty = format_qty(symbol, base_qty)
    if not qty:
        return {"executed": False, "reason": "BELOW_MIN_ORDER_SIZE"}

    set_leverage(symbol, plan["leverage"])

    client_oid = f"v11-{symbol.lower()}-{int(time.time())}-{secrets.token_hex(3)}"
    order = place_market_order(symbol, side, qty, client_oid)

    # Allow exchange a moment to register the position before position-level TPSL.
    time.sleep(0.7)
    tpsl = place_position_tpsl(symbol, side, plan["tp"], plan["sl"])

    return {
        "executed": True,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "client_oid": client_oid,
        "order": order,
        "tpsl": tpsl,
        "plan": plan,
    }

# ============================================================
# COMPLETE SIGNAL EVALUATION
# ============================================================

def cooldown_ok(symbol):
    with state_lock:
        last = float(state["cooldowns"].get(symbol) or 0)
    return (time.time() - last) >= SIGNAL_COOLDOWN_SECONDS

def mark_cooldown(symbol):
    with state_lock:
        state["cooldowns"][symbol] = time.time()

def evaluate_symbol(symbol):
    if not cooldown_ok(symbol):
        return None

    setup = reversal_setup_15m(symbol)
    if not setup.get("valid"):
        return None

    side = setup["side"]
    htf = htf_context(symbol)

    # Hard veto only when higher timeframe is already stretched strongly
    # in the direction opposite the reversal.
    if HTF_EXTREME_VETO:
        if side == "SELL" and htf["trend"] == "BULL" and htf["rsi"] >= 72 and setup["strength"] == "NORMAL":
            return None
        if side == "BUY" and htf["trend"] == "BEAR" and htf["rsi"] <= 28 and setup["strength"] == "NORMAL":
            return None

    trig = trigger_5m(symbol, side)
    if not trig["valid"]:
        return None

    book = orderbook_metrics(symbol)
    if not book:
        return None
    flow = flow_metrics(symbol)
    oi = oi_change_pct(symbol)

    live_ok, reasons = live_confirmation(
        side,
        flow["delta"],
        book["book_imb"],
        book["spread_bps"],
        oi,
    )
    if not live_ok:
        return None

    entry = current_price(symbol)
    plan = risk_plan(entry, side, setup)
    if not plan:
        return None

    signal = {
        "time_utc": utc_now().isoformat(),
        "strategy_version": STRATEGY_VERSION,
        "symbol": symbol,
        "side": side,
        "strength": setup["strength"],
        "entry": entry,
        "tp": plan["tp"],
        "sl": plan["sl"],
        "rsi_now": setup["rsi_now"],
        "rsi_prev": setup["rsi_prev"],
        "rsi_turn": setup["rsi_turn"],
        "volume_ratio": setup["volume_ratio"],
        "atr_pct": setup["atr_pct"],
        "flow_delta": flow["delta"],
        "book_imb": book["book_imb"],
        "spread_bps": book["spread_bps"],
        "oi_change_pct": oi,
        "htf_trend": htf["trend"],
        "htf_rsi": htf["rsi"],
        "planned_loss_usdt": plan["planned_loss_usdt"],
        "notional_usdt": plan["notional_usdt"],
        "leverage": plan["leverage"],
    }

    execution = execute_trade(symbol, side, plan)
    signal["execution"] = execution

    mark_cooldown(symbol)

    with state_lock:
        state["latest_signal"] = signal

    telegram_async(
        f"V11 {side} {symbol}\n"
        f"RSI {setup['rsi_prev']:.1f} -> {setup['rsi_now']:.1f}\n"
        f"Volume {setup['volume_ratio']:.2f}x\n"
        f"Entry {entry}\nTP {plan['tp']}\nSL {plan['sl']}\n"
        f"Risk ~${plan['planned_loss_usdt']:.2f}\n"
        f"Leverage {plan['leverage']:.2f}x\n"
        f"Auto: {execution.get('executed')} ({execution.get('reason','OK')})"
    )

    return signal

# ============================================================
# SCANNER LOOP
# ============================================================

def run_scan():
    symbols = top_symbols()
    with state_lock:
        state["watchlist"] = symbols
        state["last_scan"] = utc_now().isoformat()

    for sym in symbols:
        try:
            result = evaluate_symbol(sym)
            if result:
                log(f"SIGNAL {sym} {result['side']} | RSI={result['rsi_now']:.1f} | VOL={result['volume_ratio']:.2f}")
        except Exception as e:
            log(f"{sym}: {type(e).__name__}: {e}")

def scanner_loop():
    with state_lock:
        state["running"] = True
    while True:
        try:
            daily_reset_if_needed()
            run_scan()
            with state_lock:
                state["last_error"] = None
        except Exception as e:
            with state_lock:
                state["last_error"] = f"{type(e).__name__}: {e}"
            log(state["last_error"])
        time.sleep(SCAN_INTERVAL)

# ============================================================
# SIMPLE WEB API — NO TELEGRAM OTP / NO LOGIN SECURITY
# ============================================================

@app.get("/")
def home():
    with state_lock:
        snapshot = dict(state)
    return jsonify(snapshot)

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "strategy": STRATEGY_VERSION,
        "auto_trading": AUTO_TRADING,
        "dry_run": DRY_RUN,
        "time_utc": utc_now().isoformat(),
    })

@app.get("/state")
def get_state():
    with state_lock:
        return jsonify(dict(state))

def start_background():
    t = threading.Thread(target=scanner_loop, daemon=True)
    t.start()

if __name__ == "__main__":
    start_background()
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
