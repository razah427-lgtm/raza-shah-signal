
import os, time, csv, threading
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, jsonify, render_template, send_from_directory

BASE_URL = "https://fapi.binance.com"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "900"))   # 15 min
TOP_COINS = int(os.getenv("TOP_COINS", "100"))
DEEP_CHECK = int(os.getenv("DEEP_CHECK", "25"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "85"))

TP_PCT = float(os.getenv("TP_PCT", "0.006"))
SL_PCT = float(os.getenv("SL_PCT", "0.004"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)
LIVE_FILE = DATA_DIR / "live_signals.csv"

CSV_COLUMNS = [
    "time_utc","symbol","signal","score","price",
    "flow_delta","buy_usd_60s","sell_usd_60s",
    "spread_bps","book_imb","trend_5m",
    "oi_change_pct","tp","sl"
]

session = requests.Session()
session.headers.update({"User-Agent":"RAZA-iPhone-Signal/1.0"})

app = Flask(__name__)
state_lock = threading.Lock()
state = {
    "running": False,
    "status": "Starting...",
    "last_scan": None,
    "next_scan": None,
    "alerts_last_scan": 0,
    "latest_signal": None,
    "last_error": None
}

def get_json(path, params=None, timeout=12):
    r = session.get(BASE_URL + path, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()

def telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        r = session.post(url, data={"chat_id":TELEGRAM_CHAT_ID,"text":text}, timeout=10)
        r.raise_for_status()
        return True
    except Exception:
        return False

def top_symbols():
    info = get_json("/fapi/v1/exchangeInfo")
    allowed = {
        s["symbol"] for s in info["symbols"]
        if s.get("quoteAsset") == "USDT"
        and s.get("contractType") == "PERPETUAL"
        and s.get("status") == "TRADING"
    }
    tickers = get_json("/fapi/v1/ticker/24hr")
    rows = []
    for x in tickers:
        if x.get("symbol") in allowed:
            try:
                rows.append((x["symbol"], float(x.get("quoteVolume",0))))
            except Exception:
                pass
    rows.sort(key=lambda x:x[1], reverse=True)
    return [s for s,_ in rows[:TOP_COINS]]

def klines(symbol, interval="5m", limit=60):
    return get_json("/fapi/v1/klines", {"symbol":symbol,"interval":interval,"limit":limit})

def ema(values, n):
    a = 2/(n+1)
    e = values[0]
    for v in values[1:]:
        e = a*v + (1-a)*e
    return e

def light_metrics(symbol):
    k = klines(symbol, "5m", 60)
    closes = [float(x[4]) for x in k]
    vols = [float(x[5]) for x in k]
    if len(closes) < 25:
        return None
    e9 = ema(closes[-30:], 9)
    e21 = ema(closes[-40:], 21)
    trend = "BULL" if e9 > e21 else "BEAR"
    momentum = closes[-1]/closes[-4]-1 if closes[-4] else 0
    avg_vol = sum(vols[-21:-1])/20 if len(vols) >= 21 else 0
    vol_ratio = vols[-1]/avg_vol if avg_vol else 0
    return {"price":closes[-1],"trend":trend,"momentum":momentum,"vol_ratio":vol_ratio}

def depth_metrics(symbol, limit=100):
    d = get_json("/fapi/v1/depth", {"symbol":symbol,"limit":limit})
    bids, asks = d.get("bids",[]), d.get("asks",[])
    if not bids or not asks:
        return None
    best_bid = float(bids[0][0]); best_ask = float(asks[0][0])
    mid = (best_bid+best_ask)/2
    spread_bps = ((best_ask-best_bid)/mid)*10000 if mid else 999
    bid_usd = sum(float(p)*float(q) for p,q in bids)
    ask_usd = sum(float(p)*float(q) for p,q in asks)
    total = bid_usd + ask_usd
    book_imb = (bid_usd-ask_usd)/total if total else 0
    return spread_bps, book_imb

def flow_metrics(symbol):
    trades = get_json("/fapi/v1/aggTrades", {"symbol":symbol,"limit":1000})
    cutoff = int(time.time()*1000)-60000
    buy = sell = 0.0
    for t in trades:
        if int(t["T"]) < cutoff:
            continue
        usd = float(t["p"])*float(t["q"])
        if bool(t["m"]):
            sell += usd
        else:
            buy += usd
    total = buy + sell
    delta = (buy-sell)/total if total else 0
    return delta,buy,sell

def oi_change_pct(symbol):
    hist = get_json("/futures/data/openInterestHist", {
        "symbol":symbol, "period":"5m", "limit":2
    })
    if len(hist) < 2:
        return 0.0
    a = float(hist[-2].get("sumOpenInterestValue",0))
    b = float(hist[-1].get("sumOpenInterestValue",0))
    return ((b/a)-1)*100 if a else 0.0

def build_score(light, delta, spread, book, oi):
    if light["trend"] == "BULL" and delta > 0 and book > 0:
        side = "BUY"
    elif light["trend"] == "BEAR" and delta < 0 and book < 0:
        side = "SELL"
    else:
        return None,0

    score = 15

    if (side=="BUY" and light["momentum"]>0) or (side=="SELL" and light["momentum"]<0):
        score += 5

    vr = light["vol_ratio"]
    score += 15 if vr>=1.5 else 10 if vr>=1.2 else 5 if vr>=1.0 else 0

    ad = abs(delta)
    score += 25 if ad>=0.50 else 20 if ad>=0.35 else 15 if ad>=0.20 else 8 if ad>=0.10 else 0

    ab = abs(book)
    score += 25 if ab>=0.55 else 20 if ab>=0.40 else 15 if ab>=0.25 else 8 if ab>=0.15 else 0

    score += 10 if spread<=0.5 else 7 if spread<=1.0 else 4 if spread<=2.0 else 0
    if oi > 0:
        score += 5

    return side, min(score,100)

def hard_confirm(side, delta, book, spread):
    if spread > 2.0:
        return False
    return (
        (side=="BUY" and delta>=0.20 and book>=0.20) or
        (side=="SELL" and delta<=-0.20 and book<=-0.20)
    )

def save_signal(row):
    new = not LIVE_FILE.exists()
    with LIVE_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if new:
            w.writeheader()
        w.writerow(row)

def recent_signals(limit=20):
    if not LIVE_FILE.exists():
        return []
    with LIVE_FILE.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return list(reversed(rows[-limit:]))

def scan_once():
    with state_lock:
        state["status"] = "Scanning Top 100..."
        state["last_error"] = None

    candidates = []
    for symbol in top_symbols():
        try:
            lm = light_metrics(symbol)
            if not lm:
                continue
            rank = lm["vol_ratio"]*3 + abs(lm["momentum"])*10000
            candidates.append((rank,symbol,lm))
        except Exception:
            pass

    candidates.sort(reverse=True, key=lambda x:x[0])
    alerts = 0

    for _,symbol,lm in candidates[:DEEP_CHECK]:
        try:
            dm = depth_metrics(symbol)
            if not dm:
                continue
            spread, book = dm
            delta,buy,sell = flow_metrics(symbol)
            oi = oi_change_pct(symbol)
            side,score = build_score(lm,delta,spread,book,oi)

            if not side or score < MIN_SCORE or not hard_confirm(side,delta,book,spread):
                continue

            price = lm["price"]
            if side == "BUY":
                tp = price*(1+TP_PCT); sl = price*(1-SL_PCT)
            else:
                tp = price*(1-TP_PCT); sl = price*(1+SL_PCT)

            row = {
                "time_utc":datetime.now(timezone.utc).isoformat(),
                "symbol":symbol,"signal":side,"score":score,"price":price,
                "flow_delta":delta,"buy_usd_60s":buy,"sell_usd_60s":sell,
                "spread_bps":spread,"book_imb":book,"trend_5m":lm["trend"],
                "oi_change_pct":oi,"tp":tp,"sl":sl
            }
            save_signal(row)
            with state_lock:
                state["latest_signal"] = row

            telegram(
                f"🚨 {side} NOW — {symbol}\n"
                f"Score: {score}/100 ✅\n"
                f"Entry: {price:.8g}\nTP: {tp:.8g}\nSL: {sl:.8g}\n"
                f"Flow: {delta:+.2f}\nBook: {book:+.2f}\nSpread: {spread:.2f} bps\nOI: {oi:+.2f}%"
            )
            alerts += 1
        except Exception:
            pass

    now = datetime.now(timezone.utc)
    with state_lock:
        state["last_scan"] = now.isoformat()
        state["alerts_last_scan"] = alerts
        state["status"] = "Waiting for 85+ setup"

def scanner_loop():
    with state_lock:
        state["running"] = True

    telegram("✅ RAZA SHAH iPHONE SIGNAL APP STARTED\n85+ scanner active.")

    while True:
        start = time.time()
        try:
            scan_once()
        except Exception as e:
            with state_lock:
                state["last_error"] = str(e)
                state["status"] = "Scan error — retrying"

        wait = max(10, SCAN_INTERVAL-(time.time()-start))
        with state_lock:
            state["next_scan"] = datetime.fromtimestamp(time.time()+wait, timezone.utc).isoformat()
        time.sleep(wait)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/status")
def api_status():
    with state_lock:
        x = dict(state)
    x["signals"] = recent_signals(20)
    x["telegram"] = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    x["min_score"] = MIN_SCORE
    return jsonify(x)
@app.route("/api/signal")
def api_signal():
    with state_lock:
        return jsonify(dict(state))
@app.route("/manifest.webmanifest")
def manifest():
    return send_from_directory("static","manifest.webmanifest", mimetype="application/manifest+json")

@app.route("/sw.js")
def sw():
    return send_from_directory("static","sw.js", mimetype="application/javascript")

threading.Thread(target=scanner_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))