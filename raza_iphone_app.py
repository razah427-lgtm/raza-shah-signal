import os
import time
import csv
import threading
import secrets
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
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
# BYBIT USDT PERPETUAL FUTURES
# SIGNAL ONLY — NO AUTO ORDERS
# ============================================================

BASE_URL = "https://api.bybit.com"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "900"))   # 15 minutes
TOP_COINS = int(os.getenv("TOP_COINS", "100"))
DEEP_CHECK = int(os.getenv("DEEP_CHECK", "25"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "85"))

TP_PCT = float(os.getenv("TP_PCT", "0.006"))   # 0.60%
SL_PCT = float(os.getenv("SL_PCT", "0.004"))   # 0.40%

# Lower concurrency to avoid public API rate-limit pressure
LIGHT_SCAN_WORKERS = int(os.getenv("LIGHT_SCAN_WORKERS", "8"))
DEEP_SCAN_WORKERS = int(os.getenv("DEEP_SCAN_WORKERS", "4"))

SCAN_HTTP_TIMEOUT = float(os.getenv("SCAN_HTTP_TIMEOUT", "10"))

SIGNAL_COOLDOWN_SECONDS = int(
    os.getenv("SIGNAL_COOLDOWN_SECONDS", "14400")
)  # 4 hours

# ============================================================
# TELEGRAM / WEB
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

APP_URL = os.getenv(
    "APP_URL",
    "https://raza-shah-signal-2.onrender.com"
).rstrip("/")

APP_SECRET_KEY = os.getenv(
    "APP_SECRET_KEY",
    "raza-signal-change-this-secret"
)

OTP_TTL_SECONDS = int(
    os.getenv("OTP_TTL_SECONDS", "300")
)

OTP_MAX_ATTEMPTS = int(
    os.getenv("OTP_MAX_ATTEMPTS", "5")
)

ACCESS_TTL_SECONDS = int(
    os.getenv("ACCESS_TTL_SECONDS", "86400")
)

TELEGRAM_STATUS_INTERVAL = int(
    os.getenv("TELEGRAM_STATUS_INTERVAL", "3600")
)

# ============================================================
# FILES
# ============================================================

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)

LIVE_FILE = DATA_DIR / "live_signals.csv"
TRADES_FILE = DATA_DIR / "trade_results.csv"

CSV_COLUMNS = [
    "time_utc",
    "symbol",
    "signal",
    "score",
    "price",
    "flow_delta",
    "buy_usd_60s",
    "sell_usd_60s",
    "spread_bps",
    "book_imb",
    "trend_5m",
    "oi_change_pct",
    "tp",
    "sl",
]

TRADE_COLUMNS = [
    "trade_id",
    "time_utc",
    "closed_time_utc",
    "symbol",
    "signal",
    "score",
    "entry",
    "tp",
    "sl",
    "status",
    "exit_price",
]

# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "RAZA-SHAH-SIGNAL-BYBIT/2.0",
    "Accept": "application/json",
})

# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)
app.secret_key = APP_SECRET_KEY

app.permanent_session_lifetime = timedelta(
    seconds=ACCESS_TTL_SECONDS
)

# ============================================================
# STATE
# ============================================================

state_lock = threading.Lock()

state = {
    "running": False,
    "exchange": "BYBIT",
    "status": "Starting...",
    "last_scan": None,
    "next_scan": None,
    "alerts_last_scan": 0,
    "latest_signal": None,
    "best_candidate": None,
    "scan_progress": "0/0",
    "last_scan_seconds": None,
    "last_error": None,
}

otp_lock = threading.Lock()
otp_by_ip = {}
authorized_ips = {}

# ============================================================
# LOGGING
# ============================================================

def scan_log(message):
    print(f"[SCANNER] {message}", flush=True)


def telegram_log(message):
    print(f"[TELEGRAM] {message}", flush=True)

# ============================================================
# BYBIT API
# ============================================================

def bybit_get(path, params=None, timeout=None):

    if timeout is None:
        timeout = SCAN_HTTP_TIMEOUT

    r = session.get(
        BASE_URL + path,
        params=params or {},
        timeout=timeout,
    )

    r.raise_for_status()

    data = r.json()

    ret_code = data.get("retCode", -999)

    if ret_code != 0:
        raise RuntimeError(
            f"Bybit retCode={ret_code} "
            f"retMsg={data.get('retMsg')}"
        )

    return data.get("result", {})

# ============================================================
# TELEGRAM
# ============================================================

def telegram(text):

    if not TELEGRAM_BOT_TOKEN:
        telegram_log("BOT TOKEN missing")
        return False

    if not TELEGRAM_CHAT_ID:
        telegram_log("CHAT ID missing")
        return False

    try:

        url = (
            f"https://api.telegram.org/"
            f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        )

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
                f"FAILED HTTP {r.status_code} | "
                f"{r.text[:300]}"
            )

            return False

        telegram_log("Message sent OK")
        return True

    except Exception as e:

        telegram_log(
            f"ERROR {type(e).__name__}: {e}"
        )

        return False


def telegram_async(text):

    threading.Thread(
        target=telegram,
        args=(text,),
        daemon=True,
    ).start()

# ============================================================
# TRADE FILE
# ============================================================

def trade_rows():

    if not TRADES_FILE.exists():
        return []

    try:

        with TRADES_FILE.open(
            "r",
            newline="",
            encoding="utf-8"
        ) as f:

            return list(
                csv.DictReader(f)
            )

    except Exception:
        return []


def write_trade_rows(rows):

    with TRADES_FILE.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        w = csv.DictWriter(
            f,
            fieldnames=TRADE_COLUMNS
        )

        w.writeheader()
        w.writerows(rows)


def add_open_trade(row):

    rows = trade_rows()

    trade_id = (
        f'{row["symbol"]}-'
        f'{int(time.time()*1000)}'
    )

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


def performance():

    rows = trade_rows()

    closed = [
        r for r in rows
        if r.get("status") in ("WIN", "LOSS")
    ]

    wins = sum(
        1 for r in closed
        if r.get("status") == "WIN"
    )

    losses = sum(
        1 for r in closed
        if r.get("status") == "LOSS"
    )

    total = wins + losses

    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": (
            round((wins / total) * 100, 2)
            if total else 0.0
        ),
        "open_trades": sum(
            1 for r in rows
            if r.get("status") == "OPEN"
        ),
    }

# ============================================================
# CURRENT PRICE
# ============================================================

def current_price(symbol):

    result = bybit_get(
        "/v5/market/tickers",
        {
            "category": "linear",
            "symbol": symbol,
        }
    )

    items = result.get("list", [])

    if not items:
        raise RuntimeError(
            f"No ticker for {symbol}"
        )

    return float(
        items[0]["lastPrice"]
    )

# ============================================================
# CHECK OPEN TRADES
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

                r["closed_time_utc"] = (
                    datetime.now(
                        timezone.utc
                    ).isoformat()
                )

                changed = True

                closed_messages.append(
                    (
                        symbol,
                        side,
                        result,
                        entry,
                        tp,
                        sl,
                        px,
                    )
                )

        except Exception as e:

            scan_log(
                f"OPEN TRADE CHECK ERROR "
                f"{r.get('symbol')}: {e}"
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

            icon = (
                "✅"
                if result == "WIN"
                else "❌"
            )

            label = (
                "TP HIT / WIN"
                if result == "WIN"
                else "SL HIT / LOSS"
            )

            telegram_async(
                f"{icon} {label} — {symbol}\n"
                f"Exchange: BYBIT\n"
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
                f"🔗 {APP_URL}"
            )

# ============================================================
# SECURITY / OTP
# ============================================================

def client_ip():

    xff = request.headers.get(
        "X-Forwarded-For",
        ""
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

            if (
                otp_by_ip[ip]
                .get("expires", 0)
                <= now
            ):

                otp_by_ip.pop(
                    ip,
                    None
                )

        for ip in list(
            authorized_ips
        ):

            if (
                authorized_ips[ip]
                <= now
            ):

                authorized_ips.pop(
                    ip,
                    None
                )


def ensure_otp_for_ip(ip):

    cleanup_access()

    now = time.time()

    with otp_lock:

        existing = otp_by_ip.get(ip)

        if (
            existing
            and
            existing.get(
                "expires",
                0
            ) > now
        ):

            return (
                existing["code"],
                False
            )

        code = (
            f"{secrets.randbelow(1000000):06d}"
        )

        otp_by_ip[ip] = {
            "code": code,
            "expires": (
                now
                + OTP_TTL_SECONDS
            ),
            "attempts": 0,
        }

    telegram_async(
        f"🔐 RAZA SHAH SIGNAL\n"
        f"ACCESS REQUEST\n\n"
        f"Permission Code: {code}\n"
        f"IP: {ip}\n"
        f"Valid: "
        f"{OTP_TTL_SECONDS//60} minutes\n"
        f"Access after approval: 24 hours\n\n"
        f"🔗 {APP_URL}"
    )

    return code, True


def is_authorized():

    cleanup_access()

    ip = client_ip()
    now = time.time()

    with otp_lock:

        if (
            authorized_ips.get(
                ip,
                0
            ) > now
        ):

            return True

    auth_until = float(
        flask_session.get(
            "authorized_until",
            0
        )
        or 0
    )

    auth_ip = flask_session.get(
        "authorized_ip"
    )

    return (
        auth_ip == ip
        and
        auth_until > now
    )


def login_html(message=""):

    return f"""
<!doctype html>
<html>
<head>

<meta name="viewport"
content="width=device-width,initial-scale=1">

<title>
RAZA SHAH SIGNAL — Secure Access
</title>

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

<h2>
🔐 RAZA SHAH SIGNAL
</h2>

<p>
<b>Waiting for Admin Permission</b>
</p>

<p>
Enter 6-Digit Permission Code
</p>

<div class="msg">
{message}
</div>

<form method="post"
action="/verify">

<input
name="code"
inputmode="numeric"
maxlength="6"
placeholder="000000"
required
>

<br>

<button type="submit">
REQUEST ACCESS
</button>

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
# TOP BYBIT USDT PERPETUAL COINS
# ============================================================

def top_symbols():

    scan_log(
        "TOP SYMBOLS: "
        "loading Bybit linear tickers..."
    )

    result = bybit_get(
        "/v5/market/tickers",
        {
            "category": "linear"
        },
        timeout=10,
    )

    tickers = result.get(
        "list",
        []
    )

    rows = []

    for x in tickers:

        try:

            symbol = str(
                x.get(
                    "symbol",
                    ""
                )
            )

            # Only USDT linear contracts
            if not symbol.endswith(
                "USDT"
            ):
                continue

            turnover = float(
                x.get(
                    "turnover24h",
                    0
                )
                or 0
            )

            price = float(
                x.get(
                    "lastPrice",
                    0
                )
                or 0
            )

            if (
                turnover <= 0
                or
                price <= 0
            ):
                continue

            rows.append(
                (
                    symbol,
                    turnover
                )
            )

        except Exception:
            pass

    rows.sort(
        key=lambda z: z[1],
        reverse=True
    )

    symbols = [
        symbol
        for symbol, _
        in rows[:TOP_COINS]
    ]

    if not symbols:

        raise RuntimeError(
            "No Bybit USDT "
            "linear symbols found"
        )

    scan_log(
        f"TOP SYMBOLS LOADED: "
        f"{len(symbols)}"
    )

    return symbols

# ============================================================
# KLINES
# ============================================================

def klines(
    symbol,
    interval="5",
    limit=60
):

    result = bybit_get(
        "/v5/market/kline",
        {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }
    )

    rows = result.get(
        "list",
        []
    )

    # Bybit returns newest first.
    # Reverse to oldest -> newest.
    rows.reverse()

    return rows

# ============================================================
# EMA
# ============================================================

def ema(values, n):

    a = 2 / (n + 1)

    e = values[0]

    for v in values[1:]:

        e = (
            a * v
            +
            (1 - a) * e
        )

    return e

# ============================================================
# LIGHT METRICS
# ============================================================

def light_metrics(symbol):

    k = klines(
        symbol,
        "5",
        60
    )

    closes = [
        float(x[4])
        for x in k
    ]

    vols = [
        float(x[5])
        for x in k
    ]

    if len(closes) < 25:
        return None

    e9 = ema(
        closes[-30:],
        9
    )

    e21 = ema(
        closes[-40:],
        21
    )

    trend = (
        "BULL"
        if e9 > e21
        else "BEAR"
    )

    momentum = (
        closes[-1]
        /
        closes[-4]
        - 1
        if closes[-4]
        else 0
    )

    avg_vol = (
        sum(
            vols[-21:-1]
        ) / 20
        if len(vols) >= 21
        else 0
    )

    vol_ratio = (
        vols[-1]
        /
        avg_vol
        if avg_vol
        else 0
    )

    return {
        "price": closes[-1],
        "trend": trend,
        "momentum": momentum,
        "vol_ratio": vol_ratio,
    }

# ============================================================
# ORDER BOOK
# ============================================================

def depth_metrics(
    symbol,
    limit=100
):

    result = bybit_get(
        "/v5/market/orderbook",
        {
            "category": "linear",
            "symbol": symbol,
            "limit": limit,
        }
    )

    bids = result.get(
        "b",
        []
    )

    asks = result.get(
        "a",
        []
    )

    if not bids or not asks:
        return None

    best_bid = float(
        bids[0][0]
    )

    best_ask = float(
        asks[0][0]
    )

    mid = (
        best_bid
        +
        best_ask
    ) / 2

    spread_bps = (
        (
            best_ask
            -
            best_bid
        )
        /
        mid
        *
        10000
        if mid
        else 999
    )

    bid_usd = sum(
        float(price)
        *
        float(qty)
        for price, qty
        in bids
    )

    ask_usd = sum(
        float(price)
        *
        float(qty)
        for price, qty
        in asks
    )

    total = (
        bid_usd
        +
        ask_usd
    )

    book_imb = (
        (
            bid_usd
            -
            ask_usd
        )
        /
        total
        if total
        else 0
    )

    return (
        spread_bps,
        book_imb
    )

# ============================================================
# LIVE TRADE FLOW
# ============================================================

def flow_metrics(symbol):

    result = bybit_get(
        "/v5/market/recent-trade",
        {
            "category": "linear",
            "symbol": symbol,
            "limit": 1000,
        }
    )

    trades = result.get(
        "list",
        []
    )

    cutoff = (
        int(time.time() * 1000)
        -
        60000
    )

    buy = 0.0
    sell = 0.0

    for t in trades:

        try:

            trade_time = int(
                t.get(
                    "time",
                    0
                )
            )

            if trade_time < cutoff:
                continue

            price = float(
                t.get(
                    "price",
                    0
                )
            )

            size = float(
                t.get(
                    "size",
                    0
                )
            )

            usd = (
                price
                *
                size
            )

            taker_side = str(
                t.get(
                    "side",
                    ""
                )
            ).upper()

            if taker_side == "BUY":

                buy += usd

            elif taker_side == "SELL":

                sell += usd

        except Exception:
            pass

    total = (
        buy
        +
        sell
    )

    delta = (
        (
            buy
            -
            sell
        )
        /
        total
        if total
        else 0
    )

    return (
        delta,
        buy,
        sell
    )

# ============================================================
# OPEN INTEREST CHANGE
# ============================================================

def oi_change_pct(symbol):

    result = bybit_get(
        "/v5/market/open-interest",
        {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": "5min",
            "limit": 2,
        }
    )

    rows = result.get(
        "list",
        []
    )

    if len(rows) < 2:
        return 0.0

    # Make order deterministic
    rows = sorted(
        rows,
        key=lambda x: int(
            x.get(
                "timestamp",
                0
            )
        )
    )

    old = float(
        rows[-2].get(
            "openInterest",
            0
        )
        or 0
    )

    new = float(
        rows[-1].get(
            "openInterest",
            0
        )
        or 0
    )

    if not old:
        return 0.0

    return (
        (
            new / old
        )
        -
        1
    ) * 100

# ============================================================
# SCORE
# ============================================================

def build_score(
    light,
    delta,
    spread,
    book,
    oi
):

    if (
        light["trend"] == "BULL"
        and
        delta > 0
        and
        book > 0
    ):

        side = "BUY"

    elif (
        light["trend"] == "BEAR"
        and
        delta < 0
        and
        book < 0
    ):

        side = "SELL"

    else:

        return None, 0

    score = 15

    # Momentum
    if (
        (
            side == "BUY"
            and
            light["momentum"] > 0
        )
        or
        (
            side == "SELL"
            and
            light["momentum"] < 0
        )
    ):

        score += 5

    # Volume
    vr = light["vol_ratio"]

    if vr >= 1.5:
        score += 15

    elif vr >= 1.2:
        score += 10

    elif vr >= 1.0:
        score += 5

    # Aggressive flow
    ad = abs(delta)

    if ad >= 0.50:
        score += 25

    elif ad >= 0.35:
        score += 20

    elif ad >= 0.20:
        score += 15

    elif ad >= 0.10:
        score += 8

    # Order book
    ab = abs(book)

    if ab >= 0.55:
        score += 25

    elif ab >= 0.40:
        score += 20

    elif ab >= 0.25:
        score += 15

    elif ab >= 0.15:
        score += 8

    # Spread
    if spread <= 0.5:
        score += 10

    elif spread <= 1.0:
        score += 7

    elif spread <= 2.0:
        score += 4

    # OI confirmation
    if oi > 0:
        score += 5

    return (
        side,
        min(
            score,
            100
        )
    )

# ============================================================
# HARD CONFIRMATION
# ============================================================

def hard_confirm(
    side,
    delta,
    book,
    spread
):

    if spread > 2.0:
        return False

    return (
        (
            side == "BUY"
            and
            delta >= 0.20
            and
            book >= 0.20
        )
        or
        (
            side == "SELL"
            and
            delta <= -0.20
            and
            book <= -0.20
        )
    )

# ============================================================
# SAVE SIGNAL
# ============================================================

def save_signal(row):

    new_file = (
        not LIVE_FILE.exists()
    )

    with LIVE_FILE.open(
        "a",
        newline="",
        encoding="utf-8"
    ) as f:

        w = csv.DictWriter(
            f,
            fieldnames=CSV_COLUMNS
        )

        if new_file:
            w.writeheader()

        w.writerow(row)


def recent_signals(limit=20):

    if not LIVE_FILE.exists():
        return []

    try:

        with LIVE_FILE.open(
            "r",
            newline="",
            encoding="utf-8"
        ) as f:

            rows = list(
                csv.DictReader(f)
            )

        return list(
            reversed(
                rows[-limit:]
            )
        )

    except Exception:
        return []

# ============================================================
# COOLDOWN
# ============================================================

def has_recent_or_open_trade(
    symbol,
    side
):

    now = datetime.now(
        timezone.utc
    )

    for r in trade_rows():

        if (
            r.get("symbol")
            != symbol
            or
            r.get("signal")
            != side
        ):
            continue

        if r.get("status") == "OPEN":
            return True

        try:

            t = datetime.fromisoformat(
                r.get(
                    "time_utc",
                    ""
                )
            )

            if t.tzinfo is None:

                t = t.replace(
                    tzinfo=timezone.utc
                )

            if (
                now - t
            ).total_seconds() < (
                SIGNAL_COOLDOWN_SECONDS
            ):

                return True

        except Exception:
            pass

    return False

# ============================================================
# CANDIDATE
# ============================================================

def candidate_payload(
    symbol,
    lm,
    side,
    score,
    delta,
    buy,
    sell,
    spread,
    book,
    oi
):

    price = lm["price"]

    if side == "BUY":

        tp = (
            price
            *
            (1 + TP_PCT)
        )

        sl = (
            price
            *
            (1 - SL_PCT)
        )

    else:

        tp = (
            price
            *
            (1 - TP_PCT)
        )

        sl = (
            price
            *
            (1 + SL_PCT)
        )

    confirmed = hard_confirm(
        side,
        delta,
        book,
        spread
    )

    return {
        "time_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        "exchange": "BYBIT",
        "symbol": symbol,
        "signal": side,
        "score": score,
        "price": price,
        "flow_delta": delta,
        "buy_usd_60s": buy,
        "sell_usd_60s": sell,
        "spread_bps": spread,
        "book_imb": book,
        "trend_5m": lm["trend"],
        "oi_change_pct": oi,
        "tp": tp,
        "sl": sl,
        "points_to_85": max(
            0,
            MIN_SCORE - score
        ),
        "hard_confirm": confirmed,
        "status": (
            "TRADE READY"
            if (
                score >= MIN_SCORE
                and
                confirmed
            )
            else
            "FORMING"
        ),
    }

# ============================================================
# LIGHT SCAN
# ============================================================

def _light_scan_one(symbol):

    try:

        lm = light_metrics(
            symbol
        )

        if not lm:
            return None

        rank = (
            lm["vol_ratio"] * 3
            +
            abs(
                lm["momentum"]
            ) * 10000
        )

        return (
            rank,
            symbol,
            lm,
            None
        )

    except Exception as e:

        return (
            None,
            symbol,
            None,
            str(e)
        )

# ============================================================
# DEEP SCAN
# ============================================================

def _deep_scan_one(item):

    _, symbol, lm = item

    try:

        dm = depth_metrics(
            symbol
        )

        if not dm:

            return {
                "symbol": symbol,
                "error": "No order book",
            }

        spread, book = dm

        delta, buy, sell = (
            flow_metrics(
                symbol
            )
        )

        oi = oi_change_pct(
            symbol
        )

        side, score = build_score(
            lm,
            delta,
            spread,
            book,
            oi
        )

        if not side:

            return {
                "symbol": symbol,
                "side": None,
                "score": 0,
                "error": None,
            }

        c = candidate_payload(
            symbol,
            lm,
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
            "lm": lm,
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

    scan_log(
        "================================"
    )

    scan_log(
        "BYBIT SCAN START"
    )

    with state_lock:

        state["status"] = (
            f"Scanning Top "
            f"{TOP_COINS} Bybit..."
        )

        state["last_error"] = None

        state["scan_progress"] = "0/0"

    # --------------------------------
    # OPEN TRADES
    # --------------------------------

    scan_log(
        "Checking open trades..."
    )

    check_open_trades()

    # --------------------------------
    # TOP SYMBOLS
    # --------------------------------

    scan_log(
        "Loading Top Bybit "
        "USDT Futures symbols..."
    )

    try:

        symbols = top_symbols()

    except Exception as e:

        error = (
            f"{type(e).__name__}: {e}"
        )

        scan_log(
            f"TOP SYMBOL LOAD ERROR: "
            f"{error}"
        )

        with state_lock:

            state["last_error"] = (
                error
            )

            state["status"] = (
                "Bybit symbol load "
                "error — retrying"
            )

            state["last_scan"] = (
                datetime.now(
                    timezone.utc
                ).isoformat()
            )

            state[
                "last_scan_seconds"
            ] = round(
                time.time()
                -
                scan_start,
                2
            )

        return

    with state_lock:

        state["scan_progress"] = (
            f"0/{len(symbols)}"
        )

    # --------------------------------
    # LIGHT SCAN
    # --------------------------------

    light_candidates = []
    light_errors = []

    with ThreadPoolExecutor(
        max_workers=max(
            1,
            LIGHT_SCAN_WORKERS
        )
    ) as pool:

        futures = {
            pool.submit(
                _light_scan_one,
                symbol
            ): symbol
            for symbol
            in symbols
        }

        done = 0

        for future in as_completed(
            futures
        ):

            done += 1

            if (
                done == 1
                or
                done % 10 == 0
                or
                done == len(symbols)
            ):

                scan_log(
                    f"LIGHT SCAN: "
                    f"{done}/"
                    f"{len(symbols)}"
                )

            result = future.result()

            if result:

                (
                    rank,
                    symbol,
                    lm,
                    err
                ) = result

                if (
                    rank is not None
                    and
                    lm is not None
                ):

                    light_candidates.append(
                        (
                            rank,
                            symbol,
                            lm
                        )
                    )

                elif err:

                    light_errors.append(
                        f"{symbol}: {err}"
                    )

            with state_lock:

                state[
                    "scan_progress"
                ] = (
                    f"{done}/"
                    f"{len(symbols)}"
                )

    light_candidates.sort(
        reverse=True,
        key=lambda x: x[0]
    )

    scan_log(
        f"LIGHT SCAN COMPLETE: "
        f"{len(light_candidates)} "
        f"candidates"
    )

    # --------------------------------
    # DEEP TOP 25
    # --------------------------------

    deep_items = (
        light_candidates[
            :DEEP_CHECK
        ]
    )

    scan_log(
        f"DEEP SCAN START: "
        f"{len(deep_items)} "
        f"candidates"
    )

    alerts = 0
    best = None
    deep_errors = []

    with ThreadPoolExecutor(
        max_workers=max(
            1,
            DEEP_SCAN_WORKERS
        )
    ) as pool:

        futures = {
            pool.submit(
                _deep_scan_one,
                item
            ): item[1]
            for item
            in deep_items
        }

        deep_done = 0

        for future in as_completed(
            futures
        ):

            deep_done += 1

            if (
                deep_done == 1
                or
                deep_done % 5 == 0
                or
                deep_done
                == len(deep_items)
            ):

                scan_log(
                    f"DEEP SCAN: "
                    f"{deep_done}/"
                    f"{len(deep_items)}"
                )

            result = future.result()

            if not result:
                continue

            if result.get("error"):

                deep_errors.append(
                    f"{result.get('symbol')}: "
                    f"{result['error']}"
                )

                continue

            side = result.get(
                "side"
            )

            score = result.get(
                "score",
                0
            )

            if not side:
                continue

            c = result[
                "candidate"
            ]

            # -------------------------
            # BEST CANDIDATE
            # -------------------------

            if (
                best is None
                or
                score
                >
                best["score"]
            ):

                best = c

                scan_log(
                    f"BEST CANDIDATE: "
                    f"{best['symbol']} "
                    f"{best['signal']} "
                    f"{best['score']}/100 "
                    f"| Hard="
                    f"{best['hard_confirm']}"
                )

                with state_lock:

                    state[
                        "best_candidate"
                    ] = best

                    state["status"] = (
                        f"Best forming: "
                        f"{best['symbol']} "
                        f"{'LONG' if best['signal']=='BUY' else 'SHORT'} "
                        f"{best['score']}/100"
                    )

            # -------------------------
            # VERIFIED ONLY
            # -------------------------

            if (
                score < MIN_SCORE
                or
                not c["hard_confirm"]
            ):
                continue

            symbol = result[
                "symbol"
            ]

            if has_recent_or_open_trade(
                symbol,
                side
            ):
                continue

            row = {
                "time_utc": c[
                    "time_utc"
                ],
                "symbol": symbol,
                "signal": side,
                "score": score,
                "price": c["price"],
                "flow_delta": result[
                    "delta"
                ],
                "buy_usd_60s": result[
                    "buy"
                ],
                "sell_usd_60s": result[
                    "sell"
                ],
                "spread_bps": result[
                    "spread"
                ],
                "book_imb": result[
                    "book"
                ],
                "trend_5m": result[
                    "lm"
                ]["trend"],
                "oi_change_pct": result[
                    "oi"
                ],
                "tp": c["tp"],
                "sl": c["sl"],
            }

            scan_log(
                f"🚨 TRADE READY: "
                f"{symbol} "
                f"{side} "
                f"{score}/100"
            )

            save_signal(row)
            add_open_trade(row)

            with state_lock:

                state[
                    "latest_signal"
                ] = row

                state[
                    "best_candidate"
                ] = c

            p = performance()

            telegram_async(
                f"🚨 RAZA SHAH SIGNAL\n"
                f"TRADE READY\n\n"
                f"Exchange: BYBIT\n"
                f"Coin: {symbol}\n"
                f"Direction: "
                f"{'BUY / LONG' if side=='BUY' else 'SELL / SHORT'}\n"
                f"Score: {score}/100 ✅\n\n"
                f"Entry: "
                f"{c['price']:.8g}\n"
                f"TP: "
                f"{c['tp']:.8g}\n"
                f"SL: "
                f"{c['sl']:.8g}\n\n"
                f"Flow: "
                f"{result['delta']:+.3f}\n"
                f"Order Book: "
                f"{result['book']:+.3f}\n"
                f"Spread: "
                f"{result['spread']:.2f} bps\n"
                f"OI: "
                f"{result['oi']:+.3f}%\n\n"
                f"Closed Trades: "
                f"{p['total_trades']}\n"
                f"Win Rate: "
                f"{p['win_rate']:.2f}%\n\n"
                f"🔗 {APP_URL}"
            )

            alerts += 1

    # --------------------------------
    # FINISH
    # --------------------------------

    now = datetime.now(
        timezone.utc
    )

    elapsed = round(
        time.time()
        -
        scan_start,
        2
    )

    scan_log(
        f"SCAN COMPLETE in "
        f"{elapsed}s "
        f"| alerts={alerts} "
        f"| best="
        f"{best['symbol'] if best else 'NONE'}"
    )

    with state_lock:

        state[
            "best_candidate"
        ] = best

        state["last_scan"] = (
            now.isoformat()
        )

        state[
            "alerts_last_scan"
        ] = alerts

        state[
            "last_scan_seconds"
        ] = elapsed

        if alerts:

            state["status"] = (
                f"{alerts} verified "
                f"85+ trade ready"
            )

        elif best:

            state["status"] = (
                f"Best forming: "
                f"{best['symbol']} "
                f"{'LONG' if best['signal']=='BUY' else 'SHORT'} "
                f"{best['score']}/100"
            )

        else:

            state["status"] = (
                "Waiting for "
                "85+ setup"
            )

        if (
            not best
            and
            (
                light_errors
                or
                deep_errors
            )
        ):

            errors = (
                light_errors
                +
                deep_errors
            )[:3]

            state["last_error"] = (
                " | ".join(errors)
            )

# ============================================================
# HOURLY TELEGRAM STATUS
# ============================================================

def telegram_hourly_status_loop():

    while True:

        time.sleep(
            TELEGRAM_STATUS_INTERVAL
        )

        try:

            p = performance()

            with state_lock:
                st = dict(state)

            best = st.get(
                "best_candidate"
            )

            best_line = (
                "Best Candidate: none"
            )

            if best:

                best_line = (
                    f"Best Candidate: "
                    f"{best.get('symbol')} | "
                    f"{'LONG' if best.get('signal')=='BUY' else 'SHORT'} | "
                    f"{best.get('score',0)}/100"
                )

            telegram_async(
                f"🟢 RAZA SHAH SIGNAL\n"
                f"1 HOUR STATUS\n\n"
                f"Exchange: BYBIT\n"
                f"Scanner: "
                f"{'LIVE' if st.get('running') else 'OFFLINE'}\n"
                f"Status: "
                f"{st.get('status') or '—'}\n"
                f"{best_line}\n"
                f"Last scan alerts: "
                f"{st.get('alerts_last_scan',0)}\n"
                f"Last scan time: "
                f"{st.get('last_scan_seconds','—')}s\n"
                f"Progress: "
                f"{st.get('scan_progress','—')}\n\n"
                f"Trades: "
                f"{p['total_trades']}\n"
                f"Wins: "
                f"{p['wins']}\n"
                f"Losses: "
                f"{p['losses']}\n"
                f"Winning: "
                f"{p['win_rate']:.2f}%\n"
                f"Open: "
                f"{p['open_trades']}\n\n"
                f"🔗 {APP_URL}"
            )

        except Exception as e:

            scan_log(
                f"HOURLY STATUS ERROR: {e}"
            )

# ============================================================
# SCANNER LOOP
# ============================================================

def scanner_loop():

    with state_lock:

        state["running"] = True

        state["exchange"] = (
            "BYBIT"
        )

    p = performance()

    telegram_async(
        f"🟢 RAZA SHAH SIGNAL\n"
        f"LIVE ACTIVE\n\n"
        f"Exchange: BYBIT\n"
        f"Top Coins: {TOP_COINS}\n"
        f"Deep Scan: {DEEP_CHECK}\n"
        f"Verified Score: "
        f"{MIN_SCORE}+\n"
        f"Scan interval: "
        f"{SCAN_INTERVAL//60} minutes\n"
        f"Hourly status: ON\n\n"
        f"All-Time: "
        f"{p['total_trades']} trades\n"
        f"Winning: "
        f"{p['win_rate']:.2f}%\n\n"
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

                state["last_error"] = (
                    str(e)
                )

                state["status"] = (
                    "Scan error — retrying"
                )

        wait = max(
            10,
            SCAN_INTERVAL
            -
            (
                time.time()
                -
                start
            )
        )

        with state_lock:

            state["next_scan"] = (
                datetime.fromtimestamp(
                    time.time()
                    +
                    wait,
                    timezone.utc
                ).isoformat()
            )

        time.sleep(wait)

# ============================================================
# WEB ROUTES
# ============================================================

@app.route(
    "/",
    methods=[
        "GET",
        "HEAD"
    ]
)
def home():

    if request.method == "HEAD":
        return Response(status=200)

    if not is_authorized():

        ip = client_ip()

        _, created = (
            ensure_otp_for_ip(ip)
        )

        msg = (
            "Permission code sent "
            "to Telegram."
            if created
            else
            "Use the active Telegram "
            "code already sent for this IP."
        )

        return login_html(msg)

    return render_template(
        "index.html"
    )


@app.route(
    "/verify",
    methods=["POST"]
)
def verify():

    ip = client_ip()

    code = (
        request.form.get(
            "code"
        )
        or ""
    ).strip()

    cleanup_access()

    now = time.time()

    with otp_lock:

        rec = otp_by_ip.get(ip)

        if (
            not rec
            or
            rec.get(
                "expires",
                0
            ) <= now
        ):

            otp_by_ip.pop(
                ip,
                None
            )

            return login_html(
                "Code expired. "
                "Reload once for "
                "a new code."
            ), 401

        if (
            rec.get(
                "attempts",
                0
            )
            >=
            OTP_MAX_ATTEMPTS
        ):

            otp_by_ip.pop(
                ip,
                None
            )

            return login_html(
                "Too many attempts. "
                "Reload once."
            ), 429

        rec["attempts"] = (
            rec.get(
                "attempts",
                0
            )
            +
            1
        )

        ok = secrets.compare_digest(
            code,
            rec.get(
                "code",
                ""
            )
        )

        if ok:

            access_until = (
                now
                +
                ACCESS_TTL_SECONDS
            )

            authorized_ips[ip] = (
                access_until
            )

            otp_by_ip.pop(
                ip,
                None
            )

    if not ok:

        return login_html(
            "Invalid code."
        ), 401

    flask_session[
        "authorized_ip"
    ] = ip

    flask_session[
        "authorized_until"
    ] = access_until

    flask_session.permanent = True

    telegram_async(
        f"✅ RAZA SHAH SIGNAL\n"
        f"ACCESS GRANTED\n\n"
        f"IP: {ip}\n"
        f"Access valid: 24 hours\n"
        f"🔗 {APP_URL}"
    )

    return redirect(
        url_for("home")
    )


@app.route("/logout")
def logout():

    ip = client_ip()

    with otp_lock:

        authorized_ips.pop(
            ip,
            None
        )

        otp_by_ip.pop(
            ip,
            None
        )

    flask_session.clear()

    return redirect(
        url_for("home")
    )


@app.route("/api/status")
def api_status():

    if not is_authorized():

        return jsonify({
            "error": "unauthorized"
        }), 401

    with state_lock:
        x = dict(state)

    x["signals"] = (
        recent_signals(20)
    )

    x["telegram"] = bool(
        TELEGRAM_BOT_TOKEN
        and
        TELEGRAM_CHAT_ID
    )

    x["min_score"] = (
        MIN_SCORE
    )

    x["performance"] = (
        performance()
    )

    return jsonify(x)


@app.route("/api/signal")
def api_signal():

    if not is_authorized():

        return jsonify({
            "error": "unauthorized"
        }), 401

    with state_lock:
        x = dict(state)

    x["performance"] = (
        performance()
    )

    return jsonify(x)


@app.route(
    "/manifest.webmanifest"
)
def manifest():

    return send_from_directory(
        "static",
        "manifest.webmanifest",
        mimetype=(
            "application/manifest+json"
        )
    )


@app.route("/sw.js")
def sw():

    return send_from_directory(
        "static",
        "sw.js",
        mimetype=(
            "application/javascript"
        )
    )

# ============================================================
# START THREADS
# ============================================================

threading.Thread(
    target=scanner_loop,
    daemon=True
).start()

threading.Thread(
    target=telegram_hourly_status_loop,
    daemon=True
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
                "10000"
            )
        )
    )
