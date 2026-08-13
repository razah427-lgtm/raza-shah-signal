import requests
import json
from datetime import datetime, timezone

BASE = "https://api.bitget.com"
TIMEOUT = 12

tests = [
    (
        "Ticker",
        "/api/v2/mix/market/ticker",
        {
            "symbol": "BTCUSDT",
            "productType": "usdt-futures",
        },
    ),
    (
        "Mark Candles",
        "/api/v2/mix/market/history-mark-candles",
        {
            "symbol": "BTCUSDT",
            "productType": "usdt-futures",
            "granularity": "5m",
            "limit": "20",
        },
    ),
    (
        "Open Interest",
        "/api/v2/mix/market/open-interest",
        {
            "symbol": "BTCUSDT",
            "productType": "usdt-futures",
        },
    ),
]

print("=" * 70)
print("RAZA SHAH SIGNAL — BITGET FUTURES CONNECTION TEST")
print("UTC:", datetime.now(timezone.utc).isoformat())
print("=" * 70)

all_ok = True

for name, path, params in tests:
    url = BASE + path
    print(f"\n[{name}]")
    print("URL:", url)

    try:
        r = requests.get(
            url,
            params=params,
            headers={
                "User-Agent": "RAZA-SHAH-SIGNAL-BITGET-TEST/1.0",
                "Accept": "application/json",
            },
            timeout=TIMEOUT,
        )

        print("HTTP:", r.status_code)

        if r.status_code != 200:
            all_ok = False
            print("FAIL:", r.text[:500])
            continue

        data = r.json()
        print("code:", data.get("code"))
        print("msg:", data.get("msg"))

        if str(data.get("code")) != "00000":
            all_ok = False
            print("FAIL RESPONSE:", json.dumps(data, indent=2)[:1000])
            continue

        print("PASS")
        print("sample:", json.dumps(data.get("data"), ensure_ascii=False)[:800])

    except Exception as e:
        all_ok = False
        print("ERROR:", type(e).__name__, str(e))

print("\n" + "=" * 70)

if all_ok:
    print("FINAL RESULT: BITGET FUTURES CONNECTION = PASS")
    print("Render can reach Bitget public futures market data.")
else:
    print("FINAL RESULT: BITGET FUTURES CONNECTION = FAIL")
    print("Send these logs back before changing the main scanner.")

print("=" * 70)
