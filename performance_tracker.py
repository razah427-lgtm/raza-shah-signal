import json
import os
import threading
import uuid
from datetime import datetime, timezone

DATA_FILE = os.environ.get(
    "PERFORMANCE_DATA_FILE",
    "performance_data.json"
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


class PerformanceTracker:

    def __init__(self, data_file=DATA_FILE):
        self.data_file = data_file
        self.lock = threading.Lock()
        self.ensure_file()

    def default_data(self):
        return {"trades": []}

    def ensure_file(self):
        if not os.path.exists(self.data_file):
            self.save(self.default_data())

    def load(self):
        try:
            with open(self.data_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            if "trades" not in data:
                data["trades"] = []

            return data

        except Exception:
            return self.default_data()

    def save(self, data):
        with open(self.data_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def bucket(self, score):

        score = float(score)

        if 60 <= score <= 84:
            return "forming"

        if score >= 85:
            return "strong"

        return None

    # ==========================================
    # OPEN NEW TRADE
    # ==========================================

    def open_trade(
        self,
        coin,
        direction,
        score,
        entry,
        tp,
        sl
    ):

        score = float(score)

        bucket = self.bucket(score)

        if bucket is None:
            return None

        trade = {
            "id": uuid.uuid4().hex[:12],
            "time_opened": utc_now(),
            "time_closed": None,
            "coin": str(coin).upper(),
            "direction": str(direction).upper(),
            "score": score,
            "bucket": bucket,
            "entry": float(entry),
            "tp": float(tp),
            "sl": float(sl),
            "status": "OPEN",
            "result": None
        }

        with self.lock:

            data = self.load()

            data["trades"].append(trade)

            self.save(data)

        return trade

    # ==========================================
    # CLOSE TRADE
    # ==========================================

    def close_trade(self, trade_id, result):

        result = str(result).upper()

        if result not in ["WIN", "LOSS"]:
            return False

        with self.lock:

            data = self.load()

            for trade in data["trades"]:

                if (
                    trade.get("id") == trade_id
                    and trade.get("status") == "OPEN"
                ):

                    trade["status"] = "CLOSED"

                    trade["result"] = result

                    trade["time_closed"] = utc_now()

                    self.save(data)

                    return True

        return False

    # ==========================================
    # PERFORMANCE CALCULATION
    # ==========================================

    def calculate_stats(self, trades, bucket):

        rows = [
            t for t in trades
            if t.get("bucket") == bucket
        ]

        open_trades = [
            t for t in rows
            if t.get("status") == "OPEN"
        ]

        closed = [
            t for t in rows
            if t.get("status") == "CLOSED"
        ]

        wins = [
            t for t in closed
            if t.get("result") == "WIN"
        ]

        losses = [
            t for t in closed
            if t.get("result") == "LOSS"
        ]

        total = len(rows)

        closed_total = len(closed)

        if closed_total > 0:
            win_rate = (
                len(wins) / closed_total
            ) * 100
        else:
            win_rate = 0

        if total > 0:

            avg_score = sum(
                float(t.get("score", 0))
                for t in rows
            ) / total

        else:
            avg_score = 0

        return {
            "total": total,
            "closed": closed_total,
            "wins": len(wins),
            "losses": len(losses),
            "open": len(open_trades),
            "win_rate": round(win_rate, 2),
            "avg_score": round(avg_score, 1)
        }

    # ==========================================
    # RECENT 85+ VERIFIED TRADES
    # ==========================================

    def recent_verified(self, limit=20):

        data = self.load()

        trades = [
            t for t in data["trades"]
            if t.get("bucket") == "strong"
        ]

        trades.sort(
            key=lambda x: x.get(
                "time_opened", ""
            ),
            reverse=True
        )

        return trades[:limit]

    # ==========================================
    # DASHBOARD DATA
    # ==========================================

    def get_dashboard_stats(self):

        data = self.load()

        trades = data["trades"]

        forming = self.calculate_stats(
            trades,
            "forming"
        )

        strong = self.calculate_stats(
            trades,
            "strong"
        )

        return {

            "best_forming_performance":
                forming,

            "strong_ready_performance":
                strong,

            "recent_verified_trades":
                self.recent_verified(20)
        }


# ==============================================
# GLOBAL TRACKER
# ==============================================

performance_tracker = PerformanceTracker()


if __name__ == "__main__":

    print(
        json.dumps(
            performance_tracker.get_dashboard_stats(),
            indent=2
        )
    )