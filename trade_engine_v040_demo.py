import asyncio
import base64
import hashlib
import hmac
import html
import json
import logging
import os
import secrets
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread

import main as core

log = logging.getLogger("trade-engine")


def env_bool(name, default="false"):
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


AUTO_LIVE_ORDERS_ENABLED = False
MANUAL_LIVE_APPROVAL_ENABLED = env_bool("MANUAL_LIVE_APPROVAL_ENABLED", "false")
APPROVAL_USER = os.getenv("APPROVAL_USER", "dodo")
APPROVAL_PIN = os.getenv("APPROVAL_PIN", "")
MAX_LIVE_NOTIONAL_USDT = Decimal(os.getenv("MAX_LIVE_NOTIONAL_USDT", "5"))
DEFAULT_TEST_DURATION_SEC = int(os.getenv("DEFAULT_TEST_DURATION_SEC", "300"))
DEMO_LEVERAGE = int(os.getenv("DEMO_LEVERAGE", "20"))
MAX_OPEN_LINEAR_POSITIONS = int(os.getenv("MAX_OPEN_LINEAR_POSITIONS", "10"))
ALLOWED_DURATIONS = {300, 600, 900}
MIN_MANUAL_NOTIONAL_USDT = Decimal(os.getenv("MIN_MANUAL_NOTIONAL_USDT", "1000"))
ANALYTICS_CACHE = {}


LIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS live_trade_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT,
  qty TEXT,
  notional_usdt REAL,
  duration_sec INTEGER,
  signal_score REAL,
  best_window_min INTEGER,
  order_id TEXT,
  status TEXT NOT NULL,
  note TEXT
);
CREATE TABLE IF NOT EXISTS approval_nonces (
  nonce_hash TEXT PRIMARY KEY,
  created_ts_ms INTEGER NOT NULL,
  expires_ts_ms INTEGER NOT NULL,
  used_ts_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_live_trade_events_ts ON live_trade_events(ts_ms);
"""


def dec(v, default=None):
    try:
        return Decimal(str(v)) if v not in (None, "") else default
    except Exception:
        return default


def dtxt(v: Decimal):
    s = format(v, "f")
    return s.rstrip("0").rstrip(".") if "." in s else s


def floor_step(value: Decimal, step: Decimal):
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def ceil_step(value: Decimal, step: Decimal):
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_UP) * step


class BybitTradeClient(core.BybitPrivateClient):
    def _post(self, path, body, allow_codes=None):
        if not self.configured:
            raise RuntimeError("Bybit API is not configured")
        allow_codes = set(allow_codes or [])
        body_text = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        ts = str(core.now_ms())
        payload = ts + self.api_key + self.recv_window + body_text
        signature = hmac.new(
            self.api_secret.encode(), payload.encode(), hashlib.sha256
        ).hexdigest()
        req = urllib.request.Request(
            self.base_url + path,
            data=body_text.encode(),
            headers={
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": ts,
                "X-BAPI-SIGN": signature,
                "X-BAPI-RECV-WINDOW": self.recv_window,
                "Content-Type": "application/json",
                "User-Agent": "trade-engine-v0.4.0-demo",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=12) as response:
                data = json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            body_err = exc.read().decode(errors="replace")
            raise RuntimeError(f"HTTP {exc.code}: {body_err[:300]}") from exc
        code = data.get("retCode")
        if code != 0 and code not in allow_codes:
            raise RuntimeError(f"Bybit retCode={code} retMsg={data.get('retMsg')}")
        return data

    def public_get(self, path, params=None):
        query = urllib.parse.urlencode(params or {})
        url = self.base_url + path + (("?" + query) if query else "")
        req = urllib.request.Request(url, headers={"User-Agent": "trade-engine-v0.4.0-demo"})
        try:
            with urllib.request.urlopen(req, timeout=12) as response:
                data = json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            body_err = exc.read().decode(errors="replace")
            raise RuntimeError(f"HTTP {exc.code}: {body_err[:300]}") from exc
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit retCode={data.get('retCode')} retMsg={data.get('retMsg')}")
        return data

    def instrument(self, symbol):
        data = self.public_get(
            "/v5/market/instruments-info", {"category": "linear", "symbol": symbol}
        )
        rows = data.get("result", {}).get("list", [])
        if not rows:
            raise RuntimeError(f"Instrument not found: {symbol}")
        return rows[0]

    def set_leverage_demo(self, symbol):
        return self._post(
            "/v5/position/set-leverage",
            {
                "category": "linear",
                "symbol": symbol,
                "buyLeverage": str(DEMO_LEVERAGE),
                "sellLeverage": str(DEMO_LEVERAGE),
            },
            allow_codes={110043},
        )

    def market_order(self, symbol, side, qty, reduce_only):
        return self._post(
            "/v5/order/create",
            {
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "orderType": "Market",
                "qty": qty,
                "reduceOnly": bool(reduce_only),
                "positionIdx": 0,
            },
        )



def mean_or_none(values):
    values = [float(v) for v in values if v is not None]
    return (sum(values) / len(values)) if values else None


def clamp01(v):
    return max(0.0, min(1.0, float(v)))


def historical_edge(db, symbol, best_window, raw_score, horizon_min):
    """Descriptive benchmark from already-matured forward labels."""
    if not best_window or raw_score == 0:
        return {"n": 0, "hit_rate": None, "avg_move_pct": None}
    direction = 1 if raw_score > 0 else -1
    rows = db.query(
        """SELECT l.future_ret_pct
           FROM labels l
           JOIN features f
             ON f.symbol=l.symbol
            AND f.ts_ms=l.feature_ts_ms
            AND f.window_min=l.window_min
           WHERE f.symbol=?
             AND f.window_min=?
             AND l.horizon_min=?
             AND f.research_score IS NOT NULL
             AND l.future_ret_pct IS NOT NULL
             AND ((? > 0 AND f.research_score > 0)
                  OR (? < 0 AND f.research_score < 0))
             AND ABS(f.research_score - ?) <= 25
           ORDER BY l.feature_ts_ms DESC
           LIMIT 250""",
        (symbol, int(best_window), int(horizon_min),
         direction, direction, float(raw_score)),
    )
    vals = [float(r[0]) for r in rows if r[0] is not None]
    if not vals:
        return {"n": 0, "hit_rate": None, "avg_move_pct": None}
    hits = sum(1 for v in vals if v * direction > 0)
    directional_moves = [v * direction for v in vals]
    return {
        "n": len(vals),
        "hit_rate": round(hits / len(vals), 4),
        "avg_move_pct": round(sum(directional_moves) / len(directional_moves), 4),
    }


def suggested_notional(decision_abs, agreement, persistence, turnover_5m,
                       spread_bps, hist, conflict):
    t = float(turnover_5m or 0.0)
    if t >= 200_000_000:
        liquidity = 1.0
    elif t >= 50_000_000:
        liquidity = 0.90
    elif t >= 10_000_000:
        liquidity = 0.78
    elif t >= 2_000_000:
        liquidity = 0.64
    elif t >= 500_000:
        liquidity = 0.48
    elif t >= 100_000:
        liquidity = 0.34
    else:
        liquidity = 0.22

    spread = max(0.0, float(spread_bps or 0.0))
    spread_quality = max(0.35, min(1.0, 1.0 - spread / 20.0))
    quality = clamp01(float(decision_abs) / 100.0)

    factor = (
        0.14
        + 0.31 * quality
        + 0.20 * clamp01(agreement)
        + 0.14 * clamp01(persistence)
        + 0.15 * liquidity
        + 0.06 * spread_quality
    )

    if hist and hist.get("n", 0) >= 10 and hist.get("hit_rate") is not None:
        factor *= 0.82 + 0.36 * float(hist["hit_rate"])
    else:
        factor *= 0.92

    if conflict:
        factor *= 0.62

    raw = float(MAX_LIVE_NOTIONAL_USDT) * factor
    raw = max(5_000.0, min(float(MAX_LIVE_NOTIONAL_USDT), raw))
    return int(round(raw / 500.0) * 500)


def build_signal_analytics(db, symbol):
    row = db.one(
        """SELECT ts_ms,best_window_min,score,side,price,reason_json,status
           FROM signals WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1""",
        (symbol,),
    )
    if not row:
        return None

    cache_key = (symbol, int(row[0]))
    cached = ANALYTICS_CACHE.get(cache_key)
    if cached is not None:
        ticker = db.one(
            """SELECT ts_ms,last_price FROM ticker_snapshots
               WHERE symbol=? AND last_price IS NOT NULL
               ORDER BY ts_ms DESC LIMIT 1""",
            (symbol,),
        )
        result = dict(cached)
        if ticker:
            result["market_ts_ms"] = ticker[0]
            result["price"] = float(ticker[1])
        return result

    feature_rows = db.query(
        """SELECT window_min,research_score,turnover,avg_spread_bps
           FROM features
           WHERE symbol=? AND ts_ms=?
           ORDER BY window_min""",
        (symbol, row[0]),
    )
    window_scores = {
        int(r[0]): float(r[1])
        for r in feature_rows
        if r[1] is not None
    }
    scores = list(window_scores.values())
    if not scores:
        return None

    short_vals = [v for w, v in window_scores.items() if w <= 5]
    mid_vals = [v for w, v in window_scores.items() if 6 <= w <= 12]
    long_vals = [v for w, v in window_scores.items() if w >= 15]
    short_score = mean_or_none(short_vals)
    mid_score = mean_or_none(mid_vals)
    long_score = mean_or_none(long_vals)
    consensus = mean_or_none(scores) or 0.0

    groups = []
    for val, weight in ((short_score, 0.35), (mid_score, 0.40), (long_score, 0.25)):
        if val is not None:
            groups.append((val, weight))
    weight_total = sum(w for _, w in groups) or 1.0
    structure_score = sum(v * w for v, w in groups) / weight_total

    raw_best = float(row[2])
    direction_source = structure_score if abs(structure_score) >= 2.0 else consensus
    if abs(direction_source) < 2.0:
        direction_source = raw_best
    direction = 1 if direction_source >= 0 else -1

    directional_scores = [v for v in scores if abs(v) >= 5.0]
    agreement = (
        sum(1 for v in directional_scores if v * direction > 0) / len(directional_scores)
        if directional_scores else 0.5
    )

    recent = db.query(
        """SELECT ts_ms,AVG(research_score)
           FROM features
           WHERE symbol=? AND research_score IS NOT NULL
           GROUP BY ts_ms
           ORDER BY ts_ms DESC
           LIMIT 5""",
        (symbol,),
    )
    recent_consensus = [float(r[1]) for r in recent if r[1] is not None]
    persistence = (
        sum(1 for v in recent_consensus if v * direction > 0) / len(recent_consensus)
        if recent_consensus else 0.5
    )

    conflict = bool(
        short_score is not None and long_score is not None
        and short_score * long_score < 0
        and abs(short_score) >= 15 and abs(long_score) >= 15
    )

    history = {
        "300": historical_edge(db, symbol, row[1], raw_best, 5),
        "600": historical_edge(db, symbol, row[1], raw_best, 10),
        "900": historical_edge(db, symbol, row[1], raw_best, 15),
    }
    hist10 = history["600"]

    strength = min(100.0, max(abs(raw_best), abs(structure_score)) * 1.25)
    conviction = 0.60 * strength + 20.0 * agreement + 10.0 * persistence
    if hist10["n"] >= 10 and hist10["hit_rate"] is not None:
        evidence_weight = min(1.0, hist10["n"] / 50.0)
        conviction += (hist10["hit_rate"] - 0.5) * 20.0 * evidence_weight
    if abs(consensus) < 8:
        conviction -= 4.0
    if conflict:
        conviction *= 0.65
    conviction = max(0.0, min(100.0, conviction))
    decision_score = round(direction * conviction, 2)

    decision_side = (
        "LONG" if decision_score >= core.SIGNAL_THRESHOLD
        else "SHORT" if decision_score <= -core.SIGNAL_THRESHOLD
        else "WAIT"
    )

    turnover_5m = None
    spread_5m = None
    for w, _, turnover, spread in feature_rows:
        if int(w) == 5:
            turnover_5m = float(turnover or 0.0)
            spread_5m = float(spread or 0.0) if spread is not None else None
            break
    if turnover_5m is None and feature_rows:
        turnover_5m = float(feature_rows[0][2] or 0.0)
        spread_5m = float(feature_rows[0][3] or 0.0) if feature_rows[0][3] is not None else None

    suggestions = {}
    for duration_key in ("300", "600", "900"):
        suggestions[duration_key] = suggested_notional(
            abs(decision_score), agreement, persistence, turnover_5m,
            spread_5m, history[duration_key], conflict
        )

    ticker = db.one(
        """SELECT ts_ms,last_price FROM ticker_snapshots
           WHERE symbol=? AND last_price IS NOT NULL
           ORDER BY ts_ms DESC LIMIT 1""",
        (symbol,),
    )

    reasons = json.loads(row[5] or "{}")
    result = {
        "symbol": symbol,
        "ts_ms": row[0],
        "market_ts_ms": ticker[0] if ticker else None,
        "best_window_min": row[1],
        "best_score": raw_best,
        "score": decision_score,
        "decision_score": decision_score,
        "decision_side": decision_side,
        "consensus_score": round(consensus, 2),
        "short_score": round(short_score, 2) if short_score is not None else None,
        "mid_score": round(mid_score, 2) if mid_score is not None else None,
        "long_score": round(long_score, 2) if long_score is not None else None,
        "agreement": round(agreement, 4),
        "persistence": round(persistence, 4),
        "conflict": conflict,
        "window_count": len(scores),
        "window_total": len(core.FEATURE_WINDOWS_MIN),
        "side": decision_side,
        "price": float(ticker[1]) if ticker and ticker[1] is not None else float(row[4]),
        "reasons": reasons,
        "history": history,
        "suggested_notional": suggestions,
        "turnover_5m": round(float(turnover_5m or 0.0), 2),
        "spread_bps": round(float(spread_5m or 0.0), 4) if spread_5m is not None else None,
        "status": "SIGNAL" if decision_side != "WAIT" else "WAIT",
    }

    if len(ANALYTICS_CACHE) > 500:
        ANALYTICS_CACHE.clear()
    ANALYTICS_CACHE[cache_key] = dict(result)
    return result


def latest_signals(db):
    return [
        item for item in (build_signal_analytics(db, symbol) for symbol in core.SYMBOLS)
        if item
    ]


class SafeFeatureEngine(core.FeatureEngine):
    """Reject stale endpoint snapshots so rotated/reconnected symbols don't
    accidentally stitch old market data into a fresh feature window."""

    def latest_ticker(self, symbol, ts_ms):
        row = super().latest_ticker(symbol, ts_ms)
        if not row:
            return None
        max_age_ms = max(15_000, int(core.SNAPSHOT_INTERVAL_SEC * 1000 * 4))
        if int(ts_ms) - int(row[0]) > max_age_ms:
            return None
        return row


class LiveApproval:
    def __init__(self, db):
        self.db = db
        self.client = BybitTradeClient()
        self.lock = Lock()
        with db.lock:
            db.conn.executescript(LIVE_SCHEMA)
            db.conn.commit()

    @property
    def ready(self):
        return (
            MANUAL_LIVE_APPROVAL_ENABLED
            and bool(APPROVAL_PIN)
            and self.client.configured
            and not AUTO_LIVE_ORDERS_ENABLED
            and "api-demo.bybit.com" in core.BYBIT_REST_URL
        )

    def positions(self):
        data = self.client.positions()
        rows = []
        for p in data.get("result", {}).get("list", []):
            size = dec(p.get("size"), Decimal("0")) or Decimal("0")
            if size > 0:
                rows.append(
                    {
                        "symbol": p.get("symbol"),
                        "side": p.get("side"),
                        "size": dtxt(size),
                        "avg_price": p.get("avgPrice"),
                        "mark_price": p.get("markPrice"),
                        "leverage": p.get("leverage"),
                        "unrealised_pnl": p.get("unrealisedPnl"),
                        "position_value": p.get("positionValue"),
                        "position_idx": p.get("positionIdx"),
                    }
                )
        return rows

    def signal(self, symbol):
        return build_signal_analytics(self.db, symbol)

    def issue_nonce(self, ttl=120):
        token = secrets.token_urlsafe(24)
        digest = hashlib.sha256(token.encode()).hexdigest()
        ts = core.now_ms()
        self.db.execute(
            "INSERT OR REPLACE INTO approval_nonces VALUES (?,?,?,NULL)",
            (digest, ts, ts + ttl * 1000),
        )
        return token

    def consume_nonce(self, token):
        digest = hashlib.sha256((token or "").encode()).hexdigest()
        row = self.db.one(
            "SELECT expires_ts_ms,used_ts_ms FROM approval_nonces WHERE nonce_hash=?",
            (digest,),
        )
        if not row or row[1] is not None or row[0] < core.now_ms():
            raise RuntimeError("Approval expired/already used. Refresh the page.")
        self.db.execute(
            "UPDATE approval_nonces SET used_ts_ms=? WHERE nonce_hash=?",
            (core.now_ms(), digest),
        )

    def qty_for_notional(self, symbol, notional):
        sig = self.signal(symbol)
        if not sig or sig["price"] <= 0:
            raise RuntimeError("No current market price for this symbol")
        inst = self.client.instrument(symbol)
        lot = inst.get("lotSizeFilter", {})
        lev = inst.get("leverageFilter", {})
        max_lev = dec(lev.get("maxLeverage"))
        if max_lev is not None and Decimal(str(DEMO_LEVERAGE)) > max_lev:
            raise RuntimeError(
                f"{symbol} max leverage is {dtxt(max_lev)}x; refusing requested {DEMO_LEVERAGE}x"
            )

        step = dec(lot.get("qtyStep"), Decimal("0")) or Decimal("0")
        min_qty = dec(lot.get("minOrderQty"), Decimal("0")) or Decimal("0")
        min_notional = dec(lot.get("minNotionalValue"), Decimal("0")) or Decimal("0")
        max_mkt_qty = (
            dec(lot.get("maxMktOrderQty"))
            or dec(lot.get("maxMarketOrderQty"))
            or dec(lot.get("maxOrderQty"))
        )

        latest = self.db.one(
            """SELECT ts_ms,last_price FROM ticker_snapshots
               WHERE symbol=? AND last_price IS NOT NULL
               ORDER BY ts_ms DESC LIMIT 1""",
            (symbol,),
        )
        if not latest or core.now_ms() - int(latest[0]) > 20_000:
            raise RuntimeError(f"{symbol} market price is stale; no order sent")
        price = Decimal(str(latest[1]))
        if price <= 0:
            raise RuntimeError(f"{symbol} current market price is invalid")

        qty = floor_step(notional / price, step)
        actual = qty * price
        if qty < min_qty:
            qty = min_qty
            actual = qty * price
        if min_notional and actual < min_notional:
            qty = ceil_step(min_notional / price, step)
            actual = qty * price

        if max_mkt_qty is not None and max_mkt_qty > 0 and qty > max_mkt_qty:
            max_notional = max_mkt_qty * price
            raise RuntimeError(
                f"{symbol}: {float(notional):,.2f} USDT requires qty {dtxt(qty)}, but Bybit's current "
                f"single Market-order limit is qty {dtxt(max_mkt_qty)} "
                f"(about {float(max_notional):,.2f} USDT at the current price). "
                f"No order sent."
            )

        if actual > MAX_LIVE_NOTIONAL_USDT + Decimal("0.02"):
            raise RuntimeError(
                f"{symbol} minimum order is about {actual:.4f} USDT, above the "
                f"${float(MAX_LIVE_NOTIONAL_USDT + Decimal('0.02')):,.2f} safety cap."
            )
        return sig, dtxt(qty), actual

    def open(self, symbol, duration, mode="test", expected_ts=None,
             expected_direction=None, requested_notional=None):
        with self.lock:
            if not self.ready:
                raise RuntimeError("Manual live approval is not enabled/configured")
            if symbol not in core.SYMBOLS or duration not in ALLOWED_DURATIONS:
                raise RuntimeError("Invalid symbol or duration")

            positions = self.positions()
            if len(positions) >= MAX_OPEN_LINEAR_POSITIONS:
                raise RuntimeError(
                    f"Guardrail: maximum {MAX_OPEN_LINEAR_POSITIONS} open linear positions reached"
                )
            if any(p.get("symbol") == symbol for p in positions):
                raise RuntimeError(
                    f"{symbol} already has an open position. Close it before opening this symbol again."
                )

            sig = self.signal(symbol)
            if not sig:
                raise RuntimeError("No current research snapshot for this symbol")

            try:
                notional = Decimal(str(requested_notional))
            except Exception:
                raise RuntimeError("Invalid order amount")
            if notional < MIN_MANUAL_NOTIONAL_USDT:
                raise RuntimeError(
                    f"Minimum manual demo amount is {float(MIN_MANUAL_NOTIONAL_USDT):,.0f} USDT"
                )
            if notional > MAX_LIVE_NOTIONAL_USDT:
                raise RuntimeError(
                    f"Manual amount cannot exceed {float(MAX_LIVE_NOTIONAL_USDT):,.0f} USDT"
                )

            direction = "Buy" if sig["decision_score"] >= 0 else "Sell"

            if expected_ts is not None:
                try:
                    expected_ts = int(expected_ts)
                except (TypeError, ValueError):
                    raise RuntimeError("Invalid signal snapshot")
                if int(sig["ts_ms"]) != expected_ts:
                    raise RuntimeError(
                        "Signal changed since this card was displayed. No order sent; tap the refreshed card again."
                    )

            if expected_direction:
                expected_direction = str(expected_direction).strip().title()
                if expected_direction not in {"Buy", "Sell"}:
                    raise RuntimeError("Invalid expected direction")
                if direction != expected_direction:
                    raise RuntimeError(
                        f"Decision direction changed from {expected_direction} to {direction}. "
                        "No order sent; tap the refreshed card again."
                    )

            if mode == "model":
                expected = (
                    "Buy" if sig["decision_side"] == "LONG"
                    else "Sell" if sig["decision_side"] == "SHORT"
                    else None
                )
                if expected is None or abs(sig["decision_score"]) < core.SIGNAL_THRESHOLD:
                    raise RuntimeError("No threshold-qualified V0.4 decision signal")
                direction = expected

            _, qty, actual = self.qty_for_notional(symbol, notional)

            self.client.set_leverage_demo(symbol)
            result = self.client.market_order(symbol, direction, qty, False)
            oid = result.get("result", {}).get("orderId")

            note = json.dumps(
                {
                    "mode": mode,
                    "decision_score": sig["decision_score"],
                    "best_score": sig["best_score"],
                    "agreement": sig["agreement"],
                    "conflict": sig["conflict"],
                    "requested_notional": float(notional),
                },
                separators=(",", ":"),
            )
            self.db.execute(
                """INSERT INTO live_trade_events
                (ts_ms,event_type,symbol,side,qty,notional_usdt,duration_sec,
                 signal_score,best_window_min,order_id,status,note)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    core.now_ms(), "OPEN", symbol, direction, qty,
                    float(actual), duration, sig["decision_score"],
                    sig["best_window_min"], oid, "SUBMITTED", note,
                ),
            )
            log.warning(
                "MANUAL DEMO OPEN approved symbol=%s side=%s decision=%s best=%s "
                "qty=%s notional≈%.4f duration=%ss",
                symbol, direction, sig["decision_score"], sig["best_score"],
                qty, float(actual), duration,
            )
            return {
                "symbol": symbol,
                "side": direction,
                "qty": qty,
                "notional": float(actual),
                "order_id": oid,
                "signal": sig,
            }

    def close_symbol(self, symbol):
        with self.lock:
            if not self.ready:
                raise RuntimeError("Manual live approval is not enabled/configured")
            symbol = (symbol or "").upper()
            if symbol not in core.SYMBOLS:
                raise RuntimeError("Invalid symbol")

            positions = self.positions()
            p = next((x for x in positions if x.get("symbol") == symbol), None)
            if not p:
                raise RuntimeError(f"No open position found for {symbol}")
            if str(p.get("position_idx")) not in {"0", "None"}:
                raise RuntimeError("Hedge mode is not supported")

            side = "Sell" if p["side"] == "Buy" else "Buy"
            result = self.client.market_order(symbol, side, p["size"], True)
            oid = result.get("result", {}).get("orderId")
            self.db.execute(
                """INSERT INTO live_trade_events
                (ts_ms,event_type,symbol,side,qty,notional_usdt,duration_sec,
                 signal_score,best_window_min,order_id,status,note)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    core.now_ms(), "CLOSE", symbol, side, p["size"],
                    core.f(p.get("position_value")), None, None, None,
                    oid, "SUBMITTED", "manual_reduce_only_close_one",
                ),
            )
            log.warning(
                "MANUAL DEMO CLOSE ONE approved symbol=%s side=%s qty=%s",
                symbol, side, p["size"],
            )
            return {
                "symbol": symbol,
                "size": p["size"],
                "order_id": oid,
            }

    def close(self):
        with self.lock:
            if not self.ready:
                raise RuntimeError("Manual live approval is not enabled/configured")

            positions = self.positions()
            if not positions:
                raise RuntimeError("No open linear positions found")
            if len(positions) > MAX_OPEN_LINEAR_POSITIONS:
                raise RuntimeError(
                    f"Guardrail: found {len(positions)} open positions, above configured max "
                    f"{MAX_OPEN_LINEAR_POSITIONS}; refusing bulk close"
                )

            # Validate every position first. If any is unsupported, touch none of them.
            for p in positions:
                if p["symbol"] not in core.SYMBOLS:
                    raise RuntimeError(
                        "Position is outside whitelist; refusing to touch any positions"
                    )
                if str(p.get("position_idx")) not in {"0", "None"}:
                    raise RuntimeError(
                        "Hedge mode is not supported in V0.4"
                    )

            closed = []
            for p in positions:
                side = "Sell" if p["side"] == "Buy" else "Buy"
                result = self.client.market_order(
                    p["symbol"], side, p["size"], True
                )
                oid = result.get("result", {}).get("orderId")

                self.db.execute(
                    """INSERT INTO live_trade_events
                    (ts_ms,event_type,symbol,side,qty,notional_usdt,duration_sec,
                     signal_score,best_window_min,order_id,status,note)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        core.now_ms(),
                        "CLOSE",
                        p["symbol"],
                        side,
                        p["size"],
                        core.f(p.get("position_value")),
                        None,
                        None,
                        None,
                        oid,
                        "SUBMITTED",
                        "manual_reduce_only_close_all",
                    ),
                )
                log.warning(
                    "MANUAL DEMO CLOSE approved symbol=%s side=%s qty=%s",
                    p["symbol"],
                    side,
                    p["size"],
                )
                closed.append(
                    {
                        "symbol": p["symbol"],
                        "size": p["size"],
                        "order_id": oid,
                    }
                )

            return {"closed": closed, "count": len(closed)}

    def position_states(self):
        """Return only positions that are actually open at Bybit, enriched with
        the latest manual OPEN event for per-symbol countdowns."""
        now = core.now_ms()
        out = []
        for p in self.positions():
            ev = self.db.one(
                """SELECT ts_ms,duration_sec,signal_score,best_window_min,notional_usdt
                   FROM live_trade_events
                   WHERE event_type='OPEN' AND symbol=?
                   ORDER BY id DESC LIMIT 1""",
                (p["symbol"],),
            )
            opened_ts = None
            duration = None
            due_ms = None
            entry_score = None
            entry_window = None
            entry_notional = None
            if ev and int(ev[0]) >= now - 86_400_000:
                opened_ts = int(ev[0])
                duration = int(ev[1]) if ev[1] else None
                due_ms = opened_ts + duration * 1000 if duration else None
                entry_score = core.f(ev[2])
                entry_window = ev[3]
                entry_notional = core.f(ev[4])
            out.append(
                {
                    **p,
                    "opened_ts_ms": opened_ts,
                    "duration_sec": duration,
                    "due_ms": due_ms,
                    "entry_score": entry_score,
                    "entry_window_min": entry_window,
                    "entry_notional_usdt": entry_notional,
                }
            )
        return out

    def current_plan(self):
        return self.db.one(
            """SELECT ts_ms,symbol,side,qty,duration_sec,signal_score,best_window_min
               FROM live_trade_events
               WHERE event_type='OPEN'
               ORDER BY id DESC LIMIT 1"""
        )


def auth_ok(header):
    if not APPROVAL_PIN:
        return False
    try:
        if not header or not header.startswith("Basic "):
            return False
        raw = base64.b64decode(header.split(" ", 1)[1]).decode()
        user, pin = raw.split(":", 1)
        return hmac.compare_digest(
            user, APPROVAL_USER
        ) and hmac.compare_digest(pin, APPROVAL_PIN)
    except Exception:
        return False


def start_server(collector, db, account, live):
    class Handler(BaseHTTPRequestHandler):
        def send_json(self, payload, status=200):
            body = json.dumps(payload, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_page(self, text, status=200):
            body = text.encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'none'; style-src 'unsafe-inline'; "
                "script-src 'unsafe-inline'; connect-src 'self'; "
                "form-action 'self'; frame-ancestors 'none'",
            )
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def require_auth(self):
            if auth_ok(self.headers.get("Authorization", "")):
                return True
            self.send_response(401)
            self.send_header(
                "WWW-Authenticate", 'Basic realm="Trade Approval"'
            )
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return False

        def form(self):
            try:
                n = min(int(self.headers.get("Content-Length", "0")), 8192)
                d = urllib.parse.parse_qs(self.rfile.read(n).decode())
                return {k: v[-1] for k, v in d.items() if v}
            except Exception:
                return {}

        def health(self):
            return {
                "status": "ok" if collector.last_message_ms else "starting",
                "version": "0.4.0-demo",
                "mode": "research_plus_manual_live_approval",
                "auto_live_orders_enabled": False,
                "manual_live_approval_enabled": live.ready,
                "max_live_notional_usdt": float(
                    MAX_LIVE_NOTIONAL_USDT
                ),
                "live_leverage": DEMO_LEVERAGE,
                "max_open_linear_positions": MAX_OPEN_LINEAR_POSITIONS,
                "symbols": core.SYMBOLS,
                "private_api": {
                    "configured": bool(
                        core.BYBIT_API_KEY and core.BYBIT_API_SECRET
                    ),
                    "status": account.status,
                    "last_ok_ms": account.last_ok_ms,
                },
                "rows": {
                    "features": db.scalar(
                        "SELECT COUNT(*) FROM features"
                    )
                    or 0,
                    "labels": db.scalar(
                        "SELECT COUNT(*) FROM labels"
                    )
                    or 0,
                    "signals": db.scalar(
                        "SELECT COUNT(*) FROM signals"
                    )
                    or 0,
                    "live_trade_events": db.scalar(
                        "SELECT COUNT(*) FROM live_trade_events"
                    )
                    or 0,
                },
            }

        def trade_page(self, message="", error=False):
            signals = latest_signals(db)
            strongest = (
                max(signals, key=lambda x: abs(x["score"]))
                if signals
                else None
            )

            try:
                positions = (
                    live.position_states()
                    if live.client.configured
                    else []
                )
                pos_error = ""
            except Exception as exc:
                positions, pos_error = [], str(exc)

            if strongest:
                symbol = strongest["symbol"]
                side = "Buy" if strongest["score"] >= 0 else "Sell"
                qualified = (
                    abs(strongest["score"]) >= core.SIGNAL_THRESHOLD
                    and strongest["side"] != "WAIT"
                )
            else:
                symbol = core.SYMBOLS[0]
                side = "Buy"
                qualified = False

            options = "".join(
                f'<option value="{html.escape(s)}" '
                f'{"selected" if s == symbol else ""}>'
                f"{html.escape(s)}</option>"
                for s in core.SYMBOLS
            )

            if positions:
                pos = "".join(
                    (
                        "<div class='position-row'>"
                        "<div class='position-main'>"
                        f"<div class='position-title'><b>{html.escape(p['symbol'])}</b>"
                        f"<span class='position-side'>{html.escape(str(p['side']))}</span></div>"
                        f"<div class='position-meta'>"
                        f"<span>Qty <b>{html.escape(str(p['size']))}</b></span>"
                        f"<span>Avg <b>{html.escape(str(p['avg_price']))}</b></span>"
                        f"<span>Mark <b>{html.escape(str(p['mark_price']))}</b></span>"
                        f"<span>Lev <b>{html.escape(str(p['leverage']))}x</b></span>"
                        f"<span>PnL <b>{html.escape(str(p['unrealised_pnl']))}</b></span>"
                        "</div></div>"
                        "<form method='post' action='/trade/close-one' "
                        "onsubmit='freshSubmit(event);return false;'>"
                        "<input type='hidden' name='nonce' value=''>"
                        f"<input type='hidden' name='symbol' value='{html.escape(p['symbol'])}'>"
                        "<button class='mini-close' type='submit'>CLOSE</button>"
                        "</form>"
                        "</div>"
                    )
                    for p in positions
                )
            else:
                pos = "<div class='empty-state'>No open linear position.</div>"

            if pos_error:
                pos += f"<div class='err'>{html.escape(pos_error)}</div>"

            msg = (
                f"<div class='notice {'err' if error else 'ok'}'>"
                f"{html.escape(message)}</div>"
                if message
                else ""
            )
            disabled = "" if live.ready else "disabled"
            qualification = (
                "MODEL SIGNAL" if qualified else "EXECUTION TEST ONLY"
            )

            # Initial state is embedded so the page paints instantly; JavaScript
            # then refreshes it from /signals without a page reload.
            initial_payload = json.dumps(
                {
                    "server_ms": core.now_ms(),
                    "symbols": core.SYMBOLS,
                    "threshold": float(core.SIGNAL_THRESHOLD),
                    "engine_interval_sec": int(core.FEATURE_INTERVAL_SEC),
                    "window_total": len(core.FEATURE_WINDOWS_MIN),
                    "signals": signals,
                    "positions": positions,
                },
                separators=(",", ":"),
            ).replace("</", "<\\/")

            text = f"""<!doctype html>
<html>
<head>
<meta name='viewport' content='width=device-width,initial-scale=1,viewport-fit=cover'>
<meta name='theme-color' content='#0b0e13'>
<style>
*{{box-sizing:border-box}}
:root{{--bg:#0b0e13;--card:#12171f;--card2:#171d26;--line:#27303c;--text:#f3f6f9;--muted:#8f9bab;--green:#25c785;--red:#ff5b67;--amber:#f4b740;--orange:#ff9f1a;--blue:#6fa8ff}}
body{{margin:0;font-family:Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif;background:var(--bg);color:var(--text)}}
.shell{{width:min(1180px,100%);margin:0 auto;padding:18px 16px 44px}}
.topbar{{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:14px}}
.eyebrow{{font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);font-weight:800}}
h1{{font-size:26px;line-height:1.1;margin:5px 0 4px}}
.subtitle{{margin:0;color:var(--muted);font-size:13px}}
.live-dot{{display:inline-flex;align-items:center;gap:7px;border:1px solid var(--line);background:#10151c;border-radius:999px;padding:8px 10px;font-size:12px;white-space:nowrap}}
.live-dot:before{{content:'';width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 0 4px rgba(37,199,133,.11)}}
.stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:9px;margin:13px 0}}
.stat{{background:var(--card);border:1px solid var(--line);border-radius:13px;padding:11px}}
.stat .v{{font-size:20px;font-weight:800;margin-top:3px}}
.stat .k{{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:15px;margin:12px 0;box-shadow:0 12px 36px rgba(0,0,0,.12)}}
.section-head{{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:12px}}
.section-head h2{{margin:0;font-size:17px}}
.section-note{{font-size:11px;color:var(--muted);text-align:right}}
.thresholds{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:10px 0 14px}}
.threshold{{border:1px solid var(--line);background:#0e131a;border-radius:12px;padding:10px;text-align:center}}
.threshold b{{display:block;font-size:13px}}
.threshold span{{font-size:11px;color:var(--muted)}}
.threshold.sell b{{color:var(--red)}} .threshold.buy b{{color:var(--green)}} .threshold.ready b{{color:var(--orange)}}
.research-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(290px,1fr));gap:9px}}
.coin{{position:relative;background:var(--card2);border:1px solid var(--line);border-radius:14px;padding:12px;overflow:hidden;transition:border-color .2s,transform .2s}}
.coin.flash{{transform:translateY(-1px)}}
.coin.positive{{border-color:rgba(37,199,133,.28)}} .coin.negative{{border-color:rgba(255,91,103,.28)}} .coin.ready{{border-color:rgba(255,159,26,.88);box-shadow:0 0 0 1px rgba(255,159,26,.18),0 10px 28px rgba(255,159,26,.08)}}
.coin-top{{display:flex;justify-content:space-between;align-items:center;gap:8px}}
.symbol{{font-weight:850;font-size:14px;letter-spacing:.02em}}
.pill{{font-size:10px;font-weight:900;padding:5px 7px;border-radius:999px;letter-spacing:.06em}}
.pill.buy{{background:rgba(37,199,133,.13);color:var(--green)}} .pill.sell{{background:rgba(255,91,103,.13);color:var(--red)}} .pill.wait{{background:#242c36;color:#b6c0cd}} .pill.warm{{background:rgba(244,183,64,.13);color:var(--amber)}} .pill.ready{{background:rgba(255,159,26,.16);color:var(--orange);border:1px solid rgba(255,159,26,.35)}}
.score-row{{display:flex;align-items:flex-end;justify-content:space-between;gap:8px;margin-top:9px}}
.score{{font-size:28px;line-height:1;font-weight:900;font-variant-numeric:tabular-nums}}
.score.pos{{color:var(--green)}} .score.neg{{color:var(--red)}} .score.ready{{color:var(--orange)}} .score.zero{{color:#d6dde6}}
.to-trigger{{font-size:10px;color:var(--muted);text-align:right;line-height:1.3}}
.track{{height:5px;background:#242c36;border-radius:999px;margin:10px 0 8px;position:relative;overflow:hidden}}
.track:before{{content:'';position:absolute;left:50%;top:0;bottom:0;width:1px;background:#586474}}
.fill{{position:absolute;top:0;bottom:0;border-radius:999px}}
.fill.pos{{left:50%;background:var(--green)}} .fill.neg{{right:50%;background:var(--red)}} .fill.ready-pos{{left:50%;background:var(--orange)}} .fill.ready-neg{{right:50%;background:var(--orange)}}
.coin-meta{{display:grid;grid-template-columns:1fr 1fr;gap:5px;font-size:10px;color:var(--muted)}}
.coin-meta b{{color:#cbd4df;font-weight:700}}
.fresh{{margin-top:7px;font-size:9px;color:#697687}}
.controls{{display:flex;gap:7px;flex-wrap:nowrap;overflow-x:auto;margin-bottom:10px;position:sticky;top:0;z-index:8;background:rgba(11,14,19,.94);backdrop-filter:blur(10px);padding:8px 0;scrollbar-width:none}}
.filter{{border:1px solid var(--line);background:#111821;color:#aeb9c6;border-radius:999px;padding:7px 10px;font-size:11px;font-weight:750;cursor:pointer}}
.filter.active{{background:#232d39;color:#fff;border-color:#3b4859}}
.position-row{{display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center;padding:11px 0;border-bottom:1px solid var(--line)}} .position-row:last-child{{border-bottom:0}}
.position-title{{display:flex;gap:8px;align-items:center}}.position-side{{font-size:10px;color:var(--muted)}}.position-meta{{display:flex;gap:9px;flex-wrap:wrap;font-size:10px;color:var(--muted);margin-top:5px}}.position-meta b{{color:#d5dde7}}.mini-close{{border:1px solid #6c252d;background:#3a171b;color:#ff9ea5;border-radius:9px;padding:9px 11px;font-weight:900;font-size:10px}}
.countdown{{margin-top:10px;background:#0e151d;border:1px solid var(--line);border-radius:11px;padding:10px;font-size:12px}}
.countdown b{{float:right;color:var(--amber)}}
.form-grid{{display:grid;grid-template-columns:1fr 1fr;gap:9px}}
label{{font-size:11px;color:var(--muted);font-weight:700}}
select{{width:100%;margin-top:5px;padding:12px;border-radius:10px;background:#0d1218;color:#fff;border:1px solid #303a47;font-size:14px}}
button.action{{width:100%;padding:13px 14px;border:0;border-radius:11px;font-weight:850;cursor:pointer;margin-top:10px}}
button.open{{background:#e9eef5;color:#101318}} button.close{{background:#3a171b;color:#ff9ea5;border:1px solid #6c252d}}
button:disabled{{opacity:.4}}
.notice{{padding:11px;border-radius:11px;margin:10px 0;font-size:12px}} .ok{{background:#123220;color:#aee8c9}} .err{{background:#3e191d;color:#ffb6bc}}
.warn{{background:#171d25;border:1px solid var(--line);border-radius:11px;padding:10px;font-size:11px;color:#aeb8c5}}
.muted{{color:var(--muted)}} .empty-state{{color:var(--muted);font-size:12px;padding:5px 0}}
.footer-note{{font-size:10px;color:#677384;line-height:1.5;margin-top:9px}}
@media(max-width:900px){{
  .shell{{padding:13px 10px 36px}}
  h1{{font-size:22px}}
  .topbar{{align-items:center}}
  .subtitle{{font-size:11px}}
  .stats{{grid-template-columns:repeat(2,1fr)}}
  .research-grid{{grid-template-columns:1fr;gap:8px}}
  .coin{{padding:10px;border-radius:12px}}
  .score{{font-size:24px}}
  .coin-meta{{grid-template-columns:repeat(3,minmax(0,1fr));font-size:9px}}
  .to-trigger{{font-size:9px}}
  .thresholds{{gap:5px}}
  .threshold{{padding:8px 4px}}
  .form-grid{{grid-template-columns:1fr}}
  .section-head{{align-items:flex-start}}
  .coin{{padding:11px}}
  .score{{font-size:27px}}
  .coin-meta div:nth-child(3){{display:none}}
  .snapshot-note{{display:none}}
  .thresholds{{grid-template-columns:1fr}}
  .threshold{{display:flex;justify-content:space-between;align-items:center;text-align:left}}
  .threshold b{{font-size:11px}} .threshold span{{font-size:10px}}
  .stats{{gap:6px}} .stat{{padding:9px}} .stat .v{{font-size:17px}}
}}
@media(max-width:350px){{.research-grid{{grid-template-columns:1fr}}}}

.card-action{{margin-top:13px;padding-top:12px;border-top:1px solid var(--line)}}
.card-action-row{{display:grid;grid-template-columns:90px 1fr;gap:8px;align-items:center}}
.card-duration{{width:100%;border:1px solid var(--line);background:#0d1218;color:var(--text);border-radius:10px;padding:10px 8px;font-weight:800}}
.card-open{{width:100%;border-radius:10px;padding:11px 10px;font-weight:900;letter-spacing:.01em;cursor:pointer}}
.card-open.long{{background:rgba(37,199,133,.16);color:#49dfa2;border:1px solid rgba(37,199,133,.38)}}
.card-open.short{{background:rgba(255,91,103,.15);color:#ff7b84;border:1px solid rgba(255,91,103,.38)}} .card-open.ready{{background:rgba(255,159,26,.18);color:#ffb14d;border:1px solid rgba(255,159,26,.55)}}
.card-open:disabled{{opacity:.45;cursor:not-allowed}}
.snapshot-note{{font-size:10px;color:var(--muted);margin-top:6px;line-height:1.35}}

.inline-position{{margin-top:11px;padding:10px;border:1px solid rgba(111,168,255,.25);background:#101822;border-radius:11px}}
.inline-pos-top{{display:flex;justify-content:space-between;gap:8px;align-items:center}}
.inline-pos-title{{font-size:11px;font-weight:900;color:#b9d2ff;letter-spacing:.05em}}
.inline-pos-timer{{font-size:12px;font-weight:900;color:var(--amber);font-variant-numeric:tabular-nums}}
.inline-pos-meta{{display:grid;grid-template-columns:repeat(3,1fr);gap:5px;margin:8px 0;font-size:9px;color:var(--muted)}}
.inline-pos-meta b{{display:block;color:#dce6f2;font-size:10px;margin-top:2px}}
.inline-close{{width:100%;border:1px solid #74313a;background:#3d171c;color:#ff9aa3;border-radius:9px;padding:9px;font-size:10px;font-weight:900}}
.coin.open-position{{border-color:rgba(111,168,255,.45)}}
.market-fresh{{color:#7890a9}}


.decision-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:5px;margin:8px 0}}
.decision-box{{background:#10151c;border:1px solid var(--line);border-radius:9px;padding:7px;text-align:center}}
.decision-box span{{display:block;font-size:8px;color:var(--muted);text-transform:uppercase;letter-spacing:.05em}}
.decision-box b{{display:block;margin-top:2px;font-size:11px}}
.decision-box.pos b{{color:var(--green)}} .decision-box.neg b{{color:var(--red)}}
.coin.conflict{{border-color:rgba(244,183,64,.55)}}
.pill.conflict{{background:rgba(244,183,64,.13);color:var(--amber);border:1px solid rgba(244,183,64,.28)}}
.quality-row{{display:grid;grid-template-columns:repeat(3,1fr);gap:5px;margin-top:7px;font-size:9px;color:var(--muted)}}
.quality-row b{{display:block;color:#d7e0ea;font-size:10px;margin-top:2px}}
.sizing{{margin-top:10px;padding:9px;border:1px solid rgba(111,168,255,.20);background:#101720;border-radius:10px}}
.sizing-head{{display:flex;justify-content:space-between;gap:8px;align-items:center;font-size:10px;color:var(--muted)}}
.sizing-head b{{font-size:13px;color:#cfe0ff}}
.amount-row{{display:grid;grid-template-columns:92px 1fr;gap:7px;margin-top:7px}}
.amount-input{{width:100%;background:#0c1117;color:#fff;border:1px solid #344151;border-radius:9px;padding:10px;font-size:13px;font-weight:800}}
.details{{margin-top:8px;border-top:1px solid var(--line);padding-top:7px}}
.details summary{{font-size:9px;color:#8795a6;cursor:pointer}}
.detail-body{{font-size:9px;color:var(--muted);line-height:1.55;padding-top:6px}}
.detail-body b{{color:#d4dde7}}
.why-line{{margin-top:5px;white-space:normal;word-break:break-word}}
.hist.good{{color:var(--green)}} .hist.bad{{color:var(--red)}}
@media(max-width:900px){{
  .quality-row{{grid-template-columns:repeat(3,1fr)}}
  .decision-grid{{grid-template-columns:repeat(3,1fr)}}
  .amount-row{{grid-template-columns:84px 1fr}}
}}

</style>
</head>
<body>
<div class='shell'>
  <div class='topbar'>
    <div>
      <div class='eyebrow'>Bybit demo · manual approval</div>
      <h1>Trade Engine V0.4</h1>
      <p class='subtitle'>30-coin research · per-coin position timer · 3s dashboard sync</p>
    </div>
    <div class='live-dot' id='connectionState'>LIVE</div>
  </div>

  <div class='stats'>
    <div class='stat'><div class='k'>Tracked</div><div class='v' id='trackedCount'>{len(core.SYMBOLS)}</div></div>
    <div class='stat'><div class='k'>Orange</div><div class='v' id='readyCount'>{sum(1 for x in signals if abs(float(x['score'])) >= float(core.SIGNAL_THRESHOLD))}</div></div>
    <div class='stat'><div class='k'>Trigger</div><div class='v'>±{core.SIGNAL_THRESHOLD:g}</div></div>
    <div class='stat'><div class='k'>Open</div><div class='v'>{len(positions)}/{MAX_OPEN_LINEAR_POSITIONS}</div></div>
  </div>

  {msg}

  <div class='card'>
    <div class='section-head'>
      <div>
        <h2>Latest research</h2>
        <div class='footer-note'>Big number = V0.4 Decision Score. Best raw score stays in Details. Orange = threshold-ready. Suggested amount uses liquidity, agreement, persistence and matured historical labels.</div>
      </div>
      <div class='section-note'><span id='lastSync'>syncing…</span><br>screen poll 3s</div>
    </div>

    <div class='thresholds'>
      <div class='threshold sell'><b>SHORT BIAS</b><span>score &lt; 0</span></div>
      <div class='threshold ready'><b>ORANGE = READY</b><span>|score| ≥ {core.SIGNAL_THRESHOLD:g}</span></div>
      <div class='threshold buy'><b>LONG BIAS</b><span>score &gt; 0</span></div>
    </div>

    <div class='controls'>
      <button class='filter active' data-filter='ALL' type='button'>All</button>
      <button class='filter' data-filter='OPEN' type='button'>Open</button>
      <button class='filter' data-filter='READY' type='button'>Orange / Ready</button>
      <button class='filter' data-filter='LONG_BIAS' type='button'>Long bias</button>
      <button class='filter' data-filter='SHORT_BIAS' type='button'>Short bias</button>
      <button class='filter' data-filter='WARMING' type='button'>Warming</button>
      <select id='sortMode' class='filter' style='width:auto;margin:0;padding:7px 10px'>
        <option value='OPEN_FIRST'>Open first</option>
        <option value='CLOSEST'>Closest trigger</option>
        <option value='HIGH'>Score high</option>
        <option value='LOW'>Score low</option>
        <option value='SYMBOL'>Symbol A-Z</option>
      </select>
    </div>
    <div class='research-grid' id='researchGrid'></div>
  </div>

  <details class='card'>
    <summary><b>Open positions summary</b> · {len(positions)}/{MAX_OPEN_LINEAR_POSITIONS}</summary>
    <div style='margin-top:10px'>{pos}</div>
  </details>

  <details class='card'><summary><b>Advanced close all</b></summary>
    <div class='section-head' style='margin-top:12px'><h2>Close positions</h2><div class='section-note'>reduce-only</div></div>
    <form method='post' action='/trade/close' onsubmit='freshSubmit(event);return false;'>
      <input type='hidden' name='nonce' value=''>
      <button class='action close' {disabled} type='submit'>Approve & Close All Open Positions</button>
    </form>
    <div class='footer-note'>Never automatic. Backend re-reads and validates all open positions before closing.</div>
  </details>
</div>

<script>
const INITIAL={initial_payload};
const CARD_OPEN_DISABLED={'false' if live.ready else 'true'};
const cardDuration={{}};
const cardAmount={{}};
const cardAmountTouched={{}};
let activeFilter='ALL';
let latestPayload=INITIAL;

function esc(v){{
  return String(v ?? '').replace(/[&<>"']/g, ch => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}})[ch]);
}}
function uiSignal(side){{
  if(side==='LONG') return 'BUY';
  if(side==='SHORT') return 'SELL';
  return side==='WAIT' ? 'WAIT' : 'WARMING';
}}
function triggerText(score, threshold){{
  if(score >= threshold) return 'BUY trigger active';
  if(score <= -threshold) return 'SELL trigger active';
  if(score > 0) return (threshold-score).toFixed(1)+' pts to BUY';
  if(score < 0) return (threshold-Math.abs(score)).toFixed(1)+' pts to SELL';
  return threshold.toFixed(0)+' pts to trigger';
}}
function render(payload){{
  latestPayload=payload;
  const threshold=Number(payload.threshold || {float(core.SIGNAL_THRESHOLD)});
  const map=new Map((payload.signals||[]).map(s=>[s.symbol,s]));
  const symbols=payload.symbols||[];
  document.getElementById('trackedCount').textContent=symbols.length;
  const rows=symbols.map(sym=>{{
    const s=map.get(sym);
    if(!s) return {{symbol:sym, ui:'WARMING', score:null}};
    return {{...s, ui:uiSignal(s.side)}};
  }});

  document.getElementById('readyCount').textContent=rows.filter(r=>r.score!==null && Math.abs(Number(r.score))>=threshold).length;

  const positions=payload.positions||latestPayload.positions||INITIAL.positions||[];
  const positionMap=new Map(positions.map(p=>[p.symbol,p]));
  const openSet=new Set(positions.map(p=>p.symbol));
  let filtered=rows.filter(r=>{{
    if(activeFilter==='ALL') return true;
    if(activeFilter==='OPEN') return openSet.has(r.symbol);
    if(activeFilter==='WARMING') return r.score===null;
    if(r.score===null) return false;
    const s=Number(r.score);
    if(activeFilter==='READY') return Math.abs(s)>=threshold && !r.conflict;
    if(activeFilter==='CONFLICT') return !!r.conflict;
    if(activeFilter==='LONG_BIAS') return s>0;
    if(activeFilter==='SHORT_BIAS') return s<0;
    return true;
  }});
  const sortMode=document.getElementById('sortMode')?.value||'CLOSEST';
  filtered=[...filtered].sort((a,b)=>{{
    if(a.score===null && b.score!==null) return 1;
    if(b.score===null && a.score!==null) return -1;
    if(sortMode==='OPEN_FIRST'){{
      const ao=openSet.has(a.symbol)?0:1, bo=openSet.has(b.symbol)?0:1;
      if(ao!==bo) return ao-bo;
    }}
    if(sortMode==='SYMBOL') return String(a.symbol).localeCompare(String(b.symbol));
    if(sortMode==='HIGH') return Number(b.score||-999)-Number(a.score||-999);
    if(sortMode==='LOW') return Number(a.score||999)-Number(b.score||999);
    return Math.abs(threshold-Math.abs(Number(a.score||0))) - Math.abs(threshold-Math.abs(Number(b.score||0)));
  }});
  const now=Date.now();
  document.getElementById('researchGrid').innerHTML=filtered.map(r=>{{
    if(r.score===null){{
      return `<div class="coin" data-signal="WARMING">
        <div class="coin-top"><div class="symbol">${{esc(r.symbol)}}</div><span class="pill warm">WARMING</span></div>
        <div class="score-row"><div class="score zero">—</div><div class="to-trigger">collecting history</div></div>
        <div class="track"></div>
        <div class="coin-meta"><div>Best <b>pending</b></div><div>Avg <b>pending</b></div></div>
        <div class="fresh">needs at least the first 3m window</div>
      </div>`;
    }}
    const score=Number(r.decision_score ?? r.score);
    const best=Number(r.best_score ?? score);
    const consensus=Number(r.consensus_score ?? score);
    const isReady=Math.abs(score)>=threshold && !r.conflict;
    const cls=r.conflict?'conflict':isReady?'ready':score>0?'positive':score<0?'negative':'';
    const scoreCls=isReady?'ready':score>0?'pos':score<0?'neg':'zero';
    const pillCls=r.conflict?'conflict':isReady?'ready':'wait';
    const pillText=r.conflict?'CONFLICT':isReady?(score>0?'READY LONG':'READY SHORT'):'WAIT';
    const width=Math.min(50, Math.abs(score)/2);
    const fill=score>0
      ? `<span class="fill ${{isReady?'ready-pos':'pos'}}" style="width:${{width}}%"></span>`
      : score<0
      ? `<span class="fill ${{isReady?'ready-neg':'neg'}}" style="width:${{width}}%"></span>` : '';
    const age=Math.max(0,Math.round((now-Number(r.ts_ms))/1000));
    const marketAge=r.market_ts_ms ? Math.max(0,Math.round((now-Number(r.market_ts_ms))/1000)) : null;
    const p=positionMap.get(r.symbol);
    const coverage=`${{esc(r.window_count ?? 0)}}/${{esc(r.window_total ?? 12)}}`;
    const dur=cardDuration[r.symbol]||'600';
    const suggestion=(r.suggested_notional||{{}})[dur] ?? 50000;
    if(cardAmount[r.symbol]===undefined || !cardAmountTouched[r.symbol]) cardAmount[r.symbol]=suggestion;
    const hist=(r.history||{{}})[dur]||{{n:0,hit_rate:null,avg_move_pct:null}};
    const histText=hist.hit_rate===null ? `warming N${{hist.n||0}}` : `${{(Number(hist.hit_rate)*100).toFixed(0)}}% · N${{hist.n}}`;
    const histCls=hist.hit_rate===null?'':Number(hist.hit_rate)>=0.55?'good':Number(hist.hit_rate)<0.45?'bad':'';
    const pct=v=>v===null||v===undefined?'—':(Number(v)>0?'+':'')+Number(v).toFixed(1);
    const reasons=r.reasons||{{}};
    const why=`Mom ${{pct(reasons.momentum)}} · Flow ${{pct(reasons.orderflow)}} · Book ${{pct(reasons.orderbook)}} · BTC ${{pct(reasons.btc_context)}} · OI ${{pct(reasons.oi_confirmation)}}`;

    const actionHtml=p ? `
      <div class="inline-position">
        <div class="inline-pos-top">
          <div class="inline-pos-title">OPEN ${{p.side==='Buy'?'LONG':'SHORT'}} · ${{esc(p.leverage)}}x</div>
          <div class="inline-pos-timer pos-timer" data-due="${{esc(p.due_ms||'')}}">—</div>
        </div>
        <div class="inline-pos-meta">
          <div>PnL<b>${{esc(p.unrealised_pnl)}}</b></div>
          <div>Notional<b>${{p.entry_notional_usdt?Number(p.entry_notional_usdt).toLocaleString():'—'}}</b></div>
          <div>Avg<b>${{esc(p.avg_price)}}</b></div>
          <div>Mark<b>${{esc(p.mark_price)}}</b></div>
          <div>Entry score<b>${{p.entry_score===null||p.entry_score===undefined?'—':Number(p.entry_score).toFixed(2)}}</b></div>
          <div>Window<b>${{p.entry_window_min?esc(p.entry_window_min)+'m':'—'}}</b></div>
        </div>
        <form method="post" action="/trade/close-one" onsubmit="freshSubmit(event);return false;">
          <input type="hidden" name="nonce" value="">
          <input type="hidden" name="symbol" value="${{esc(r.symbol)}}">
          <button class="inline-close" type="submit">CLOSE ${{esc(r.symbol)}} · REDUCE ONLY</button>
        </form>
      </div>` : `
      <div class="sizing">
        <div class="sizing-head"><span>Suggested amount</span><b>$${{Number(suggestion).toLocaleString()}}</b></div>
        <div class="amount-row">
          <select class="card-duration"
            onchange="cardDuration['${{esc(r.symbol)}}']=this.value;cardAmountTouched['${{esc(r.symbol)}}']=false;render(latestPayload)">
            <option value="300" ${{dur==='300'?'selected':''}}>5m</option>
            <option value="600" ${{dur==='600'?'selected':''}}>10m</option>
            <option value="900" ${{dur==='900'?'selected':''}}>15m</option>
          </select>
          <input class="amount-input" type="number" min="1000" max="50000" step="500"
            value="${{esc(cardAmount[r.symbol])}}"
            oninput="cardAmount['${{esc(r.symbol)}}']=this.value;cardAmountTouched['${{esc(r.symbol)}}']=true">
        </div>
        <form id="open-${{esc(r.symbol)}}" class="card-action" method="post" action="/trade/open" onsubmit="freshSubmit(event);return false;">
          <input type="hidden" name="nonce" value="">
          <input type="hidden" name="symbol" value="${{esc(r.symbol)}}">
          <input type="hidden" name="duration" value="${{esc(dur)}}">
          <input type="hidden" name="notional" value="${{esc(cardAmount[r.symbol])}}">
          <input type="hidden" name="expected_ts" value="${{esc(r.ts_ms)}}">
          <input type="hidden" name="expected_direction" value="${{score>=0?'Buy':'Sell'}}">
          <button class="card-open ${{isReady?'ready':score>=0?'long':'short'}}" type="submit"
            ${{CARD_OPEN_DISABLED?'disabled':''}}
            onclick="this.form.querySelector('input[name=duration]').value=cardDuration['${{esc(r.symbol)}}']||'600';this.form.querySelector('input[name=notional]').value=cardAmount['${{esc(r.symbol)}}']||${{suggestion}}">
            ${{isReady?'OPEN':'TEST'}} ${{score>=0?'LONG':'SHORT'}} · $${{Number(cardAmount[r.symbol]||suggestion).toLocaleString()}}
          </button>
        </form>
        <div class="snapshot-note">Manual amount allowed $1,000–$50,000. Snapshot + direction are locked at approval.</div>
      </div>`;
    return `<div class="coin ${{cls}} ${{p?'open-position':''}}" data-signal="${{r.ui}}">
      <div class="coin-top"><div class="symbol">${{esc(r.symbol)}}</div><span class="pill ${{pillCls}}">${{pillText}}</span></div>
      <div class="score-row"><div class="score ${{scoreCls}}">${{score>0?'+':''}}${{score.toFixed(2)}}</div><div class="to-trigger">${{r.conflict?'short/long horizons disagree':triggerText(score,threshold)}}</div></div>
      <div class="track">${{fill}}</div>
      <div class="decision-grid">
        <div class="decision-box ${{Number(r.short_score||0)>=0?'pos':'neg'}}"><span>3–5m</span><b>${{pct(r.short_score)}}</b></div>
        <div class="decision-box ${{Number(r.mid_score||0)>=0?'pos':'neg'}}"><span>6–12m</span><b>${{pct(r.mid_score)}}</b></div>
        <div class="decision-box ${{Number(r.long_score||0)>=0?'pos':'neg'}}"><span>15–30m</span><b>${{pct(r.long_score)}}</b></div>
      </div>
      <div class="quality-row">
        <div>Agreement<b>${{Math.round(Number(r.agreement||0)*100)}}%</b></div>
        <div>Persistence<b>${{Math.round(Number(r.persistence||0)*100)}}%</b></div>
        <div>History<b class="hist ${{histCls}}">${{histText}}</b></div>
      </div>
      <div class="fresh">Signal ${{age}}s · <span class="market-fresh">Market ${{marketAge===null?'—':marketAge+'s'}}</span> · Coverage ${{coverage}}</div>
      ${{actionHtml}}
      <details class="details">
        <summary>Details / why</summary>
        <div class="detail-body">
          Best raw <b>${{best>0?'+':''}}${{best.toFixed(2)}} @ ${{esc(r.best_window_min)}}m</b> ·
          Avg <b>${{consensus>0?'+':''}}${{consensus.toFixed(2)}}</b> ·
          Spread <b>${{r.spread_bps===null||r.spread_bps===undefined?'—':Number(r.spread_bps).toFixed(2)+' bps'}}</b><br>
          5m turnover <b>$${{Number(r.turnover_5m||0).toLocaleString(undefined,{{maximumFractionDigits:0}})}}</b>
          <div class="why-line">${{why}}</div>
          ${{hist.avg_move_pct===null||hist.avg_move_pct===undefined?'':`Similar horizon samples avg directional move <b>${{Number(hist.avg_move_pct).toFixed(3)}}%</b>.`}}
        </div>
      </details>
    </div>`;
  }}).join('') || `<div class="empty-state">No coins in this filter.</div>`;

  const ready=rows.filter(r=>r.score!==null);
  if(ready.length){{
    const strongest=ready.reduce((a,b)=>Math.abs(Number(b.decision_score??b.score))>Math.abs(Number(a.decision_score??a.score))?b:a);
    const sig=uiSignal(strongest.side);
    const direction=Number(strongest.decision_score??strongest.score)>=0?'Buy':'Sell';
    const sb=document.getElementById('strongestBanner'); if(sb) sb.innerHTML=
      `Strongest now: <b>${{esc(strongest.symbol)}}</b> / ${{direction}} · decision <b>${{Number(strongest.decision_score??strongest.score).toFixed(2)}}</b> · `+
      `${{sig==='BUY'||sig==='SELL' ? sig+' MODEL SIGNAL' : 'WAIT — execution test only'}}. `+
      `Trigger is ±${{threshold}}.`;
  }}
  document.getElementById('lastSync').textContent='synced '+new Date().toLocaleTimeString();
}}
async function freshSubmit(ev){{
  const form=ev.currentTarget;
  const btn=form.querySelector('button[type="submit"]');
  if(btn){{btn.disabled=true;btn.dataset.old=btn.textContent;btn.textContent='CHECKING…';}}
  try{{
    const r=await fetch('/trade/nonce?_='+Date.now(),{{cache:'no-store'}});
    if(!r.ok) throw new Error('nonce HTTP '+r.status);
    const p=await r.json();
    const n=form.querySelector('input[name="nonce"]');
    if(!n) throw new Error('nonce field missing');
    n.value=p.nonce;
    form.submit();
  }}catch(e){{
    if(btn){{btn.disabled=false;btn.textContent=btn.dataset.old||'TRY AGAIN';}}
    alert('Approval refresh failed. No order sent.');
  }}
}}
function timerText(due){{
  if(!due) return 'MANUAL';
  const s=Math.ceil((Number(due)-Date.now())/1000);
  return s>0 ? Math.floor(s/60)+'m '+String(s%60).padStart(2,'0')+'s' : 'READY TO CLOSE';
}}
function updatePositionTimers(){{
  document.querySelectorAll('.pos-timer').forEach(el=>{{
    el.textContent=timerText(el.dataset.due);
  }});
}}
async function pollDashboard(){{
  try{{
    const stamp=Date.now();
    const [sr,pr]=await Promise.all([
      fetch('/signals?_='+stamp,{{cache:'no-store'}}),
      fetch('/trade/state?_='+stamp,{{cache:'no-store'}})
    ]);
    if(!sr.ok || !pr.ok) throw new Error('dashboard HTTP '+sr.status+'/'+pr.status);
    const s=await sr.json();
    const ps=await pr.json();
    s.positions=ps.positions||[];
    render(s);
    updatePositionTimers();
    document.getElementById('connectionState').textContent='LIVE';
  }}catch(e){{
    document.getElementById('connectionState').textContent='RETRY';
  }}
}}
document.querySelectorAll('button.filter').forEach(btn=>btn.addEventListener('click',()=>{{
  document.querySelectorAll('button.filter').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  activeFilter=btn.dataset.filter;
  render(latestPayload);
}}));
document.getElementById('sortMode')?.addEventListener('change',()=>render(latestPayload));
render(INITIAL);
setInterval(pollDashboard,3000);
setInterval(updatePositionTimers,1000);

updatePositionTimers();
</script>
</body>
</html>"""
            self.send_page(text)

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            if path in ("/", "/health"):
                self.send_json(self.health())
            elif path == "/signals":
                self.send_json(
                    {
                        "version": "0.4.0-demo",
                        "server_ms": core.now_ms(),
                        "symbols": core.SYMBOLS,
                        "threshold": float(core.SIGNAL_THRESHOLD),
                        "engine_interval_sec": int(core.FEATURE_INTERVAL_SEC),
                        "window_total": len(core.FEATURE_WINDOWS_MIN),
                        "signals": latest_signals(db),
                    }
                )
            elif path == "/trade/state":
                if self.require_auth():
                    try:
                        self.send_json(
                            {
                                "server_ms": core.now_ms(),
                                "positions": live.position_states(),
                            }
                        )
                    except Exception as exc:
                        self.send_json({"error": str(exc)[:300], "positions": []}, 503)
            elif path == "/trade/nonce":
                if self.require_auth():
                    self.send_json({"nonce": live.issue_nonce()})
            elif path == "/trade":
                if self.require_auth():
                    self.trade_page()
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            path = urllib.parse.urlparse(self.path).path
            if path not in {"/trade/open", "/trade/close", "/trade/close-one"}:
                self.send_response(404)
                self.end_headers()
                return

            if not self.require_auth():
                return

            form = self.form()
            try:
                live.consume_nonce(form.get("nonce", ""))

                if path == "/trade/open":
                    symbol = form.get("symbol", "").upper()
                    duration = int(
                        form.get(
                            "duration",
                            str(DEFAULT_TEST_DURATION_SEC),
                        )
                    )
                    result = live.open(
                        symbol,
                        duration,
                        "test",
                        expected_ts=form.get("expected_ts") or None,
                        expected_direction=form.get("expected_direction") or None,
                        requested_notional=form.get("notional") or None,
                    )
                    self.trade_page(
                        f"OPEN submitted: {result['symbol']} "
                        f"{result['side']} qty={result['qty']} "
                        f"≈{result['notional']:.4f} USDT. "
                        f"Order ID {result['order_id'] or 'pending'}."
                    )
                elif path == "/trade/close-one":
                    symbol = form.get("symbol", "").upper()
                    result = live.close_symbol(symbol)
                    self.trade_page(
                        f"CLOSE submitted: {result['symbol']} "
                        f"reduce-only qty={result['size']}."
                    )
                else:
                    result = live.close()
                    self.trade_page(
                        f"CLOSE ALL submitted: {result['count']} "
                        f"reduce-only order(s)."
                    )
            except Exception as exc:
                log.error(
                    "Manual approval action failed path=%s: %s",
                    path,
                    str(exc)[:500],
                )
                self.trade_page(str(exc), True)

        def log_message(self, fmt, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", core.PORT), Handler)
    Thread(target=server.serve_forever, daemon=True).start()
    log.info(
        "V0.3 HTTP endpoint on 0.0.0.0:%s /health /signals /trade",
        core.PORT,
    )


async def run():
    if MANUAL_LIVE_APPROVAL_ENABLED and "api-demo.bybit.com" not in core.BYBIT_REST_URL:
        raise RuntimeError("V0.4 demo guard: manual approval requires api-demo.bybit.com")
    if DEMO_LEVERAGE != 20:
        raise RuntimeError("V0.4 demo guard: DEMO_LEVERAGE must be 20")
    if MAX_LIVE_NOTIONAL_USDT != Decimal("50000"):
        raise RuntimeError("V0.4 demo guard: MAX_LIVE_NOTIONAL_USDT must be 50000")
    if MAX_OPEN_LINEAR_POSITIONS != 10:
        raise RuntimeError("V0.4 demo guard: MAX_OPEN_LINEAR_POSITIONS must be 10")
    db = core.Database(core.DB_PATH)
    collector = core.Collector(db)
    account = core.AccountMonitor(db)
    features = SafeFeatureEngine(db)
    live = LiveApproval(db)

    start_server(collector, db, account, live)

    log.info("trade-engine v0.4.0-demo")
    log.info("AUTO_LIVE_ORDERS_ENABLED=False")
    log.info(
        "MANUAL_LIVE_APPROVAL_ENABLED=%s approval_pin_configured=%s "
        "max_notional=%s leverage=%sx max_open=%s demo_only=%s",
        MANUAL_LIVE_APPROVAL_ENABLED,
        bool(APPROVAL_PIN),
        MAX_LIVE_NOTIONAL_USDT,
        DEMO_LEVERAGE,
        MAX_OPEN_LINEAR_POSITIONS,
        "api-demo.bybit.com" in core.BYBIT_REST_URL,
    )

    await asyncio.gather(
        collector.run_forever(),
        core.feature_loop(features),
        core.account_loop(account),
    )


if __name__ == "__main__":
    asyncio.run(run())
