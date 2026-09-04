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
                "User-Agent": "trade-engine-v0.3.3-demo",
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
        req = urllib.request.Request(url, headers={"User-Agent": "trade-engine-v0.3.3-demo"})
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


def latest_signals(db):
    out = []
    for symbol in core.SYMBOLS:
        row = db.one(
            """SELECT ts_ms,best_window_min,score,side,price,reason_json,status
               FROM signals WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1""",
            (symbol,),
        )
        if not row:
            continue

        window_scores = db.query(
            """SELECT window_min,research_score
               FROM features
               WHERE symbol=? AND ts_ms=?
               ORDER BY window_min""",
            (symbol, row[0]),
        )
        scores = [float(r[1]) for r in window_scores if r[1] is not None]
        consensus = (sum(scores) / len(scores)) if scores else float(row[2])

        out.append(
            {
                "symbol": symbol,
                "ts_ms": row[0],
                "best_window_min": row[1],
                "score": float(row[2]),
                "consensus_score": round(consensus, 2),
                "window_count": len(scores),
                "side": row[3],
                "price": float(row[4]),
                "reasons": json.loads(row[5] or "{}"),
                "status": row[6],
            }
        )
    return out


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
        row = self.db.one(
            """SELECT ts_ms,best_window_min,score,side,price,status
               FROM signals WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1""",
            (symbol,),
        )
        if not row:
            return None
        return {
            "ts_ms": row[0],
            "best_window_min": row[1],
            "score": float(row[2]),
            "side": row[3],
            "price": float(row[4]),
            "status": row[5],
        }

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
        step = dec(lot.get("qtyStep"), Decimal("0")) or Decimal("0")
        min_qty = dec(lot.get("minOrderQty"), Decimal("0")) or Decimal("0")
        min_notional = dec(lot.get("minNotionalValue"), Decimal("0")) or Decimal("0")
        price = Decimal(str(sig["price"]))
        qty = floor_step(notional / price, step)
        actual = qty * price
        if qty < min_qty:
            qty = min_qty
            actual = qty * price
        if min_notional and actual < min_notional:
            qty = ceil_step(min_notional / price, step)
            actual = qty * price
        if actual > MAX_LIVE_NOTIONAL_USDT + Decimal("0.02"):
            raise RuntimeError(
                f"{symbol} minimum order is about {actual:.4f} USDT, above the "
                f"${float(MAX_LIVE_NOTIONAL_USDT + Decimal('0.02')):,.2f} safety cap."
            )
        return sig, dtxt(qty), actual

    def open(self, symbol, duration, mode="test"):
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

            sig, qty, actual = self.qty_for_notional(
                symbol, MAX_LIVE_NOTIONAL_USDT
            )
            direction = "Buy" if sig["score"] >= 0 else "Sell"

            if mode == "model":
                expected = (
                    "Buy"
                    if sig["side"] == "LONG"
                    else "Sell"
                    if sig["side"] == "SHORT"
                    else None
                )
                if expected is None or abs(sig["score"]) < core.SIGNAL_THRESHOLD:
                    raise RuntimeError("No threshold-qualified model signal")
                direction = expected

            self.client.set_leverage_demo(symbol)
            result = self.client.market_order(symbol, direction, qty, False)
            oid = result.get("result", {}).get("orderId")

            self.db.execute(
                """INSERT INTO live_trade_events
                (ts_ms,event_type,symbol,side,qty,notional_usdt,duration_sec,
                 signal_score,best_window_min,order_id,status,note)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    core.now_ms(),
                    "OPEN",
                    symbol,
                    direction,
                    qty,
                    float(actual),
                    duration,
                    sig["score"],
                    sig["best_window_min"],
                    oid,
                    "SUBMITTED",
                    mode,
                ),
            )
            log.warning(
                "MANUAL LIVE OPEN approved symbol=%s side=%s qty=%s notional≈%.4f duration=%ss",
                symbol,
                direction,
                qty,
                float(actual),
                duration,
            )
            return {
                "symbol": symbol,
                "side": direction,
                "qty": qty,
                "notional": float(actual),
                "order_id": oid,
                "signal": sig,
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
                        "Hedge mode is not supported in V0.3.3"
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
                "script-src 'unsafe-inline'; form-action 'self'; "
                "frame-ancestors 'none'",
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
                "version": "0.3.3-demo",
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
                    live.positions()
                    if live.client.configured
                    else []
                )
                pos_error = ""
            except Exception as exc:
                positions, pos_error = [], str(exc)

            nonce_open = live.issue_nonce()
            nonce_close = live.issue_nonce()

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
                        f"<div><b>{html.escape(p['symbol'])}</b> "
                        f"<span class='muted'>{html.escape(str(p['side']))}</span></div>"
                        f"<div class='position-meta'>qty {html.escape(str(p['size']))}"
                        f" · avg {html.escape(str(p['avg_price']))}"
                        f" · mark {html.escape(str(p['mark_price']))}"
                        f" · {html.escape(str(p['leverage']))}x"
                        f" · PnL {html.escape(str(p['unrealised_pnl']))}</div>"
                        "</div>"
                    )
                    for p in positions
                )
            else:
                pos = "<div class='empty-state'>No open linear position.</div>"

            if pos_error:
                pos += f"<div class='err'>{html.escape(pos_error)}</div>"

            plan = live.current_plan()
            due_html = ""
            if plan and positions and plan[4]:
                due_ms = int(plan[0]) + int(plan[4]) * 1000
                due_html = (
                    f"<div class='countdown'>Global timer "
                    f"<b id='cd' data-due='{due_ms}'>calculating…</b></div>"
                )

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
                    "signals": signals,
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
:root{{--bg:#0b0e13;--card:#12171f;--card2:#171d26;--line:#27303c;--text:#f3f6f9;--muted:#8f9bab;--green:#25c785;--red:#ff5b67;--amber:#f4b740;--blue:#6fa8ff}}
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
.threshold.sell b{{color:var(--red)}} .threshold.buy b{{color:var(--green)}}
.research-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:9px}}
.coin{{position:relative;background:var(--card2);border:1px solid var(--line);border-radius:14px;padding:12px;overflow:hidden;transition:border-color .2s,transform .2s}}
.coin.flash{{transform:translateY(-1px)}}
.coin.positive{{border-color:rgba(37,199,133,.35)}} .coin.negative{{border-color:rgba(255,91,103,.35)}}
.coin-top{{display:flex;justify-content:space-between;align-items:center;gap:8px}}
.symbol{{font-weight:850;font-size:14px;letter-spacing:.02em}}
.pill{{font-size:10px;font-weight:900;padding:5px 7px;border-radius:999px;letter-spacing:.06em}}
.pill.buy{{background:rgba(37,199,133,.13);color:var(--green)}} .pill.sell{{background:rgba(255,91,103,.13);color:var(--red)}} .pill.wait{{background:#242c36;color:#b6c0cd}} .pill.warm{{background:rgba(244,183,64,.13);color:var(--amber)}}
.score-row{{display:flex;align-items:flex-end;justify-content:space-between;gap:8px;margin-top:9px}}
.score{{font-size:28px;line-height:1;font-weight:900;font-variant-numeric:tabular-nums}}
.score.pos{{color:var(--green)}} .score.neg{{color:var(--red)}} .score.zero{{color:#d6dde6}}
.to-trigger{{font-size:10px;color:var(--muted);text-align:right;line-height:1.3}}
.track{{height:5px;background:#242c36;border-radius:999px;margin:10px 0 8px;position:relative;overflow:hidden}}
.track:before{{content:'';position:absolute;left:50%;top:0;bottom:0;width:1px;background:#586474}}
.fill{{position:absolute;top:0;bottom:0;border-radius:999px}}
.fill.pos{{left:50%;background:var(--green)}} .fill.neg{{right:50%;background:var(--red)}}
.coin-meta{{display:grid;grid-template-columns:1fr 1fr;gap:5px;font-size:10px;color:var(--muted)}}
.coin-meta b{{color:#cbd4df;font-weight:700}}
.fresh{{margin-top:7px;font-size:9px;color:#697687}}
.controls{{display:flex;gap:7px;flex-wrap:wrap;margin-bottom:10px}}
.filter{{border:1px solid var(--line);background:#111821;color:#aeb9c6;border-radius:999px;padding:7px 10px;font-size:11px;font-weight:750;cursor:pointer}}
.filter.active{{background:#232d39;color:#fff;border-color:#3b4859}}
.position-row{{padding:10px 0;border-bottom:1px solid var(--line)}} .position-row:last-child{{border-bottom:0}}
.position-meta{{font-size:11px;color:var(--muted);margin-top:4px;overflow-wrap:anywhere}}
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
@media(max-width:680px){{
  .shell{{padding:13px 10px 36px}}
  h1{{font-size:22px}}
  .topbar{{align-items:center}}
  .subtitle{{font-size:11px}}
  .stats{{grid-template-columns:repeat(2,1fr)}}
  .research-grid{{grid-template-columns:repeat(2,minmax(0,1fr));gap:7px}}
  .coin{{padding:10px;border-radius:12px}}
  .score{{font-size:24px}}
  .coin-meta{{grid-template-columns:1fr;font-size:9px}}
  .to-trigger{{font-size:9px}}
  .thresholds{{gap:5px}}
  .threshold{{padding:8px 4px}}
  .form-grid{{grid-template-columns:1fr}}
  .section-head{{align-items:flex-start}}
}}
@media(max-width:350px){{.research-grid{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class='shell'>
  <div class='topbar'>
    <div>
      <div class='eyebrow'>Bybit demo · manual approval</div>
      <h1>Trade Engine V0.3.3</h1>
      <p class='subtitle'>Live research control center · screen updates without refresh</p>
    </div>
    <div class='live-dot' id='connectionState'>LIVE</div>
  </div>

  <div class='stats'>
    <div class='stat'><div class='k'>Tracked</div><div class='v' id='trackedCount'>{len(core.SYMBOLS)}</div></div>
    <div class='stat'><div class='k'>Ready</div><div class='v' id='readyCount'>{len(signals)}</div></div>
    <div class='stat'><div class='k'>Trigger</div><div class='v'>±{core.SIGNAL_THRESHOLD:g}</div></div>
    <div class='stat'><div class='k'>Engine</div><div class='v'>{int(core.FEATURE_INTERVAL_SEC)}s</div></div>
  </div>

  {msg}

  <div class='card'>
    <div class='section-head'>
      <div>
        <h2>Latest research</h2>
        <div class='footer-note'>Big number = strongest window score. Avg = consensus across available windows. Score is directional conviction, not win probability.</div>
      </div>
      <div class='section-note'><span id='lastSync'>syncing…</span><br>screen poll 3s</div>
    </div>

    <div class='thresholds'>
      <div class='threshold sell'><b>SELL / SHORT</b><span>score ≤ -{core.SIGNAL_THRESHOLD:g}</span></div>
      <div class='threshold'><b>WAIT</b><span>-{core.SIGNAL_THRESHOLD:g} &lt; score &lt; +{core.SIGNAL_THRESHOLD:g}</span></div>
      <div class='threshold buy'><b>BUY / LONG</b><span>score ≥ +{core.SIGNAL_THRESHOLD:g}</span></div>
    </div>

    <div class='controls'>
      <button class='filter active' data-filter='ALL' type='button'>All</button>
      <button class='filter' data-filter='BUY' type='button'>Buy</button>
      <button class='filter' data-filter='SELL' type='button'>Sell</button>
      <button class='filter' data-filter='WAIT' type='button'>Wait</button>
      <button class='filter' data-filter='WARMING' type='button'>Warming</button>
    </div>
    <div class='research-grid' id='researchGrid'></div>
  </div>

  <div class='card'>
    <div class='section-head'><h2>Current positions</h2><div class='section-note'>max {MAX_OPEN_LINEAR_POSITIONS}</div></div>
    {pos}
    {due_html}
  </div>

  <div class='card'>
    <div class='section-head'><h2>Approve open</h2><div class='section-note'>${float(MAX_LIVE_NOTIONAL_USDT):,.0f} · {DEMO_LEVERAGE}x demo</div></div>
    <div class='warn' id='strongestBanner'>
      Strongest now: <b>{html.escape(symbol)}</b> / {html.escape(side)} — {qualification}.
      Model trigger requires ±{core.SIGNAL_THRESHOLD:g}; test mode can still execute below threshold and follows score sign.
    </div>
    <form method='post' action='/trade/open'>
      <input type='hidden' name='nonce' value='{html.escape(nonce_open)}'>
      <div class='form-grid'>
        <label>Symbol<select name='symbol'>{options}</select></label>
        <label>Duration<select name='duration'>
          <option value='300'>5 min</option>
          <option value='600'>10 min</option>
          <option value='900'>15 min</option>
        </select></label>
      </div>
      <button class='action open' {disabled} type='submit'>Approve & Open Demo</button>
    </form>
    <div class='footer-note'>Each successful OPEN resets the global countdown. Same-symbol one-way orders can merge on Bybit.</div>
  </div>

  <div class='card'>
    <div class='section-head'><h2>Close positions</h2><div class='section-note'>reduce-only</div></div>
    <form method='post' action='/trade/close'>
      <input type='hidden' name='nonce' value='{html.escape(nonce_close)}'>
      <button class='action close' {disabled} type='submit'>Approve & Close All Open Positions</button>
    </form>
    <div class='footer-note'>Never automatic. Backend re-reads and validates all open positions before closing.</div>
  </div>
</div>

<script>
const INITIAL={initial_payload};
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
  document.getElementById('readyCount').textContent=map.size;

  const rows=symbols.map(sym=>{{
    const s=map.get(sym);
    if(!s) return {{symbol:sym, ui:'WARMING', score:null}};
    return {{...s, ui:uiSignal(s.side)}};
  }});

  const filtered=rows.filter(r=>activeFilter==='ALL' || r.ui===activeFilter);
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
    const score=Number(r.score);
    const consensus=Number(r.consensus_score ?? score);
    const cls=score>0?'positive':score<0?'negative':'';
    const scoreCls=score>0?'pos':score<0?'neg':'zero';
    const pillCls=r.ui==='BUY'?'buy':r.ui==='SELL'?'sell':'wait';
    const width=Math.min(50, Math.abs(score)/2);
    const fill=score>0
      ? `<span class="fill pos" style="width:${{width}}%"></span>`
      : score<0
      ? `<span class="fill neg" style="width:${{width}}%"></span>` : '';
    const age=Math.max(0,Math.round((now-Number(r.ts_ms))/1000));
    return `<div class="coin ${{cls}}" data-signal="${{r.ui}}">
      <div class="coin-top"><div class="symbol">${{esc(r.symbol)}}</div><span class="pill ${{pillCls}}">${{r.ui}}</span></div>
      <div class="score-row"><div class="score ${{scoreCls}}">${{score>0?'+':''}}${{score.toFixed(2)}}</div><div class="to-trigger">${{triggerText(score,threshold)}}</div></div>
      <div class="track">${{fill}}</div>
      <div class="coin-meta">
        <div>Best <b>${{esc(r.best_window_min)}}m</b></div>
        <div>Avg <b>${{consensus>0?'+':''}}${{consensus.toFixed(2)}}</b></div>
        <div>Windows <b>${{esc(r.window_count ?? 1)}}</b></div>
        <div>Price <b>${{esc(r.price)}}</b></div>
      </div>
      <div class="fresh">engine update ${{age}}s ago</div>
    </div>`;
  }}).join('') || `<div class="empty-state">No coins in this filter.</div>`;

  const ready=rows.filter(r=>r.score!==null);
  if(ready.length){{
    const strongest=ready.reduce((a,b)=>Math.abs(Number(b.score))>Math.abs(Number(a.score))?b:a);
    const sig=uiSignal(strongest.side);
    const direction=Number(strongest.score)>=0?'Buy':'Sell';
    document.getElementById('strongestBanner').innerHTML=
      `Strongest now: <b>${{esc(strongest.symbol)}}</b> / ${{direction}} · score <b>${{Number(strongest.score).toFixed(2)}}</b> · `+
      `${{sig==='BUY'||sig==='SELL' ? sig+' MODEL SIGNAL' : 'WAIT — execution test only'}}. `+
      `Trigger is ±${{threshold}}.`;
  }}
  document.getElementById('lastSync').textContent='synced '+new Date().toLocaleTimeString();
}}
async function pollSignals(){{
  try{{
    const r=await fetch('/signals?_='+Date.now(),{{cache:'no-store'}});
    if(!r.ok) throw new Error('HTTP '+r.status);
    const p=await r.json();
    render(p);
    document.getElementById('connectionState').textContent='LIVE';
  }}catch(e){{
    document.getElementById('connectionState').textContent='RETRY';
  }}
}}
document.querySelectorAll('.filter').forEach(btn=>btn.addEventListener('click',()=>{{
  document.querySelectorAll('.filter').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  activeFilter=btn.dataset.filter;
  render(latestPayload);
}}));
render(INITIAL);
setInterval(pollSignals,3000);

const c=document.getElementById('cd');
if(c){{
  const due=Number(c.dataset.due);
  const tick=()=>{{
    const s=Math.ceil((due-Date.now())/1000);
    c.textContent=s>0?Math.floor(s/60)+'m '+(s%60)+'s':'READY TO CLOSE';
  }};
  tick();
  setInterval(tick,1000);
}}
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
                        "version": "0.3.3-demo",
                        "server_ms": core.now_ms(),
                        "symbols": core.SYMBOLS,
                        "threshold": float(core.SIGNAL_THRESHOLD),
                        "engine_interval_sec": int(core.FEATURE_INTERVAL_SEC),
                        "signals": latest_signals(db),
                    }
                )
            elif path == "/trade":
                if self.require_auth():
                    self.trade_page()
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            path = urllib.parse.urlparse(self.path).path
            if path not in {"/trade/open", "/trade/close"}:
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
                    )
                    self.trade_page(
                        f"OPEN submitted: {result['symbol']} "
                        f"{result['side']} qty={result['qty']} "
                        f"≈{result['notional']:.4f} USDT. "
                        f"Order ID {result['order_id'] or 'pending'}."
                    )
                else:
                    result = live.close()
                    self.trade_page(
                        f"CLOSE ALL submitted: {result['count']} "
                        f"reduce-only order(s)."
                    )
            except Exception as exc:
                log.error(
                    "Manual approval action failed: %s",
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
        raise RuntimeError("V0.3.1 demo guard: manual approval requires api-demo.bybit.com")
    if DEMO_LEVERAGE != 20:
        raise RuntimeError("V0.3.3 demo guard: DEMO_LEVERAGE must be 20")
    if MAX_LIVE_NOTIONAL_USDT != Decimal("50000"):
        raise RuntimeError("V0.3.3 demo guard: MAX_LIVE_NOTIONAL_USDT must be 50000")
    if MAX_OPEN_LINEAR_POSITIONS != 10:
        raise RuntimeError("V0.3.3 demo guard: MAX_OPEN_LINEAR_POSITIONS must be 10")
    db = core.Database(core.DB_PATH)
    collector = core.Collector(db)
    account = core.AccountMonitor(db)
    features = core.FeatureEngine(db)
    live = LiveApproval(db)

    start_server(collector, db, account, live)

    log.info("trade-engine v0.3.3-demo")
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
