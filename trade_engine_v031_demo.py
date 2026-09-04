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
                "User-Agent": "trade-engine-v0.3",
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
        req = urllib.request.Request(url, headers={"User-Agent": "trade-engine-v0.3"})
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
        if row:
            out.append(
                {
                    "symbol": symbol,
                    "ts_ms": row[0],
                    "best_window_min": row[1],
                    "score": float(row[2]),
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
                f"{symbol} minimum order is about {actual:.4f} USDT, above the $5.02 safety cap."
            )
        return sig, dtxt(qty), actual

    def open(self, symbol, duration, mode="test"):
        with self.lock:
            if not self.ready:
                raise RuntimeError("Manual live approval is not enabled/configured")
            if symbol not in core.SYMBOLS or duration not in ALLOWED_DURATIONS:
                raise RuntimeError("Invalid symbol or duration")
            if self.positions():
                raise RuntimeError("Guardrail: an open linear position already exists")

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
            if len(positions) != 1:
                raise RuntimeError(
                    f"Expected exactly one open position; found {len(positions)}"
                )

            p = positions[0]
            if p["symbol"] not in core.SYMBOLS:
                raise RuntimeError(
                    "Position is outside whitelist; refusing to touch it"
                )
            if str(p.get("position_idx")) not in {"0", "None"}:
                raise RuntimeError("Hedge mode is not supported in V0.3")

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
                    "manual_reduce_only",
                ),
            )
            log.warning(
                "MANUAL LIVE CLOSE approved symbol=%s side=%s qty=%s",
                p["symbol"],
                side,
                p["size"],
            )
            return {"position": p, "order_id": oid}

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
                "version": "0.3.1-demo",
                "mode": "research_plus_manual_live_approval",
                "auto_live_orders_enabled": False,
                "manual_live_approval_enabled": live.ready,
                "max_live_notional_usdt": float(
                    MAX_LIVE_NOTIONAL_USDT
                ),
                "live_leverage": DEMO_LEVERAGE,
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

            rows = (
                "".join(
                    f"<tr><td>{html.escape(s['symbol'])}</td>"
                    f"<td>{html.escape(s['side'])}</td>"
                    f"<td>{s['score']:.2f}</td>"
                    f"<td>{s['best_window_min']}m</td>"
                    f"<td>{s['price']}</td></tr>"
                    for s in signals
                )
                or "<tr><td colspan=5>No signal data</td></tr>"
            )

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

            pos = "No open linear position."
            if positions:
                p = positions[0]
                pos = (
                    f"<b>{html.escape(p['symbol'])}</b> "
                    f"{html.escape(str(p['side']))} "
                    f"qty={html.escape(str(p['size']))} "
                    f"avg={html.escape(str(p['avg_price']))} "
                    f"mark={html.escape(str(p['mark_price']))} "
                    f"uPnL={html.escape(str(p['unrealised_pnl']))}"
                )

            if pos_error:
                pos += f"<div class='err'>{html.escape(pos_error)}</div>"

            plan = live.current_plan()
            due_html = ""
            if plan and positions and plan[4]:
                due_ms = int(plan[0]) + int(plan[4]) * 1000
                due_html = (
                    f"<p>Planned close countdown: "
                    f"<b id='cd' data-due='{due_ms}'>calculating…</b></p>"
                )

            msg = (
                f"<div class='{'err' if error else 'ok'}'>"
                f"{html.escape(message)}</div>"
                if message
                else ""
            )
            disabled = "" if live.ready else "disabled"
            qualification = (
                "MODEL SIGNAL" if qualified else "EXECUTION TEST ONLY"
            )

            text = f"""<!doctype html>
<html>
<head>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<style>
body{{font-family:system-ui;background:#0f1115;color:#eee;max-width:820px;margin:24px auto;padding:0 16px}}
.card{{background:#181c22;border:1px solid #303743;border-radius:14px;padding:18px;margin:14px 0}}
table{{width:100%;border-collapse:collapse}}
td,th{{padding:8px;border-bottom:1px solid #2b313b;text-align:left}}
button{{padding:13px 16px;border:0;border-radius:10px;font-weight:700;cursor:pointer}}
button:disabled{{opacity:.4}}
select{{padding:10px;border-radius:8px;background:#11151a;color:#eee;border:1px solid #3a424e}}
.ok{{padding:12px;background:#14351f;border-radius:10px}}
.err{{padding:12px;background:#441b1b;border-radius:10px}}
.warn{{padding:12px;background:#4a3513;border-radius:10px}}
small{{color:#aab3bf}}
</style>
</head>
<body>
<h1>Trade Engine V0.3</h1>
<p><b>Manual approval only.</b> Auto-live is hard-disabled. Max ${float(MAX_LIVE_NOTIONAL_USDT):.2f} notional, 1x, one position.</p>
{msg}

<div class='card'>
<h2>Latest research</h2>
<table>
<tr><th>Symbol</th><th>Signal</th><th>Score</th><th>Window</th><th>Price</th></tr>
{rows}
</table>
</div>

<div class='card'>
<h2>Current position</h2>
<p>{pos}</p>
{due_html}
</div>

<div class='card'>
<h2>Approve open</h2>
<div class='warn'>
Strongest now: <b>{html.escape(symbol)}</b> / {html.escape(side)} — {qualification}.
Test mode may trade below ±{core.SIGNAL_THRESHOLD:g}, but direction always follows the latest score sign.
</div>
<form method='post' action='/trade/open'>
<input type='hidden' name='nonce' value='{html.escape(nonce_open)}'>
<label>Symbol
<select name='symbol'>{options}</select>
</label>
<label>Duration
<select name='duration'>
<option value='300'>5 min</option>
<option value='600'>10 min</option>
<option value='900'>15 min</option>
</select>
</label>
<p><button {disabled} type='submit'>Approve & Open ≈${float(MAX_LIVE_NOTIONAL_USDT):,.0f} Demo</button></p>
</form>
<small>Some symbols may be rejected if Bybit minimum order exceeds the $5.02 cap.</small>
</div>

<div class='card'>
<h2>Approve close</h2>
<form method='post' action='/trade/close'>
<input type='hidden' name='nonce' value='{html.escape(nonce_close)}'>
<button {disabled} type='submit'>Approve & Close Current Position (reduce-only)</button>
</form>
<small>Never automatic. Backend re-reads the live position before reduce-only close.</small>
</div>

<script>
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
                    {"version": "0.3.1-demo", "signals": latest_signals(db)}
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
                    p = result["position"]
                    self.trade_page(
                        f"CLOSE submitted: {p['symbol']} "
                        f"reduce-only qty={p['size']}. "
                        f"Order ID {result['order_id'] or 'pending'}."
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
    if DEMO_LEVERAGE < 1 or DEMO_LEVERAGE > 100:
        raise RuntimeError("DEMO_LEVERAGE out of allowed demo range")
    db = core.Database(core.DB_PATH)
    collector = core.Collector(db)
    account = core.AccountMonitor(db)
    features = core.FeatureEngine(db)
    live = LiveApproval(db)

    start_server(collector, db, account, live)

    log.info("trade-engine v0.3.1-demo")
    log.info("AUTO_LIVE_ORDERS_ENABLED=False")
    log.info(
        "MANUAL_LIVE_APPROVAL_ENABLED=%s approval_pin_configured=%s max_notional=%s leverage=%sx demo_only=%s",
        MANUAL_LIVE_APPROVAL_ENABLED,
        bool(APPROVAL_PIN),
        MAX_LIVE_NOTIONAL_USDT,
        DEMO_LEVERAGE,
        "api-demo.bybit.com" in core.BYBIT_REST_URL,
    )

    await asyncio.gather(
        collector.run_forever(),
        core.feature_loop(features),
        core.account_loop(account),
    )


if __name__ == "__main__":
    asyncio.run(run())
