import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Dict

import websockets


def env_csv_int(name: str, default: str):
    return [int(x.strip()) for x in os.getenv(name, default).split(",") if x.strip()]


SYMBOLS = [
    s.strip().upper()
    for s in os.getenv(
        "SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,DOGEUSDT,BEAMUSDT"
    ).split(",")
    if s.strip()
]

BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")
# Bybit documents a regional REST endpoint for Georgia users.
# Override this Railway variable if your account belongs to a different Bybit region.
BYBIT_REST_URL = os.getenv("BYBIT_REST_URL", "https://api.bybitgeorgia.ge").rstrip("/")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")

DB_PATH = os.getenv("DB_PATH", "/data/trade_engine.db")
ORDERBOOK_DEPTH = int(os.getenv("ORDERBOOK_DEPTH", "50"))
SNAPSHOT_INTERVAL_SEC = float(os.getenv("SNAPSHOT_INTERVAL_SEC", "5"))
PORT = int(os.getenv("PORT", "8000"))

FEATURE_WINDOWS_MIN = env_csv_int(
    "FEATURE_WINDOWS_MIN", "3,4,5,6,7,8,9,10,12,15,20,30"
)
LABEL_HORIZONS_MIN = env_csv_int(
    "LABEL_HORIZONS_MIN", "1,3,5,10,15,30"
)
FEATURE_INTERVAL_SEC = int(os.getenv("FEATURE_INTERVAL_SEC", "60"))
PRIVATE_POLL_INTERVAL_SEC = int(os.getenv("PRIVATE_POLL_INTERVAL_SEC", "60"))
SIGNAL_THRESHOLD = float(os.getenv("SIGNAL_THRESHOLD", "70"))

# Hard safety switch. V0.2 contains no order-placement function.
LIVE_ORDERS_ENABLED = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("trade-engine")


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;

CREATE TABLE IF NOT EXISTS candles_1m (
  symbol TEXT NOT NULL,
  start_ms INTEGER NOT NULL,
  end_ms INTEGER NOT NULL,
  open REAL NOT NULL,
  high REAL NOT NULL,
  low REAL NOT NULL,
  close REAL NOT NULL,
  volume REAL NOT NULL,
  turnover REAL NOT NULL,
  source_ts_ms INTEGER NOT NULL,
  PRIMARY KEY(symbol, start_ms)
);

CREATE TABLE IF NOT EXISTS ticker_snapshots (
  symbol TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  last_price REAL,
  mark_price REAL,
  index_price REAL,
  bid1_price REAL,
  bid1_size REAL,
  ask1_price REAL,
  ask1_size REAL,
  open_interest REAL,
  open_interest_value REAL,
  funding_rate REAL,
  volume_24h REAL,
  turnover_24h REAL,
  price_24h_pct REAL,
  PRIMARY KEY(symbol, ts_ms)
);

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
  symbol TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  bid1 REAL,
  ask1 REAL,
  spread REAL,
  mid REAL,
  bid_depth_5 REAL,
  ask_depth_5 REAL,
  bid_depth_10 REAL,
  ask_depth_10 REAL,
  imbalance_5 REAL,
  imbalance_10 REAL,
  microprice REAL,
  update_id INTEGER,
  seq INTEGER,
  PRIMARY KEY(symbol, ts_ms)
);

CREATE TABLE IF NOT EXISTS trade_buckets_1s (
  symbol TEXT NOT NULL,
  second_ms INTEGER NOT NULL,
  trade_count INTEGER NOT NULL,
  buy_qty REAL NOT NULL,
  sell_qty REAL NOT NULL,
  buy_turnover REAL NOT NULL,
  sell_turnover REAL NOT NULL,
  vwap REAL,
  last_price REAL,
  PRIMARY KEY(symbol, second_ms)
);

CREATE TABLE IF NOT EXISTS account_snapshots (
  ts_ms INTEGER PRIMARY KEY,
  account_type TEXT,
  total_equity REAL,
  total_wallet_balance REAL,
  total_available_balance REAL,
  total_perp_upl REAL
);

CREATE TABLE IF NOT EXISTS position_snapshots (
  symbol TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  side TEXT,
  size REAL,
  avg_price REAL,
  mark_price REAL,
  leverage REAL,
  unrealised_pnl REAL,
  liq_price REAL,
  position_value REAL,
  PRIMARY KEY(symbol, ts_ms)
);

CREATE TABLE IF NOT EXISTS features (
  symbol TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  window_min INTEGER NOT NULL,
  price REAL NOT NULL,
  ret_pct REAL,
  trade_count INTEGER,
  buy_qty REAL,
  sell_qty REAL,
  buy_ratio REAL,
  turnover REAL,
  avg_spread_bps REAL,
  avg_imbalance_10 REAL,
  oi_change_pct REAL,
  funding_rate REAL,
  btc_ret_pct REAL,
  research_score REAL,
  research_side TEXT,
  PRIMARY KEY(symbol, ts_ms, window_min)
);

CREATE TABLE IF NOT EXISTS labels (
  symbol TEXT NOT NULL,
  feature_ts_ms INTEGER NOT NULL,
  window_min INTEGER NOT NULL,
  horizon_min INTEGER NOT NULL,
  future_ret_pct REAL,
  mfe_pct REAL,
  mae_pct REAL,
  labeled_at_ms INTEGER NOT NULL,
  PRIMARY KEY(symbol, feature_ts_ms, window_min, horizon_min)
);

CREATE TABLE IF NOT EXISTS signals (
  symbol TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  best_window_min INTEGER,
  score REAL,
  side TEXT,
  price REAL,
  reason_json TEXT,
  status TEXT NOT NULL,
  PRIMARY KEY(symbol, ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_ticker_symbol_ts
  ON ticker_snapshots(symbol, ts_ms);
CREATE INDEX IF NOT EXISTS idx_trade_symbol_ts
  ON trade_buckets_1s(symbol, second_ms);
CREATE INDEX IF NOT EXISTS idx_book_symbol_ts
  ON orderbook_snapshots(symbol, ts_ms);
CREATE INDEX IF NOT EXISTS idx_features_ts
  ON features(ts_ms);
"""


def f(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def now_ms():
    return int(time.time() * 1000)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


class Database:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def execute(self, sql, params=()):
        with self.lock:
            self.conn.execute(sql, params)
            self.conn.commit()

    def executemany(self, sql, rows):
        rows = list(rows)
        if not rows:
            return
        with self.lock:
            self.conn.executemany(sql, rows)
            self.conn.commit()

    def one(self, sql, params=()):
        with self.lock:
            return self.conn.execute(sql, params).fetchone()

    def query(self, sql, params=()):
        with self.lock:
            return self.conn.execute(sql, params).fetchall()

    def scalar(self, sql, params=()):
        row = self.one(sql, params)
        return row[0] if row else None


@dataclass
class OrderBook:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    update_id: int | None = None
    seq: int | None = None

    def apply(self, typ: str, data: dict):
        if typ == "snapshot":
            self.bids.clear()
            self.asks.clear()

        for price, qty in data.get("b", []):
            p, q = float(price), float(qty)
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        for price, qty in data.get("a", []):
            p, q = float(price), float(qty)
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

        self.update_id = data.get("u")
        self.seq = data.get("seq")

    def summary(self):
        if not self.bids or not self.asks:
            return None

        bids = sorted(self.bids.items(), reverse=True)
        asks = sorted(self.asks.items())

        bid1, bid1_qty = bids[0]
        ask1, ask1_qty = asks[0]
        spread = ask1 - bid1
        mid = (ask1 + bid1) / 2

        bd5 = sum(q for _, q in bids[:5])
        ad5 = sum(q for _, q in asks[:5])
        bd10 = sum(q for _, q in bids[:10])
        ad10 = sum(q for _, q in asks[:10])

        imb5 = (bd5 - ad5) / (bd5 + ad5) if bd5 + ad5 else 0.0
        imb10 = (bd10 - ad10) / (bd10 + ad10) if bd10 + ad10 else 0.0

        denom = bid1_qty + ask1_qty
        microprice = (
            ((ask1 * bid1_qty) + (bid1 * ask1_qty)) / denom if denom else mid
        )

        return (
            bid1, ask1, spread, mid,
            bd5, ad5, bd10, ad10,
            imb5, imb10, microprice,
        )


class Collector:
    def __init__(self, db: Database):
        self.db = db
        self.books = {s: OrderBook() for s in SYMBOLS}
        self.tickers = {s: {} for s in SYMBOLS}
        self.trade_buckets = defaultdict(
            lambda: {
                "count": 0,
                "buy_qty": 0.0,
                "sell_qty": 0.0,
                "buy_turnover": 0.0,
                "sell_turnover": 0.0,
                "pv": 0.0,
                "qty": 0.0,
                "last_price": None,
            }
        )
        self.last_message_ms = 0
        self.last_connect_ms = 0
        self.messages = 0
        self.reconnects = 0

    async def run_forever(self):
        delay = 1
        while True:
            try:
                await self.run_once()
                delay = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.reconnects += 1
                log.exception("WebSocket error: %s", exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def run_once(self):
        log.info("Connecting public WS %s", BYBIT_WS_URL)

        async with websockets.connect(
            BYBIT_WS_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=8 * 1024 * 1024,
        ) as ws:
            self.last_connect_ms = now_ms()

            args = []
            for s in SYMBOLS:
                args.extend(
                    [
                        f"kline.1.{s}",
                        f"tickers.{s}",
                        f"orderbook.{ORDERBOOK_DEPTH}.{s}",
                        f"publicTrade.{s}",
                    ]
                )

            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            log.info("Subscribed to %d public topics: %s", len(args), ", ".join(SYMBOLS))

            flush_task = asyncio.create_task(self.flush_loop())

            try:
                async for raw in ws:
                    self.messages += 1
                    self.last_message_ms = now_ms()
                    self.handle(json.loads(raw))
            finally:
                flush_task.cancel()
                try:
                    await flush_task
                except asyncio.CancelledError:
                    pass
                self.flush_all()

    def handle(self, msg):
        topic = msg.get("topic", "")

        if not topic:
            if msg.get("success") is False:
                log.error("Subscription rejected: %s", msg)
            return

        if topic.startswith("kline.1."):
            self.handle_kline(msg)
        elif topic.startswith("tickers."):
            self.handle_ticker(msg)
        elif topic.startswith("orderbook."):
            self.handle_orderbook(msg)
        elif topic.startswith("publicTrade."):
            self.handle_trade(msg)

    def handle_kline(self, msg):
        symbol = msg["topic"].split(".")[-1]

        for k in msg.get("data", []):
            if not k.get("confirm"):
                continue

            self.db.execute(
                """INSERT OR REPLACE INTO candles_1m
                (symbol,start_ms,end_ms,open,high,low,close,volume,turnover,source_ts_ms)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    symbol,
                    int(k["start"]),
                    int(k["end"]),
                    float(k["open"]),
                    float(k["high"]),
                    float(k["low"]),
                    float(k["close"]),
                    float(k["volume"]),
                    float(k["turnover"]),
                    int(k.get("timestamp") or msg.get("ts") or now_ms()),
                ),
            )

    def handle_ticker(self, msg):
        data = msg.get("data") or {}
        if isinstance(data, list):
            data = data[0] if data else {}

        symbol = data.get("symbol") or msg["topic"].split(".")[-1]
        if symbol in self.tickers:
            self.tickers[symbol].update(data)

    def handle_orderbook(self, msg):
        data = msg.get("data") or {}
        symbol = data.get("s") or msg["topic"].split(".")[-1]

        if symbol in self.books:
            self.books[symbol].apply(msg.get("type", "delta"), data)

    def handle_trade(self, msg):
        for t in msg.get("data", []):
            symbol = t.get("s")
            if symbol not in SYMBOLS:
                continue

            ts = int(t.get("T") or msg.get("ts") or now_ms())
            sec = (ts // 1000) * 1000
            price = float(t["p"])
            qty = float(t["v"])
            bucket = self.trade_buckets[(symbol, sec)]
            turnover = price * qty

            bucket["count"] += 1

            if t.get("S") == "Buy":
                bucket["buy_qty"] += qty
                bucket["buy_turnover"] += turnover
            else:
                bucket["sell_qty"] += qty
                bucket["sell_turnover"] += turnover

            bucket["pv"] += turnover
            bucket["qty"] += qty
            bucket["last_price"] = price

    async def flush_loop(self):
        while True:
            await asyncio.sleep(SNAPSHOT_INTERVAL_SEC)
            self.flush_all()

    def flush_all(self):
        self.flush_tickers()
        self.flush_books()
        self.flush_trades()

    def flush_tickers(self):
        bucket_ms = max(1000, int(SNAPSHOT_INTERVAL_SEC * 1000))
        ts = (now_ms() // bucket_ms) * bucket_ms
        rows = []

        for symbol, d in self.tickers.items():
            if not d:
                continue

            rows.append(
                (
                    symbol,
                    ts,
                    f(d.get("lastPrice")),
                    f(d.get("markPrice")),
                    f(d.get("indexPrice")),
                    f(d.get("bid1Price")),
                    f(d.get("bid1Size")),
                    f(d.get("ask1Price")),
                    f(d.get("ask1Size")),
                    f(d.get("openInterest")),
                    f(d.get("openInterestValue")),
                    f(d.get("fundingRate")),
                    f(d.get("volume24h")),
                    f(d.get("turnover24h")),
                    f(d.get("price24hPcnt")),
                )
            )

        self.db.executemany(
            """INSERT OR REPLACE INTO ticker_snapshots
            (symbol,ts_ms,last_price,mark_price,index_price,bid1_price,bid1_size,
             ask1_price,ask1_size,open_interest,open_interest_value,funding_rate,
             volume_24h,turnover_24h,price_24h_pct)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )

    def flush_books(self):
        bucket_ms = max(1000, int(SNAPSHOT_INTERVAL_SEC * 1000))
        ts = (now_ms() // bucket_ms) * bucket_ms
        rows = []

        for symbol, book in self.books.items():
            summary = book.summary()
            if summary:
                rows.append((symbol, ts, *summary, book.update_id, book.seq))

        self.db.executemany(
            """INSERT OR REPLACE INTO orderbook_snapshots
            (symbol,ts_ms,bid1,ask1,spread,mid,bid_depth_5,ask_depth_5,
             bid_depth_10,ask_depth_10,imbalance_5,imbalance_10,microprice,
             update_id,seq)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )

    def flush_trades(self):
        current_sec = (now_ms() // 1000) * 1000
        ready = [key for key in self.trade_buckets if key[1] < current_sec]
        rows = []

        for key in ready:
            symbol, sec = key
            bucket = self.trade_buckets.pop(key)
            vwap = bucket["pv"] / bucket["qty"] if bucket["qty"] else None

            rows.append(
                (
                    symbol,
                    sec,
                    bucket["count"],
                    bucket["buy_qty"],
                    bucket["sell_qty"],
                    bucket["buy_turnover"],
                    bucket["sell_turnover"],
                    vwap,
                    bucket["last_price"],
                )
            )

        self.db.executemany(
            """INSERT OR REPLACE INTO trade_buckets_1s
            (symbol,second_ms,trade_count,buy_qty,sell_qty,buy_turnover,
             sell_turnover,vwap,last_price)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            rows,
        )


class BybitPrivateClient:
    def __init__(self):
        self.api_key = BYBIT_API_KEY
        self.api_secret = BYBIT_API_SECRET
        self.base_url = BYBIT_REST_URL
        self.recv_window = BYBIT_RECV_WINDOW

    @property
    def configured(self):
        return bool(self.api_key and self.api_secret)

    def get(self, path: str, params=None):
        if not self.configured:
            raise RuntimeError("BYBIT_API_KEY / BYBIT_API_SECRET not configured")

        params = params or {}
        query = urllib.parse.urlencode(params)
        ts = str(now_ms())
        payload = ts + self.api_key + self.recv_window + query

        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        url = self.base_url + path
        if query:
            url += "?" + query

        req = urllib.request.Request(
            url,
            headers={
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": ts,
                "X-BAPI-SIGN": signature,
                "X-BAPI-RECV-WINDOW": self.recv_window,
                "Content-Type": "application/json",
                "User-Agent": "trade-engine-v0.2",
            },
            method="GET",
        )

        try:
            with urllib.request.urlopen(req, timeout=12) as response:
                data = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code}: {body[:300]}") from exc

        if data.get("retCode") != 0:
            raise RuntimeError(
                f"Bybit retCode={data.get('retCode')} retMsg={data.get('retMsg')}"
            )

        return data

    def api_key_info(self):
        return self.get("/v5/user/query-api")

    def wallet_balance(self):
        return self.get(
            "/v5/account/wallet-balance",
            {"accountType": "UNIFIED", "coin": "USDT"},
        )

    def positions(self):
        return self.get(
            "/v5/position/list",
            {"category": "linear", "settleCoin": "USDT", "limit": 200},
        )


class AccountMonitor:
    def __init__(self, db: Database):
        self.db = db
        self.client = BybitPrivateClient()
        self.status = "not_configured"
        self.last_ok_ms = 0
        self.last_error = ""
        self.permission_checked = False

    def poll(self):
        if not self.client.configured:
            self.status = "not_configured"
            return

        try:
            if not self.permission_checked:
                info = self.client.api_key_info()
                result = info.get("result", {})
                read_only = result.get("readOnly")
                permissions = result.get("permissions", {})
                log.info(
                    "Private API authenticated. readOnly=%s permission-groups=%s",
                    read_only,
                    ",".join(sorted(permissions.keys())) if isinstance(permissions, dict) else "unknown",
                )
                self.permission_checked = True

            wallet = self.client.wallet_balance()
            positions = self.client.positions()

            ts = now_ms()
            account_list = wallet.get("result", {}).get("list", [])
            account = account_list[0] if account_list else {}

            self.db.execute(
                """INSERT OR REPLACE INTO account_snapshots
                (ts_ms,account_type,total_equity,total_wallet_balance,
                 total_available_balance,total_perp_upl)
                VALUES (?,?,?,?,?,?)""",
                (
                    ts,
                    account.get("accountType"),
                    f(account.get("totalEquity")),
                    f(account.get("totalWalletBalance")),
                    f(account.get("totalAvailableBalance")),
                    f(account.get("totalPerpUPL")),
                ),
            )

            rows = []
            open_count = 0

            for p in positions.get("result", {}).get("list", []):
                size = f(p.get("size")) or 0.0
                if size > 0:
                    open_count += 1

                rows.append(
                    (
                        p.get("symbol"),
                        ts,
                        p.get("side"),
                        size,
                        f(p.get("avgPrice")),
                        f(p.get("markPrice")),
                        f(p.get("leverage")),
                        f(p.get("unrealisedPnl")),
                        f(p.get("liqPrice")),
                        f(p.get("positionValue")),
                    )
                )

            self.db.executemany(
                """INSERT OR REPLACE INTO position_snapshots
                (symbol,ts_ms,side,size,avg_price,mark_price,leverage,
                 unrealised_pnl,liq_price,position_value)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )

            self.status = "ok"
            self.last_ok_ms = ts
            self.last_error = ""
            log.info("Private API poll OK; open linear positions=%d", open_count)

        except Exception as exc:
            self.status = "error"
            self.last_error = str(exc)[:500]
            log.error("Private API poll failed: %s", self.last_error)


class FeatureEngine:
    def __init__(self, db: Database):
        self.db = db

    def latest_ticker(self, symbol, ts_ms):
        return self.db.one(
            """SELECT ts_ms,last_price,open_interest,funding_rate
               FROM ticker_snapshots
               WHERE symbol=? AND ts_ms<=? AND last_price IS NOT NULL
               ORDER BY ts_ms DESC LIMIT 1""",
            (symbol, ts_ms),
        )

    def build_feature(self, symbol: str, ts_ms: int, window_min: int):
        start_ms = ts_ms - window_min * 60_000
        end_tick = self.latest_ticker(symbol, ts_ms)
        start_tick = self.latest_ticker(symbol, start_ms)

        if not end_tick or not start_tick:
            return None

        end_price = f(end_tick[1])
        start_price = f(start_tick[1])

        if not end_price or not start_price:
            return None

        ret_pct = (end_price / start_price - 1.0) * 100.0

        trade = self.db.one(
            """SELECT
                 COALESCE(SUM(trade_count),0),
                 COALESCE(SUM(buy_qty),0),
                 COALESCE(SUM(sell_qty),0),
                 COALESCE(SUM(buy_turnover),0),
                 COALESCE(SUM(sell_turnover),0)
               FROM trade_buckets_1s
               WHERE symbol=? AND second_ms>? AND second_ms<=?""",
            (symbol, start_ms, ts_ms),
        )

        trade_count, buy_qty, sell_qty, buy_to, sell_to = trade
        total_qty = (buy_qty or 0.0) + (sell_qty or 0.0)
        buy_ratio = (buy_qty / total_qty) if total_qty > 0 else 0.5
        turnover = (buy_to or 0.0) + (sell_to or 0.0)

        book = self.db.one(
            """SELECT
                 AVG(CASE WHEN mid>0 THEN (spread/mid)*10000.0 END),
                 AVG(imbalance_10)
               FROM orderbook_snapshots
               WHERE symbol=? AND ts_ms>? AND ts_ms<=?""",
            (symbol, start_ms, ts_ms),
        )

        avg_spread_bps = f(book[0]) if book else None
        avg_imbalance_10 = f(book[1]) if book else None

        start_oi = f(start_tick[2])
        end_oi = f(end_tick[2])

        if start_oi and end_oi and start_oi != 0:
            oi_change_pct = (end_oi / start_oi - 1.0) * 100.0
        else:
            oi_change_pct = None

        funding_rate = f(end_tick[3])

        btc_end = self.latest_ticker("BTCUSDT", ts_ms)
        btc_start = self.latest_ticker("BTCUSDT", start_ms)
        btc_ret_pct = None

        if btc_end and btc_start and f(btc_end[1]) and f(btc_start[1]):
            btc_ret_pct = (float(btc_end[1]) / float(btc_start[1]) - 1.0) * 100.0

        score, side, reasons = self.research_score(
            ret_pct=ret_pct,
            buy_ratio=buy_ratio,
            imbalance=avg_imbalance_10,
            oi_change_pct=oi_change_pct,
            btc_ret_pct=btc_ret_pct,
            spread_bps=avg_spread_bps,
        )

        return {
            "symbol": symbol,
            "ts_ms": ts_ms,
            "window_min": window_min,
            "price": end_price,
            "ret_pct": ret_pct,
            "trade_count": int(trade_count or 0),
            "buy_qty": float(buy_qty or 0.0),
            "sell_qty": float(sell_qty or 0.0),
            "buy_ratio": buy_ratio,
            "turnover": turnover,
            "avg_spread_bps": avg_spread_bps,
            "avg_imbalance_10": avg_imbalance_10,
            "oi_change_pct": oi_change_pct,
            "funding_rate": funding_rate,
            "btc_ret_pct": btc_ret_pct,
            "research_score": score,
            "research_side": side,
            "reasons": reasons,
        }

    def research_score(
        self,
        ret_pct,
        buy_ratio,
        imbalance,
        oi_change_pct,
        btc_ret_pct,
        spread_bps,
    ):
        # Baseline only. This is deliberately simple so later trained models
        # have a transparent benchmark to beat out-of-sample.
        momentum = math.tanh((ret_pct or 0.0) / 0.45) * 32.0
        flow = clamp(((buy_ratio or 0.5) - 0.5) * 2.0, -1, 1) * 24.0
        book = clamp(imbalance or 0.0, -1, 1) * 20.0
        btc = math.tanh((btc_ret_pct or 0.0) / 0.60) * 12.0

        oi_component = 0.0
        if oi_change_pct is not None and ret_pct:
            oi_strength = clamp(abs(oi_change_pct) / 1.5, 0, 1) * 12.0
            oi_component = math.copysign(oi_strength, ret_pct)

        raw = momentum + flow + book + btc + oi_component

        # Wide spread lowers conviction instead of flipping direction.
        spread_penalty = clamp((spread_bps or 0.0) * 1.5, 0, 15)
        if raw > 0:
            raw = max(0.0, raw - spread_penalty)
        elif raw < 0:
            raw = min(0.0, raw + spread_penalty)

        score = round(clamp(raw, -100, 100), 2)

        if score >= SIGNAL_THRESHOLD:
            side = "LONG"
        elif score <= -SIGNAL_THRESHOLD:
            side = "SHORT"
        else:
            side = "WAIT"

        reasons = {
            "momentum": round(momentum, 2),
            "orderflow": round(flow, 2),
            "orderbook": round(book, 2),
            "btc_context": round(btc, 2),
            "oi_confirmation": round(oi_component, 2),
            "spread_penalty": round(spread_penalty, 2),
        }

        return score, side, reasons

    def run_once(self):
        # Align features to minute boundaries.
        ts_ms = (now_ms() // 60_000) * 60_000
        best_by_symbol = {}

        for symbol in SYMBOLS:
            for window_min in FEATURE_WINDOWS_MIN:
                feature = self.build_feature(symbol, ts_ms, window_min)
                if not feature:
                    continue

                self.db.execute(
                    """INSERT OR REPLACE INTO features
                    (symbol,ts_ms,window_min,price,ret_pct,trade_count,buy_qty,
                     sell_qty,buy_ratio,turnover,avg_spread_bps,avg_imbalance_10,
                     oi_change_pct,funding_rate,btc_ret_pct,research_score,research_side)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        feature["symbol"],
                        feature["ts_ms"],
                        feature["window_min"],
                        feature["price"],
                        feature["ret_pct"],
                        feature["trade_count"],
                        feature["buy_qty"],
                        feature["sell_qty"],
                        feature["buy_ratio"],
                        feature["turnover"],
                        feature["avg_spread_bps"],
                        feature["avg_imbalance_10"],
                        feature["oi_change_pct"],
                        feature["funding_rate"],
                        feature["btc_ret_pct"],
                        feature["research_score"],
                        feature["research_side"],
                    ),
                )

                current = best_by_symbol.get(symbol)
                if (
                    current is None
                    or abs(feature["research_score"]) > abs(current["research_score"])
                ):
                    best_by_symbol[symbol] = feature

        for symbol, feature in best_by_symbol.items():
            side = feature["research_side"]
            status = "SIGNAL" if side != "WAIT" else "WAIT"

            self.db.execute(
                """INSERT OR REPLACE INTO signals
                (symbol,ts_ms,best_window_min,score,side,price,reason_json,status)
                VALUES (?,?,?,?,?,?,?,?)""",
                (
                    symbol,
                    ts_ms,
                    feature["window_min"],
                    feature["research_score"],
                    side,
                    feature["price"],
                    json.dumps(feature["reasons"], separators=(",", ":")),
                    status,
                ),
            )

            log.info(
                "Research signal %s %s score=%s best_window=%sm price=%s",
                symbol,
                side,
                feature["research_score"],
                feature["window_min"],
                feature["price"],
            )

        self.label_pending(ts_ms)

    def label_pending(self, now_ts_ms):
        max_horizon_ms = max(LABEL_HORIZONS_MIN) * 60_000
        oldest_needed = now_ts_ms - max_horizon_ms - 60_000

        features = self.db.query(
            """SELECT symbol,ts_ms,window_min,price
               FROM features
               WHERE ts_ms>=? AND ts_ms<?""",
            (oldest_needed, now_ts_ms),
        )

        for symbol, feature_ts, window_min, entry_price in features:
            entry_price = f(entry_price)
            if not entry_price:
                continue

            for horizon_min in LABEL_HORIZONS_MIN:
                target_ts = feature_ts + horizon_min * 60_000

                if target_ts > now_ts_ms:
                    continue

                exists = self.db.one(
                    """SELECT 1 FROM labels
                       WHERE symbol=? AND feature_ts_ms=? AND window_min=?
                         AND horizon_min=? LIMIT 1""",
                    (symbol, feature_ts, window_min, horizon_min),
                )

                if exists:
                    continue

                future = self.latest_ticker(symbol, target_ts)
                if not future or not f(future[1]):
                    continue

                future_price = float(future[1])
                future_ret_pct = (future_price / entry_price - 1.0) * 100.0

                extremes = self.db.one(
                    """SELECT MAX(last_price), MIN(last_price)
                       FROM ticker_snapshots
                       WHERE symbol=? AND ts_ms>? AND ts_ms<=?
                         AND last_price IS NOT NULL""",
                    (symbol, feature_ts, target_ts),
                )

                max_price = f(extremes[0]) if extremes else None
                min_price = f(extremes[1]) if extremes else None

                mfe_pct = (
                    (max_price / entry_price - 1.0) * 100.0
                    if max_price is not None
                    else None
                )
                mae_pct = (
                    (min_price / entry_price - 1.0) * 100.0
                    if min_price is not None
                    else None
                )

                self.db.execute(
                    """INSERT OR REPLACE INTO labels
                    (symbol,feature_ts_ms,window_min,horizon_min,
                     future_ret_pct,mfe_pct,mae_pct,labeled_at_ms)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        symbol,
                        feature_ts,
                        window_min,
                        horizon_min,
                        future_ret_pct,
                        mfe_pct,
                        mae_pct,
                        now_ms(),
                    ),
                )


async def feature_loop(engine: FeatureEngine):
    # Let the collector accumulate a short initial buffer.
    await asyncio.sleep(10)

    while True:
        try:
            await asyncio.to_thread(engine.run_once)
        except Exception as exc:
            log.exception("Feature engine error: %s", exc)

        await asyncio.sleep(FEATURE_INTERVAL_SEC)


async def account_loop(monitor: AccountMonitor):
    await asyncio.sleep(3)

    while True:
        await asyncio.to_thread(monitor.poll)
        await asyncio.sleep(PRIVATE_POLL_INTERVAL_SEC)


def start_health_server(collector: Collector, db: Database, account: AccountMonitor):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in ("/", "/health"):
                self.send_response(404)
                self.end_headers()
                return

            payload = {
                "status": "ok" if collector.last_message_ms else "starting",
                "version": "0.2",
                "mode": "research_only",
                "live_orders_enabled": LIVE_ORDERS_ENABLED,
                "symbols": SYMBOLS,
                "feature_windows_min": FEATURE_WINDOWS_MIN,
                "label_horizons_min": LABEL_HORIZONS_MIN,
                "public_market": {
                    "last_message_ms": collector.last_message_ms,
                    "last_connect_ms": collector.last_connect_ms,
                    "messages": collector.messages,
                    "reconnects": collector.reconnects,
                },
                "private_api": {
                    "configured": bool(BYBIT_API_KEY and BYBIT_API_SECRET),
                    "status": account.status,
                    "last_ok_ms": account.last_ok_ms,
                },
                "rows": {
                    "candles_1m": db.scalar("SELECT COUNT(*) FROM candles_1m") or 0,
                    "ticker_snapshots": db.scalar("SELECT COUNT(*) FROM ticker_snapshots") or 0,
                    "orderbook_snapshots": db.scalar("SELECT COUNT(*) FROM orderbook_snapshots") or 0,
                    "trade_buckets_1s": db.scalar("SELECT COUNT(*) FROM trade_buckets_1s") or 0,
                    "account_snapshots": db.scalar("SELECT COUNT(*) FROM account_snapshots") or 0,
                    "position_snapshots": db.scalar("SELECT COUNT(*) FROM position_snapshots") or 0,
                    "features": db.scalar("SELECT COUNT(*) FROM features") or 0,
                    "labels": db.scalar("SELECT COUNT(*) FROM labels") or 0,
                    "signals": db.scalar("SELECT COUNT(*) FROM signals") or 0,
                },
            }

            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    Thread(target=server.serve_forever, daemon=True).start()
    log.info("Health endpoint on 0.0.0.0:%s/health", PORT)


async def main():
    db = Database(DB_PATH)
    collector = Collector(db)
    account = AccountMonitor(db)
    features = FeatureEngine(db)

    start_health_server(collector, db, account)

    log.info("trade-engine v0.2")
    log.info("Symbols: %s", ", ".join(SYMBOLS))
    log.info("Feature windows: %s", FEATURE_WINDOWS_MIN)
    log.info("Label horizons: %s", LABEL_HORIZONS_MIN)
    log.info("Private API configured: %s", account.client.configured)
    log.info("Private REST base: %s", BYBIT_REST_URL)
    log.info("LIVE_ORDERS_ENABLED=%s", LIVE_ORDERS_ENABLED)

    await asyncio.gather(
        collector.run_forever(),
        feature_loop(features),
        account_loop(account),
    )


if __name__ == "__main__":
    asyncio.run(main())
