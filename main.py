import asyncio
import json
import logging
import os
import sqlite3
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Dict

import websockets

SYMBOLS = [s.strip().upper() for s in os.getenv(
    "SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,DOGEUSDT,BEAMUSDT"
).split(",") if s.strip()]
BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")
DB_PATH = os.getenv("DB_PATH", "/data/trade_engine.db")
ORDERBOOK_DEPTH = int(os.getenv("ORDERBOOK_DEPTH", "50"))
SNAPSHOT_INTERVAL_SEC = float(os.getenv("SNAPSHOT_INTERVAL_SEC", "5"))
PORT = int(os.getenv("PORT", "8000"))

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

    def scalar(self, sql, params=()):
        with self.lock:
            row = self.conn.execute(sql, params).fetchone()
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
        microprice = ((ask1 * bid1_qty) + (bid1 * ask1_qty)) / denom if denom else mid
        return bid1, ask1, spread, mid, bd5, ad5, bd10, ad10, imb5, imb10, microprice

class Collector:
    def __init__(self, db):
        self.db = db
        self.books = {s: OrderBook() for s in SYMBOLS}
        self.tickers = {s: {} for s in SYMBOLS}
        self.trade_buckets = defaultdict(lambda: {
            "count": 0, "buy_qty": 0.0, "sell_qty": 0.0,
            "buy_turnover": 0.0, "sell_turnover": 0.0,
            "pv": 0.0, "qty": 0.0, "last_price": None,
        })
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
        log.info("Connecting %s", BYBIT_WS_URL)
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
                args.extend([
                    f"kline.1.{s}",
                    f"tickers.{s}",
                    f"orderbook.{ORDERBOOK_DEPTH}.{s}",
                    f"publicTrade.{s}",
                ])
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            log.info("Subscribed to %d topics: %s", len(args), ", ".join(SYMBOLS))

            flush_task = asyncio.create_task(self.flush_loop())
            try:
                async for raw in ws:
                    self.messages += 1
                    self.last_message_ms = now_ms()
                    msg = json.loads(raw)
                    self.handle(msg)
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
                (symbol, int(k["start"]), int(k["end"]), float(k["open"]),
                 float(k["high"]), float(k["low"]), float(k["close"]),
                 float(k["volume"]), float(k["turnover"]),
                 int(k.get("timestamp") or msg.get("ts") or now_ms()))
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
            b = self.trade_buckets[(symbol, sec)]
            turnover = price * qty
            b["count"] += 1
            if t.get("S") == "Buy":
                b["buy_qty"] += qty
                b["buy_turnover"] += turnover
            else:
                b["sell_qty"] += qty
                b["sell_turnover"] += turnover
            b["pv"] += turnover
            b["qty"] += qty
            b["last_price"] = price

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
            rows.append((
                symbol, ts, f(d.get("lastPrice")), f(d.get("markPrice")),
                f(d.get("indexPrice")), f(d.get("bid1Price")), f(d.get("bid1Size")),
                f(d.get("ask1Price")), f(d.get("ask1Size")), f(d.get("openInterest")),
                f(d.get("openInterestValue")), f(d.get("fundingRate")), f(d.get("volume24h")),
                f(d.get("turnover24h")), f(d.get("price24hPcnt")),
            ))
        self.db.executemany(
            """INSERT OR REPLACE INTO ticker_snapshots
            (symbol,ts_ms,last_price,mark_price,index_price,bid1_price,bid1_size,ask1_price,ask1_size,
             open_interest,open_interest_value,funding_rate,volume_24h,turnover_24h,price_24h_pct)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)

    def flush_books(self):
        bucket_ms = max(1000, int(SNAPSHOT_INTERVAL_SEC * 1000))
        ts = (now_ms() // bucket_ms) * bucket_ms
        rows = []
        for symbol, book in self.books.items():
            s = book.summary()
            if s:
                rows.append((symbol, ts, *s, book.update_id, book.seq))
        self.db.executemany(
            """INSERT OR REPLACE INTO orderbook_snapshots
            (symbol,ts_ms,bid1,ask1,spread,mid,bid_depth_5,ask_depth_5,bid_depth_10,ask_depth_10,
             imbalance_5,imbalance_10,microprice,update_id,seq)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)

    def flush_trades(self):
        current_sec = (now_ms() // 1000) * 1000
        ready = [key for key in self.trade_buckets if key[1] < current_sec]
        rows = []
        for key in ready:
            symbol, sec = key
            b = self.trade_buckets.pop(key)
            vwap = b["pv"] / b["qty"] if b["qty"] else None
            rows.append((
                symbol, sec, b["count"], b["buy_qty"], b["sell_qty"],
                b["buy_turnover"], b["sell_turnover"], vwap, b["last_price"]
            ))
        self.db.executemany(
            """INSERT OR REPLACE INTO trade_buckets_1s
            (symbol,second_ms,trade_count,buy_qty,sell_qty,buy_turnover,sell_turnover,vwap,last_price)
            VALUES (?,?,?,?,?,?,?,?,?)""", rows)

def start_health_server(collector, db):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in ("/", "/health"):
                self.send_response(404)
                self.end_headers()
                return
            payload = {
                "status": "ok" if collector.last_message_ms else "starting",
                "symbols": SYMBOLS,
                "last_message_ms": collector.last_message_ms,
                "last_connect_ms": collector.last_connect_ms,
                "messages": collector.messages,
                "reconnects": collector.reconnects,
                "rows": {
                    "candles_1m": db.scalar("SELECT COUNT(*) FROM candles_1m") or 0,
                    "ticker_snapshots": db.scalar("SELECT COUNT(*) FROM ticker_snapshots") or 0,
                    "orderbook_snapshots": db.scalar("SELECT COUNT(*) FROM orderbook_snapshots") or 0,
                    "trade_buckets_1s": db.scalar("SELECT COUNT(*) FROM trade_buckets_1s") or 0,
                },
            }
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
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
    start_health_server(collector, db)
    log.info("Symbols: %s", ", ".join(SYMBOLS))
    await collector.run_forever()

if __name__ == "__main__":
    asyncio.run(main())
