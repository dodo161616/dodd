import asyncio
import html
import json
import logging
import math
import os
import time
import urllib.parse
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread

import main as core
import trade_engine_v040_demo as v4

log = logging.getLogger("trade-engine")

VERSION = "v0.5.0-auto-demo"

AUTO_DEMO_TRADING_ENABLED = os.getenv(
    "AUTO_DEMO_TRADING_ENABLED", "false"
).strip().lower() in {"1", "true", "yes", "on"}

AUTO_NOTIONAL_USDT = Decimal(os.getenv("AUTO_NOTIONAL_USDT", "25000"))
AUTO_HOLD_SEC = int(os.getenv("AUTO_HOLD_SEC", "300"))
AUTO_ENTRY_THRESHOLD = float(os.getenv("AUTO_ENTRY_THRESHOLD", "30"))
AUTO_EXCEPTION_THRESHOLD = float(os.getenv("AUTO_EXCEPTION_THRESHOLD", "50"))
AUTO_BASE_SLOTS = int(os.getenv("AUTO_BASE_SLOTS", "6"))
AUTO_EXCEPTION_SLOTS = int(os.getenv("AUTO_EXCEPTION_SLOTS", "4"))
AUTO_TOTAL_SLOTS = AUTO_BASE_SLOTS + AUTO_EXCEPTION_SLOTS
AUTO_LOOP_SEC = float(os.getenv("AUTO_LOOP_SEC", "1"))
AUTO_ENTRY_CHECK_SEC = float(os.getenv("AUTO_ENTRY_CHECK_SEC", "5"))
REPORT_UTC_OFFSET_HOURS = int(os.getenv("REPORT_UTC_OFFSET_HOURS", "4"))

AUTO_SCHEMA = """
CREATE TABLE IF NOT EXISTS auto_trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  signal_ts_ms INTEGER NOT NULL,
  opened_ts_ms INTEGER,
  due_ts_ms INTEGER,
  closed_ts_ms INTEGER,
  duration_sec INTEGER NOT NULL,
  notional_usdt REAL NOT NULL,
  leverage INTEGER NOT NULL,
  slot_type TEXT NOT NULL,
  mid_score REAL NOT NULL,
  decision_score REAL,
  best_score REAL,
  agreement REAL,
  persistence REAL,
  conflict INTEGER NOT NULL DEFAULT 0,
  qty TEXT,
  entry_price REAL,
  exit_price REAL,
  entry_order_id TEXT,
  close_order_id TEXT,
  gross_pnl REAL,
  fees REAL,
  net_pnl REAL,
  status TEXT NOT NULL,
  close_reason TEXT,
  error_text TEXT,
  created_ts_ms INTEGER NOT NULL,
  updated_ts_ms INTEGER NOT NULL,
  UNIQUE(symbol, signal_ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_auto_trades_status_due
  ON auto_trades(status, due_ts_ms);
CREATE INDEX IF NOT EXISTS idx_auto_trades_closed
  ON auto_trades(closed_ts_ms);
"""


def ff(v, default=None):
    try:
        if v in (None, ""):
            return default
        return float(v)
    except Exception:
        return default


def local_dt(ts_ms):
    if not ts_ms:
        return None
    tz = timezone(timedelta(hours=REPORT_UTC_OFFSET_HOURS))
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).astimezone(tz)


def local_text(ts_ms, with_date=True):
    dt = local_dt(ts_ms)
    if not dt:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M:%S" if with_date else "%H:%M:%S")


class AutoDemoManager(v4.LiveApproval):
    """Autonomous DEMO-only 5-minute strategy.

    Entry:
      - use V0.4 6–12 minute mid_score
      - abs(mid_score) >= 30 for base 6 slots
      - once 6 slots are occupied, abs(mid_score) >= 50 for 4 exception slots
      - 12/12 coverage, fresh market/signal, no short-vs-long conflict
      - one position per symbol
      - one attempt per symbol/signal batch

    Exit:
      - reduce-only market close after 300 seconds regardless of PnL
      - restart reconciliation closes overdue auto-managed positions
      - positions not created by this auto strategy are never auto-closed
    """

    def __init__(self, db):
        super().__init__(db)
        with db.lock:
            db.conn.executescript(AUTO_SCHEMA)
            db.conn.commit()
        self.cache_lock = Lock()
        self._positions_cache = []
        self._positions_cache_ms = 0
        self.last_entry_check_ms = 0
        self.last_reconcile_ms = 0
        self.last_error = ""
        self.last_tick_ms = 0

    @property
    def auto_ready(self):
        return (
            AUTO_DEMO_TRADING_ENABLED
            and self.client.configured
            and "api-demo.bybit.com" in core.BYBIT_REST_URL
            and v4.DEMO_LEVERAGE == 20
            and AUTO_NOTIONAL_USDT == Decimal("25000")
            and AUTO_HOLD_SEC == 300
            and AUTO_BASE_SLOTS == 6
            and AUTO_EXCEPTION_SLOTS == 4
            and AUTO_TOTAL_SLOTS == 10
        )

    def positions_cached(self, force=False, max_age_ms=1800):
        now = core.now_ms()
        with self.cache_lock:
            if (
                not force
                and self._positions_cache_ms
                and now - self._positions_cache_ms <= max_age_ms
            ):
                return [dict(x) for x in self._positions_cache]

        rows = super().positions()
        with self.cache_lock:
            self._positions_cache = [dict(x) for x in rows]
            self._positions_cache_ms = core.now_ms()
        return rows

    def _update_trade(self, trade_id, **fields):
        if not fields:
            return
        fields["updated_ts_ms"] = core.now_ms()
        keys = list(fields)
        sql = "UPDATE auto_trades SET " + ",".join(f"{k}=?" for k in keys) + " WHERE id=?"
        self.db.execute(sql, tuple(fields[k] for k in keys) + (trade_id,))

    def _trade_row(self, trade_id):
        return self.db.one(
            """SELECT id,symbol,side,signal_ts_ms,opened_ts_ms,due_ts_ms,
                      closed_ts_ms,duration_sec,notional_usdt,leverage,slot_type,
                      mid_score,decision_score,best_score,agreement,persistence,
                      conflict,qty,entry_price,exit_price,entry_order_id,
                      close_order_id,gross_pnl,fees,net_pnl,status,close_reason,
                      error_text,created_ts_ms,updated_ts_ms
               FROM auto_trades WHERE id=?""",
            (trade_id,),
        )

    @staticmethod
    def _row_dict(r):
        if not r:
            return None
        keys = [
            "id","symbol","side","signal_ts_ms","opened_ts_ms","due_ts_ms",
            "closed_ts_ms","duration_sec","notional_usdt","leverage","slot_type",
            "mid_score","decision_score","best_score","agreement","persistence",
            "conflict","qty","entry_price","exit_price","entry_order_id",
            "close_order_id","gross_pnl","fees","net_pnl","status","close_reason",
            "error_text","created_ts_ms","updated_ts_ms",
        ]
        return dict(zip(keys, r))

    def managed_open_rows(self):
        rows = self.db.query(
            """SELECT id,symbol,side,signal_ts_ms,opened_ts_ms,due_ts_ms,
                      closed_ts_ms,duration_sec,notional_usdt,leverage,slot_type,
                      mid_score,decision_score,best_score,agreement,persistence,
                      conflict,qty,entry_price,exit_price,entry_order_id,
                      close_order_id,gross_pnl,fees,net_pnl,status,close_reason,
                      error_text,created_ts_ms,updated_ts_ms
               FROM auto_trades
               WHERE status IN ('OPENING','OPEN_PENDING','OPEN','CLOSING','CLOSE_RETRY')
               ORDER BY opened_ts_ms ASC, id ASC"""
        )
        return [self._row_dict(r) for r in rows]

    def _attempt_exists(self, symbol, signal_ts_ms):
        return bool(
            self.db.one(
                "SELECT 1 FROM auto_trades WHERE symbol=? AND signal_ts_ms=? LIMIT 1",
                (symbol, int(signal_ts_ms)),
            )
        )

    def candidate_ok(self, sig, total_open):
        if not sig:
            return False, None, "missing signal"
        try:
            mid = float(sig.get("mid_score"))
        except Exception:
            return False, None, "no 6-12 score"

        if sig.get("window_count", 0) < sig.get("window_total", 12):
            return False, None, "coverage"
        if bool(sig.get("conflict")):
            return False, None, "conflict"

        now = core.now_ms()
        signal_age = now - int(sig.get("ts_ms") or 0)
        market_age = now - int(sig.get("market_ts_ms") or 0)
        if signal_age < 0 or signal_age > 120_000:
            return False, None, "stale signal"
        if market_age < 0 or market_age > 20_000:
            return False, None, "stale market"

        if total_open < AUTO_BASE_SLOTS:
            if abs(mid) < AUTO_ENTRY_THRESHOLD:
                return False, None, "below base threshold"
            return True, "BASE", ""

        if total_open < AUTO_TOTAL_SLOTS:
            if abs(mid) < AUTO_EXCEPTION_THRESHOLD:
                return False, None, "below exception threshold"
            return True, "EXCEPTION", ""

        return False, None, "slot limit"

    def execution_summary(self, order_id):
        if not order_id:
            return None
        try:
            data = self.client.get(
                "/v5/execution/list",
                {"category": "linear", "orderId": order_id, "limit": 100},
            )
            rows = data.get("result", {}).get("list", [])
        except Exception as exc:
            log.warning("Execution lookup failed order=%s: %s", order_id, exc)
            return None
        qty = 0.0
        value = 0.0
        fee = 0.0
        exec_pnl = 0.0
        for r in rows:
            q = ff(r.get("execQty"), 0.0) or 0.0
            p = ff(r.get("execPrice"), 0.0) or 0.0
            qty += q
            value += q * p
            fee += abs(ff(r.get("execFee"), 0.0) or 0.0)
            exec_pnl += ff(r.get("execPnl"), 0.0) or 0.0
        if qty <= 0:
            return None
        return {
            "qty": qty,
            "vwap": value / qty,
            "fee": fee,
            "exec_pnl": exec_pnl,
        }

    def bybit_closed_pnl(self, trade):
        try:
            data = self.client.get(
                "/v5/position/closed-pnl",
                {
                    "category": "linear",
                    "symbol": trade["symbol"],
                    "limit": 50,
                },
            )
            rows = data.get("result", {}).get("list", [])
        except Exception as exc:
            log.warning("Closed-PnL lookup failed %s: %s", trade["symbol"], exc)
            return None

        opened = int(trade.get("opened_ts_ms") or 0)
        qty_target = ff(trade.get("qty"), 0.0) or 0.0
        candidates = []
        for r in rows:
            updated = int(r.get("updatedTime") or r.get("createdTime") or 0)
            if updated and updated < opened - 30_000:
                continue
            closed_size = ff(r.get("closedSize") or r.get("qty"), 0.0) or 0.0
            qty_diff = abs(closed_size - qty_target) if qty_target else 0.0
            candidates.append((qty_diff, -updated, r))
        if not candidates:
            return None

        candidates.sort(key=lambda x: (x[0], x[1]))
        r = candidates[0][2]
        net = ff(r.get("closedPnl"))
        entry = ff(r.get("avgEntryPrice"))
        exitp = ff(r.get("avgExitPrice"))
        open_fee = abs(ff(r.get("openFee"), 0.0) or 0.0)
        close_fee = abs(ff(r.get("closeFee"), 0.0) or 0.0)
        fees = open_fee + close_fee
        gross = (net + fees) if net is not None else None
        return {
            "entry_price": entry,
            "exit_price": exitp,
            "gross_pnl": gross,
            "fees": fees,
            "net_pnl": net,
        }

    def _fallback_pnl(self, trade):
        entry_exec = self.execution_summary(trade.get("entry_order_id"))
        close_exec = self.execution_summary(trade.get("close_order_id"))
        entry = (
            (entry_exec or {}).get("vwap")
            or ff(trade.get("entry_price"))
        )
        exitp = (close_exec or {}).get("vwap")
        qty = (
            (close_exec or {}).get("qty")
            or (entry_exec or {}).get("qty")
            or ff(trade.get("qty"))
        )
        if not entry or not exitp or not qty:
            return None

        if trade["side"] == "Buy":
            gross = (exitp - entry) * qty
        else:
            gross = (entry - exitp) * qty
        fees = (
            (entry_exec or {}).get("fee", 0.0)
            + (close_exec or {}).get("fee", 0.0)
        )
        return {
            "entry_price": entry,
            "exit_price": exitp,
            "gross_pnl": gross,
            "fees": fees,
            "net_pnl": gross - fees,
        }

    def finalize_pnl(self, trade_id, close_reason=None):
        trade = self._row_dict(self._trade_row(trade_id))
        if not trade:
            return False

        result = self.bybit_closed_pnl(trade)
        if result is None:
            result = self._fallback_pnl(trade)
        if result is None:
            self._update_trade(
                trade_id,
                status="CLOSED_PENDING_PNL",
                closed_ts_ms=trade.get("closed_ts_ms") or core.now_ms(),
                close_reason=close_reason or trade.get("close_reason") or "AUTO_5M",
            )
            return False

        self._update_trade(
            trade_id,
            status="CLOSED",
            closed_ts_ms=trade.get("closed_ts_ms") or core.now_ms(),
            close_reason=close_reason or trade.get("close_reason") or "AUTO_5M",
            entry_price=result.get("entry_price") or trade.get("entry_price"),
            exit_price=result.get("exit_price"),
            gross_pnl=result.get("gross_pnl"),
            fees=result.get("fees"),
            net_pnl=result.get("net_pnl"),
            error_text=None,
        )
        log.warning(
            "AUTO DEMO HISTORY symbol=%s net_pnl=%s gross=%s fees=%s",
            trade["symbol"],
            result.get("net_pnl"),
            result.get("gross_pnl"),
            result.get("fees"),
        )
        return True

    def auto_open(self, sig, slot_type):
        symbol = str(sig["symbol"]).upper()
        signal_ts = int(sig["ts_ms"])
        mid = float(sig["mid_score"])
        side = "Buy" if mid >= 0 else "Sell"

        with self.lock:
            if not self.auto_ready:
                return False
            if self._attempt_exists(symbol, signal_ts):
                return False

            positions = self.positions_cached(force=True)
            if any(p.get("symbol") == symbol for p in positions):
                return False
            if len(positions) >= AUTO_TOTAL_SLOTS:
                return False

            now = core.now_ms()
            self.db.execute(
                """INSERT OR IGNORE INTO auto_trades
                (symbol,side,signal_ts_ms,duration_sec,notional_usdt,leverage,
                 slot_type,mid_score,decision_score,best_score,agreement,
                 persistence,conflict,status,created_ts_ms,updated_ts_ms)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    symbol, side, signal_ts, AUTO_HOLD_SEC,
                    float(AUTO_NOTIONAL_USDT), v4.DEMO_LEVERAGE,
                    slot_type, mid, ff(sig.get("decision_score")),
                    ff(sig.get("best_score")), ff(sig.get("agreement")),
                    ff(sig.get("persistence")), 1 if sig.get("conflict") else 0,
                    "OPENING", now, now,
                ),
            )
            row = self.db.one(
                "SELECT id,status FROM auto_trades WHERE symbol=? AND signal_ts_ms=?",
                (symbol, signal_ts),
            )
            if not row or row[1] != "OPENING":
                return False
            trade_id = int(row[0])

            try:
                _, qty, actual = self.qty_for_notional(symbol, AUTO_NOTIONAL_USDT)
                self.client.set_leverage_demo(symbol)
                result = self.client.market_order(symbol, side, qty, False)
                order_id = result.get("result", {}).get("orderId")
                opened = core.now_ms()
                due = opened + AUTO_HOLD_SEC * 1000

                entry_price = None
                confirmed_qty = qty
                # Confirm against Bybit, but never fabricate an OPEN position.
                for _ in range(6):
                    time.sleep(0.35)
                    ps = self.positions_cached(force=True)
                    p = next((x for x in ps if x.get("symbol") == symbol), None)
                    if p:
                        entry_price = ff(p.get("avg_price"))
                        confirmed_qty = p.get("size") or qty
                        break

                status = "OPEN" if entry_price is not None else "OPEN_PENDING"
                self._update_trade(
                    trade_id,
                    status=status,
                    opened_ts_ms=opened,
                    due_ts_ms=due,
                    qty=str(confirmed_qty),
                    entry_price=entry_price,
                    entry_order_id=order_id,
                    notional_usdt=float(actual),
                    error_text=None,
                )
                log.warning(
                    "AUTO DEMO OPEN symbol=%s side=%s mid_6_12=%s slot=%s "
                    "qty=%s notional≈%.2f due_in=%ss order=%s",
                    symbol, side, mid, slot_type, confirmed_qty,
                    float(actual), AUTO_HOLD_SEC, order_id,
                )
                return True
            except Exception as exc:
                self._update_trade(
                    trade_id,
                    status="OPEN_FAILED",
                    error_text=str(exc)[:500],
                )
                log.error("AUTO DEMO OPEN FAILED %s: %s", symbol, exc)
                return False

    def close_trade(self, trade):
        trade_id = int(trade["id"])
        symbol = trade["symbol"]

        with self.lock:
            latest = self._row_dict(self._trade_row(trade_id))
            if not latest or latest["status"] not in {
                "OPEN","OPEN_PENDING","CLOSE_RETRY","CLOSING"
            }:
                return

            positions = self.positions_cached(force=True)
            p = next((x for x in positions if x.get("symbol") == symbol), None)

            if p is None:
                # It may have been closed externally or a previous close may have filled.
                self._update_trade(
                    trade_id,
                    closed_ts_ms=latest.get("closed_ts_ms") or core.now_ms(),
                    status="CLOSED_PENDING_PNL",
                    close_reason=latest.get("close_reason") or "AUTO_5M",
                )
                self.finalize_pnl(
                    trade_id,
                    latest.get("close_reason") or "AUTO_5M",
                )
                return

            if str(p.get("position_idx")) not in {"0", "None"}:
                self._update_trade(
                    trade_id,
                    status="CLOSE_RETRY",
                    error_text="Hedge mode position refused",
                )
                return

            if latest.get("close_order_id"):
                # A reduce-only close was already submitted. Do not duplicate it.
                return

            close_side = "Sell" if p["side"] == "Buy" else "Buy"
            try:
                result = self.client.market_order(
                    symbol, close_side, p["size"], True
                )
                order_id = result.get("result", {}).get("orderId")
                self._update_trade(
                    trade_id,
                    status="CLOSING",
                    close_order_id=order_id,
                    close_reason="AUTO_5M",
                    error_text=None,
                )
                log.warning(
                    "AUTO DEMO CLOSE SUBMITTED symbol=%s side=%s qty=%s "
                    "reason=AUTO_5M order=%s",
                    symbol, close_side, p["size"], order_id,
                )
            except Exception as exc:
                self._update_trade(
                    trade_id,
                    status="CLOSE_RETRY",
                    error_text=str(exc)[:500],
                )
                log.error("AUTO DEMO CLOSE FAILED %s: %s", symbol, exc)
                return

        # Confirm outside the manager lock.
        for _ in range(8):
            time.sleep(0.35)
            ps = self.positions_cached(force=True)
            if not any(x.get("symbol") == symbol for x in ps):
                self._update_trade(
                    trade_id,
                    closed_ts_ms=core.now_ms(),
                    status="CLOSED_PENDING_PNL",
                    close_reason="AUTO_5M",
                )
                # Allow exchange history a moment to settle.
                time.sleep(0.4)
                self.finalize_pnl(trade_id, "AUTO_5M")
                return

    def reconcile(self):
        """Restart/external-close recovery. Never closes an unmanaged position."""
        managed = self.managed_open_rows()
        if not managed:
            # Still refresh cache so slot accounting reflects manual/external positions.
            try:
                self.positions_cached(force=True)
            except Exception:
                pass
            return

        positions = self.positions_cached(force=True)
        pos_map = {p["symbol"]: p for p in positions}
        now = core.now_ms()

        for trade in managed:
            p = pos_map.get(trade["symbol"])
            status = trade["status"]

            if status in {"OPENING", "OPEN_PENDING"} and p:
                opened = trade.get("opened_ts_ms") or now
                due = trade.get("due_ts_ms") or (opened + AUTO_HOLD_SEC * 1000)
                self._update_trade(
                    trade["id"],
                    status="OPEN",
                    opened_ts_ms=opened,
                    due_ts_ms=due,
                    qty=p.get("size") or trade.get("qty"),
                    entry_price=ff(p.get("avg_price")) or trade.get("entry_price"),
                )
                trade["status"] = "OPEN"
                trade["due_ts_ms"] = due

            if p is None and status in {
                "OPEN","OPEN_PENDING","CLOSING","CLOSE_RETRY"
            }:
                self._update_trade(
                    trade["id"],
                    status="CLOSED_PENDING_PNL",
                    closed_ts_ms=trade.get("closed_ts_ms") or now,
                    close_reason=trade.get("close_reason") or "EXTERNAL_OR_FILLED",
                )
                self.finalize_pnl(
                    trade["id"],
                    trade.get("close_reason") or "EXTERNAL_OR_FILLED",
                )
                continue

            due = int(trade.get("due_ts_ms") or 0)
            if p and due and now >= due:
                # This includes restart recovery: overdue positions are closed immediately.
                self.close_trade(trade)

        # Fill PnL that was not immediately available.
        pending = self.db.query(
            """SELECT id FROM auto_trades
               WHERE status='CLOSED_PENDING_PNL'
                 AND closed_ts_ms IS NOT NULL
                 AND closed_ts_ms>=?
               ORDER BY id DESC LIMIT 30""",
            (now - 86_400_000,),
        )
        for (trade_id,) in pending:
            self.finalize_pnl(int(trade_id))

    def evaluate_entries(self):
        if not self.auto_ready:
            return
        positions = self.positions_cached(force=True)
        total_open = len(positions)
        open_symbols = {p.get("symbol") for p in positions}

        signals = v4.latest_signals(self.db)
        candidates = []
        for sig in signals:
            if sig["symbol"] in open_symbols:
                continue
            if self._attempt_exists(sig["symbol"], sig["ts_ms"]):
                continue
            ok, slot_type, reason = self.candidate_ok(sig, total_open)
            if ok:
                candidates.append(sig)

        # Strongest 6–12 minute metrics receive the available slots first.
        candidates.sort(key=lambda s: abs(float(s.get("mid_score") or 0)), reverse=True)

        for sig in candidates:
            if total_open >= AUTO_TOTAL_SLOTS:
                break
            ok, slot_type, _ = self.candidate_ok(sig, total_open)
            if not ok:
                continue
            if self.auto_open(sig, slot_type):
                total_open += 1
                open_symbols.add(sig["symbol"])

    def tick(self):
        self.last_tick_ms = core.now_ms()
        if not self.auto_ready:
            return
        try:
            now = core.now_ms()
            if now - self.last_reconcile_ms >= 1000:
                self.reconcile()
                self.last_reconcile_ms = now
            if now - self.last_entry_check_ms >= int(AUTO_ENTRY_CHECK_SEC * 1000):
                self.evaluate_entries()
                self.last_entry_check_ms = now
            self.last_error = ""
        except Exception as exc:
            self.last_error = str(exc)[:500]
            log.exception("AUTO DEMO tick failed: %s", exc)

    def dashboard_state(self):
        positions = self.positions_cached(force=False)
        pos_map = {p["symbol"]: p for p in positions}
        managed = self.managed_open_rows()

        open_rows = []
        for t in managed:
            p = pos_map.get(t["symbol"])
            # UI marks the actual exchange state explicitly.
            open_rows.append(
                {
                    **t,
                    "exchange_open": bool(p),
                    "current_mark": ff((p or {}).get("mark_price")),
                    "current_pnl": ff((p or {}).get("unrealised_pnl")),
                    "current_size": (p or {}).get("size"),
                    "current_avg": ff((p or {}).get("avg_price")),
                }
            )

        rows = self.db.query(
            """SELECT id,symbol,side,signal_ts_ms,opened_ts_ms,due_ts_ms,
                      closed_ts_ms,duration_sec,notional_usdt,leverage,slot_type,
                      mid_score,decision_score,best_score,agreement,persistence,
                      conflict,qty,entry_price,exit_price,entry_order_id,
                      close_order_id,gross_pnl,fees,net_pnl,status,close_reason,
                      error_text,created_ts_ms,updated_ts_ms
               FROM auto_trades
               WHERE status IN ('CLOSED','CLOSED_PENDING_PNL')
               ORDER BY closed_ts_ms DESC, id DESC
               LIMIT 200"""
        )
        history = [self._row_dict(r) for r in rows]

        total_net = sum(ff(x.get("net_pnl"), 0.0) or 0.0 for x in history if x["status"] == "CLOSED")
        total_fees = sum(ff(x.get("fees"), 0.0) or 0.0 for x in history if x["status"] == "CLOSED")
        wins = sum(1 for x in history if x["status"] == "CLOSED" and (ff(x.get("net_pnl"), 0.0) or 0.0) > 0)
        losses = sum(1 for x in history if x["status"] == "CLOSED" and (ff(x.get("net_pnl"), 0.0) or 0.0) < 0)
        flats = sum(1 for x in history if x["status"] == "CLOSED" and (ff(x.get("net_pnl"), 0.0) or 0.0) == 0)
        closed_count = wins + losses + flats

        all_closed = self.db.query(
            """SELECT closed_ts_ms,net_pnl
               FROM auto_trades
               WHERE status='CLOSED' AND closed_ts_ms IS NOT NULL
               ORDER BY closed_ts_ms ASC"""
        )
        daily = defaultdict(lambda: {"pnl": 0.0, "trades": 0, "wins": 0, "losses": 0})
        for ts, pnl in all_closed:
            d = local_dt(ts)
            if not d:
                continue
            key = d.strftime("%Y-%m-%d")
            val = ff(pnl, 0.0) or 0.0
            daily[key]["pnl"] += val
            daily[key]["trades"] += 1
            daily[key]["wins"] += 1 if val > 0 else 0
            daily[key]["losses"] += 1 if val < 0 else 0
        daily_rows = [
            {"date": day, **vals}
            for day, vals in sorted(daily.items(), reverse=True)
        ]

        signals = v4.latest_signals(self.db)
        monitor = []
        now = core.now_ms()
        for s in signals:
            mid = ff(s.get("mid_score"))
            if mid is None:
                continue
            monitor.append(
                {
                    "symbol": s["symbol"],
                    "mid_score": mid,
                    "decision_score": ff(s.get("decision_score")),
                    "conflict": bool(s.get("conflict")),
                    "coverage": f"{s.get('window_count',0)}/{s.get('window_total',12)}",
                    "signal_age_sec": max(0, int((now - int(s["ts_ms"])) / 1000)),
                    "market_age_sec": (
                        max(0, int((now - int(s["market_ts_ms"])) / 1000))
                        if s.get("market_ts_ms") else None
                    ),
                    "qualifies_base": abs(mid) >= AUTO_ENTRY_THRESHOLD,
                    "qualifies_exception": abs(mid) >= AUTO_EXCEPTION_THRESHOLD,
                }
            )
        monitor.sort(key=lambda x: abs(x["mid_score"]), reverse=True)

        actual_auto_symbols = {
            x["symbol"] for x in open_rows if x.get("exchange_open")
        }
        return {
            "version": VERSION,
            "server_ms": core.now_ms(),
            "auto_enabled": AUTO_DEMO_TRADING_ENABLED,
            "auto_ready": self.auto_ready,
            "last_tick_ms": self.last_tick_ms,
            "last_error": self.last_error,
            "config": {
                "notional_usdt": float(AUTO_NOTIONAL_USDT),
                "leverage": v4.DEMO_LEVERAGE,
                "hold_sec": AUTO_HOLD_SEC,
                "entry_threshold": AUTO_ENTRY_THRESHOLD,
                "exception_threshold": AUTO_EXCEPTION_THRESHOLD,
                "base_slots": AUTO_BASE_SLOTS,
                "exception_slots": AUTO_EXCEPTION_SLOTS,
                "total_slots": AUTO_TOTAL_SLOTS,
            },
            "exchange_open_count": len(positions),
            "managed_exchange_open_count": len(actual_auto_symbols),
            "open": open_rows,
            "history": history,
            "daily": daily_rows,
            "summary": {
                "total_net_pnl": total_net,
                "total_fees": total_fees,
                "wins": wins,
                "losses": losses,
                "flats": flats,
                "closed_count": closed_count,
                "win_rate": (wins / (wins + losses)) if (wins + losses) else None,
                "avg_net_pnl": (total_net / closed_count) if closed_count else None,
            },
            "monitor": monitor,
        }


def esc(v):
    return html.escape(str(v if v is not None else "—"))


def start_server(collector, db, account, auto):
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
                "frame-ancestors 'none'; form-action 'self'",
            )
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def require_auth(self):
            if v4.auth_ok(self.headers.get("Authorization", "")):
                return True
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="Auto Demo Dashboard"')
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return False

        def health(self):
            return {
                "status": "ok" if collector.last_message_ms else "starting",
                "version": VERSION,
                "mode": "fully_automatic_demo_5m",
                "auto_demo_trading_enabled": AUTO_DEMO_TRADING_ENABLED,
                "auto_ready": auto.auto_ready,
                "demo_only": "api-demo.bybit.com" in core.BYBIT_REST_URL,
                "config": {
                    "notional_usdt": float(AUTO_NOTIONAL_USDT),
                    "leverage": v4.DEMO_LEVERAGE,
                    "hold_sec": AUTO_HOLD_SEC,
                    "entry_threshold_6_12": AUTO_ENTRY_THRESHOLD,
                    "exception_threshold_6_12": AUTO_EXCEPTION_THRESHOLD,
                    "base_slots": AUTO_BASE_SLOTS,
                    "exception_slots": AUTO_EXCEPTION_SLOTS,
                    "total_slots": AUTO_TOTAL_SLOTS,
                },
                "private_api": {
                    "configured": bool(core.BYBIT_API_KEY and core.BYBIT_API_SECRET),
                    "status": account.status,
                    "last_ok_ms": account.last_ok_ms,
                },
                "rows": {
                    "features": db.scalar("SELECT COUNT(*) FROM features") or 0,
                    "labels": db.scalar("SELECT COUNT(*) FROM labels") or 0,
                    "signals": db.scalar("SELECT COUNT(*) FROM signals") or 0,
                    "auto_trades": db.scalar("SELECT COUNT(*) FROM auto_trades") or 0,
                },
            }

        def dashboard(self):
            state = auto.dashboard_state()
            initial = json.dumps(state, separators=(",", ":")).replace("</", "<\\/")
            text = f"""<!doctype html>
<html>
<head>
<meta name='viewport' content='width=device-width,initial-scale=1,viewport-fit=cover'>
<meta name='theme-color' content='#090c11'>
<style>
*{{box-sizing:border-box}}
:root{{--bg:#090c11;--card:#111720;--line:#273140;--text:#f4f7fa;--muted:#8d9aaa;--green:#27ce88;--red:#ff5d69;--orange:#ff9f1a;--blue:#72a7ff;--amber:#f5bd4f}}
body{{margin:0;background:var(--bg);color:var(--text);font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif}}
.shell{{max-width:1040px;margin:auto;padding:16px 12px 44px}}
.top{{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}}
.eyebrow{{font-size:10px;color:var(--orange);font-weight:800;letter-spacing:.08em}}
h1{{margin:4px 0 4px;font-size:24px}} .sub{{font-size:11px;color:var(--muted);line-height:1.5}}
.live{{padding:7px 9px;border-radius:999px;background:#152219;color:var(--green);font-size:10px;font-weight:900}}
.stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin:14px 0}}
.stat,.panel{{background:var(--card);border:1px solid var(--line);border-radius:13px}}
.stat{{padding:10px}} .stat span{{font-size:8px;color:var(--muted);text-transform:uppercase}} .stat b{{display:block;margin-top:4px;font-size:15px}}
.panel{{padding:12px;margin-top:10px}} h2{{font-size:14px;margin:0 0 9px}}
.rule{{font-size:10px;color:var(--muted);line-height:1.6}}
.rule b{{color:#dce5ef}}
.open-grid{{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}}
.trade{{background:#0d131a;border:1px solid var(--line);border-radius:11px;padding:11px}}
.trade-head{{display:flex;justify-content:space-between;align-items:center;gap:8px}}
.sym{{font-weight:900;font-size:14px}} .long{{color:var(--green)}} .short{{color:var(--red)}}
.timer{{font-weight:900;font-size:16px;color:var(--orange)}}
.meta{{display:grid;grid-template-columns:repeat(3,1fr);gap:5px;margin-top:8px}}
.meta div{{background:#121a23;border-radius:8px;padding:6px;font-size:8px;color:var(--muted)}} .meta b{{display:block;font-size:10px;color:var(--text);margin-top:2px}}
.pnl.pos{{color:var(--green)}} .pnl.neg{{color:var(--red)}}
.history-row{{display:grid;grid-template-columns:105px 1fr 70px;gap:8px;align-items:center;padding:9px 0;border-bottom:1px solid var(--line);font-size:10px}}
.history-row:last-child{{border-bottom:0}} .hmeta{{color:var(--muted);font-size:9px;margin-top:2px}}
.daily-row{{display:grid;grid-template-columns:1fr 80px 80px;gap:8px;padding:9px 0;border-bottom:1px solid var(--line);font-size:10px}}
.monitor{{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}}
.mon{{padding:8px;border:1px solid var(--line);border-radius:9px;background:#0d131a}}
.mon .s{{font-size:10px;font-weight:850}} .mon .m{{font-size:15px;font-weight:900;margin-top:3px}} .mon .x{{font-size:8px;color:var(--muted);margin-top:3px}}
.qual{{border-color:rgba(255,159,26,.55)}} .exception{{box-shadow:inset 0 0 0 1px rgba(255,159,26,.7)}}
.empty{{color:var(--muted);font-size:10px;padding:12px 0}}
.footer-total{{margin-top:12px;padding-top:12px;border-top:1px solid var(--line);display:flex;justify-content:space-between;align-items:end}} .footer-total strong{{font-size:23px}}
@media(max-width:700px){{.stats{{grid-template-columns:repeat(2,1fr)}}.open-grid{{grid-template-columns:1fr}}.monitor{{grid-template-columns:repeat(2,1fr)}}.shell{{padding:12px 9px 36px}}}}
</style>
</head>
<body>
<div class='shell'>
  <div class='top'>
    <div><div class='eyebrow'>BYBIT DEMO · FULL AUTO</div><h1>5-Minute Auto Engine</h1>
    <div class='sub'>$25,000 each · 20x · 6–12m ±30 · 6 base slots · ±50 unlocks 4 exception slots · forced close at 5:00</div></div>
    <div class='live' id='live'>LIVE</div>
  </div>
  <div class='stats'>
    <div class='stat'><span>Open</span><b id='openStat'>—</b></div>
    <div class='stat'><span>Total Net PnL</span><b id='totalStat'>—</b></div>
    <div class='stat'><span>Win Rate</span><b id='winStat'>—</b></div>
    <div class='stat'><span>Closed</span><b id='closedStat'>—</b></div>
  </div>
  <div class='panel'>
    <h2>Strategy</h2>
    <div class='rule'>Normal entry: <b>6–12m ≥ +30 LONG / ≤ -30 SHORT</b>. Up to <b>6</b> open positions.
    After 6 slots are occupied, only <b>|6–12m| ≥ 50</b> may use the <b>4 exception slots</b>.
    Requires fresh market data, <b>12/12 coverage</b>, and no V0.4 short-vs-long conflict.
    Every auto-managed trade is <b>reduce-only closed at 300 seconds regardless of profit or loss</b>.</div>
  </div>
  <div class='panel'><h2>OPEN</h2><div id='openGrid' class='open-grid'></div></div>
  <div class='panel'><h2>Signal Monitor · strongest 6–12m</h2><div id='monitor' class='monitor'></div></div>
  <div class='panel'><h2>HISTORY</h2><div id='history'></div>
    <div class='footer-total'><div><div class='sub'>All closed auto trades</div><div id='summaryLine' class='sub'></div></div><strong id='bottomTotal'>—</strong></div>
  </div>
  <div class='panel'><h2>Daily PnL</h2><div id='daily'></div></div>
</div>
<script>
const INITIAL={initial};
let state=INITIAL;
const e=v=>String(v??'—').replace(/[&<>"']/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}})[c]);
const money=v=>v===null||v===undefined?'—':(Number(v)>=0?'+':'-')+'$'+Math.abs(Number(v)).toLocaleString(undefined,{{minimumFractionDigits:2,maximumFractionDigits:2}});
const num=v=>v===null||v===undefined?'—':Number(v).toLocaleString(undefined,{{maximumFractionDigits:4}});
function timer(due){{
  if(!due)return 'CONFIRMING';
  const s=Math.ceil((Number(due)-Date.now())/1000);
  if(s<=0)return 'AUTO CLOSING';
  return Math.floor(s/60)+':'+String(s%60).padStart(2,'0');
}}
function render(s){{
  state=s;
  const cfg=s.config||{{}};
  const actual=(s.open||[]).filter(x=>x.exchange_open).length;
  document.getElementById('openStat').textContent=actual+'/'+cfg.total_slots;
  document.getElementById('totalStat').textContent=money(s.summary.total_net_pnl);
  const wr=s.summary.win_rate;
  document.getElementById('winStat').textContent=wr===null||wr===undefined?'—':(Number(wr)*100).toFixed(1)+'%';
  document.getElementById('closedStat').textContent=s.summary.closed_count||0;
  const og=document.getElementById('openGrid');
  og.innerHTML=(s.open||[]).map(t=>{{
    const pnl=Number(t.current_pnl||0), pc=pnl>=0?'pos':'neg';
    return `<div class="trade">
      <div class="trade-head"><div><div class="sym">${{e(t.symbol)}} <span class="${{t.side==='Buy'?'long':'short'}}">${{t.side==='Buy'?'LONG':'SHORT'}}</span></div>
      <div class="hmeta">${{e(t.slot_type)}} · 6–12 score ${{Number(t.mid_score).toFixed(2)}} · $${{Number(t.notional_usdt).toLocaleString()}}</div></div>
      <div class="timer" data-due="${{e(t.due_ts_ms)}}">${{timer(t.due_ts_ms)}}</div></div>
      <div class="meta">
        <div>Current PnL<b class="pnl ${{pc}}">${{money(t.current_pnl)}}</b></div>
        <div>Entry<b>${{num(t.entry_price||t.current_avg)}}</b></div>
        <div>Mark<b>${{num(t.current_mark)}}</b></div>
        <div>Qty<b>${{e(t.current_size||t.qty)}}</b></div>
        <div>20x<b>${{e(t.leverage)}}x</b></div>
        <div>Exchange<b>${{t.exchange_open?'OPEN':'SYNCING'}}</b></div>
      </div></div>`;
  }}).join('')||'<div class="empty">No auto-managed open trade.</div>';

  const mon=document.getElementById('monitor');
  mon.innerHTML=(s.monitor||[]).slice(0,12).map(x=>{{
    const cls=x.qualifies_exception?'exception qual':x.qualifies_base?'qual':'';
    return `<div class="mon ${{cls}}"><div class="s">${{e(x.symbol)}}</div><div class="m ${{Number(x.mid_score)>=0?'long':'short'}}">${{Number(x.mid_score)>0?'+':''}}${{Number(x.mid_score).toFixed(2)}}</div>
    <div class="x">${{x.conflict?'CONFLICT · ':''}}${{e(x.coverage)}} · signal ${{e(x.signal_age_sec)}}s · market ${{e(x.market_age_sec)}}s</div></div>`;
  }}).join('');

  const hist=document.getElementById('history');
  hist.innerHTML=(s.history||[]).map(t=>{{
    const p=t.net_pnl, pc=Number(p||0)>=0?'pos':'neg';
    const status=t.status==='CLOSED'?'CLOSED':'PNL SYNC';
    return `<div class="history-row"><div>${{new Date(Number(t.closed_ts_ms||t.updated_ts_ms)).toLocaleString()}}</div>
      <div><b>${{e(t.symbol)}} · ${{t.side==='Buy'?'LONG':'SHORT'}}</b><div class="hmeta">6–12 ${{Number(t.mid_score).toFixed(2)}} · $${{Number(t.notional_usdt).toLocaleString()}} · Entry ${{num(t.entry_price)}} → Exit ${{num(t.exit_price)}} · fees $${{Number(t.fees||0).toFixed(2)}} · ${{status}}</div></div>
      <div class="pnl ${{pc}}" style="text-align:right;font-weight:900">${{p===null||p===undefined?'—':money(p)}}</div></div>`;
  }}).join('')||'<div class="empty">No closed auto trade yet.</div>';

  document.getElementById('bottomTotal').textContent=money(s.summary.total_net_pnl);
  document.getElementById('summaryLine').textContent=`Wins ${{s.summary.wins}} · Losses ${{s.summary.losses}} · Fees $${{Number(s.summary.total_fees||0).toFixed(2)}} · Avg ${{money(s.summary.avg_net_pnl)}}`;

  document.getElementById('daily').innerHTML=(s.daily||[]).map(d=>`<div class="daily-row"><div><b>${{e(d.date)}}</b><div class="hmeta">${{d.trades}} trades · ${{d.wins}}W / ${{d.losses}}L</div></div><div></div><div class="pnl ${{Number(d.pnl)>=0?'pos':'neg'}}" style="text-align:right;font-weight:900">${{money(d.pnl)}}</div></div>`).join('')||'<div class="empty">Daily totals will appear after the first close.</div>';
  document.getElementById('live').textContent=s.auto_ready?'AUTO LIVE':'PAUSED';
}}
function tickTimers(){{document.querySelectorAll('.timer').forEach(x=>x.textContent=timer(x.dataset.due));}}
async function poll(){{
  try{{
    const r=await fetch('/auto/state?_='+Date.now(),{{cache:'no-store'}});
    if(!r.ok)throw new Error(r.status);
    render(await r.json());document.getElementById('live').textContent=state.auto_ready?'AUTO LIVE':'PAUSED';
  }}catch(e){{document.getElementById('live').textContent='RETRY';}}
}}
render(INITIAL);setInterval(tickTimers,250);setInterval(poll,2000);
</script>
</body></html>"""
            self.send_page(text)

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            if path in ("/", "/health"):
                self.send_json(self.health())
            elif path == "/signals":
                self.send_json(
                    {
                        "version": VERSION,
                        "server_ms": core.now_ms(),
                        "symbols": core.SYMBOLS,
                        "signals": v4.latest_signals(db),
                    }
                )
            elif path == "/trade":
                if self.require_auth():
                    self.dashboard()
            elif path == "/auto/state":
                if self.require_auth():
                    self.send_json(auto.dashboard_state())
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            # V0.5 has no web-triggered OPEN/CLOSE route.
            self.send_json(
                {"error": "V0.5 auto demo has no manual order endpoint"},
                405,
            )

        def log_message(self, fmt, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", core.PORT), Handler)
    Thread(target=server.serve_forever, daemon=True).start()
    log.info(
        "V0.5 HTTP endpoint on 0.0.0.0:%s /health /signals /trade /auto/state",
        core.PORT,
    )


async def auto_loop(manager):
    await asyncio.sleep(5)
    while True:
        await asyncio.to_thread(manager.tick)
        await asyncio.sleep(AUTO_LOOP_SEC)


async def run():
    if AUTO_DEMO_TRADING_ENABLED and "api-demo.bybit.com" not in core.BYBIT_REST_URL:
        raise RuntimeError("V0.5 hard guard: AUTO trading is DEMO-only")
    if v4.DEMO_LEVERAGE != 20:
        raise RuntimeError("V0.5 guard: DEMO_LEVERAGE must be 20")
    if AUTO_NOTIONAL_USDT != Decimal("25000"):
        raise RuntimeError("V0.5 guard: AUTO_NOTIONAL_USDT must be 25000")
    if AUTO_HOLD_SEC != 300:
        raise RuntimeError("V0.5 guard: AUTO_HOLD_SEC must be 300")
    if AUTO_BASE_SLOTS != 6 or AUTO_EXCEPTION_SLOTS != 4:
        raise RuntimeError("V0.5 guard: slots must be 6 + 4")
    if AUTO_TOTAL_SLOTS != 10:
        raise RuntimeError("V0.5 guard: total slots must be 10")
    if v4.MAX_OPEN_LINEAR_POSITIONS != 10:
        raise RuntimeError("V0.5 guard: MAX_OPEN_LINEAR_POSITIONS must remain 10")
    if v4.MAX_LIVE_NOTIONAL_USDT < AUTO_NOTIONAL_USDT:
        raise RuntimeError("V0.5 guard: max live notional cap is below 25000")

    db = core.Database(core.DB_PATH)
    collector = core.Collector(db)
    account = core.AccountMonitor(db)
    features = v4.SafeFeatureEngine(db)
    auto = AutoDemoManager(db)

    start_server(collector, db, account, auto)

    log.info("trade-engine %s", VERSION)
    log.info(
        "AUTO_DEMO_TRADING_ENABLED=%s auto_ready=%s demo_only=%s "
        "notional=%s leverage=%sx hold=%ss threshold=%s exception=%s "
        "slots=%s+%s=%s",
        AUTO_DEMO_TRADING_ENABLED,
        auto.auto_ready,
        "api-demo.bybit.com" in core.BYBIT_REST_URL,
        AUTO_NOTIONAL_USDT,
        v4.DEMO_LEVERAGE,
        AUTO_HOLD_SEC,
        AUTO_ENTRY_THRESHOLD,
        AUTO_EXCEPTION_THRESHOLD,
        AUTO_BASE_SLOTS,
        AUTO_EXCEPTION_SLOTS,
        AUTO_TOTAL_SLOTS,
    )

    await asyncio.gather(
        collector.run_forever(),
        core.feature_loop(features),
        core.account_loop(account),
        auto_loop(auto),
    )


if __name__ == "__main__":
    asyncio.run(run())
