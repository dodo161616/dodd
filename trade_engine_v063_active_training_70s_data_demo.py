import asyncio
import html
import json
import logging
import math
import os
import statistics
import time
import urllib.parse
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread

import main as core
import trade_engine_v040_demo as v4
import trade_engine_v050_auto_demo as v5

log = logging.getLogger("trade-engine")

VERSION = "v0.6.3-active-training-70s-data-demo"

AUTO_DEMO_TRADING_ENABLED = os.getenv(
    "AUTO_DEMO_TRADING_ENABLED", "false"
).strip().lower() in {"1", "true", "yes", "on"}

AUTO_NOTIONAL_USDT = Decimal(os.getenv("AUTO_NOTIONAL_USDT", "25000"))
AUTO_HOLD_SEC = int(os.getenv("AUTO_HOLD_SEC", "70"))
AUTO_BASE_SLOTS = int(os.getenv("AUTO_BASE_SLOTS", str(len(core.SYMBOLS))))
AUTO_EXCEPTION_SLOTS = int(os.getenv("AUTO_EXCEPTION_SLOTS", "0"))
AUTO_TOTAL_SLOTS = AUTO_BASE_SLOTS + AUTO_EXCEPTION_SLOTS

MICRO_ENTRY_THRESHOLD = float(os.getenv("MICRO_ENTRY_THRESHOLD", "16"))
MICRO_EXCEPTION_THRESHOLD = float(os.getenv("MICRO_EXCEPTION_THRESHOLD", "16"))
MICRO_ENTRY_CHECK_SEC = float(os.getenv("MICRO_ENTRY_CHECK_SEC", "5"))
MICRO_CONFIRMATIONS = int(os.getenv("MICRO_CONFIRMATIONS", "1"))
MICRO_COOLDOWN_SEC = int(os.getenv("MICRO_COOLDOWN_SEC", "0"))

MICRO_MAX_SPREAD_BPS = float(os.getenv("MICRO_MAX_SPREAD_BPS", "20.0"))
MICRO_MIN_DEPTH_MULT = float(os.getenv("MICRO_MIN_DEPTH_MULT", "0.0"))
MICRO_MIN_TURNOVER_60S = float(os.getenv("MICRO_MIN_TURNOVER_60S", "0"))
MICRO_MIN_RANGE_120_BPS = float(os.getenv("MICRO_MIN_RANGE_120_BPS", "0"))
ROUND_TRIP_FEE_BPS = float(os.getenv("ROUND_TRIP_FEE_BPS", "11.0"))

REPORT_UTC_OFFSET_HOURS = int(os.getenv("REPORT_UTC_OFFSET_HOURS", "4"))
OBSERVATION_INTERVAL_SEC = int(os.getenv("MICRO_OBSERVATION_INTERVAL_SEC", "10"))
LEGACY_OBSERVATION_HORIZON_SEC = 120
LEARNING_HORIZON_SEC = 70
LEARNING_LABEL_BATCH = int(os.getenv("LEARNING_LABEL_BATCH", "250"))
HISTORY_SCORE_BAND = float(os.getenv("HISTORY_SCORE_BAND", "12"))
HISTORY_SYMBOL_LIMIT = int(os.getenv("HISTORY_SYMBOL_LIMIT", "300"))
HISTORY_GLOBAL_LIMIT = int(os.getenv("HISTORY_GLOBAL_LIMIT", "900"))
HISTORY_GLOBAL_PRIOR = int(os.getenv("HISTORY_GLOBAL_PRIOR", "25"))
HISTORY_VETO_N = int(os.getenv("HISTORY_VETO_N", "60"))
HISTORY_VETO_NET_BPS = float(os.getenv("HISTORY_VETO_NET_BPS", "-6"))
HISTORY_VETO_HIT = float(os.getenv("HISTORY_VETO_HIT", "0.42"))

MICRO_SCHEMA = """
CREATE TABLE IF NOT EXISTS micro_trade_meta (
  trade_id INTEGER PRIMARY KEY,
  micro_score REAL,
  short_score REAL,
  mid_score REAL,
  long_score REAL,
  open_fee REAL,
  close_fee REAL,
  metrics_json TEXT,
  FOREIGN KEY(trade_id) REFERENCES auto_trades(id)
);

CREATE TABLE IF NOT EXISTS micro_observations (
  symbol TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  score REAL NOT NULL,
  price REAL NOT NULL,
  direction INTEGER NOT NULL,
  metrics_json TEXT NOT NULL,
  future_2m_ret_pct REAL,
  labeled_ts_ms INTEGER,
  PRIMARY KEY(symbol, ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_micro_obs_label
  ON micro_observations(labeled_ts_ms, ts_ms);

CREATE TABLE IF NOT EXISTS micro_outcomes_70s (
  symbol TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  score REAL NOT NULL,
  direction INTEGER NOT NULL,
  ret15_pct REAL,
  ret30_pct REAL,
  ret45_pct REAL,
  ret70_pct REAL,
  dir_ret70_bps REAL,
  mfe70_bps REAL,
  mae70_bps REAL,
  labeled_ts_ms INTEGER NOT NULL,
  PRIMARY KEY(symbol, ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_micro70_profile
  ON micro_outcomes_70s(symbol, direction, score, ts_ms);
CREATE INDEX IF NOT EXISTS idx_micro70_global
  ON micro_outcomes_70s(direction, score, ts_ms);

CREATE TABLE IF NOT EXISTS trade_learning_70s (
  trade_id INTEGER PRIMARY KEY,
  symbol TEXT NOT NULL,
  direction INTEGER NOT NULL,
  entry_score REAL,
  learned_score REAL,
  edge_rank REAL,
  hist_symbol_n INTEGER,
  hist_global_n INTEGER,
  hist_hit_rate REAL,
  hist_avg_dir_bps REAL,
  hist_expected_net_bps REAL,
  hist_mfe_bps REAL,
  hist_mae_bps REAL,
  entry_spread_bps REAL,
  entry_turnover60 REAL,
  entry_depth_notional REAL,
  entry_range70_bps REAL,
  signal_price REAL,
  open_slippage_bps REAL,
  realized_dir_bps REAL,
  realized_net_bps REAL,
  realized_mfe_bps REAL,
  realized_mae_bps REAL,
  updated_ts_ms INTEGER NOT NULL,
  FOREIGN KEY(trade_id) REFERENCES auto_trades(id)
);
CREATE INDEX IF NOT EXISTS idx_trade_learning70_symbol
  ON trade_learning_70s(symbol, updated_ts_ms);
"""


def ff(v, default=None):
    try:
        if v in (None, ""):
            return default
        return float(v)
    except Exception:
        return default


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def sgn(v):
    if v > 0:
        return 1
    if v < 0:
        return -1
    return 0


def pct(a, b):
    if a in (None, 0) or b is None:
        return None
    return (b / a - 1.0) * 100.0


def bps(a, b):
    p = pct(a, b)
    return None if p is None else p * 100.0


def weighted(values):
    total = sum(w for v, w in values if v is not None)
    if total <= 0:
        return 0.0
    return sum(v * w for v, w in values if v is not None) / total


def local_dt(ts_ms):
    if not ts_ms:
        return None
    tz = timezone(timedelta(hours=REPORT_UTC_OFFSET_HOURS))
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).astimezone(tz)


class Micro70SManager(v5.AutoDemoManager):
    def __init__(self, db):
        super().__init__(db)
        with db.lock:
            db.conn.executescript(MICRO_SCHEMA)
            db.conn.commit()

        self.score_history = defaultdict(lambda: deque(maxlen=6))
        self.last_obs_bucket = {}
        self.last_eval_ms = 0
        self.last_label_ms = 0
        self.last_micro_error = ""
        self.state_lock = Lock()
        self._latest_missing_symbols = []
        self._last_missing_log_ms = 0
        self._row_count_cache_ms = 0
        self._row_count_cache = {}

    @property
    def auto_ready(self):
        return (
            AUTO_DEMO_TRADING_ENABLED
            and self.client.configured
            and "api-demo.bybit.com" in core.BYBIT_REST_URL
            and v4.DEMO_LEVERAGE == 20
            and AUTO_NOTIONAL_USDT == Decimal("25000")
            and AUTO_HOLD_SEC == 70
            and AUTO_BASE_SLOTS >= 1
            and AUTO_EXCEPTION_SLOTS == 0
            and AUTO_TOTAL_SLOTS >= len(core.SYMBOLS)
        )

    # ---------- low-level market reads ----------
    def ticker_at(self, symbol, ts_ms):
        return self.db.one(
            """SELECT ts_ms,last_price,open_interest
               FROM ticker_snapshots
               WHERE symbol=? AND ts_ms<=? AND last_price IS NOT NULL
               ORDER BY ts_ms DESC LIMIT 1""",
            (symbol, int(ts_ms)),
        )

    def latest_book(self, symbol, ts_ms):
        return self.db.one(
            """SELECT ts_ms,bid1,ask1,mid,bid_depth_10,ask_depth_10,
                      imbalance_10,microprice,
                      CASE WHEN mid>0 THEN (spread/mid)*10000.0 END
               FROM orderbook_snapshots
               WHERE symbol=? AND ts_ms<=?
               ORDER BY ts_ms DESC LIMIT 1""",
            (symbol, int(ts_ms)),
        )

    def avg_book(self, symbol, start_ms, end_ms):
        return self.db.one(
            """SELECT AVG(imbalance_10),
                      AVG(CASE WHEN mid>0 THEN (spread/mid)*10000.0 END)
               FROM orderbook_snapshots
               WHERE symbol=? AND ts_ms>? AND ts_ms<=?""",
            (symbol, int(start_ms), int(end_ms)),
        )

    def trade_flow(self, symbol, start_ms, end_ms):
        row = self.db.one(
            """SELECT COALESCE(SUM(buy_turnover),0),
                      COALESCE(SUM(sell_turnover),0),
                      COALESCE(SUM(trade_count),0)
               FROM trade_buckets_1s
               WHERE symbol=? AND second_ms>? AND second_ms<=?""",
            (symbol, int(start_ms), int(end_ms)),
        )
        buy = ff(row[0], 0.0) or 0.0
        sell = ff(row[1], 0.0) or 0.0
        total = buy + sell
        imbalance = ((buy - sell) / total) if total > 0 else 0.0
        return {
            "buy": buy,
            "sell": sell,
            "turnover": total,
            "imbalance": imbalance,
            "trades": int(row[2] or 0),
        }

    def range_120_bps(self, symbol, start_ms, end_ms):
        row = self.db.one(
            """SELECT MIN(last_price),MAX(last_price)
               FROM ticker_snapshots
               WHERE symbol=? AND ts_ms>? AND ts_ms<=?
                 AND last_price IS NOT NULL""",
            (symbol, int(start_ms), int(end_ms)),
        )
        lo, hi = ff(row[0]), ff(row[1])
        if not lo or not hi:
            return 0.0
        return (hi / lo - 1.0) * 10000.0

    def v4_context(self):
        return {x["symbol"]: x for x in v4.latest_signals(self.db)}

    def historical_micro_edge(self, symbol, direction, score_abs):
        """70s outcome profile with symbol-specific samples shrunk to a global prior."""
        band = float(HISTORY_SCORE_BAND)
        sym_rows = self.db.query(
            """SELECT dir_ret70_bps,mfe70_bps,mae70_bps
               FROM micro_outcomes_70s
               WHERE symbol=? AND direction=? AND ABS(ABS(score)-?)<=?
               ORDER BY ts_ms DESC LIMIT ?""",
            (symbol,int(direction),float(score_abs),band,int(HISTORY_SYMBOL_LIMIT)),
        )
        global_rows = self.db.query(
            """SELECT dir_ret70_bps,mfe70_bps,mae70_bps
               FROM micro_outcomes_70s
               WHERE symbol<>? AND direction=? AND ABS(ABS(score)-?)<=?
               ORDER BY ts_ms DESC LIMIT ?""",
            (symbol,int(direction),float(score_abs),band,int(HISTORY_GLOBAL_LIMIT)),
        )
        def clean(rows):
            out=[]
            for r,mfe,mae in rows:
                if r is None: continue
                out.append((clamp(float(r),-250,250),clamp(float(mfe or 0),-250,250),clamp(float(mae or 0),-250,250)))
            return out
        def agg(vals):
            if not vals: return None
            rets=[x[0] for x in vals]
            return {
                "n":len(vals),"avg":sum(rets)/len(rets),
                "hit":sum(x>0 for x in rets)/len(rets),
                "net_hit":sum(x>ROUND_TRIP_FEE_BPS for x in rets)/len(rets),
                "mfe":sum(x[1] for x in vals)/len(vals),
                "mae":sum(x[2] for x in vals)/len(vals),
            }
        sa,ga=agg(clean(sym_rows)),agg(clean(global_rows))
        if not sa and not ga:
            return {"n":0,"symbol_n":0,"global_n":0,"direction_hit":None,"net_edge_hit":None,"avg_dir_bps":None,"expected_net_bps":None,"avg_mfe_bps":None,"avg_mae_bps":None,"confidence":0.0}
        sn=sa["n"] if sa else 0; gn=ga["n"] if ga else 0; prior_n=min(int(HISTORY_GLOBAL_PRIOR),gn)
        if sa:
            base=(sa["avg"],sa["hit"],sa["net_hit"],sa["mfe"],sa["mae"])
        else:
            base=(0,0,0,0,0)
        if ga and prior_n>0:
            den=sn+prior_n
            expected=(base[0]*sn+ga["avg"]*prior_n)/den
            hit=(base[1]*sn+ga["hit"]*prior_n)/den
            net_hit=(base[2]*sn+ga["net_hit"]*prior_n)/den
            mfe=(base[3]*sn+ga["mfe"]*prior_n)/den
            mae=(base[4]*sn+ga["mae"]*prior_n)/den
        elif sa:
            expected,hit,net_hit,mfe,mae=sa["avg"],sa["hit"],sa["net_hit"],sa["mfe"],sa["mae"]
        else:
            expected,hit,net_hit,mfe,mae=ga["avg"],ga["hit"],ga["net_hit"],ga["mfe"],ga["mae"]
        confidence=clamp((sn+min(gn*0.25,40.0))/80.0,0.0,1.0)
        return {
            "n":sn,"symbol_n":sn,"global_n":gn,"direction_hit":hit,"net_edge_hit":net_hit,
            "avg_dir_bps":expected,"expected_net_bps":expected-ROUND_TRIP_FEE_BPS,
            "avg_mfe_bps":mfe,"avg_mae_bps":mae,"confidence":confidence,
        }

    # ---------- micro strategy ----------
    def micro_snapshot(self, symbol, context=None):
        now = core.now_ms()
        t0 = self.ticker_at(symbol, now)
        if not t0:
            return None

        ts = int(t0[0])
        px = ff(t0[1])
        oi_now = ff(t0[2])
        if not px:
            return None
        if now - ts > 12_000:
            return None

        def pback(sec):
            row = self.ticker_at(symbol, ts - sec * 1000)
            return ff(row[1]) if row else None

        p5 = pback(5)
        p10 = pback(10)
        p20 = pback(20)
        p40 = pback(40)
        p70 = pback(70)
        p120 = pback(120)

        ret5 = pct(p5, px) or 0.0
        ret10 = pct(p10, px) or 0.0
        ret20 = pct(p20, px) or 0.0
        ret40 = pct(p40, px) or 0.0
        ret70 = pct(p70, px) or 0.0
        ret120 = pct(p120, px) or 0.0

        f10 = self.trade_flow(symbol, ts - 10_000, ts)
        f20 = self.trade_flow(symbol, ts - 20_000, ts)
        f40 = self.trade_flow(symbol, ts - 40_000, ts)
        f60 = self.trade_flow(symbol, ts - 60_000, ts)

        book = self.latest_book(symbol, ts)
        if not book:
            return None
        book_ts, bid1, ask1, mid, bid_depth10, ask_depth10, imb_latest, microprice, spread_bps = book
        if now - int(book_ts) > 12_000:
            return None

        b10 = self.avg_book(symbol, ts - 10_000, ts)
        avg_imb10 = ff(b10[0], 0.0) if b10 else 0.0
        avg_spread10 = ff(b10[1], spread_bps or 0.0) if b10 else (spread_bps or 0.0)

        micro_bps = 0.0
        if ff(mid) and ff(microprice):
            micro_bps = (float(microprice) / float(mid) - 1.0) * 10000.0

        oi60 = None
        old_oi_row = self.ticker_at(symbol, ts - 60_000)
        if old_oi_row:
            old_oi = ff(old_oi_row[2])
            if old_oi and oi_now:
                oi60 = (oi_now / old_oi - 1.0) * 100.0

        btc_now = self.ticker_at("BTCUSDT", ts)
        btc20 = self.ticker_at("BTCUSDT", ts - 20_000)
        btc70 = self.ticker_at("BTCUSDT", ts - 70_000)
        btc_ret20 = pct(ff(btc20[1]) if btc20 else None, ff(btc_now[1]) if btc_now else None) or 0.0
        btc_ret70 = pct(ff(btc70[1]) if btc70 else None, ff(btc_now[1]) if btc_now else None) or 0.0

        ctx = (context or {}).get(symbol, {})
        short_score = ff(ctx.get("short_score"), 0.0) or 0.0
        mid_score = ff(ctx.get("mid_score"), 0.0) or 0.0
        long_score = ff(ctx.get("long_score"), 0.0) or 0.0
        coverage_ok = int(ctx.get("window_count") or 0) >= min(6, int(ctx.get("window_total") or 12))
        conflict = bool(ctx.get("conflict"))

        momentum_signal = weighted([
            (math.tanh(ret5 / 0.04), 0.20),
            (math.tanh(ret10 / 0.06), 0.25),
            (math.tanh(ret20 / 0.09), 0.25),
            (math.tanh(ret40 / 0.14), 0.20),
            (math.tanh(ret70 / 0.20), 0.10),
        ])
        momentum = momentum_signal * 24.0

        flow_signal = weighted([
            (clamp(f10["imbalance"], -1, 1), 0.45),
            (clamp(f20["imbalance"], -1, 1), 0.35),
            (clamp(f40["imbalance"], -1, 1), 0.20),
        ])
        flow = flow_signal * 22.0
        flow_accel_signal = clamp(f10["imbalance"] - f40["imbalance"], -1.0, 1.0)
        flow_accel = flow_accel_signal * 6.0

        book_signal = clamp(weighted([
            (avg_imb10 or 0.0, 0.65),
            (ff(imb_latest, 0.0) or 0.0, 0.35),
        ]), -1, 1)
        book_component = book_signal * 16.0
        micro_component = math.tanh(micro_bps / 1.5) * 7.0

        oi_component = 0.0
        if oi60 is not None and abs(ret20) > 0.00001:
            oi_component = sgn(ret20) * math.tanh(abs(oi60) / 0.15) * 6.0

        btc_component = (
            math.tanh(btc_ret20 / 0.07) * 3.0
            + math.tanh(btc_ret70 / 0.16) * 2.0
        )
        structure_component = clamp(short_score / 100.0, -1, 1) * 8.0

        raw = (
            momentum + flow + flow_accel + book_component + micro_component
            + oi_component + btc_component + structure_component
        )
        raw_dir = sgn(raw)

        spread = ff(spread_bps, 99.0) or 99.0
        spread_penalty = clamp(spread / MICRO_MAX_SPREAD_BPS, 0, 1) * 12.0

        primary_components = [momentum, flow, flow_accel, book_component, micro_component, structure_component]
        aligned = sum(1 for x in primary_components if sgn(x) == raw_dir and abs(x) >= 1.0)
        opposed = sum(1 for x in primary_components if sgn(x) == -raw_dir and abs(x) >= 2.0)
        disagreement_penalty = opposed * 4.0
        if aligned < 3:
            disagreement_penalty += 10.0

        # A burst penalty only; range is recorded for learning, not used as a blanket hard gate.
        range70 = self.range_120_bps(symbol, ts - 70_000, ts)
        range120 = self.range_120_bps(symbol, ts - 120_000, ts)
        ret5_bps = abs(ret5) * 100.0
        exhaustion_penalty = 0.0
        if range70 > 0 and ret5_bps > max(6.0, range70 * 0.70):
            exhaustion_penalty = 6.0

        score_mag = max(
            0.0,
            abs(raw) - spread_penalty - disagreement_penalty - exhaustion_penalty,
        )
        score = clamp(raw_dir * score_mag, -100.0, 100.0)

        direction = sgn(score)
        relevant_depth = (
            (ff(ask_depth10, 0.0) or 0.0) * (ff(mid, px) or px)
            if direction > 0
            else (ff(bid_depth10, 0.0) or 0.0) * (ff(mid, px) or px)
        )
        depth_required = float(AUTO_NOTIONAL_USDT) * MICRO_MIN_DEPTH_MULT
        fee_range_required = max(
            MICRO_MIN_RANGE_120_BPS,
            ROUND_TRIP_FEE_BPS + 2.0 * spread,
        )

        hist = self.historical_micro_edge(symbol, direction or 1, abs(score)) if direction else {
            "n": 0, "direction_hit": None, "net_edge_hit": None, "avg_dir_bps": None
        }

        base_score = score
        history_bonus = 0.0
        if direction and hist.get("avg_dir_bps") is not None:
            confidence = float(hist.get("confidence") or 0.0)
            history_bonus += clamp(hist["avg_dir_bps"] / 5.0, -7.0, 7.0)
            if hist.get("direction_hit") is not None:
                history_bonus += clamp((hist["direction_hit"] - 0.50) * 14.0, -4.0, 4.0)
            history_bonus *= confidence
            score = clamp(score + direction * history_bonus, -100.0, 100.0)
        expected_net_bps = hist.get("expected_net_bps")
        edge_rank = abs(score)
        if expected_net_bps is not None:
            edge_rank += clamp(expected_net_bps, -20.0, 20.0) * 0.35
        if hist.get("direction_hit") is not None:
            edge_rank += clamp((hist["direction_hit"] - 0.50) * 10.0, -3.0, 3.0)
        history_not_hostile = not (
            int(hist.get("symbol_n") or 0) >= HISTORY_VETO_N
            and hist.get("expected_net_bps") is not None
            and hist["expected_net_bps"] <= HISTORY_VETO_NET_BPS
            and hist.get("direction_hit") is not None
            and hist["direction_hit"] <= HISTORY_VETO_HIT
        )

        metrics = {
            "symbol": symbol,
            "ts_ms": ts,
            "price": px,
            "score": round(score, 2),
            "base_score": round(base_score, 2),
            "edge_rank": round(edge_rank, 2),
            "history_bonus": round(history_bonus, 2),
            "raw": round(raw, 2),
            "direction": direction,
            "ret5_pct": round(ret5, 5),
            "ret10_pct": round(ret10, 5),
            "ret20_pct": round(ret20, 5),
            "ret40_pct": round(ret40, 5),
            "ret70_pct": round(ret70, 5),
            "ret120_pct": round(ret120, 5),
            "momentum": round(momentum, 2),
            "flow": round(flow, 2),
            "flow_accel": round(flow_accel, 2),
            "book": round(book_component, 2),
            "microprice": round(micro_component, 2),
            "oi": round(oi_component, 2),
            "btc": round(btc_component, 2),
            "structure": round(structure_component, 2),
            "short_score": round(short_score, 2),
            "mid_score": round(mid_score, 2),
            "long_score": round(long_score, 2),
            "spread_bps": round(spread, 4),
            "range70_bps": round(range70, 3),
            "range120_bps": round(range120, 3),
            "turnover60": round(f60["turnover"], 2),
            "depth10_notional": round(relevant_depth, 2),
            "aligned_components": aligned,
            "opposed_components": opposed,
            "coverage_ok": coverage_ok,
            "conflict": conflict,
            "history": hist,
            "gates": {
                "fresh_market": True,
                "coverage": coverage_ok,
                "no_conflict": not conflict,
                "spread": spread <= MICRO_MAX_SPREAD_BPS,
                "range": range70 >= fee_range_required,
                "turnover": f60["turnover"] >= MICRO_MIN_TURNOVER_60S,
                "depth": relevant_depth >= depth_required,
                "short_not_opposite": not (
                    direction and short_score * direction < -18
                ),
                "mid_not_strongly_opposite": not (
                    direction and mid_score * direction < -28
                ),
                "component_alignment": aligned >= 3 and opposed <= 2,
                "history_not_hostile": history_not_hostile,
            },
        }
        return metrics

    def remember_score(self, m):
        if not m:
            return
        h = self.score_history[m["symbol"]]
        if h and h[-1]["ts_ms"] == m["ts_ms"]:
            h[-1] = m
        else:
            h.append(m)

    def confirmation_ok(self, m, threshold):
        h = list(self.score_history[m["symbol"]])
        if len(h) < MICRO_CONFIRMATIONS:
            return False
        last = h[-MICRO_CONFIRMATIONS:]
        direction = sgn(m["score"])
        if direction == 0:
            return False
        if any(sgn(x["score"]) != direction for x in last):
            return False
        if any(abs(x["score"]) < threshold for x in last):
            return False
        # Make sure confirmations came from distinct market snapshots.
        if len({x["ts_ms"] for x in last}) < MICRO_CONFIRMATIONS:
            return False
        return True

    def recent_close_ms(self, symbol):
        row = self.db.one(
            """SELECT MAX(closed_ts_ms) FROM auto_trades
               WHERE symbol=? AND status IN ('CLOSED','CLOSED_PENDING_PNL')""",
            (symbol,),
        )
        return int(row[0] or 0) if row else 0

    def confirmation_progress(self, m, threshold):
        """Consecutive distinct snapshots above threshold in the same direction."""
        direction = sgn(m.get("score", 0))
        if not direction:
            return 0
        count = 0
        seen = set()
        for item in reversed(list(self.score_history[m["symbol"]])):
            ts = int(item.get("ts_ms") or 0)
            if ts in seen:
                continue
            seen.add(ts)
            if sgn(item.get("score", 0)) != direction:
                break
            if abs(float(item.get("score") or 0)) < threshold:
                break
            count += 1
            if count >= MICRO_CONFIRMATIONS:
                break
        return count

    def diagnose_candidate(self, m, total_open):
        threshold = MICRO_ENTRY_THRESHOLD
        slot_type = "ACTIVE"

        core_keys = (
            "fresh_market", "coverage", "spread", "history_not_hostile",
        )
        soft_keys = (
            "no_conflict", "short_not_opposite", "mid_not_strongly_opposite",
            "component_alignment", "history_not_hostile",
        )
        gates = m.get("gates") or {}
        core_failed = [k for k in core_keys if not gates.get(k, False)]
        soft_failed = [k for k in soft_keys if not gates.get(k, False)]
        soft_pass = len(soft_keys) - len(soft_failed)
        required_soft = 0
        confirmations = self.confirmation_progress(m, threshold)

        reasons = []
        if total_open >= AUTO_TOTAL_SLOTS:
            reasons.append("slot_limit")
        if abs(float(m.get("score") or 0)) < threshold:
            reasons.append("score")
        if core_failed:
            reasons.append("core:" + ",".join(core_failed))
        if soft_pass < required_soft:
            reasons.append("context:" + ",".join(soft_failed))
        if confirmations < MICRO_CONFIRMATIONS:
            reasons.append(f"confirm:{confirmations}/{MICRO_CONFIRMATIONS}")

        closed = self.recent_close_ms(m["symbol"])
        cooldown_left = 0
        if closed:
            cooldown_left = max(
                0,
                MICRO_COOLDOWN_SEC - int((core.now_ms() - closed) / 1000),
            )
            if cooldown_left > 0:
                reasons.append(f"cooldown:{cooldown_left}s")

        return {
            "threshold": threshold,
            "slot_type": slot_type,
            "confirmations": confirmations,
            "confirmations_required": MICRO_CONFIRMATIONS,
            "core_failed": core_failed,
            "soft_failed": soft_failed,
            "soft_pass": soft_pass,
            "soft_required": required_soft,
            "cooldown_left_sec": cooldown_left,
            "ready": not reasons,
            "reason": "READY" if not reasons else " | ".join(reasons),
        }

    def candidate_ok_micro(self, m, total_open):
        if not m or not m.get("direction"):
            return False, None, "no edge"

        d = self.diagnose_candidate(m, total_open)
        if not d["ready"]:
            return False, d["slot_type"], d["reason"]
        return True, d["slot_type"], ""

    # ---------- observation store / self-benchmark ----------
    def store_observation(self, m):
        if not m:
            return
        bucket = (m["ts_ms"] // (OBSERVATION_INTERVAL_SEC * 1000)) * (OBSERVATION_INTERVAL_SEC * 1000)
        if self.last_obs_bucket.get(m["symbol"]) == bucket:
            return
        self.last_obs_bucket[m["symbol"]] = bucket
        self.db.execute(
            """INSERT OR IGNORE INTO micro_observations
               (symbol,ts_ms,score,price,direction,metrics_json)
               VALUES (?,?,?,?,?,?)""",
            (
                m["symbol"],
                int(m["ts_ms"]),
                float(m["score"]),
                float(m["price"]),
                int(m["direction"]),
                json.dumps(m, separators=(",", ":")),
            ),
        )

    def label_observations(self):
        now = core.now_ms()
        rows = self.db.query(
            """SELECT symbol,ts_ms,price
               FROM micro_observations
               WHERE labeled_ts_ms IS NULL
                 AND ts_ms<=?
               ORDER BY ts_ms ASC LIMIT 500""",
            (now - LEGACY_OBSERVATION_HORIZON_SEC * 1000,),
        )
        for symbol, ts_ms, entry in rows:
            target = int(ts_ms) + LEGACY_OBSERVATION_HORIZON_SEC * 1000
            fut = self.ticker_at(symbol, target)
            if not fut or not ff(fut[1]):
                continue
            ret = (float(fut[1]) / float(entry) - 1.0) * 100.0
            self.db.execute(
                """UPDATE micro_observations
                   SET future_2m_ret_pct=?,labeled_ts_ms=?
                   WHERE symbol=? AND ts_ms=?""",
                (ret, now, symbol, int(ts_ms)),
            )

    def _path_outcome_70s(self, symbol, ts_ms, entry_price, direction):
        end_ms = int(ts_ms) + 78_000
        rows = self.db.query(
            """SELECT ts_ms,last_price FROM ticker_snapshots
               WHERE symbol=? AND ts_ms>? AND ts_ms<=? AND last_price IS NOT NULL
               ORDER BY ts_ms ASC""",
            (symbol,int(ts_ms),end_ms),
        )
        pts=[(int(t),ff(p)) for t,p in rows if ff(p)]
        if not pts: return None
        def first_at(delta):
            target=int(ts_ms)+delta*1000
            for t,p in pts:
                if t>=target and t<=target+8000: return p
            return None
        p15,p30,p45,p70=first_at(15),first_at(30),first_at(45),first_at(70)
        if p70 is None: return None
        path=[p for t,p in pts if t<=int(ts_ms)+70_000 and p is not None]
        if not path: return None
        def raw_ret(p): return None if p is None else (float(p)/float(entry_price)-1.0)*100.0
        moves=[(float(p)/float(entry_price)-1.0)*10000.0*int(direction) for p in path]
        return {
            "ret15_pct":raw_ret(p15),"ret30_pct":raw_ret(p30),"ret45_pct":raw_ret(p45),"ret70_pct":raw_ret(p70),
            "dir_ret70_bps":(float(p70)/float(entry_price)-1.0)*10000.0*int(direction),
            "mfe70_bps":max(moves),"mae70_bps":min(moves),
        }

    def label_70s_observations(self):
        now=core.now_ms()
        rows=self.db.query(
            """SELECT o.symbol,o.ts_ms,o.score,o.price,o.direction
               FROM micro_observations o
               WHERE o.ts_ms<=? AND NOT EXISTS (
                 SELECT 1 FROM micro_outcomes_70s x WHERE x.symbol=o.symbol AND x.ts_ms=o.ts_ms
               )
               ORDER BY o.ts_ms ASC LIMIT ?""",
            (now-LEARNING_HORIZON_SEC*1000,int(LEARNING_LABEL_BATCH)),
        )
        done=0
        for symbol,ts_ms,score,entry,direction in rows:
            if not entry or not direction: continue
            o=self._path_outcome_70s(symbol,int(ts_ms),float(entry),int(direction))
            if not o: continue
            self.db.execute(
                """INSERT OR IGNORE INTO micro_outcomes_70s
                   (symbol,ts_ms,score,direction,ret15_pct,ret30_pct,ret45_pct,ret70_pct,dir_ret70_bps,mfe70_bps,mae70_bps,labeled_ts_ms)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol,int(ts_ms),float(score),int(direction),o["ret15_pct"],o["ret30_pct"],o["ret45_pct"],o["ret70_pct"],o["dir_ret70_bps"],o["mfe70_bps"],o["mae70_bps"],now),
            )
            done+=1
        if done: log.info("MICRO 70S LEARNING backfilled=%s",done)

    def _actual_trade_path(self, symbol, opened_ms, closed_ms, entry_price, direction):
        if not opened_ms or not closed_ms or not entry_price: return None
        rows=self.db.query(
            """SELECT last_price FROM ticker_snapshots
               WHERE symbol=? AND ts_ms>=? AND ts_ms<=? AND last_price IS NOT NULL
               ORDER BY ts_ms ASC""",
            (symbol,int(opened_ms),int(closed_ms)),
        )
        vals=[ff(r[0]) for r in rows if ff(r[0])]
        if not vals: return None
        moves=[(float(p)/float(entry_price)-1.0)*10000.0*int(direction) for p in vals]
        return max(moves),min(moves)

    def update_trade_learning_outcome(self, trade_id):
        trade=self._row_dict(self._trade_row(trade_id))
        if not trade or trade.get("status")!="CLOSED": return
        direction=1 if trade.get("side")=="Buy" else -1
        entry=ff(trade.get("entry_price")); exitp=ff(trade.get("exit_price")); notional=ff(trade.get("notional_usdt"),float(AUTO_NOTIONAL_USDT))
        meta=self.db.one("SELECT metrics_json FROM micro_trade_meta WHERE trade_id=?",(int(trade_id),))
        metrics={}
        if meta and meta[0]:
            try: metrics=json.loads(meta[0])
            except Exception: metrics={}
        signal_price=ff(metrics.get("price"))
        open_slippage=((entry/signal_price-1.0)*10000.0*direction) if entry and signal_price else None
        realized_dir=((exitp/entry-1.0)*10000.0*direction) if entry and exitp else None
        realized_net=(float(trade["net_pnl"])/notional*10000.0) if notional and trade.get("net_pnl") is not None else None
        path=self._actual_trade_path(trade["symbol"],trade.get("opened_ts_ms"),trade.get("closed_ts_ms"),entry,direction)
        mfe=path[0] if path else None; mae=path[1] if path else None
        self.db.execute(
            """UPDATE trade_learning_70s SET open_slippage_bps=?,realized_dir_bps=?,realized_net_bps=?,realized_mfe_bps=?,realized_mae_bps=?,updated_ts_ms=? WHERE trade_id=?""",
            (open_slippage,realized_dir,realized_net,mfe,mae,core.now_ms(),int(trade_id)),
        )

    def cached_row_counts(self):
        now=core.now_ms()
        if self._row_count_cache and now-self._row_count_cache_ms<30_000: return self._row_count_cache
        counts={
            "ticker":self.db.scalar("SELECT COUNT(*) FROM ticker_snapshots") or 0,
            "book":self.db.scalar("SELECT COUNT(*) FROM orderbook_snapshots") or 0,
            "trades_1s":self.db.scalar("SELECT COUNT(*) FROM trade_buckets_1s") or 0,
            "features":self.db.scalar("SELECT COUNT(*) FROM features") or 0,
            "labels":self.db.scalar("SELECT COUNT(*) FROM labels") or 0,
            "micro_obs":self.db.scalar("SELECT COUNT(*) FROM micro_observations") or 0,
            "micro_labeled":self.db.scalar("SELECT COUNT(*) FROM micro_observations WHERE labeled_ts_ms IS NOT NULL") or 0,
            "micro70":self.db.scalar("SELECT COUNT(*) FROM micro_outcomes_70s") or 0,
            "trade_learning70":self.db.scalar("SELECT COUNT(*) FROM trade_learning_70s") or 0,
        }
        self._row_count_cache=counts; self._row_count_cache_ms=now
        return counts

    # ---------- execution ----------
    def auto_open_micro(self, m, slot_type):
        symbol = m["symbol"]
        side = "Buy" if m["score"] > 0 else "Sell"
        signal_ts = int(m["ts_ms"])

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
                    slot_type, float(m["score"]), float(m["score"]),
                    float(m["raw"]),
                    None, None, 1 if m.get("conflict") else 0,
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

            self.db.execute(
                """INSERT OR REPLACE INTO micro_trade_meta
                   (trade_id,micro_score,short_score,mid_score,long_score,metrics_json)
                   VALUES (?,?,?,?,?,?)""",
                (
                    trade_id, float(m["score"]),
                    float(m["short_score"]), float(m["mid_score"]),
                    float(m["long_score"]),
                    json.dumps(m, separators=(",", ":")),
                ),
            )

            hist=m.get("history") or {}
            self.db.execute(
                """INSERT OR REPLACE INTO trade_learning_70s
                   (trade_id,symbol,direction,entry_score,learned_score,edge_rank,hist_symbol_n,hist_global_n,hist_hit_rate,hist_avg_dir_bps,hist_expected_net_bps,hist_mfe_bps,hist_mae_bps,entry_spread_bps,entry_turnover60,entry_depth_notional,entry_range70_bps,signal_price,updated_ts_ms)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (trade_id,symbol,1 if side=="Buy" else -1,ff(m.get("base_score"),m.get("score")),ff(m.get("score")),ff(m.get("edge_rank")),int(hist.get("symbol_n") or 0),int(hist.get("global_n") or 0),ff(hist.get("direction_hit")),ff(hist.get("avg_dir_bps")),ff(hist.get("expected_net_bps")),ff(hist.get("avg_mfe_bps")),ff(hist.get("avg_mae_bps")),ff(m.get("spread_bps")),ff(m.get("turnover60")),ff(m.get("depth10_notional")),ff(m.get("range70_bps")),ff(m.get("price")),core.now_ms()),
            )

            try:
                _, qty, actual = self.qty_for_notional(symbol, AUTO_NOTIONAL_USDT)
                self.client.set_leverage_demo(symbol)
                result = self.client.market_order(symbol, side, qty, False)
                order_id = result.get("result", {}).get("orderId")
                opened = core.now_ms()
                due = opened + AUTO_HOLD_SEC * 1000

                entry_price = None
                confirmed_qty = qty
                for _ in range(6):
                    time.sleep(0.30)
                    ps = self.positions_cached(force=True)
                    p = next((x for x in ps if x.get("symbol") == symbol), None)
                    if p:
                        entry_price = ff(p.get("avg_price"))
                        confirmed_qty = p.get("size") or qty
                        break

                entry_exec = self.execution_summary(order_id)
                open_fee = (entry_exec or {}).get("fee")
                if entry_exec and entry_exec.get("vwap"):
                    entry_price = entry_exec["vwap"]

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
                if open_fee is not None:
                    self.db.execute(
                        "UPDATE micro_trade_meta SET open_fee=? WHERE trade_id=?",
                        (float(open_fee), trade_id),
                    )

                log.warning(
                    "MICRO 70S OPEN symbol=%s side=%s score=%.2f slot=%s "
                    "qty=%s notional≈%.2f hold=%ss spread=%.3fbps "
                    "range70=%.1fbps histN=%s expNet=%s depth=%.0f order=%s",
                    symbol, side, m["score"], slot_type, confirmed_qty,
                    float(actual), AUTO_HOLD_SEC, m["spread_bps"],
                    m["range70_bps"], int((m.get("history") or {}).get("symbol_n") or 0),
                    (m.get("history") or {}).get("expected_net_bps"), m["depth10_notional"], order_id,
                )
                return True
            except Exception as exc:
                self._update_trade(
                    trade_id,
                    status="OPEN_FAILED",
                    error_text=str(exc)[:500],
                )
                log.error("MICRO 70S OPEN FAILED %s: %s", symbol, exc)
                return False

    def finalize_pnl_micro(self, trade_id, close_reason=None):
        trade = self._row_dict(self._trade_row(trade_id))
        if not trade:
            return False

        entry_exec = self.execution_summary(trade.get("entry_order_id"))
        close_exec = self.execution_summary(trade.get("close_order_id"))

        if entry_exec and close_exec:
            entry = entry_exec.get("vwap")
            exitp = close_exec.get("vwap")
            qty = min(
                entry_exec.get("qty") or 0.0,
                close_exec.get("qty") or 0.0,
            )
            if entry and exitp and qty:
                gross = (
                    (exitp - entry) * qty
                    if trade["side"] == "Buy"
                    else (entry - exitp) * qty
                )
                open_fee = entry_exec.get("fee", 0.0) or 0.0
                close_fee = close_exec.get("fee", 0.0) or 0.0
                fees = open_fee + close_fee
                net = gross - fees

                self._update_trade(
                    trade_id,
                    status="CLOSED",
                    closed_ts_ms=trade.get("closed_ts_ms") or core.now_ms(),
                    close_reason=close_reason or "AUTO_70S",
                    entry_price=entry,
                    exit_price=exitp,
                    gross_pnl=gross,
                    fees=fees,
                    net_pnl=net,
                    error_text=None,
                )
                self.db.execute(
                    """UPDATE micro_trade_meta
                       SET open_fee=?,close_fee=? WHERE trade_id=?""",
                    (open_fee, close_fee, trade_id),
                )
                log.warning(
                    "MICRO 70S HISTORY symbol=%s gross=%.4f open_fee=%.4f "
                    "close_fee=%.4f net=%.4f",
                    trade["symbol"], gross, open_fee, close_fee, net,
                )
                self.update_trade_learning_outcome(trade_id)
                return True

        # Fallback to Bybit closed-PnL logic from V0.5 if one order's execution history is delayed.
        ok = super().finalize_pnl(trade_id, close_reason or "AUTO_70S")
        if ok:
            row = self.db.one(
                "SELECT fees FROM auto_trades WHERE id=?", (trade_id,)
            )
            total_fee = ff(row[0], 0.0) if row else 0.0
            self.db.execute(
                """UPDATE micro_trade_meta
                   SET open_fee=COALESCE(open_fee,?),
                       close_fee=COALESCE(close_fee,?)
                   WHERE trade_id=?""",
                ((total_fee or 0.0) / 2, (total_fee or 0.0) / 2, trade_id),
            )
            self.update_trade_learning_outcome(trade_id)
        return ok

    def close_trade_micro(self, trade):
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
                self._update_trade(
                    trade_id,
                    closed_ts_ms=latest.get("closed_ts_ms") or core.now_ms(),
                    status="CLOSED_PENDING_PNL",
                    close_reason=latest.get("close_reason") or "AUTO_70S",
                )
                self.finalize_pnl_micro(
                    trade_id, latest.get("close_reason") or "AUTO_70S"
                )
                return

            if latest.get("close_order_id"):
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
                    close_reason="AUTO_70S",
                    error_text=None,
                )
                log.warning(
                    "MICRO 70S CLOSE SUBMITTED symbol=%s side=%s qty=%s "
                    "reason=AUTO_70S order=%s",
                    symbol, close_side, p["size"], order_id,
                )
            except Exception as exc:
                self._update_trade(
                    trade_id,
                    status="CLOSE_RETRY",
                    error_text=str(exc)[:500],
                )
                log.error("MICRO 70S CLOSE FAILED %s: %s", symbol, exc)
                return

        for _ in range(8):
            time.sleep(0.30)
            ps = self.positions_cached(force=True)
            if not any(x.get("symbol") == symbol for x in ps):
                self._update_trade(
                    trade_id,
                    closed_ts_ms=core.now_ms(),
                    status="CLOSED_PENDING_PNL",
                    close_reason="AUTO_70S",
                )
                time.sleep(0.35)
                self.finalize_pnl_micro(trade_id, "AUTO_70S")
                return

    def reconcile_micro(self):
        managed = self.managed_open_rows()
        positions = self.positions_cached(force=True)
        pos_map = {p["symbol"]: p for p in positions}
        now = core.now_ms()

        for trade in managed:
            p = pos_map.get(trade["symbol"])
            status = trade["status"]

            if status in {"OPENING","OPEN_PENDING"} and p:
                opened = trade.get("opened_ts_ms") or now
                # Preserve existing due_ts across deploys. Only missing due gets the new 70s horizon.
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
                self.finalize_pnl_micro(
                    trade["id"],
                    trade.get("close_reason") or "EXTERNAL_OR_FILLED",
                )
                continue

            due = int(trade.get("due_ts_ms") or 0)
            if p and due and now >= due:
                self.close_trade_micro(trade)

        pending = self.db.query(
            """SELECT id FROM auto_trades
               WHERE status='CLOSED_PENDING_PNL'
                 AND closed_ts_ms IS NOT NULL
                 AND closed_ts_ms>=?
               ORDER BY id DESC LIMIT 40""",
            (now - 86_400_000,),
        )
        for (trade_id,) in pending:
            self.finalize_pnl_micro(int(trade_id))

    def evaluate_entries_micro(self):
        if not self.auto_ready:
            return

        positions = self.positions_cached(force=True)
        total_open = len(positions)
        open_symbols = {p.get("symbol") for p in positions}
        context = self.v4_context()

        snapshots = []
        missing_symbols = []
        for symbol in core.SYMBOLS:
            try:
                m = self.micro_snapshot(symbol, context)
                if not m:
                    missing_symbols.append(symbol)
                    continue
                self.remember_score(m)
                self.store_observation(m)
                # Attach live diagnostics so admin shows exactly why a coin is blocked.
                m["diagnostic"] = self.diagnose_candidate(m, total_open)
                snapshots.append(m)
            except Exception as exc:
                log.debug("micro snapshot failed %s: %s", symbol, exc)

        snapshots.sort(key=lambda x: float(x.get("edge_rank") or abs(x["score"])), reverse=True)

        # Human-readable gate diagnostics every 15 seconds.
        now = core.now_ms()
        last_diag = getattr(self, "_last_diag_log_ms", 0)
        if now - last_diag >= 15_000:
            for m in snapshots[:8]:
                d = m.get("diagnostic") or {}
                log.info(
                    "MICRO 70S CHECK %s score=%+.2f rank=%.2f conf=%s/%s ready=%s "
                    "reason=%s histN=%s expNet=%s spread=%.3fbps range70=%.1fbps turn60=%.0f depth=%.0f",
                    m["symbol"], m["score"], m.get("edge_rank", abs(m["score"])),
                    d.get("confirmations", 0), d.get("confirmations_required", MICRO_CONFIRMATIONS),
                    d.get("ready", False), d.get("reason", "unknown"),
                    int((m.get("history") or {}).get("symbol_n") or 0),
                    (m.get("history") or {}).get("expected_net_bps"),
                    m.get("spread_bps", 0.0), m.get("range70_bps", 0.0),
                    m.get("turnover60", 0.0), m.get("depth10_notional", 0.0),
                )
            self._last_diag_log_ms = now

        # Persist diagnostics for admin polling.
        self._latest_micro_snapshots = snapshots
        self._latest_missing_symbols = missing_symbols
        if missing_symbols and now - self._last_missing_log_ms >= 60_000:
            log.warning("MICRO DATA MISSING symbols=%s", ",".join(missing_symbols))
            self._last_missing_log_ms = now

        for m in snapshots:
            if total_open >= AUTO_TOTAL_SLOTS:
                break
            if m["symbol"] in open_symbols:
                continue

            # Re-diagnose after each successful OPEN because slot tier can change.
            m["diagnostic"] = self.diagnose_candidate(m, total_open)
            ok, slot_type, reason = self.candidate_ok_micro(m, total_open)
            if not ok:
                continue
            if self.auto_open_micro(m, slot_type):
                total_open += 1
                open_symbols.add(m["symbol"])

    def tick(self):
        self.last_tick_ms = core.now_ms()
        if not self.auto_ready:
            return
        try:
            self.reconcile_micro()
            now = core.now_ms()

            if now - self.last_eval_ms >= int(MICRO_ENTRY_CHECK_SEC * 1000):
                self.evaluate_entries_micro()
                self.last_eval_ms = now

            if now - self.last_label_ms >= 10_000:
                self.label_70s_observations()
                self.label_observations()
                self.last_label_ms = now

            self.last_error = ""
            self.last_micro_error = ""
        except Exception as exc:
            self.last_error = str(exc)[:500]
            self.last_micro_error = self.last_error
            log.exception("MICRO 70S tick failed: %s", exc)

    # ---------- admin state ----------
    def joined_history(self, limit=250):
        rows = self.db.query(
            """SELECT a.id,a.symbol,a.side,a.signal_ts_ms,a.opened_ts_ms,a.due_ts_ms,
                      a.closed_ts_ms,a.duration_sec,a.notional_usdt,a.leverage,a.slot_type,
                      a.mid_score,a.decision_score,a.best_score,a.agreement,a.persistence,
                      a.conflict,a.qty,a.entry_price,a.exit_price,a.entry_order_id,
                      a.close_order_id,a.gross_pnl,a.fees,a.net_pnl,a.status,a.close_reason,
                      a.error_text,a.created_ts_ms,a.updated_ts_ms,
                      m.micro_score,m.short_score,m.mid_score,m.long_score,
                      m.open_fee,m.close_fee,m.metrics_json
               FROM auto_trades a
               LEFT JOIN micro_trade_meta m ON m.trade_id=a.id
               WHERE a.status IN ('CLOSED','CLOSED_PENDING_PNL')
               ORDER BY a.closed_ts_ms DESC,a.id DESC LIMIT ?""",
            (int(limit),),
        )
        keys = [
            "id","symbol","side","signal_ts_ms","opened_ts_ms","due_ts_ms",
            "closed_ts_ms","duration_sec","notional_usdt","leverage","slot_type",
            "legacy_mid_score","decision_score","best_score","agreement","persistence",
            "conflict","qty","entry_price","exit_price","entry_order_id","close_order_id",
            "gross_pnl","fees","net_pnl","status","close_reason","error_text",
            "created_ts_ms","updated_ts_ms","micro_score","short_score","mid_score",
            "long_score","open_fee","close_fee","metrics_json",
        ]
        return [dict(zip(keys, r)) for r in rows]

    def admin_state(self):
        positions = self.positions_cached(force=False)
        pos_map = {p["symbol"]: p for p in positions}
        managed = self.managed_open_rows()

        meta_rows = self.db.query(
            """SELECT trade_id,micro_score,short_score,mid_score,long_score,
                      open_fee,close_fee,metrics_json
               FROM micro_trade_meta"""
        )
        meta = {
            r[0]: {
                "micro_score": r[1], "short_score": r[2], "mid_score": r[3],
                "long_score": r[4], "open_fee": r[5], "close_fee": r[6],
                "metrics_json": r[7],
            }
            for r in meta_rows
        }

        open_rows = []
        for t in managed:
            p = pos_map.get(t["symbol"])
            open_rows.append({
                **t,
                **meta.get(t["id"], {}),
                "exchange_open": bool(p),
                "current_mark": ff((p or {}).get("mark_price")),
                "current_pnl": ff((p or {}).get("unrealised_pnl")),
                "current_size": (p or {}).get("size"),
                "current_avg": ff((p or {}).get("avg_price")),
            })

        history = self.joined_history()
        closed = [x for x in history if x["status"] == "CLOSED" and x["net_pnl"] is not None]

        total_net = sum(ff(x["net_pnl"], 0.0) or 0.0 for x in closed)
        total_gross = sum(ff(x["gross_pnl"], 0.0) or 0.0 for x in closed)
        total_fees = sum(ff(x["fees"], 0.0) or 0.0 for x in closed)
        wins = sum((ff(x["net_pnl"], 0.0) or 0.0) > 0 for x in closed)
        losses = sum((ff(x["net_pnl"], 0.0) or 0.0) < 0 for x in closed)
        gross_wins = sum((ff(x["gross_pnl"], 0.0) or 0.0) > 0 for x in closed)

        daily = defaultdict(lambda: {"net":0.0,"gross":0.0,"fees":0.0,"trades":0,"wins":0})
        all_closed = self.db.query(
            """SELECT closed_ts_ms,gross_pnl,fees,net_pnl
               FROM auto_trades
               WHERE status='CLOSED' AND closed_ts_ms IS NOT NULL
               ORDER BY closed_ts_ms ASC"""
        )
        for ts, gross, fees, net in all_closed:
            dt = local_dt(ts)
            if not dt:
                continue
            k = dt.strftime("%Y-%m-%d")
            d = daily[k]
            d["gross"] += ff(gross,0.0) or 0.0
            d["fees"] += ff(fees,0.0) or 0.0
            d["net"] += ff(net,0.0) or 0.0
            d["trades"] += 1
            d["wins"] += 1 if (ff(net,0.0) or 0.0) > 0 else 0

        context = self.v4_context()
        monitor = []
        cached = {
            x["symbol"]: x
            for x in getattr(self, "_latest_micro_snapshots", [])
        }
        for symbol in core.SYMBOLS:
            m = cached.get(symbol)
            if not m:
                h = self.score_history.get(symbol)
                if h:
                    m = h[-1]
            if not m:
                try:
                    m = self.micro_snapshot(symbol, context)
                except Exception:
                    m = None
            if m:
                if "diagnostic" not in m:
                    m["diagnostic"] = self.diagnose_candidate(
                        m, len(positions)
                    )
                monitor.append(m)
        monitor.sort(key=lambda x: abs(x["score"]), reverse=True)

        row_counts = self.cached_row_counts()

        return {
            "version": VERSION,
            "server_ms": core.now_ms(),
            "auto_ready": self.auto_ready,
            "auto_enabled": AUTO_DEMO_TRADING_ENABLED,
            "exchange_open_count": len(positions),
            "open": open_rows,
            "history": history,
            "monitor": monitor,
            "missing_symbols": list(self._latest_missing_symbols),
            "daily": [{"date":k, **v} for k,v in sorted(daily.items(), reverse=True)],
            "summary": {
                "total_net": total_net,
                "total_gross": total_gross,
                "total_fees": total_fees,
                "closed": len(closed),
                "wins": wins,
                "losses": losses,
                "net_win_rate": wins / (wins + losses) if wins + losses else None,
                "gross_win_rate": gross_wins / len(closed) if closed else None,
                "avg_net": total_net / len(closed) if closed else None,
            },
            "config": {
                "notional": float(AUTO_NOTIONAL_USDT),
                "leverage": v4.DEMO_LEVERAGE,
                "hold_sec": AUTO_HOLD_SEC,
                "base_slots": AUTO_BASE_SLOTS,
                "exception_slots": AUTO_EXCEPTION_SLOTS,
                "micro_threshold": MICRO_ENTRY_THRESHOLD,
                "exception_threshold": MICRO_EXCEPTION_THRESHOLD,
                "confirmations": MICRO_CONFIRMATIONS,
                "max_spread_bps": MICRO_MAX_SPREAD_BPS,
                "min_range70_bps": MICRO_MIN_RANGE_120_BPS,
                "min_turnover60": MICRO_MIN_TURNOVER_60S,
                "min_depth_multiple": MICRO_MIN_DEPTH_MULT,
                "round_trip_fee_bps": ROUND_TRIP_FEE_BPS,
            },
            "rows": row_counts,
            "last_error": self.last_error,
        }


def start_server(collector, db, account, manager):
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
            self.send_header("WWW-Authenticate", 'Basic realm="Micro 70S Admin"')
            self.end_headers()
            return False

        def health(self):
            return {
                "status": "ok" if collector.last_message_ms else "starting",
                "version": VERSION,
                "mode": "fully_automatic_demo_micro_70s_learned",
                "auto_ready": manager.auto_ready,
                "demo_only": "api-demo.bybit.com" in core.BYBIT_REST_URL,
                "hold_sec": AUTO_HOLD_SEC,
                "notional": float(AUTO_NOTIONAL_USDT),
                "leverage": v4.DEMO_LEVERAGE,
                "private_api": {
                    "configured": bool(core.BYBIT_API_KEY and core.BYBIT_API_SECRET),
                    "status": account.status,
                    "last_ok_ms": account.last_ok_ms,
                },
                "rows": manager.admin_state()["rows"],
            }

        def dashboard(self):
            state = manager.admin_state()
            initial = json.dumps(state, separators=(",", ":")).replace("</", "<\\/")
            page = f"""<!doctype html>
<html><head>
<meta name='viewport' content='width=device-width,initial-scale=1,viewport-fit=cover'>
<meta name='theme-color' content='#080b10'>
<style>
*{{box-sizing:border-box}}:root{{--bg:#080b10;--card:#111720;--line:#273140;--text:#f5f7fb;--muted:#8d9aaa;--green:#21d08a;--red:#ff5d69;--orange:#ff9f1a;--blue:#6ea8ff}}
body{{margin:0;background:var(--bg);color:var(--text);font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif}}
.shell{{max-width:1160px;margin:auto;padding:16px 12px 48px}}
.top{{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}}h1{{font-size:24px;margin:4px 0}}.eyebrow{{font-size:10px;color:var(--orange);font-weight:900;letter-spacing:.08em}}.sub{{font-size:10px;color:var(--muted);line-height:1.55}}
.badge{{padding:8px 10px;border-radius:999px;background:#14241b;color:var(--green);font-size:10px;font-weight:900}}
.nav{{display:flex;gap:6px;overflow:auto;margin:14px 0 10px}}.nav button{{border:1px solid var(--line);background:#0d131a;color:var(--muted);padding:8px 11px;border-radius:999px;font-size:10px;font-weight:800}}.nav button.active{{color:#fff;border-color:#4c617b}}
.stats{{display:grid;grid-template-columns:repeat(6,1fr);gap:7px}}.stat,.panel{{background:var(--card);border:1px solid var(--line);border-radius:13px}}.stat{{padding:10px}}.stat span{{font-size:8px;color:var(--muted);text-transform:uppercase}}.stat b{{display:block;font-size:15px;margin-top:4px}}
.panel{{padding:12px;margin-top:10px}}h2{{font-size:14px;margin:0 0 9px}}.grid2{{display:grid;grid-template-columns:1.2fr .8fr;gap:8px}}
.open-grid{{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}}.trade,.mon{{background:#0d131a;border:1px solid var(--line);border-radius:11px;padding:10px}}
.head{{display:flex;justify-content:space-between;gap:8px;align-items:center}}.sym{{font-weight:900}}.long{{color:var(--green)}}.short{{color:var(--red)}}.timer{{font-size:16px;font-weight:900;color:var(--orange)}}
.meta{{display:grid;grid-template-columns:repeat(4,1fr);gap:5px;margin-top:8px}}.meta div{{background:#121a23;border-radius:8px;padding:6px;font-size:8px;color:var(--muted)}}.meta b{{display:block;color:#fff;font-size:10px;margin-top:2px}}
.monitor{{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}}.mon .score{{font-size:18px;font-weight:900;margin:3px 0}}.mon .tiny{{font-size:8px;color:var(--muted);line-height:1.45}}.ready{{border-color:rgba(33,208,138,.6)}}.exception{{box-shadow:inset 0 0 0 1px rgba(255,159,26,.65)}}
.row{{display:grid;grid-template-columns:120px 1fr 95px;gap:8px;padding:9px 0;border-bottom:1px solid var(--line);font-size:10px}}.row:last-child{{border-bottom:0}}.pnl{{font-weight:900;text-align:right}}.pos{{color:var(--green)}}.neg{{color:var(--red)}}.hidden{{display:none}}.empty{{font-size:10px;color:var(--muted);padding:10px 0}}
.daily{{display:grid;grid-template-columns:1fr 90px 90px 90px;gap:8px;padding:8px 0;border-bottom:1px solid var(--line);font-size:10px}}
.rule{{font-size:10px;color:var(--muted);line-height:1.6}}.rule b{{color:#eef4fb}}
@media(max-width:850px){{.stats{{grid-template-columns:repeat(2,1fr)}}.grid2,.open-grid{{grid-template-columns:1fr}}.monitor{{grid-template-columns:1fr}}.meta{{grid-template-columns:repeat(2,1fr)}}.shell{{padding:12px 9px 36px}}.row{{grid-template-columns:95px 1fr 75px}}}}
</style></head><body>
<div class='shell'>
<div class='top'><div><div class='eyebrow'>BYBIT DEMO · MICRO 70S</div><h1>Trade Engine Admin</h1><div class='sub'>$25k · 20x · 120s forced exit · ACTIVE TRAINING · no artificial 10-slot cap</div></div><div class='badge' id='live'>AUTO</div></div>
<div class='nav'><button class='active' data-tab='dash'>Dashboard</button><button data-tab='open'>OPEN</button><button data-tab='signals'>Signals</button><button data-tab='history'>History</button><button data-tab='daily'>Daily PnL</button></div>
<div class='stats'>
<div class='stat'><span>Open</span><b id='stOpen'>—</b></div>
<div class='stat'><span>Net PnL</span><b id='stNet'>—</b></div>
<div class='stat'><span>Gross PnL</span><b id='stGross'>—</b></div>
<div class='stat'><span>Fees</span><b id='stFees'>—</b></div>
<div class='stat'><span>Net Win Rate</span><b id='stWR'>—</b></div>
<div class='stat'><span>Gross Direction Win</span><b id='stGWR'>—</b></div>
</div>

<section id='dash' class='tab'>
<div class='panel'><h2>70-Second Learned Strategy</h2><div class='rule'>
Entry uses <b>5/10/20/40/70s momentum</b>, <b>10/20/40s real trade-flow</b>, flow acceleration, orderbook imbalance, microprice, OI, BTC impulse and a light 3–5m context.
ACTIVE TRAINING keeps only fresh-data, minimum feature coverage and extreme-spread protection as hard gates. Turnover, depth, range, conflict and context remain visible for analysis but do not block exploration trades.
A single qualifying fresh snapshot can open a Demo exploration trade. New trades close at <b>120 seconds</b> regardless of PnL.
</div></div>
<div class='grid2'><div class='panel'><h2>OPEN</h2><div id='dashOpen' class='open-grid'></div></div><div class='panel'><h2>Data</h2><div id='dataBox' class='rule'></div></div></div>
<div class='panel'><h2>Strongest Micro Signals</h2><div id='dashSignals' class='monitor'></div></div>
</section>

<section id='open' class='tab hidden'><div class='panel'><h2>OPEN</h2><div id='openFull' class='open-grid'></div></div></section>
<section id='signals' class='tab hidden'><div class='panel'><h2>Signals</h2><div id='signalsFull' class='monitor'></div></div></section>
<section id='history' class='tab hidden'><div class='panel'><h2>HISTORY</h2><div id='historyRows'></div></div></section>
<section id='daily' class='tab hidden'><div class='panel'><h2>Daily PnL</h2><div id='dailyRows'></div></div></section>
</div>
<script>
let S={initial};
const e=v=>String(v??'—').replace(/[&<>"']/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}})[c]);
const money=v=>v===null||v===undefined?'—':(Number(v)>=0?'+':'-')+'$'+Math.abs(Number(v)).toLocaleString(undefined,{{minimumFractionDigits:2,maximumFractionDigits:2}});
const num=v=>v===null||v===undefined?'—':Number(v).toLocaleString(undefined,{{maximumFractionDigits:5}});
const pc=v=>Number(v||0)>=0?'pos':'neg';
function timer(due){{if(!due)return 'SYNC';let s=Math.ceil((Number(due)-Date.now())/1000);if(s<=0)return 'CLOSING';return Math.floor(s/60)+':'+String(s%60).padStart(2,'0')}}
function tradeCard(t){{return `<div class="trade"><div class="head"><div><div class="sym">${{e(t.symbol)}} <span class="${{t.side==='Buy'?'long':'short'}}">${{t.side==='Buy'?'LONG':'SHORT'}}</span></div><div class="sub">${{e(t.slot_type)}} · Micro ${{num(t.micro_score??t.decision_score)}} · $${{Number(t.notional_usdt||0).toLocaleString()}}</div></div><div class="timer" data-due="${{e(t.due_ts_ms)}}">${{timer(t.due_ts_ms)}}</div></div><div class="meta"><div>Current PnL<b class="${{pc(t.current_pnl)}}">${{money(t.current_pnl)}}</b></div><div>Entry<b>${{num(t.entry_price||t.current_avg)}}</b></div><div>Mark<b>${{num(t.current_mark)}}</b></div><div>Exchange<b>${{t.exchange_open?'OPEN':'SYNC'}}</b></div></div></div>`}}
function signalCard(m){{const d=m.diagnostic||{{}};const ready=!!d.ready;const gates=m.gates||{{}};const gv=k=>gates[k]?'✓':'✕';const h=m.history||{{}};const hp=h.direction_hit==null?'—':(Number(h.direction_hit)*100).toFixed(0)+'%';const en=h.expected_net_bps==null?'—':Number(h.expected_net_bps).toFixed(1)+'bp';return `<div class="mon ${{ready?'ready':''}}"><div class="head"><div class="sym">${{e(m.symbol)}}</div><div class="${{ready?'long':'sub'}}">${{ready?'READY':e(d.reason||'WAIT')}}</div></div><div class="score ${{Number(m.score)>=0?'long':'short'}}">${{Number(m.score)>0?'+':''}}${{Number(m.score).toFixed(2)}} <span class="sub">rank ${{num(m.edge_rank)}}</span></div><div class="tiny">Base ${{num(m.base_score)}} · learned bonus ${{num(m.history_bonus)}} · CONF ${{d.confirmations||0}}/${{d.confirmations_required||S.config.confirmations}}<br>70s hist: symbol N=${{h.symbol_n||0}} · global N=${{h.global_n||0}} · hit ${{hp}} · expected NET ${{en}} · MFE ${{num(h.avg_mfe_bps)}}bp · MAE ${{num(h.avg_mae_bps)}}bp<br>Spread ${{gv('spread')}} ${{num(m.spread_bps)}}bp · Range70 ${{num(m.range70_bps)}}bp · Turn60 $${{Number(m.turnover60||0).toLocaleString(undefined,{{maximumFractionDigits:0}})}} · Depth $${{Number(m.depth10_notional||0).toLocaleString(undefined,{{maximumFractionDigits:0}})}}<br>5s ${{num(m.ret5_pct)}}% · 20s ${{num(m.ret20_pct)}}% · 70s ${{num(m.ret70_pct)}}% · flow ${{num(m.flow)}} · accel ${{num(m.flow_accel)}} · book ${{num(m.book)}}</div></div>`}}\nfunction render(s){{S=s;const q=s.summary||{{}};document.getElementById('live').textContent=s.auto_ready?'AUTO LIVE':'PAUSED';document.getElementById('stOpen').textContent=(s.open||[]).filter(x=>x.exchange_open).length+'/'+s.config.total_slots;document.getElementById('stNet').textContent=money(q.total_net);document.getElementById('stGross').textContent=money(q.total_gross);document.getElementById('stFees').textContent='$'+Number(q.total_fees||0).toFixed(2);document.getElementById('stWR').textContent=q.net_win_rate==null?'—':(q.net_win_rate*100).toFixed(1)+'%';document.getElementById('stGWR').textContent=q.gross_win_rate==null?'—':(q.gross_win_rate*100).toFixed(1)+'%';
const oc=(s.open||[]).map(tradeCard).join('')||'<div class="empty">No open auto trade.</div>';document.getElementById('dashOpen').innerHTML=oc;document.getElementById('openFull').innerHTML=oc;
const sig=(s.monitor||[]).map(signalCard).join('')||'<div class="empty">Waiting for micro data.</div>';document.getElementById('signalsFull').innerHTML=sig;document.getElementById('dashSignals').innerHTML=(s.monitor||[]).slice(0,6).map(signalCard).join('');
document.getElementById('dataBox').innerHTML=`Ticker rows <b>${{Number(s.rows.ticker).toLocaleString()}}</b><br>Orderbook rows <b>${{Number(s.rows.book).toLocaleString()}}</b><br>1s trade buckets <b>${{Number(s.rows.trades_1s).toLocaleString()}}</b><br>Features <b>${{Number(s.rows.features).toLocaleString()}}</b><br>Legacy labels <b>${{Number(s.rows.labels).toLocaleString()}}</b><br>Micro observations <b>${{Number(s.rows.micro_obs).toLocaleString()}}</b><br>70s outcomes <b>${{Number(s.rows.micro70||0).toLocaleString()}}</b><br>70s trade learning <b>${{Number(s.rows.trade_learning70||0).toLocaleString()}}</b><br>Missing symbols <b>${{e((s.missing_symbols||[]).join(', ')||'none')}}</b>`;
document.getElementById('historyRows').innerHTML=(s.history||[]).map(t=>`<div class="row"><div>${{new Date(Number(t.closed_ts_ms||t.updated_ts_ms)).toLocaleString()}}</div><div><b>${{e(t.symbol)}} · ${{t.side==='Buy'?'LONG':'SHORT'}}</b><div class="sub">Micro ${{num(t.micro_score??t.decision_score)}} · Entry ${{num(t.entry_price)}} → Exit ${{num(t.exit_price)}} · Gross ${{money(t.gross_pnl)}} · Open fee $${{Number(t.open_fee??0).toFixed(2)}} · Close fee $${{Number(t.close_fee??0).toFixed(2)}}</div></div><div class="pnl ${{pc(t.net_pnl)}}">${{t.net_pnl==null?'SYNC':money(t.net_pnl)}}</div></div>`).join('')||'<div class="empty">No history yet.</div>';
document.getElementById('dailyRows').innerHTML=(s.daily||[]).map(d=>`<div class="daily"><div><b>${{e(d.date)}}</b><div class="sub">${{d.trades}} trades · ${{d.wins}} wins</div></div><div>Gross<br><b>${{money(d.gross)}}</b></div><div>Fees<br><b>-$${{Number(d.fees||0).toFixed(2)}}</b></div><div class="${{pc(d.net)}}">Net<br><b>${{money(d.net)}}</b></div></div>`).join('')||'<div class="empty">No daily PnL yet.</div>';
}}
document.querySelectorAll('.nav button').forEach(b=>b.onclick=()=>{{document.querySelectorAll('.nav button').forEach(x=>x.classList.remove('active'));b.classList.add('active');document.querySelectorAll('.tab').forEach(x=>x.classList.add('hidden'));document.getElementById(b.dataset.tab).classList.remove('hidden')}});
async function poll(){{try{{let r=await fetch('/admin/state?_='+Date.now(),{{cache:'no-store'}});if(r.ok)render(await r.json())}}catch(e){{document.getElementById('live').textContent='RETRY'}}}}
function timers(){{document.querySelectorAll('.timer').forEach(x=>x.textContent=timer(x.dataset.due))}}
render(S);setInterval(timers,250);setInterval(poll,2000);
</script></body></html>"""
            self.send_page(page)

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            if path in ("/", "/health"):
                self.send_json(self.health())
            elif path == "/signals":
                self.send_json({
                    "version": VERSION,
                    "server_ms": core.now_ms(),
                    "signals": manager.admin_state()["monitor"],
                })
            elif path in ("/trade", "/admin"):
                if self.require_auth():
                    self.dashboard()
            elif path == "/admin/state":
                if self.require_auth():
                    self.send_json(manager.admin_state())
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            self.send_json({"error": "V0.6.3 has no manual order endpoint"}, 405)

        def log_message(self, fmt, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", core.PORT), Handler)
    Thread(target=server.serve_forever, daemon=True).start()
    log.info("V0.6.3 HTTP endpoint on 0.0.0.0:%s /health /trade /admin", core.PORT)


async def auto_loop(manager):
    await asyncio.sleep(5)
    while True:
        await asyncio.to_thread(manager.tick)
        await asyncio.sleep(1)


async def run():
    if AUTO_DEMO_TRADING_ENABLED and "api-demo.bybit.com" not in core.BYBIT_REST_URL:
        raise RuntimeError("V0.6.3 hard guard: AUTO trading is DEMO-only")
    if v4.DEMO_LEVERAGE != 20:
        raise RuntimeError("V0.6.3 guard: DEMO_LEVERAGE must be 20")
    if AUTO_NOTIONAL_USDT != Decimal("25000"):
        raise RuntimeError("V0.6.3 guard: AUTO_NOTIONAL_USDT must be 25000")
    if AUTO_HOLD_SEC != 70:
        raise RuntimeError("V0.6.3 guard: AUTO_HOLD_SEC must be 70")
    if AUTO_EXCEPTION_SLOTS != 0:
        raise RuntimeError("V0.6.3 guard: exception slots must be 0 in ACTIVE TRAINING")
    if AUTO_TOTAL_SLOTS < len(core.SYMBOLS):
        raise RuntimeError("V0.6.3 guard: active slots must cover all configured symbols")

    db = core.Database(core.DB_PATH)
    collector = core.Collector(db)
    account = core.AccountMonitor(db)
    features = v4.SafeFeatureEngine(db)
    manager = Micro70SManager(db)

    start_server(collector, db, account, manager)

    log.info("trade-engine %s", VERSION)
    log.info(
        "MICRO_70S auto=%s ready=%s demo_only=%s notional=%s leverage=%sx "
        "hold=%ss threshold=%.1f exception=%.1f confirmations=%s "
        "spread<=%.2fbps range2m>=%.1fbps turnover60>=%.0f depth>=%.1fx slots=%s+%s",
        AUTO_DEMO_TRADING_ENABLED, manager.auto_ready,
        "api-demo.bybit.com" in core.BYBIT_REST_URL,
        AUTO_NOTIONAL_USDT, v4.DEMO_LEVERAGE, AUTO_HOLD_SEC,
        MICRO_ENTRY_THRESHOLD, MICRO_EXCEPTION_THRESHOLD, MICRO_CONFIRMATIONS,
        MICRO_MAX_SPREAD_BPS, MICRO_MIN_RANGE_120_BPS,
        MICRO_MIN_TURNOVER_60S, MICRO_MIN_DEPTH_MULT,
        AUTO_BASE_SLOTS, AUTO_EXCEPTION_SLOTS,
    )

    await asyncio.gather(
        collector.run_forever(),
        core.feature_loop(features),
        core.account_loop(account),
        auto_loop(manager),
    )


if __name__ == "__main__":
    asyncio.run(run())
