import asyncio
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
from threading import Thread

import main as core
import trade_engine_v040_demo as v4
import trade_engine_v063_active_training_70s_data_demo as base

log = logging.getLogger("trade-engine")
VERSION = "v0.7.0-ha70-demo"

AUTO_DEMO_TRADING_ENABLED = os.getenv("AUTO_DEMO_TRADING_ENABLED", "false").strip().lower() in {"1","true","yes","on"}
AUTO_NOTIONAL_USDT = Decimal(os.getenv("AUTO_NOTIONAL_USDT", "25000"))
AUTO_HOLD_SEC = int(os.getenv("AUTO_HOLD_SEC", "70"))
AUTO_BASE_SLOTS = int(os.getenv("AUTO_BASE_SLOTS", str(len(core.SYMBOLS))))
AUTO_EXCEPTION_SLOTS = int(os.getenv("AUTO_EXCEPTION_SLOTS", "0"))
AUTO_TOTAL_SLOTS = AUTO_BASE_SLOTS + AUTO_EXCEPTION_SLOTS
HA_MAX_ENTRY_DELAY_SEC = int(os.getenv("HA_MAX_ENTRY_DELAY_SEC", "12"))
HA_MAX_SPREAD_BPS = float(os.getenv("HA_MAX_SPREAD_BPS", "20"))
HA_EST_ROUNDTRIP_FEE_BPS = float(os.getenv("HA_EST_ROUNDTRIP_FEE_BPS", "11"))
HA_OUTCOME_BATCH = int(os.getenv("HA_OUTCOME_BATCH", "240"))
HA_INITIAL_CANDLE_LIMIT = int(os.getenv("HA_INITIAL_CANDLE_LIMIT", "10000"))
HA_MICRO_SAMPLE_SEC = int(os.getenv("HA_MICRO_SAMPLE_SEC", "10"))
REPORT_UTC_OFFSET_HOURS = int(os.getenv("REPORT_UTC_OFFSET_HOURS", "4"))

# Keep inherited execution layer aligned with this release even if an old env default exists.
base.AUTO_HOLD_SEC = AUTO_HOLD_SEC
base.AUTO_NOTIONAL_USDT = AUTO_NOTIONAL_USDT
base.AUTO_BASE_SLOTS = AUTO_BASE_SLOTS
base.AUTO_EXCEPTION_SLOTS = AUTO_EXCEPTION_SLOTS
base.AUTO_TOTAL_SLOTS = AUTO_TOTAL_SLOTS

HA_SCHEMA = """
CREATE TABLE IF NOT EXISTS heikin_ashi_1m (
  symbol TEXT NOT NULL,
  start_ms INTEGER NOT NULL,
  end_ms INTEGER NOT NULL,
  real_open REAL NOT NULL,
  real_high REAL NOT NULL,
  real_low REAL NOT NULL,
  real_close REAL NOT NULL,
  ha_open REAL NOT NULL,
  ha_high REAL NOT NULL,
  ha_low REAL NOT NULL,
  ha_close REAL NOT NULL,
  color INTEGER NOT NULL,
  body_bps REAL NOT NULL,
  upper_wick_bps REAL NOT NULL,
  lower_wick_bps REAL NOT NULL,
  streak INTEGER NOT NULL,
  pattern8 TEXT NOT NULL,
  volume REAL,
  turnover REAL,
  source_ts_ms INTEGER,
  computed_ts_ms INTEGER NOT NULL,
  PRIMARY KEY(symbol,start_ms)
);
CREATE INDEX IF NOT EXISTS idx_ha_symbol_start ON heikin_ashi_1m(symbol,start_ms);

CREATE TABLE IF NOT EXISTS ha_outcomes_70s (
  symbol TEXT NOT NULL,
  source_candle_start_ms INTEGER NOT NULL,
  decision_minute_ms INTEGER NOT NULL,
  direction INTEGER NOT NULL,
  pattern8 TEXT,
  streak INTEGER,
  body_bps REAL,
  upper_wick_bps REAL,
  lower_wick_bps REAL,
  entry_price REAL NOT NULL,
  ret15_pct REAL,
  ret30_pct REAL,
  ret45_pct REAL,
  ret70_pct REAL,
  dir_ret70_bps REAL,
  mfe70_bps REAL,
  mae70_bps REAL,
  labeled_ts_ms INTEGER NOT NULL,
  PRIMARY KEY(symbol,source_candle_start_ms)
);
CREATE INDEX IF NOT EXISTS idx_ha70_profile ON ha_outcomes_70s(symbol,direction,pattern8,source_candle_start_ms);
CREATE INDEX IF NOT EXISTS idx_ha70_global ON ha_outcomes_70s(direction,pattern8,source_candle_start_ms);

CREATE TABLE IF NOT EXISTS ha_minute_decisions (
  symbol TEXT NOT NULL,
  decision_minute_ms INTEGER NOT NULL,
  source_candle_start_ms INTEGER NOT NULL,
  direction INTEGER NOT NULL,
  ha_color TEXT NOT NULL,
  pattern8 TEXT,
  streak INTEGER,
  body_bps REAL,
  upper_wick_bps REAL,
  lower_wick_bps REAL,
  confidence REAL,
  hist_pattern_n INTEGER,
  hist_symbol_n INTEGER,
  hist_global_n INTEGER,
  hist_hit_rate REAL,
  hist_avg_dir_bps REAL,
  hist_expected_net_bps REAL,
  micro_score REAL,
  orderflow_score REAL,
  spread_bps REAL,
  oi_score REAL,
  btc_score REAL,
  action TEXT NOT NULL,
  trade_id INTEGER,
  created_ts_ms INTEGER NOT NULL,
  PRIMARY KEY(symbol,decision_minute_ms)
);
CREATE INDEX IF NOT EXISTS idx_ha_decisions_ts ON ha_minute_decisions(decision_minute_ms);
"""


def ff(v, default=None):
    try:
        if v is None or v == "": return default
        return float(v)
    except Exception:
        return default


def clamp(v, lo, hi): return max(lo, min(hi, v))
def sgn(v): return 1 if v > 0 else (-1 if v < 0 else 0)
def color_name(d): return "GREEN" if d > 0 else "RED"


class HA70Manager(base.Micro70SManager):
    def __init__(self, db):
        super().__init__(db)
        with db.lock:
            db.conn.executescript(HA_SCHEMA)
            db.conn.commit()
        self._last_ha_sync_ms = 0
        self._last_micro_collect_ms = 0
        self._last_ha_label_ms = 0
        self._last_legacy_label_ms = 0
        self._last_decision_minute = 0
        self._bootstrapped = False

    @property
    def auto_ready(self):
        return (
            AUTO_DEMO_TRADING_ENABLED
            and self.client.configured
            and "api-demo.bybit.com" in core.BYBIT_REST_URL
            and v4.DEMO_LEVERAGE == 20
            and AUTO_NOTIONAL_USDT == Decimal("25000")
            and AUTO_HOLD_SEC == 70
            and AUTO_EXCEPTION_SLOTS == 0
            and AUTO_TOTAL_SLOTS >= len(core.SYMBOLS)
        )

    # ---------- Heikin Ashi storage ----------
    def sync_ha_symbol(self, symbol):
        last = self.db.one(
            """SELECT start_ms,ha_open,ha_close,color,streak,pattern8
               FROM heikin_ashi_1m WHERE symbol=? ORDER BY start_ms DESC LIMIT 1""",
            (symbol,),
        )
        if last:
            rows = self.db.query(
                """SELECT start_ms,end_ms,open,high,low,close,volume,turnover,source_ts_ms
                   FROM candles_1m WHERE symbol=? AND start_ms>? ORDER BY start_ms ASC""",
                (symbol, int(last[0])),
            )
            prev_ho, prev_hc, prev_color, prev_streak, prev_pattern = float(last[1]), float(last[2]), int(last[3]), int(last[4]), str(last[5] or "")
        else:
            rows = self.db.query(
                """SELECT start_ms,end_ms,open,high,low,close,volume,turnover,source_ts_ms
                   FROM candles_1m WHERE symbol=? ORDER BY start_ms DESC LIMIT ?""",
                (symbol, HA_INITIAL_CANDLE_LIMIT),
            )
            rows = list(reversed(rows))
            prev_ho = prev_hc = None
            prev_color = 0
            prev_streak = 0
            prev_pattern = ""
        if not rows: return 0

        inserts=[]
        now=core.now_ms()
        for start_ms,end_ms,o,h,l,c,vol,turn,source_ts in rows:
            o,h,l,c = map(float,(o,h,l,c))
            hc=(o+h+l+c)/4.0
            ho=(o+c)/2.0 if prev_ho is None else (prev_ho+prev_hc)/2.0
            hh=max(h,ho,hc); hl=min(l,ho,hc)
            mid=max((ho+hc)/2.0, 1e-12)
            if hc > ho: color=1
            elif hc < ho: color=-1
            else: color=prev_color if prev_color else (1 if c>=o else -1)
            body=abs(hc-ho)/mid*10000.0
            upper=max(0.0,hh-max(ho,hc))/mid*10000.0
            lower=max(0.0,min(ho,hc)-hl)/mid*10000.0
            streak=(prev_streak+1) if color==prev_color and prev_streak else 1
            ch="G" if color>0 else "R"
            pattern=(prev_pattern+ch)[-8:]
            inserts.append((symbol,int(start_ms),int(end_ms),o,h,l,c,ho,hh,hl,hc,color,body,upper,lower,streak,pattern,ff(vol,0.0),ff(turn,0.0),int(source_ts or 0),now))
            prev_ho,prev_hc,prev_color,prev_streak,prev_pattern=ho,hc,color,streak,pattern
        self.db.executemany(
            """INSERT OR REPLACE INTO heikin_ashi_1m
               (symbol,start_ms,end_ms,real_open,real_high,real_low,real_close,ha_open,ha_high,ha_low,ha_close,color,body_bps,upper_wick_bps,lower_wick_bps,streak,pattern8,volume,turnover,source_ts_ms,computed_ts_ms)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            inserts,
        )
        return len(inserts)

    def sync_all_ha(self):
        total=0
        for symbol in core.SYMBOLS:
            try: total += self.sync_ha_symbol(symbol)
            except Exception as exc: log.warning("HA SYNC failed %s: %s",symbol,exc)
        if total: log.info("HA 1M synced=%s", total)
        return total

    def latest_ha_for_decision(self, symbol, minute_ms):
        return self.db.one(
            """SELECT start_ms,end_ms,ha_open,ha_high,ha_low,ha_close,color,body_bps,upper_wick_bps,lower_wick_bps,streak,pattern8,real_close,volume,turnover
               FROM heikin_ashi_1m WHERE symbol=? AND start_ms=?""",
            (symbol, int(minute_ms)-60_000),
        )

    def ticker_after(self, symbol, ts_ms, tolerance_ms=8000):
        row=self.db.one(
            """SELECT ts_ms,last_price FROM ticker_snapshots
               WHERE symbol=? AND ts_ms>=? AND ts_ms<=? AND last_price IS NOT NULL
               ORDER BY ts_ms ASC LIMIT 1""",
            (symbol,int(ts_ms),int(ts_ms)+int(tolerance_ms)),
        )
        return row

    # ---------- historical HA learning ----------
    def label_ha_outcomes(self, limit=None):
        limit=int(limit or HA_OUTCOME_BATCH)
        now=core.now_ms()
        rows=self.db.query(
            """SELECT h.symbol,h.start_ms,h.color,h.pattern8,h.streak,h.body_bps,h.upper_wick_bps,h.lower_wick_bps
               FROM heikin_ashi_1m h
               WHERE h.start_ms+60000<=?
                 AND NOT EXISTS (SELECT 1 FROM ha_outcomes_70s x WHERE x.symbol=h.symbol AND x.source_candle_start_ms=h.start_ms)
               ORDER BY h.start_ms ASC LIMIT ?""",
            (now-70_000,limit),
        )
        done=0
        for symbol,start_ms,direction,pattern,streak,body,uw,lw in rows:
            decision=int(start_ms)+60_000
            ent=self.ticker_after(symbol,decision)
            if not ent or not ff(ent[1]): continue
            entry=ff(ent[1]); actual_ts=int(ent[0])
            out=self._path_outcome_70s(symbol,actual_ts,entry,int(direction))
            if not out: continue
            self.db.execute(
                """INSERT OR IGNORE INTO ha_outcomes_70s
                   (symbol,source_candle_start_ms,decision_minute_ms,direction,pattern8,streak,body_bps,upper_wick_bps,lower_wick_bps,entry_price,ret15_pct,ret30_pct,ret45_pct,ret70_pct,dir_ret70_bps,mfe70_bps,mae70_bps,labeled_ts_ms)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol,int(start_ms),decision,int(direction),pattern,int(streak),ff(body),ff(uw),ff(lw),entry,out["ret15_pct"],out["ret30_pct"],out["ret45_pct"],out["ret70_pct"],out["dir_ret70_bps"],out["mfe70_bps"],out["mae70_bps"],now),
            )
            done+=1
        if done: log.info("HA 70S OUTCOME backfilled=%s",done)
        return done

    def ha_history_profile(self, symbol, direction, pattern8):
        pattern3=(pattern8 or "")[-3:]
        exact=self.db.query(
            """SELECT dir_ret70_bps,mfe70_bps,mae70_bps FROM ha_outcomes_70s
               WHERE symbol=? AND direction=? AND substr(pattern8,-3)=?
               ORDER BY source_candle_start_ms DESC LIMIT 300""",
            (symbol,int(direction),pattern3),
        ) if pattern3 else []
        sym=self.db.query(
            """SELECT dir_ret70_bps,mfe70_bps,mae70_bps FROM ha_outcomes_70s
               WHERE symbol=? AND direction=? ORDER BY source_candle_start_ms DESC LIMIT 500""",
            (symbol,int(direction)),
        )
        glob=self.db.query(
            """SELECT dir_ret70_bps,mfe70_bps,mae70_bps FROM ha_outcomes_70s
               WHERE symbol<>? AND direction=? AND substr(pattern8,-3)=?
               ORDER BY source_candle_start_ms DESC LIMIT 1000""",
            (symbol,int(direction),pattern3),
        ) if pattern3 else []
        def agg(rows):
            vals=[(clamp(float(r),-300,300),clamp(float(m or 0),-300,300),clamp(float(a or 0),-300,300)) for r,m,a in rows if r is not None]
            if not vals: return None
            rs=[x[0] for x in vals]
            return {"n":len(vals),"avg":sum(rs)/len(rs),"hit":sum(x>0 for x in rs)/len(rs),"mfe":sum(x[1] for x in vals)/len(vals),"mae":sum(x[2] for x in vals)/len(vals)}
        a,b,c=agg(exact),agg(sym),agg(glob)
        sources=[]
        if a: sources.append((a, min(a["n"],40)*2.0))
        if b: sources.append((b, min(b["n"],80)*1.0))
        if c: sources.append((c, min(c["n"],80)*0.35))
        if not sources:
            return {"pattern_n":0,"symbol_n":0,"global_n":0,"direction_hit":None,"avg_dir_bps":None,"expected_net_bps":None,"avg_mfe_bps":None,"avg_mae_bps":None,"confidence":0.0}
        den=sum(w for _,w in sources) or 1.0
        avg=sum(x["avg"]*w for x,w in sources)/den
        hit=sum(x["hit"]*w for x,w in sources)/den
        mfe=sum(x["mfe"]*w for x,w in sources)/den
        mae=sum(x["mae"]*w for x,w in sources)/den
        n=(a["n"] if a else 0)+(b["n"] if b else 0)
        return {
            "pattern_n":a["n"] if a else 0,
            "symbol_n":b["n"] if b else 0,
            "global_n":c["n"] if c else 0,
            "direction_hit":hit,"avg_dir_bps":avg,
            "expected_net_bps":avg-HA_EST_ROUNDTRIP_FEE_BPS,
            "avg_mfe_bps":mfe,"avg_mae_bps":mae,
            "confidence":clamp(n/80.0,0.0,1.0),
        }

    def confidence_score(self, ha, micro, hist):
        # Direction remains HA color. Everything below is soft confidence only.
        direction=int(ha[6]); body=float(ha[7]); upper=float(ha[8]); lower=float(ha[9]); streak=int(ha[10]); pattern=str(ha[11] or "")
        score=50.0
        score += min(max(streak-1,0)*3.0,12.0)
        score += min(body/2.0,12.0)
        against=lower if direction>0 else upper
        score += clamp(6.0 - against/2.0,-6.0,6.0)
        if len(pattern)>=3 and len(set(pattern[-3:]))==1: score += 5.0
        if micro:
            score += clamp((ff(micro.get("score"),0.0) or 0.0)*direction/5.0,-10.0,10.0)
            score += clamp((ff(micro.get("flow"),0.0) or 0.0)*direction/4.0,-6.0,6.0)
            score += clamp((ff(micro.get("book"),0.0) or 0.0)*direction/6.0,-4.0,4.0)
        if hist.get("direction_hit") is not None:
            hc=float(hist.get("confidence") or 0.0)
            score += clamp((hist["direction_hit"]-0.5)*30.0,-8.0,8.0)*hc
            score += clamp((hist.get("expected_net_bps") or 0.0)/4.0,-8.0,8.0)*hc
        spread=ff((micro or {}).get("spread_bps"),0.0) or 0.0
        score -= clamp(spread/HA_MAX_SPREAD_BPS*5.0,0.0,5.0)
        return clamp(score,0.0,100.0)

    def decision_exists(self, symbol, minute_ms):
        return bool(self.db.one("SELECT 1 FROM ha_minute_decisions WHERE symbol=? AND decision_minute_ms=?",(symbol,int(minute_ms))))

    def record_decision(self, symbol, minute_ms, ha, confidence, hist, micro, action, trade_id=None):
        self.db.execute(
            """INSERT OR REPLACE INTO ha_minute_decisions
               (symbol,decision_minute_ms,source_candle_start_ms,direction,ha_color,pattern8,streak,body_bps,upper_wick_bps,lower_wick_bps,confidence,hist_pattern_n,hist_symbol_n,hist_global_n,hist_hit_rate,hist_avg_dir_bps,hist_expected_net_bps,micro_score,orderflow_score,spread_bps,oi_score,btc_score,action,trade_id,created_ts_ms)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (symbol,int(minute_ms),int(ha[0]),int(ha[6]),color_name(int(ha[6])),str(ha[11] or ""),int(ha[10]),ff(ha[7]),ff(ha[8]),ff(ha[9]),ff(confidence),int(hist.get("pattern_n") or 0),int(hist.get("symbol_n") or 0),int(hist.get("global_n") or 0),ff(hist.get("direction_hit")),ff(hist.get("avg_dir_bps")),ff(hist.get("expected_net_bps")),ff((micro or {}).get("score")),ff((micro or {}).get("flow")),ff((micro or {}).get("spread_bps")),ff((micro or {}).get("oi")),ff((micro or {}).get("btc")),action,trade_id,core.now_ms()),
        )

    def managed_trade_for_symbol(self, symbol):
        for t in self.managed_open_rows():
            if t.get("symbol")==symbol: return t
        return None

    def close_for_ha_flip(self, trade, position):
        trade_id=int(trade["id"]); symbol=trade["symbol"]
        with self.lock:
            latest=self._row_dict(self._trade_row(trade_id))
            if not latest or latest["status"] not in {"OPEN","OPEN_PENDING","CLOSE_RETRY"}: return False
            close_side="Sell" if position["side"]=="Buy" else "Buy"
            try:
                result=self.client.market_order(symbol,close_side,position["size"],True)
                order_id=result.get("result",{}).get("orderId")
                self._update_trade(trade_id,status="CLOSING",close_order_id=order_id,close_reason="HA_FLIP",error_text=None)
                log.warning("HA FLIP CLOSE symbol=%s side=%s qty=%s order=%s",symbol,close_side,position["size"],order_id)
            except Exception as exc:
                self._update_trade(trade_id,status="CLOSE_RETRY",error_text=str(exc)[:500])
                log.error("HA FLIP CLOSE FAILED %s: %s",symbol,exc); return False
        for _ in range(10):
            time.sleep(0.25)
            if not any(x.get("symbol")==symbol for x in self.positions_cached(force=True)):
                self._update_trade(trade_id,status="CLOSED_PENDING_PNL",closed_ts_ms=core.now_ms(),close_reason="HA_FLIP")
                time.sleep(0.25)
                self.finalize_pnl_micro(trade_id,"HA_FLIP")
                return True
        return False

    def build_entry_metrics(self, symbol, minute_ms, ha, micro, hist, confidence):
        direction=int(ha[6])
        m=dict(micro or {})
        m.update({
            "symbol":symbol,"ts_ms":int(minute_ms),"price":ff((micro or {}).get("price"),ff(ha[12])),
            "score":direction*float(confidence),"base_score":direction*float(confidence),"edge_rank":float(confidence),
            "raw":direction*float(confidence),"direction":direction,
            "short_score":ff((micro or {}).get("short_score"),0.0) or 0.0,"mid_score":ff((micro or {}).get("mid_score"),0.0) or 0.0,"long_score":ff((micro or {}).get("long_score"),0.0) or 0.0,
            "spread_bps":ff((micro or {}).get("spread_bps"),0.0) or 0.0,"range70_bps":ff((micro or {}).get("range70_bps"),0.0) or 0.0,
            "turnover60":ff((micro or {}).get("turnover60"),0.0) or 0.0,"depth10_notional":ff((micro or {}).get("depth10_notional"),0.0) or 0.0,
            "conflict":bool((micro or {}).get("conflict",False)),
            "history":{
                "symbol_n":int(hist.get("symbol_n") or 0),"global_n":int(hist.get("global_n") or 0),"direction_hit":hist.get("direction_hit"),
                "avg_dir_bps":hist.get("avg_dir_bps"),"expected_net_bps":hist.get("expected_net_bps"),"avg_mfe_bps":hist.get("avg_mfe_bps"),"avg_mae_bps":hist.get("avg_mae_bps"),
            },
            "ha":{"source_candle_start_ms":int(ha[0]),"color":color_name(direction),"pattern8":str(ha[11] or ""),"streak":int(ha[10]),"body_bps":ff(ha[7]),"upper_wick_bps":ff(ha[8]),"lower_wick_bps":ff(ha[9]),"confidence":confidence},
        })
        return m

    # ---------- minute-open execution ----------
    def evaluate_ha_minute(self):
        if not self.auto_ready: return
        now=core.now_ms(); minute=(now//60_000)*60_000; delay=(now-minute)/1000.0
        if delay > HA_MAX_ENTRY_DELAY_SEC: return
        self.sync_all_ha()
        positions=self.positions_cached(force=True); pos_map={p["symbol"]:p for p in positions}
        context=self.v4_context()
        for symbol in core.SYMBOLS:
            if self.decision_exists(symbol,minute): continue
            ha=self.latest_ha_for_decision(symbol,minute)
            if not ha: continue
            direction=int(ha[6]); pattern=str(ha[11] or "")
            try: micro=self.micro_snapshot(symbol,context)
            except Exception: micro=None
            if not micro:
                empty_hist={"pattern_n":0,"symbol_n":0,"global_n":0,"direction_hit":None,"avg_dir_bps":None,"expected_net_bps":None,"avg_mfe_bps":None,"avg_mae_bps":None,"confidence":0.0}
                self.record_decision(symbol,minute,ha,50.0,empty_hist,None,"SKIP_CURRENT_DATA_MISSING")
                continue
            hist=self.ha_history_profile(symbol,direction,pattern)
            conf=self.confidence_score(ha,micro,hist)
            spread=ff((micro or {}).get("spread_bps"),0.0) or 0.0
            if spread > HA_MAX_SPREAD_BPS:
                self.record_decision(symbol,minute,ha,conf,hist,micro,"SKIP_EXTREME_SPREAD")
                continue
            p=pos_map.get(symbol)
            desired_side="Buy" if direction>0 else "Sell"
            action="OPEN_"+color_name(direction)
            if p:
                if p.get("side")==desired_side:
                    self.record_decision(symbol,minute,ha,conf,hist,micro,"HOLD_SAME_DIRECTION")
                    continue
                managed=self.managed_trade_for_symbol(symbol)
                if not managed:
                    self.record_decision(symbol,minute,ha,conf,hist,micro,"SKIP_UNMANAGED_POSITION")
                    continue
                if not self.close_for_ha_flip(managed,p):
                    self.record_decision(symbol,minute,ha,conf,hist,micro,"FLIP_CLOSE_PENDING")
                    continue
                action="FLIP_TO_"+color_name(direction)
                positions=self.positions_cached(force=True); pos_map={x["symbol"]:x for x in positions}
            m=self.build_entry_metrics(symbol,minute,ha,micro,hist,conf)
            ok=self.auto_open_micro(m,"HA70")
            trade_id=None
            if ok:
                row=self.db.one("SELECT id FROM auto_trades WHERE symbol=? AND signal_ts_ms=?",(symbol,int(minute)))
                trade_id=int(row[0]) if row else None
                log.warning("HA70 OPEN symbol=%s color=%s pattern=%s streak=%s confidence=%.1f histN=%s hit=%s expNet=%s",symbol,color_name(direction),pattern,int(ha[10]),conf,int(hist.get("symbol_n") or 0),hist.get("direction_hit"),hist.get("expected_net_bps"))
            else:
                action="OPEN_FAILED"
            self.record_decision(symbol,minute,ha,conf,hist,micro,action,trade_id)

    def collect_micro_only(self):
        context=self.v4_context()
        latest=[]; missing=[]
        for symbol in core.SYMBOLS:
            try: m=self.micro_snapshot(symbol,context)
            except Exception: m=None
            if not m: missing.append(symbol); continue
            self.remember_score(m); self.store_observation(m); latest.append(m)
        self._latest_micro_snapshots=latest; self._latest_missing_symbols=missing

    def tick(self):
        self.last_tick_ms=core.now_ms()
        if not self.auto_ready: return
        try:
            self.reconcile_micro()
            now=core.now_ms()
            if now-self._last_ha_sync_ms>=5_000:
                self.sync_all_ha(); self._last_ha_sync_ms=now
            if now-self._last_micro_collect_ms>=HA_MICRO_SAMPLE_SEC*1000:
                self.collect_micro_only(); self._last_micro_collect_ms=now
            if now-self._last_ha_label_ms>=10_000:
                self.label_ha_outcomes(); self.label_70s_observations(); self._last_ha_label_ms=now
            if now-self._last_legacy_label_ms>=30_000:
                self.label_observations(); self._last_legacy_label_ms=now
            self.evaluate_ha_minute()
            self.last_error=""; self.last_micro_error=""
        except Exception as exc:
            self.last_error=str(exc)[:500]; self.last_micro_error=self.last_error
            log.exception("HA70 tick failed: %s",exc)

    def bootstrap(self):
        try:
            n=self.sync_all_ha()
            # Several small batches let existing 1m/ticker history become useful immediately.
            filled=0
            for _ in range(6):
                x=self.label_ha_outcomes(HA_OUTCOME_BATCH)
                filled+=x
                if x==0: break
            log.info("HA70 BOOTSTRAP candles_synced=%s outcomes_backfilled=%s",n,filled)
        except Exception as exc:
            log.exception("HA70 bootstrap failed: %s",exc)

    def ha_monitor(self):
        out=[]
        for symbol in core.SYMBOLS:
            r=self.db.one(
                """SELECT start_ms,color,body_bps,upper_wick_bps,lower_wick_bps,streak,pattern8,ha_open,ha_close
                   FROM heikin_ashi_1m WHERE symbol=? ORDER BY start_ms DESC LIMIT 1""",(symbol,))
            if not r: continue
            d=self.db.one(
                """SELECT decision_minute_ms,confidence,hist_hit_rate,hist_expected_net_bps,action
                   FROM ha_minute_decisions WHERE symbol=? ORDER BY decision_minute_ms DESC LIMIT 1""",(symbol,))
            out.append({"symbol":symbol,"start_ms":r[0],"color":color_name(int(r[1])),"direction":int(r[1]),"body_bps":r[2],"upper_wick_bps":r[3],"lower_wick_bps":r[4],"streak":r[5],"pattern8":r[6],"ha_open":r[7],"ha_close":r[8],"decision_minute_ms":d[0] if d else None,"confidence":d[1] if d else None,"hist_hit":d[2] if d else None,"hist_expected_net_bps":d[3] if d else None,"action":d[4] if d else None})
        return out

    def admin_state(self):
        s=super().admin_state()
        s["version"]=VERSION
        s["ha_monitor"]=self.ha_monitor()
        s["rows"]["ha_candles"]=self.db.scalar("SELECT COUNT(*) FROM heikin_ashi_1m") or 0
        s["rows"]["ha70_outcomes"]=self.db.scalar("SELECT COUNT(*) FROM ha_outcomes_70s") or 0
        s["rows"]["ha_decisions"]=self.db.scalar("SELECT COUNT(*) FROM ha_minute_decisions") or 0
        s["config"].update({"hold_sec":70,"total_slots":AUTO_TOTAL_SLOTS,"direction_engine":"last completed 1m Heikin Ashi","ha_entry_delay_sec":HA_MAX_ENTRY_DELAY_SEC,"ha_max_spread_bps":HA_MAX_SPREAD_BPS,"estimated_roundtrip_fee_bps":HA_EST_ROUNDTRIP_FEE_BPS})
        return s


def start_server(collector, db, account, manager):
    class Handler(BaseHTTPRequestHandler):
        def send_json(self,payload,status=200):
            body=json.dumps(payload,separators=(",",":"),default=str).encode()
            self.send_response(status); self.send_header("Content-Type","application/json"); self.send_header("Cache-Control","no-store"); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
        def send_page(self,text,status=200):
            body=text.encode(); self.send_response(status); self.send_header("Content-Type","text/html; charset=utf-8"); self.send_header("Cache-Control","no-store"); self.send_header("X-Frame-Options","DENY"); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
        def require_auth(self):
            if v4.auth_ok(self.headers.get("Authorization","")): return True
            self.send_response(401); self.send_header("WWW-Authenticate",'Basic realm="HA70 Admin"'); self.end_headers(); return False
        def dashboard(self):
            initial=json.dumps(manager.admin_state(),separators=(",",":"),default=str).replace("</","<\\/")
            page=f'''<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>
body{{margin:0;background:#080b10;color:#f4f7fb;font-family:system-ui}}.s{{max-width:1180px;margin:auto;padding:14px}}h1{{font-size:22px}}.sub{{color:#8d9aaa;font-size:11px}}.stats,.grid{{display:grid;gap:7px}}.stats{{grid-template-columns:repeat(5,1fr)}}.grid{{grid-template-columns:repeat(3,1fr)}}.c,.st{{background:#111720;border:1px solid #283342;border-radius:12px;padding:10px}}.st b{{display:block;font-size:15px}}.st span{{font-size:9px;color:#8d9aaa}}.g{{color:#21d08a}}.r{{color:#ff5d69}}.tiny{{font-size:9px;color:#8d9aaa;line-height:1.5}}table{{width:100%;border-collapse:collapse;font-size:10px}}td,th{{padding:7px;border-bottom:1px solid #283342;text-align:left}}@media(max-width:800px){{.stats{{grid-template-columns:repeat(2,1fr)}}.grid{{grid-template-columns:1fr}}}}
</style></head><body><div class="s"><div class="sub">BYBIT DEMO · V0.7 HA70</div><h1>Heikin Ashi 70s Engine</h1><div class="sub">Direction = last completed 1m Heikin Ashi candle · 70s max hold · HA flip can reverse at the next minute open · historical 70s outcomes are soft confidence, not hard gates.</div><div class="stats"><div class="st"><span>Open</span><b id="o">—</b></div><div class="st"><span>Net PnL</span><b id="n">—</b></div><div class="st"><span>Fees</span><b id="f">—</b></div><div class="st"><span>HA candles</span><b id="hc">—</b></div><div class="st"><span>HA70 outcomes</span><b id="ho">—</b></div></div><h3>Latest Heikin Ashi</h3><div id="cards" class="grid"></div><h3>History</h3><div class="c"><table><thead><tr><th>Coin</th><th>Side</th><th>Entry → Exit</th><th>Gross</th><th>Fees</th><th>Net</th></tr></thead><tbody id="hist"></tbody></table></div></div><script>
let S={initial}; const money=x=>x==null?'—':(Number(x)>=0?'+':'-')+'$'+Math.abs(Number(x)).toFixed(2); const num=x=>x==null?'—':Number(x).toFixed(4);
function render(s){{S=s;document.getElementById('o').textContent=s.exchange_open_count+'/'+s.config.total_slots;document.getElementById('n').textContent=money(s.summary.total_net);document.getElementById('f').textContent='$'+Number(s.summary.total_fees||0).toFixed(2);document.getElementById('hc').textContent=Number(s.rows.ha_candles||0).toLocaleString();document.getElementById('ho').textContent=Number(s.rows.ha70_outcomes||0).toLocaleString();document.getElementById('cards').innerHTML=(s.ha_monitor||[]).map(x=>`<div class="c"><b>${{x.symbol}} <span class="${{x.direction>0?'g':'r'}}">${{x.color}}</span></b><div class="tiny">pattern ${{x.pattern8}} · streak ${{x.streak}} · body ${{num(x.body_bps)}}bp<br>wick ↑ ${{num(x.upper_wick_bps)}} / ↓ ${{num(x.lower_wick_bps)}}bp<br>confidence ${{x.confidence==null?'—':Number(x.confidence).toFixed(1)}} · hist hit ${{x.hist_hit==null?'—':(x.hist_hit*100).toFixed(0)+'%'}} · exp net ${{x.hist_expected_net_bps==null?'—':Number(x.hist_expected_net_bps).toFixed(1)+'bp'}}<br>last action ${{x.action||'—'}}</div></div>`).join('');document.getElementById('hist').innerHTML=(s.history||[]).slice(0,80).map(t=>`<tr><td>${{t.symbol}}</td><td class="${{t.side==='Buy'?'g':'r'}}">${{t.side==='Buy'?'LONG':'SHORT'}}</td><td>${{num(t.entry_price)}} → ${{num(t.exit_price)}}</td><td>${{money(t.gross_pnl)}}</td><td>$${{Number(t.fees||0).toFixed(2)}}</td><td class="${{Number(t.net_pnl||0)>=0?'g':'r'}}">${{money(t.net_pnl)}}</td></tr>`).join('')}}
async function poll(){{try{{let r=await fetch('/admin/state?_='+Date.now(),{{cache:'no-store'}});if(r.ok)render(await r.json())}}catch(e){{}}}}render(S);setInterval(poll,2000);
</script></body></html>'''
            self.send_page(page)
        def do_GET(self):
            path=urllib.parse.urlparse(self.path).path
            if path in ("/","/health"):
                self.send_json({"status":"ok" if collector.last_message_ms else "starting","version":VERSION,"mode":"ha70_demo","auto_ready":manager.auto_ready,"demo_only":"api-demo.bybit.com" in core.BYBIT_REST_URL,"hold_sec":70,"rows":manager.admin_state()["rows"]})
            elif path in ("/trade","/admin"):
                if self.require_auth(): self.dashboard()
            elif path=="/admin/state":
                if self.require_auth(): self.send_json(manager.admin_state())
            elif path=="/signals":
                self.send_json({"version":VERSION,"ha":manager.ha_monitor()})
            else:
                self.send_response(404); self.end_headers()
        def do_POST(self): self.send_json({"error":"V0.7 HA70 has no manual order endpoint"},405)
        def log_message(self,fmt,*args): pass
    server=ThreadingHTTPServer(("0.0.0.0",core.PORT),Handler); Thread(target=server.serve_forever,daemon=True).start(); log.info("V0.7 HA70 HTTP on 0.0.0.0:%s /health /trade /admin",core.PORT)


async def auto_loop(manager):
    await asyncio.sleep(3)
    await asyncio.to_thread(manager.bootstrap)
    while True:
        await asyncio.to_thread(manager.tick)
        await asyncio.sleep(1)


async def run():
    if AUTO_DEMO_TRADING_ENABLED and "api-demo.bybit.com" not in core.BYBIT_REST_URL: raise RuntimeError("V0.7 hard guard: DEMO only")
    if v4.DEMO_LEVERAGE!=20: raise RuntimeError("V0.7 guard: leverage must be 20")
    if AUTO_NOTIONAL_USDT!=Decimal("25000"): raise RuntimeError("V0.7 guard: notional must be 25000")
    if AUTO_HOLD_SEC!=70: raise RuntimeError("V0.7 guard: AUTO_HOLD_SEC must be 70")
    if AUTO_EXCEPTION_SLOTS!=0 or AUTO_TOTAL_SLOTS<len(core.SYMBOLS): raise RuntimeError("V0.7 guard: slots must cover configured symbols")
    db=core.Database(core.DB_PATH); collector=core.Collector(db); account=core.AccountMonitor(db); features=v4.SafeFeatureEngine(db); manager=HA70Manager(db)
    start_server(collector,db,account,manager)
    log.info("trade-engine %s",VERSION)
    log.info("HA70 auto=%s ready=%s demo_only=%s notional=%s leverage=%sx hold=%ss slots=%s entry_delay<=%ss extreme_spread<=%.1fbps",AUTO_DEMO_TRADING_ENABLED,manager.auto_ready,"api-demo.bybit.com" in core.BYBIT_REST_URL,AUTO_NOTIONAL_USDT,v4.DEMO_LEVERAGE,AUTO_HOLD_SEC,AUTO_TOTAL_SLOTS,HA_MAX_ENTRY_DELAY_SEC,HA_MAX_SPREAD_BPS)
    await asyncio.gather(collector.run_forever(),core.feature_loop(features),core.account_loop(account),auto_loop(manager))

if __name__=="__main__": asyncio.run(run())
