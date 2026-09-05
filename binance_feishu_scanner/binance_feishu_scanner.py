#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Binance USDⓈ-M futures signal scanner + Feishu push.

What it does
------------
1) Uses public Binance market-data REST endpoints. No Binance API key is required.
2) Selects liquid USDT perpetual contracts by 24h quote volume.
3) Scans closed 30m candles (configurable) with four strategies:
   - MACD bearish divergence after a rally + consolidation + failed new high
   - Liquidity sweep / failed breakout (pure price action)
   - Breakout + retest
   - Volatility compression + expansion breakout
4) Assigns a 0-100 signal-quality score and grade.
5) Pushes a digest to a Feishu custom-bot webhook every 30 minutes.
6) Does NOT place orders.

Run:
    pip install -r requirements.txt
    cp .env.example .env
    python binance_feishu_scanner.py --test-feishu
    python binance_feishu_scanner.py --once
    python binance_feishu_scanner.py
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import logging
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

BASE_URL = os.getenv("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com").rstrip("/")
BASE_INTERVAL = os.getenv("BASE_INTERVAL", "30m")
TOP_N = int(os.getenv("TOP_N", "80"))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME", "10000000"))
KLINE_LIMIT = int(os.getenv("KLINE_LIMIT", "300"))
MIN_SCORE = float(os.getenv("MIN_SCORE", "72"))
MAX_PUSH = int(os.getenv("MAX_PUSH", "12"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "10"))
SCHEDULE_MINUTES = os.getenv("SCHEDULE_MINUTES", "1,31")
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "90"))
SEND_EMPTY_REPORT = os.getenv("SEND_EMPTY_REPORT", "true").lower() in {"1", "true", "yes", "y"}
STATE_FILE = Path(os.getenv("STATE_FILE", "scanner_state.json"))
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK", "").strip()
FEISHU_SECRET = os.getenv("FEISHU_SECRET", "").strip()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("scanner")


@dataclass
class Signal:
    symbol: str
    strategy: str
    direction: str  # LONG / SHORT
    setup_score: float
    final_score: float
    grade: str
    price: float
    stop_price: float
    atr: float
    candle_close_time: int
    reasons: List[str]
    quote_volume_24h: float = 0.0

    @property
    def key(self) -> str:
        return f"{self.symbol}|{self.strategy}|{self.direction}"


def build_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.6,
        status_forcelist=(418, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS * 2, pool_maxsize=MAX_WORKERS * 2)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "binance-feishu-signal-scanner/1.0"})
    return session


SESSION = build_session()


class BinanceFuturesClient:
    def __init__(self, base_url: str = BASE_URL, session: requests.Session = SESSION):
        self.base_url = base_url
        self.session = session

    def _get(self, path: str, params: Optional[dict] = None):
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def exchange_info(self) -> dict:
        return self._get("/fapi/v1/exchangeInfo")

    def ticker_24h(self) -> list:
        return self._get("/fapi/v1/ticker/24hr")

    def klines(
        self,
        symbol: str,
        interval: str = BASE_INTERVAL,
        limit: int = KLINE_LIMIT,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> list:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = int(start_time)
        if end_time is not None:
            params["endTime"] = int(end_time)
        return self._get("/fapi/v1/klines", params=params)


def rows_to_df(rows: Sequence[Sequence]) -> pd.DataFrame:
    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades",
        "taker_buy_base", "taker_buy_quote", "ignore",
    ]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df
    numeric_cols = ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base", "taker_buy_quote"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")
    df["close_time"] = pd.to_numeric(df["close_time"], errors="coerce").astype("int64")
    df["trades"] = pd.to_numeric(df["trades"], errors="coerce")
    return df.dropna(subset=["open", "high", "low", "close"])


def only_closed_candles(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    now_ms = int(time.time() * 1000)
    return df.loc[df["close_time"] < now_ms].reset_index(drop=True)


def prepare_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # Wilder-style ATR
    df["atr"] = tr.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()

    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()

    candle_range = (df["high"] - df["low"]).replace(0, np.nan)
    df["body_frac"] = (df["close"] - df["open"]).abs() / candle_range
    df["upper_wick_frac"] = (df["high"] - df[["open", "close"]].max(axis=1)) / candle_range
    df["lower_wick_frac"] = (df[["open", "close"]].min(axis=1) - df["low"]) / candle_range
    return df


def clamp(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def grade(score: float) -> str:
    if score >= 90:
        return "S"
    if score >= 82:
        return "A"
    if score >= 72:
        return "B"
    if score >= 62:
        return "C"
    return "D"


def last_confirmed_swing_high(df: pd.DataFrame, lookback: int = 80, wing: int = 3, min_gap: int = 5) -> Optional[int]:
    n = len(df)
    if n < wing * 2 + min_gap + 10:
        return None
    hi = df["high"].to_numpy()
    latest_allowed = n - 1 - min_gap
    start = max(wing, n - lookback)
    for i in range(latest_allowed, start - 1, -1):
        left = hi[i - wing:i]
        right = hi[i + 1:i + wing + 1]
        if len(right) < wing:
            continue
        if hi[i] >= np.max(left) and hi[i] > np.max(right):
            return i
    return None


def last_confirmed_swing_low(df: pd.DataFrame, lookback: int = 80, wing: int = 3, min_gap: int = 5) -> Optional[int]:
    n = len(df)
    if n < wing * 2 + min_gap + 10:
        return None
    lo = df["low"].to_numpy()
    latest_allowed = n - 1 - min_gap
    start = max(wing, n - lookback)
    for i in range(latest_allowed, start - 1, -1):
        left = lo[i - wing:i]
        right = lo[i + 1:i + wing + 1]
        if len(right) < wing:
            continue
        if lo[i] <= np.min(left) and lo[i] < np.min(right):
            return i
    return None


def strategy_macd_bearish_divergence(df: pd.DataFrame) -> Optional[dict]:
    """
    User-inspired short setup, made stricter:
    rally -> confirmed swing high -> consolidation/pullback -> marginal new high
    while MACD is weaker -> rejection / failed breakout.
    """
    n = len(df)
    if n < 100:
        return None
    cur = df.iloc[-1]
    if not np.isfinite(cur["atr"]) or cur["atr"] <= 0:
        return None

    p = last_confirmed_swing_high(df, lookback=75, wing=3, min_gap=6)
    if p is None or p < 30:
        return None

    prev = df.iloc[p]
    prev_high = float(prev["high"])
    atr_p = float(prev["atr"]) if np.isfinite(prev["atr"]) else float(cur["atr"])
    atr_c = float(cur["atr"])

    # 1) The first high should have been preceded by a meaningful rally.
    pre = df.iloc[max(0, p - 28):p + 1]
    pre_floor = float(pre["low"].min())
    rally_pct = prev_high / max(pre_floor, 1e-12) - 1.0
    rally_atr = (prev_high - pre_floor) / max(atr_p, 1e-12)
    if rally_pct < 0.025 or rally_atr < 4.0:
        return None

    # 2) There must be enough bars between the old high and today's challenge.
    middle = df.iloc[p + 1:-1]
    if len(middle) < 4:
        return None

    # 3) Old high was not already decisively broken during the middle phase.
    if float(middle["close"].max()) > prev_high + 0.65 * atr_p:
        return None

    # 4) New marginal high now.
    overshoot = float(cur["high"]) - prev_high
    if overshoot < 0.10 * atr_c:
        return None
    if overshoot > 2.2 * atr_c:
        return None  # likely a genuine expansion breakout instead of marginal divergence

    # 5) MACD weakening.
    prev_macd = float(prev["macd"])
    cur_macd = float(cur["macd"])
    prev_hist = float(prev["macd_hist"])
    cur_hist = float(cur["macd_hist"])
    if prev_macd <= 0:
        return None
    macd_weaker = cur_macd < prev_macd * 0.88
    hist_weaker = cur_hist < prev_hist
    recent_macd_peak = float(df["macd"].iloc[-16:].max())
    rolled_over = cur_macd < recent_macd_peak * 0.94
    if not (macd_weaker and hist_weaker and rolled_over):
        return None

    # 6) Rejection: don't short a clean breakout candle.
    rejected_back_below = float(cur["close"]) < prev_high
    bearish_close = float(cur["close"]) < float(cur["open"])
    upper_wick = float(cur["upper_wick_frac"]) if np.isfinite(cur["upper_wick_frac"]) else 0.0
    rejection = rejected_back_below or (bearish_close and upper_wick >= 0.28)
    if not rejection:
        return None

    # 7) Consolidation quality: middle phase shouldn't be an uncontrolled crash.
    middle_low = float(middle["low"].min())
    pullback_atr = (prev_high - middle_low) / max(atr_p, 1e-12)
    if pullback_atr > 8.0:
        return None

    divergence_ratio = (prev_macd - cur_macd) / max(abs(prev_macd), 1e-12)
    score = 58.0
    score += clamp(rally_atr - 4.0, 0, 8)
    score += clamp(divergence_ratio * 35, 0, 12)
    score += 8 if rejected_back_below else 4
    score += clamp(upper_wick * 12, 0, 7)
    score += clamp(5.0 - abs(pullback_atr - 3.0), 0, 5)
    score = clamp(score, 0, 96)

    return {
        "strategy": "MACD顶背离+假突破",
        "direction": "SHORT",
        "setup_score": score,
        "stop_price": float(cur["high"] + 0.35 * atr_c),
        "reasons": [
            f"前高前涨幅约 {rally_pct * 100:.1f}%",
            f"价格新高超出前高 {overshoot / max(prev_high,1e-12) * 100:.2f}%",
            f"MACD较前高弱 {divergence_ratio * 100:.1f}%",
            "新高后出现价格拒绝/收回",
        ],
    }


def strategy_liquidity_sweep(df: pd.DataFrame, lookback: int = 24) -> Optional[dict]:
    """Pure price action: sweep a recent extreme, then close back inside the range."""
    if len(df) < lookback + 20:
        return None
    cur = df.iloc[-1]
    atr = float(cur["atr"])
    if not np.isfinite(atr) or atr <= 0:
        return None

    prior = df.iloc[-lookback - 1:-1]
    prior_high = float(prior["high"].max())
    prior_low = float(prior["low"].min())
    close = float(cur["close"])
    high = float(cur["high"])
    low = float(cur["low"])
    open_ = float(cur["open"])
    upper_wick = float(cur["upper_wick_frac"]) if np.isfinite(cur["upper_wick_frac"]) else 0.0
    lower_wick = float(cur["lower_wick_frac"]) if np.isfinite(cur["lower_wick_frac"]) else 0.0

    short_sweep = high > prior_high + 0.08 * atr and close < prior_high and upper_wick >= 0.28
    long_sweep = low < prior_low - 0.08 * atr and close > prior_low and lower_wick >= 0.28

    if short_sweep:
        sweep_atr = (high - prior_high) / atr
        score = 62 + clamp(sweep_atr * 10, 0, 8) + clamp(upper_wick * 22, 0, 12)
        if close < open_:
            score += 5
        return {
            "strategy": "裸K扫前高后收回",
            "direction": "SHORT",
            "setup_score": clamp(score, 0, 95),
            "stop_price": high + 0.25 * atr,
            "reasons": [
                f"刺破近{lookback}根K前高后收回",
                f"上影占当根振幅约 {upper_wick * 100:.0f}%",
                "属于假突破/流动性扫单结构",
            ],
        }

    if long_sweep:
        sweep_atr = (prior_low - low) / atr
        score = 62 + clamp(sweep_atr * 10, 0, 8) + clamp(lower_wick * 22, 0, 12)
        if close > open_:
            score += 5
        return {
            "strategy": "裸K扫前低后收回",
            "direction": "LONG",
            "setup_score": clamp(score, 0, 95),
            "stop_price": low - 0.25 * atr,
            "reasons": [
                f"刺破近{lookback}根K前低后收回",
                f"下影占当根振幅约 {lower_wick * 100:.0f}%",
                "属于假跌破/流动性扫单结构",
            ],
        }
    return None


def strategy_breakout_retest(df: pd.DataFrame, lookback: int = 22) -> Optional[dict]:
    """Breakout 1-3 bars ago, current candle retests old structure and holds."""
    if len(df) < lookback + 30:
        return None

    cur = df.iloc[-1]
    atr = float(cur["atr"])
    if not np.isfinite(atr) or atr <= 0:
        return None

    for breakout_idx in range(len(df) - 2, max(len(df) - 5, lookback), -1):
        b = df.iloc[breakout_idx]
        hist = df.iloc[breakout_idx - lookback:breakout_idx]
        if len(hist) < lookback:
            continue
        resistance = float(hist["high"].max())
        support = float(hist["low"].min())
        b_atr = float(b["atr"]) if np.isfinite(b["atr"]) else atr

        # Long breakout -> retest resistance as support
        if float(b["close"]) > resistance + 0.18 * b_atr:
            touched = float(cur["low"]) <= resistance + 0.30 * atr
            held = float(cur["close"]) > resistance
            not_too_deep = float(cur["low"]) >= resistance - 0.75 * atr
            if touched and held and not_too_deep:
                body = float(b["body_frac"]) if np.isfinite(b["body_frac"]) else 0
                score = 61 + clamp(body * 18, 0, 12)
                score += clamp((float(b["close"]) - resistance) / max(b_atr, 1e-12) * 10, 0, 8)
                if float(cur["close"]) > float(cur["open"]):
                    score += 5
                return {
                    "strategy": "突破回踩确认",
                    "direction": "LONG",
                    "setup_score": clamp(score, 0, 94),
                    "stop_price": min(float(cur["low"]), resistance - 0.55 * atr) - 0.15 * atr,
                    "reasons": [
                        f"突破近{lookback}根K阻力",
                        "当前回踩旧阻力并收回其上",
                        "属于顺势结构确认",
                    ],
                }

        # Short breakout -> retest support as resistance
        if float(b["close"]) < support - 0.18 * b_atr:
            touched = float(cur["high"]) >= support - 0.30 * atr
            held = float(cur["close"]) < support
            not_too_high = float(cur["high"]) <= support + 0.75 * atr
            if touched and held and not_too_high:
                body = float(b["body_frac"]) if np.isfinite(b["body_frac"]) else 0
                score = 61 + clamp(body * 18, 0, 12)
                score += clamp((support - float(b["close"])) / max(b_atr, 1e-12) * 10, 0, 8)
                if float(cur["close"]) < float(cur["open"]):
                    score += 5
                return {
                    "strategy": "跌破回抽确认",
                    "direction": "SHORT",
                    "setup_score": clamp(score, 0, 94),
                    "stop_price": max(float(cur["high"]), support + 0.55 * atr) + 0.15 * atr,
                    "reasons": [
                        f"跌破近{lookback}根K支撑",
                        "当前回抽旧支撑并被压回其下",
                        "属于顺势结构确认",
                    ],
                }
    return None


def strategy_compression_breakout(df: pd.DataFrame, box_lookback: int = 14, compression_bars: int = 6) -> Optional[dict]:
    """Narrow recent true ranges + decisive break of the prior box."""
    if len(df) < 70:
        return None
    cur = df.iloc[-1]
    atr = float(cur["atr"])
    if not np.isfinite(atr) or atr <= 0:
        return None

    prior_box = df.iloc[-box_lookback - 1:-1]
    box_high = float(prior_box["high"].max())
    box_low = float(prior_box["low"].min())
    comp = df.iloc[-compression_bars - 1:-1]
    older = df.iloc[-24:-compression_bars - 1]

    comp_tr = (comp["high"] - comp["low"]).mean()
    older_tr = (older["high"] - older["low"]).mean()
    if not np.isfinite(comp_tr) or not np.isfinite(older_tr) or older_tr <= 0:
        return None
    compression_ratio = comp_tr / older_tr
    if compression_ratio > 0.78:
        return None

    close = float(cur["close"])
    open_ = float(cur["open"])
    body = abs(close - open_)
    body_atr = body / atr
    if body_atr < 0.55:
        return None

    if close > box_high + 0.12 * atr and close > open_:
        score = 60
        score += clamp((0.78 - compression_ratio) * 35, 0, 10)
        score += clamp((body_atr - 0.55) * 12, 0, 12)
        return {
            "strategy": "压缩后向上扩张",
            "direction": "LONG",
            "setup_score": clamp(score, 0, 93),
            "stop_price": max(box_low, close - 2.2 * atr),
            "reasons": [
                f"近{compression_bars}根K平均振幅明显收缩",
                f"实体约 {body_atr:.2f} ATR",
                f"收盘突破近{box_lookback}根K箱体上沿",
            ],
        }

    if close < box_low - 0.12 * atr and close < open_:
        score = 60
        score += clamp((0.78 - compression_ratio) * 35, 0, 10)
        score += clamp((body_atr - 0.55) * 12, 0, 12)
        return {
            "strategy": "压缩后向下扩张",
            "direction": "SHORT",
            "setup_score": clamp(score, 0, 93),
            "stop_price": min(box_high, close + 2.2 * atr),
            "reasons": [
                f"近{compression_bars}根K平均振幅明显收缩",
                f"实体约 {body_atr:.2f} ATR",
                f"收盘跌破近{box_lookback}根K箱体下沿",
            ],
        }
    return None


STRATEGIES = [
    strategy_macd_bearish_divergence,
    strategy_liquidity_sweep,
    strategy_breakout_retest,
    strategy_compression_breakout,
]


def evaluate_strategies(df: pd.DataFrame) -> List[dict]:
    out = []
    for func in STRATEGIES:
        try:
            s = func(df)
            if s:
                out.append(s)
        except Exception:
            log.exception("Strategy error in %s", func.__name__)
    return out


def liquidity_quality(quote_volume_24h: float) -> float:
    """
    Returns 0-100 liquidity quality.
    Roughly: $1m=0, $10m=33, $100m=67, $1b=100.
    It is a ranking component, NOT probability.
    """
    if quote_volume_24h <= 0:
        return 0.0
    return clamp((math.log10(quote_volume_24h) - 6.0) / 3.0 * 100.0, 0, 100)


def final_quality_score(setup_score: float, quote_volume_24h: float) -> float:
    # Technical setup dominates. Liquidity is a secondary execution-quality proxy.
    return clamp(0.87 * setup_score + 0.13 * liquidity_quality(quote_volume_24h), 0, 100)


def select_universe(client: BinanceFuturesClient) -> List[dict]:
    info = client.exchange_info()
    valid = {
        x["symbol"]
        for x in info.get("symbols", [])
        if x.get("status") == "TRADING"
        and x.get("contractType") == "PERPETUAL"
        and x.get("quoteAsset") == "USDT"
    }

    tickers = client.ticker_24h()
    universe = []
    for t in tickers:
        sym = t.get("symbol")
        if sym not in valid:
            continue
        try:
            qv = float(t.get("quoteVolume", 0.0))
            last = float(t.get("lastPrice", 0.0))
        except (TypeError, ValueError):
            continue
        if qv < MIN_QUOTE_VOLUME:
            continue
        universe.append({"symbol": sym, "quote_volume": qv, "last_price": last})

    universe.sort(key=lambda x: x["quote_volume"], reverse=True)
    return universe[:TOP_N]


def scan_symbol(client: BinanceFuturesClient, item: dict) -> List[Signal]:
    symbol = item["symbol"]
    qv = float(item["quote_volume"])
    rows = client.klines(symbol=symbol, interval=BASE_INTERVAL, limit=KLINE_LIMIT)
    df = prepare_indicators(only_closed_candles(rows_to_df(rows)))
    if len(df) < 100:
        return []

    sigs: List[Signal] = []
    for raw in evaluate_strategies(df):
        setup = float(raw["setup_score"])
        final = final_quality_score(setup, qv)
        cur = df.iloc[-1]
        sigs.append(
            Signal(
                symbol=symbol,
                strategy=raw["strategy"],
                direction=raw["direction"],
                setup_score=round(setup, 1),
                final_score=round(final, 1),
                grade=grade(final),
                price=float(cur["close"]),
                stop_price=float(raw["stop_price"]),
                atr=float(cur["atr"]),
                candle_close_time=int(cur["close_time"]),
                reasons=list(raw["reasons"]),
                quote_volume_24h=qv,
            )
        )
    return sigs


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        log.warning("Could not read state file; starting fresh.")
        return {}


def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def dedupe_signals(signals: List[Signal]) -> List[Signal]:
    state = load_state()
    now = time.time()
    keep = []

    for s in signals:
        prev = state.get(s.key)
        allow = True
        if prev:
            elapsed_min = (now - float(prev.get("sent_at", 0))) / 60.0
            prev_score = float(prev.get("score", 0))
            # During cooldown, only resend if score improved materially.
            if elapsed_min < COOLDOWN_MINUTES and s.final_score < prev_score + 8:
                allow = False
        if allow:
            keep.append(s)
            state[s.key] = {
                "sent_at": now,
                "score": s.final_score,
                "candle_close_time": s.candle_close_time,
            }

    # prune old keys after 3 days
    cutoff = now - 3 * 24 * 3600
    state = {k: v for k, v in state.items() if float(v.get("sent_at", 0)) >= cutoff}
    save_state(state)
    return keep


def fmt_money(x: float) -> str:
    if x >= 1e9:
        return f"${x / 1e9:.2f}B"
    if x >= 1e6:
        return f"${x / 1e6:.1f}M"
    if x >= 1e3:
        return f"${x / 1e3:.1f}K"
    return f"${x:.0f}"


def fmt_price(x: float) -> str:
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:.5f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def build_digest(signals: List[Signal], scanned: int, failures: int) -> str:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"【币安合约量化扫描】{now_text}",
        f"周期：{BASE_INTERVAL} | 扫描：{scanned} 个高流动性USDT永续 | 阈值：{MIN_SCORE:.0f}",
        f"发现：{len(signals)} 个新信号 | 请求失败：{failures}",
        "说明：评级=形态质量+流动性排序，不代表胜率；仅用于研究/回测。",
        "",
    ]
    if not signals:
        lines.append("本轮没有达到阈值且通过去重规则的新信号。")
        return "\n".join(lines)

    for i, s in enumerate(signals[:MAX_PUSH], 1):
        side = "做多观察" if s.direction == "LONG" else "做空观察"
        lines.extend(
            [
                f"{i}. {s.symbol} | {side} | {s.grade}级 {s.final_score:.1f}",
                f"   策略：{s.strategy}",
                f"   收盘：{fmt_price(s.price)} | 结构失效参考：{fmt_price(s.stop_price)}",
                f"   24h成交额：{fmt_money(s.quote_volume_24h)}",
                f"   依据：{'；'.join(s.reasons)}",
                "",
            ]
        )
    if len(signals) > MAX_PUSH:
        lines.append(f"另有 {len(signals) - MAX_PUSH} 个信号未展示；可提高 MAX_PUSH。")
    return "\n".join(lines)


def feishu_sign(timestamp: int, secret: str) -> str:
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(
        string_to_sign.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def send_feishu(text: str) -> None:
    if not FEISHU_WEBHOOK:
        log.warning("FEISHU_WEBHOOK is empty; printing instead of pushing.")
        print(text)
        return

    payload = {
        "msg_type": "text",
        "content": {"text": text},
    }
    if FEISHU_SECRET:
        ts = int(time.time())
        payload["timestamp"] = ts
        payload["sign"] = feishu_sign(ts, FEISHU_SECRET)

    resp = SESSION.post(FEISHU_WEBHOOK, json=payload, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    # Feishu responses exist in more than one shape in examples/docs.
    code = data.get("code", data.get("StatusCode", 0))
    if code not in (0, None):
        raise RuntimeError(f"Feishu push failed: {data}")


def run_scan(push: bool = True) -> List[Signal]:
    client = BinanceFuturesClient()
    started = time.time()
    log.info("Loading market universe...")
    universe = select_universe(client)
    log.info("Scanning %d symbols on %s...", len(universe), BASE_INTERVAL)

    all_signals: List[Signal] = []
    failures = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(scan_symbol, client, item): item["symbol"] for item in universe}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                all_signals.extend(future.result())
            except Exception as e:
                failures += 1
                log.warning("%s scan failed: %s", sym, e)

    all_signals = [s for s in all_signals if s.final_score >= MIN_SCORE]
    all_signals.sort(key=lambda s: (s.final_score, s.quote_volume_24h), reverse=True)
    fresh = dedupe_signals(all_signals)

    digest = build_digest(fresh, scanned=len(universe), failures=failures)
    log.info("Scan finished in %.1fs; %d threshold signals, %d fresh.", time.time() - started, len(all_signals), len(fresh))

    if push and (fresh or SEND_EMPTY_REPORT):
        send_feishu(digest)
    else:
        print(digest)
    return fresh


def test_feishu() -> None:
    text = (
        "【测试】币安量化扫描器已连接飞书。\n"
        "后续会按设定周期推送信号摘要；该程序只做监控与研究，不自动下单。"
    )
    send_feishu(text)


def main():
    parser = argparse.ArgumentParser(description="Binance futures scanner + Feishu bot")
    parser.add_argument("--once", action="store_true", help="Run one scan and exit.")
    parser.add_argument("--test-feishu", action="store_true", help="Send one Feishu test message and exit.")
    parser.add_argument("--no-push", action="store_true", help="Run scan but print instead of Feishu push.")
    args = parser.parse_args()

    if args.test_feishu:
        test_feishu()
        return

    if args.once:
        run_scan(push=not args.no_push)
        return

    # Run once immediately, then at minute 1 and 31 by default.
    # Using :01/:31 instead of exact :00/:30 reduces the chance of reading a still-closing 30m candle.
    try:
        run_scan(push=not args.no_push)
    except Exception:
        log.exception("Initial scan failed.")

    scheduler = BlockingScheduler()
    trigger = CronTrigger(minute=SCHEDULE_MINUTES)
    scheduler.add_job(
        lambda: run_scan(push=not args.no_push),
        trigger=trigger,
        id="binance_scan",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )
    log.info("Scheduler active. Minutes each hour: %s", SCHEDULE_MINUTES)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Stopped.")


if __name__ == "__main__":
    main()
