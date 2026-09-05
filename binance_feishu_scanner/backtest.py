#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple event-based backtester for the scanner strategies.

Important:
- Entry is NEXT candle open after a signal, to reduce look-ahead bias.
- One concurrent position per strategy per symbol.
- If stop and target are both touched in the same candle, STOP is assumed first
  (conservative, because intrabar path is unknown from OHLC).
- Fees and slippage are configurable.
- This is research code, not a production execution engine.

Examples:
    python backtest.py --symbols BTCUSDT,ETHUSDT,SOLUSDT --days 180
    python backtest.py --symbols BTCUSDT --days 365 --rr 2.5 --fee-bps 4 --slippage-bps 2
"""

from __future__ import annotations

import argparse
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from binance_feishu_scanner import (
    BASE_INTERVAL,
    BinanceFuturesClient,
    evaluate_strategies,
    only_closed_candles,
    prepare_indicators,
    rows_to_df,
)

INTERVAL_MS = {
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "6h": 6 * 60 * 60_000,
    "8h": 8 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}


@dataclass
class Trade:
    symbol: str
    strategy: str
    direction: str
    signal_time: int
    entry_time: int
    exit_time: int
    entry: float
    stop: float
    target: float
    exit_price: float
    exit_reason: str
    net_r: float
    bars_held: int
    setup_score: float


def fetch_history(client: BinanceFuturesClient, symbol: str, interval: str, days: int) -> pd.DataFrame:
    if interval not in INTERVAL_MS:
        raise ValueError(f"Unsupported interval for this backtester: {interval}")

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 24 * 60 * 60 * 1000
    step = INTERVAL_MS[interval]
    chunks = []
    cursor = start_ms

    while cursor < end_ms:
        rows = client.klines(
            symbol=symbol,
            interval=interval,
            limit=1500,
            start_time=cursor,
            end_time=end_ms,
        )
        if not rows:
            break
        chunks.extend(rows)
        last_open = int(rows[-1][0])
        nxt = last_open + step
        if nxt <= cursor:
            break
        cursor = nxt
        if len(rows) < 1500:
            break
        time.sleep(0.08)

    df = rows_to_df(chunks)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time").reset_index(drop=True)
    df = only_closed_candles(df)
    return prepare_indicators(df)


def simulate_trade(
    df: pd.DataFrame,
    signal_idx: int,
    signal: dict,
    rr: float,
    max_hold: int,
    fee_bps: float,
    slippage_bps: float,
    symbol: str,
) -> Optional[Tuple[Trade, int]]:
    entry_idx = signal_idx + 1
    if entry_idx >= len(df):
        return None

    direction = signal["direction"]
    entry_raw = float(df.iloc[entry_idx]["open"])
    atr = float(df.iloc[signal_idx]["atr"])
    stop = float(signal["stop_price"])

    slip = slippage_bps / 10_000.0
    entry = entry_raw * (1 + slip if direction == "LONG" else 1 - slip)

    # Gap can make a structure stop invalid. Fall back to 1.5 ATR.
    if direction == "LONG" and stop >= entry:
        stop = entry - 1.5 * atr
    if direction == "SHORT" and stop <= entry:
        stop = entry + 1.5 * atr

    risk = abs(entry - stop)
    if not np.isfinite(risk) or risk <= 0:
        return None

    target = entry + rr * risk if direction == "LONG" else entry - rr * risk

    last_idx = min(len(df) - 1, entry_idx + max_hold - 1)
    exit_idx = last_idx
    exit_reason = "time"
    exit_raw = float(df.iloc[last_idx]["close"])

    for j in range(entry_idx, last_idx + 1):
        bar = df.iloc[j]
        hi = float(bar["high"])
        lo = float(bar["low"])

        if direction == "LONG":
            hit_stop = lo <= stop
            hit_target = hi >= target
            if hit_stop and hit_target:
                exit_idx, exit_reason, exit_raw = j, "stop_same_bar", stop
                break
            if hit_stop:
                exit_idx, exit_reason, exit_raw = j, "stop", stop
                break
            if hit_target:
                exit_idx, exit_reason, exit_raw = j, "target", target
                break
        else:
            hit_stop = hi >= stop
            hit_target = lo <= target
            if hit_stop and hit_target:
                exit_idx, exit_reason, exit_raw = j, "stop_same_bar", stop
                break
            if hit_stop:
                exit_idx, exit_reason, exit_raw = j, "stop", stop
                break
            if hit_target:
                exit_idx, exit_reason, exit_raw = j, "target", target
                break

    exit_price = exit_raw * (1 - slip if direction == "LONG" else 1 + slip)

    if direction == "LONG":
        pnl_pct = exit_price / entry - 1.0
    else:
        pnl_pct = 1.0 - exit_price / entry

    fee_pct = (2.0 * fee_bps) / 10_000.0
    net_pct = pnl_pct - fee_pct
    risk_pct = risk / entry
    net_r = net_pct / risk_pct

    trade = Trade(
        symbol=symbol,
        strategy=signal["strategy"],
        direction=direction,
        signal_time=int(df.iloc[signal_idx]["close_time"]),
        entry_time=int(df.iloc[entry_idx]["open_time"]),
        exit_time=int(df.iloc[exit_idx]["close_time"]),
        entry=entry,
        stop=stop,
        target=target,
        exit_price=exit_price,
        exit_reason=exit_reason,
        net_r=float(net_r),
        bars_held=int(exit_idx - entry_idx + 1),
        setup_score=float(signal["setup_score"]),
    )
    return trade, exit_idx


def backtest_symbol(
    df: pd.DataFrame,
    symbol: str,
    min_setup_score: float,
    rr: float,
    max_hold: int,
    fee_bps: float,
    slippage_bps: float,
) -> List[Trade]:
    trades: List[Trade] = []
    blocked_until: Dict[str, int] = {}
    warmup = 120

    for i in range(warmup, len(df) - 1):
        # Strategies only need recent history. Indicators were precomputed on full past-only series.
        window = df.iloc[max(0, i - 180):i + 1]
        signals = evaluate_strategies(window)
        for sig in signals:
            if float(sig["setup_score"]) < min_setup_score:
                continue
            strategy = sig["strategy"]
            if i <= blocked_until.get(strategy, -1):
                continue
            result = simulate_trade(
                df=df,
                signal_idx=i,
                signal=sig,
                rr=rr,
                max_hold=max_hold,
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
                symbol=symbol,
            )
            if result is None:
                continue
            trade, exit_idx = result
            trades.append(trade)
            blocked_until[strategy] = exit_idx
    return trades


def max_drawdown_r(rs: pd.Series) -> float:
    if rs.empty:
        return 0.0
    equity = rs.cumsum()
    peak = equity.cummax()
    dd = equity - peak
    return float(dd.min())


def summarize(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return pd.DataFrame(columns=[
            "symbol", "strategy", "trades", "win_rate_pct", "total_r",
            "avg_r", "profit_factor", "max_dd_r", "avg_bars"
        ])

    rows = []
    for (symbol, strategy), g in trades_df.groupby(["symbol", "strategy"], dropna=False):
        wins = g.loc[g["net_r"] > 0, "net_r"]
        losses = g.loc[g["net_r"] <= 0, "net_r"]
        gross_profit = wins.sum()
        gross_loss = abs(losses.sum())
        pf = gross_profit / gross_loss if gross_loss > 0 else np.inf
        rows.append({
            "symbol": symbol,
            "strategy": strategy,
            "trades": len(g),
            "win_rate_pct": round((g["net_r"] > 0).mean() * 100, 2),
            "total_r": round(g["net_r"].sum(), 2),
            "avg_r": round(g["net_r"].mean(), 3),
            "profit_factor": round(float(pf), 3) if np.isfinite(pf) else np.inf,
            "max_dd_r": round(max_drawdown_r(g["net_r"]), 2),
            "avg_bars": round(g["bars_held"].mean(), 1),
        })
    return pd.DataFrame(rows).sort_values(["total_r", "profit_factor"], ascending=False).reset_index(drop=True)


def main():
    p = argparse.ArgumentParser(description="Backtest scanner strategies")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT", help="Comma-separated symbols")
    p.add_argument("--days", type=int, default=180)
    p.add_argument("--interval", default=BASE_INTERVAL)
    p.add_argument("--min-setup-score", type=float, default=65.0)
    p.add_argument("--rr", type=float, default=2.0, help="Take-profit multiple of initial risk")
    p.add_argument("--max-hold", type=int, default=24, help="Max bars to hold")
    p.add_argument("--fee-bps", type=float, default=4.0, help="Fee per side in basis points; change to your real fee")
    p.add_argument("--slippage-bps", type=float, default=2.0, help="Slippage per side in basis points")
    p.add_argument("--out-dir", default="backtest_output")
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    client = BinanceFuturesClient()
    all_trades: List[Trade] = []

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    for symbol in symbols:
        print(f"\nFetching {symbol} {args.interval}, last {args.days} days...")
        df = fetch_history(client, symbol, args.interval, args.days)
        print(f"{symbol}: {len(df)} closed candles")
        if len(df) < 200:
            print("Not enough data; skipping.")
            continue
        trades = backtest_symbol(
            df=df,
            symbol=symbol,
            min_setup_score=args.min_setup_score,
            rr=args.rr,
            max_hold=args.max_hold,
            fee_bps=args.fee_bps,
            slippage_bps=args.slippage_bps,
        )
        all_trades.extend(trades)
        print(f"{symbol}: {len(trades)} trades")

    trades_df = pd.DataFrame([t.__dict__ for t in all_trades])
    summary_df = summarize(trades_df)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    trades_path = out_dir / f"trades_{stamp}.csv"
    summary_path = out_dir / f"summary_{stamp}.csv"
    trades_df.to_csv(trades_path, index=False)
    summary_df.to_csv(summary_path, index=False)

    print("\n=== SUMMARY ===")
    if summary_df.empty:
        print("No trades.")
    else:
        print(summary_df.to_string(index=False))
    print(f"\nSaved:\n  {trades_path}\n  {summary_path}")


if __name__ == "__main__":
    main()
