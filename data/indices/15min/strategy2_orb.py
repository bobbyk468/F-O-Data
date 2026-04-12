"""
Strategy 2: Opening Range Breakout (ORB — 30-min)
───────────────────────────────────────────────────
Concept:
  The first 30 minutes (09:15 + 09:30 candles) establish the Opening Range.
  A breakout beyond this range often signals the direction of the day's trend.
  Combined with pivot levels as confluence filters.

Entry Rules:
  - OR = High/Low of the first two 15-min candles (09:15 & 09:30)
  - Long  : first close above OR_High after 09:30
             — only if OR_High is also above or near daily PP (bullish bias)
  - Short : first close below OR_Low  after 09:30
             — only if OR_Low  is below or near daily PP (bearish bias)
  - Minimum OR range: 0.15% of price (filter low-volatility days)

Exit Rules:
  - Target : entry + OR_range × 1.5
  - Stop   : opposite OR boundary - 0.2% buffer
  - EOD    : hard exit at 14:45 (avoid last-30-min reversal risk)

Risk Controls:
  - Max 1 trade per day
  - Skip days where OR range > 1.5% of price (too gappy/volatile)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from utils import (load_data, daily_ohlc, calc_classic_pivots,
                   compute_metrics, plot_equity, print_metrics,
                   save_trades, save_metrics, check_exit, pnl_from_exit,
                   OUTPUT_DIR)

STRATEGY_NAME  = "Strategy 2: Opening Range Breakout (ORB 30-min)"
TGT_MULT       = 1.5
SL_BUFFER_PCT  = 0.002    # 0.2% buffer beyond OR
MIN_OR_PCT     = 0.0015   # min OR = 0.15% of price
MAX_OR_PCT     = 0.015    # max OR = 1.5% of price (skip volatile days)
EXIT_TIME      = pd.Timestamp("1900-01-01 14:45:00").time()
OR_END_TIME    = pd.Timestamp("1900-01-01 09:30:00").time()
ENTRY_AFTER    = pd.Timestamp("1900-01-01 09:30:00").time()


def run(df: pd.DataFrame, daily: pd.DataFrame) -> tuple:
    # Build daily pivot lookup for PP filter
    pivots = {}
    days = daily["day"].tolist()
    for i in range(1, len(days)):
        prev = daily.iloc[i - 1]
        lvl  = calc_classic_pivots(prev["high"], prev["low"], prev["close"])
        pivots[days[i].date()] = lvl

    trades  = []
    equity  = [0.0]
    cum_pnl = 0.0

    for day, grp in df.groupby("day"):
        bars = grp.sort_values("date")

        # Build Opening Range from first 2 bars
        or_bars  = bars[bars["date"].dt.time <= OR_END_TIME]
        if len(or_bars) < 2:
            equity.extend([cum_pnl] * len(bars))
            continue

        or_high  = or_bars["high"].max()
        or_low   = or_bars["low"].min()
        or_range = or_high - or_low
        mid_px   = (or_high + or_low) / 2

        or_pct = or_range / mid_px
        if or_pct < MIN_OR_PCT or or_pct > MAX_OR_PCT:
            equity.extend([cum_pnl] * len(bars))
            continue

        # PP for bias filter
        pp = pivots[day]["PP"] if day in pivots else mid_px

        position  = None
        entry_px  = None
        sl        = None
        tgt       = None
        traded    = False

        for _, bar in bars.iterrows():
            t = bar["date"].time()
            c, h, l = bar["close"], bar["high"], bar["low"]
            is_eod  = t >= EXIT_TIME

            if position is not None:
                exit_px, reason = check_exit(position, entry_px, sl, tgt,
                                             h, l, c, is_eod)
                if exit_px is not None:
                    pnl = pnl_from_exit(position, entry_px, exit_px)
                    cum_pnl += pnl
                    trades.append({
                        "day": day, "side": position,
                        "entry": round(entry_px, 2), "exit": round(exit_px, 2),
                        "pnl": round(pnl, 2), "reason": reason,
                        "or_high": round(or_high, 2), "or_low": round(or_low, 2),
                        "sl": round(sl, 2), "tgt": round(tgt, 2),
                    })
                    position = None

            equity.append(cum_pnl)

            if not traded and position is None and t > ENTRY_AFTER and not is_eod:
                # Long breakout — only if OR_High is bullish relative to PP
                if c > or_high and or_high >= pp * 0.999:
                    entry_px = c
                    sl  = or_low  * (1 - SL_BUFFER_PCT)
                    tgt = entry_px + or_range * TGT_MULT
                    position = "long"
                    traded   = True

                # Short breakout — only if OR_Low is bearish relative to PP
                elif c < or_low and or_low <= pp * 1.001:
                    entry_px = c
                    sl  = or_high * (1 + SL_BUFFER_PCT)
                    tgt = entry_px - or_range * TGT_MULT
                    position = "short"
                    traded   = True

    equity_s = pd.Series(equity) + 10000
    return trades, equity_s


def main():
    print(f"Running {STRATEGY_NAME}...")
    df    = load_data()
    daily = daily_ohlc(df)

    trades, equity = run(df, daily)
    metrics, dd    = compute_metrics(equity, trades)

    print_metrics(STRATEGY_NAME, metrics)

    plot_equity(equity, dd, STRATEGY_NAME, "s2_orb_equity.png")
    save_trades(trades,   "s2_orb_trades.csv")
    save_metrics(metrics, "s2_orb_metrics.csv")

    print(f"\n  Results saved to: {OUTPUT_DIR}/")
    return trades, equity, metrics


if __name__ == "__main__":
    main()
