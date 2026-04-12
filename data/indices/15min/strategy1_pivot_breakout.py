"""
Strategy 1: Classic Pivot Point Breakout
─────────────────────────────────────────
Concept (Pivot Boss Ch.1):
  Daily pivot levels (PP, R1-R3, S1-S3) act as key S/R zones.
  Price breaking above R1 with PP as support = bullish momentum trade.
  Price breaking below S1 with PP as resistance = bearish momentum trade.

Entry Rules:
  - Compute daily pivots from previous day's OHLC
  - Long  : first 15-min close above R1, provided close > PP (trend filter)
  - Short : first 15-min close below S1, provided close < PP
  - Entry only after 09:30 (skip opening noise)

Exit Rules:
  - Target : R2 (long) / S2 (short)  [or 0.7% profit if R2/S2 too far]
  - Stop   : 0.4% from entry
  - EOD    : hard exit at 15:00

Risk Controls:
  - Max 1 trade per day
  - No trade if day's open gaps > 1% beyond R1/S1 (avoid chasing)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from utils import (load_data, daily_ohlc, calc_classic_pivots,
                   compute_metrics, plot_equity, print_metrics,
                   save_trades, save_metrics, check_exit, pnl_from_exit,
                   OUTPUT_DIR)

STRATEGY_NAME = "Strategy 1: Classic Pivot Breakout"
SL_PCT        = 0.004    # 0.4%
TGT_CAP_PCT   = 0.007    # max 0.7% target if R2/S2 too far
GAP_FILTER    = 0.010    # skip if gap > 1%
EXIT_TIME     = pd.Timestamp("1900-01-01 15:00:00").time()
ENTRY_AFTER   = pd.Timestamp("1900-01-01 09:30:00").time()


def run(df: pd.DataFrame, daily: pd.DataFrame) -> tuple:
    # Build pivot lookup: trading day → levels
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
        if day not in pivots:
            equity.append(cum_pnl)
            continue

        lvl = pivots[day]
        PP, R1, R2, S1, S2 = lvl["PP"], lvl["R1"], lvl["R2"], lvl["S1"], lvl["S2"]

        bars      = grp.sort_values("date")
        day_open  = bars.iloc[0]["open"]

        # Gap filter: skip day if open is already well beyond R1 or S1
        if day_open > R1 * (1 + GAP_FILTER) or day_open < S1 * (1 - GAP_FILTER):
            equity.extend([cum_pnl] * len(bars))
            continue

        position  = None
        entry_px  = None
        sl        = None
        tgt       = None
        traded    = False
        prev_close = None

        for _, bar in bars.iterrows():
            t = bar["date"].time()
            c, h, l = bar["close"], bar["high"], bar["low"]
            is_eod  = t >= EXIT_TIME

            # Manage open position
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
                        "sl": round(sl, 2), "tgt": round(tgt, 2),
                    })
                    position = None

            equity.append(cum_pnl)

            # Entry logic (only after 09:30, one trade per day)
            if not traded and position is None and prev_close is not None \
                    and t > ENTRY_AFTER and not is_eod:

                # Long breakout above R1
                if prev_close < R1 and c > R1 and c > PP:
                    entry_px = c
                    sl  = entry_px * (1 - SL_PCT)
                    tgt = min(R2, entry_px * (1 + TGT_CAP_PCT))
                    position = "long"
                    traded   = True

                # Short breakout below S1
                elif prev_close > S1 and c < S1 and c < PP:
                    entry_px = c
                    sl  = entry_px * (1 + SL_PCT)
                    tgt = max(S2, entry_px * (1 - TGT_CAP_PCT))
                    position = "short"
                    traded   = True

            prev_close = c

    equity_s = pd.Series(equity) + 10000
    return trades, equity_s


def main():
    print(f"Running {STRATEGY_NAME}...")
    df    = load_data()
    daily = daily_ohlc(df)

    trades, equity = run(df, daily)
    metrics, dd    = compute_metrics(equity, trades)

    print_metrics(STRATEGY_NAME, metrics)

    chart = plot_equity(equity, dd, STRATEGY_NAME, "s1_pivot_breakout_equity.png")
    save_trades(trades,   "s1_pivot_breakout_trades.csv")
    save_metrics(metrics, "s1_pivot_breakout_metrics.csv")

    print(f"\n  Results saved to: {OUTPUT_DIR}/")
    return trades, equity, metrics


if __name__ == "__main__":
    main()
