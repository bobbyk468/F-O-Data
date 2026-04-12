"""
Strategy 3: Camarilla Pivot — Mean Reversion + Breakout
─────────────────────────────────────────────────────────
Concept (Pivot Boss):
  Camarilla levels act as tightly clustered intraday S/R zones.
  H3/L3 = mean-reversion levels (price fades back to PP ~70% of the time)
  H4/L4 = breakout levels (if breached, strong directional move follows)

  Two sub-modes trade different market conditions:
    A) Mean Reversion: fade the move at H3 (short) or L3 (long)
    B) Breakout      : go with the move beyond H4 (long) or L4 (short)

Entry Rules — Mean Reversion (mode A):
  - Long  : bar low touches L3 (within 0.1%), bar closes above L3
             Target = PP,  Stop = L4 - small buffer
  - Short : bar high touches H3 (within 0.1%), bar closes below H3
             Target = PP,  Stop = H4 + small buffer

Entry Rules — Breakout (mode B):
  - Long  : close above H4 (confirmed breakout)
             Target = H4 + 2×(H4-H3),  Stop = H3 - buffer
  - Short : close below L4
             Target = L4 - 2×(H3-L3),  Stop = L3 + buffer

Exit Rules:
  - EOD hard exit at 14:45

Risk Controls:
  - Max 1 trade per day
  - Mean reversion only taken if PP is between L3 and H3 (validates level integrity)
  - Breakout only taken if OR already broke in same direction
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from utils import (load_data, daily_ohlc, calc_camarilla_pivots,
                   compute_metrics, plot_equity, print_metrics,
                   save_trades, save_metrics, check_exit, pnl_from_exit,
                   OUTPUT_DIR)

STRATEGY_NAME = "Strategy 3: Camarilla Pivot"
TOUCH_TOL     = 0.0012   # 0.12% tolerance to "touch" a level
SL_BUFFER_PCT = 0.001    # 0.1% buffer beyond L4/H4
EXIT_TIME     = pd.Timestamp("1900-01-01 14:45:00").time()
ENTRY_AFTER   = pd.Timestamp("1900-01-01 09:30:00").time()


def run(df: pd.DataFrame, daily: pd.DataFrame) -> tuple:
    cam_levels = {}
    days = daily["day"].tolist()
    for i in range(1, len(days)):
        prev = daily.iloc[i - 1]
        lvl  = calc_camarilla_pivots(prev["high"], prev["low"], prev["close"])
        cam_levels[days[i].date()] = lvl

    trades  = []
    equity  = [0.0]
    cum_pnl = 0.0

    for day, grp in df.groupby("day"):
        if day not in cam_levels:
            equity.append(cum_pnl)
            continue

        lvl = cam_levels[day]
        H4, H3, L3, L4, PP = lvl["H4"], lvl["H3"], lvl["L3"], lvl["L4"], lvl["PP"]

        # Sanity check: levels properly ordered
        if not (L4 < L3 < PP < H3 < H4):
            equity.extend([cum_pnl] * len(grp))
            continue

        position  = None
        entry_px  = None
        sl        = None
        tgt       = None
        traded    = False
        mode      = None

        # Determine opening bias from first bar vs PP
        bars = grp.sort_values("date")

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
                        "day": day, "side": position, "mode": mode,
                        "entry": round(entry_px, 2), "exit": round(exit_px, 2),
                        "pnl": round(pnl, 2), "reason": reason,
                        "H3": round(H3, 2), "H4": round(H4, 2),
                        "L3": round(L3, 2), "L4": round(L4, 2),
                        "PP": round(PP, 2),
                    })
                    position = None

            equity.append(cum_pnl)

            if not traded and position is None and t > ENTRY_AFTER and not is_eod:

                # ── Mean Reversion: Long near L3 ──
                if abs(l - L3) / L3 <= TOUCH_TOL and c > L3:
                    entry_px = c
                    sl  = L4 * (1 - SL_BUFFER_PCT)
                    tgt = PP
                    if tgt > entry_px:
                        position = "long"; traded = True; mode = "reversion"

                # ── Mean Reversion: Short near H3 ──
                elif abs(h - H3) / H3 <= TOUCH_TOL and c < H3:
                    entry_px = c
                    sl  = H4 * (1 + SL_BUFFER_PCT)
                    tgt = PP
                    if tgt < entry_px:
                        position = "short"; traded = True; mode = "reversion"

                # ── Breakout: Long above H4 ──
                elif c > H4:
                    entry_px = c
                    sl  = H3 * (1 - SL_BUFFER_PCT)
                    tgt = entry_px + 2 * (H4 - H3)
                    position = "long"; traded = True; mode = "breakout"

                # ── Breakout: Short below L4 ──
                elif c < L4:
                    entry_px = c
                    sl  = L3 * (1 + SL_BUFFER_PCT)
                    tgt = entry_px - 2 * (H3 - L3)
                    position = "short"; traded = True; mode = "breakout"

    equity_s = pd.Series(equity) + 10000
    return trades, equity_s


def main():
    print(f"Running {STRATEGY_NAME}...")
    df    = load_data()
    daily = daily_ohlc(df)

    trades, equity = run(df, daily)
    metrics, dd    = compute_metrics(equity, trades)

    print_metrics(STRATEGY_NAME, metrics)

    # Mode breakdown
    df_t = pd.DataFrame(trades)
    if not df_t.empty and "mode" in df_t.columns:
        for m in df_t["mode"].unique():
            sub = df_t[df_t["mode"] == m]
            wins = (sub["pnl"] > 0).sum()
            print(f"  [{m}]  trades={len(sub)}, wins={wins}, "
                  f"win%={wins/len(sub)*100:.1f}, total_pnl={sub['pnl'].sum():.1f}")

    plot_equity(equity, dd, STRATEGY_NAME, "s3_camarilla_equity.png")
    save_trades(trades,   "s3_camarilla_trades.csv")
    save_metrics(metrics, "s3_camarilla_metrics.csv")

    print(f"\n  Results saved to: {OUTPUT_DIR}/")
    return trades, equity, metrics


if __name__ == "__main__":
    main()
