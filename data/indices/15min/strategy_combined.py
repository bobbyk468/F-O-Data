"""
Strategy 5: Combined Pivot Strategy (Multi-Strategy Voting)
─────────────────────────────────────────────────────────────
Concept:
  Only trade when 2+ of the 4 individual strategies agree on direction
  for the same bar. Agreement = higher-probability setup, naturally
  filters out noise and reduces drawdown.

How it works:
  1. Each strategy generates a directional signal (+1 long, -1 short, 0 none)
     for every 15-min bar independently.
  2. Signals are combined per bar: vote_score = sum of all signals.
  3. Entry rules:
       vote_score >= +2  →  Long  (at least 2 strategies bullish)
       vote_score <= -2  →  Short (at least 2 strategies bearish)
  4. Exit:
       - Tightest SL among all agreeing strategies
       - Most conservative TGT (smallest for long, largest for short)
       - EOD exit at 14:45
  5. Max 1 trade per day.

Why this reduces drawdown:
  - Eliminates low-confidence setups that are often random noise.
  - Multiple independent frameworks confirming the same direction ≈
    strong institutional confluence.
  - Fewer trades but higher win rate.

Strategies in the vote:
  S1: Classic Pivot Breakout  (momentum)
  S2: Opening Range Breakout  (trend)
  S3: Camarilla Pivot         (structure)
  S4: Pivot Confluence        (multi-level S/R)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
from utils import (load_data, daily_ohlc, weekly_ohlc,
                   calc_classic_pivots, calc_camarilla_pivots, calc_woodie_pivots,
                   compute_metrics, plot_equity, print_metrics,
                   save_trades, save_metrics, check_exit, pnl_from_exit,
                   OUTPUT_DIR)

STRATEGY_NAME  = "Strategy 5: Combined (Multi-Strategy Vote)"
# S1 + S2 MUST both agree (core requirement — both are individually profitable).
# S3 (breakout mode only) and S4 serve as optional bonus confirmations
# that tighten the SL/TGT further when they fire in the same direction.
# MIN_VOTES of 2 means S1 + S2 must align; S3/S4 bonuses improve quality.
MIN_VOTES      = 2
EXIT_TIME      = pd.Timestamp("1900-01-01 14:45:00").time()
ENTRY_AFTER    = pd.Timestamp("1900-01-01 09:30:00").time()
NO_ENTRY_AFTER = pd.Timestamp("1900-01-01 14:15:00").time()

# ─── Per-bar signal generators (return signal dict or None) ──────────────────

def s1_signal(bar, prev_close, lvl, day_open):
    """Classic Pivot Breakout signal."""
    if prev_close is None:
        return None
    PP, R1, R2, S1, S2 = lvl["PP"], lvl["R1"], lvl["R2"], lvl["S1"], lvl["S2"]
    c = bar["close"]
    GAP_FILTER = 0.010
    if day_open > R1 * (1 + GAP_FILTER) or day_open < S1 * (1 - GAP_FILTER):
        return None
    if prev_close < R1 and c > R1 and c > PP:
        return {"dir": 1,  "sl": c * (1 - 0.004), "tgt": min(R2, c * (1 + 0.007))}
    if prev_close > S1 and c < S1 and c < PP:
        return {"dir": -1, "sl": c * (1 + 0.004), "tgt": max(S2, c * (1 - 0.007))}
    return None


def s2_signal(bar, or_high, or_low, or_range, pp, day_open):
    """Opening Range Breakout signal."""
    c = bar["close"]
    MIN_OR_PCT = 0.0015
    MAX_OR_PCT = 0.015
    mid_px = (or_high + or_low) / 2
    or_pct = or_range / mid_px
    if or_pct < MIN_OR_PCT or or_pct > MAX_OR_PCT:
        return None
    if c > or_high and or_high >= pp * 0.999:
        return {"dir": 1,  "sl": or_low * (1 - 0.002),  "tgt": c + or_range * 1.5}
    if c < or_low and or_low <= pp * 1.001:
        return {"dir": -1, "sl": or_high * (1 + 0.002), "tgt": c - or_range * 1.5}
    return None


def s3_signal(bar, lvl):
    """Camarilla Pivot — BREAKOUT MODE ONLY (mean reversion excluded due to bad R:R)."""
    H4, H3, L3, L4, PP = lvl["H4"], lvl["H3"], lvl["L3"], lvl["L4"], lvl["PP"]
    if not (L4 < L3 < PP < H3 < H4):
        return None
    c = bar["close"]
    SL_BUF = 0.001
    # Breakout long above H4
    if c > H4:
        return {"dir": 1,  "sl": H3 * (1 - SL_BUF), "tgt": c + 2 * (H4 - H3)}
    # Breakout short below L4
    if c < L4:
        return {"dir": -1, "sl": L3 * (1 + SL_BUF), "tgt": c - 2 * (H3 - L3)}
    return None


def s4_signal(bar, zones, day_open):
    """Pivot Confluence signal."""
    c, h, l, o = bar["close"], bar["high"], bar["low"], bar["open"]
    TOUCH_TOL = 0.002
    SL_PCT    = 0.0035
    TGT_PCT   = 0.008
    for zone in zones:
        if zone["score"] < 2:
            continue
        z_lo = zone["low"]  * (1 - TOUCH_TOL)
        z_hi = zone["high"] * (1 + TOUCH_TOL)
        if not (l <= z_hi and h >= z_lo):
            continue
        zone_px = zone["price"]
        # Support: bullish close
        if day_open > zone_px and c >= zone["low"] and c <= zone["high"] * 1.002 and c >= o:
            tgt = c * (1 + TGT_PCT)
            sl  = zone["low"] * (1 - SL_PCT)
            if tgt > c * 1.002:
                return {"dir": 1,  "sl": sl, "tgt": tgt}
        # Resistance: bearish close
        elif day_open < zone_px and c <= zone["high"] and c >= zone["low"] * 0.998 and c <= o:
            tgt = c * (1 - TGT_PCT)
            sl  = zone["high"] * (1 + SL_PCT)
            if tgt < c * 0.998:
                return {"dir": -1, "sl": sl, "tgt": tgt}
    return None


# ─── Confluence zone builder (from S4) ───────────────────────────────────────

def build_levels_and_zones(H, L, C, O_today, weekly_H, weekly_L, weekly_C,
                            zone_tol_pct=0.0025, min_score=2):
    levels = {}
    cl = calc_classic_pivots(H, L, C)
    for k, v in cl.items():
        levels[f"CL_{k}"] = v
    cam = calc_camarilla_pivots(H, L, C)
    for k, v in cam.items():
        levels[f"CAM_{k}"] = v
    wd = calc_woodie_pivots(H, L, C, O_today or C)
    for k, v in wd.items():
        levels[f"WD_{k}"] = v
    if weekly_H and weekly_L and weekly_C:
        wkly = calc_classic_pivots(weekly_H, weekly_L, weekly_C)
        for k, v in wkly.items():
            levels[f"WK_{k}"] = v

    items = sorted(levels.items(), key=lambda x: x[1])
    zones = []
    used  = set()
    for i, (lbl_i, px_i) in enumerate(items):
        if i in used:
            continue
        cluster_lbls   = [lbl_i]
        cluster_prices = [px_i]
        used.add(i)
        for j, (lbl_j, px_j) in enumerate(items):
            if j <= i or j in used:
                continue
            mid = np.mean(cluster_prices)
            if abs(px_j - mid) / mid <= zone_tol_pct:
                cluster_lbls.append(lbl_j)
                cluster_prices.append(px_j)
                used.add(j)
        if len(cluster_lbls) >= min_score:
            mid = np.mean(cluster_prices)
            zones.append({
                "price": mid, "score": len(cluster_lbls),
                "labels": cluster_lbls,
                "low":  min(cluster_prices),
                "high": max(cluster_prices),
            })
    return levels, sorted(zones, key=lambda z: z["price"])


# ─── Main backtest ────────────────────────────────────────────────────────────

def run(df: pd.DataFrame, daily: pd.DataFrame) -> tuple:
    # Precompute weekly pivots
    wk_df = weekly_ohlc(df)
    wk_pivots = {}
    for i in range(1, len(wk_df)):
        prev = wk_df.iloc[i - 1]
        week_start = wk_df.iloc[i]["week"].date()
        wk_pivots[week_start] = (prev["high"], prev["low"], prev["close"])

    def get_week_start(d):
        dt = pd.Timestamp(d)
        return (dt - pd.Timedelta(days=dt.weekday())).date()

    # Precompute day-level data
    day_list = daily["day"].tolist()
    day_data = {}
    for i in range(1, len(day_list)):
        prev  = daily.iloc[i - 1]
        today = daily.iloc[i]
        d_key = day_list[i].date()
        O_today = today["open"]
        wk_key  = get_week_start(d_key)
        wH, wL, wC = wk_pivots.get(wk_key, (None, None, None))

        lvls_cl  = calc_classic_pivots(prev["high"], prev["low"], prev["close"])
        lvls_cam = calc_camarilla_pivots(prev["high"], prev["low"], prev["close"])
        _, zones = build_levels_and_zones(
            prev["high"], prev["low"], prev["close"],
            O_today, wH, wL, wC
        )
        day_data[d_key] = {
            "cl":    lvls_cl,
            "cam":   lvls_cam,
            "zones": zones,
        }

    OR_END = pd.Timestamp("1900-01-01 09:30:00").time()

    trades  = []
    equity  = [0.0]
    cum_pnl = 0.0

    for day, grp in df.groupby("day"):
        if day not in day_data:
            equity.append(cum_pnl)
            continue

        dd      = day_data[day]
        bars    = grp.sort_values("date")
        day_open = bars.iloc[0]["open"]

        # Build OR
        or_bars  = bars[bars["date"].dt.time <= OR_END]
        or_high  = or_bars["high"].max() if len(or_bars) >= 2 else None
        or_low   = or_bars["low"].min()  if len(or_bars) >= 2 else None
        or_range = (or_high - or_low)    if or_high else None
        pp_cl    = dd["cl"]["PP"]

        position    = None
        entry_px    = None
        sl          = None
        tgt         = None
        traded      = False
        vote_detail = None
        prev_close  = None

        for _, bar in bars.iterrows():
            t = bar["date"].time()
            c, h, l = bar["close"], bar["high"], bar["low"]
            is_eod = t >= EXIT_TIME

            # Manage open position
            if position is not None:
                exit_px, reason = check_exit(position, entry_px, sl, tgt,
                                             h, l, c, is_eod)
                if exit_px is not None:
                    pnl = pnl_from_exit(position, entry_px, exit_px)
                    cum_pnl += pnl
                    trades.append({
                        "day":        day,
                        "side":       position,
                        "entry":      round(entry_px, 2),
                        "exit":       round(exit_px, 2),
                        "pnl":        round(pnl, 2),
                        "reason":     reason,
                        "votes":      vote_detail["votes"],
                        "vote_score": vote_detail["score"],
                        "strategies": vote_detail["strategies"],
                        "sl":         round(sl, 2),
                        "tgt":        round(tgt, 2),
                    })
                    position = None

            equity.append(cum_pnl)

            # Entry: collect votes from all strategies
            can_trade = (
                not is_eod
                and t > ENTRY_AFTER
                and t < NO_ENTRY_AFTER
                and position is None
                and not traded
            )

            if can_trade:
                # ── Core votes: S1 and S2 must BOTH agree ──────────────────
                sig1 = s1_signal(bar, prev_close, dd["cl"], day_open)
                sig2 = (s2_signal(bar, or_high, or_low, or_range, pp_cl, day_open)
                        if (or_high and or_range) else None)

                # No trade unless S1 and S2 both fire in the same direction
                core_dir = None
                if sig1 and sig2 and sig1["dir"] == sig2["dir"]:
                    core_dir = sig1["dir"]

                if core_dir is not None:
                    # ── Bonus votes: S3 (breakout only) and S4 ─────────────
                    sig3 = s3_signal(bar, dd["cam"])
                    sig4 = s4_signal(bar, dd["zones"], day_open)

                    # Collect all agreeing signals
                    agreed = {"S1_Pivot": sig1, "S2_ORB": sig2}
                    if sig3 and sig3["dir"] == core_dir:
                        agreed["S3_Camarilla_BO"] = sig3
                    if sig4 and sig4["dir"] == core_dir:
                        agreed["S4_Confluence"] = sig4

                    direction = "long" if core_dir == 1 else "short"
                    entry_px  = c

                    # Use S1+S2 SL/TGT as base; bonus signals tighten further
                    if direction == "long":
                        sl  = max(v["sl"] for v in agreed.values())   # highest SL floor
                        tgt = min(v["tgt"] for v in agreed.values())  # nearest target
                    else:
                        sl  = min(v["sl"] for v in agreed.values())   # lowest SL ceiling
                        tgt = max(v["tgt"] for v in agreed.values())  # nearest target

                    valid = ((direction == "long"  and tgt > entry_px) or
                             (direction == "short" and tgt < entry_px))

                    if valid and abs(tgt - entry_px) / entry_px > 0.001:
                        position    = direction
                        traded      = True
                        vote_detail = {
                            "votes":      len(agreed),
                            "score":      core_dir * len(agreed),
                            "strategies": "|".join(agreed.keys()),
                        }

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

    # Vote distribution
    df_t = pd.DataFrame(trades)
    if not df_t.empty:
        print("\n  Vote Count Distribution:")
        vc = df_t.groupby("votes").agg(
            trades   = ("pnl","count"),
            total_pnl= ("pnl","sum"),
            win_rate = ("pnl", lambda x: f"{(x>0).mean()*100:.1f}%")
        )
        print(vc.to_string())

        print("\n  Strategy Combination Frequency (top 10):")
        print(df_t["strategies"].value_counts().head(10).to_string())

    plot_equity(equity, dd, STRATEGY_NAME, "s5_combined_equity.png")
    save_trades(trades,   "s5_combined_trades.csv")
    save_metrics(metrics, "s5_combined_metrics.csv")

    print(f"\n  Results saved to: {OUTPUT_DIR}/")
    return trades, equity, metrics


if __name__ == "__main__":
    main()
