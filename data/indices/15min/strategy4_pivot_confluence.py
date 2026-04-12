"""
Strategy 4: Pivot Confluence Zone Trading
──────────────────────────────────────────
Concept (Pivot Boss — core thesis):
  "When multiple pivot levels from different calculation methods cluster
   within a tight zone, that area represents an extremely high-probability
   support or resistance point."

  A confluence zone is formed when 2+ of the following levels are
  within 0.25% of each other:
    • Classic Pivot  : PP, R1, R2, S1, S2
    • Camarilla      : H3, H4, L3, L4
    • Woodie's Pivot : PP, R1, R2, S1, S2
    • Weekly Classic : PP (adds higher-timeframe weight)

  The more levels that cluster → the stronger the zone.

Entry Rules:
  - Identify confluence zones above and below price at session open
  - Long  : price pulls back to a SUPPORT confluence zone and shows
             a reversal bar (close > open on the 15-min bar touching zone)
  - Short : price rallies into a RESISTANCE confluence zone and shows
             a rejection bar (close < open on the 15-min bar touching zone)
  - Min 2 levels must converge within 0.25% band
  - Confluence "strength" score = number of converging levels (2–6+)
    Only trade zones with score ≥ 2

Exit Rules:
  - Target : next confluence zone in trade direction
             (if none found, use 0.8% from entry)
  - Stop   : 0.35% beyond the zone boundary
  - EOD    : hard exit at 14:45

Risk Controls:
  - Max 2 trades per day (one long, one short if setups arise)
  - No entry in final 45 minutes
  - Score ≥ 3 required for second trade
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

STRATEGY_NAME   = "Strategy 4: Pivot Confluence"
ZONE_TOL_PCT    = 0.0025   # 0.25% band for confluence
MIN_SCORE       = 2        # min levels in zone to trade
SL_PCT          = 0.0035   # 0.35% stop
TGT_PCT         = 0.008    # 0.8% target (if no next zone found)
MAX_TRADES_DAY  = 2
EXIT_TIME       = pd.Timestamp("1900-01-01 14:45:00").time()
ENTRY_AFTER     = pd.Timestamp("1900-01-01 09:30:00").time()
NO_ENTRY_AFTER  = pd.Timestamp("1900-01-01 14:15:00").time()
TOUCH_TOL_PCT   = 0.002    # 0.2% tolerance to "touch" a zone


def build_all_levels(H, L, C, O_today=None, weekly_H=None, weekly_L=None, weekly_C=None):
    """Return dict of {label: price} for all pivot types."""
    levels = {}

    # Classic
    cl = calc_classic_pivots(H, L, C)
    for k, v in cl.items():
        levels[f"CL_{k}"] = v

    # Camarilla
    cam = calc_camarilla_pivots(H, L, C)
    for k, v in cam.items():
        levels[f"CAM_{k}"] = v

    # Woodie's (use today's open if available, else yesterday's close)
    open_px = O_today if O_today else C
    wd = calc_woodie_pivots(H, L, C, open_px)
    for k, v in wd.items():
        levels[f"WD_{k}"] = v

    # Weekly (if available) — adds higher-TF weight
    if weekly_H and weekly_L and weekly_C:
        wkly = calc_classic_pivots(weekly_H, weekly_L, weekly_C)
        for k, v in wkly.items():
            levels[f"WK_{k}"] = v

    return levels


def find_confluence_zones(levels: dict, zone_tol_pct: float) -> list:
    """
    Cluster nearby levels into confluence zones.
    Returns list of zones sorted by price:
      [{"price": mid, "score": n, "labels": [...], "low": ..., "high": ...}, ...]
    """
    items = sorted(levels.items(), key=lambda x: x[1])
    zones = []
    used  = set()

    for i, (lbl_i, px_i) in enumerate(items):
        if i in used:
            continue
        cluster_lbls  = [lbl_i]
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

        if len(cluster_lbls) >= MIN_SCORE:
            mid = np.mean(cluster_prices)
            zones.append({
                "price":  mid,
                "score":  len(cluster_lbls),
                "labels": cluster_lbls,
                "low":    min(cluster_prices),
                "high":   max(cluster_prices),
            })

    return sorted(zones, key=lambda z: z["price"])


def price_touches_zone(bar_low, bar_high, zone) -> bool:
    """Check if bar's range overlaps with the zone band."""
    z_lo = zone["low"]  * (1 - TOUCH_TOL_PCT)
    z_hi = zone["high"] * (1 + TOUCH_TOL_PCT)
    return bar_low <= z_hi and bar_high >= z_lo


def next_zone_above(zones, price):
    for z in zones:
        if z["price"] > price * 1.001:
            return z
    return None


def next_zone_below(zones, price):
    for z in reversed(zones):
        if z["price"] < price * 0.999:
            return z
    return None


def run(df: pd.DataFrame, daily: pd.DataFrame) -> tuple:
    # Weekly pivots lookup
    wk_df    = weekly_ohlc(df)
    wk_pivots = {}
    for i in range(1, len(wk_df)):
        prev = wk_df.iloc[i - 1]
        week_start = wk_df.iloc[i]["week"].date()
        wk_pivots[week_start] = (prev["high"], prev["low"], prev["close"])

    def get_week_start(d):
        dt = pd.Timestamp(d)
        return (dt - pd.Timedelta(days=dt.weekday())).date()

    # Daily level lookup
    day_list = daily["day"].tolist()
    day_levels = {}
    for i in range(1, len(day_list)):
        prev   = daily.iloc[i - 1]
        today  = daily.iloc[i]
        d_key  = day_list[i].date()
        O_today = today["open"]

        wk_key = get_week_start(d_key)
        wH, wL, wC = wk_pivots.get(wk_key, (None, None, None))

        lvls = build_all_levels(
            prev["high"], prev["low"], prev["close"],
            O_today, wH, wL, wC
        )
        day_levels[d_key] = lvls

    trades  = []
    equity  = [0.0]
    cum_pnl = 0.0

    for day, grp in df.groupby("day"):
        if day not in day_levels:
            equity.append(cum_pnl)
            continue

        lvls  = day_levels[day]
        zones = find_confluence_zones(lvls, ZONE_TOL_PCT)

        if not zones:
            equity.extend([cum_pnl] * len(grp))
            continue

        bars       = grp.sort_values("date")
        day_open   = bars.iloc[0]["open"]

        position   = None
        entry_px   = None
        sl         = None
        tgt        = None
        trades_today = 0
        active_zone  = None

        for _, bar in bars.iterrows():
            t = bar["date"].time()
            c, h, l, o = bar["close"], bar["high"], bar["low"], bar["open"]
            is_eod = t >= EXIT_TIME

            if position is not None:
                exit_px, reason = check_exit(position, entry_px, sl, tgt,
                                             h, l, c, is_eod)
                if exit_px is not None:
                    pnl = pnl_from_exit(position, entry_px, exit_px)
                    cum_pnl += pnl
                    trades.append({
                        "day":   day, "side": position,
                        "entry": round(entry_px, 2), "exit": round(exit_px, 2),
                        "pnl":   round(pnl, 2), "reason": reason,
                        "zone_price": round(active_zone["price"], 2),
                        "zone_score": active_zone["score"],
                        "zone_labels": "|".join(active_zone["labels"]),
                        "sl": round(sl, 2), "tgt": round(tgt, 2),
                    })
                    position = None

            equity.append(cum_pnl)

            # Entry logic
            can_trade = (
                not is_eod
                and t > ENTRY_AFTER
                and t < NO_ENTRY_AFTER
                and position is None
                and trades_today < MAX_TRADES_DAY
            )
            # Second trade requires stronger zone
            min_score_needed = MIN_SCORE if trades_today == 0 else 3

            if can_trade:
                # Check each zone for a touch
                for zone in zones:
                    if zone["score"] < min_score_needed:
                        continue
                    if not price_touches_zone(l, h, zone):
                        continue

                    zone_px = zone["price"]

                    # Support zone (price is above zone → potential long)
                    if day_open > zone_px and c >= zone["low"] and o > c:
                        # Reversal bar: bearish bar touching support = buy dip
                        # Actually we want bullish close off zone
                        pass

                    if day_open > zone_px and c > zone["low"] and c < zone["high"] * 1.002:
                        # Bar touching zone from above, bullish close (close > open)
                        if c >= o:
                            entry_px = c
                            sl  = zone["low"] * (1 - SL_PCT)
                            nxt = next_zone_above(zones, entry_px)
                            tgt = nxt["price"] if nxt else entry_px * (1 + TGT_PCT)
                            if tgt > entry_px * 1.002:
                                position     = "long"
                                active_zone  = zone
                                trades_today += 1
                                break

                    # Resistance zone (price is below zone → potential short)
                    elif day_open < zone_px and c <= zone["high"] and c >= zone["low"] * 0.998:
                        # Bar touching zone from below, bearish close (close < open)
                        if c <= o:
                            entry_px = c
                            sl  = zone["high"] * (1 + SL_PCT)
                            nxt = next_zone_below(zones, entry_px)
                            tgt = nxt["price"] if nxt else entry_px * (1 - TGT_PCT)
                            if tgt < entry_px * 0.998:
                                position     = "short"
                                active_zone  = zone
                                trades_today += 1
                                break

    equity_s = pd.Series(equity) + 10000
    return trades, equity_s


def main():
    print(f"Running {STRATEGY_NAME}...")
    df    = load_data()
    daily = daily_ohlc(df)

    trades, equity = run(df, daily)
    metrics, dd    = compute_metrics(equity, trades)

    print_metrics(STRATEGY_NAME, metrics)

    # Zone score distribution
    df_t = pd.DataFrame(trades)
    if not df_t.empty and "zone_score" in df_t.columns:
        print("\n  Zone Score Distribution:")
        sc = df_t.groupby("zone_score").agg(
            trades=("pnl","count"),
            total_pnl=("pnl","sum"),
            win_rate=("pnl", lambda x: (x > 0).mean() * 100)
        ).round(2)
        print(sc.to_string())

    plot_equity(equity, dd, STRATEGY_NAME, "s4_confluence_equity.png")
    save_trades(trades,   "s4_confluence_trades.csv")
    save_metrics(metrics, "s4_confluence_metrics.csv")

    print(f"\n  Results saved to: {OUTPUT_DIR}/")
    return trades, equity, metrics


if __name__ == "__main__":
    main()
