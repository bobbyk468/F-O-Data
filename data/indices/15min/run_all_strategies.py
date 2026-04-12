"""
Run All Pivot Strategies & Generate Comparison Report
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from utils import load_data, daily_ohlc, compute_metrics, OUTPUT_DIR

import strategy1_pivot_breakout   as s1
import strategy2_orb              as s2
import strategy3_camarilla        as s3
import strategy4_pivot_confluence  as s4
import strategy_combined           as s5


def plot_combined(results: dict):
    n = len(results)
    cols = 3
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(20, 5 * rows))
    axes = axes.flatten()

    for ax, (name, (eq, dd)) in zip(axes, results.items()):
        color = "green" if dd.min() > -10 else "crimson"
        ax.plot(eq.values - 10000, color="steelblue", linewidth=1.0, label="P&L")
        ax2 = ax.twinx()
        ax2.fill_between(range(len(dd)), dd.values, 0, color=color, alpha=0.25)
        ax2.axhline(-10, color="black", linestyle="--", linewidth=0.7)
        ax2.set_ylabel("DD %", fontsize=8, color="crimson")
        ax2.set_ylim(-30, 5)
        ax.set_title(name, fontsize=9, fontweight="bold")
        ax.set_ylabel("Cum P&L (pts)", fontsize=8)
        ax.grid(alpha=0.2)

    for ax in axes[n:]:   # hide unused subplots
        ax.set_visible(False)
    plt.suptitle("Nifty50 Pivot Strategies — Backtest Comparison\n(2015–2026, 15-min bars)",
                 fontsize=12, fontweight="bold")
    plt.tight_layout()
    out = OUTPUT_DIR / "all_strategies_comparison.png"
    plt.savefig(out, dpi=130)
    plt.close()
    print(f"\n  Combined chart → {out}")


def print_comparison(all_metrics: dict):
    rows = []
    for name, m in all_metrics.items():
        rows.append({
            "Strategy":        name,
            "Total Return (%)": m["Total Return (%)"],
            "Max Drawdown (%)": m["Max Drawdown (%)"],
            "Sharpe":          m["Sharpe Ratio"],
            "Trades":          m["Total Trades"],
            "Win Rate (%)":    m["Win Rate (%)"],
            "Risk/Reward":     m["Risk/Reward"],
            "DD < 10%":        "PASS" if m["Max Drawdown (%)"] > -10 else "FAIL",
        })
    df = pd.DataFrame(rows).set_index("Strategy")
    print("\n")
    print("═" * 90)
    print("  STRATEGY COMPARISON SUMMARY")
    print("═" * 90)
    print(df.to_string())
    print("═" * 90)
    return df


def main():
    print("=" * 60)
    print("  Nifty50 Pivot Strategies Backtest")
    print("  Data: 2015-09-01 → 2026-02-20  |  15-min bars")
    print("=" * 60)

    df    = load_data()
    daily = daily_ohlc(df)
    print(f"  Loaded {len(df):,} bars across {df['day'].nunique():,} trading days\n")

    # ── Run all strategies ────────────────────────────────────────────
    print("─" * 60)
    t1, eq1 = s1.run(df, daily)
    m1, dd1 = compute_metrics(eq1, t1)
    print(f"  S1 Pivot Breakout     : {m1['Total Trades']} trades, "
          f"DD={m1['Max Drawdown (%)']}%, Return={m1['Total Return (%)']}%")

    t2, eq2 = s2.run(df, daily)
    m2, dd2 = compute_metrics(eq2, t2)
    print(f"  S2 ORB                : {m2['Total Trades']} trades, "
          f"DD={m2['Max Drawdown (%)']}%, Return={m2['Total Return (%)']}%")

    t3, eq3 = s3.run(df, daily)
    m3, dd3 = compute_metrics(eq3, t3)
    print(f"  S3 Camarilla          : {m3['Total Trades']} trades, "
          f"DD={m3['Max Drawdown (%)']}%, Return={m3['Total Return (%)']}%")

    t4, eq4 = s4.run(df, daily)
    m4, dd4 = compute_metrics(eq4, t4)
    print(f"  S4 Pivot Confluence   : {m4['Total Trades']} trades, "
          f"DD={m4['Max Drawdown (%)']}%, Return={m4['Total Return (%)']}%")

    t5, eq5 = s5.run(df, daily)
    m5, dd5 = compute_metrics(eq5, t5)
    print(f"  S5 Combined (Vote)    : {m5['Total Trades']} trades, "
          f"DD={m5['Max Drawdown (%)']}%, Return={m5['Total Return (%)']}%")

    # ── Comparison table ─────────────────────────────────────────────
    all_metrics = {
        "S1: Pivot Breakout":    m1,
        "S2: ORB (30-min)":      m2,
        "S3: Camarilla":         m3,
        "S4: Confluence":        m4,
        "S5: Combined (Vote)":   m5,
    }
    cmp_df = print_comparison(all_metrics)

    # ── Save comparison ───────────────────────────────────────────────
    cmp_df.to_csv(OUTPUT_DIR / "all_strategies_comparison.csv")

    # ── Combined equity chart ────────────────────────────────────────
    plot_combined({
        "S1: Pivot Breakout":   (eq1, dd1),
        "S2: ORB (30-min)":     (eq2, dd2),
        "S3: Camarilla":        (eq3, dd3),
        "S4: Confluence":       (eq4, dd4),
        "S5: Combined (Vote)":  (eq5, dd5),
    })

    # ── Drawdown compliance ───────────────────────────────────────────
    print("\n  DRAWDOWN COMPLIANCE (target: Max DD > -10%)")
    print("  " + "─" * 45)
    for name, m in all_metrics.items():
        dd_val = m["Max Drawdown (%)"]
        status = "✓ PASS" if dd_val > -10 else "✗ FAIL"
        print(f"  {name:<30} {dd_val:>8}%   [{status}]")

    print(f"\n  All results saved to: {OUTPUT_DIR}/\n")


if __name__ == "__main__":
    main()
