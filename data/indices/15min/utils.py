"""
Shared utilities for Nifty50 Pivot Strategy Backtesting
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

DATA_PATH  = Path(__file__).parent / "nifty_50_15min.csv"
OUTPUT_DIR = Path(__file__).parent / "backtest_results"
OUTPUT_DIR.mkdir(exist_ok=True)


# ─── Data Loading ─────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    df = pd.read_csv(DATA_PATH, parse_dates=["date"])
    df["date"] = pd.to_datetime(df["date"], utc=False).dt.tz_localize(None)
    df = df.sort_values("date").reset_index(drop=True)
    df["time"] = df["date"].dt.time
    df["day"]  = df["date"].dt.date
    return df


def daily_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 15-min bars to daily OHLC for pivot calculations."""
    daily = (
        df.groupby("day")
        .agg(open=("open","first"), high=("high","max"),
             low=("low","min"),   close=("close","last"))
        .reset_index()
    )
    daily["day"] = pd.to_datetime(daily["day"])
    return daily


def weekly_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 15-min bars to weekly OHLC for weekly pivot calculations."""
    tmp = df.copy()
    tmp["week"] = pd.to_datetime(tmp["day"]) - pd.to_timedelta(
        pd.to_datetime(tmp["day"]).dt.weekday, unit="D"
    )
    weekly = (
        tmp.groupby("week")
        .agg(high=("high","max"), low=("low","min"), close=("close","last"))
        .reset_index()
    )
    return weekly


# ─── Pivot Level Calculators ──────────────────────────────────────────────────

def calc_classic_pivots(H, L, C) -> dict:
    PP = (H + L + C) / 3
    return {
        "PP": PP,
        "R1": 2*PP - L,  "R2": PP + (H - L),  "R3": H + 2*(PP - L),
        "S1": 2*PP - H,  "S2": PP - (H - L),  "S3": L - 2*(H - PP),
    }


def calc_camarilla_pivots(H, L, C) -> dict:
    rng = H - L
    return {
        "H4": C + rng * 1.1 / 2,
        "H3": C + rng * 1.1 / 4,
        "L3": C - rng * 1.1 / 4,
        "L4": C - rng * 1.1 / 2,
        "PP": (H + L + C) / 3,
    }


def calc_woodie_pivots(H, L, C, O_today) -> dict:
    PP = (H + L + 2*C) / 4
    return {
        "PP": PP,
        "R1": 2*PP - L,  "R2": PP + (H - L),
        "S1": 2*PP - H,  "S2": PP - (H - L),
    }


# ─── Metrics ──────────────────────────────────────────────────────────────────

def compute_metrics(equity: pd.Series, trades: list) -> tuple:
    total_return = (equity.iloc[-1] / equity.iloc[0] - 1) * 100
    running_max  = equity.cummax()
    drawdown     = (equity - running_max) / running_max * 100
    max_dd       = drawdown.min()

    rets   = equity.pct_change().dropna()
    sharpe = (rets.mean() / rets.std() * np.sqrt(252 * 25)) if rets.std() > 0 else 0.0

    wins   = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    total  = len(trades)

    win_rate = len(wins) / total * 100 if total > 0 else 0.0
    avg_win  = np.mean([t["pnl"] for t in wins])   if wins   else 0.0
    avg_loss = np.mean([t["pnl"] for t in losses]) if losses else 0.0
    rr_ratio = abs(avg_win / avg_loss)              if avg_loss != 0 else 0.0

    metrics = {
        "Total Return (%)": round(total_return, 2),
        "Max Drawdown (%)": round(max_dd, 2),
        "Sharpe Ratio":     round(sharpe, 2),
        "Total Trades":     total,
        "Win Rate (%)":     round(win_rate, 2),
        "Avg Win (pts)":    round(avg_win, 2),
        "Avg Loss (pts)":   round(avg_loss, 2),
        "Risk/Reward":      round(rr_ratio, 2),
    }
    return metrics, drawdown


# ─── Plotting ─────────────────────────────────────────────────────────────────

def plot_equity(equity: pd.Series, drawdown: pd.Series, title: str, filename: str):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8),
                                   gridspec_kw={"height_ratios": [3, 1]})
    ax1.plot(equity.values, color="steelblue", linewidth=1.2)
    ax1.set_title(title, fontsize=13, fontweight="bold")
    ax1.set_ylabel("Cumulative P&L (pts)")
    ax1.grid(alpha=0.3)

    ax2.fill_between(range(len(drawdown)), drawdown.values, 0,
                     color="crimson", alpha=0.5)
    ax2.axhline(-10, color="black", linestyle="--", linewidth=0.9, label="-10% limit")
    ax2.set_ylabel("Drawdown (%)")
    ax2.set_xlabel("Bar #")
    ax2.legend(fontsize=9)
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    out = OUTPUT_DIR / filename
    plt.savefig(out, dpi=120)
    plt.close()
    return out


# ─── Position Management Helpers ─────────────────────────────────────────────

def check_exit(position, entry_px, sl, tgt, bar_high, bar_low, bar_close,
               is_eod=False):
    """
    Returns (exit_price, reason) or (None, None) if no exit yet.
    Checks SL, TGT (intrabar), then EOD.
    """
    if position == "long":
        if bar_low <= sl:
            return sl, "SL"
        if bar_high >= tgt:
            return tgt, "TGT"
        if is_eod:
            return bar_close, "EOD"
    elif position == "short":
        if bar_high >= sl:
            return sl, "SL"
        if bar_low <= tgt:
            return tgt, "TGT"
        if is_eod:
            return bar_close, "EOD"
    return None, None


def pnl_from_exit(position, entry_px, exit_px):
    if position == "long":
        return exit_px - entry_px
    return entry_px - exit_px


# ─── Trade Log ────────────────────────────────────────────────────────────────

def save_trades(trades: list, filename: str):
    path = OUTPUT_DIR / filename
    pd.DataFrame(trades).to_csv(path, index=False)
    return path


def save_metrics(metrics: dict, filename: str):
    path = OUTPUT_DIR / filename
    pd.DataFrame([metrics]).to_csv(path, index=False)
    return path


# ─── Print helper ─────────────────────────────────────────────────────────────

def print_metrics(name: str, metrics: dict):
    print(f"\n{'═'*52}")
    print(f"  {name}")
    print(f"{'═'*52}")
    for k, v in metrics.items():
        flag = "  ⚠️  EXCEEDS 10% LIMIT" if k == "Max Drawdown (%)" and v < -10 else ""
        print(f"  {k:<25} {v}{flag}")
