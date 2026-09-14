"""
Bollinger Band Backtest — TCS 15-minute data
Runs Method I (Squeeze), Method II (Trend), Method III (Reversal)

15-min parameter choices
  BB period 20  ≈ 1 trading day (25 bars/day)
  MFI period 10 ≈ half BB period (book rule)
  II  period 21
  Squeeze lookback 500 bars ≈ 1 month (book says 6 months daily → scale for intraday)
"""

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

DATA_PATH = (
    "/Users/brahmajikatragadda/Desktop/Zerodha_Data/"
    "jugaad-trader/data/nifty50/15min/tcs_15min.csv"
)
OUT_DIR = "/Users/brahmajikatragadda/Desktop/Zerodha_Data/jugaad-trader/Bollinger/"

# ── Parameters ─────────────────────────────────────────────────────────────────
BB_PERIOD    = 20
BB_STD       = 2.0
MFI_PERIOD   = 10
II_PERIOD    = 21
SQUEEZE_LB   = 500        # bars (~1 month of 15-min bars)
CAPITAL      = 100_000    # INR capital per trade
SL_PCT       = 0.008      # 0.8% stop-loss
TP_PCT       = 0.016      # 1.6% take-profit  (2:1 R:R)
SLIP_PCT     = 0.0005     # 0.05% slippage each side
COMM_PCT     = 0.0003     # 0.03% commission each side
COST_PCT     = (SLIP_PCT + COMM_PCT) * 2   # round-trip cost fraction

# ── Load ───────────────────────────────────────────────────────────────────────
print("Loading data …")
df = pd.read_csv(DATA_PATH, parse_dates=["date"])
df = df.sort_values("date").reset_index(drop=True)
df["date"] = pd.to_datetime(df["date"], utc=False).dt.tz_localize(None)
df["session_date"] = df["date"].dt.date
df["bar_time"]     = df["date"].dt.strftime("%H:%M")

# ── Indicators ─────────────────────────────────────────────────────────────────
print("Computing indicators …")
c, h, l, o, v = df["close"], df["high"], df["low"], df["open"], df["volume"]

mb  = c.rolling(BB_PERIOD).mean()
std = c.rolling(BB_PERIOD).std(ddof=1)
ub  = mb + BB_STD * std
lb  = mb - BB_STD * std
pct_b     = (c - lb) / (ub - lb)
bandwidth = (ub - lb) / mb

bw_min  = bandwidth.rolling(SQUEEZE_LB).min()
squeeze = bandwidth <= bw_min

# Intraday Intensity %  (closed oscillator form)
ii_raw = (2*c - h - l) / (h - l).replace(0, np.nan) * v
ii_pct = ii_raw.rolling(II_PERIOD).sum() / v.rolling(II_PERIOD).sum()

# AD%
ad_raw = (c - o) / (h - l).replace(0, np.nan) * v
ad_pct = ad_raw.rolling(II_PERIOD).sum() / v.rolling(II_PERIOD).sum()

# MFI
tp     = (h + l + c) / 3
mf     = tp * v
pos_mf = mf.where(tp > tp.shift(1), 0.0).rolling(MFI_PERIOD).sum()
neg_mf = mf.where(tp < tp.shift(1), 0.0).rolling(MFI_PERIOD).sum()
mfi    = 100 - 100 / (1 + pos_mf / neg_mf.replace(0, np.nan))

df = df.assign(
    MB=mb, UB=ub, LB=lb, pct_b=pct_b, BandWidth=bandwidth,
    squeeze=squeeze, II_pct=ii_pct, AD_pct=ad_pct, MFI=mfi,
)
df = df.dropna(subset=["MB","MFI","II_pct","AD_pct"]).reset_index(drop=True)
print(f"  {len(df):,} bars  ({df['date'].iloc[0]} → {df['date'].iloc[-1]})")


# ── Backtest engine ─────────────────────────────────────────────────────────────
def run_backtest(signals: pd.Series, df: pd.DataFrame,
                 sl_pct=SL_PCT, tp_pct=TP_PCT,
                 intraday=True, label="") -> pd.DataFrame:
    """
    Next-bar-open execution. Exits via SL, TP, or EOD (15:15 bar).
    PnL is in % of capital deployed (CAPITAL constant).

    Key fix: cost is a pure fraction (not multiplied by price).
    """
    trades = []
    pos    = 0        # 0 flat | +1 long | -1 short
    epx    = 0.0
    edt    = None

    for i in range(1, len(df)):
        row = df.iloc[i]

        # ── Check exit conditions first ──
        if pos != 0:
            sl_hit = tp_hit = eod = False
            if pos == 1:
                sl_px  = epx * (1 - sl_pct)
                tp_px  = epx * (1 + tp_pct)
                sl_hit = row["low"]  <= sl_px
                tp_hit = row["high"] >= tp_px
            else:
                sl_px  = epx * (1 + sl_pct)
                tp_px  = epx * (1 - tp_pct)
                sl_hit = row["high"] >= sl_px
                tp_hit = row["low"]  <= tp_px

            eod = intraday and row["bar_time"] == "15:15"

            if sl_hit or tp_hit or eod:
                if sl_hit:
                    xpx    = sl_px
                    reason = "SL"
                elif tp_hit:
                    xpx    = tp_px
                    reason = "TP"
                else:
                    xpx    = row["close"]
                    reason = "EOD"

                # ← pnl_pct is purely a fraction; cost is also a fraction
                raw_pct = pos * (xpx - epx) / epx
                pnl_pct = raw_pct - COST_PCT          # round-trip cost deducted
                pnl_abs = pnl_pct * CAPITAL
                shares  = int(CAPITAL / epx)

                trades.append({
                    "entry_dt":  edt,
                    "exit_dt":   row["date"],
                    "direction": "Long" if pos == 1 else "Short",
                    "entry_px":  round(epx, 2),
                    "exit_px":   round(xpx, 2),
                    "shares":    shares,
                    "reason":    reason,
                    "pnl_pct":   round(pnl_pct * 100, 3),
                    "pnl_abs":   round(pnl_abs, 2),
                })
                pos = 0

        # ── Check entry: use signal from previous bar, enter at this bar's open ──
        if pos == 0:
            sig = signals.iloc[i - 1]
            if sig != 0:
                pos  = int(sig)
                epx  = row["open"] * (1 + SLIP_PCT * pos)
                edt  = row["date"]

    return pd.DataFrame(trades)


# ── Build signals ──────────────────────────────────────────────────────────────

# ── Method I — Squeeze Breakout ────────────────────────────────────────────────
m1_sig    = pd.Series(0, index=df.index)
in_sq_end = False
for i in range(1, len(df)):
    if df["squeeze"].iloc[i-1] and not df["squeeze"].iloc[i]:
        in_sq_end = True
    if in_sq_end and not df["squeeze"].iloc[i]:
        cl = df["close"].iloc[i]
        if cl > df["UB"].iloc[i]:
            m1_sig.iloc[i] = 1;  in_sq_end = False
        elif cl < df["LB"].iloc[i]:
            m1_sig.iloc[i] = -1; in_sq_end = False

# ── Method II — Trend Following (%b + MFI) ────────────────────────────────────
m2_sig = pd.Series(0, index=df.index)
m2_sig[(df["pct_b"] > 0.80) & (df["MFI"] > 80)] =  1
m2_sig[(df["pct_b"] < 0.20) & (df["MFI"] < 20)] = -1

# ── Method III — Reversals (%b + II%) ─────────────────────────────────────────
m3_sig = pd.Series(0, index=df.index)
m3_sig[(df["pct_b"] < 0.05) & (df["II_pct"] > 0)] =  1
m3_sig[(df["pct_b"] > 0.95) & (df["II_pct"] < 0)] = -1

print("\nRunning backtests …")
trades_m1 = run_backtest(m1_sig, df, label="Method I")
trades_m2 = run_backtest(m2_sig, df, label="Method II")
trades_m3 = run_backtest(m3_sig, df, label="Method III")


# ── Summary printer ─────────────────────────────────────────────────────────────
def summarize(trades: pd.DataFrame, label: str):
    sep = "═" * 62
    if trades.empty:
        print(f"\n{sep}\n  {label}: No trades.\n{sep}"); return

    n    = len(trades)
    wins = trades[trades["pnl_abs"] > 0]
    loss = trades[trades["pnl_abs"] <= 0]
    wr   = len(wins) / n * 100

    avg_w  = wins["pnl_abs"].mean() if len(wins) else 0
    avg_l  = loss["pnl_abs"].mean() if len(loss) else 0
    rr     = abs(avg_w / avg_l)     if avg_l else float("inf")

    gross  = trades["pnl_abs"].sum()
    eq     = trades["pnl_abs"].cumsum()
    max_dd = (eq - eq.cummax()).min()

    avg_bars = 0
    if "entry_dt" in trades and "exit_dt" in trades:
        dur = (pd.to_datetime(trades["exit_dt"]) - pd.to_datetime(trades["entry_dt"]))
        avg_bars = dur.mean().total_seconds() / 900   # 900s = 15min

    longs  = (trades["direction"] == "Long").sum()
    shorts = (trades["direction"] == "Short").sum()
    tp_n   = (trades["reason"] == "TP").sum()
    sl_n   = (trades["reason"] == "SL").sum()
    eod_n  = (trades["reason"] == "EOD").sum()

    monthly = trades.copy()
    monthly["month"] = pd.to_datetime(monthly["entry_dt"]).dt.to_period("M")
    mon_pnl = monthly.groupby("month")["pnl_abs"].sum()
    pos_months = (mon_pnl > 0).sum()
    tot_months = len(mon_pnl)

    print(f"""
{sep}
  {label}
{"─"*62}
  Trades        : {n:>6,}    Long:{longs}  Short:{shorts}
  Win Rate      : {wr:>6.1f}%
  Avg Win       : ₹{avg_w:>9,.0f}
  Avg Loss      : ₹{avg_l:>9,.0f}
  Risk-Reward   : {rr:>6.2f}x
  Gross PnL     : ₹{gross:>12,.0f}
  Max Drawdown  : ₹{max_dd:>12,.0f}
  Profitable months: {pos_months}/{tot_months}
  Avg hold (bars):  {avg_bars:.1f}
  Exits → TP:{tp_n}  SL:{sl_n}  EOD:{eod_n}
{sep}""")

    # Year-by-year breakdown
    yearly = monthly.copy()
    yearly["year"] = pd.to_datetime(yearly["entry_dt"]).dt.year
    yr_grp = yearly.groupby("year")["pnl_abs"].agg(["sum","count"])
    yr_grp.columns = ["PnL","Trades"]
    yr_grp["WinRate%"] = (
        yearly[yearly["pnl_abs"] > 0].groupby(
            yearly[yearly["pnl_abs"] > 0]["year"])["pnl_abs"].count()
        / yr_grp["Trades"] * 100
    ).round(1)
    print("  Year-by-year:")
    print(yr_grp.to_string())
    print()


summarize(trades_m1, "METHOD I  — Volatility Breakout (Squeeze)")
summarize(trades_m2, "METHOD II — Trend Following  (%b > 0.8 + MFI > 80)")
summarize(trades_m3, "METHOD III — Reversals       (%b < 0.05 + II% > 0)")


# ── Equity curve printout (2020 onwards) ──────────────────────────────────────
print("\n=== EQUITY CURVES — 2020 onwards (₹ cumulative PnL) ===\n")
for label, trades in [("M1-Squeeze", trades_m1),
                       ("M2-Trend",   trades_m2),
                       ("M3-Reversal",trades_m3)]:
    if trades.empty: continue
    sub = trades[pd.to_datetime(trades["entry_dt"]) >= "2020-01-01"].copy()
    if sub.empty: continue
    sub["month"] = pd.to_datetime(sub["entry_dt"]).dt.to_period("M")
    mon = sub.groupby("month")["pnl_abs"].sum().cumsum()
    print(f"  {label}:")
    for m, val in mon.items():
        bar  = ("▓" if val >= 0 else "░") * min(int(abs(val) / 3000), 30)
        sign = "+" if val >= 0 else ""
        print(f"    {m}  {sign}₹{val:>10,.0f}  {bar}")
    print()

# ── Save logs ─────────────────────────────────────────────────────────────────
trades_m1.to_csv(OUT_DIR + "trades_m1.csv", index=False)
trades_m2.to_csv(OUT_DIR + "trades_m2.csv", index=False)
trades_m3.to_csv(OUT_DIR + "trades_m3.csv", index=False)
print(f"Trade logs → {OUT_DIR}trades_m[1-3].csv")
