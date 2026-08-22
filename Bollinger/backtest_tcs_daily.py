"""
Bollinger Band Backtest — TCS Daily (EOD) data
Book-accurate parameters for daily charts:
  BB : 20-period SMA, ±2 std dev
  MFI: 10-period  (half BB period — book rule)
  II : 21-period
  Squeeze lookback: 126 bars (6 months — exactly as book specifies)

Three exit modes per method:
  A) Fixed SL/TP  (3% SL / 6% TP  → 2:1 R:R)
  B) Opposite-band exit (book's preferred exit for longer trades)
"""

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

DATA_PATH = (
    "/Users/brahmajikatragadda/Desktop/Zerodha_Data/"
    "jugaad-trader/data/nifty50/eod/tcs_eod.csv"
)
OUT_DIR = "/Users/brahmajikatragadda/Desktop/Zerodha_Data/jugaad-trader/Bollinger/"

# ── Parameters (book-accurate for daily) ──────────────────────────────────────
BB_PERIOD  = 20
BB_STD     = 2.0
MFI_PERIOD = 10       # half of BB period
II_PERIOD  = 21
SQUEEZE_LB = 126      # 6 months of trading days (book default)
CAPITAL    = 100_000  # INR per trade
SL_PCT     = 0.03     # 3%
TP_PCT     = 0.06     # 6%  → 2:1 R:R
SLIP_PCT   = 0.0005
COMM_PCT   = 0.0003
COST_PCT   = (SLIP_PCT + COMM_PCT) * 2   # round-trip

# ── Load & clean ───────────────────────────────────────────────────────────────
print("Loading TCS daily data …")
df = pd.read_csv(DATA_PATH, parse_dates=["date"])
df = df.sort_values("date").reset_index(drop=True)
df["date"] = pd.to_datetime(df["date"], utc=False).dt.tz_localize(None)

c, h, l, o, v = df["close"], df["high"], df["low"], df["open"], df["volume"]

# ── Indicators ─────────────────────────────────────────────────────────────────
print("Computing indicators …")

mb  = c.rolling(BB_PERIOD).mean()
std = c.rolling(BB_PERIOD).std(ddof=1)
ub  = mb + BB_STD * std
lb  = mb - BB_STD * std
pct_b     = (c - lb) / (ub - lb)
bandwidth = (ub - lb) / mb

bw_min  = bandwidth.rolling(SQUEEZE_LB).min()
squeeze = bandwidth <= bw_min

# Intraday Intensity % (21-day closed oscillator)
ii_raw = (2*c - h - l) / (h - l).replace(0, np.nan) * v
ii_pct = ii_raw.rolling(II_PERIOD).sum() / v.rolling(II_PERIOD).sum()

# Accumulation Distribution %
ad_raw = (c - o) / (h - l).replace(0, np.nan) * v
ad_pct = ad_raw.rolling(II_PERIOD).sum() / v.rolling(II_PERIOD).sum()

# Money Flow Index (10-day)
tp     = (h + l + c) / 3
mf     = tp * v
pos_mf = mf.where(tp > tp.shift(1), 0.0).rolling(MFI_PERIOD).sum()
neg_mf = mf.where(tp < tp.shift(1), 0.0).rolling(MFI_PERIOD).sum()
mfi    = 100 - 100 / (1 + pos_mf / neg_mf.replace(0, np.nan))

# 50-day SMA for trend filter
sma50  = c.rolling(50).mean()

df = df.assign(
    MB=mb, UB=ub, LB=lb,
    pct_b=pct_b, BandWidth=bandwidth,
    squeeze=squeeze,
    II_pct=ii_pct, AD_pct=ad_pct, MFI=mfi,
    SMA50=sma50,
)
df = df.dropna(subset=["MB","MFI","II_pct","AD_pct","SMA50"]).reset_index(drop=True)
print(f"  {len(df):,} daily bars  ({df['date'].iloc[0].date()} → {df['date'].iloc[-1].date()})")


# ── Backtest engine ─────────────────────────────────────────────────────────────
def run_backtest(signals: pd.Series, df: pd.DataFrame,
                 exit_mode: str = "fixed",   # "fixed" | "band"
                 sl_pct: float = SL_PCT,
                 tp_pct: float = TP_PCT,
                 label: str = "") -> pd.DataFrame:
    """
    Next-bar-open execution on daily bars.

    exit_mode="fixed" : SL at -sl_pct, TP at +tp_pct from entry.
    exit_mode="band"  : SL at -sl_pct, exit when close crosses opposite band.
    """
    trades = []
    pos    = 0
    epx    = 0.0
    edt    = None
    e_ub   = 0.0
    e_lb   = 0.0

    for i in range(1, len(df)):
        row = df.iloc[i]

        if pos != 0:
            sl_hit = tp_hit = band_exit = False

            if pos == 1:
                sl_px  = epx * (1 - sl_pct)
                tp_px  = epx * (1 + tp_pct)
                sl_hit = row["low"] <= sl_px
                if exit_mode == "fixed":
                    tp_hit = row["high"] >= tp_px
                else:
                    band_exit = row["close"] <= row["LB"]   # close crosses lower band
            else:
                sl_px  = epx * (1 + sl_pct)
                tp_px  = epx * (1 - tp_pct)
                sl_hit = row["high"] >= sl_px
                if exit_mode == "fixed":
                    tp_hit = row["low"] <= tp_px
                else:
                    band_exit = row["close"] >= row["UB"]

            if sl_hit or tp_hit or band_exit:
                if sl_hit:
                    xpx    = sl_px
                    reason = "SL"
                elif tp_hit:
                    xpx    = tp_px
                    reason = "TP"
                else:
                    xpx    = row["close"]
                    reason = "BAND"

                pnl_pct = pos * (xpx - epx) / epx - COST_PCT
                pnl_abs = pnl_pct * CAPITAL
                hold    = (row["date"] - edt).days

                trades.append({
                    "entry_dt":  edt,
                    "exit_dt":   row["date"],
                    "hold_days": hold,
                    "direction": "Long" if pos == 1 else "Short",
                    "entry_px":  round(epx, 2),
                    "exit_px":   round(xpx, 2),
                    "reason":    reason,
                    "pnl_pct":   round(pnl_pct * 100, 3),
                    "pnl_abs":   round(pnl_abs, 2),
                })
                pos = 0

        if pos == 0:
            sig = signals.iloc[i - 1]
            if sig != 0:
                pos  = int(sig)
                epx  = row["open"] * (1 + SLIP_PCT * pos)
                edt  = row["date"]
                e_ub = row["UB"]
                e_lb = row["LB"]

    return pd.DataFrame(trades)


# ── Signal builders ─────────────────────────────────────────────────────────────

# Method I — Squeeze breakout
m1_sig    = pd.Series(0, index=df.index)
in_sq_end = False
for i in range(1, len(df)):
    if df["squeeze"].iloc[i-1] and not df["squeeze"].iloc[i]:
        in_sq_end = True
    if in_sq_end and not df["squeeze"].iloc[i]:
        if df["close"].iloc[i] > df["UB"].iloc[i]:
            m1_sig.iloc[i] = 1;  in_sq_end = False
        elif df["close"].iloc[i] < df["LB"].iloc[i]:
            m1_sig.iloc[i] = -1; in_sq_end = False

# Method II — %b + MFI (book: %b>0.8 & MFI>80 buy | %b<0.2 & MFI<20 sell)
m2_sig = pd.Series(0, index=df.index)
m2_sig[(df["pct_b"] > 0.80) & (df["MFI"] > 80)] =  1
m2_sig[(df["pct_b"] < 0.20) & (df["MFI"] < 20)] = -1

# Method II with trend filter (only long above SMA50, only short below SMA50)
m2_tf_sig = pd.Series(0, index=df.index)
m2_tf_sig[(df["pct_b"] > 0.80) & (df["MFI"] > 80) & (df["close"] > df["SMA50"])] =  1
m2_tf_sig[(df["pct_b"] < 0.20) & (df["MFI"] < 20) & (df["close"] < df["SMA50"])] = -1

# Method III — %b + II% (book: lower tag + positive oscillator)
m3_sig = pd.Series(0, index=df.index)
m3_sig[(df["pct_b"] < 0.05) & (df["II_pct"] > 0)] =  1
m3_sig[(df["pct_b"] > 0.95) & (df["II_pct"] < 0)] = -1

# Method III — using AD% instead of II%
m3_ad_sig = pd.Series(0, index=df.index)
m3_ad_sig[(df["pct_b"] < 0.05) & (df["AD_pct"] > 0)] =  1
m3_ad_sig[(df["pct_b"] > 0.95) & (df["AD_pct"] < 0)] = -1


# ── Run all combinations ────────────────────────────────────────────────────────
print("Running backtests …\n")
results = {}
combos = [
    ("M1 Fixed SL/TP",          m1_sig,    "fixed"),
    ("M1 Band Exit",            m1_sig,    "band"),
    ("M2 Fixed SL/TP",          m2_sig,    "fixed"),
    ("M2 Band Exit",            m2_sig,    "band"),
    ("M2+TrendFilter Fixed",    m2_tf_sig, "fixed"),
    ("M2+TrendFilter Band",     m2_tf_sig, "band"),
    ("M3 (II%) Fixed",          m3_sig,    "fixed"),
    ("M3 (II%) Band",           m3_sig,    "band"),
    ("M3 (AD%) Fixed",          m3_ad_sig, "fixed"),
    ("M3 (AD%) Band",           m3_ad_sig, "band"),
]
for label, sig, mode in combos:
    results[label] = run_backtest(sig, df, exit_mode=mode, label=label)


# ── Summary ──────────────────────────────────────────────────────────────────
def summarize(trades: pd.DataFrame, label: str):
    sep = "═" * 64
    if trades.empty:
        print(f"{sep}\n  {label}: No trades.\n"); return

    n    = len(trades)
    wins = trades[trades["pnl_abs"] > 0]
    loss = trades[trades["pnl_abs"] <= 0]
    wr   = len(wins) / n * 100
    aw   = wins["pnl_abs"].mean() if len(wins) else 0
    al   = loss["pnl_abs"].mean() if len(loss) else 0
    rr   = abs(aw / al) if al else float("inf")

    gross   = trades["pnl_abs"].sum()
    eq      = trades["pnl_abs"].cumsum()
    max_dd  = (eq - eq.cummax()).min()
    avg_hld = trades["hold_days"].mean()

    longs  = (trades["direction"] == "Long").sum()
    shorts = (trades["direction"] == "Short").sum()

    trades2 = trades.copy()
    trades2["year"] = pd.to_datetime(trades2["entry_dt"]).dt.year
    yr = trades2.groupby("year")["pnl_abs"].agg(["sum","count"])
    yr.columns = ["PnL_INR","Trades"]
    pos_yr = (yr["PnL_INR"] > 0).sum()

    print(f"""
{sep}
  {label}
{"─"*64}
  Trades        : {n:>5}    Long:{longs}  Short:{shorts}
  Win Rate      : {wr:>5.1f}%
  Avg Win       : ₹{aw:>9,.0f}
  Avg Loss      : ₹{al:>9,.0f}
  Risk-Reward   : {rr:>5.2f}x
  Gross PnL     : ₹{gross:>12,.0f}
  Max Drawdown  : ₹{max_dd:>12,.0f}
  Avg Hold      : {avg_hld:.0f} days
  Profitable yrs: {pos_yr}/{len(yr)}
{sep}
  Year-by-year:""")

    for yr_val, row2 in yr.iterrows():
        bar  = ("▓" * min(int(abs(row2["PnL_INR"]) / 2000), 20))
        sign = "+" if row2["PnL_INR"] >= 0 else " "
        print(f"    {yr_val}  {sign}₹{row2['PnL_INR']:>9,.0f}  [{row2['Trades']:>3} trades]  {bar}")
    print()


for label, trades in results.items():
    summarize(trades, label)


# ── Leaderboard ──────────────────────────────────────────────────────────────
print("\n" + "═"*64)
print("  LEADERBOARD — Ranked by Gross PnL")
print("═"*64)
board = [(lbl, t["pnl_abs"].sum() if not t.empty else 0,
          len(t) if not t.empty else 0,
          (t["pnl_abs"] > 0).sum() / len(t) * 100 if not t.empty else 0)
         for lbl, t in results.items()]
board.sort(key=lambda x: -x[1])
for rank, (lbl, pnl, n, wr) in enumerate(board, 1):
    sign = "+" if pnl >= 0 else ""
    flag = " ✓" if pnl > 0 else ""
    print(f"  {rank:>2}. {lbl:<28}  {sign}₹{pnl:>10,.0f}  WR:{wr:>4.1f}%  ({n} trades){flag}")
print()

# ── Save best result trade log ────────────────────────────────────────────────
best_label = board[0][0]
best_trades = results[best_label]
best_trades.to_csv(OUT_DIR + "trades_best_daily.csv", index=False)
print(f"Best strategy: {best_label}")
print(f"Trade log → {OUT_DIR}trades_best_daily.csv")
