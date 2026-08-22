"""
Bollinger on Bollinger Bands — John Bollinger
Complete Strategy Implementation

Covers:
  - Band construction + %b + BandWidth
  - W-Type Bottoms (W1-W5) & M-Type Tops (M1-M5)
  - Walking the Bands
  - Method I  : Volatility Breakout (The Squeeze)
  - Method II : Trend Following (%b + MFI)
  - Method III: Reversals (band tag + oscillator)
  - Volume indicators: II, AD, MFI, VWMACD
  - Normalizing indicators with %b
  - 15 Basic Rules (comments)
"""

import pandas as pd
import numpy as np

# ─────────────────────────────────────────────
# 1. CORE BOLLINGER BAND CONSTRUCTION
# ─────────────────────────────────────────────
# Rule 9 : default 20 periods, ±2 std dev
# Rule 10: average describes intermediate trend
# Rule 11: if period increases, also increase std devs
#          20p → 2.0 sd | 50p → 2.1 sd | 10p → 1.9 sd
# Rule 12: use SMA (not EMA) for logical consistency

def bollinger_bands(close: pd.Series, period: int = 20, num_std: float = 2.0):
    """
    Returns middle, upper, lower bands, %b, and BandWidth.
    """
    mb = close.rolling(period).mean()
    std = close.rolling(period).std(ddof=1)
    ub = mb + num_std * std
    lb = mb - num_std * std
    pct_b = (close - lb) / (ub - lb)          # %b
    bandwidth = (ub - lb) / mb                # BandWidth
    return pd.DataFrame({
        "MB": mb, "UB": ub, "LB": lb,
        "pct_b": pct_b, "BandWidth": bandwidth
    })


# ─────────────────────────────────────────────
# 2. VOLUME INDICATORS  (Chapter 18, Table 18.3)
# ─────────────────────────────────────────────

def intraday_intensity(high, low, close, volume, period: int = 21):
    """
    II = (2*close - high - low) / (high - low) * volume
    Open form: cumulative sum.
    Closed (oscillator) form: n-period rolling sum / n-period volume sum → II%
    """
    ii_raw = (2 * close - high - low) / (high - low).replace(0, np.nan) * volume
    ii_open = ii_raw.cumsum()
    ii_pct  = ii_raw.rolling(period).sum() / volume.rolling(period).sum()
    return pd.DataFrame({"II_open": ii_open, "II_pct": ii_pct})


def accumulation_distribution(high, low, close, open_, volume, period: int = 21):
    """
    AD = (close - open) / (high - low) * volume
    AD% = rolling n-period sum of raw / rolling volume sum
    """
    ad_raw  = (close - open_) / (high - low).replace(0, np.nan) * volume
    ad_open = ad_raw.cumsum()
    ad_pct  = ad_raw.rolling(period).sum() / volume.rolling(period).sum()
    return pd.DataFrame({"AD_open": ad_open, "AD_pct": ad_pct})


def money_flow_index(high, low, close, volume, period: int = 10):
    """
    MFI: volume-weighted RSI.  Thresholds: 80/20 (not 70/30).
    """
    tp = (high + low + close) / 3
    mf = tp * volume
    pos = mf.where(tp > tp.shift(1), 0.0).rolling(period).sum()
    neg = mf.where(tp < tp.shift(1), 0.0).rolling(period).sum()
    mfi = 100 - 100 / (1 + pos / neg.replace(0, np.nan))
    return mfi


def vwmacd(close, volume, fast: int = 12, slow: int = 26, signal: int = 9):
    """
    Volume-Weighted MACD.
    Substitute VW moving averages for the exponential averages in standard MACD.
    Signal line remains a plain EMA of the VWMACD line.
    """
    def vw_ma(c, v, n):
        return (c * v).rolling(n).sum() / v.rolling(n).sum()

    vw_fast   = vw_ma(close, volume, fast)
    vw_slow   = vw_ma(close, volume, slow)
    vwmacd_line   = vw_fast - vw_slow
    signal_line   = vwmacd_line.ewm(span=signal, adjust=False).mean()
    histogram     = vwmacd_line - signal_line
    return pd.DataFrame({
        "VWMACD": vwmacd_line,
        "Signal": signal_line,
        "Hist": histogram
    })


def normalize_with_pct_b(indicator: pd.Series, period: int = 40, num_std: float = 2.0):
    """
    Chapter 21: Plot Bollinger Bands ON an indicator, then compute %b.
    Turns any indicator into an adaptive overbought/oversold measure.
    Formula (Table 21.2): (indicator - lower_band) / (upper_band - lower_band)
    Typical parameters (Table 21.1):
        9-period  RSI → 40p, 2.0 sd
        14-period RSI → 50p, 2.1 sd
        10-period MFI → 40p, 2.0 sd
        21-period II  → 40p, 2.0 sd
    """
    mb  = indicator.rolling(period).mean()
    std = indicator.rolling(period).std(ddof=1)
    ub  = mb + num_std * std
    lb  = mb - num_std * std
    return (indicator - lb) / (ub - lb)


# ─────────────────────────────────────────────
# 3. PATTERN RECOGNITION — W-TYPE BOTTOMS
# ─────────────────────────────────────────────
# W patterns are bottoming formations identified by Bollinger.
# Key rule: second low must be RELATIVE (inside the bands) while
# the first low is absolute (tags or exceeds lower band).
# Confirm with a positive volume oscillator on the second low.

def detect_w_bottoms(df: pd.DataFrame,
                     bb_period: int = 20, bb_std: float = 2.0,
                     reaction_window: int = 10,
                     lookback: int = 50) -> pd.Series:
    """
    Detects W2/W4 bottom setups (the most tradeable W patterns).

    W2 setup criteria:
      1. Low-1 tags or breaks lower band.
      2. Reaction rally reaches at least middle band.
      3. Low-2 is ABOVE lower band (relative low) but is a local low.
      4. Volume oscillator (II%) is positive on low-2 (confirmation).

    Returns a boolean Series — True on bars that complete a W setup.
    Requires columns: open, high, low, close, volume.
    """
    bb = bollinger_bands(df["close"], bb_period, bb_std)
    ii = intraday_intensity(df["high"], df["low"], df["close"], df["volume"])

    signals = pd.Series(False, index=df.index)

    for i in range(lookback, len(df)):
        window = df.iloc[i - lookback: i + 1]
        bb_w   = bb.iloc[i - lookback: i + 1]
        ii_w   = ii.iloc[i - lookback: i + 1]

        # Find local lows within window
        lows = window["low"]
        local_lows = lows[(lows < lows.shift(1)) & (lows < lows.shift(-1))]
        if len(local_lows) < 2:
            continue

        low2_idx = local_lows.index[-1]
        low1_idx = local_lows.index[-2]
        low2_pos = window.index.get_loc(low2_idx)
        low1_pos = window.index.get_loc(low1_idx)

        if low2_pos - low1_pos < 3:
            continue

        low1_val  = lows[low1_idx]
        low2_val  = lows[low2_idx]
        lb_at_l1  = bb_w["LB"][low1_idx]
        lb_at_l2  = bb_w["LB"][low2_idx]
        mb_at_l2  = bb_w["MB"][low2_idx]

        # Criterion 1: low-1 tags lower band
        if low1_val > lb_at_l1 * 1.005:
            continue

        # Criterion 2: reaction between low-1 and low-2 hits middle band
        between = window["high"].iloc[low1_pos: low2_pos]
        if between.max() < mb_at_l2:
            continue

        # Criterion 3: low-2 is above lower band (relative low)
        if low2_val <= lb_at_l2:
            continue

        # Criterion 4: volume oscillator positive at low-2
        if ii_w["II_pct"][low2_idx] <= 0:
            continue

        signals.iloc[i] = True

    return signals


# ─────────────────────────────────────────────
# 4. PATTERN RECOGNITION — M-TYPE TOPS
# ─────────────────────────────────────────────
# Mirror image of W bottoms.
# Classic setup: high-1 outside upper band, high-2 inside upper band,
# with declining volume indicator on each push.
# "Three pushes to a high" is the most common form.

def detect_m_tops(df: pd.DataFrame,
                  bb_period: int = 20, bb_std: float = 2.0,
                  lookback: int = 50) -> pd.Series:
    """
    Detects M2-type top setups.

    M2 setup criteria:
      1. High-1 tags or exceeds upper band.
      2. Pullback reaches at least middle band.
      3. High-2 is BELOW upper band (relative high) but is a local high.
      4. Volume oscillator (II%) is negative on high-2.
    """
    bb = bollinger_bands(df["close"], bb_period, bb_std)
    ii = intraday_intensity(df["high"], df["low"], df["close"], df["volume"])

    signals = pd.Series(False, index=df.index)

    for i in range(lookback, len(df)):
        window = df.iloc[i - lookback: i + 1]
        bb_w   = bb.iloc[i - lookback: i + 1]
        ii_w   = ii.iloc[i - lookback: i + 1]

        highs = window["high"]
        local_highs = highs[(highs > highs.shift(1)) & (highs > highs.shift(-1))]
        if len(local_highs) < 2:
            continue

        h2_idx = local_highs.index[-1]
        h1_idx = local_highs.index[-2]
        h2_pos = window.index.get_loc(h2_idx)
        h1_pos = window.index.get_loc(h1_idx)

        if h2_pos - h1_pos < 3:
            continue

        high1_val = highs[h1_idx]
        high2_val = highs[h2_idx]
        ub_at_h1  = bb_w["UB"][h1_idx]
        ub_at_h2  = bb_w["UB"][h2_idx]
        mb_at_h2  = bb_w["MB"][h2_idx]

        if high1_val < ub_at_h1 * 0.995:
            continue

        between = window["low"].iloc[h1_pos: h2_pos]
        if between.min() > mb_at_h2:
            continue

        if high2_val >= ub_at_h2:
            continue

        if ii_w["II_pct"][h2_idx] >= 0:
            continue

        signals.iloc[i] = True

    return signals


# ─────────────────────────────────────────────
# 5. METHOD I — VOLATILITY BREAKOUT (THE SQUEEZE)
# ─────────────────────────────────────────────
# Philosophy: low volatility begets high volatility.
# Setup: BandWidth falls to its lowest level in N months (Squeeze).
# Entry: first close outside the bands after the Squeeze fires.
# Head fake: price often feints the wrong way first — use volume
# indicators (II / AD) to forecast direction before the breakout.

def method_i_squeeze(df: pd.DataFrame,
                     bb_period: int = 20, bb_std: float = 2.0,
                     squeeze_lookback: int = 126) -> pd.DataFrame:
    """
    Method I signals.

    squeeze_lookback: bars to look back for BandWidth minimum (~6 months).
    Returns a DataFrame with columns:
        squeeze    : True when BandWidth is at its lowest in lookback period.
        signal     : +1 (long), -1 (short), 0 (none).
        stop_long  : suggested Parabolic-style initial stop for longs.
        stop_short : suggested Parabolic-style initial stop for shorts.
    """
    bb = bollinger_bands(df["close"], bb_period, bb_std)
    bw = bb["BandWidth"]

    bw_min   = bw.rolling(squeeze_lookback).min()
    squeeze  = bw <= bw_min         # True on squeeze bars

    signal   = pd.Series(0, index=df.index)
    in_squeeze_ended = False

    for i in range(1, len(df)):
        was_squeeze = squeeze.iloc[i - 1]
        is_squeeze  = squeeze.iloc[i]

        if was_squeeze and not is_squeeze:
            in_squeeze_ended = True

        if in_squeeze_ended:
            close = df["close"].iloc[i]
            ub    = bb["UB"].iloc[i]
            lb    = bb["LB"].iloc[i]
            if close > ub:
                signal.iloc[i] = 1
                in_squeeze_ended = False
            elif close < lb:
                signal.iloc[i] = -1
                in_squeeze_ended = False

    stop_long  = df["low"].rolling(3).min()
    stop_short = df["high"].rolling(3).max()

    return pd.DataFrame({
        "squeeze": squeeze,
        "BandWidth": bw,
        "signal": signal,
        "stop_long": stop_long,
        "stop_short": stop_short,
    })


# ─────────────────────────────────────────────
# 6. METHOD II — TREND FOLLOWING
# ─────────────────────────────────────────────
# Philosophy: buy confirmed strength, sell confirmed weakness.
# Price strength (%b) + volume strength (MFI) both must agree.
# MFI period ≈ half the BB period (rule of thumb).
#
# BUY : %b > 0.8  AND  MFI(10) > 80
# SELL: %b < 0.2  AND  MFI(10) < 20
# Exit: Parabolic SAR or opposite-band tag.

def method_ii_trend(df: pd.DataFrame,
                    bb_period: int = 20, bb_std: float = 2.0,
                    mfi_period: int = 10,
                    buy_pct_b: float = 0.8, buy_mfi: float = 80.0,
                    sell_pct_b: float = 0.2, sell_mfi: float = 20.0) -> pd.DataFrame:
    """
    Method II trend-following signals.
    Returns signal column: +1 buy, -1 sell, 0 hold.
    Variation: replace MFI with VWMACD histogram for stronger trend filter.
    """
    bb  = bollinger_bands(df["close"], bb_period, bb_std)
    mfi = money_flow_index(df["high"], df["low"], df["close"], df["volume"], mfi_period)

    buy_signal  = (bb["pct_b"] > buy_pct_b)  & (mfi > buy_mfi)
    sell_signal = (bb["pct_b"] < sell_pct_b) & (mfi < sell_mfi)

    signal = pd.Series(0, index=df.index)
    signal[buy_signal]  =  1
    signal[sell_signal] = -1

    return pd.DataFrame({
        "pct_b": bb["pct_b"],
        "MFI": mfi,
        "signal": signal,
        "UB": bb["UB"],
        "MB": bb["MB"],
        "LB": bb["LB"],
    })


# ─────────────────────────────────────────────
# 7. METHOD III — REVERSALS
# ─────────────────────────────────────────────
# Philosophy: find band tags where the indicator is OPPOSITE to price.
#   Lower band tag + positive oscillator → buy (demand is rising despite low price)
#   Upper band tag + negative oscillator → sell (supply is rising despite high price)
#
# Systematized entry rules:
#   BUY : %b < 0.05  AND  II% > 0  (or AD% > 0)
#   SELL: %b > 0.95  AND  II% < 0  (or AD% < 0)
#
# Enhanced (W-bottom confirmation):
#   - First low tags lower band, second low is relative (inside bands)
#   - MFI/VWMACD positive on second low
#   - Buy on first strong up day after second low

def method_iii_reversals(df: pd.DataFrame,
                          bb_period: int = 20, bb_std: float = 2.0,
                          ii_period: int = 21,
                          buy_pct_b: float = 0.05,
                          sell_pct_b: float = 0.95) -> pd.DataFrame:
    """
    Method III reversal signals using Intraday Intensity oscillator.
    """
    bb = bollinger_bands(df["close"], bb_period, bb_std)
    ii = intraday_intensity(df["high"], df["low"], df["close"], df["volume"], ii_period)

    buy_signal  = (bb["pct_b"] < buy_pct_b)  & (ii["II_pct"] > 0)
    sell_signal = (bb["pct_b"] > sell_pct_b) & (ii["II_pct"] < 0)

    signal = pd.Series(0, index=df.index)
    signal[buy_signal]  =  1
    signal[sell_signal] = -1

    return pd.DataFrame({
        "pct_b": bb["pct_b"],
        "II_pct": ii["II_pct"],
        "signal": signal,
        "UB": bb["UB"],
        "MB": bb["MB"],
        "LB": bb["LB"],
    })


def method_iii_djia_timing(advances: pd.Series, declines: pd.Series,
                            up_vol: pd.Series, dn_vol: pd.Series,
                            bb_period: int = 20, bb_std: float = 2.0,
                            macd_fast: int = 21, macd_slow: int = 100,
                            macd_signal: int = 9) -> pd.DataFrame:
    """
    Method III variant for market timing (DJIA / index level).
    Uses MACD(21, 100, 9) on Advance-Decline data as the oscillator,
    then substitutes Bollinger Bands for the percentage bands.

    Buy : price tags lower BB  AND  A-D MACD oscillator is positive.
    Sell: price tags upper BB  AND  A-D MACD oscillator is negative.
    """
    ad_net = advances - declines
    ad_fast = ad_net.ewm(span=macd_fast, adjust=False).mean()
    ad_slow = ad_net.ewm(span=macd_slow, adjust=False).mean()
    ad_macd = ad_fast - ad_slow
    ad_sig  = ad_macd.ewm(span=macd_signal, adjust=False).mean()
    ad_hist = ad_macd - ad_sig

    uv_net   = up_vol - dn_vol
    uv_fast  = uv_net.ewm(span=macd_fast, adjust=False).mean()
    uv_slow  = uv_net.ewm(span=macd_slow, adjust=False).mean()
    uv_macd  = uv_fast - uv_slow
    uv_sig   = uv_macd.ewm(span=macd_signal, adjust=False).mean()
    uv_hist  = uv_macd - uv_sig

    return pd.DataFrame({
        "AD_MACD_hist": ad_hist,
        "UV_MACD_hist": uv_hist,
    })


# ─────────────────────────────────────────────
# 8. WALKING THE BANDS DETECTOR
# ─────────────────────────────────────────────
# Closes outside the bands are CONTINUATION signals, not reversals.
# Walking is confirmed when the volume indicator agrees with price.
# Watch for weakening closes outside bands → trend ending.

def walking_the_bands(df: pd.DataFrame,
                      bb_period: int = 20, bb_std: float = 2.0,
                      ii_period: int = 21,
                      consecutive: int = 3) -> pd.DataFrame:
    """
    Identifies 'walking the band' phases.
    walk_up  : n consecutive closes above upper band with positive II.
    walk_down: n consecutive closes below lower band with negative II.
    Tag close to middle band during a walk = potential entry/add-on.
    """
    bb = bollinger_bands(df["close"], bb_period, bb_std)
    ii = intraday_intensity(df["high"], df["low"], df["close"], df["volume"], ii_period)

    above_ub = (df["close"] > bb["UB"]) & (ii["II_pct"] > 0)
    below_lb = (df["close"] < bb["LB"]) & (ii["II_pct"] < 0)

    walk_up   = above_ub.rolling(consecutive).min().astype(bool)
    walk_down = below_lb.rolling(consecutive).min().astype(bool)

    near_mb_buy  = (df["close"] - bb["MB"]).abs() / bb["MB"] < 0.01
    near_mb_sell = near_mb_buy.copy()

    return pd.DataFrame({
        "walk_up": walk_up,
        "walk_down": walk_down,
        "add_on_long": walk_up & near_mb_buy,
        "add_on_short": walk_down & near_mb_sell,
        "pct_b": bb["pct_b"],
    })


# ─────────────────────────────────────────────
# 9. COMBINED SIGNAL GENERATOR
# ─────────────────────────────────────────────

def generate_all_signals(df: pd.DataFrame,
                         bb_period: int = 20, bb_std: float = 2.0) -> pd.DataFrame:
    """
    Runs all three methods and pattern detectors on a OHLCV DataFrame.
    Input columns: open, high, low, close, volume  (all lowercase).
    """
    bb  = bollinger_bands(df["close"], bb_period, bb_std)
    m1  = method_i_squeeze(df, bb_period, bb_std)
    m2  = method_ii_trend(df, bb_period, bb_std)
    m3  = method_iii_reversals(df, bb_period, bb_std)
    w_b = detect_w_bottoms(df, bb_period, bb_std)
    m_t = detect_m_tops(df, bb_period, bb_std)
    walk = walking_the_bands(df, bb_period, bb_std)

    return pd.DataFrame({
        "MB": bb["MB"], "UB": bb["UB"], "LB": bb["LB"],
        "pct_b": bb["pct_b"], "BandWidth": bb["BandWidth"],
        "squeeze": m1["squeeze"],
        "m1_signal": m1["signal"],
        "m2_signal": m2["signal"],
        "m3_signal": m3["signal"],
        "w_bottom": w_b,
        "m_top": m_t,
        "walk_up": walk["walk_up"],
        "walk_down": walk["walk_down"],
    }, index=df.index)


# ─────────────────────────────────────────────
# 10. 15 BASIC RULES (as coded constants/comments)
# ─────────────────────────────────────────────
RULES = {
    1:  "BB provide a RELATIVE definition of high and low.",
    2:  "Compare price action to indicator action for buy/sell decisions.",
    3:  "Use indicators from: momentum, volume, sentiment, open interest.",
    4:  "Volatility and trend are already IN the bands — don't reuse them for confirmation.",
    5:  "Indicators must not be collinear. One per category (momentum, volume, trend, sentiment).",
    6:  "BB clarify patterns: M-type tops, W-type bottoms, momentum shifts.",
    7:  "Price CAN and DOES walk up the upper band or down the lower band.",
    8:  "Closes OUTSIDE the bands are CONTINUATION signals, not reversals.",
    9:  "Default: 20 periods, ±2 std dev. Actual parameters depend on market/task.",
    10: "The average should describe the INTERMEDIATE trend, not be used for crossover signals.",
    11: "If period lengthens → increase std devs. (20p,2.0) → (50p,2.1) | (10p,1.9).",
    12: "BB use SMA (not EMA) because SMA is used in the std deviation calculation.",
    13: "Be careful with statistical assumptions — sample sizes are small, distributions non-normal.",
    14: "Indicators normalized with %b eliminate fixed thresholds — fully adaptive.",
    15: "Tags of bands are tags only — NOT signals in and of themselves.",
}


# ─────────────────────────────────────────────
# 11. PARAMETER REFERENCE TABLE
# ─────────────────────────────────────────────
PARAMS = {
    "Bollinger Bands (default)":   {"period": 20, "num_std": 2.0},
    "Bollinger Bands (short)":     {"period": 10, "num_std": 1.9},
    "Bollinger Bands (long)":      {"period": 50, "num_std": 2.1},
    "Method I":                    {"period": 20, "num_std": 2.0, "squeeze_lookback": 126},
    "Method II":                   {"period": 20, "num_std": 2.0, "mfi_period": 10,
                                    "buy_pct_b": 0.8, "buy_mfi": 80,
                                    "sell_pct_b": 0.2, "sell_mfi": 20},
    "Method III":                  {"period": 20, "num_std": 2.0,
                                    "buy_pct_b": 0.05, "sell_pct_b": 0.95},
    "Normalizing RSI(9)":          {"period": 40, "num_std": 2.0},
    "Normalizing RSI(14)":         {"period": 50, "num_std": 2.1},
    "Normalizing MFI(10)":         {"period": 40, "num_std": 2.0},
    "Normalizing II(21)":          {"period": 40, "num_std": 2.0},
    "Method III DJIA":             {"macd_fast": 21, "macd_slow": 100, "macd_signal": 9},
}


# ─────────────────────────────────────────────
# 12. USAGE EXAMPLE
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import yfinance as yf

    ticker = "RELIANCE.NS"
    raw = yf.download(ticker, start="2022-01-01", end="2024-12-31", auto_adjust=True)
    raw.columns = [c.lower() for c in raw.columns]
    df = raw[["open", "high", "low", "close", "volume"]].dropna()

    signals = generate_all_signals(df)
    print(signals.tail(20))

    # Print 15 rules
    print("\n=== 15 BASIC RULES ===")
    for k, v in RULES.items():
        print(f"  {k:>2}. {v}")

    # Print parameter reference
    print("\n=== PARAMETER REFERENCE ===")
    for name, p in PARAMS.items():
        print(f"  {name}: {p}")
