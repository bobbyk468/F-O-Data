# Bollinger on Bollinger Bands — Complete Strategy Reference
**Author: John Bollinger | Decoded for Implementation**

---

## Book Structure

| Part | Chapters | Topic |
|------|----------|-------|
| I    | 1–3      | History & Relativity |
| II   | 4–6      | Basics & Statistics |
| III  | 7–16     | Bollinger Bands on Their Own |
| IV   | 17–20    | BB with Indicators |
| V    | 21–22    | Advanced Topics (Normalize, Day Trading) |
| VI   | —        | 15 Basic Rules |

---

## Part 1 — Core Construction

### Bollinger Band Formula
```
Middle Band (MB) = SMA(close, 20)
Upper Band (UB)  = MB + 2 × StdDev(close, 20)
Lower Band (LB)  = MB − 2 × StdDev(close, 20)
```
**Use SMA, not EMA** — SMA is used in the std dev calculation; must be consistent.

### %b (Percent b)
```
%b = (close − LB) / (UB − LB)
   = 1.0 → at upper band
   = 0.5 → at middle band
   = 0.0 → at lower band
   > 1.0 → above upper band (continuation)
   < 0.0 → below lower band (continuation)
```

### BandWidth
```
BandWidth = (UB − LB) / MB
```
Measures volatility relative to price. Low BandWidth = Squeeze forming.

### Parameter Scaling Rules
| Period | Std Dev |
|--------|---------|
| 10     | 1.9     |
| 20     | 2.0     |
| 50     | 2.1     |

---

## Part 2 — Volume Indicators (Chapter 18)

All formulas from **Table 18.3**:

| Indicator | Formula |
|-----------|---------|
| On Balance Volume | `volume × sign(price change)` |
| Volume-Price Trend | `volume × % price change` |
| Negative Volume Index (NVI) | Accumulate price change when volume falls |
| Positive Volume Index (PVI) | Accumulate price change when volume rises |
| Intraday Intensity (II) | `(2×close − high − low) / (high − low) × volume` |
| Accumulation Distribution (AD) | `(close − open) / (high − low) × volume` |
| Money Flow Index (MFI) | `100 − 100/(1 + pos_mf / neg_mf)` |
| VWMACD | VW_MA(12) − VW_MA(26); Signal = EMA(9) of VWMACD |
| Normalized Oscillator (II%) | `rolling_sum(II_raw, n) / rolling_sum(volume, n)` |

**Preferred indicators:** II, AD, MFI, VWMACD  
**MFI thresholds:** 80 / 20 (not 70/30 like RSI)  
**Open form** = cumulative sum (trend line)  
**Closed form** = n-period rolling sum / volume sum (oscillator, %)

### Indicator Categories (avoid collinearity — pick one per row)
| Category | Example Indicators |
|----------|--------------------|
| Momentum | Rate of change, Stochastics |
| Trend | Linear regression, MACD |
| Sentiment | Survey, put-call ratio |
| Volume (open) | Intraday Intensity, Accumulation Distribution |
| Volume (closed) | Money Flow Index, Volume-Weighted MACD |
| Overbought/Oversold | CCI, RSI |

---

## Part 3 — Pattern Recognition

### W-Type Bottoms (Bullish Reversals)

Bollinger names W patterns by the number of price extremes (W1 = 2 points, W2 = 4 points, etc.)

#### The Key W2 / W4 Setup (Most Tradeable)
```
1. Low-1: Tags or breaks lower band (absolute low)
2. Reaction rally: Price rises to AT LEAST the middle band
3. Low-2: Forms ABOVE lower band (relative low — this is the key)
4. Volume oscillator (II% or AD%): Positive at low-2
5. Confirmation: Strong up day (above-average volume + range)
```

**Why relative low matters:** Bollinger Bands define high/low relatively.  
A new absolute price low that is INSIDE the bands is actually a HIGHER relative low — bullish.

#### Pattern Taxonomy
| Pattern | Description |
|---------|-------------|
| W1 | Simple double bottom — both lows tag band |
| W2 | Second low higher than first, inside band ← **most common setup** |
| W3 | Flat bottom — both lows at same level |
| W4 | Second low makes new price low but stays inside band (relative W) |
| W5 | Rounded bottom / saucer |

---

### M-Type Tops (Bearish Reversals)

Mirror image of W bottoms. Tops take longer to form — more patience required.

#### The Classic M2 Setup
```
1. High-1: Touches or exceeds upper band (absolute high)
2. Pullback: Price retreats to AT LEAST the middle band
3. High-2: Forms BELOW upper band (relative high — failing momentum)
4. Volume oscillator: Negative/declining at high-2
5. Confirmation: Wait for sign of weakness (big down day, high volume)
6. Entry: Throwback rally after neckline break = optimal short entry
```

#### Head-and-Shoulders as M-Pattern Decomposition
- Left shoulder + Head = M14 or M15 pattern
- Head + Right shoulder = M3 or M7 pattern
- Classic: left shoulder OUTSIDE band, head TAGS band, right shoulder FAILS band

#### Three Pushes to a High (Very Common Top)
```
Push 1: Outside upper band
Push 2: Makes new high, tags upper band
Push 3: Makes marginal new high but FAILS to tag upper band
→ Volume diminishing steadily across all three pushes
```

---

## Part 4 — Walking the Bands (Chapter 14)

**Critical concept:** Tags of the band are NOT buy/sell signals.

During strong trends:
- Price can make **multiple closes outside** the bands — these are **continuation signals**
- The middle band acts as **support/resistance** during the walk
- Volume indicators confirm: II or AD rising with price walk = healthy trend

### Walking the Band Rules
```
1. Close outside upper band + positive II → continuation (NOT a sell)
2. Close outside lower band + negative II → continuation (NOT a buy)
3. Closes returning INSIDE the band → first warning of trend end
4. Volume indicator diverging on band tag → real warning signal
5. Middle band pulls in price → entry / add-on opportunity
```

### Expansion Reversal Rule
When BandWidth has been expanding (strong trend), and then **lower band turns up** (uptrend) or **upper band turns down** (downtrend) → current leg is most likely over.

---

## Part 5 — The Squeeze (Chapter 15)

**Core principle:** Low volatility begets high volatility.

```
Squeeze triggered when:
  BandWidth = lowest reading in last 6 months (126 trading days)

Head Fake warning:
  Price often feints the wrong direction first, then reverses
  Strategy: take half position on fake, add full on real breakout
  OR: use Parabolic SAR style stop to reverse if faked out
```

### Direction Forecasting During Squeeze
Before the breakout, use volume indicators to forecast direction:
- **II% or AD% rising** → likely bullish breakout
- **II% or AD% falling** → likely bearish breakout
- News is often the catalyst — watch for it

---

## Part 6 — The Three Methods

### Method I: Volatility Breakout

**Philosophy:** Exploit the cyclical nature of volatility.

```
SETUP:   BandWidth at 6-month low (Squeeze active)
ENTRY:   First close ABOVE upper band → LONG
         First close BELOW lower band → SHORT
EXIT 1:  Parabolic SAR (initial stop below range low for longs)
EXIT 2:  Tag of OPPOSITE band (longer holds, bigger moves)
```

**Handling Head Fakes:**
```
Option A: Wait for Squeeze + first move + reversal, trade the real move
Option B: Trade half on initial breakout, add on confirmation
          Use Parabolic or opposite-band stop to stay protected
```

**Parameters:** 20-period SMA, ±2 std dev, 6-month squeeze lookback  
**Short-term traders:** Shorten to 15 periods, tighten to ±1.5 std dev

---

### Method II: Trend Following

**Philosophy:** Buy confirmed strength, sell confirmed weakness.

```
BUY  signal: %b > 0.8   AND  MFI(10) > 80
SELL signal: %b < 0.2   AND  MFI(10) < 20

EXIT: Parabolic SAR  OR  tag of opposite band
```

**What this means:**
- `%b > 0.8` = price is in top 20% of the band (near upper band)
- `MFI > 80` = volume is overwhelmingly bullish
- Both must agree → high-confidence trend entry

**Variations (Table 19.1):**
- Substitute VWMACD for MFI (slower but smoother)
- Adjust %b threshold higher (>0.9) for volatile stocks
- Start Parabolic under the most recent significant low (not entry day)
- Use signals as ALERTS, then buy first pullback → better risk-reward

**MFI period rule:** ~half the BB period (20-period BB → 10-period MFI)

---

### Method III: Reversals

**Philosophy:** Find band tags where volume indicator CONTRADICTS price.

```
BUY  setup: %b < 0.05  AND  II%(21) > 0   (or AD% > 0)
SELL setup: %b > 0.95  AND  II%(21) < 0   (or AD% < 0)

→ Lower band tag with POSITIVE oscillator = smart money buying the dip
→ Upper band tag with NEGATIVE oscillator = smart money selling the rip
```

**Enhanced W-Bottom (Method III + Pattern):**
```
1. First low: price tags lower band
2. Second low: price makes new absolute low BUT stays inside bands (%b > 0)
3. Volume indicator (MFI/VWMACD) is POSITIVE on second low
4. Buy on first strong up day (above-avg volume + range)
```

**Enhanced M-Top (Method III + Pattern):**
```
1. Multiple pushes to high with %b declining on each push
2. Volume indicator (AD or II) declining on each push
3. Sell on meaningful down day (above-avg volume + range)
```

**DJIA Market Timing Variant:**
```
Oscillators: MACD(21, 100, 9) applied to:
  - Advancing minus declining issues (NYSE)
  - Up volume minus down volume (NYSE)
Replace percentage bands with Bollinger Bands

BUY : price at lower BB  AND  A-D MACD histogram positive
SELL: price at upper BB  AND  A-D MACD histogram negative
```

---

## Part 7 — Normalizing Indicators (Chapter 21)

Apply Bollinger Bands ON an indicator to dynamically define overbought/oversold.

```
Step 1: Calculate your indicator (RSI, MFI, etc.)
Step 2: Plot Bollinger Bands on the indicator
Step 3: Calculate %b of the indicator:
          %b(indicator) = (indicator − lower_BB) / (upper_BB − lower_BB)
Step 4: Use %b thresholds (0 = oversold, 1 = overbought) instead of fixed levels
```

**Why this works:** Fixed levels (RSI 70/30) shift up in bull markets and down in bear markets. Bollinger Bands adapt automatically.

### Recommended Parameters (Table 21.1)
| Indicator | BB Period | Std Dev |
|-----------|-----------|---------|
| RSI(9)    | 40        | 2.0     |
| RSI(14)   | 50        | 2.1     |
| MFI(10)   | 40        | 2.0     |
| II(21)    | 40        | 2.0     |

---

## Part 8 — 15 Basic Rules

1. BB provide a **relative** definition of high and low.
2. That relative definition compares **price action to indicator action** → buy/sell decisions.
3. Indicators can come from momentum, **volume**, sentiment, open interest, intermarket data.
4. Volatility and trend are already deployed in band construction — **don't use them again** for confirmation.
5. Indicators used for confirmation **must not be collinear**. One per category only.
6. BB clarify pure price patterns: M-type tops, W-type bottoms, momentum shifts.
7. Price **can and does walk** up the upper band and down the lower band.
8. **Closes outside the bands are continuation signals**, not reversal signals.
9. Default = 20 periods, ±2 std dev. **Actual parameters depend on the market.**
10. The average should be **descriptive of the intermediate trend**, not used for crossover signals.
11. If average is lengthened → increase std devs simultaneously. (20→50 periods, 2.0→2.1 std).
12. BB are based on **SMA** (not EMA) — logical consistency with std deviation calculation.
13. **Be careful with statistical assumptions** — sample sizes are small; distributions are rarely normal.
14. Indicators can be **normalized with %b**, eliminating fixed thresholds.
15. **Tags of bands are just tags** — not signals in and of themselves.

---

## Part 9 — Quick Signal Cheat Sheet

| Signal | Condition | Action |
|--------|-----------|--------|
| Method I Long | Squeeze + close > UB | Buy |
| Method I Short | Squeeze + close < LB | Sell |
| Method II Buy | %b > 0.8 AND MFI > 80 | Buy |
| Method II Sell | %b < 0.2 AND MFI < 20 | Sell |
| Method III Buy | %b < 0.05 AND II% > 0 | Buy |
| Method III Sell | %b > 0.95 AND II% < 0 | Sell |
| W Bottom | Low-1 at LB, Low-2 inside bands, II% > 0 | Buy |
| M Top | High-1 at UB, High-2 inside bands, II% < 0 | Sell |
| Walk Up Continue | Close > UB AND II% > 0 | Hold long |
| Walk Down Continue | Close < LB AND II% < 0 | Hold short |
| Band Expansion Reversal | Lower band turns up in uptrend | Exit long |

---

## Part 10 — Implementation Files

| File | Contents |
|------|----------|
| `bollinger_strategies.py` | All formulas + signal generators (Python/Pandas) |
| `BOLLINGER_BOOK_DECODED.md` | This reference document |

### Dependencies
```bash
pip install pandas numpy yfinance
```

### Quick Start
```python
import pandas as pd
from bollinger_strategies import generate_all_signals

df = pd.read_csv("your_ohlcv.csv", index_col="date", parse_dates=True)
df.columns = [c.lower() for c in df.columns]  # ensure: open,high,low,close,volume

signals = generate_all_signals(df)
print(signals[signals["m1_signal"] != 0])  # Method I trades
print(signals[signals["w_bottom"]])         # W-Bottom setups
```
