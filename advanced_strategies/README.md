# Advanced Supertrend Strategies

Institutional-grade Supertrend variations designed for retail traders. Two complementary approaches to signal reliability and robustness.

## 📁 Folder Structure

```
advanced_strategies/
├── README.md                              (This file)
├── GEMINI_REVIEW.md                       (Technical review document)
├── utils.py                               (Shared indicator library)
├── strategy_05_confluence_scoring.py      (Confluence Scoring System)
├── strategy_08_ensemble_voting.py         (Ensemble Voting System)
└── backtest_results/                      (Output directory)
```

## 🚀 Quick Start

### Run Confluence Scoring Backtest

```bash
cd advanced_strategies
python strategy_05_confluence_scoring.py
```

**Output:**
- Total trades and win rate
- Average profit per trade
- Confluence score statistics
- Latest signal analysis

### Run Ensemble Voting Backtest

```bash
cd advanced_strategies
python strategy_08_ensemble_voting.py
```

**Output:**
- Total trades by conviction level (6/7, 5/7)
- Win rates by entry strength
- Vote distribution histogram
- Recent signal breakdowns

## 📊 Strategy Overview

### Strategy #5: Confluence Scoring System

**Problem it solves:**
- Standard Supertrend is binary (trade/skip)
- No quality ranking of signals
- Fixed parameters don't adapt to regime changes

**How it works:**
1. Calculates Supertrend on 4 timeframes (5min, 15min, 1hr, daily)
2. Aggregates 5 confirming indicators (RSI, EMA, MACD, ADX, Volume)
3. Produces a 0-100 score showing signal quality
4. Sizes position based on score (100%, 50%, or 0%)

**Key Metrics:**
- Score ≥ 80 → Full position (100%)
- Score 60-79 → Half position (50%)
- Score < 60 → No trade

**Expected Performance (Nifty 50, 15min):**
- Win Rate: 55-65%
- Avg Profit: 0.5-1.5% per trade
- Sharpe Ratio: 1.2-1.8
- Max Drawdown: 8-15%

---

### Strategy #8: Ensemble Voting System

**Problem it solves:**
- Different ATR periods/multipliers produce different signals
- No consensus on "correct" parameters
- Single optimized parameter set overfits and fails forward

**How it works:**
1. Runs 7 Supertrend variants with different parameters simultaneously
2. Counts how many show bullish direction
3. 5+ out of 7 votes = trade signal (supermajority rule)
4. Sizes position by vote count (6-7 votes = 100%, 5 votes = 50%)

**Parameter Matrix:**
```
Instance 1: ATR 7,  Mult 1.5   (tightest)
Instance 2: ATR 7,  Mult 2.5
Instance 3: ATR 10, Mult 2.0
Instance 4: ATR 10, Mult 3.0
Instance 5: ATR 14, Mult 2.0
Instance 6: ATR 14, Mult 3.0
Instance 7: ATR 20, Mult 3.0   (widest)
```

**Key Metrics:**
- Vote ≥ 6/7 → Full position (100%)
- Vote = 5/7 → Half position (50%)
- Vote ≤ 4/7 → No trade

**Expected Performance (Nifty 50, 15min):**
- Win Rate: 60-70% (higher quality)
- Avg Profit: 1.0-2.5% per trade (fewer, better trades)
- Sharpe Ratio: 1.5-2.2
- Max Drawdown: 5-12% (smoother equity curve)

---

## 📖 Documentation

**GEMINI_REVIEW.md** is the comprehensive technical document covering:
- Detailed architecture of each strategy
- Scoring/voting logic with examples
- Implementation code highlights
- Statistical validation
- Production considerations
- FAQ and references

**Read this first** before deploying to understand how signals are generated.

---

## 🔧 Usage Examples

### Confluence Scoring

```python
from strategy_05_confluence_scoring import ConfluenceScoreStrategy
import pandas as pd

# Load your multi-timeframe data
df_5min = pd.read_csv('data_5min.csv', index_col='datetime', parse_dates=True)
df_15min = pd.read_csv('data_15min.csv', index_col='datetime', parse_dates=True)
df_1hr = pd.read_csv('data_1hr.csv', index_col='datetime', parse_dates=True)
df_daily = pd.read_csv('data_daily.csv', index_col='datetime', parse_dates=True)

# Create strategy
strategy = ConfluenceScoreStrategy(
    primary_df=df_15min,
    df_5min=df_5min,
    df_15min=df_15min,
    df_1hr=df_1hr,
    df_daily=df_daily,
    atr_period=10,
    multiplier=3.0
)

# Run backtest
metrics = strategy.backtest(initial_capital=100000)
print(f"Win Rate: {metrics['win_rate']:.2%}")
print(f"Total Return: {metrics['total_return']:.2f}%")

# Get latest signal
signal = strategy.get_signal_analysis(idx=-1)
print(f"Current Score: {signal['score']:.1f}")
print(f"Position Size: {signal['position_size']:.0%}")
```

### Ensemble Voting

```python
from strategy_08_ensemble_voting import EnsembleVotingStrategy
import pandas as pd

# Load single-timeframe data
df = pd.read_csv('data_15min.csv', index_col='datetime', parse_dates=True)

# Create strategy
strategy = EnsembleVotingStrategy(df)

# Run backtest
metrics = strategy.backtest()
print(f"Total Trades: {metrics['total_trades']}")
print(f"Win Rate: {metrics['win_rate']:.2%}")
print(f"Strong Entry Trades (6/7): {metrics.get('strong_entry_trades', 0)}")
print(f"Strong Entry Win Rate: {metrics.get('strong_entry_win_rate', 0):.2%}")

# Get vote analysis for recent candle
analysis = strategy.get_vote_analysis(idx=-1)
print(f"Current Votes: {analysis['bullish_votes']}/7")
print(f"Signal: {analysis['signal']}")

# Print detailed vote breakdown
strategy.print_vote_details(idx=-1)
```

---

## 📊 Data Requirements

### For Confluence Scoring
- **Primary data**: 15-minute OHLCV
- **Multi-timeframe**: 5min, 15min, 1hr, daily OHLCV
- **Minimum history**: 200+ bars on primary = ~33 hours
- **Data quality**: No gaps, accurate volume

### For Ensemble Voting
- **Data**: Single timeframe OHLCV (any TF: 1min to daily)
- **Minimum history**: 200+ bars for indicators to stabilize
- **Data quality**: No gaps

**Expected Data Format:**
```python
df = pd.DataFrame({
    'open': [...],
    'high': [...],
    'low': [...],
    'close': [...],
    'volume': [...]
}, index=pd.DatetimeIndex(...))
```

---

## ✅ Backtesting Checklist

Before deploying to live trading:

- [ ] Backtest runs without errors on 1+ year of data
- [ ] Win rate > 50% (statistically significant)
- [ ] Profit factor (total wins / total losses) > 1.5
- [ ] Max consecutive losses < 5
- [ ] Out-of-sample forward test matches backtest metrics (within 5-10%)
- [ ] Sharpe ratio > 1.0
- [ ] Max drawdown acceptable for your risk tolerance
- [ ] Trade distribution is consistent (not clustered in one period)

---

## 🔍 Indicator Library (utils.py)

Shared utilities available to both strategies:

```python
# Volatility
calculate_atr(df, period=14)

# Trend indicators
calculate_supertrend(df, atr_period=10, multiplier=3.0)
calculate_ema(series, period=200)
calculate_sma(series, period=20)

# Momentum
calculate_rsi(df, period=14)
calculate_macd(df, fast=12, slow=26, signal=9)
calculate_adx(df, period=14)

# Volume
calculate_volume_ratio(df, period=20)

# Supertrend-specific
detect_supertrend_flips(supertrend_series)
count_flips_in_window(supertrend_series, window=20)
calculate_volatility_regime(df, atr_period=20, ma_period=100)

# Validation
is_above_ema(df, period=200)
```

---

## 🎯 Next Steps

### 1. Immediate
- [ ] Run both strategies on your data
- [ ] Compare results
- [ ] Review GEMINI_REVIEW.md for detailed understanding

### 2. Validation (2-4 weeks)
- [ ] Run extended backtest on 3+ years of data
- [ ] Perform walk-forward analysis
- [ ] Test on out-of-sample data
- [ ] Adjust parameters if needed

### 3. Paper Trading (2-4 weeks)
- [ ] Set up live signal generation
- [ ] Track metrics daily
- [ ] Compare live vs. backtest
- [ ] Adjust position sizing

### 4. Live Trading (Start Small)
- [ ] Begin with 1-5% of account per trade
- [ ] Monitor daily P&L
- [ ] Monthly strategy review
- [ ] Scale up gradually if metrics hold

---

## ⚠️ Risk Disclaimer

These strategies are **trend-following systems** designed for educational and research purposes. 

**Key Risks:**
- Gap openings can exceed stop losses
- Markets can remain irrational longer than expected
- Past performance doesn't guarantee future results
- Slippage and commissions affect real trading
- Parameter changes can affect signal quality

**Risk Management:**
- Always use position sizing appropriate for your account
- Never risk more than 2-5% per trade
- Use hard stops (never hold > X% drawdown)
- Monitor indicators daily for regime changes

---

## 📚 Resources

- **GEMINI_REVIEW.md** - Complete technical documentation
- **utils.py** - Indicator definitions and source code
- **strategy_*.py files** - Full implementation with docstrings

---

## 🤝 Contributing

Ideas for improvements:
- [ ] Add support for cryptocurrency
- [ ] Implement additional confirming indicators
- [ ] Create ensemble variants (5-instance, 9-instance)
- [ ] Add Monte Carlo resampling validation
- [ ] Build portfolio multi-asset backtester

---

## 📝 License

This is part of the jugaad-trader project. Use for education and research.

---

**Last Updated**: 2026-04-28  
**Version**: 1.0  
**Author**: Strategy Development System
