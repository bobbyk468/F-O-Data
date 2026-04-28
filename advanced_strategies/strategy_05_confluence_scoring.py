"""
Strategy #5: Supertrend Confluence Scoring System

Core Idea:
Instead of binary (trade/don't trade), build a 0-100 confluence score that
aggregates multiple Supertrend timeframes and confirming indicators.
Only trade when score crosses a threshold with dynamic position sizing.

Scoring Components:
1. Timeframe Alignment (max 100 points)
   - 5min ST Bullish: +10
   - 15min ST Bullish: +20
   - 1hr ST Bullish: +30
   - Daily ST Bullish: +40

2. Confirming Factors (max 60 bonus points)
   - RSI > 50: +15
   - Price > 200 EMA: +15
   - MACD positive: +10
   - Volume ratio > 1.5x: +10
   - ADX > 25: +10

3. Deduction Factors
   - Recent flip count > 2 in 20 bars: -20
   - Low liquidity/spread issues: -10

Position Sizing by Score:
- Score 80-100: Full position (100%)
- Score 60-79: Half position (50%)
- Score < 60: No trade
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple
from utils import (
    calculate_supertrend, calculate_rsi, calculate_ema, calculate_macd,
    calculate_adx, count_flips_in_window, calculate_volume_ratio,
    BacktestMetrics
)


class ConfluenceScoreStrategy:
    """
    Multi-timeframe Supertrend with confluence scoring system.
    """

    def __init__(
        self,
        primary_df: pd.DataFrame,
        df_5min: pd.DataFrame,
        df_15min: pd.DataFrame,
        df_1hr: pd.DataFrame,
        df_daily: pd.DataFrame,
        atr_period: int = 10,
        multiplier: float = 3.0
    ):
        """
        Initialize strategy with multi-timeframe data.

        Args:
            primary_df: Primary timeframe (e.g., 15min) for entries
            df_5min, df_15min, df_1hr, df_daily: Other timeframes
            atr_period: ATR period for Supertrend
            multiplier: ATR multiplier for Supertrend
        """
        self.primary_df = primary_df.copy()
        self.dfs = {
            '5min': df_5min.copy(),
            '15min': df_15min.copy(),
            '1hr': df_1hr.copy(),
            'daily': df_daily.copy()
        }
        self.atr_period = atr_period
        self.multiplier = multiplier

        # Calculate Supertrend for all timeframes
        self._calculate_supertrends()
        # Calculate confirming indicators on primary
        self._calculate_indicators()

    def _calculate_supertrends(self):
        """Calculate Supertrend for all timeframes."""
        self.supertrends = {}
        for tf_name, df in self.dfs.items():
            st, ut, dt = calculate_supertrend(
                df, self.atr_period, self.multiplier
            )
            self.supertrends[tf_name] = {
                'supertrend': st,
                'uptrend': ut,
                'downtrend': dt,
                'direction': (st > 0).astype(int)  # 1 for bullish, 0 for bearish
            }

    def _calculate_indicators(self):
        """Calculate indicators on primary timeframe."""
        df = self.primary_df

        self.rsi = calculate_rsi(df, 14)
        self.ema200 = calculate_ema(df['close'], 200)
        self.macd_line, self.macd_signal, self.macd_hist = calculate_macd(df)
        self.adx, self.plus_di, self.minus_di = calculate_adx(df, 14)
        self.volume_ratio = calculate_volume_ratio(df, 20)
        self.flip_count = count_flips_in_window(self.supertrends['15min']['direction'], 20)

    def calculate_confluence_score(self, idx: int) -> Tuple[float, Dict]:
        """
        Calculate confluence score at given index.

        Returns:
            (score, components_dict)
        """
        if idx < 200:  # Need enough data for all indicators
            return 0, {}

        score = 0.0
        components = {
            'timeframe_score': 0,
            'confirming_score': 0,
            'deduction_score': 0,
        }

        # 1. TIMEFRAME ALIGNMENT (max 100)
        timeframe_score = 0
        tf_weights = {'5min': 10, '15min': 20, '1hr': 30, 'daily': 40}

        for tf_name, weight in tf_weights.items():
            direction = self.supertrends[tf_name]['direction'].iloc[idx]
            if direction == 1:  # Bullish
                timeframe_score += weight

        components['timeframe_score'] = timeframe_score
        score += timeframe_score

        # 2. CONFIRMING FACTORS (max 60)
        confirming_score = 0

        # RSI > 50
        if self.rsi.iloc[idx] > 50:
            confirming_score += 15

        # Price > 200 EMA
        if self.primary_df['close'].iloc[idx] > self.ema200.iloc[idx]:
            confirming_score += 15

        # MACD positive (histogram positive)
        if self.macd_hist.iloc[idx] > 0:
            confirming_score += 10

        # Volume ratio > 1.5x
        if self.volume_ratio.iloc[idx] > 1.5:
            confirming_score += 10

        # ADX > 25 (strong trend)
        if self.adx.iloc[idx] > 25:
            confirming_score += 10

        components['confirming_score'] = confirming_score
        score += confirming_score

        # 3. DEDUCTIONS
        deductions = 0

        # Recent flips (chop)
        if self.flip_count.iloc[idx] > 2:
            deductions -= 20

        components['deduction_score'] = deductions
        score += deductions

        # Clamp score to 0-100
        score = max(0, min(100, score))

        return score, components

    def get_position_size(self, score: float) -> float:
        """Determine position size based on confluence score."""
        if score >= 80:
            return 1.0  # 100% full position
        elif score >= 60:
            return 0.5  # 50% position
        else:
            return 0.0  # No trade

    def backtest(self, initial_capital: float = 100000) -> Dict:
        """
        Run full backtest on primary timeframe.

        Args:
            initial_capital: Starting capital

        Returns:
            Performance metrics dictionary
        """
        trades = []
        in_trade = False
        entry_price = 0
        entry_score = 0
        entry_idx = 0
        position_size = 0

        scores = []
        position_sizes = []

        for idx in range(200, len(self.primary_df)):
            score, _ = self.calculate_confluence_score(idx)
            scores.append(score)
            pos_size = self.get_position_size(score)
            position_sizes.append(pos_size)

            # Entry logic: score >= 80 and ST bullish and not in trade
            if not in_trade and pos_size > 0:
                st_direction = self.supertrends['15min']['direction'].iloc[idx]
                if st_direction == 1:  # Bullish
                    in_trade = True
                    entry_price = self.primary_df['close'].iloc[idx]
                    entry_score = score
                    entry_idx = idx
                    position_size = pos_size

            # Exit logic: ST flips bearish or score drops below 40
            elif in_trade:
                st_direction = self.supertrends['15min']['direction'].iloc[idx]
                if st_direction == 0 or score < 40:  # Bearish or score collapse
                    exit_price = self.primary_df['close'].iloc[idx]
                    profit = (exit_price - entry_price) / entry_price * position_size
                    trade = {
                        'entry_idx': entry_idx,
                        'exit_idx': idx,
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'entry_score': entry_score,
                        'position_size': position_size,
                        'direction': 1,
                        'profit_pct': profit * 100
                    }
                    trades.append(trade)
                    in_trade = False

        # Calculate metrics
        metrics = {
            'trades': trades,
            'total_trades': len(trades),
        }

        if trades:
            profits = [t['profit_pct'] for t in trades]
            metrics['win_rate'] = len([p for p in profits if p > 0]) / len(profits)
            metrics['avg_profit'] = np.mean(profits)
            metrics['total_return'] = np.sum(profits)
            metrics['max_profit'] = max(profits)
            metrics['max_loss'] = min(profits)
            metrics['std_profit'] = np.std(profits)

        # Add score statistics
        metrics['avg_score'] = np.mean(scores) if scores else 0
        metrics['max_score'] = max(scores) if scores else 0
        metrics['scores'] = scores
        metrics['position_sizes'] = position_sizes

        return metrics

    def get_signal_analysis(self, idx: int) -> Dict:
        """
        Get detailed signal analysis at given index.
        Useful for understanding what drives the score.
        """
        if idx < 200:
            return {}

        score, components = self.calculate_confluence_score(idx)

        analysis = {
            'idx': idx,
            'datetime': self.primary_df.index[idx],
            'score': score,
            'position_size': self.get_position_size(score),
            'components': components,
            'supertrend_states': {
                tf: self.supertrends[tf]['direction'].iloc[idx]
                for tf in self.supertrends
            },
            'rsi': self.rsi.iloc[idx],
            'price': self.primary_df['close'].iloc[idx],
            'ema200': self.ema200.iloc[idx],
            'macd_hist': self.macd_hist.iloc[idx],
            'adx': self.adx.iloc[idx],
            'volume_ratio': self.volume_ratio.iloc[idx],
            'flip_count': self.flip_count.iloc[idx],
        }

        return analysis


def run_backtest_example():
    """
    Example: Run confluence scoring backtest on dummy data.
    """
    # Generate synthetic data
    np.random.seed(42)
    dates = pd.date_range('2023-01-01', periods=1000, freq='15min')
    n = len(dates)

    # Create synthetic OHLCV data
    close = 100 + np.cumsum(np.random.randn(n) * 0.5)
    high = close + np.abs(np.random.randn(n) * 0.3)
    low = close - np.abs(np.random.randn(n) * 0.3)
    volume = np.random.uniform(1000000, 5000000, n)

    df_15min = pd.DataFrame({
        'open': close,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume
    }, index=dates)

    # Resample for other timeframes
    df_5min = df_15min.resample('5min').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    df_1hr = df_15min.resample('1H').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    df_daily = df_15min.resample('D').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    # Run strategy
    strategy = ConfluenceScoreStrategy(
        df_15min,
        df_5min,
        df_15min,
        df_1hr,
        df_daily,
        atr_period=10,
        multiplier=3.0
    )

    metrics = strategy.backtest()

    print("\n" + "=" * 60)
    print("CONFLUENCE SCORING BACKTEST RESULTS")
    print("=" * 60)
    print(f"Total Trades: {metrics['total_trades']}")
    if metrics['total_trades'] > 0:
        print(f"Win Rate: {metrics['win_rate']:.2%}")
        print(f"Avg Profit per Trade: {metrics['avg_profit']:.2f}%")
        print(f"Total Return: {metrics['total_return']:.2f}%")
        print(f"Max Profit: {metrics['max_profit']:.2f}%")
        print(f"Max Loss: {metrics['max_loss']:.2f}%")
        print(f"Profit Std Dev: {metrics['std_profit']:.2f}%")

    print(f"\nAverage Confluence Score: {metrics['avg_score']:.1f}")
    print(f"Max Score: {metrics['max_score']:.1f}")

    # Show recent signal
    print("\n" + "=" * 60)
    print("LATEST SIGNAL ANALYSIS")
    print("=" * 60)
    latest_analysis = strategy.get_signal_analysis(len(df_15min) - 1)
    if latest_analysis:
        print(f"Score: {latest_analysis['score']:.1f}")
        print(f"Position Size: {latest_analysis['position_size']:.0%}")
        print(f"Timeframe Alignment: {latest_analysis['components']['timeframe_score']:.0f}")
        print(f"Confirming Factors: {latest_analysis['components']['confirming_score']:.0f}")
        print(f"Deductions: {latest_analysis['components']['deduction_score']:.0f}")

    return strategy, metrics


if __name__ == "__main__":
    strategy, metrics = run_backtest_example()
