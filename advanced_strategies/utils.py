"""
Shared utilities for advanced Supertrend strategies.
Common indicators, helpers, and backtesting utilities.
"""

import numpy as np
import pandas as pd
from typing import Tuple, Dict, List


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Average True Range.

    Args:
        df: DataFrame with 'high', 'low', 'close' columns
        period: ATR period (default 14)

    Returns:
        Series of ATR values
    """
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())

    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(period).mean()
    return atr


def calculate_supertrend(
    df: pd.DataFrame,
    atr_period: int = 10,
    multiplier: float = 3.0
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Calculate Supertrend indicator.

    Args:
        df: DataFrame with 'high', 'low', 'close' columns
        atr_period: ATR period
        multiplier: ATR multiplier for bands

    Returns:
        (supertrend, uptrend, downtrend) Series
    """
    atr = calculate_atr(df, atr_period)
    hl2 = (df['high'] + df['low']) / 2

    basic_ub = hl2 + multiplier * atr
    basic_lb = hl2 - multiplier * atr

    final_ub = np.zeros(len(df))
    final_lb = np.zeros(len(df))

    for i in range(atr_period, len(df)):
        final_ub[i] = basic_ub.iloc[i] if basic_ub.iloc[i] < final_ub[i-1] or df['close'].iloc[i-1] > final_ub[i-1] else final_ub[i-1]
        final_lb[i] = basic_lb.iloc[i] if basic_lb.iloc[i] > final_lb[i-1] or df['close'].iloc[i-1] < final_lb[i-1] else final_lb[i-1]

    supertrend = np.zeros(len(df))
    direction = np.zeros(len(df))

    for i in range(atr_period, len(df)):
        if supertrend[i-1] == 0:
            direction[i] = 1 if df['close'].iloc[i] <= final_ub[i] else -1
        else:
            direction[i] = direction[i-1]

        supertrend[i] = final_lb[i] if direction[i] == 1 else final_ub[i]

    uptrend = pd.Series(final_lb, index=df.index)
    downtrend = pd.Series(final_ub, index=df.index)
    supertrend = pd.Series(supertrend, index=df.index)
    direction_series = pd.Series(direction, index=df.index)

    return supertrend, uptrend, downtrend


def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Relative Strength Index.
    """
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """
    Calculate Exponential Moving Average.
    """
    return series.ewm(span=period, adjust=False).mean()


def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    """
    Calculate Simple Moving Average.
    """
    return series.rolling(window=period).mean()


def calculate_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Calculate MACD.

    Returns:
        (macd_line, signal_line, histogram)
    """
    ema_fast = calculate_ema(df['close'], fast)
    ema_slow = calculate_ema(df['close'], slow)

    macd_line = ema_fast - ema_slow
    signal_line = calculate_ema(macd_line, signal)
    histogram = macd_line - signal_line

    return macd_line, signal_line, histogram


def calculate_adx(
    df: pd.DataFrame,
    period: int = 14
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Calculate ADX (Average Directional Index).

    Returns:
        (adx, plus_di, minus_di)
    """
    high_diff = df['high'].diff()
    low_diff = -df['low'].diff()

    plus_dm = high_diff.where((high_diff > low_diff) & (high_diff > 0), 0)
    minus_dm = low_diff.where((low_diff > high_diff) & (low_diff > 0), 0)

    atr = calculate_atr(df, period)
    plus_di = 100 * calculate_sma(plus_dm, period) / atr
    minus_di = 100 * calculate_sma(minus_dm, period) / atr

    di_diff = np.abs(plus_di - minus_di)
    di_sum = plus_di + minus_di

    adx = 100 * calculate_sma(di_diff / di_sum, period)

    return adx, plus_di, minus_di


def detect_supertrend_flips(supertrend_series: pd.Series) -> pd.Series:
    """
    Detect Supertrend direction changes (flips).
    Returns 1 for bullish, -1 for bearish, 0 for no flip.
    """
    direction = np.where(supertrend_series > 0, 1, -1)
    direction_shift = direction.shift(1)
    flips = pd.Series(np.where(direction != direction_shift, direction, 0), index=supertrend_series.index)
    return flips


def count_flips_in_window(supertrend_series: pd.Series, window: int = 20) -> pd.Series:
    """
    Count number of Supertrend flips in a rolling window.
    """
    flips = detect_supertrend_flips(supertrend_series)
    flip_count = (flips != 0).rolling(window=window).sum()
    return flip_count


def calculate_volatility_regime(df: pd.DataFrame, atr_period: int = 20, ma_period: int = 100) -> pd.Series:
    """
    Calculate volatility regime ratio (Current ATR / Average ATR).
    """
    atr = calculate_atr(df, atr_period)
    atr_ma = calculate_sma(atr, ma_period)
    ratio = atr / atr_ma
    return ratio


def is_above_ema(df: pd.DataFrame, period: int = 200) -> pd.Series:
    """
    Check if close is above EMA.
    """
    ema = calculate_ema(df['close'], period)
    return df['close'] > ema


def calculate_volume_ratio(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """
    Calculate volume ratio (current volume / average volume).
    """
    volume_ma = calculate_sma(df['volume'], period)
    ratio = df['volume'] / volume_ma
    return ratio


class BacktestMetrics:
    """Calculate backtest performance metrics."""

    @staticmethod
    def calculate_returns(trades: List[Dict]) -> Dict:
        """
        Calculate return metrics from trades list.

        Each trade dict should have:
        - entry_price, exit_price
        - entry_time, exit_time
        - direction (1 for long, -1 for short)
        """
        if not trades:
            return {}

        returns = []
        for trade in trades:
            if trade['direction'] == 1:  # Long
                ret = (trade['exit_price'] - trade['entry_price']) / trade['entry_price']
            else:  # Short
                ret = (trade['entry_price'] - trade['exit_price']) / trade['entry_price']
            returns.append(ret)

        returns = np.array(returns)

        return {
            'total_trades': len(trades),
            'winning_trades': len(returns[returns > 0]),
            'losing_trades': len(returns[returns <= 0]),
            'win_rate': len(returns[returns > 0]) / len(returns) if len(returns) > 0 else 0,
            'total_return': returns.sum(),
            'avg_return': returns.mean(),
            'std_return': returns.std(),
            'sharpe_ratio': (returns.mean() / returns.std() * np.sqrt(252)) if returns.std() > 0 else 0,
            'max_return': returns.max(),
            'min_return': returns.min(),
            'max_consecutive_losses': self._max_consecutive_losses(returns),
        }

    @staticmethod
    def _max_consecutive_losses(returns: np.ndarray) -> int:
        """Calculate max consecutive losing trades."""
        losing = returns <= 0
        max_streak = 0
        current_streak = 0

        for is_loss in losing:
            if is_loss:
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0

        return max_streak
