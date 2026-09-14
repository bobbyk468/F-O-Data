"""
Strategy #8: Supertrend Ensemble (Voting System)

Core Idea:
Run 5-7 Supertrend instances with different ATR/Multiplier combinations
simultaneously. At any candle, count how many are Bullish vs Bearish.
Trade only when supermajority (e.g., 5+ out of 7) agree.

Benefits:
- No single parameter set is "correct" — ensemble is robust to parameter sensitivity
- Naturally adapts: Strong trends have all agreeing, choppy markets have disagreement → no trade
- This mirrors how quantitative hedge funds think about signals — ensemble over single signals

Parameter Matrix (7 instances):
ST1: ATR 7,  Mult 1.5
ST2: ATR 7,  Mult 2.5
ST3: ATR 10, Mult 2.0
ST4: ATR 10, Mult 3.0
ST5: ATR 14, Mult 2.0
ST6: ATR 14, Mult 3.0
ST7: ATR 20, Mult 3.0

Vote Thresholds:
- Vote ≥ 6/7 → Strong trend → Full size (100%)
- Vote = 5/7 → Moderate conviction → Half size (50%)
- Vote = 4/7 → Split market → No trade (neutral zone)
- Vote ≤ 3/7 → Bearish majority → Short side (if enabled)
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple
from utils import calculate_supertrend


class EnsembleVotingStrategy:
    """
    Supertrend ensemble with multi-parameter voting system.
    """

    # Ensemble parameter matrix
    PARAMETER_SETS = [
        {'atr_period': 7, 'multiplier': 1.5},
        {'atr_period': 7, 'multiplier': 2.5},
        {'atr_period': 10, 'multiplier': 2.0},
        {'atr_period': 10, 'multiplier': 3.0},
        {'atr_period': 14, 'multiplier': 2.0},
        {'atr_period': 14, 'multiplier': 3.0},
        {'atr_period': 20, 'multiplier': 3.0},
    ]

    def __init__(self, df: pd.DataFrame):
        """
        Initialize ensemble strategy.

        Args:
            df: DataFrame with 'open', 'high', 'low', 'close', 'volume'
        """
        self.df = df.copy()
        self.supertrends = []
        self.vote_history = []

        # Calculate all Supertrend variants
        self._calculate_ensemble()

    def _calculate_ensemble(self):
        """Calculate all Supertrend variants."""
        for params in self.PARAMETER_SETS:
            st, ut, dt = calculate_supertrend(
                self.df,
                atr_period=params['atr_period'],
                multiplier=params['multiplier']
            )
            # Direction: 1 for bullish, -1 for bearish
            direction = np.where(st > 0, 1, -1)
            direction_series = pd.Series(direction, index=self.df.index)

            self.supertrends.append({
                'params': params,
                'supertrend': st,
                'direction': direction_series
            })

    def calculate_vote_count(self, idx: int) -> Tuple[int, int, float]:
        """
        Calculate vote count at given index.

        Returns:
            (bullish_votes, bearish_votes, bullish_percentage)
        """
        if idx >= len(self.df):
            return 0, 0, 0.0

        bullish_votes = 0
        bearish_votes = 0

        for st_data in self.supertrends:
            direction = st_data['direction'].iloc[idx]
            if direction == 1:
                bullish_votes += 1
            else:
                bearish_votes += 1

        total_votes = bullish_votes + bearish_votes
        bullish_pct = bullish_votes / total_votes if total_votes > 0 else 0.0

        return bullish_votes, bearish_votes, bullish_pct

    def get_position_size(self, bullish_votes: int) -> float:
        """Determine position size based on vote count."""
        total = len(self.PARAMETER_SETS)

        if bullish_votes >= 6:
            return 1.0  # 100% full position
        elif bullish_votes == 5:
            return 0.5  # 50% position
        elif bullish_votes == 4:
            return 0.0  # No trade (neutral)
        elif bullish_votes <= 3:
            return -0.5  # Short side (if enabled)
        else:
            return 0.0

    def get_signal(self, idx: int) -> str:
        """Get signal description."""
        bullish_votes, bearish_votes, bullish_pct = self.calculate_vote_count(idx)

        if bullish_votes >= 6:
            return f"STRONG LONG ({bullish_votes}/7 votes)"
        elif bullish_votes == 5:
            return f"MODERATE LONG ({bullish_votes}/7 votes)"
        elif bullish_votes == 4:
            return f"NEUTRAL ({bullish_votes}/7 votes)"
        elif bullish_votes <= 3:
            return f"BEARISH ({bullish_votes}/7 votes)"

    def backtest(self, initial_capital: float = 100000) -> Dict:
        """
        Run full backtest.

        Args:
            initial_capital: Starting capital

        Returns:
            Performance metrics dictionary
        """
        trades = []
        in_trade = False
        entry_price = 0
        entry_votes = 0
        entry_idx = 0
        position_size = 0
        entry_signal = ""

        votes_history = []
        position_sizes = []

        for idx in range(len(self.df)):
            bullish_votes, bearish_votes, bullish_pct = self.calculate_vote_count(idx)
            votes_history.append(bullish_votes)

            pos_size = self.get_position_size(bullish_votes)
            position_sizes.append(pos_size)

            # Entry logic: bullish_votes >= 5 and not in trade
            if not in_trade and bullish_votes >= 5:
                in_trade = True
                entry_price = self.df['close'].iloc[idx]
                entry_votes = bullish_votes
                entry_idx = idx
                position_size = pos_size
                entry_signal = self.get_signal(idx)

            # Exit logic: votes drop to 3 or less, or votes flip significantly
            elif in_trade:
                if bullish_votes <= 3:  # Bearish conviction
                    exit_price = self.df['close'].iloc[idx]
                    profit = (exit_price - entry_price) / entry_price * position_size
                    trade = {
                        'entry_idx': entry_idx,
                        'exit_idx': idx,
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'entry_votes': entry_votes,
                        'exit_votes': bullish_votes,
                        'entry_signal': entry_signal,
                        'exit_signal': self.get_signal(idx),
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

            # Group by entry conviction
            strong_trades = [t for t in trades if t['entry_votes'] >= 6]
            moderate_trades = [t for t in trades if t['entry_votes'] == 5]

            if strong_trades:
                strong_profits = [t['profit_pct'] for t in strong_trades]
                metrics['strong_entry_trades'] = len(strong_trades)
                metrics['strong_entry_win_rate'] = len([p for p in strong_profits if p > 0]) / len(strong_profits)
                metrics['strong_entry_avg_profit'] = np.mean(strong_profits)

            if moderate_trades:
                moderate_profits = [t['profit_pct'] for t in moderate_trades]
                metrics['moderate_entry_trades'] = len(moderate_trades)
                metrics['moderate_entry_win_rate'] = len([p for p in moderate_profits if p > 0]) / len(moderate_profits)
                metrics['moderate_entry_avg_profit'] = np.mean(moderate_profits)

        # Add vote statistics
        metrics['avg_votes'] = np.mean(votes_history) if votes_history else 0
        metrics['vote_distribution'] = self._get_vote_distribution(votes_history)
        metrics['votes_history'] = votes_history
        metrics['position_sizes'] = position_sizes

        return metrics

    def _get_vote_distribution(self, votes: List[int]) -> Dict:
        """Get distribution of votes across 7-vote spectrum."""
        dist = {}
        for i in range(8):
            count = sum(1 for v in votes if v == i)
            dist[f'{i}_votes'] = count

        return dist

    def get_vote_analysis(self, idx: int) -> Dict:
        """
        Get detailed vote analysis at given index.
        """
        if idx >= len(self.df):
            return {}

        bullish_votes, bearish_votes, bullish_pct = self.calculate_vote_count(idx)

        analysis = {
            'idx': idx,
            'datetime': self.df.index[idx],
            'bullish_votes': bullish_votes,
            'bearish_votes': bearish_votes,
            'bullish_pct': bullish_pct,
            'position_size': self.get_position_size(bullish_votes),
            'signal': self.get_signal(idx),
            'price': self.df['close'].iloc[idx],
            'individual_votes': []
        }

        # Detail each ST instance
        for i, st_data in enumerate(self.supertrends):
            direction = st_data['direction'].iloc[idx]
            vote = "BULL" if direction == 1 else "BEAR"
            params = st_data['params']
            analysis['individual_votes'].append({
                'instance': i + 1,
                'atr_period': params['atr_period'],
                'multiplier': params['multiplier'],
                'vote': vote,
                'supertrend_value': st_data['supertrend'].iloc[idx]
            })

        return analysis

    def print_vote_details(self, idx: int):
        """Print detailed vote breakdown for given index."""
        analysis = self.get_vote_analysis(idx)

        if not analysis:
            print("Invalid index")
            return

        print("\n" + "=" * 70)
        print(f"ENSEMBLE VOTE ANALYSIS - {analysis['datetime']}")
        print("=" * 70)
        print(f"Price: {analysis['price']:.2f}")
        print(f"Signal: {analysis['signal']}")
        print(f"Position Size: {analysis['position_size']:.0%}")
        print(f"\nVote Breakdown ({analysis['bullish_votes']}/{len(self.PARAMETER_SETS)}):")
        print("-" * 70)
        print(f"{'#':<4} {'ATR':<8} {'Mult':<8} {'Vote':<12} {'ST Value':<15}")
        print("-" * 70)

        for vote in analysis['individual_votes']:
            print(
                f"{vote['instance']:<4} {vote['atr_period']:<8} "
                f"{vote['multiplier']:<8} {vote['vote']:<12} {vote['supertrend_value']:<15.2f}"
            )

        print("-" * 70)


def run_backtest_example():
    """
    Example: Run ensemble voting backtest on dummy data.
    """
    # Generate synthetic data
    np.random.seed(42)
    dates = pd.date_range('2023-01-01', periods=2000, freq='15min')
    n = len(dates)

    # Create synthetic OHLCV data with trend
    trend = np.linspace(0, 20, n)
    noise = np.cumsum(np.random.randn(n) * 0.5)
    close = 100 + trend + noise

    high = close + np.abs(np.random.randn(n) * 0.5)
    low = close - np.abs(np.random.randn(n) * 0.5)
    volume = np.random.uniform(1000000, 5000000, n)

    df = pd.DataFrame({
        'open': close,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume
    }, index=dates)

    # Run strategy
    strategy = EnsembleVotingStrategy(df)
    metrics = strategy.backtest()

    print("\n" + "=" * 70)
    print("ENSEMBLE VOTING BACKTEST RESULTS")
    print("=" * 70)
    print(f"Total Trades: {metrics['total_trades']}")

    if metrics['total_trades'] > 0:
        print(f"Win Rate: {metrics['win_rate']:.2%}")
        print(f"Avg Profit per Trade: {metrics['avg_profit']:.2f}%")
        print(f"Total Return: {metrics['total_return']:.2f}%")
        print(f"Max Profit: {metrics['max_profit']:.2f}%")
        print(f"Max Loss: {metrics['max_loss']:.2f}%")
        print(f"Profit Std Dev: {metrics['std_profit']:.2f}%")

        if 'strong_entry_trades' in metrics:
            print(f"\nStrong Entry Trades (6/7 votes): {metrics['strong_entry_trades']}")
            print(f"  Win Rate: {metrics['strong_entry_win_rate']:.2%}")
            print(f"  Avg Profit: {metrics['strong_entry_avg_profit']:.2f}%")

        if 'moderate_entry_trades' in metrics:
            print(f"\nModerate Entry Trades (5/7 votes): {metrics['moderate_entry_trades']}")
            print(f"  Win Rate: {metrics['moderate_entry_win_rate']:.2%}")
            print(f"  Avg Profit: {metrics['moderate_entry_avg_profit']:.2f}%")

    print(f"\nAverage Votes (Bullish): {metrics['avg_votes']:.2f}/7")
    print(f"\nVote Distribution:")
    for vote_level, count in sorted(metrics['vote_distribution'].items()):
        print(f"  {vote_level}: {count} candles")

    # Show recent signals
    print("\n" + "=" * 70)
    print("LATEST SIGNALS")
    print("=" * 70)
    for offset in [0, -10, -20]:
        idx = len(df) + offset
        if 0 <= idx < len(df):
            strategy.print_vote_details(idx)

    return strategy, metrics


if __name__ == "__main__":
    strategy, metrics = run_backtest_example()
