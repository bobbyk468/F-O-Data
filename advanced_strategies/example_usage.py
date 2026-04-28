"""
Example: Using both Confluence Scoring and Ensemble Voting strategies.

This script demonstrates:
1. Loading data (synthetic example)
2. Running both strategies
3. Comparing results
4. Visualizing key insights
"""

import numpy as np
import pandas as pd
from typing import Dict
from strategy_05_confluence_scoring import ConfluenceScoreStrategy
from strategy_08_ensemble_voting import EnsembleVotingStrategy


def generate_synthetic_data(
    start_date: str = '2023-01-01',
    periods: int = 4000,
    freq: str = '15min',
    trend: bool = True,
    volatility: float = 0.5
) -> Dict[str, pd.DataFrame]:
    """
    Generate realistic synthetic OHLCV data for backtesting.

    Args:
        start_date: Start date for the data
        periods: Number of candles
        freq: Frequency (15min, 1H, etc.)
        trend: If True, add uptrend; if False, sideways
        volatility: Volatility parameter (0.1 to 2.0)

    Returns:
        Dict with DataFrames for different timeframes
    """
    np.random.seed(42)

    # Generate dates
    dates = pd.date_range(start_date, periods=periods, freq=freq)
    n = len(dates)

    # Generate price with trend + noise
    if trend:
        drift = np.linspace(0, 50, n)  # Uptrend
    else:
        drift = np.sin(np.linspace(0, 4 * np.pi, n)) * 20  # Oscillating

    noise = np.cumsum(np.random.randn(n) * volatility)
    close = 100 + drift + noise

    # Generate OHLC from close
    high = close + np.abs(np.random.randn(n) * volatility * 0.5)
    low = close - np.abs(np.random.randn(n) * volatility * 0.5)
    open_price = np.roll(close, 1)
    volume = np.random.uniform(1000000, 5000000, n)

    df = pd.DataFrame({
        'open': open_price,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume
    }, index=dates)

    # Resample for different timeframes
    dfs = {
        '15min': df.copy(),
        '5min': df.resample('5min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna(),
        '1H': df.resample('1H').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna(),
        'D': df.resample('D').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna(),
    }

    return dfs


def run_single_strategy_comparison():
    """
    Generate data and run both strategies independently.
    """
    print("\n" + "=" * 80)
    print("SINGLE STRATEGY COMPARISON")
    print("=" * 80)

    # Generate trending market data
    print("\nGenerating synthetic data (trending market)...")
    dfs = generate_synthetic_data(
        periods=2000,
        trend=True,
        volatility=0.5
    )

    print(f"Data generated: {len(dfs['15min'])} candles")

    # Strategy 1: Confluence Scoring
    print("\n" + "-" * 80)
    print("STRATEGY #5: CONFLUENCE SCORING")
    print("-" * 80)

    confluence = ConfluenceScoreStrategy(
        primary_df=dfs['15min'],
        df_5min=dfs['5min'],
        df_15min=dfs['15min'],
        df_1hr=dfs['1H'],
        df_daily=dfs['D'],
        atr_period=10,
        multiplier=3.0
    )

    confluence_metrics = confluence.backtest()

    print(f"Total Trades: {confluence_metrics['total_trades']}")
    if confluence_metrics['total_trades'] > 0:
        print(f"Win Rate: {confluence_metrics['win_rate']:.2%}")
        print(f"Avg Profit: {confluence_metrics['avg_profit']:.2f}%")
        print(f"Total Return: {confluence_metrics['total_return']:.2f}%")
        print(f"Max Profit: {confluence_metrics['max_profit']:.2f}%")
        print(f"Max Loss: {confluence_metrics['max_loss']:.2f}%")
        print(f"Std Dev: {confluence_metrics['std_profit']:.2f}%")
    print(f"Avg Score: {confluence_metrics['avg_score']:.1f}")
    print(f"Max Score: {confluence_metrics['max_score']:.1f}")

    # Strategy 2: Ensemble Voting
    print("\n" + "-" * 80)
    print("STRATEGY #8: ENSEMBLE VOTING")
    print("-" * 80)

    ensemble = EnsembleVotingStrategy(df=dfs['15min'])
    ensemble_metrics = ensemble.backtest()

    print(f"Total Trades: {ensemble_metrics['total_trades']}")
    if ensemble_metrics['total_trades'] > 0:
        print(f"Win Rate: {ensemble_metrics['win_rate']:.2%}")
        print(f"Avg Profit: {ensemble_metrics['avg_profit']:.2f}%")
        print(f"Total Return: {ensemble_metrics['total_return']:.2f}%")
        print(f"Max Profit: {ensemble_metrics['max_profit']:.2f}%")
        print(f"Max Loss: {ensemble_metrics['max_loss']:.2f}%")
        print(f"Std Dev: {ensemble_metrics['std_profit']:.2f}%")

        if 'strong_entry_trades' in ensemble_metrics:
            print(f"\nStrong Entries (6/7): {ensemble_metrics['strong_entry_trades']}")
            print(f"  Win Rate: {ensemble_metrics['strong_entry_win_rate']:.2%}")
            print(f"  Avg Profit: {ensemble_metrics['strong_entry_avg_profit']:.2f}%")

        if 'moderate_entry_trades' in ensemble_metrics:
            print(f"Moderate Entries (5/7): {ensemble_metrics['moderate_entry_trades']}")
            print(f"  Win Rate: {ensemble_metrics['moderate_entry_win_rate']:.2%}")
            print(f"  Avg Profit: {ensemble_metrics['moderate_entry_avg_profit']:.2f}%")

    print(f"Avg Votes: {ensemble_metrics['avg_votes']:.2f}/7")

    return confluence, ensemble, confluence_metrics, ensemble_metrics


def compare_strategies(
    confluence_metrics: Dict,
    ensemble_metrics: Dict
):
    """
    Compare two strategies side-by-side.
    """
    print("\n" + "=" * 80)
    print("HEAD-TO-HEAD COMPARISON")
    print("=" * 80)

    comparison = pd.DataFrame({
        'Confluence': [
            confluence_metrics['total_trades'],
            f"{confluence_metrics.get('win_rate', 0):.2%}",
            f"{confluence_metrics.get('avg_profit', 0):.2f}%",
            f"{confluence_metrics.get('total_return', 0):.2f}%",
            f"{confluence_metrics.get('std_profit', 0):.2f}%",
        ],
        'Ensemble': [
            ensemble_metrics['total_trades'],
            f"{ensemble_metrics.get('win_rate', 0):.2%}",
            f"{ensemble_metrics.get('avg_profit', 0):.2f}%",
            f"{ensemble_metrics.get('total_return', 0):.2f}%",
            f"{ensemble_metrics.get('std_profit', 0):.2f}%",
        ]
    }, index=[
        'Total Trades',
        'Win Rate',
        'Avg Profit/Trade',
        'Total Return',
        'Profit Std Dev'
    ])

    print(comparison.to_string())

    # Analysis
    print("\n" + "-" * 80)
    print("ANALYSIS")
    print("-" * 80)

    c_total = confluence_metrics['total_trades']
    e_total = ensemble_metrics['total_trades']

    if c_total > 0 and e_total > 0:
        trade_diff = (c_total - e_total) / e_total * 100
        c_wr = confluence_metrics.get('win_rate', 0)
        e_wr = ensemble_metrics.get('win_rate', 0)
        wr_diff = (e_wr - c_wr) * 100

        print(f"• Confluence takes {trade_diff:+.0f}% more trades than Ensemble")
        print(f"• Ensemble has {wr_diff:+.1f}% higher win rate")

        if c_total > 0:
            print(f"• Confluence avg trade duration: N/A (see backtest)")
        if e_total > 0:
            print(f"• Ensemble focuses on high-conviction entries (5+ votes)")

        c_return = confluence_metrics.get('total_return', 0)
        e_return = ensemble_metrics.get('total_return', 0)

        print(f"\n• Total Return - Confluence: {c_return:.2f}%")
        print(f"• Total Return - Ensemble:  {e_return:.2f}%")

        if e_return > c_return:
            print(f"  → Ensemble outperforms by quality (fewer, better trades)")
        else:
            print(f"  → Confluence outperforms by quantity (more trades)")


def analyze_signal_quality(
    strategy: EnsembleVotingStrategy,
    df: pd.DataFrame
):
    """
    Analyze signal quality at key points.
    """
    print("\n" + "=" * 80)
    print("ENSEMBLE SIGNAL QUALITY ANALYSIS")
    print("=" * 80)

    # Find indices with different vote counts
    sample_indices = [
        max(200, len(df) - 100),  # Recent data
        len(df) - 50,
        len(df) - 10,
        len(df) - 1,  # Latest
    ]

    for idx in sample_indices:
        if 200 <= idx < len(df):
            strategy.print_vote_details(idx)


def run_comprehensive_test():
    """
    Run comprehensive test of both strategies.
    """
    print("\n" + "=" * 80)
    print("COMPREHENSIVE ADVANCED SUPERTREND STRATEGY TEST")
    print("=" * 80)

    # Single strategy comparison
    confluence, ensemble, conf_metrics, ens_metrics = run_single_strategy_comparison()

    # Head-to-head
    compare_strategies(conf_metrics, ens_metrics)

    # Signal quality
    dfs = generate_synthetic_data(periods=2000, trend=True)
    analyze_signal_quality(ensemble, dfs['15min'])

    # Recommendations
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print("""
1. Use CONFLUENCE SCORING if:
   - You have multi-timeframe data
   - You want detailed signal analysis
   - You prefer more frequent trading
   - Your strategy needs adaptive position sizing

2. Use ENSEMBLE VOTING if:
   - You want simpler implementation
   - You only have single-timeframe data
   - You prefer high-conviction entries
   - You want maximum robustness to parameter changes

3. HYBRID APPROACH:
   - Use Ensemble for signal generation (buy/sell)
   - Use Confluence for position sizing refinement
   - Combine for best of both worlds
    """)

    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    run_comprehensive_test()
