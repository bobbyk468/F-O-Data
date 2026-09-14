import pandas as pd
import numpy as np

# Load the signals CSV
df = pd.read_csv('/Users/brahmajikatragadda/Desktop/Zerodha_Data/jugaad-trader/Bollinger/nifty_bb_bottom_w_signals.csv')

# We only care about signals
trades = df[df['signals'] != 0].copy()

if len(trades) == 0:
    print("No trades found.")
    exit()

print(f"Total signals: {len(trades)}")

pos = 0
entry_price = 0
entry_date = None

pnl_pcts = []
pnl_points = []
trade_records = []

for idx, row in trades.iterrows():
    if row['signals'] == 1 and pos == 0:
        pos = 1
        entry_price = float(row['close'])
        entry_date = row['date']
    elif row['signals'] == -2 and pos == 0:
        pos = -1
        entry_price = float(row['close'])
        entry_date = row['date']
    elif row['signals'] == -1 and pos == 1:
        pos = 0
        exit_price = float(row['close'])
        exit_date = row['date']
        
        pnl = exit_price - entry_price
        pnl_pct = (pnl / entry_price) * 100
        
        pnl_points.append(pnl)
        pnl_pcts.append(pnl_pct)
        
        trade_records.append({
            'Type': 'LONG',
            'Entry Date': entry_date,
            'Entry Price': entry_price,
            'Exit Date': exit_date,
            'Exit Price': exit_price,
            'PnL (Points)': pnl,
            'PnL (%)': pnl_pct
        })
    elif row['signals'] == 2 and pos == -1:
        pos = 0
        exit_price = float(row['close'])
        exit_date = row['date']
        
        pnl = entry_price - exit_price # Short PnL
        pnl_pct = (pnl / entry_price) * 100
        
        pnl_points.append(pnl)
        pnl_pcts.append(pnl_pct)
        
        trade_records.append({
            'Type': 'SHORT',
            'Entry Date': entry_date,
            'Entry Price': entry_price,
            'Exit Date': exit_date,
            'Exit Price': exit_price,
            'PnL (Points)': pnl,
            'PnL (%)': pnl_pct
        })

if not trade_records:
    print("No complete trades found.")
    exit()

trades_df = pd.DataFrame(trade_records)
print("\n--- Trade List ---")
print(trades_df.to_string(index=False))

wins = [p for p in pnl_pcts if p > 0]
losses = [p for p in pnl_pcts if p <= 0]

win_rate = len(wins) / len(pnl_pcts) * 100 if len(pnl_pcts) > 0 else 0

print("\n--- Strategy Statistics ---")
print(f"Total Trades: {len(pnl_pcts)}")
print(f"Win Rate: {win_rate:.2f}%")
print(f"Total PnL (Points): {sum(pnl_points):.2f}")
print(f"Average PnL per trade (%): {np.mean(pnl_pcts):.2f}%")
if len(wins) > 0:
    print(f"Average Win (%): {np.mean(wins):.2f}%")
if len(losses) > 0:
    print(f"Average Loss (%): {np.mean(losses):.2f}%")
print(f"Max Drawdown / Worst Trade (%): {min(pnl_pcts):.2f}%")
print(f"Best Trade (%): {max(pnl_pcts):.2f}%")

long_trades = trades_df[trades_df['Type'] == 'LONG']
short_trades = trades_df[trades_df['Type'] == 'SHORT']

print("\n--- Breakdown ---")
if not long_trades.empty:
    long_win_rate = len(long_trades[long_trades['PnL (%)'] > 0]) / len(long_trades) * 100
    print(f"LONG Trades: {len(long_trades)}, Win Rate: {long_win_rate:.2f}%, Avg PnL: {long_trades['PnL (%)'].mean():.2f}%")
if not short_trades.empty:
    short_win_rate = len(short_trades[short_trades['PnL (%)'] > 0]) / len(short_trades) * 100
    print(f"SHORT Trades: {len(short_trades)}, Win Rate: {short_win_rate:.2f}%, Avg PnL: {short_trades['PnL (%)'].mean():.2f}%")