"""
Bollinger bottom-W on NIFTY OHLC files.

Your CSVs are regular candlesticks. The strategy logic runs on **Heikin-Ashi close**
by default (computed from open/high/low/close). Pass --regular-close to use raw close.
"""
import argparse
import os
import copy
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

DEFAULT_CSV = "/Users/brahmajikatragadda/Desktop/Zerodha_Data/jugaad-trader/data/indices/eod/nifty_50_eod.csv"
OUT_DIR = os.path.dirname(os.path.abspath(__file__))


def compute_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    """Build Heikin-Ashi OHLC from standard OHLC (same timestamps)."""
    o = df["open"].astype(float).to_numpy()
    h = df["high"].astype(float).to_numpy()
    l = df["low"].astype(float).to_numpy()
    c = df["close"].astype(float).to_numpy()
    n = len(df)
    ha_o = np.empty(n)
    ha_h = np.empty(n)
    ha_l = np.empty(n)
    ha_c = np.empty(n)

    ha_c[0] = (o[0] + h[0] + l[0] + c[0]) / 4.0
    ha_o[0] = (o[0] + c[0]) / 2.0
    ha_h[0] = max(h[0], ha_o[0], ha_c[0])
    ha_l[0] = min(l[0], ha_o[0], ha_c[0])
    for i in range(1, n):
        ha_c[i] = (o[i] + h[i] + l[i] + c[i]) / 4.0
        ha_o[i] = (ha_o[i - 1] + ha_c[i - 1]) / 2.0
        ha_h[i] = max(h[i], ha_o[i], ha_c[i])
        ha_l[i] = min(l[i], ha_o[i], ha_c[i])

    out = df.copy()
    out["ha_open"] = ha_o
    out["ha_high"] = ha_h
    out["ha_low"] = ha_l
    out["ha_close"] = ha_c
    return out


def bollinger_bands(df):
    data=copy.deepcopy(df)
    data['std']=data['price'].rolling(window=20,min_periods=20).std()
    data['mid band']=data['price'].rolling(window=20,min_periods=20).mean()
    data['upper band']=data['mid band']+2*data['std']
    data['lower band']=data['mid band']-2*data['std']
    return data

def signal_generation(data,method):
    period=75
    
    df=method(data)
    df['signals']=0
    df['cumsum']=0
    df['coordinates']=''
    
    # Pre-calculate median bandwidth for contraction logic
    mid = df["mid band"].replace(0, np.nan)
    df["bandwidth"] = (df["upper band"] - df["lower band"]) / mid
    med_bw = df["bandwidth"].rolling(100, min_periods=20).median()
    
    for i in range(period,len(df)):
        moveon=False
        threshold=0.0
        
        # dynamic alpha/beta based on price
        # 0.5% tolerance for touching bands
        alpha = df['mid band'][i] * 0.005 
        
        # exit when bandwidth is less than 60% of median bandwidth
        beta_bw = med_bw.iloc[i] * 0.6 if pd.notna(med_bw.iloc[i]) else np.inf
        
        if (df['price'][i]>df['upper band'][i]) and (df['cumsum'][i]==0):
            for j in range(i,i-period,-1):                
                if (np.abs(df['mid band'][j]-df['price'][j])<alpha) and \
                (np.abs(df['mid band'][j]-df['upper band'][i])<alpha):
                    moveon=True
                    break
            
            if moveon==True:
                moveon=False
                for k in range(j,i-period,-1):
                    if (np.abs(df['lower band'][k]-df['price'][k])<alpha):
                        threshold=df['price'][k]
                        moveon=True
                        break
                        
            if moveon==True:
                moveon=False
                for l in range(k,i-period,-1):
                    if (df['mid band'][l]<df['price'][l]):
                        moveon=True
                        break
                    
            if moveon==True:
                moveon=False        
                for m in range(i,j,-1):
                    if (df['price'][m]-df['lower band'][m]<alpha) and \
                    (df['price'][m]>df['lower band'][m]) and \
                    (df['price'][m]<threshold):
                        df.at[i,'signals']=1
                        df.at[i,'coordinates']='%s,%s,%s,%s,%s'%(l,k,j,m,i)
                        df['cumsum']=df['signals'].cumsum()
                        moveon=True
                        break
        
        # Exit logic based on bandwidth contraction
        current_bw = df['bandwidth'].iloc[i]
        if (
            df["cumsum"][i] != 0
            and pd.notna(current_bw)
            and pd.notna(med_bw.iloc[i])
            and (current_bw < beta_bw)
            and (not moveon)
        ):
            df.at[i,'signals']=-1
            df['cumsum']=df['signals'].cumsum()
            
    return df

def plot(new, *, title: str, y_label: str):
    signal_idx = new[new['signals']!=0].index
    if len(signal_idx) < 2:
        print("Not enough signals to plot.")
        return
        
    a, b = list(signal_idx[:2])
    
    newbie=new.iloc[max(0, a-85):min(len(new), b+30)]
    newbie = newbie.copy()
    newbie.set_index(pd.to_datetime(newbie['date']),inplace=True)

    fig=plt.figure(figsize=(10,5))
    ax=fig.add_subplot(111)
    
    ax.plot(newbie["price"], label=y_label)
    ax.fill_between(newbie.index,newbie['lower band'],newbie['upper band'],alpha=0.2,color='#45ADA8')
    ax.plot(newbie['mid band'],linestyle='--',label='moving average',c='#132226')
    
    long_signals = newbie['price'][newbie['signals']==1]
    if not long_signals.empty:
        ax.plot(long_signals, marker='^',markersize=12, lw=0,c='g',label='LONG')
    
    exit_signals = newbie['price'][newbie['signals']==-1]
    if not exit_signals.empty:
        ax.plot(exit_signals, marker='v',markersize=12, lw=0,c='r',label='SHORT/EXIT')
    
    temp=newbie['coordinates'][newbie['signals']==1]
    if not temp.empty:
        try:
            indexlist = list(map(int, str(temp.iloc[0]).split(",")))
            dates = pd.to_datetime(new["date"].iloc[indexlist], format="mixed", utc=True)
            prices = new["price"].iloc[indexlist]
            ax.plot(dates, prices, lw=5, alpha=0.7, c="#FE4365", label="double bottom pattern")
        except Exception as e:
            print(f"Could not plot W shape: {e}")
    
    longs_in_view = newbie.loc[newbie['signals']==1]
    if not longs_in_view.empty:
        plt.text(longs_in_view.index[0], \
                 longs_in_view['lower band'].iloc[0],'Expansion',fontsize=12,color='#563838')
    
    shorts_in_view = newbie.loc[newbie['signals']==-1]
    if not shorts_in_view.empty:
        plt.text(shorts_in_view.index[0], \
                 shorts_in_view['lower band'].iloc[0],'Contraction',fontsize=12,color='#563838')
    
    plt.legend(loc='best')
    plt.title(title)
    plt.ylabel(y_label)
    plt.grid(True)
    out_img = os.path.join(OUT_DIR, "nifty_bb_bottom_w_plot.png")
    plt.tight_layout()
    plt.savefig(out_img)
    plt.close()
    print(f"Plot saved to {out_img}")


def main():
    ap = argparse.ArgumentParser(description="Bollinger bottom-W (Heikin-Ashi or regular close)")
    ap.add_argument("--csv", default=DEFAULT_CSV, help="OHLC CSV path")
    ap.add_argument(
        "--regular-close",
        action="store_true",
        help="Use raw close as price (default: Heikin-Ashi close from OHLC)",
    )
    args = ap.parse_args()

    data_path = args.csv
    if not os.path.exists(data_path):
        print(f"Data file not found at {data_path}")
        return 1

    df = pd.read_csv(data_path)
    for col in ("open", "high", "low", "close"):
        if col not in df.columns:
            print(f"Missing column {col!r} — need full OHLC for Heikin-Ashi.")
            return 1

    if args.regular_close:
        df["price"] = df["close"].astype(float)
        title = "Nifty50 — Bollinger bottom-W (regular close)"
        y_label = "Close"
        print("Using regular candlestick close.")
    else:
        df = compute_heikin_ashi(df)
        df["price"] = df["ha_close"].astype(float)
        title = "Nifty 50 — Bollinger bottom-W (Heikin-Ashi close)"
        y_label = "Heikin-Ashi close"
        print("Computed Heikin-Ashi from OHLC; using HA close for bands and signals.")

    signals = signal_generation(df, bollinger_bands)
    new = copy.deepcopy(signals)

    trades = new[new["signals"] != 0]
    print(f"Total signals generated: {len(trades)}")
    if len(trades) > 0:
        longs = len(trades[trades["signals"] == 1])
        exits = len(trades[trades["signals"] == -1])
        print(f"LONGs: {longs}, EXITs: {exits}")

    csv_out = os.path.join(OUT_DIR, "nifty_bb_bottom_w_signals.csv")
    new.to_csv(csv_out, index=False)
    print(f"Signals CSV: {csv_out}")

    plot(new, title=title, y_label=y_label)
    return 0


if __name__ == "__main__":
    raise SystemExit(main() or 0)
