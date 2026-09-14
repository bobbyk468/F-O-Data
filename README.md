# F-O-Data

This repository holds **F&O and index OHLCV datasets** (15m and other timeframes where present), generated and maintained with **[jugaad-trader](https://github.com/jugaad-py/jugaad-trader)** against the Zerodha API.

- **`data/`** — `indices/` (1m, 15m, 30m, etc.), `nifty50/15min`, `other/15min`, EOD where applicable  
- **`output/`** — derived series (e.g. daily CPR width + SuperTrend)  
- **`fetch_code/`** — Zerodha **fetch and incremental update** scripts (writes to `data/`). See [`fetch_code/README.md`](fetch_code/README.md).  
- **`config/nifty50_symbols.txt`** — Nifty 50 list (routes F&O files into `data/nifty50/` vs `data/other/`).  
- **Root shims** — short `fetch_*.py` / `update_incremental.py` / shell scripts delegate to `fetch_code/` for backwards compatibility.  
- **Other scripts** — resample, verify, alignment at repo root  

Upstream library docs: https://marketsetup.in/documentation/jugaad-trader/

---

## Free Zerodha API — jugaad-trader (upstream)

Jugaad trader implements a reverse-engineered API for Zerodha in Python. With this library you can programmatically execute trades, retrieve order and trade books, holdings, margins, and more.

### Installation

```
pip install jugaad-trader
```

### Quick start — CLI session

```
$ jtrader zerodha startsession
User ID >: Zerodha User Id
Password >:
Pin >:
Logged in successfully
```

### Quick start — Python

```python
from jugaad_trader import Zerodha
kite = Zerodha()

kite.set_access_token()
profile = kite.profile()
print(profile)
```

### Contribute to the library

See https://github.com/jugaad-py/jugaad-trader/blob/master/contributing.md

### Articles and examples

https://marketsetup.in/tags/jugaad-trader/
