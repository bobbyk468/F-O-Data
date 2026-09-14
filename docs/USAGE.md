# Using jugaad-trader (cloned repo)

Repo: [jugaad-py/jugaad-trader](https://github.com/jugaad-py/jugaad-trader) – unofficial Python client for Zerodha.

## 1. Install dependencies

**Option A – virtual environment (recommended, already set up):**

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

Then use `.venv/bin/python` or activate the venv before running scripts.

**Option B – global install:**

```bash
pip install -r requirements.txt
pip install -e .
```

## 2. Log in and save session (one-time / when expired)

Use the CLI to create a session (saved in app config; no API key needed):

```bash
jtrader zerodha startsession
```

If you're using the venv, run:

```bash
.venv/bin/jtrader zerodha startsession
```

**Optional – auto 2FA with authenticator setup key:** If you set `ZERODHA_TOTP_SECRET` to your TOTP secret (the key you get when setting up Google Authenticator etc.), `test_login.py` will generate the 2FA code and you won’t be prompted:

```bash
export ZERODHA_TOTP_SECRET='YOUR_BASE32_SECRET'
.venv/bin/python test_login.py
```

Keep the secret private (e.g. don’t commit it to git).

You will be prompted for:

- **User ID** – Zerodha client ID  
- **Password** – Zerodha password  
- **Pin** – 2FA TOTP (e.g. from authenticator app)

On success you’ll see “Logged in successfully” and the session is stored.

Other useful commands:

- `jtrader zerodha configdir` – show where session is stored  
- `jtrader zerodha savecreds` – save user id/password/2FA to a file (optional)  
- `jtrader zerodha rm session` – delete saved session  

## 3. Run the try script

If the package is installed:

```bash
python try_jugaad_trader.py
```

If you only have the repo and did not install:

```bash
PYTHONPATH=. python try_jugaad_trader.py
```

The script loads the stored session, then prints profile, margins, and holdings (first 5).

## 4. Use in your own code

```python
from jugaad_trader import Zerodha

kite = Zerodha()
kite.set_access_token()

# Examples (same as KiteConnect-style API)
profile = kite.profile()
margins = kite.margins()
holdings = kite.holdings()
orders = kite.orders()
positions = kite.positions()
# Place order (example): kite.place_order(variety="regular", ...)
```

Docs: https://marketsetup.in/documentation/jugaad-trader/
