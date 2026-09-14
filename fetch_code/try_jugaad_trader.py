#!/usr/bin/env python3
"""
Try jugaad-trader (Zerodha API) from the cloned repo.

Prerequisites:
  1. Install dependencies: pip install -r requirements.txt
  2. Start a session (one-time or when session expires):
     $ jtrader zerodha startsession
     Enter your Zerodha User ID, Password, and 2FA Pin.

Then run:
  python try_jugaad_trader.py
  # or with PYTHONPATH if not installed: PYTHONPATH=. python try_jugaad_trader.py
"""

# --- fetch_code/: data/ and jugaad_trader/ live at repository root ---
import sys
from pathlib import Path as _Path
_FC = _Path(__file__).resolve().parent
_REPO = _FC.parent
for _d in (_REPO, _FC):
    _s = str(_d)
    if _s not in sys.path:
        sys.path.insert(0, _s)
from repo_paths import REPO_ROOT  # noqa: E402

import sys
import os

# Allow running from repo root without installing the package
# paths: bootstrap above

def main():
    from jugaad_trader import Zerodha

    print("Connecting using stored session...")
    kite = Zerodha()
    kite.set_access_token()

    # Profile
    profile = kite.profile()
    print("\n--- Profile ---")
    print(f"User: {profile.get('user_name')} (ID: {profile.get('user_id')})")
    print(f"Email: {profile.get('email')}")

    # Margins (equity)
    try:
        margins = kite.margins()
        equity = margins.get("equity", {})
        print("\n--- Margins (Equity) ---")
        print(f"Available cash: {equity.get('available', {}).get('cash', 'N/A')}")
        print(f"Used margin: {equity.get('utilised', {}).get('debits', 'N/A')}")
    except Exception as e:
        print(f"\nMargins (skipped): {e}")

    # Holdings (if any)
    try:
        holdings = kite.holdings()
        print("\n--- Holdings ---")
        if not holdings:
            print("No equity holdings.")
        else:
            for h in holdings[:5]:  # first 5
                print(f"  {h.get('tradingsymbol')}: {h.get('quantity')} qty")
            if len(holdings) > 5:
                print(f"  ... and {len(holdings) - 5} more")
    except Exception as e:
        print(f"\nHoldings (skipped): {e}")

    print("\nDone. You can use kite.orders(), kite.positions(), kite.holdings(), kite.margins(), etc.")


if __name__ == "__main__":
    main()
