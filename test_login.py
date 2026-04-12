#!/usr/bin/env python3
"""
One-time login test for Zerodha via jugaad-trader.
Prompts for 2FA PIN, or uses ZERODHA_TOTP_SECRET to generate it (authenticator setup key).
"""
import os
import sys
import pickle
import click

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from jugaad_trader import Zerodha
from jugaad_trader.util import CLI_NAME
import functools
import requests


def load_dotenv_if_present():
    """
    Minimal .env loader for local secrets (e.g. ZERODHA_TOTP_SECRET).
    Loads KEY=VALUE pairs from a .env file in this directory into os.environ
    only if the key is not already set.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base_dir, ".env")
    if not os.path.isfile(env_path):
        return
    try:
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        # Fail silently if .env cannot be read; script will fall back to prompts/env
        pass
    return True


def _safe_env_status():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base_dir, ".env")
    return {
        "dotenv_path": env_path,
        "dotenv_exists": os.path.isfile(env_path),
        "totp_present": bool(os.environ.get("ZERODHA_TOTP_SECRET", "").strip()),
        "user_present": bool(os.environ.get("ZERODHA_USER_ID", "").strip()),
        "password_present": bool(os.environ.get("ZERODHA_PASSWORD", "").strip()),
    }


# Load local .env (if present) before reading credentials / TOTP from env vars
_dotenv_loaded = bool(load_dotenv_if_present())

# Use your credentials (remove or use env vars in production)
USER_ID = os.environ.get("ZERODHA_USER_ID", "MEX578")
PASSWORD = os.environ.get("ZERODHA_PASSWORD", "Trading@1")
# Optional: TOTP secret (authenticator setup key) to auto-generate 2FA code.
# e.g. put ZERODHA_TOTP_SECRET=YOUR_KEY in a local .env (git-ignored).
TOTP_SECRET = os.environ.get("ZERODHA_TOTP_SECRET", "").strip()

app_dir = click.get_app_dir(CLI_NAME)
session_file = ".zsession"
session_path = os.path.join(app_dir, session_file)


def get_2fa_code():
    if TOTP_SECRET:
        import pyotp
        return pyotp.TOTP(TOTP_SECRET).now()
    raise RuntimeError(
        "ZERODHA_TOTP_SECRET is not set. Create a local .env with "
        "ZERODHA_TOTP_SECRET=... (and optionally ZERODHA_USER_ID/ZERODHA_PASSWORD) "
        "to run non-interactively."
    )


def main():
    s = _safe_env_status()
    print(f".env path: {s['dotenv_path']}")
    print(f".env exists: {s['dotenv_exists']}")
    print(f".env loaded: {_dotenv_loaded}")
    print(f"TOTP present: {s['totp_present']}")
    print(f"User present: {s['user_present']}")
    print(f"Password present: {s['password_present']}")
    print()
    print("Logging in to Zerodha...")
    z = Zerodha(user_id=USER_ID, password=PASSWORD)

    # Ensure login calls don't hang indefinitely (login_step1/2 use raw requests without timeout)
    try:
        z.reqsession.request = functools.partial(z.reqsession.request, timeout=30)
        z.s.request = functools.partial(z.s.request, timeout=30)
    except Exception:
        pass

    print("Step 1/2: username+password ...", flush=True)
    try:
        j = z.login_step1()
    except (requests.Timeout, requests.ConnectionError) as e:
        print("Login step1 failed (network/timeout):", str(e))
        return 1
    except Exception as e:
        print("Login step1 failed:", str(e))
        return 1

    if j.get("status") == "error":
        print("Error:", j.get("message", "Unknown error"))
        return 1
    print("Step 1/2: OK", flush=True)

    try:
        pin = get_2fa_code()
    except Exception as e:
        print(str(e))
        return 1
    z.twofa = pin
    print("Step 2/2: 2FA ...", flush=True)
    try:
        j = z.login_step2(j)
    except (requests.Timeout, requests.ConnectionError) as e:
        print("Login step2 failed (network/timeout):", str(e))
        return 1
    except Exception as e:
        print("Login step2 failed:", str(e))
        return 1
    if j.get("status") == "error":
        print("Error:", j.get("message", "Unknown error"))
        return 1
    z.enc_token = z.r.cookies["enctoken"]
    profile = z.profile()
    print("\nLogged in successfully.")
    print("Profile:", profile.get("user_name"), "| Email:", profile.get("email", ""))

    os.makedirs(app_dir, exist_ok=True)
    with open(session_path, "wb") as f:
        pickle.dump(z.reqsession, f)
    print("Session saved to:", session_path)
    print("\nYou can now run: .venv/bin/python try_jugaad_trader.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
