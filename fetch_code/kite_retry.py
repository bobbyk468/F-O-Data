"""Retrying wrapper around kite.historical_data that never silently drops a chunk."""
from __future__ import annotations

import time


class FetchError(RuntimeError):
    """A historical_data chunk still failed after all retries."""


def _is_fatal(e: Exception) -> bool:
    return "TokenException" in type(e).__name__ or "Invalid" in str(e)


def historical_with_retry(kite, token, start, end, interval, retries: int = 5, backoff: float = 1.5):
    """Return candles for [start, end]. Empty list only means the API returned no data.

    Token/invalid-request errors are re-raised immediately; other errors are retried
    with linear backoff and raise FetchError if they never succeed.
    """
    last = None
    for attempt in range(retries):
        try:
            return kite.historical_data(token, start, end, interval=interval) or []
        except Exception as e:
            if _is_fatal(e):
                raise
            last = e
            time.sleep(backoff * (attempt + 1))
    raise FetchError(f"{interval} {start}..{end} token={token} failed after {retries} attempts: {last}")
