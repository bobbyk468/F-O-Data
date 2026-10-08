"""Detect Zerodha's retroactive price re-adjustment (dividends, splits, bonuses) and rebase history.

Kite re-adjusts *all* past prices after a corporate action. An incremental update that only appends
new bars then leaves a price jump inside the file. Incremental updaters re-download a few overlapping
bars; if those no longer match what is stored, the symbol's whole history must be refetched.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

TOL = 5e-4  # 0.05% close-to-close difference on overlapping bars counts as "re-adjusted"
PRICE_COLS = ["open", "high", "low", "close"]


def judge_ratios(ratios, tol: float = TOL, min_n: int = 2):
    """(re_adjusted, median_ratio, n) from new_close/old_close ratios of overlapping bars.

    Needs at least ``min_n`` usable bars, a median off by more than ``tol`` and at least half of the
    bars off by more than ``tol`` (so one in-progress or corrected bar cannot trigger a refetch).
    """
    r = np.asarray([x for x in ratios if x is not None and np.isfinite(x) and x > 0], dtype=float)
    if len(r) < min_n:
        return False, None, len(r)
    med = float(np.median(r))
    share = float(np.mean(np.abs(r - 1.0) > tol))
    return (abs(med - 1.0) > tol and share >= 0.5), med, len(r)


def frames_readjusted(old: pd.DataFrame, new: pd.DataFrame, tol: float = TOL, min_n: int = 2):
    """Compare overlapping bars of two OHLCV frames with tz-aware ``date`` columns."""
    m = old[["date", "close"]].merge(new[["date", "close"]], on="date", suffixes=("_o", "_n"))
    m = m[(m.close_o > 0) & (m.close_n > 0)]
    return judge_ratios((m.close_n / m.close_o).tolist(), tol, min_n)


def rebase_merge(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Full-history ``new`` wins. Old bars that ``new`` no longer contains (e.g. after-hours bars of
    special sessions the API stopped returning) are kept, with prices scaled onto the new basis by the
    median new/old ratio of that day's common bars (nearest earlier day, then later day, as fallback).
    """
    new = new.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
    only = old[~old["date"].isin(set(new["date"]))].copy()
    if only.empty:
        return new
    common = old.merge(new[["date", "close"]], on="date", suffixes=("_o", "_n"))
    common = common[(common.close_o > 0) & (common.close_n > 0)]
    common["day"] = common["date"].dt.date
    fday = (common.close_n / common.close_o).groupby(common["day"]).median().sort_index()
    only["day"] = only["date"].dt.date
    days = pd.Index(sorted(set(only["day"]) | set(fday.index)))
    fac = fday.reindex(days).ffill().bfill().fillna(1.0)
    f = only["day"].map(fac).astype(float)
    for c in PRICE_COLS:
        only[c] = (only[c] * f).round(2)
    out = pd.concat([new, only[new.columns]], ignore_index=True)
    return out.sort_values("date").reset_index(drop=True)
