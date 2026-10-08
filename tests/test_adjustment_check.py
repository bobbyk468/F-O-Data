"""Tests for Kite price re-adjustment detection and the 5min full-refetch path. Run: .venv/bin/python -m pytest tests/test_adjustment_check.py"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT, ROOT / "fetch_code"):
    sys.path.insert(0, str(p))

from adjustment_check import frames_readjusted, judge_ratios, rebase_merge  # noqa: E402
import fetch_code.fetch_all_indices_5min as f5  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))


def test_judge_ratios():
    assert judge_ratios([1.0] * 10)[0] is False
    assert judge_ratios([1.0002] * 10)[0] is False                       # inside 0.05% tolerance
    flagged, med, n = judge_ratios([0.974] * 10)
    assert flagged and abs(med - 0.974) < 1e-9 and n == 10
    assert judge_ratios([1.0] * 9 + [1.5])[0] is False                   # a single odd bar must not trigger
    assert judge_ratios([0.97])[0] is False                              # too little overlap
    assert judge_ratios([])[0] is False


def _frame(rows):
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert("Asia/Kolkata")
    return df


def test_frames_readjusted():
    old = _frame([[f"2026-01-0{d} 04:00:00+00:00", 100, 101, 99, 100, 1] for d in range(1, 6)])
    same = old.copy()
    adj = old.copy(); adj[["open", "high", "low", "close"]] *= 0.97
    assert frames_readjusted(old, same)[0] is False
    flagged, ratio, n = frames_readjusted(old, adj)
    assert flagged and abs(ratio - 0.97) < 1e-9 and n == 5


def test_rebase_merge_keeps_special_session_bars_on_new_basis():
    old = _frame([
        ["2021-02-24 04:00:00+00:00", 100, 101, 99, 100, 1],
        ["2021-02-24 10:30:00+00:00", 100, 101, 99, 100, 1],   # after-hours bar the API no longer returns
        ["2021-02-25 04:00:00+00:00", 100, 101, 99, 100, 1],
    ])
    new = _frame([
        ["2021-02-24 04:00:00+00:00", 90, 91, 89, 90, 1],      # day factor 0.9
        ["2021-02-25 04:00:00+00:00", 80, 81, 79, 80, 1],
    ])
    out = rebase_merge(old, new)
    assert len(out) == 3 and out["date"].is_monotonic_increasing
    kept = out[out["date"] == old["date"].iloc[1]].iloc[0]
    assert abs(kept.close - 90.0) < 1e-9 and abs(kept.high - 90.9) < 1e-9 and abs(kept.low - 89.1) < 1e-9
    assert out.iloc[0].close == 90 and out.iloc[2].close == 80   # new data untouched


class FakeKite:
    """Serves 5min bars for 2026-03-02..2026-03-06 at a price level; optionally dies on later calls."""
    def __init__(self, level, fail_after_probe=False):
        self.level, self.fail, self.calls = level, fail_after_probe, 0

    def historical_data(self, token, start, end, interval):
        self.calls += 1
        if self.fail and self.calls > 1:
            raise RuntimeError("boom")
        out = []
        d = datetime(2026, 3, 2, tzinfo=IST)
        for day in range(5):
            for b in range(3):
                ts = d + timedelta(days=day, hours=9, minutes=15 + 5 * b)
                if start <= ts.date() <= end:
                    out.append({"date": ts, "open": self.level, "high": self.level + 1, "low": self.level - 1, "close": self.level, "volume": 10})
        return out


def _seed(tmp_path, level):
    f5._write_5min_csv(str(tmp_path / "x_5min.csv"), [
        {"date": datetime(2026, 3, 2, 9, 15 + 5 * b, tzinfo=IST) + timedelta(days=day), "open": level, "high": level + 1, "low": level - 1, "close": level, "volume": 10}
        for day in range(5) for b in range(3)
    ])
    return str(tmp_path / "x_5min.csv")


def _closes(path):
    return set(pd.read_csv(path).close)


def test_5min_no_false_positive(tmp_path):
    path = _seed(tmp_path, 100.0)
    f5.fetch_one_index(FakeKite(100.0), 1, "X", pd.Timestamp("2026-03-02").date(), pd.Timestamp("2026-03-06").date(), str(tmp_path))
    assert _closes(path) == {100.0}


def test_5min_readjusted_prices_trigger_full_refetch(tmp_path):
    path = _seed(tmp_path, 100.0)                       # stored on the old basis
    f5.fetch_one_index(FakeKite(97.0), 1, "X", pd.Timestamp("2026-03-02").date(), pd.Timestamp("2026-03-06").date(), str(tmp_path))
    assert _closes(path) == {97.0} and len(pd.read_csv(path)) == 15


def test_5min_failed_full_refetch_leaves_file_untouched(tmp_path):
    path = _seed(tmp_path, 100.0)
    before = Path(path).read_text()
    f5.fetch_one_index(FakeKite(97.0, fail_after_probe=True), 1, "X", pd.Timestamp("2026-03-02").date(), pd.Timestamp("2026-03-06").date(), str(tmp_path))
    assert Path(path).read_text() == before


def test_5min_rewrite_preserves_line_endings(tmp_path):
    for eol in ("\r\n", "\n"):
        path = tmp_path / f"e{len(eol)}_5min.csv"
        path.write_bytes(f"date,open,high,low,close,volume{eol}".encode())
        f5._write_5min_csv(str(path), [{"date": "2026-03-02 09:15:00+05:30", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 3}])
        raw = path.read_bytes()
        assert (b"\r\n" in raw) == (eol == "\r\n")
