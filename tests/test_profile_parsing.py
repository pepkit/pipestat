"""Tests for profile-parsing functions in reports.py."""

from ubiquerg import parse_timedelta

from pipestat.const import PROFILE_COLNAMES
from pipestat.reports import _get_maxmem, _get_runtime


def _make_profile_rows(raw_rows: list[list]) -> list[dict]:
    """Build a list of dicts mimicking csv-parsed profile rows."""
    rows = []
    for raw in raw_rows:
        r = dict(zip(PROFILE_COLNAMES, raw))
        r["runtime"] = parse_timedelta(r["runtime"])
        r["mem"] = float(r["mem"])
        rows.append(r)
    return rows


class TestGetRuntime:
    def test_basic_sum(self):
        rows = _make_profile_rows(
            [
                ["1", "abc", "1", "0:00:10", " 100.0", "cmd1", "lock.1"],
                ["1", "abc", "2", "0:00:20", " 200.0", "cmd2", "lock.2"],
            ]
        )
        assert _get_runtime(rows) == "0:00:30"

    def test_dedup_keeps_last(self):
        rows = _make_profile_rows(
            [
                ["1", "abc", "1", "0:00:05", " 100.0", "cmd1", "lock.1"],
                ["1", "abc", "2", "0:01:30", " 200.0", "cmd2", "lock.2"],
                ["1", "abc", "2", "0:02:00", " 300.0", "cmd2", "lock.2"],
            ]
        )
        assert _get_runtime(rows) == "0:02:05"

    def test_single_row(self):
        rows = _make_profile_rows(
            [
                ["1", "abc", "1", "1:00:00", " 512.0", "cmd1", "lock.1"],
            ]
        )
        assert _get_runtime(rows) == "1:00:00"


class TestGetMaxmem:
    def test_returns_max(self):
        rows = _make_profile_rows(
            [
                ["1", "abc", "1", "0:00:10", " 128.5", "cmd1", "lock.1"],
                ["1", "abc", "2", "0:00:20", " 512.0", "cmd2", "lock.2"],
                ["1", "abc", "3", "0:00:30", " 256.0", "cmd3", "lock.3"],
            ]
        )
        result = _get_maxmem(rows)
        assert "512.0" in result
        assert "GB" in result

    def test_single_row(self):
        rows = _make_profile_rows(
            [
                ["1", "abc", "1", "0:00:10", " 64.25", "cmd1", "lock.1"],
            ]
        )
        assert "64.25" in _get_maxmem(rows)

    def test_empty_rows(self):
        assert _get_maxmem([]) == "0 GB"
