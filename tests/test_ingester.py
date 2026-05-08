import datetime
import logging
from collections import deque
from unittest.mock import MagicMock, patch

import pytest

from alertingest.ingester import (
    IngestWorker,
    _last_noon,
    _read_confluent_wire_format_header,
)


def _make_worker():
    """Return a minimal IngestWorker with mocked dependencies."""
    return IngestWorker(
        kafka_params=MagicMock(),
        backend=MagicMock(),
        registry=MagicMock(),
        prefix_idle_timeout=300,
        max_logged_prefixes=30,
    )


def test_last_noon_after_noon():
    """When current time is after noon, returns today's noon."""
    fake_now = datetime.datetime(2026, 1, 15, 14, 30, 0)
    with patch("alertingest.ingester.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = fake_now
        mock_dt.timedelta = datetime.timedelta
        result = _last_noon()
    assert result == datetime.datetime(2026, 1, 15, 12, 0, 0)


def test_last_noon_before_noon():
    """When current time is before noon, returns yesterday's noon."""
    fake_now = datetime.datetime(2026, 1, 15, 9, 0, 0)
    with patch("alertingest.ingester.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = fake_now
        mock_dt.timedelta = datetime.timedelta
        result = _last_noon()
    assert result == datetime.datetime(2026, 1, 14, 12, 0, 0)


def test_last_noon_exactly_at_noon():
    """When current time is exactly noon, returns today's noon."""
    fake_now = datetime.datetime(2026, 1, 15, 12, 0, 0)
    with patch("alertingest.ingester.datetime") as mock_dt:
        mock_dt.datetime.now.return_value = fake_now
        mock_dt.timedelta = datetime.timedelta
        result = _last_noon()
    assert result == datetime.datetime(2026, 1, 15, 12, 0, 0)


def test_last_noon_range():
    """Result is always within the past 24 hours."""
    result = _last_noon()
    now = datetime.datetime.now()
    assert result <= now
    assert now - result < datetime.timedelta(days=1)


def test_check_daily_reset_no_rollover(caplog):
    """No log or reset when less than 24 hours have passed."""
    worker = _make_worker()
    start = datetime.datetime(2026, 1, 15, 12, 0, 0)
    state = {"daily_stored": 42, "day_start_time": start}
    with caplog.at_level(logging.INFO, logger="alertingest.ingester"):
        worker._check_daily_reset(start + datetime.timedelta(seconds=86399), state)
    assert state["daily_stored"] == 42
    assert "24 hours" not in caplog.text


def test_check_daily_reset_single_rollover(caplog):
    """Counter is logged and reset after exactly 24 hours."""
    worker = _make_worker()
    start = datetime.datetime(2026, 1, 15, 12, 0, 0)
    state = {"daily_stored": 100, "day_start_time": start}
    with caplog.at_level(logging.INFO, logger="alertingest.ingester"):
        worker._check_daily_reset(start + datetime.timedelta(days=1), state)
    assert state["daily_stored"] == 0
    assert state["day_start_time"] == start + datetime.timedelta(days=1)
    assert "100" in caplog.text


def test_check_daily_reset_multiple_rollovers(caplog):
    """When more than 48 hours pass, rolls over twice and advances
    day_start_time correctly."""
    worker = _make_worker()
    start = datetime.datetime(2026, 1, 15, 12, 0, 0)
    state = {"daily_stored": 50, "day_start_time": start}
    with caplog.at_level(logging.INFO, logger="alertingest.ingester"):
        worker._check_daily_reset(start + datetime.timedelta(days=2, seconds=1), state)
    assert state["daily_stored"] == 0
    assert state["day_start_time"] == start + datetime.timedelta(days=2)


def test_check_idle_prefixes_not_idle(caplog):
    """No log when a prefix has been active recently."""
    worker = _make_worker()
    prefix_counts = {"123456": 5}
    prefix_last_write = {"123456": 1000.0}
    logged = deque(maxlen=30)
    with caplog.at_level(logging.INFO, logger="alertingest.ingester"):
        worker._check_idle_prefixes(1200.0, prefix_counts, prefix_last_write, logged)
    assert "123456" not in caplog.text
    assert len(logged) == 0


def test_check_idle_prefixes_logs_idle_prefix(caplog):
    """A prefix idle longer than prefix_idle_timeout is logged and retired."""
    worker = _make_worker()
    prefix_counts = {"123456": 7}
    prefix_last_write = {"123456": 0.0}
    logged = deque(maxlen=30)
    with caplog.at_level(logging.INFO, logger="alertingest.ingester"):
        worker._check_idle_prefixes(400.0, prefix_counts, prefix_last_write, logged)
    assert "123456" in caplog.text
    assert "7" in caplog.text
    assert "123456" in logged
    assert "123456" not in prefix_counts
    assert "123456" not in prefix_last_write


def test_check_idle_prefixes_skips_already_logged(caplog):
    """A prefix that was already logged is not logged again."""
    worker = _make_worker()
    prefix_counts = {"123456": 7}
    prefix_last_write = {"123456": 0.0}
    logged = deque(["123456"], maxlen=30)
    with caplog.at_level(logging.INFO, logger="alertingest.ingester"):
        worker._check_idle_prefixes(400.0, prefix_counts, prefix_last_write, logged)
    assert caplog.text == ""


def test_log_final_summary_logs_stored_count(caplog):
    """Logs the current-period stored count."""
    worker = _make_worker()
    state = {
        "daily_stored": 99,
        "prefix_counts": {},
        "logged_prefixes": deque(maxlen=30),
    }
    with caplog.at_level(logging.INFO, logger="alertingest.ingester"):
        worker._log_final_summary(state)
    assert "99" in caplog.text


def test_log_final_summary_logs_unlogged_prefixes(caplog):
    """Logs per-prefix counts for prefixes not yet reported."""
    worker = _make_worker()
    state = {
        "daily_stored": 10,
        "prefix_counts": {"aabbcc": 3, "ddeeff": 5},
        "logged_prefixes": deque(["aabbcc"], maxlen=30),
    }
    with caplog.at_level(logging.INFO, logger="alertingest.ingester"):
        worker._log_final_summary(state)
    assert "ddeeff" in caplog.text
    assert "aabbcc" not in caplog.text


def test_confluent_wire_format_parsing():
    data = b"\x00\x00\x00\x00\x02"
    have = _read_confluent_wire_format_header(data)
    assert have == 2

    data = b"\x00"
    with pytest.raises(ValueError):
        # too short
        _read_confluent_wire_format_header(data)

    data = b"\x01\x00\x00\x00\x02"
    with pytest.raises(ValueError):
        # wrong magic byte
        _read_confluent_wire_format_header(data)

    # extra data is ok
    data = b"\x00\x00\x00\x00\x04\xd3"
    have = _read_confluent_wire_format_header(data)
    assert have == 4
