import asyncio
import datetime
import logging
from collections import deque
from unittest.mock import AsyncMock, MagicMock, patch

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


def _make_msg(offset=0):
    """Return a minimal mock ConsumerRecord."""
    msg = MagicMock()
    msg.offset = offset
    msg.topic = "test-topic"
    msg.partition = 0
    return msg


def _make_consumer(batches):
    """Return a mock aiokafka consumer whose getmany yields successive batches.

    Each element of `batches` is a dict that will be returned by one
    getmany() call. An empty dict simulates a timeout (no messages).
    """
    consumer = MagicMock()
    consumer.getmany = AsyncMock(side_effect=batches)
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.commit = AsyncMock()
    consumer.assignment.return_value = []  # skip per-partition logging in
    # handle_commit
    return consumer


def test_run_processes_full_batch():
    """All messages in a batch are passed to handle_kafka_message."""
    worker = _make_worker()
    msgs = [_make_msg(offset=i) for i in range(3)]
    consumer = _make_consumer(batches=[{"tp": msgs}])
    worker._create_consumer = MagicMock(return_value=consumer)
    worker.handle_kafka_message = AsyncMock(side_effect=[100000, 200000, 300000])

    asyncio.run(worker.run(limit=3, commit_interval=100))

    assert worker.handle_kafka_message.call_count == 3
    consumer.start.assert_awaited_once()
    consumer.stop.assert_awaited_once()


def test_run_state_updated_for_batch():
    """daily_stored increments by the full batch size."""
    worker = _make_worker()
    msgs = [_make_msg(offset=i) for i in range(4)]
    consumer = _make_consumer(batches=[{"tp": msgs}])
    worker._create_consumer = MagicMock(return_value=consumer)
    # Use alert IDs with distinct 6-char prefixes to produce two prefix
    # buckets.
    worker.handle_kafka_message = AsyncMock(
        side_effect=[111111000, 111111001, 222222000, 222222001]
    )

    # Capture state by hooking _log_final_summary (called when limit is hit).
    captured = {}
    original = worker._log_final_summary

    def capture(state):
        captured.update(state)
        original(state)

    worker._log_final_summary = capture

    asyncio.run(worker.run(limit=4, commit_interval=100))

    assert captured["daily_stored"] == 4
    assert captured["prefix_counts"]["111111"] == 2
    assert captured["prefix_counts"]["222222"] == 2


def test_run_empty_batch_invokes_process_timeout():
    """An empty getmany result (timeout) calls process_timeout."""
    worker = _make_worker()
    msg = _make_msg()
    consumer = _make_consumer(batches=[{}, {"tp": [msg]}])
    worker._create_consumer = MagicMock(return_value=consumer)
    worker.handle_kafka_message = AsyncMock(return_value=123456789)
    worker.process_timeout = AsyncMock(return_value=(False, 0))

    asyncio.run(worker.run(limit=1, commit_interval=100))

    worker.process_timeout.assert_awaited_once()


def test_run_batch_error_is_logged_and_reraised(caplog):
    """A failing message is logged with its offset and the exception
    propagates."""
    worker = _make_worker()
    msgs = [_make_msg(offset=0), _make_msg(offset=7)]
    consumer = _make_consumer(batches=[{"tp": msgs}])
    worker._create_consumer = MagicMock(return_value=consumer)
    exc = ValueError("bad alert")
    worker.handle_kafka_message = AsyncMock(side_effect=[123456789, exc])

    with caplog.at_level(logging.ERROR, logger="alertingest.ingester"):
        with pytest.raises(ValueError, match="bad alert"):
            asyncio.run(worker.run(limit=10, commit_interval=100))

    assert "offset 7" in caplog.text


def test_run_commits_when_interval_reached():
    """Commit fires when commit_interval_counter meets or exceeds the
    threshold."""
    worker = _make_worker()
    msgs = [_make_msg(offset=i) for i in range(5)]
    consumer = _make_consumer(batches=[{"tp": msgs}])
    worker._create_consumer = MagicMock(return_value=consumer)
    worker.handle_kafka_message = AsyncMock(side_effect=list(range(100000, 100005)))

    asyncio.run(worker.run(limit=5, commit_interval=3))

    consumer.commit.assert_awaited()


def test_run_commits_on_commit_timeout():
    """Commit fires when commit_timeout seconds elapse since the last commit,
    even if commit_interval hasn't been reached."""
    worker = _make_worker()
    msgs = [_make_msg(offset=i) for i in range(2)]
    consumer = _make_consumer(batches=[{"tp": msgs}])
    worker._create_consumer = MagicMock(return_value=consumer)
    worker.handle_kafka_message = AsyncMock(side_effect=[100000, 200000])

    # commit_interval=10 means the count-based commit won't fire for 2 messages
    # commit_timeout=0 ensures any elapsed time triggers a time-based commit
    asyncio.run(worker.run(limit=2, commit_interval=10, commit_timeout=0))

    consumer.commit.assert_awaited()
