"""OC-18: naive queue-clock values convert with the offset of their own instant."""
import os
import time
from datetime import datetime, timezone

import pytest

from iac_plugin.app.controller import queue as queue_mod


@pytest.fixture
def berlin():
    old = os.environ.get("TZ")
    os.environ["TZ"] = "Europe/Berlin"
    time.tzset()
    yield
    if old is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = old
    time.tzset()


def test_value_after_the_dst_change_is_not_shifted_by_todays_offset(berlin, monkeypatch):
    """p09: parked on 2026-10-20 (CEST, +02:00) for a window on 2026-10-27 01:00 UTC
    (CET, +01:00). Read back while it is still summer time."""
    window = datetime(2026, 10, 27, 1, 0, tzinfo=timezone.utc)
    stored = queue_mod.to_queue_clock(window)
    assert stored == datetime(2026, 10, 27, 2, 0)            # naive local wall clock (CET)

    class _Summer(datetime):
        @classmethod
        def now(cls, tz=None):
            now = datetime(2026, 10, 20, 12, 0)
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(queue_mod, "datetime", _Summer)
    assert queue_mod.from_queue_clock(stored) == window     # was 00:00 UTC before the fix


def test_round_trip_in_both_seasons(berlin):
    for aware in (datetime(2026, 7, 1, 10, 30, tzinfo=timezone.utc), datetime(2026, 12, 1, 10, 30, tzinfo=timezone.utc)):
        assert queue_mod.from_queue_clock(queue_mod.to_queue_clock(aware)) == aware
    assert queue_mod.from_queue_clock(None) is None
