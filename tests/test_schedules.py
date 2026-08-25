"""Tests for the schedule SPEC — what times the server will fire on.

No client and no server here. `_spec()` is a pure function of config, and a wrong spec is the
kind of bug you would otherwise discover by nobody noticing the ingest did not run.
"""

import pytest

from durable_ingest import config, schedules


@pytest.fixture
def daily(monkeypatch):
    """Switch config to daily mode at a given time and zone."""

    def configure(at="15:00", tz="Asia/Kolkata"):
        monkeypatch.setattr(config, "SCHEDULE_MODE", "daily")
        monkeypatch.setattr(config, "SCHEDULE_DAILY_AT", at)
        monkeypatch.setattr(config, "SCHEDULE_TIMEZONE", tz)

    return configure


# --- daily ----------------------------------------------------------------------


def test_daily_fires_once_at_the_named_hour(daily):
    daily("15:00")
    spec = schedules._spec()

    assert not spec.intervals, "a daily schedule must not also carry an interval"
    assert len(spec.calendars) == 1

    cal = spec.calendars[0]
    assert [r.start for r in cal.hour] == [15]
    assert [r.start for r in cal.minute] == [0]
    assert [r.start for r in cal.second] == [0]


def test_daily_carries_the_time_zone(daily):
    """WITHOUT this the server evaluates the calendar in UTC, and 15:00 fires at 20:30 IST."""
    daily("15:00", "Asia/Kolkata")
    assert schedules._spec().time_zone_name == "Asia/Kolkata"


def test_daily_honours_minutes(daily):
    daily("09:30")
    cal = schedules._spec().calendars[0]
    assert [r.start for r in cal.hour] == [9]
    assert [r.start for r in cal.minute] == [30]


def test_daily_without_minutes_means_on_the_hour(daily):
    daily("15")
    cal = schedules._spec().calendars[0]
    assert [r.start for r in cal.hour] == [15]
    assert [r.start for r in cal.minute] == [0]


def test_daily_matches_every_day_of_every_month(daily):
    """The defaults are what make this DAILY rather than one single date."""
    daily("15:00")
    cal = schedules._spec().calendars[0]
    assert (cal.day_of_month[0].start, cal.day_of_month[0].end) == (1, 31)
    assert (cal.month[0].start, cal.month[0].end) == (1, 12)
    assert (cal.day_of_week[0].start, cal.day_of_week[0].end) == (0, 6)


# --- interval -------------------------------------------------------------------


def test_interval_is_the_default_mode(monkeypatch):
    monkeypatch.setattr(config, "SCHEDULE_MODE", "interval")
    monkeypatch.setattr(config, "SCHEDULE_INTERVAL_MINUTES", 5)
    spec = schedules._spec()

    assert not spec.calendars
    assert spec.intervals[0].every.total_seconds() == 300
    assert spec.time_zone_name is None, "an interval needs no zone; it is a duration, not a time"


# --- a typo must not silently become something else -----------------------------


def test_an_unknown_mode_fails_loudly(monkeypatch):
    """Falling back to a 5-minute interval on a typo would be a nasty surprise on a daily job."""
    monkeypatch.setattr(config, "SCHEDULE_MODE", "dialy")
    with pytest.raises(ValueError, match="must be 'interval' or 'daily'"):
        schedules._spec()
