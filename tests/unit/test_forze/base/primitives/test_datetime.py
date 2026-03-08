from datetime import UTC, datetime

from forze.base.primitives import utcnow


def test_utcnow_returns_timezone_aware_utc() -> None:
    now = utcnow()
    assert isinstance(now, datetime)
    assert now.tzinfo is UTC
