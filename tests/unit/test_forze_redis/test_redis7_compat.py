"""redis-py 7 compatibility guards for forze_redis.

The core package targets redis-py 7.3+, so it must not depend on redis-py-8-only APIs at
import time. Client-side caching invalidation is the one opt-in feature that requires 8+.
"""

import pathlib

import pytest

from forze_redis._compat import redis_supports_client_side_caching
from forze_redis.kernel.client.utils import parse_stream_entries

pytestmark = pytest.mark.unit

_SRC = pathlib.Path(__file__).resolve().parents[3] / "src" / "forze_redis"


def test_no_redis_typing_import() -> None:
    """``redis.typing`` aliases are redis-py 8-only; importing them breaks redis-py 7."""

    offenders = [
        str(path.relative_to(_SRC))
        for path in _SRC.rglob("*.py")
        if "from redis.typing import" in path.read_text()
    ]
    assert offenders == [], f"redis.typing imported in: {offenders}"


def test_parse_stream_entries_handles_resp2_and_resp3() -> None:
    """The self-owned stream types parse both RESP2 (list) and RESP3 (dict) shapes."""

    resp2 = [(b"s", [(b"1-0", {b"f": b"v"})])]
    resp3 = {b"s": [(b"2-0", {b"f": b"v"})]}

    assert parse_stream_entries(resp2) == [("s", [("1-0", {b"f": b"v"})])]
    assert parse_stream_entries(resp3) == [("s", [("2-0", {b"f": b"v"})])]
    assert parse_stream_entries(None) == []


def test_capability_probe_reflects_redis_py_major(monkeypatch: pytest.MonkeyPatch) -> None:
    import redis

    monkeypatch.setattr(redis, "__version__", "8.0.1", raising=False)
    assert redis_supports_client_side_caching() is True

    monkeypatch.setattr(redis, "__version__", "7.4.0", raising=False)
    assert redis_supports_client_side_caching() is False

    monkeypatch.setattr(redis, "__version__", "not-a-version", raising=False)
    assert redis_supports_client_side_caching() is False
