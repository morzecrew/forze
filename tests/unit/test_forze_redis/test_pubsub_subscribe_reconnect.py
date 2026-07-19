"""The auto-reconnect subscribe path — the (re)subscribe leg retries, cleanup is best-effort.

# covers: RedisClient.subscribe (auto-reconnect branch: subscribe retry + suppressed cleanup)

Drives the generator against a stubbed redis object (no broker): the first subscribe raises a
transport error — previously fatal, now retried with backoff — the second succeeds, one
message flows, and the teardown's unsubscribe failure is suppressed instead of masking the
path the reconnect branch exists to save.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

pytest.importorskip("redis")

from redis.exceptions import ConnectionError as RedisConnectionError

from forze_redis.kernel.client import RedisClient, RedisConfig

# ----------------------- #


class _FlakyPubSub:
    """Fails the first subscribe; then serves one message; unsubscribe always fails."""

    def __init__(self, shared: dict[str, Any]) -> None:
        self.shared = shared

    async def subscribe(self, *channels: str) -> None:
        self.shared["subscribes"] += 1

        if self.shared["subscribes"] == 1:
            raise RedisConnectionError("broker down at subscribe")

    async def get_message(
        self, *, ignore_subscribe_messages: bool = True, timeout: float | None = None
    ) -> dict[str, Any] | None:
        if not self.shared["delivered"]:
            self.shared["delivered"] = True

            return {"type": "message", "channel": b"chan", "data": b"payload"}

        return None

    async def unsubscribe(self, *channels: str) -> None:
        raise RedisConnectionError("connection already gone")  # must not mask the exit

    async def aclose(self) -> None:
        self.shared["closed"] += 1


class _StubRedis:
    def __init__(self, shared: dict[str, Any]) -> None:
        self.shared = shared

    def pubsub(self) -> _FlakyPubSub:
        return _FlakyPubSub(self.shared)


# ----------------------- #


async def test_subscribe_retries_the_subscribe_leg_and_survives_cleanup_errors() -> None:
    shared: dict[str, Any] = {"subscribes": 0, "delivered": False, "closed": 0}
    hooks = {"n": 0}

    def _hook() -> None:
        hooks["n"] += 1

    client = RedisClient()
    # wire the stub through the private seams — no broker in a unit test
    client._RedisClient__client = _StubRedis(shared)  # type: ignore[attr-defined]  # noqa: SLF001
    client._RedisClient__redis_config = RedisConfig(  # type: ignore[attr-defined]  # noqa: SLF001
        pubsub_auto_reconnect=True,
        pubsub_reconnect_max_delay=timedelta(milliseconds=10),
        on_pubsub_reconnect=_hook,
    )

    received = []

    stream = client.subscribe(["chan"], timeout=timedelta(milliseconds=5))
    received.append(await anext(stream))  # one message proves the retried subscribe delivered
    await stream.aclose()  # drive finalization now — a bare `break` would defer the finally

    assert shared["subscribes"] == 2  # first failed, retry succeeded
    assert hooks["n"] == 1  # the reconnect hook observed the subscribe failure
    assert received == [("chan", b"payload")]
    # Two closes: the failed-subscribe pubsub AND the final one — the final cleanup's
    # unsubscribe failure was suppressed *independently*, so aclose still ran (a shared
    # suppress block would have skipped it and leaked the connection).
    assert shared["closed"] == 2
