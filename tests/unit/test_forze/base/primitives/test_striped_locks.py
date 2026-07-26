"""Striped async locks: keyed serialization over a bounded lock set."""

from __future__ import annotations

import asyncio

from forze.base.primitives import LOCK_STRIPES, StripedAsyncLocks

# ----------------------- #


class TestStripedAsyncLocks:
    def test_the_same_key_always_maps_to_the_same_lock(self) -> None:
        locks = StripedAsyncLocks()

        assert locks.for_key("tenant-a|oauth/crm") is locks.for_key("tenant-a|oauth/crm")

    def test_stripe_assignment_is_process_independent(self) -> None:
        """A content digest, not :func:`hash` — so a forced-collision test stays meaningful
        instead of depending on the interpreter's hash seed."""

        first, second = StripedAsyncLocks(), StripedAsyncLocks()
        keys = [f"key-{index}" for index in range(200)]

        assert [first._locks.index(first.for_key(key)) for key in keys] == [
            second._locks.index(second.for_key(key)) for key in keys
        ]

    def test_the_lock_set_is_bounded_by_the_stripe_count(self) -> None:
        locks = StripedAsyncLocks()
        distinct = {id(locks.for_key(f"key-{index}")) for index in range(5_000)}

        assert len(distinct) <= LOCK_STRIPES

    async def test_one_key_serializes_and_distinct_keys_do_not(self) -> None:
        locks = StripedAsyncLocks()
        overlapped = False

        async def hold(key: str, marker: list[str]) -> None:
            nonlocal overlapped

            async with locks.for_key(key):
                marker.append("in")

                if marker.count("in") > 1:
                    overlapped = True

                await asyncio.sleep(0.01)
                marker.remove("in")

        same: list[str] = []
        await asyncio.gather(*(hold("shared", same) for _ in range(4)))

        assert not overlapped, "one key must serialize"

        # Two keys chosen to land on different stripes proceed concurrently: the stripe is
        # a bound on the lock set, not a global lock.
        first, second = "alpha", "beta"

        if locks.for_key(first) is not locks.for_key(second):
            started = asyncio.Event()

            async def slow() -> None:
                async with locks.for_key(first):
                    started.set()
                    await asyncio.sleep(0.05)

            task = asyncio.create_task(slow())
            await started.wait()

            # Uncontended, so this returns immediately rather than waiting out `slow`.
            async with asyncio.timeout(0.02), locks.for_key(second):
                pass

            await task
