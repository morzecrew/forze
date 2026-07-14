"""`RedisCounterAdminAdapter` honours the two ``SCAN`` guarantees a naive loop gets wrong.

Redis is allowed to return an **empty step with a non-zero cursor** (``count`` bounds the work
a step does, not the keys it returns — so a step that examines only non-matching keys yields
nothing) and to return **the same key on more than one step**. Both are rare against a real
server and neither is reproducible on demand, which is exactly why they are pinned here with a
stub client instead: the integration suite proves the adapter works against Redis, and these
prove it works against the Redis the documentation actually promises.

The first of the two is the dangerous one. Stopping on an empty step under-reports the
counters, and an under-reported counter is not an error anywhere — it is a sequence number the
target reissues, silently, months later.
"""

from __future__ import annotations

from typing import Any

from forze_redis.adapters import RedisCounterAdminAdapter

# ----------------------- #


class _ScriptedScanClient:
    """A client whose ``SCAN`` replays a fixed script of ``(next_cursor, keys)`` steps."""

    def __init__(self, *steps: tuple[int, list[str]], values: dict[str, bytes | None]) -> None:
        self._steps = list(steps)
        self._values = values
        self.scans = 0
        self.mget_keys: list[str] = []

    async def scan(
        self,
        cursor: int = 0,
        *,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, list[str]]:
        step = self._steps[self.scans]
        self.scans += 1
        return step

    async def mget(self, keys: Any) -> list[bytes | None]:
        self.mget_keys = list(keys)
        return [self._values.get(key) for key in self.mget_keys]


def _admin(client: Any) -> RedisCounterAdminAdapter:
    return RedisCounterAdminAdapter(client=client, namespace="orders")


def _pairs(entries) -> set[tuple[str | None, int]]:
    return {(e.suffix, e.value) for e in entries}


# ....................... #


class TestScanTermination:
    async def test_an_empty_step_with_a_nonzero_cursor_does_not_end_the_scan(self) -> None:
        # Step 1 finds nothing but the cursor is still live; the counters are only on step 2.
        # A loop that treated "no keys" as "no more keys" would report this application as
        # having no counters at all.
        client = _ScriptedScanClient(
            (42, []),
            (0, ["counter:orders", "counter:orders:2026"]),
            values={b"": None, "counter:orders": b"7", "counter:orders:2026": b"3"},
        )

        entries = await _admin(client).list_counters()

        assert _pairs(entries) == {(None, 7), ("2026", 3)}
        assert client.scans == 2  # it kept going

    async def test_the_scan_runs_to_a_zero_cursor_across_many_steps(self) -> None:
        client = _ScriptedScanClient(
            (7, ["counter:orders:a"]),
            (9, []),
            (3, ["counter:orders:b"]),
            (0, []),
            values={"counter:orders:a": b"1", "counter:orders:b": b"2"},
        )

        entries = await _admin(client).list_counters()

        assert _pairs(entries) == {("a", 1), ("b", 2)}
        assert client.scans == 4

    async def test_a_zero_cursor_on_the_first_step_ends_it(self) -> None:
        client = _ScriptedScanClient((0, ["counter:orders"]), values={"counter:orders": b"5"})

        assert _pairs(await _admin(client).list_counters()) == {(None, 5)}
        assert client.scans == 1


# ....................... #


class TestScanDuplicates:
    async def test_a_key_returned_twice_is_read_and_reported_once(self) -> None:
        client = _ScriptedScanClient(
            (5, ["counter:orders:2026"]),
            (0, ["counter:orders:2026", "counter:orders"]),
            values={"counter:orders": b"4", "counter:orders:2026": b"9"},
        )

        entries = await _admin(client).list_counters()

        assert _pairs(entries) == {(None, 4), ("2026", 9)}
        # Deduped before the read, so a repeated key costs nothing and cannot be double-listed.
        assert sorted(client.mget_keys) == ["counter:orders", "counter:orders:2026"]


# ....................... #


class TestVanishedKeys:
    async def test_a_key_deleted_between_the_scan_and_the_read_is_dropped_not_zeroed(
        self,
    ) -> None:
        # ``MGET`` reports the gone key as ``None``. That is not a counter whose value is
        # zero — it is not a counter any more. Exporting it as 0 would rewind a live sequence
        # to the start on import, which is the very failure this plane exists to prevent.
        client = _ScriptedScanClient(
            (0, ["counter:orders", "counter:orders:gone"]),
            values={"counter:orders": b"12", "counter:orders:gone": None},
        )

        assert _pairs(await _admin(client).list_counters()) == {(None, 12)}


# ....................... #


class TestForeignKeys:
    async def test_a_key_from_a_namespace_that_merely_shares_the_prefix_is_ignored(
        self,
    ) -> None:
        # ``MATCH`` is a prefix glob, so Redis hands back ``orders_archive``'s keys too.
        client = _ScriptedScanClient(
            (
                0,
                [
                    "counter:orders",
                    "counter:orders:eu",
                    "counter:orders_archive",
                    "counter:orders_archive:eu",
                ],
            ),
            values={
                "counter:orders": b"3",
                "counter:orders:eu": b"4",
                "counter:orders_archive": b"99",
                "counter:orders_archive:eu": b"50",
            },
        )

        entries = await _admin(client).list_counters()

        # The archive's sequence numbers must not be exported as this route's.
        assert _pairs(entries) == {(None, 3), ("eu", 4)}
        assert "counter:orders_archive" not in client.mget_keys
