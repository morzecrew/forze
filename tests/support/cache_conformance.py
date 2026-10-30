"""Value-contract battery for :class:`~forze.application.contracts.cache.CachePort`.

The port promises two kinds of value: JSON, returned parsed, and pre-encoded ``bytes``,
stored verbatim and returned for the caller to decode. The in-memory mock kept bytes as
written while Redis parsed every body as JSON, so code that decoded its own bytes (for
example ``TypeAdapter.validate_json``) passed against the mock and failed against Redis.

:func:`assert_value_contract` runs the same claims against any adapter, through every write
path (``set``, ``set_many``, ``set_versioned``, ``set_many_versioned``) and both reads:

1. **bytes come back as the same bytes**, including JSON bytes (the case Redis parsed), the
   empty value, a leading NUL, Redis's own bytes marker and bytes that are not UTF-8;
2. **JSON values come back parsed and unchanged**, including a ``str`` that starts with a NUL.
"""

from __future__ import annotations

from typing import Any

from forze.application.contracts.cache import CachePort

# ----------------------- #

BYTES_VALUES: tuple[bytes, ...] = (
    b'{"id":"1","items":[1,2,3]}',
    b"[1,2]",
    b'"text"',
    b"",
    b"\x00",
    b"\x00{}",
    b"\x00\xffforze:bytes\x00",
    b"\xff\xfe\x00not utf-8",
)
"""Pre-encoded values, each of which must come back byte for byte."""

JSON_VALUES: tuple[Any, ...] = (
    {"a": 1, "b": [1, 2]},
    [1, "two", {"three": 3}],
    "text",
    "",
    "\x00text",
    0,
    -1.5,
    True,
    False,
)
"""JSON values, each of which must come back equal and of the same type."""

# ....................... #


def _assert_same(got: Any, expected: Any) -> None:
    assert type(got) is type(expected), (type(got), expected)
    assert got == expected


# ....................... #


async def assert_value_contract(cache: CachePort) -> None:
    """Assert both value kinds round-trip through every write path of *cache*."""

    values = (*BYTES_VALUES, *JSON_VALUES)

    for i, value in enumerate(values):
        await cache.set(f"plain-{i}", value)
        _assert_same(await cache.get(f"plain-{i}"), value)

        await cache.set_versioned(f"versioned-{i}", "v1", value)
        _assert_same(await cache.get(f"versioned-{i}"), value)

    plain = {f"many-{i}": value for i, value in enumerate(values)}
    await cache.set_many(plain)
    hits, misses = await cache.get_many(list(plain))

    assert misses == []

    for key, value in plain.items():
        _assert_same(hits[key], value)

    versioned = {(f"many-versioned-{i}", "v1"): value for i, value in enumerate(values)}
    await cache.set_many_versioned(versioned)
    hits, misses = await cache.get_many([key for key, _ in versioned])

    assert misses == []

    for (key, _), value in versioned.items():
        _assert_same(hits[key], value)
