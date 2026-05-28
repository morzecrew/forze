"""Shared helpers for Mongo search integration tests."""

from __future__ import annotations

import asyncio
import time


async def wait_search_ready(
    client: object,
    coll: object,
    *,
    index_name: str,
    timeout_s: float = 120.0,
) -> None:
    """Poll until ``$search`` against *index_name* succeeds."""

    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    pipeline = [
        {
            "$search": {
                "index": index_name,
                "text": {"query": "warmup", "path": "title"},
            }
        },
        {"$limit": 1},
    ]

    while time.monotonic() < deadline:
        try:
            await client.aggregate(coll, pipeline, limit=1)  # type: ignore[attr-defined]
            return
        except Exception as exc:
            last_err = exc
            await asyncio.sleep(2)

    raise TimeoutError(f"Atlas Search index not ready: {last_err!r}")


async def wait_vector_index(
    client: object,
    coll: object,
    *,
    index_name: str,
    path: str,
    dimensions: int = 3,
    timeout_s: float = 120.0,
) -> None:
    """Poll until ``$vectorSearch`` against *index_name* succeeds."""

    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    probe = [0.0] * dimensions
    pipeline = [
        {
            "$vectorSearch": {
                "index": index_name,
                "path": path,
                "queryVector": probe,
                "numCandidates": 10,
                "limit": 1,
            }
        },
        {"$limit": 1},
    ]

    while time.monotonic() < deadline:
        try:
            await client.aggregate(coll, pipeline, limit=1)  # type: ignore[attr-defined]
            return
        except Exception as exc:
            last_err = exc
            await asyncio.sleep(2)

    raise TimeoutError(f"Vector search index not ready: {last_err!r}")
