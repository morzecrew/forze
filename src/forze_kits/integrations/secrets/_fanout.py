"""Shared multi-subscriber fan-out for change sources (package-internal)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Collection
from typing import final

import attrs

from forze.application.contracts.secrets import SecretChanged, SecretRef

# ----------------------- #


@final
@attrs.define(slots=True)
class ChangeFanout:
    """Delivers emitted changes to every live subscription (at-least-once, unordered)."""

    _subscribers: list[tuple[asyncio.Queue[SecretChanged], frozenset[str] | None]] = attrs.field(
        factory=list, init=False, repr=False
    )

    # ....................... #

    def emit(self, change: SecretChanged) -> None:
        for queue, paths in self._subscribers:
            if paths is None or change.ref.path in paths:
                queue.put_nowait(change)

    # ....................... #

    async def stream(
        self,
        refs: Collection[SecretRef] | None = None,
    ) -> AsyncIterator[SecretChanged]:
        queue: asyncio.Queue[SecretChanged] = asyncio.Queue()
        paths = None if refs is None else frozenset(ref.path for ref in refs)
        entry = (queue, paths)
        self._subscribers.append(entry)

        try:
            while True:
                yield await queue.get()

        finally:
            self._subscribers.remove(entry)
