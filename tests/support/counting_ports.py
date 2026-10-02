"""A document query port that records the reads it serves, for read-count tests."""

from __future__ import annotations

from typing import Any
from uuid import UUID


class CountingQuery:
    """Wraps a query port and appends ``"<name>.<kind>"`` to *reads* for each read it serves.

    A ``get`` and a non-empty ``get_many`` count once (an empty one reaches no store); a scan
    counts once, plus once per batch past its first, since an empty scan still reads. Anything
    else — the spec, other methods — passes through uncounted.
    """

    def __init__(self, inner: Any, reads: list[str], name: str) -> None:
        self._inner, self._reads, self._name = inner, reads, name

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._inner, attr)

    async def get(self, pk: UUID, **kwargs: Any) -> Any:
        self._reads.append(f"{self._name}.get")
        return await self._inner.get(pk, **kwargs)

    async def get_many(self, pks: Any, **kwargs: Any) -> Any:
        if pks:
            self._reads.append(f"{self._name}.get_many")

        return await self._inner.get_many(pks, **kwargs)

    async def find_stream(self, **kwargs: Any) -> Any:
        self._reads.append(f"{self._name}.scan")
        first = True

        async for batch in self._inner.find_stream(**kwargs):
            if not first:
                self._reads.append(f"{self._name}.scan")

            first = False
            yield batch
