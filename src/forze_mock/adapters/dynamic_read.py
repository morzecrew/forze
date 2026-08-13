"""In-memory :class:`~forze.application.contracts.dynamic_read.DynamicReadPort` for tests / simulation.

The mock cannot execute SQL, and — unusually for this package — it does not pretend to be a
capability superset here. Half of this plane's contract is refusals only a *database* can make:
a write rejected by a read-only transaction, a second command rejected by the wire protocol, a
cross-schema read rejected by role grants. The mock cannot detect a write in a string, so it
does not try; those live in the real-Postgres battery and are named there.

What the mock *is* a differential oracle for is everything above the engine: the byte cap, the
row cap and its probe, per-call clamping, tenant resolution and fail-closed refusal, and the
tenant-parameter merge. All of that is the shared shell, identical on both sides — which is
exactly what makes comparing them worth doing.

Each route is answered by a handler registered on a :class:`MockDynamicReadRegistry`. The
handler receives the fully-governed :class:`DynamicReadRequest` — so a test can assert what the
shell resolved — plus the :class:`MockState`, and may raise the plane's taxonomy to script a
refusal path the mock could not otherwise produce.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast, final

import attrs

from forze.application.integrations.dynamic_read import (
    DynamicReadAdapter,
    DynamicReadRequest,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, StrKey
from forze_mock.state import MockState

# ----------------------- #

MockDynamicReadHandler = Callable[
    [DynamicReadRequest, MockState],
    "Sequence[JsonDict] | Awaitable[Sequence[JsonDict]]",
]
"""Handler for one dynamic-read route.

Receives the governed request (statement, bound params, effective caps, effective timeout,
resolved tenant) and the :class:`MockState`, and returns the rows the statement would have
produced. Returning more than ``request.row_probe`` rows is a handler bug the adapter refuses,
so a scripted overflow says ``row_probe`` rows and no more."""


@final
@attrs.define(slots=True)
class MockDynamicReadRegistry:
    """Programmable in-memory dynamic-read handlers, keyed by route (spec) name."""

    _handlers: dict[str, MockDynamicReadHandler] = attrs.field(factory=dict)

    def on(
        self,
        route: StrKey | str,
        handler: MockDynamicReadHandler,
    ) -> MockDynamicReadRegistry:
        """Register *handler* for dynamic-read *route*. Returns self (chainable)."""

        self._handlers[str(route)] = handler
        return self

    def handler_for(self, route: str) -> MockDynamicReadHandler | None:
        return self._handlers.get(route)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDynamicReadAdapter(DynamicReadAdapter):
    """In-memory ``DynamicReadPort`` bound to one spec + a handler registry."""

    state: MockState
    registry: MockDynamicReadRegistry

    # ....................... #

    async def _fetch_rows(self, request: DynamicReadRequest) -> Sequence[JsonDict]:
        handler = self.registry.handler_for(str(self.spec.name))

        if handler is None:
            raise exc.configuration(
                f"MockDynamicRead {self.spec.name!r}: no handler registered — register one via "
                "MockDynamicReadRegistry.on()",
                code="mock.dynamic_read.unprogrammed",
            )

        result = handler(request, self.state)

        if inspect.isawaitable(result):
            result = await result

        return self._validated_rows(result, request)

    # ....................... #

    def _validated_rows(
        self,
        rows: Any,
        request: DynamicReadRequest,
    ) -> Sequence[JsonDict]:
        """Hold the handler to what a real engine could have returned.

        A real ``_fetch_rows`` stops at ``row_probe`` because that is all it asked the server
        for. A handler that returns more would let the mock's row-cap check fire on a number no
        engine would have produced, and the differential would be comparing two different
        things.
        """

        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            raise exc.internal(
                f"MockDynamicRead {self.spec.name!r} handler must return a sequence of row "
                f"mappings, got {type(rows).__name__}.",
            )

        # The cast is for pyright, which otherwise carries the element type through as Unknown
        # and reddens every use below. mypy sees the ``isinstance`` narrowing and calls the same
        # cast redundant — hence the ignore. Two checkers, opposite complaints, one line.
        materialized = list(cast(Sequence[Any], rows))  # type: ignore[redundant-cast]

        if len(materialized) > request.row_probe:
            raise exc.internal(
                f"MockDynamicRead {self.spec.name!r} handler returned "
                f"{len(materialized)} rows, more than the {request.row_probe} a real adapter "
                "would have fetched; return at most row_probe rows.",
            )

        for row in materialized:
            if not isinstance(row, dict):
                raise exc.internal(
                    f"MockDynamicRead {self.spec.name!r} handler must return row mappings, got "
                    f"{type(row).__name__}.",
                )

        return [dict(row) for row in materialized]


# ....................... #

__all__ = [
    "MockDynamicReadAdapter",
    "MockDynamicReadHandler",
    "MockDynamicReadRegistry",
]
