"""In-memory :class:`~forze.application.contracts.http.HttpServicePort` for tests / simulation.

No real I/O: each outbound operation is answered by a handler registered on a
:class:`MockHttpRegistry`, so an app's HTTP calls resolve in-process and
deterministically (zero external services). Args are validated against the
operation's ``args_type`` and the handler's result is coerced to its
``return_type`` — the same contract the real httpx adapter enforces.

The per-operation handler is also the seam where later DST phases attach fault
injection (raise / delay / malformed response) without touching call sites.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.http import (
    HttpOperationSpec,
    HttpServicePort,
    HttpServiceSpec,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #

HttpHandler = Callable[[Any], Any]
"""Handler for one operation: receives the (validated) args model or ``None`` and
returns the response — a ``return_type`` instance, a dict to validate, ``None``
for an empty body, or an awaitable of any of those.

The parameter is ``Any`` because this is the *stored* shape, one dict holding the handlers
for every operation on every service. What each handler actually accepts is stated where it
is registered — see :meth:`MockHttpRegistry.on`, which takes the narrower form. Spelling it
``BaseModel | None`` here refused every handler that named its own args model, since a
parameter type is contravariant: a function of ``ModelArgs | None`` is not a function of
``BaseModel | None``."""


def _return_type_allows_empty(return_type: type[BaseModel]) -> bool:
    return not any(field_info.is_required() for field_info in return_type.model_fields.values())


# ....................... #


@final
@attrs.define(slots=True)
class MockHttpRegistry:
    """Programmable in-memory HTTP responses, keyed by ``(service name, op name)``."""

    _handlers: dict[tuple[str, str], HttpHandler] = attrs.field(
        factory=dict[tuple[str, str], HttpHandler]
    )

    def on[In: BaseModel](
        self,
        service: StrKey | str,
        op: StrKey | str,
        handler: Callable[[In | None], Any],
    ) -> MockHttpRegistry:
        """Register *handler* for operation *op* on *service*. Returns self (chainable).

        The handler's own parameter type is what ``In`` resolves to, so a handler written
        against the operation's ``args_type`` is accepted as it stands — one keyed to
        ``BaseModel | None`` no longer being the only shape that fits. Nothing is checked
        against the operation's declaration here: the registry is keyed by name and never
        sees the spec. :class:`MockHttpServiceAdapter` does that check, at the call.
        """

        self._handlers[(str(service), str(op))] = handler
        return self

    def handler_for(self, service: str, op: str) -> HttpHandler | None:
        return self._handlers.get((service, op))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class MockHttpServiceAdapter(HttpServicePort):
    """In-memory ``HttpServicePort`` bound to one service spec + a handler registry."""

    spec: HttpServiceSpec
    registry: MockHttpRegistry

    # ....................... #

    async def invoke(
        self,
        op: StrKey | HttpOperationSpec[Any, Any],
        args: BaseModel | None = None,
    ) -> Any:
        operation = self.spec.operation(op)
        self._validate_args(operation, args)

        handler = self.registry.handler_for(str(self.spec.name), str(operation.name))

        if handler is None:
            raise exc.configuration(
                f"MockHttpService {self.spec.name!r}: no handler registered for "
                f"operation {operation.name!r} — register one via MockHttpRegistry.on()",
                code="mock.http.unprogrammed",
            )

        result = handler(args)

        if inspect.isawaitable(result):
            result = await result

        return self._coerce(operation, result)

    # ....................... #

    def _validate_args(
        self,
        operation: HttpOperationSpec[Any, Any],
        args: BaseModel | None,
    ) -> None:
        if args is None:
            return

        if operation.args_type is None:
            raise exc.validation(f"HTTP operation {operation.name!r} takes no args")

        if not isinstance(args, operation.args_type):
            raise exc.validation(
                f"HTTP operation {operation.name!r}: args must be "
                f"{operation.args_type.__name__}, got {type(args).__name__}"
            )

    # ....................... #

    def _coerce(
        self,
        operation: HttpOperationSpec[Any, Any],
        result: Any,
    ) -> BaseModel:
        return_type = operation.return_type

        if result is None:
            if operation.allows_empty_body or _return_type_allows_empty(return_type):
                return return_type.model_construct()

            raise exc.validation(f"HTTP operation {operation.name!r} handler returned no body")

        if isinstance(result, return_type):
            return result

        if isinstance(result, BaseModel):
            result = result.model_dump()

        return return_type.model_validate(result)
