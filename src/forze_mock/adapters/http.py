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
from typing import Any, final, overload

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


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class _Registration:
    """One handler, and the declaration it was registered against if it named one.

    Keeping the spec is what closes the door a name leaves open: registration keys by
    ``(service, op name)``, so a handler type-checked against a *foreign* declaration lands
    under the local operation's name and would be handed the local args model — the very
    mismatch taking the spec was meant to prevent, arriving from the other side. The adapter
    compares the two before it calls anything.
    """

    handler: HttpHandler
    declared: HttpOperationSpec[Any, Any] | None


# ....................... #


def _return_type_allows_empty(return_type: type[BaseModel]) -> bool:
    return not any(field_info.is_required() for field_info in return_type.model_fields.values())


# ....................... #


@final
@attrs.define(slots=True)
class MockHttpRegistry:
    """Programmable in-memory HTTP responses, keyed by ``(service name, op name)``."""

    _handlers: dict[tuple[str, str], _Registration] = attrs.field(
        factory=dict[tuple[str, str], _Registration]
    )

    @overload
    def on[In: BaseModel](
        self,
        service: StrKey | str,
        op: HttpOperationSpec[In, Any],
        handler: Callable[[In | None], Any],
    ) -> MockHttpRegistry: ...

    @overload
    def on(
        self,
        service: StrKey | str,
        op: StrKey | str,
        handler: Callable[[Any], Any],
    ) -> MockHttpRegistry: ...

    def on(
        self,
        service: StrKey | str,
        op: StrKey | str | HttpOperationSpec[Any, Any],
        handler: Callable[[Any], Any],
    ) -> MockHttpRegistry:
        """Register *handler* for operation *op* on *service*. Returns self (chainable).

        Naming the operation by its :class:`HttpOperationSpec` is what ties the handler to
        the declaration: ``In`` is solved from the spec's ``args_type`` *and* from the
        handler's parameter, so a handler written against the wrong model is refused here
        rather than receiving a model it cannot read at the call. A key leaves the two
        unrelated, and is the right form where the operation is chosen at runtime.

        The registry never sees the :class:`HttpServiceSpec`, so it cannot tell whether the
        spec belongs to the service named here — it keeps the spec instead, and
        :class:`MockHttpServiceAdapter` compares it against the operation it resolved before
        handing the handler anything.
        """

        declared = op if isinstance(op, HttpOperationSpec) else None
        key = str(declared.name) if declared is not None else str(op)
        self._handlers[(str(service), key)] = _Registration(handler=handler, declared=declared)

        return self

    def handler_for(self, service: str, op: str) -> HttpHandler | None:
        registration = self._handlers.get((service, op))

        return None if registration is None else registration.handler

    def declared_for(self, service: str, op: str) -> HttpOperationSpec[Any, Any] | None:
        """The declaration a spec-based registration kept, or ``None`` for a keyed one."""

        registration = self._handlers.get((service, op))

        return None if registration is None else registration.declared


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class MockHttpServiceAdapter(HttpServicePort):
    """In-memory ``HttpServicePort`` bound to one service spec + a handler registry."""

    spec: HttpServiceSpec
    registry: MockHttpRegistry

    # ....................... #

    @overload
    async def invoke[In: BaseModel, Out: BaseModel](
        self,
        op: HttpOperationSpec[In, Out],
        args: In | None = None,
    ) -> Out: ...

    @overload
    async def invoke(
        self,
        op: StrKey,
        args: BaseModel | None = None,
    ) -> BaseModel: ...

    # Repeated from the port rather than inherited: this class overrides `invoke`, and an
    # override replaces the overloads it is declared against. Without them a caller holding
    # the adapter itself — every test that builds one directly — gets `Any` back where the
    # same call through the port gives the operation's declared model.
    async def invoke(
        self,
        op: StrKey | HttpOperationSpec[Any, Any],
        args: BaseModel | None = None,
    ) -> Any:
        operation = self.spec.operation(op)
        self._validate_args(operation, args)

        handler = self.registry.handler_for(str(self.spec.name), str(operation.name))
        declared = self.registry.declared_for(str(self.spec.name), str(operation.name))

        if declared is not None and declared != operation:
            raise exc.validation(
                f"MockHttpService {self.spec.name!r}: the handler for operation "
                f"{operation.name!r} was registered against a different declaration, so it "
                "would be handed a model it was not written for. Register it against this "
                "service's own operation, or by name if that is deliberate.",
                code="mock.http.handler_declaration_mismatch",
            )

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
