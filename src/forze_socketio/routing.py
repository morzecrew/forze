from ._compat import require_socketio

require_socketio()

# ....................... #

from inspect import isawaitable
from typing import Any, Awaitable, Callable, final

import attrs
from pydantic import TypeAdapter
from socketio.async_server import AsyncServer

from forze.application.execution import (
    ExecutionContext,
    Usecase,
    UsecasePlan,
    UsecaseRegistry,
)
from forze.application.execution.plan import OpKey
from forze.base.errors import CoreError

# ----------------------- #

ExecutionContextFactoryPort = Callable[
    ["SocketIORequest"],
    ExecutionContext | Awaitable[ExecutionContext],
]
"""Factory that builds request-scoped :class:`ExecutionContext` instances."""

UsecaseResolverPort = Callable[[ExecutionContext, OpKey], Usecase[Any, Any]]
"""Resolver that maps operation keys to composed usecases."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SocketIORequest:
    """Inbound Socket.IO request metadata for context creation."""

    sid: str
    """Socket.IO session id for the connected client."""

    namespace: str
    """Socket.IO namespace that received the event."""

    event: str
    """Socket.IO event name."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SocketIOCommandRoute[Args, Ack]:
    """Typed mapping between an inbound event and a usecase operation."""

    event: str
    """Socket.IO event name."""

    operation: OpKey
    """Usecase operation key resolved by :class:`UsecaseRegistry`."""

    payload_type: Any
    """Validation type consumed by :class:`pydantic.TypeAdapter` for inbound payload."""

    ack_type: Any = None
    """Optional validation type for acknowledgement payload."""

    # ....................... #

    def parse_payload(self, payload: Any) -> Args:
        """Validate and coerce the inbound payload.

        :param payload: Raw event payload from Socket.IO.
        :returns: Parsed payload value passed to the usecase.
        """
        return TypeAdapter[Any](self.payload_type).validate_python(payload)

    # ....................... #

    def parse_ack(self, value: Any) -> Ack | Any:
        """Validate and normalize the usecase result for Socket.IO acknowledgement.

        :param value: Raw usecase result.
        :returns: JSON-compatible acknowledgement payload.
        """
        if self.ack_type is None:
            return value

        adapter = TypeAdapter[Any](self.ack_type)
        validated = adapter.validate_python(value)

        return adapter.dump_python(validated, mode="json")


# ....................... #


@final
@attrs.define(slots=True)
class SocketIONamespaceRouter:
    """Declarative registry of command events for a namespace."""

    namespace: str = "/"
    """Socket.IO namespace (for example ``"/chat"``)."""

    __commands: dict[str, SocketIOCommandRoute[Any, Any]] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
    )
    """Event-to-command route mapping for this namespace."""

    # ....................... #

    @property
    def commands(self) -> tuple[SocketIOCommandRoute[Any, Any], ...]:
        """Return all registered command routes."""
        return tuple(self.__commands.values())

    # ....................... #

    def register(
        self,
        route: SocketIOCommandRoute[Any, Any],
    ) -> "SocketIONamespaceRouter":
        """Register a command route for this namespace.

        :param route: Event mapping configuration.
        :returns: Current router for chaining.
        :raises CoreError: If the event is already registered.
        """
        if route.event in self.__commands:
            raise CoreError(
                f"Socket.IO event `{route.event}` is already registered for namespace `{self.namespace}`"
            )

        self.__commands[route.event] = route

        return self

    # ....................... #

    def command(
        self,
        *,
        event: str,
        operation: OpKey,
        payload_type: Any,
        ack_type: Any = None,
    ) -> "SocketIONamespaceRouter":
        """Add a typed command route.

        :param event: Socket.IO event name.
        :param operation: Usecase operation key.
        :param payload_type: Type used to validate inbound payload.
        :param ack_type: Optional type used to validate acknowledgement payload.
        :returns: Current router for chaining.
        """
        return self.register(
            SocketIOCommandRoute[Any, Any](
                event=event,
                operation=operation,
                payload_type=payload_type,
                ack_type=ack_type,
            )
        )

    # ....................... #

    def bind(
        self,
        sio: AsyncServer,
        *,
        context_factory: ExecutionContextFactoryPort,
        usecase_resolver: UsecaseResolverPort,
    ) -> "SocketIONamespaceRouter":
        """Bind registered command routes to a Socket.IO server.

        :param sio: Socket.IO async server.
        :param context_factory: Factory for request-scoped execution context.
        :param usecase_resolver: Resolver that builds a usecase by operation.
        :returns: Current router for chaining.
        """
        namespace = self.namespace

        for route in self.commands:

            async def handler(
                sid: str,
                payload: Any = None,
                *,
                _namespace: str = namespace,
                _route: SocketIOCommandRoute[Any, Any] = route,
            ) -> Any:
                request = SocketIORequest(
                    sid=sid,
                    namespace=_namespace,
                    event=_route.event,
                )
                ctx = await _resolve_context(context_factory, request)
                args = _route.parse_payload(payload)
                usecase = usecase_resolver(ctx, _route.operation)
                result = await usecase(args)

                return _route.parse_ack(result)

            sio.on(route.event, handler=handler, namespace=namespace)

        return self


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class ForzeSocketIOAdapter:
    """Socket.IO transport adapter for routing command events to usecases."""

    sio: AsyncServer
    """Bound Socket.IO server instance."""

    context_factory: ExecutionContextFactoryPort
    """Factory that builds request-scoped execution contexts."""

    usecase_resolver: UsecaseResolverPort
    """Operation resolver used to build composed usecases."""

    __routers: dict[str, SocketIONamespaceRouter] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
    )
    """Namespace routers attached to this adapter."""

    # ....................... #

    @property
    def routers(self) -> tuple[SocketIONamespaceRouter, ...]:
        """Return namespace routers currently attached to the adapter."""
        return tuple(self.__routers.values())

    # ....................... #

    def include_router(self, router: SocketIONamespaceRouter) -> "ForzeSocketIOAdapter":
        """Attach a namespace router and bind its handlers.

        :param router: Namespace router with command registrations.
        :returns: Current adapter for chaining.
        :raises CoreError: If namespace was already attached.
        """
        if router.namespace in self.__routers:
            raise CoreError(
                f"Socket.IO namespace `{router.namespace}` is already attached"
            )

        self.__routers[router.namespace] = router
        router.bind(
            self.sio,
            context_factory=self.context_factory,
            usecase_resolver=self.usecase_resolver,
        )

        return self

    # ....................... #

    def include_routers(
        self,
        *routers: SocketIONamespaceRouter,
    ) -> "ForzeSocketIOAdapter":
        """Attach multiple namespace routers in order.

        :param routers: Namespace routers to attach.
        :returns: Current adapter for chaining.
        """
        for router in routers:
            self.include_router(router)

        return self


# ....................... #


def make_registry_usecase_resolver(
    registry: UsecaseRegistry,
    *,
    plan: UsecasePlan | None = None,
) -> UsecaseResolverPort:
    """Build a resolver backed by :class:`UsecaseRegistry`.

    :param registry: Base usecase registry.
    :param plan: Optional extra plan merged before resolution.
    :returns: Callable resolver suitable for :class:`ForzeSocketIOAdapter`.
    """
    resolved_registry = registry.extend_plan(plan) if plan is not None else registry

    def resolver(ctx: ExecutionContext, operation: OpKey) -> Usecase[Any, Any]:
        return resolved_registry.resolve(operation, ctx)

    return resolver


# ....................... #


async def _resolve_context(
    factory: ExecutionContextFactoryPort,
    request: SocketIORequest,
) -> ExecutionContext:
    value = factory(request)

    if isawaitable(value):
        return await value

    return value
