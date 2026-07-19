"""Socket.IO command routing with a built-in error and identity boundary.

Every registered event handler is wrapped in an error boundary: a
:class:`~forze.base.exceptions.CoreException` raised during dispatch is
translated into the structured error acknowledgement built by
:func:`forze_socketio.exceptions.build_core_exception_ack`, and any other
exception is logged and acked with a generic internal-error payload (see
:mod:`forze_socketio.exceptions` for the exact ack shape). The error payload is
returned as the handler's return value, which python-socketio delivers as the
event acknowledgement when the client requested one.

Identity is bound per event through an optional ``identity_resolver``: it runs
once at connect time (receiving the connection ``environ`` and ``auth``
payload), the resolved :class:`~forze.application.contracts.authn.AuthnIdentity`
is stored in the Socket.IO session, and each dispatched event binds it onto the
invocation context (``ctx.inv_ctx.bind_identity(authn=...)``) around handler
execution. Tenant resolution stays the ``context_factory``'s job. Without a
resolver nothing changes: no connect handler is registered and no identity is
bound.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractContextManager, nullcontext
from inspect import isawaitable
from typing import Any, Final, final

import attrs
from pydantic import ValidationError
from socketio.async_server import AsyncServer
from socketio.exceptions import ConnectionRefusedError as SocketIOConnectionRefusedError

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.integrations.realtime import RealtimeCommandRoute
from forze.base.exceptions import CoreException, FrameErr, exc, guard_frame
from forze.base.primitives import StrKey
from forze.base.scrubbing import sanitize_pydantic_errors

from .exceptions import (
    GENERIC_INTERNAL_DETAIL,
    is_server_error_kind,
    log_server_error,
    render_error_ack,
)

# ----------------------- #

IDENTITY_SESSION_KEY: Final[str] = "forze.authn_identity"
"""Socket.IO session key holding the identity resolved at connect time."""

# ....................... #

ExecutionContextFactoryPort = Callable[
    ["SocketIORequest"],
    ExecutionContext | Awaitable[ExecutionContext],
]
"""Factory that builds request-scoped :class:`ExecutionContext` instances."""

HandlerResolverPort = Callable[[StrKey, ExecutionContext], Handler[Any, Any]]
"""Resolver that maps operation keys to composed handlers."""

IdentityResolverPort = Callable[
    ["SocketIOConnect"],
    AuthnIdentity | None | Awaitable[AuthnIdentity | None],
]
"""Resolver that authenticates a Socket.IO connection.

Called once per connection with the connect-time metadata (``environ`` and the
client ``auth`` payload). Return the authenticated identity, ``None`` for an
anonymous connection, or raise a client-safe :class:`CoreException` (for
example ``exc.authentication``) to refuse the connection.
"""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SocketIOConnect:
    """Inbound Socket.IO connection metadata passed to the identity resolver."""

    sid: str
    """Socket.IO session id for the connecting client."""

    namespace: str
    """Socket.IO namespace being connected to."""

    environ: Mapping[str, Any]
    """WSGI/ASGI environ dictionary python-socketio passes to connect handlers."""

    auth: Any = None
    """Authentication payload sent by the client, or :obj:`None`."""


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

# The typed command route lives in the transport-neutral kernel — the raw-WebSocket
# route dispatches through the same declaration, so a command's contract cannot
# drift between duplex transports. The established name stays.
SocketIOCommandRoute = RealtimeCommandRoute


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
        :raises exc.internal: If the event is already registered.
        """
        if route.event in self.__commands:
            raise exc.internal(
                f"Socket.IO event `{route.event}` is already registered for namespace `{self.namespace}`"
            )

        self.__commands[route.event] = route

        return self

    # ....................... #

    def command(
        self,
        *,
        event: str,
        operation: StrKey,
        payload_type: Any,
        ack_type: Any = None,
    ) -> "SocketIONamespaceRouter":
        """Add a typed command route.

        :param event: Socket.IO event name.
        :param operation: Operation key on the frozen registry.
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
        operation_resolver: HandlerResolverPort,
        identity_resolver: IdentityResolverPort | None = None,
    ) -> "SocketIONamespaceRouter":
        """Bind registered command routes to a Socket.IO server.

        Every event handler is wrapped in an error boundary: a
        :class:`CoreException` is acked as the structured error payload built by
        :func:`forze_socketio.exceptions.build_core_exception_ack`; any other
        exception is logged and acked with a generic internal-error payload.

        When *identity_resolver* is provided, a ``connect`` handler is
        registered for this namespace: the resolver runs once per connection,
        the resolved identity is stored in the Socket.IO session, and each
        dispatched event binds it onto the invocation context around handler
        execution. A client-safe :class:`CoreException` raised by the resolver
        (for example ``exc.authentication``) refuses the connection with its
        summary; server-side errors are logged and refused with a generic
        message.

        :param sio: Socket.IO async server.
        :param context_factory: Factory for request-scoped execution context.
        :param operation_resolver: Resolver that builds a handler by operation key.
        :param identity_resolver: Optional connection-time identity resolver.
        :returns: Current router for chaining.
        """
        namespace = self.namespace

        if identity_resolver is not None:
            sio.on(
                "connect",
                handler=_build_connect_handler(
                    sio,
                    namespace=namespace,
                    identity_resolver=identity_resolver,
                ),
                namespace=namespace,
            )

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

                outcome = await guard_frame(
                    lambda: _dispatch_event(
                        sio,
                        request,
                        _route,
                        payload,
                        context_factory=context_factory,
                        operation_resolver=operation_resolver,
                    ),
                    on_server_error=lambda core, error: log_server_error(error, core=core),
                )

                if isinstance(outcome, FrameErr):
                    return render_error_ack(outcome.envelope)

                return outcome.value

            sio.on(route.event, handler=handler, namespace=namespace)

        return self


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class ForzeSocketIOAdapter:
    """Socket.IO transport adapter for routing command events to handlers."""

    sio: AsyncServer
    """Bound Socket.IO server instance."""

    context_factory: ExecutionContextFactoryPort
    """Factory that builds request-scoped execution contexts."""

    operation_resolver: HandlerResolverPort
    """Resolver that builds composed handlers from operation keys."""

    identity_resolver: IdentityResolverPort | None = None
    """Optional connection-time identity resolver.

    When provided, each attached namespace authenticates connections with it
    and binds the resolved identity onto the invocation context per event.
    When :obj:`None` (the default) behavior is unchanged: no connect handler is
    registered and identity stays ``context_factory``-driven.
    """

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
        :raises exc.internal: If namespace was already attached.
        """
        if router.namespace in self.__routers:
            raise exc.internal(f"Socket.IO namespace `{router.namespace}` is already attached")

        self.__routers[router.namespace] = router
        router.bind(
            self.sio,
            context_factory=self.context_factory,
            operation_resolver=self.operation_resolver,
            identity_resolver=self.identity_resolver,
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


async def _dispatch_event(
    sio: AsyncServer,
    request: SocketIORequest,
    route: SocketIOCommandRoute[Any, Any],
    payload: Any,
    *,
    context_factory: ExecutionContextFactoryPort,
    operation_resolver: HandlerResolverPort,
) -> Any:
    """Resolve the context, validate the payload, run the operation, parse the ack.

    The transport-neutral error boundary (:func:`guard_frame`) wraps this thunk:
    a :class:`CoreException` (including payload-validation failures) or any other
    exception raised here is projected into the structured error ack.
    """

    ctx = await _resolve_context(context_factory, request)
    args = _parse_payload(route, payload)

    # Bind a connect-time identity whenever one was stored on the session — regardless of
    # whether this adapter has an ``identity_resolver``. ``attach_realtime_connection`` is
    # the documented connect/auth path and stores the principal here with the resolver left
    # ``None``; gating on the resolver would skip it, so a socket could join its principal
    # room yet run command events with no ambient authn (denied by auth-required hooks).
    session = await sio.get_session(request.sid, namespace=request.namespace)
    binding: AbstractContextManager[None] = nullcontext()

    if IDENTITY_SESSION_KEY in session:
        identity: AuthnIdentity | None = session[IDENTITY_SESSION_KEY]
        binding = ctx.inv_ctx.bind_identity(authn=identity)

    with binding:
        op = operation_resolver(route.operation, ctx)
        result = await op(args)

    return route.parse_ack(result)


# ....................... #


async def _resolve_context(
    factory: ExecutionContextFactoryPort,
    request: SocketIORequest,
) -> ExecutionContext:
    value = factory(request)

    if isawaitable(value):
        return await value

    return value


# ....................... #


async def _resolve_identity(
    resolver: IdentityResolverPort,
    connect: SocketIOConnect,
) -> AuthnIdentity | None:
    value = resolver(connect)

    if isawaitable(value):
        return await value

    return value


# ....................... #


def _parse_payload(route: SocketIOCommandRoute[Any, Any], payload: Any) -> Any:
    """Parse the inbound payload, translating validation failures to client-safe errors."""

    try:
        return route.parse_payload(payload)

    except ValidationError as error:
        raise exc.validation(
            "Invalid event payload",
            code="socketio.invalid_payload",
            details={"errors": sanitize_pydantic_errors(error.errors())},
        ) from error


# ....................... #


def _build_connect_handler(
    sio: AsyncServer,
    *,
    namespace: str,
    identity_resolver: IdentityResolverPort,
) -> Callable[..., Awaitable[None]]:
    """Build the namespace ``connect`` handler that resolves and stores identity."""

    async def connect_handler(
        sid: str,
        environ: Mapping[str, Any],
        auth: Any = None,
    ) -> None:
        connect = SocketIOConnect(
            sid=sid,
            namespace=namespace,
            environ=environ,
            auth=auth,
        )

        try:
            identity = await _resolve_identity(identity_resolver, connect)

        except SocketIOConnectionRefusedError:
            raise

        except CoreException as error:
            if is_server_error_kind(error.kind):
                log_server_error(error, core=error)

                raise SocketIOConnectionRefusedError(GENERIC_INTERNAL_DETAIL) from error

            raise SocketIOConnectionRefusedError(error.summary) from error

        except Exception as error:
            log_server_error(error)

            raise SocketIOConnectionRefusedError(GENERIC_INTERNAL_DETAIL) from error

        session = await sio.get_session(sid, namespace=namespace)
        session[IDENTITY_SESSION_KEY] = identity
        await sio.save_session(sid, session, namespace=namespace)

    return connect_handler
