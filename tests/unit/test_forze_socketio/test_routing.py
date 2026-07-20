"""Unit tests for :mod:`forze_socketio.routing`."""

import asyncio
import io
import json
from typing import Any
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel
from socketio.exceptions import ConnectionRefusedError as SocketIOConnectionRefusedError

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.execution import Handler
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, exc
from forze_socketio.exceptions import GENERIC_INTERNAL_DETAIL
from forze_socketio.routing import (
    IDENTITY_SESSION_KEY,
    ForzeSocketIOAdapter,
    SocketIOConnect,
    SocketIONamespaceRouter,
    SocketIORequest,
)
from tests.support.execution_context import context_from_deps

# ----------------------- #


def json_records(stream: io.StringIO) -> list[dict]:
    out: list[dict] = []
    for line in stream.getvalue().strip().split("\n"):
        line = line.strip()
        if line.startswith("{"):
            out.append(json.loads(line))
    return out


# ----------------------- #


class StubSocketIOServer:
    def __init__(self) -> None:
        self.handlers: dict[tuple[str, str], Any] = {}
        self.sessions: dict[tuple[str, str], dict[str, Any]] = {}

    def on(
        self,
        event: str,
        handler: Any = None,
        namespace: str | None = None,
    ) -> Any:
        namespace = namespace or "/"

        if handler is None:

            def decorator(func: Any) -> Any:
                self.handlers[(namespace, event)] = func
                return func

            return decorator

        self.handlers[(namespace, event)] = handler
        return handler

    async def get_session(
        self,
        sid: str,
        namespace: str | None = None,
    ) -> dict[str, Any]:
        return self.sessions.setdefault((namespace or "/", sid), {})

    async def save_session(
        self,
        sid: str,
        session: dict[str, Any],
        namespace: str | None = None,
    ) -> None:
        self.sessions[(namespace or "/", sid)] = session


class EchoPayload(BaseModel):
    text: str


class EchoAck(BaseModel):
    echoed: str


@attrs.define(slots=True, kw_only=True, frozen=True)
class EchoHandler(Handler[EchoPayload, EchoAck]):
    async def __call__(self, args: EchoPayload) -> EchoAck:
        return EchoAck(echoed=args.text)


def _frozen_echo_registry() -> OperationRegistry:
    return OperationRegistry(
        handlers={"chat.echo": lambda _ctx: EchoHandler()},
    ).freeze()


class TestSocketIORouting:
    @pytest.mark.asyncio
    async def test_event_dispatch_validates_and_runs_handler(self) -> None:
        sio = StubSocketIOServer()
        registry = _frozen_echo_registry()
        request_log: list[SocketIORequest] = []

        def context_factory(request: SocketIORequest) -> ExecutionContext:
            request_log.append(request)
            return context_from_deps(Deps())

        router = SocketIONamespaceRouter(namespace="/chat").command(
            event="echo",
            operation="chat.echo",
            payload_type=EchoPayload,
            ack_type=EchoAck,
        )
        adapter = ForzeSocketIOAdapter(
            sio=sio,  # pyright: ignore[reportArgumentType]
            context_factory=context_factory,
            operation_resolver=registry.resolve,
        )
        adapter.include_router(router)

        handler = sio.handlers[("/chat", "echo")]
        result = await handler("sid-1", {"text": "hello"})

        assert result == {"echoed": "hello"}
        assert request_log == [
            SocketIORequest(sid="sid-1", namespace="/chat", event="echo")
        ]

    def test_router_raises_on_duplicate_event(self) -> None:
        router = SocketIONamespaceRouter(namespace="/chat").command(
            event="echo",
            operation="chat.echo",
            payload_type=EchoPayload,
            ack_type=EchoAck,
        )

        with pytest.raises(CoreException, match="already registered"):
            router.command(
                event="echo",
                operation="chat.echo",
                payload_type=EchoPayload,
                ack_type=EchoAck,
            )


@pytest.mark.asyncio
async def test_event_dispatch_without_ack_type_returns_raw_value() -> None:
    sio = StubSocketIOServer()
    registry = _frozen_echo_registry()

    def context_factory(request: SocketIORequest) -> ExecutionContext:
        return context_from_deps(Deps())

    router = SocketIONamespaceRouter(namespace="/chat").command(
        event="echo",
        operation="chat.echo",
        payload_type=EchoPayload,
        ack_type=None,
    )
    adapter = ForzeSocketIOAdapter(
        sio=sio,  # pyright: ignore[reportArgumentType]
        context_factory=context_factory,
        operation_resolver=registry.resolve,
    )
    adapter.include_router(router)

    handler = sio.handlers[("/chat", "echo")]
    result = await handler("sid-1", {"text": "hello"})

    assert isinstance(result, EchoAck)
    assert result.echoed == "hello"


# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RaisingHandler(Handler[EchoPayload, EchoAck]):
    error: BaseException

    async def __call__(self, args: EchoPayload) -> EchoAck:
        raise self.error


def _bound_failing_handler(error: BaseException) -> Any:
    """Bind a `fail` event backed by a handler raising *error*; return the dispatcher."""

    sio = StubSocketIOServer()
    registry = OperationRegistry(
        handlers={"chat.fail": lambda _ctx: RaisingHandler(error=error)},
    ).freeze()

    router = SocketIONamespaceRouter(namespace="/chat").command(
        event="fail",
        operation="chat.fail",
        payload_type=EchoPayload,
    )
    ForzeSocketIOAdapter(
        sio=sio,  # pyright: ignore[reportArgumentType]
        context_factory=lambda _request: context_from_deps(Deps()),
        operation_resolver=registry.resolve,
    ).include_router(router)

    return sio.handlers[("/chat", "fail")]


class TestSocketIOErrorTranslation:
    @pytest.mark.asyncio
    async def test_core_validation_exception_acked_with_summary(self) -> None:
        handler = _bound_failing_handler(
            exc.validation("Text must not be empty", details={"field": "text"})
        )

        ack = await handler("sid-1", {"text": ""})

        assert ack == {
            "error": {
                "detail": "Text must not be empty",
                "code": "core.validation",
                "kind": "validation",
                "context": {"field": "text"},
            }
        }

    @pytest.mark.asyncio
    async def test_core_infrastructure_exception_acked_generic_and_logged(
        self, error_log_buf: io.StringIO
    ) -> None:
        handler = _bound_failing_handler(exc.infrastructure("Database is down"))

        ack = await handler("sid-1", {"text": "hello"})

        assert ack == {
            "error": {
                "detail": GENERIC_INTERNAL_DETAIL,
                "code": "core.infrastructure",
                "kind": "infrastructure",
            }
        }
        assert "Database is down" not in str(ack)

        (row,) = json_records(error_log_buf)
        assert row["error_kind"] == "infrastructure"

    @pytest.mark.asyncio
    async def test_unexpected_exception_acked_generic_and_logged_critical(
        self, error_log_buf: io.StringIO
    ) -> None:
        handler = _bound_failing_handler(ValueError("sensitive internals"))

        ack = await handler("sid-1", {"text": "hello"})

        assert ack == {
            "error": {
                "detail": GENERIC_INTERNAL_DETAIL,
                "code": "core.internal",
                "kind": "internal",
            }
        }
        assert "sensitive internals" not in str(ack)

        (row,) = json_records(error_log_buf)
        assert row["level"] == "critical"
        assert row["event"] == "Unhandled exception"

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates_unconverted(
        self, error_log_buf: io.StringIO
    ) -> None:
        handler = _bound_failing_handler(asyncio.CancelledError())

        with pytest.raises(asyncio.CancelledError):
            await handler("sid-1", {"text": "hello"})

        assert json_records(error_log_buf) == []

    @pytest.mark.asyncio
    async def test_invalid_payload_acked_as_validation(self) -> None:
        handler = _bound_failing_handler(ValueError("never reached"))

        ack = await handler("sid-1", {"text": 12.5})

        assert ack["error"]["code"] == "socketio.invalid_payload"
        assert ack["error"]["kind"] == "validation"
        assert ack["error"]["detail"] == "Invalid event payload"
        assert ack["error"]["context"]["errors"]


# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class WhoAmIHandler(Handler[EchoPayload, Any]):
    ctx: ExecutionContext

    async def __call__(self, args: EchoPayload) -> Any:
        authn = self.ctx.inv_ctx.get_authn()

        return {
            "principal_id": str(authn.principal_id) if authn is not None else None,
        }


def _whoami_setup(identity_resolver: Any) -> StubSocketIOServer:
    sio = StubSocketIOServer()
    registry = OperationRegistry(
        handlers={"chat.whoami": lambda ctx: WhoAmIHandler(ctx=ctx)},
    ).freeze()

    router = SocketIONamespaceRouter(namespace="/chat").command(
        event="whoami",
        operation="chat.whoami",
        payload_type=EchoPayload,
    )
    ForzeSocketIOAdapter(
        sio=sio,  # pyright: ignore[reportArgumentType]
        context_factory=lambda _request: context_from_deps(Deps()),
        operation_resolver=registry.resolve,
        identity_resolver=identity_resolver,
    ).include_router(router)

    return sio


class TestSocketIOIdentity:
    @pytest.mark.asyncio
    async def test_identity_resolved_on_connect_and_bound_per_event(self) -> None:
        principal_id = uuid4()
        seen_connects: list[SocketIOConnect] = []

        async def resolver(connect: SocketIOConnect) -> AuthnIdentity | None:
            seen_connects.append(connect)

            if connect.auth == {"token": "good"}:
                return AuthnIdentity(principal_id=principal_id)

            raise exc.authentication("Invalid token")

        sio = _whoami_setup(resolver)
        connect = sio.handlers[("/chat", "connect")]
        whoami = sio.handlers[("/chat", "whoami")]

        await connect("sid-1", {"REQUEST_METHOD": "GET"}, {"token": "good"})

        session = sio.sessions[("/chat", "sid-1")]
        assert session[IDENTITY_SESSION_KEY] == AuthnIdentity(principal_id=principal_id)
        assert seen_connects == [
            SocketIOConnect(
                sid="sid-1",
                namespace="/chat",
                environ={"REQUEST_METHOD": "GET"},
                auth={"token": "good"},
            )
        ]

        ack = await whoami("sid-1", {"text": "who am i"})
        assert ack == {"principal_id": str(principal_id)}

    @pytest.mark.asyncio
    async def test_connect_refused_when_resolver_raises_authentication(self) -> None:
        async def resolver(connect: SocketIOConnect) -> AuthnIdentity | None:
            raise exc.authentication("Invalid token")

        sio = _whoami_setup(resolver)
        connect = sio.handlers[("/chat", "connect")]

        with pytest.raises(SocketIOConnectionRefusedError) as excinfo:
            await connect("sid-1", {"REQUEST_METHOD": "GET"}, None)

        assert excinfo.value.error_args == {"message": "Invalid token"}

    @pytest.mark.asyncio
    async def test_connect_refused_generic_when_resolver_fails_unexpectedly(
        self, error_log_buf: io.StringIO
    ) -> None:
        async def resolver(connect: SocketIOConnect) -> AuthnIdentity | None:
            raise ValueError("sensitive internals")

        sio = _whoami_setup(resolver)
        connect = sio.handlers[("/chat", "connect")]

        with pytest.raises(SocketIOConnectionRefusedError) as excinfo:
            await connect("sid-1", {"REQUEST_METHOD": "GET"}, None)

        assert excinfo.value.error_args == {"message": GENERIC_INTERNAL_DETAIL}

        (row,) = json_records(error_log_buf)
        assert row["level"] == "critical"

    @pytest.mark.asyncio
    async def test_anonymous_identity_allows_connection(self) -> None:
        async def resolver(connect: SocketIOConnect) -> AuthnIdentity | None:
            return None

        sio = _whoami_setup(resolver)
        connect = sio.handlers[("/chat", "connect")]
        whoami = sio.handlers[("/chat", "whoami")]

        await connect("sid-1", {"REQUEST_METHOD": "GET"}, None)

        ack = await whoami("sid-1", {"text": "who am i"})
        assert ack == {"principal_id": None}

    @pytest.mark.asyncio
    async def test_session_identity_is_bound_even_without_a_resolver(self) -> None:
        # attach_realtime_connection stores the principal on the session with
        # identity_resolver left None; command events must still run under it
        principal_id = uuid4()
        sio = _whoami_setup(None)
        await sio.save_session(
            "sid-1",
            {IDENTITY_SESSION_KEY: AuthnIdentity(principal_id=principal_id)},
            namespace="/chat",
        )

        whoami = sio.handlers[("/chat", "whoami")]
        ack = await whoami("sid-1", {"text": "who am i"})

        assert ack == {"principal_id": str(principal_id)}

    async def test_without_resolver_no_connect_handler_and_no_identity(self) -> None:
        sio = _whoami_setup(None)

        assert ("/chat", "connect") not in sio.handlers

        whoami = sio.handlers[("/chat", "whoami")]
        ack = await whoami("sid-1", {"text": "who am i"})

        # no connect handler stored an identity, so the session carries none and the event
        # runs with no ambient authn (the dispatch consults the session but binds nothing)
        assert ack == {"principal_id": None}
        assert IDENTITY_SESSION_KEY not in sio.sessions.get(("/chat", "sid-1"), {})
