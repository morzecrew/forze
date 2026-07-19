"""SSE topic subscriptions — fail-closed authorization, the app's decision.

# covers: forze_fastapi.realtime.sse (_require_topic_grant, authorize_topics wiring,
#         max_topics bound, realtime_topics_unauthorized / realtime_topics_limit)

On Socket.IO, topic-room membership is granted by app code (``enter_room``) after its
own checks; the SSE ``?topics=`` param must not be weaker. Without an authorizer the
subscription is refused, a partial grant is refused naming the denied topics (never
silently narrowed), and the requested set is bounded — client-controlled fan-out
state can't be unbounded.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient
from starlette.types import ASGIApp, Receive, Scope, Send

from forze.application.contracts.authn import AuthnIdentity
from forze.application.execution import ExecutionContext
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
)
from forze.base.exceptions import CoreException
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.realtime import TopicAuthorizer, attach_realtime_sse_route
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

# ----------------------- #

_PRINCIPAL = uuid4()


class _Bind:
    def __init__(self, app: ASGIApp, *, ctx: ExecutionContext) -> None:
        self.app = app
        self.ctx = ctx

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        with self.ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=_PRINCIPAL)):
            await self.app(scope, receive, send)


def _client(
    *, authorize_topics: TopicAuthorizer | None = None, max_topics: int = 32
) -> TestClient:
    ctx = context_from_deps(MockDepsModule(state=MockState())())
    router = APIRouter()
    attach_realtime_sse_route(
        router,
        ctx_dep=lambda: ctx,
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        authorize_topics=authorize_topics,
        max_topics=max_topics,
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    app.add_middleware(_Bind, ctx=ctx)  # type: ignore[arg-type]

    return TestClient(app)


async def _allow_all(
    _ctx: ExecutionContext, _principal: str, _tenant: UUID | None, requested: frozenset[str]
) -> frozenset[str]:
    return requested


# ----------------------- #


class TestTopicAuthorization:
    def test_topics_without_an_authorizer_are_refused(self) -> None:
        response = _client().get("/realtime/sse", params={"topics": "t1"})

        assert response.status_code == 403
        assert response.headers[ERROR_CODE_HEADER] == "realtime_topics_unauthorized"

    def test_no_topics_needs_no_authorizer(self) -> None:
        assert _client().get("/realtime/sse").status_code == 200
        # a topics param that parses to nothing is the same as none
        assert _client().get("/realtime/sse", params={"topics": " , "}).status_code == 200

    def test_full_grant_admits_the_connection(self) -> None:
        response = _client(authorize_topics=_allow_all).get(
            "/realtime/sse", params={"topics": "t1,t2"}
        )

        assert response.status_code == 200

    def test_partial_grant_is_refused_naming_the_denied_topics(self) -> None:
        async def only_t1(
            _ctx: ExecutionContext,
            _principal: str,
            _tenant: UUID | None,
            requested: frozenset[str],
        ) -> frozenset[str]:
            return requested & {"t1"}

        response = _client(authorize_topics=only_t1).get(
            "/realtime/sse", params={"topics": "t1,secret,t2"}
        )

        assert response.status_code == 403
        assert response.headers[ERROR_CODE_HEADER] == "realtime_topics_unauthorized"
        # the denied names ride the client-safe summary (the client sent them)
        assert "'secret'" in response.json()["detail"]
        assert "'t2'" in response.json()["detail"]
        assert "'t1'" not in response.json()["detail"]

    def test_authorizer_sees_the_authenticated_principal(self) -> None:
        seen: list[str] = []

        async def record(
            _ctx: ExecutionContext,
            principal: str,
            _tenant: UUID | None,
            requested: frozenset[str],
        ) -> frozenset[str]:
            seen.append(principal)
            return requested

        _client(authorize_topics=record).get("/realtime/sse", params={"topics": "t1"})

        assert seen == [str(_PRINCIPAL)]


class TestTopicBound:
    def test_requests_over_the_cap_are_refused(self) -> None:
        topics = ",".join(f"t{i}" for i in range(5))
        response = _client(authorize_topics=_allow_all, max_topics=4).get(
            "/realtime/sse", params={"topics": topics}
        )

        assert response.status_code >= 400
        assert response.headers[ERROR_CODE_HEADER] == "realtime_topics_limit"

    def test_non_positive_cap_is_refused_at_attach(self) -> None:
        with pytest.raises(CoreException):
            attach_realtime_sse_route(
                APIRouter(),
                ctx_dep=lambda: None,  # type: ignore[arg-type, return-value]
                mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
                cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
                max_topics=0,
            )
