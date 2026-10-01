"""``require_permission`` gates a hand-written route with the authz hook's own decision.

Every leg runs a real request through FastAPI and the exception handlers, since what the
dependency promises is a response: the status, the body and ``X-Error-Code`` a client sees. The
delegation leg is the one that matters most — a check that asked only about the subject would
admit an agent to a route its user may use.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from typing import Any
from uuid import uuid4

import pytest
from fastapi import Depends, FastAPI, Request, Response
from httpx import ASGITransport, AsyncClient

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import AuthzSpec
from forze.application.execution import InvocationMetadata
from forze.base.exceptions import (
    CoreException,
    DenialPosture,
    configure_denial_posture,
    current_denial_posture,
    exc,
)
from forze.testing import context_from_modules
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.security import require_permission
from forze_identity.authz import ConfigGrants, ConfigGrantsProvider
from forze_mock import MockDepsModule
from forze_mock.adapters.identity import MockAuthzDecisionPort, MockDelegationGrantPort

pytestmark = pytest.mark.unit

SPEC = AuthzSpec(name="main")
ENFORCED = AuthzSpec(name="main", enforce_delegation_grant=True)
WRITE = "ledger.write"
USER = uuid4()
AGENT = uuid4()


def _app(
    module: MockDepsModule,
    identity: AuthnIdentity | None,
    spec: AuthzSpec = SPEC,
    **options: Any,
) -> FastAPI:
    ctx = context_from_modules(module)
    app = FastAPI()
    register_exception_handlers(app)

    @app.middleware("http")
    async def bind(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Any:
        metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

        with ctx.inv_ctx.bind(metadata=metadata, authn=identity):
            return await call_next(request)

    guard = require_permission(WRITE, spec=spec, ctx_dep=lambda: ctx, **options)

    @app.post("/ledger", dependencies=[Depends(guard)])
    async def write() -> dict[str, str]:
        return {"written": "yes"}

    @app.get("/missing")
    async def missing() -> None:
        raise exc.not_found("Ledger 7 not found", resource_type="ledger")

    return app


async def _post(app: FastAPI, path: str = "/ledger") -> tuple[int, bytes, str | None]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await (client.post(path) if path == "/ledger" else client.get(path))

    return response.status_code, response.content, response.headers.get(ERROR_CODE_HEADER)


def _seeded(*principals: Any) -> MockDepsModule:
    module = MockDepsModule()

    for principal in principals:
        MockAuthzDecisionPort(state=module.state).seed_grant(principal, WRITE)

    return module


@pytest.fixture
def posture() -> Iterator[Any]:
    previous = current_denial_posture()

    yield configure_denial_posture

    configure_denial_posture(previous)


# ....................... #


class TestTheGate:
    async def test_a_caller_holding_the_permission_passes(self) -> None:
        status, body, _ = await _post(_app(_seeded(USER), AuthnIdentity(principal_id=USER)))

        assert (status, body) == (200, b'{"written":"yes"}')

    async def test_a_caller_without_it_is_refused_with_the_hooks_denial(self) -> None:
        status, _, code = await _post(_app(_seeded(), AuthnIdentity(principal_id=USER)))

        assert (status, code) == (403, "permission_denied")

    async def test_an_anonymous_caller_is_asked_to_authenticate(self) -> None:
        status, _, code = await _post(_app(_seeded(USER), None))

        assert (status, code) == (401, "auth_required")

    async def test_an_agent_is_refused_on_a_route_its_user_may_use(self) -> None:
        identity = AuthnIdentity(principal_id=USER, actor=AuthnIdentity(principal_id=AGENT))

        status, _, code = await _post(_app(_seeded(USER), identity))

        assert (status, code) == (403, "delegate_denied")

    async def test_a_delegation_both_principals_hold_passes(self) -> None:
        identity = AuthnIdentity(principal_id=USER, actor=AuthnIdentity(principal_id=AGENT))

        status, _, _ = await _post(_app(_seeded(USER, AGENT), identity))

        assert status == 200

    async def test_an_enforcing_spec_needs_the_agents_may_act_grant(self) -> None:
        identity = AuthnIdentity(principal_id=USER, actor=AuthnIdentity(principal_id=AGENT))
        module = _seeded(USER, AGENT)

        refused = await _post(_app(module, identity, ENFORCED))
        await MockDelegationGrantPort(state=module.state).grant_delegation(AGENT, USER)
        allowed, _, _ = await _post(_app(module, identity, ENFORCED))

        assert (refused[0], refused[2], allowed) == (403, "delegation_not_granted", 200)

    async def test_a_configuration_grant_opens_it(self) -> None:
        config = ConfigGrantsProvider(
            keys=frozenset({WRITE}), grants=ConfigGrants(grants={WRITE: frozenset({USER})})
        )
        module = MockDepsModule(permission_providers=(config,))

        allowed, _, _ = await _post(_app(module, AuthnIdentity(principal_id=USER)))
        refused, _, _ = await _post(_app(module, AuthnIdentity(principal_id=AGENT)))

        assert (allowed, refused) == (200, 403)


class TestTheDenialShape:
    async def test_about_a_covered_type_it_is_that_types_not_found(self, posture: Any) -> None:
        posture(DenialPosture(mode="non_disclosing", resource_types=frozenset({"ledger"})))
        app = _app(_seeded(), AuthnIdentity(principal_id=USER), resource_type="ledger")

        assert await _post(app) == await _post(app, "/missing")

    async def test_without_a_resource_type_it_stays_a_403(self, posture: Any) -> None:
        posture(DenialPosture(mode="non_disclosing", resource_types=frozenset({"ledger"})))

        status, _, _ = await _post(_app(_seeded(), AuthnIdentity(principal_id=USER)))

        assert status == 403

    def test_a_blank_key_is_refused_when_the_route_is_declared(self) -> None:
        with pytest.raises(CoreException, match="permission key"):
            require_permission(" ", spec=SPEC, ctx_dep=lambda: None)  # type: ignore[arg-type,return-value]
