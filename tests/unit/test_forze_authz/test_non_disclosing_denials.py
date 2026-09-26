"""Under a non-disclosing posture, a denial about a covered resource is its not-found, byte for byte.

The load-bearing comparison is over what a client receives, not over statuses: an HTTP response
(status, body bytes, every header — ``X-Error-Code`` included) and a Socket.IO ack. Comparing
statuses alone is how a kept error code goes on leaking.

The four reasons compared are produced by the code that raises them in an application, not by
hand: the authz before-hook refusing the subject, the same hook refusing a delegating actor, the
document adapter refusing a foreign row (``owned_by``), and the adapter missing a row. A hook
that forgot to name the resource, or an adapter that forgot to tag its not-found, fails here.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.authz.value_objects import AuthzResource
from forze.application.contracts.document import OwnedBy
from forze.application.execution import ExecutionContext, InvocationMetadata, build_runtime
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.authz import AuthzBeforeAuthorize
from forze.base.exceptions import (
    CoreException,
    DenialPosture,
    ExceptionKind,
    configure_denial_posture,
    current_denial_posture,
    exc,
    guard_frame,
)
from forze.base.primitives import str_key_selector
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_mock import MockDepsModule
from forze_mock.adapters.identity import MockAuthzDecisionPort
from forze_socketio.exceptions import render_error_ack
from tests.support.execution_context import context_from_modules
from tests.support.owned_reads_conformance import OwnedCreate, owned_spec

# ----------------------- #

NOTES = owned_spec("notes")
AUTHZ = AuthzSpec(name="main")
READ = "notes.read"
COVERED = DenialPosture(mode="non_disclosing", resource_types=frozenset({"notes"}))


@pytest.fixture
def posture() -> Any:
    """Restore the process-wide posture whatever a test set it to."""

    previous = current_denial_posture()

    yield configure_denial_posture

    configure_denial_posture(previous)


# ....................... #


def _registry(*, with_resource: bool = True) -> Any:
    def _handler(ctx: ExecutionContext) -> Callable[[Any], Awaitable[Any]]:
        async def read(args: Any) -> Any:
            identity = ctx.inv_ctx.get_authn()
            assert identity is not None
            owner = OwnedBy(field="owner_id", value=identity.principal_id)

            return await ctx.doc.query(NOTES).get(args, owned_by=owner)

        return read

    def _resource(_ctx: ExecutionContext, pk: UUID) -> AuthzResource:
        return AuthzResource(resource_type="notes", resource_id=pk)

    return (
        OperationRegistry(handlers={READ: _handler})
        .patch(str_key_selector.exact(READ))
        .bind_outer()
        .before(
            AuthzBeforeAuthorize(
                spec=AUTHZ,
                action=READ,
                resource_factory=_resource if with_resource else None,
            ).to_step(step_id="authz", requires=()),
        )
        .finish(deep=True)
        .freeze()
    )


async def _refusal(
    ctx: ExecutionContext,
    identity: AuthnIdentity,
    pk: UUID,
    *,
    with_resource: bool = True,
) -> CoreException:
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.inv_ctx.bind(metadata=metadata, authn=identity), pytest.raises(CoreException) as caught:
        await _registry(with_resource=with_resource).resolve(READ, ctx)(pk)

    return caught.value


async def _four_reasons() -> dict[str, CoreException]:
    """Missing capability, a refused delegate, a foreign row, a missing row — in that order."""

    module = MockDepsModule()
    ctx = context_from_modules(module)
    decisions = MockAuthzDecisionPort(state=module.state)

    reader, stranger, agent = uuid4(), uuid4(), uuid4()
    decisions.seed_grant(reader, READ)

    theirs = await ctx.doc.command(NOTES).create(OwnedCreate(owner_id=stranger))

    return {
        "no_capability": await _refusal(ctx, AuthnIdentity(principal_id=stranger), theirs.id),
        "delegate_refused": await _refusal(
            ctx,
            AuthnIdentity(principal_id=reader, actor=AuthnIdentity(principal_id=agent)),
            theirs.id,
        ),
        "foreign_row": await _refusal(ctx, AuthnIdentity(principal_id=reader), theirs.id),
        "missing_row": await _refusal(ctx, AuthnIdentity(principal_id=reader), uuid4()),
    }


async def _http(error: CoreException) -> tuple[int, bytes, dict[str, str]]:
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/probe")
    async def probe() -> None:
        raise error

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/probe")

    return response.status_code, response.content, dict(response.headers)


async def _ack(error: CoreException) -> bytes:
    async def _raise() -> None:
        raise error

    outcome = await guard_frame(_raise)
    return json.dumps(render_error_ack(outcome.envelope)).encode()  # type: ignore[union-attr]


# ....................... #


class TestTheReasonsAreRealBeforeTheyAreHidden:
    """With the posture off, the four reasons are four different answers — or the rest proves nothing."""

    async def test_the_reasons_differ_without_the_posture(self) -> None:
        reasons = await _four_reasons()

        assert {name: (e.kind, e.code) for name, e in reasons.items()} == {
            "no_capability": (ExceptionKind.AUTHORIZATION, "permission_denied"),
            "delegate_refused": (ExceptionKind.AUTHORIZATION, "delegate_denied"),
            "foreign_row": (ExceptionKind.NOT_FOUND, "core.not_found"),
            "missing_row": (ExceptionKind.NOT_FOUND, "core.not_found"),
        }
        assert {e.resource_type for e in reasons.values()} == {"notes"}

        rendered = {name: await _http(e) for name, e in reasons.items()}
        assert rendered["no_capability"][0] == 403
        assert rendered["foreign_row"][0] == 404
        assert rendered["foreign_row"][1] != rendered["missing_row"][1]  # each names its own id


class TestNonDisclosingPosture:
    async def test_every_reason_renders_the_same_http_response(self, posture: Any) -> None:
        reasons = await _four_reasons()
        posture(COVERED)

        rendered = {name: await _http(e) for name, e in reasons.items()}

        assert len(set(map(repr, rendered.values()))) == 1, rendered
        status, body, headers = rendered["no_capability"]
        assert status == 404
        assert headers[ERROR_CODE_HEADER.lower()] == "core.not_found"
        assert json.loads(body) == {"detail": "Not found"}

    async def test_every_reason_renders_the_same_socketio_ack(self, posture: Any) -> None:
        reasons = await _four_reasons()
        posture(COVERED)

        acks = {name: await _ack(e) for name, e in reasons.items()}

        assert len(set(acks.values())) == 1, acks
        assert json.loads(acks["no_capability"])["error"]["kind"] == "not_found"

    async def test_the_server_keeps_the_real_kind(self, posture: Any) -> None:
        reasons = await _four_reasons()
        posture(COVERED)

        await _http(reasons["no_capability"])

        assert reasons["no_capability"].kind is ExceptionKind.AUTHORIZATION
        assert reasons["no_capability"].code == "permission_denied"

    async def test_a_denial_about_no_resource_stays_403(self, posture: Any) -> None:
        module = MockDepsModule()
        ctx = context_from_modules(module)
        posture(COVERED)

        denial = await _refusal(ctx, AuthnIdentity(principal_id=uuid4()), uuid4(), with_resource=False)
        status, _, headers = await _http(denial)

        assert denial.resource_type is None
        assert (status, headers[ERROR_CODE_HEADER.lower()]) == (403, "permission_denied")

    async def test_an_uncovered_type_renders_as_it_is(self, posture: Any) -> None:
        posture(DenialPosture(mode="non_disclosing", resource_types=frozenset({"invoices"})))

        status, body, _ = await _http(
            exc.authorization("no", code="permission_denied", resource_type="notes")
        )

        assert (status, json.loads(body)) == (403, {"detail": "no"})


class TestStandardPosture:
    async def test_a_denial_keeps_its_status_detail_and_code(self, posture: Any) -> None:
        posture(DenialPosture())
        denial = exc.authorization("Not yours", code="permission_denied", resource_type="notes")

        status, body, headers = await _http(denial)

        assert (status, headers[ERROR_CODE_HEADER.lower()]) == (403, "permission_denied")
        assert json.loads(body) == {"detail": "Not yours"}

    async def test_a_not_found_keeps_its_detail_and_context(self, posture: Any) -> None:
        posture(DenialPosture())
        missing = exc.not_found("Note 7 not found", details={"id": "7"}, resource_type="notes")

        _, body, _ = await _http(missing)

        assert json.loads(body) == {"detail": "Note 7 not found", "context": {"id": "7"}}

    def test_standard_is_the_default(self) -> None:
        assert DenialPosture().mode == "standard"

    def test_a_non_disclosing_posture_must_name_its_types(self) -> None:
        with pytest.raises(CoreException) as caught:
            DenialPosture(mode="non_disclosing")

        assert caught.value.code == "denial_posture_empty"


class TestTheRuntimeBindsThePosture:
    async def test_for_its_scope_and_restores_it(self, posture: Any) -> None:
        posture(DenialPosture())
        runtime = build_runtime(MockDepsModule(), denial_posture=COVERED)

        async with runtime.scope():
            assert current_denial_posture() == COVERED

        assert current_denial_posture() == DenialPosture()

    async def test_a_runtime_without_one_leaves_the_process_alone(self, posture: Any) -> None:
        posture(COVERED)

        async with build_runtime(MockDepsModule()).scope():
            assert current_denial_posture() == COVERED
