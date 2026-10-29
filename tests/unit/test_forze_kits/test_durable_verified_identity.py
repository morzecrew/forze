"""A durable service identity verified by the identity plane's real API-key verifier.

# covers: forze_kits.integrations.durable.operation_bridge

The bridge goes through the authn port, so these runs use ``HmacApiKeyVerifier``, the
lifecycle that issues and revokes keys, and policy-principal eligibility, over mock
documents: what revoking, expiring or disabling does to the next run of a schedule.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.authn import AuthnIdentity, AuthnSpec, IssuedApiKey
from forze.application.contracts.authz import PrincipalRegistryDepKey
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
    DurableRunStatus,
)
from forze.application.contracts.secrets import SecretRef
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations import OperationRegistry
from forze.application.hooks.authn import AuthnRequired
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import FrozenTimeSource, bind_time_source, str_key_selector
from forze.testing import context_from_deps
from forze_identity.authn import AuthnDepsModule
from forze_identity.authn.application.specs import api_key_account_spec
from forze_identity.authn.services import ApiKeyConfig
from forze_identity.authz.application.specs import policy_principal_spec
from forze_identity.authz.domain.models.policy_principal import (
    CreatePolicyPrincipalCmd,
    UpdatePolicyPrincipalCmd,
)
from forze_identity.authz.execution.deps.deps import ConfigurablePrincipalRegistry
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    DurableScheduler,
    DurableServiceIdentity,
    register_operation_functions,
)
from forze_mock import MockDepsModule, MockState
from forze_mock.execution.factories import ConfigurableMockDocument
from tests.support.authn_stacks import AUTHN_KERNEL

pytest.importorskip("argon2")

# ----------------------- #

ROUTE = "main"
SPEC = AuthnSpec(name=ROUTE, enabled_methods=frozenset({"api_key"}))
KEY_REF = SecretRef(path="durable/reports-key")
T0 = datetime(2026, 11, 2, 9, 30, tzinfo=UTC)


class _Args(BaseModel):
    order_id: str


def _operations(seen: list[UUID]) -> Any:
    """An ``AuthnRequired`` operation recording who it ran as."""

    def factory(ctx: Any) -> Any:
        async def _run(args: _Args) -> None:
            seen.append(ctx.inv_ctx.get_authn().principal_id)

        return _run

    return (
        OperationRegistry(handlers={"sync.run": factory})
        .patch(str_key_selector.exact("sync.run"))
        .bind_outer()
        .before(AuthnRequired().to_step())
        .finish(deep=True)
        .freeze()
    )


@attrs.define
class _App:
    ctx: ExecutionContext
    state: MockState
    runner: DurableFunctionRunner
    service: UUID
    issued: IssuedApiKey
    seen: list[UUID]

    async def run(self, order_id: str) -> tuple[DurableRunStatus, str | None]:
        """Enqueue a run and let the recovery scanner execute it, as a worker would."""

        record = await self.runner.enqueue(self.ctx, "sync", {"order_id": order_id})
        await self.runner.recover(self.ctx)
        landed = self.state.durable_runs[record.run_id]

        # The stored error is the refusal's ``str``: its kind and code, then the message.
        reason = None if landed["error"] is None else landed["error"].rsplit(": ", 1)[-1]

        return DurableRunStatus(landed["status"]), reason

    async def revoke(self) -> None:
        assert self.issued.key_id is not None
        await self.ctx.authn.api_key_lifecycle(SPEC).revoke_api_key(
            AuthnIdentity(principal_id=self.service), self.issued.key_id
        )


async def _app(*, expires_in: timedelta | None = None) -> _App:
    mock = MockDepsModule()
    document = ConfigurableMockDocument(module=mock)
    names = (policy_principal_spec.name, api_key_account_spec.name)
    identity = AuthnDepsModule(
        kernel=attrs.evolve(AUTHN_KERNEL, api_key=ApiKeyConfig(expires_in=expires_in)),
        authn={ROUTE: SPEC.enabled_methods},
        authz_route=ROUTE,
        api_key_lifecycle={ROUTE},
    )().merge(
        Deps.routed(
            {
                DocumentQueryDepKey: dict.fromkeys(names, document),
                DocumentCommandDepKey: dict.fromkeys(names, document),
                PrincipalRegistryDepKey: {ROUTE: ConfigurablePrincipalRegistry()},
            }
        )
    )
    ctx = context_from_deps(mock(), identity)

    created = await ctx.doc.command(policy_principal_spec).create(
        CreatePolicyPrincipalCmd(kind="service")
    )
    issued = await ctx.authn.api_key_lifecycle(SPEC).issue_api_key(
        AuthnIdentity(principal_id=created.id)
    )
    # The key lives in the secrets backend only; the identity names where.
    mock.state.identity["secrets"] = {KEY_REF.path: issued.key.key}

    seen: list[UUID] = []
    registry = DurableFunctionRegistry()
    register_operation_functions(
        registry,
        [
            DurableFunctionSpec(
                name="sync",
                run=DurableFunctionInvokeSpec(args_type=_Args),
                operation="sync.run",
                triggers=(DurableFunctionCronTrigger(expression="0 * * * *"),),
            )
        ],
        operations=_operations(seen),
        identity=DurableServiceIdentity(
            authn=AuthnIdentity(principal_id=created.id), api_key=KEY_REF, authn_spec=SPEC
        ),
    )

    return _App(
        ctx=ctx,
        state=mock.state,
        runner=DurableFunctionRunner(registry=registry),
        service=created.id,
        issued=issued,
        seen=seen,
    )


# ....................... #

# The verifier's message is rebuilt from its kind and code, so the row never quotes it.
_REFUSED = "The durable service identity did not verify (authentication, core.authentication)"


async def test_a_live_key_runs_as_its_principal() -> None:
    app = await _app()

    assert await app.run("o-1") == (DurableRunStatus.COMPLETED, None)
    assert app.seen == [app.service]


async def test_a_revoked_key_fails_the_run() -> None:
    app = await _app()
    await app.revoke()

    assert await app.run("o-1") == (DurableRunStatus.FAILED, _REFUSED)
    assert app.seen == []


async def test_an_expired_key_fails_the_run() -> None:
    app = await _app(expires_in=timedelta(hours=1))

    with bind_time_source(FrozenTimeSource(instant=datetime.now(UTC) + timedelta(hours=2))):
        assert await app.run("o-1") == (DurableRunStatus.FAILED, _REFUSED)

    assert app.seen == []


async def test_a_disabled_principal_fails_the_run() -> None:
    app = await _app()
    principals = app.ctx.doc.command(policy_principal_spec)
    current = await app.ctx.doc.query(policy_principal_spec).get(app.service)
    await principals.update(app.service, current.rev, UpdatePolicyPrincipalCmd(is_active=False))

    assert await app.run("o-1") == (DurableRunStatus.FAILED, _REFUSED)
    assert app.seen == []


async def test_a_key_revoked_between_two_fires_stops_the_schedule() -> None:
    app = await _app()
    scheduler = DurableScheduler()
    await scheduler.put(
        app.ctx, "hourly", "sync", "0 * * * *", input_json={"order_id": "s"}, now=T0
    )

    await scheduler.tick(app.ctx, now=T0 + timedelta(hours=1))
    await app.runner.recover(app.ctx)
    await app.revoke()
    await scheduler.tick(app.ctx, now=T0 + timedelta(hours=2))
    await app.runner.recover(app.ctx)

    statuses = sorted(row["status"] for row in app.state.durable_runs.values())
    assert statuses == ["completed", "failed"]
    assert app.seen == [app.service]
    failed = next(row for row in app.state.durable_runs.values() if row["status"] == "failed")
    assert failed["error"].endswith(f"(core.authentication): {_REFUSED}")

    stored = repr((app.state.durable_runs, app.state.durable_run_schedules))
    assert app.issued.key.key not in stored


async def test_run_now_surfaces_the_refusal_as_authentication() -> None:
    app = await _app()
    await app.revoke()

    with pytest.raises(CoreException) as caught:
        await app.runner.run_now(app.ctx, "sync", {"order_id": "o-1"})

    assert caught.value.kind is ExceptionKind.AUTHENTICATION
