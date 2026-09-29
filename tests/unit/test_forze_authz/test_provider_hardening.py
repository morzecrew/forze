"""A provider cannot stall authorization, and its keys are checked even without the boot step.

Every decision runs every provider, so a provider that hangs has to count as failed, denying the
keys it declares, instead of stalling decisions for actions it never declared. And the catalog
check on declared keys runs on the first decision per tenant, so a deployment that never
registered the boot step still cannot run with a misspelt key.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.authz import DerivedPermissions
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_identity.authz import AuthzKernelConfig
from forze_identity.authz.application.specs import permission_definition_spec
from forze_identity.authz.domain.models.permission_definition import (
    CreatePermissionDefinitionCmd,
)
from forze_identity.authz.execution.deps.configs import build_authz_shared_services
from forze_identity.authz.execution.deps.deps import _grant_resolver  # pyright: ignore[reportPrivateUsage]
from forze_mock import MockDepsModule

pytestmark = pytest.mark.unit

PRINCIPAL = uuid4()


@attrs.define(slots=True, kw_only=True)
class _Provider:
    name: str
    keys: frozenset[str]
    delay: float = 0.0
    calls: int = 0

    async def derive(self, principal_id: UUID, ctx: Any) -> DerivedPermissions:
        self.calls += 1
        await asyncio.sleep(self.delay)
        return DerivedPermissions(granted=self.keys)


async def _ctx_with_catalog(*keys: str) -> Any:
    ctx = context_from_modules(MockDepsModule())

    for key in keys:
        await ctx.doc.command(permission_definition_spec).create(
            CreatePermissionDefinitionCmd(permission_key=key)
        )

    return ctx


class TestTheDeadline:
    async def test_a_provider_that_hangs_denies_what_it_declares(self) -> None:
        slow = _Provider(name="members", keys=frozenset({"ledger.write"}), delay=5)
        ctx = await _ctx_with_catalog("ledger.write")
        shared = build_authz_shared_services(
            AuthzKernelConfig(
                permission_providers=(slow,),
                permission_provider_timeout=timedelta(milliseconds=20),
            )
        )

        grants = await asyncio.wait_for(
            _grant_resolver(ctx, shared).resolve_effective_grants(PRINCIPAL), timeout=2
        )

        assert grants.denied_keys == frozenset({"ledger.write"})

    @pytest.mark.parametrize("timeout", [timedelta(0), timedelta(seconds=-1)])
    def test_a_deadline_is_positive(self, timeout: timedelta) -> None:
        with pytest.raises(CoreException):
            AuthzKernelConfig(permission_provider_timeout=timeout)

    def test_no_deadline_is_declarable(self) -> None:
        assert (
            AuthzKernelConfig(permission_provider_timeout=None).permission_provider_timeout is None
        )


class TestTheFirstUseKeyCheck:
    async def test_a_key_the_catalog_lacks_refuses_the_first_decision(self) -> None:
        typo = _Provider(name="members", keys=frozenset({"ledger.wrte"}))
        ctx = await _ctx_with_catalog("ledger.write")
        shared = build_authz_shared_services(AuthzKernelConfig(permission_providers=(typo,)))

        with pytest.raises(CoreException) as caught:
            await _grant_resolver(ctx, shared).resolve_effective_grants(PRINCIPAL)

        assert caught.value.code == "authz_provider_unknown_keys"
        assert typo.calls == 0

    async def test_the_check_runs_once_per_process_and_tenant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from forze_identity.authz.services import grants as module

        checks: list[object] = []
        real = module.check_declared_keys

        async def _counting(query: Any, providers: Any) -> None:
            checks.append(query)
            await real(query, providers)

        monkeypatch.setattr(module, "check_declared_keys", _counting)
        member = _Provider(name="members", keys=frozenset({"ledger.write"}))
        ctx = await _ctx_with_catalog("ledger.write")
        shared = build_authz_shared_services(AuthzKernelConfig(permission_providers=(member,)))

        for _ in range(3):
            await _grant_resolver(ctx, shared).resolve_effective_grants(PRINCIPAL)

        assert len(checks) == 1
        assert member.calls == 3

    async def test_a_failed_check_is_not_remembered(self) -> None:
        typo = _Provider(name="members", keys=frozenset({"ledger.write"}))
        ctx = await _ctx_with_catalog()
        shared = build_authz_shared_services(AuthzKernelConfig(permission_providers=(typo,)))

        with pytest.raises(CoreException):
            await _grant_resolver(ctx, shared).resolve_effective_grants(PRINCIPAL)

        await ctx.doc.command(permission_definition_spec).create(
            CreatePermissionDefinitionCmd(permission_key="ledger.write")
        )

        grants = await _grant_resolver(ctx, shared).resolve_effective_grants(PRINCIPAL)
        assert "ledger.write" in grants.granted_keys
