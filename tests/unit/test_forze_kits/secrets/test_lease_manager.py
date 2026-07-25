"""Lease manager: issue/deliver, renew cadence, reissue-then-drain, shutdown revoke."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from forze.application.contracts.secrets import (
    LeasedSecret,
    SecretRef,
    SecretsCapabilities,
)
from forze.base.exceptions import CoreException
from forze_kits.integrations.secrets import SecretsLeaseManager

# ----------------------- #

_ROLE = SecretRef("db/app-role")


class _ScriptedDynamic:
    """Dynamic port double with scripted renewal outcomes and observable events."""

    secrets_capabilities = SecretsCapabilities(dynamic_credentials=True)

    def __init__(
        self,
        *,
        ttl: timedelta = timedelta(seconds=0.2),
        renew_grants: list[timedelta | Exception] | None = None,
        renewable: bool = True,
    ) -> None:
        self.ttl = ttl
        self.renewable = renewable
        self.renew_grants = renew_grants or []
        self.issued: list[str] = []
        self.renewed: list[str] = []
        self.revoked: list[str] = []
        self.issue_event = asyncio.Event()
        self.renew_event = asyncio.Event()
        self.reissue_event = asyncio.Event()

    async def issue(self, ref: SecretRef) -> LeasedSecret:
        lease_id = f"lease/{ref.path}/{len(self.issued) + 1}"
        self.issued.append(lease_id)
        self.issue_event.set()

        if len(self.issued) > 1:
            self.reissue_event.set()

        return LeasedSecret(
            text='{"username": "u", "password": "p"}',
            lease_id=lease_id,
            ttl=self.ttl,
            renewable=self.renewable,
        )

    async def renew(self, lease_id: str, increment: timedelta) -> timedelta:
        self.renewed.append(lease_id)
        self.renew_event.set()

        if self.renew_grants:
            outcome = self.renew_grants.pop(0)

            if isinstance(outcome, Exception):
                raise outcome

            return outcome

        return increment

    async def revoke(self, lease_id: str) -> None:
        self.revoked.append(lease_id)


async def _drive(manager: SecretsLeaseManager, ref: SecretRef, until: asyncio.Event) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(manager._run_role(ref, stop))  # noqa: SLF001

    try:
        await asyncio.wait_for(until.wait(), timeout=5)

    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)


class TestLeaseManager:
    async def test_issues_delivers_and_revokes_on_shutdown(self) -> None:
        dynamic = _ScriptedDynamic()
        delivered: list[tuple[str, str]] = []

        async def _on_credential(ref: SecretRef, leased: LeasedSecret) -> None:
            delivered.append((ref.path, leased.lease_id))

        manager = SecretsLeaseManager(
            dynamic=dynamic, roles=(_ROLE,), on_credential=_on_credential
        )

        await _drive(manager, _ROLE, dynamic.issue_event)

        assert delivered[0] == (_ROLE.path, dynamic.issued[0])
        assert dynamic.revoked == [dynamic.issued[-1]]

    async def test_renews_before_expiry(self) -> None:
        dynamic = _ScriptedDynamic(ttl=timedelta(seconds=0.15))

        async def _on_credential(ref: SecretRef, leased: LeasedSecret) -> None:
            pass

        manager = SecretsLeaseManager(
            dynamic=dynamic, roles=(_ROLE,), on_credential=_on_credential
        )

        await _drive(manager, _ROLE, dynamic.renew_event)

        assert dynamic.renewed

    async def test_capped_grant_triggers_reissue_then_drain_revoke(self) -> None:
        dynamic = _ScriptedDynamic(
            ttl=timedelta(seconds=0.15),
            renew_grants=[timedelta(seconds=0)],  # store caps the grant → reissue
        )
        delivered: list[str] = []

        async def _on_credential(ref: SecretRef, leased: LeasedSecret) -> None:
            delivered.append(leased.lease_id)

        manager = SecretsLeaseManager(
            dynamic=dynamic,
            roles=(_ROLE,),
            on_credential=_on_credential,
            drain_grace=timedelta(seconds=0.01),
        )

        await _drive(manager, _ROLE, dynamic.reissue_event)

        assert len(dynamic.issued) >= 2
        # The drained (first) lease was revoked before shutdown revoked the live one.
        assert dynamic.issued[0] in dynamic.revoked
        assert delivered[:2] == dynamic.issued[:2]

    async def test_renewal_failure_retries_without_abandoning(self) -> None:
        # A generous TTL so the failure handling itself can't run out the lease;
        # the property under test is recovery, not the dead-lease path.
        dynamic = _ScriptedDynamic(
            ttl=timedelta(seconds=1.0),
            renew_grants=[RuntimeError("store away"), timedelta(seconds=1.0)],
        )

        async def _on_credential(ref: SecretRef, leased: LeasedSecret) -> None:
            pass

        manager = SecretsLeaseManager(
            dynamic=dynamic,
            roles=(_ROLE,),
            on_credential=_on_credential,
            retry_backoff=timedelta(seconds=0.02),
            drain_grace=timedelta(seconds=0.01),
        )

        renewed_twice = asyncio.Event()
        original = dynamic.renew

        async def _counting(lease_id: str, increment: timedelta) -> timedelta:
            result = await original(lease_id, increment)

            if len(dynamic.renewed) >= 2:
                renewed_twice.set()

            return result

        dynamic.renew = _counting  # type: ignore[method-assign]

        await _drive(manager, _ROLE, renewed_twice)

        assert len(dynamic.renewed) >= 2

    def test_fails_closed_without_dynamic_capability(self) -> None:
        class _Static:
            pass

        async def _on_credential(ref: SecretRef, leased: LeasedSecret) -> None:
            pass

        with pytest.raises(CoreException, match="not supported"):
            SecretsLeaseManager(
                dynamic=_Static(),  # type: ignore[arg-type]
                roles=(_ROLE,),
                on_credential=_on_credential,
            )

    def test_needs_at_least_one_role(self) -> None:
        async def _on_credential(ref: SecretRef, leased: LeasedSecret) -> None:
            pass

        with pytest.raises(CoreException, match="at least one role"):
            SecretsLeaseManager(
                dynamic=_ScriptedDynamic(), roles=(), on_credential=_on_credential
            )
