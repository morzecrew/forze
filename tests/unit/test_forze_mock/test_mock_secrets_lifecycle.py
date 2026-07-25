"""Mock secrets lifecycle: monotonic versions, programmable change source, leases."""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.secrets import (
    FULL_SECRETS_CAPABILITIES,
    SecretChanged,
    SecretRef,
    SecretsAdminDepKey,
    SecretsDepKey,
    SecretsLeaseDepKey,
    SecretVersion,
)
from forze.base.exceptions import CoreException
from forze_mock import (
    MockDepsModule,
    MockDynamicSecretsPort,
    MockSecretsChangeSource,
    MockSecretsPort,
    MockState,
)
from tests.support.execution_context import context_from_modules

# ----------------------- #

_REF = SecretRef("db/dsn")


class TestMockVersions:
    async def test_put_bumps_a_monotonic_version(self) -> None:
        port = MockSecretsPort(state=MockState())

        first = await port.put(_REF, "a")
        second = await port.put(_REF, "b")

        assert (first.token, second.token) == ("1", "2")
        assert (await port.resolve_versioned(_REF)).text == "b"
        assert await port.current_version(_REF) == second

    async def test_seeded_value_reads_as_version_one(self) -> None:
        state = MockState()
        state.identity["secrets"]["db/dsn"] = "seeded"
        port = MockSecretsPort(state=state)

        assert (await port.current_version(_REF)).token == "1"

    async def test_first_put_over_a_seeded_value_advances_the_version(self) -> None:
        """A seeded entry reads as version 1, so replacing it must not also read
        as 1 — an unchanged token for a changed value is invisible to watchers."""

        state = MockState()
        state.identity["secrets"]["db/dsn"] = "seeded"
        port = MockSecretsPort(state=state)

        initial = await port.current_version(_REF)
        replaced = await port.put(_REF, "replacement")

        assert replaced != initial
        assert replaced.token == "2"
        assert await port.current_version(_REF) == replaced
        assert (await port.resolve_versioned(_REF)).text == "replacement"

    async def test_capabilities_are_full(self) -> None:
        assert MockSecretsPort(state=MockState()).secrets_capabilities == (
            FULL_SECRETS_CAPABILITIES
        )


class TestMockChangeSource:
    async def test_emit_reaches_matching_subscribers_only(self) -> None:
        source = MockSecretsChangeSource()
        all_changes: list[SecretChanged] = []
        filtered: list[SecretChanged] = []

        import asyncio

        async def _drain(out: list[SecretChanged], refs=None) -> None:
            async for change in source.subscribe(refs):
                out.append(change)

        tasks = [
            asyncio.create_task(_drain(all_changes)),
            asyncio.create_task(_drain(filtered, refs=(SecretRef("other"),))),
        ]
        await asyncio.sleep(0)

        try:
            source.emit(SecretChanged(ref=_REF, version=SecretVersion("2")))

            for _ in range(5):
                await asyncio.sleep(0)

            assert [change.ref for change in all_changes] == [_REF]
            assert filtered == []

        finally:
            for task in tasks:
                task.cancel()


class TestMockLeases:
    async def test_issue_renew_revoke_cycle(self) -> None:
        port = MockDynamicSecretsPort(state=MockState(), ttl=timedelta(seconds=60))

        leased = await port.issue(SecretRef("db/role"))
        assert leased.renewable
        assert leased.ttl == timedelta(seconds=60)

        granted = await port.renew(leased.lease_id, timedelta(seconds=30))
        assert granted == timedelta(seconds=30)

        await port.revoke(leased.lease_id)

        with pytest.raises(CoreException, match="No live lease"):
            await port.renew(leased.lease_id, timedelta(seconds=30))

    def test_lease_manager_accepts_the_mock_port(self) -> None:
        from forze_kits.integrations.secrets import SecretsLeaseManager

        async def _on_credential(ref: SecretRef, leased: object) -> None:  # pragma: no cover
            pass

        SecretsLeaseManager(
            dynamic=MockDynamicSecretsPort(state=MockState()),
            roles=(SecretRef("db/role"),),
            on_credential=_on_credential,  # type: ignore[arg-type]
        )

    async def test_issuances_are_sequenced_and_distinct(self) -> None:
        port = MockDynamicSecretsPort(state=MockState())

        first = await port.issue(SecretRef("db/role"))
        second = await port.issue(SecretRef("db/role"))

        assert first.lease_id != second.lease_id
        assert first.text != second.text


class TestModuleRegistration:
    async def test_admin_and_lease_keys_are_wired(self) -> None:
        ctx = context_from_modules(MockDepsModule())

        secrets = ctx.deps.provide(SecretsDepKey)
        admin = ctx.deps.provide(SecretsAdminDepKey)
        lease = ctx.deps.provide(SecretsLeaseDepKey)

        assert admin is secrets  # one store, two protocols
        assert isinstance(lease, MockDynamicSecretsPort)

        version = await admin.put(_REF, "value")
        assert (await secrets.resolve_versioned(_REF)).version == version
