"""Poll watcher: prime-without-emit, version diffing, ref filters, fail-closed wiring."""

from __future__ import annotations

import pytest

from forze.application.contracts.secrets import SecretChanged, SecretRef
from forze.base.exceptions import CoreException
from forze_kits.adapters.secrets import MappingSecrets
from forze_kits.integrations.secrets import SecretsPollWatcher

from ._helpers import collect_changes, settle

# ----------------------- #

_DSN = SecretRef("db/dsn")
_KEY = SecretRef("api/key")


class TestPollWatcher:
    async def test_first_tick_primes_without_emitting(self) -> None:
        backend = MappingSecrets(data={"db/dsn": "dsn-1", "api/key": "key-1"})
        watcher = SecretsPollWatcher(secrets=backend, refs=(_DSN, _KEY))
        seen: list[SecretChanged] = []
        task = collect_changes(watcher, seen)
        await settle()

        try:
            await watcher.tick()
            await settle()

            assert seen == []

        finally:
            task.cancel()

    async def test_change_emits_once_with_the_new_version(self) -> None:
        backend = MappingSecrets(data={"db/dsn": "dsn-1"})
        watcher = SecretsPollWatcher(secrets=backend, refs=(_DSN,))
        seen: list[SecretChanged] = []
        task = collect_changes(watcher, seen)
        await settle()

        try:
            await watcher.tick()
            await backend.put(_DSN, "dsn-2")
            await watcher.tick()
            # A steady tick after the change stays silent.
            await watcher.tick()
            await settle()

            assert [change.ref for change in seen] == [_DSN]
            assert seen[0].version == await backend.current_version(_DSN)

        finally:
            task.cancel()

    async def test_subscription_filter_scopes_delivery(self) -> None:
        backend = MappingSecrets(data={"db/dsn": "dsn-1", "api/key": "key-1"})
        watcher = SecretsPollWatcher(secrets=backend, refs=(_DSN, _KEY))
        dsn_only: list[SecretChanged] = []
        task = collect_changes(watcher, dsn_only, refs=(_DSN,))
        await settle()

        try:
            await watcher.tick()
            await backend.put(_KEY, "key-2")
            await watcher.tick()
            await settle()

            assert dsn_only == []

        finally:
            task.cancel()

    async def test_missing_ref_is_skipped_and_creation_emits(self) -> None:
        backend = MappingSecrets(data={"db/dsn": "dsn-1"})
        watcher = SecretsPollWatcher(secrets=backend, refs=(_DSN, _KEY))
        seen: list[SecretChanged] = []
        task = collect_changes(watcher, seen)
        await settle()

        try:
            await watcher.tick()  # api/key missing: logged, skipped, no crash
            await backend.put(_KEY, "key-1")
            await watcher.tick()
            await settle()

            assert [change.ref for change in seen] == [_KEY]

        finally:
            task.cancel()

    def test_needs_at_least_one_ref(self) -> None:
        with pytest.raises(CoreException, match="at least one ref"):
            SecretsPollWatcher(secrets=MappingSecrets(data={}), refs=())

    def test_fails_closed_on_unversioned_backend(self) -> None:
        class _Unversioned:
            async def resolve_str(self, ref: SecretRef) -> str:
                return "x"

        with pytest.raises(CoreException, match="not supported") as excinfo:
            SecretsPollWatcher(secrets=_Unversioned(), refs=(_DSN,))  # type: ignore[arg-type]

        assert excinfo.value.code == "secrets_feature_unsupported"

    def test_lifecycle_step_is_long_running(self) -> None:
        watcher = SecretsPollWatcher(secrets=MappingSecrets(data={"db/dsn": "x"}), refs=(_DSN,))

        step = watcher.lifecycle_step()

        assert step.requires_long_running
