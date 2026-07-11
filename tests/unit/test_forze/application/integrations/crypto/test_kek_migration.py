"""Replacing a tenant's key-encryption key — the read-overlap migration.

The confused-deputy guard refuses an envelope whose key id is not the one the directory
resolves for the tenant, so repointing a tenant at a *new* KEK would otherwise strand
everything written under the old one (it could not even be read back to migrate it).

Naming the previous key opens a **read overlap**: writes go to the current key while
reads still accept the previous one, so a re-encryption sweep can move the data across.
Dropping the previous key restores the guard — the overlap must not weaken it forever.
"""

from __future__ import annotations

from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
    TenantTemplateKeyDirectory,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.base.crypto import unpack_envelope
from forze.base.exceptions import CoreException
from forze_mock import MockKeyManagement

# ----------------------- #

_OLD = "kek-old"
_NEW = "kek-new"


def _keyring(directory: object) -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=directory,  # type: ignore[arg-type]
    )


def _tenant() -> TenantIdentity:
    return TenantIdentity(tenant_id=uuid4())


# ....................... #


class TestStaticKeyMigration:
    async def test_previous_key_stays_readable_during_the_overlap(self) -> None:
        tenant = _tenant()
        written = await _keyring(StaticKeyDirectory(KeyRef(key_id=_OLD))).encrypt(
            b"secret", tenant=tenant
        )

        migrating = _keyring(
            StaticKeyDirectory(KeyRef(key_id=_NEW), previous_key_ref=KeyRef(key_id=_OLD))
        )

        assert await migrating.decrypt(written, tenant=tenant) == b"secret"

    async def test_writes_never_use_the_previous_key(self) -> None:
        tenant = _tenant()
        migrating = _keyring(
            StaticKeyDirectory(KeyRef(key_id=_NEW), previous_key_ref=KeyRef(key_id=_OLD))
        )

        blob = await migrating.encrypt(b"fresh", tenant=tenant)

        assert unpack_envelope(blob).key_id == _NEW

    async def test_dropping_the_overlap_restores_the_guard(self) -> None:
        """The overlap must not weaken the confused-deputy check permanently."""

        tenant = _tenant()
        written = await _keyring(StaticKeyDirectory(KeyRef(key_id=_OLD))).encrypt(
            b"secret", tenant=tenant
        )

        migrated = _keyring(StaticKeyDirectory(KeyRef(key_id=_NEW)))  # overlap dropped

        with pytest.raises(CoreException) as ei:
            await migrated.decrypt(written, tenant=tenant)

        assert ei.value.code == "core.crypto.key_id_unauthorized"

    async def test_a_foreign_key_is_still_refused_during_the_overlap(self) -> None:
        """An overlap widens the accepted set to exactly {current, previous} — no more."""

        tenant = _tenant()
        foreign = await _keyring(StaticKeyDirectory(KeyRef(key_id="someone-else"))).encrypt(
            b"theirs", tenant=tenant
        )

        migrating = _keyring(
            StaticKeyDirectory(KeyRef(key_id=_NEW), previous_key_ref=KeyRef(key_id=_OLD))
        )

        with pytest.raises(CoreException) as ei:
            await migrating.decrypt(foreign, tenant=tenant)

        assert ei.value.code == "core.crypto.key_id_unauthorized"


# ....................... #


class TestPerTenantKeyMigration:
    async def test_each_tenant_reads_its_own_previous_key_only(self) -> None:
        """The overlap is per tenant: it does not let tenant B read tenant A's old data."""

        tenant_a, tenant_b = _tenant(), _tenant()
        old = TenantTemplateKeyDirectory(
            template="tenant/{tenant_id}/kek-v1", default_key_id="shared"
        )
        written_a = await _keyring(old).encrypt(b"a-secret", tenant=tenant_a)

        migrating = _keyring(
            TenantTemplateKeyDirectory(
                template="tenant/{tenant_id}/kek-v2",
                default_key_id="shared",
                previous_template="tenant/{tenant_id}/kek-v1",
            )
        )

        # Tenant A reads its own pre-migration data...
        assert await migrating.decrypt(written_a, tenant=tenant_a) == b"a-secret"

        # ...but tenant B still cannot: the overlap resolves *per tenant*.
        with pytest.raises(CoreException) as ei:
            await migrating.decrypt(written_a, tenant=tenant_b)

        assert ei.value.code == "core.crypto.key_id_unauthorized"

    async def test_full_migration_round_trip(self) -> None:
        """Write under the old key, re-encrypt through the overlap, drop it — all readable."""

        tenant = _tenant()
        old_dir = TenantTemplateKeyDirectory(
            template="tenant/{tenant_id}/kek-v1", default_key_id="shared"
        )
        written = await _keyring(old_dir).encrypt(b"payload", tenant=tenant)

        migrating = _keyring(
            TenantTemplateKeyDirectory(
                template="tenant/{tenant_id}/kek-v2",
                default_key_id="shared",
                previous_template="tenant/{tenant_id}/kek-v1",
            )
        )
        # The sweep's read→write round-trip, in miniature.
        plaintext = await migrating.decrypt(written, tenant=tenant)
        rewritten = await migrating.encrypt(plaintext, tenant=tenant)

        assert unpack_envelope(rewritten).key_id.endswith("kek-v2")

        # Overlap dropped: the re-encrypted value still reads, so the old KEK is
        # now free to be destroyed.
        migrated = _keyring(
            TenantTemplateKeyDirectory(
                template="tenant/{tenant_id}/kek-v2", default_key_id="shared"
            )
        )
        assert await migrated.decrypt(rewritten, tenant=tenant) == b"payload"


# ....................... #


class TestNoOverlapByDefault:
    async def test_a_directory_without_the_capability_is_unaffected(self) -> None:
        """An existing KeyDirectoryPort with no `resolve_previous` keeps its old behavior."""

        class _Plain:
            async def resolve(self, tenant: TenantIdentity | None) -> KeyRef:
                _ = tenant
                return KeyRef(key_id=_NEW)

        tenant = _tenant()
        written = await _keyring(StaticKeyDirectory(KeyRef(key_id=_OLD))).encrypt(
            b"secret", tenant=tenant
        )

        with pytest.raises(CoreException) as ei:
            await _keyring(_Plain()).decrypt(written, tenant=tenant)

        assert ei.value.code == "core.crypto.key_id_unauthorized"

    async def test_a_live_overlap_opens_and_closes_at_once(self) -> None:
        """A store-backed directory can change its overlap under a long-lived keyring.

        The previous key decides how wide the key-id guard is open, so a stale copy is
        wrong in *both* directions: a remembered absence would strand a migration that
        was just opened, and a remembered presence would keep accepting the outgoing
        key's envelopes after the operator dropped the overlap — leaving the guard
        widened past the point it was closed.
        """

        @attrs.define(slots=True)
        class _Mutable:
            """A directory whose overlap is toggled mid-flight."""

            previous: KeyRef | None = None

            async def resolve(self, tenant: TenantIdentity | None) -> KeyRef:
                _ = tenant
                return KeyRef(key_id=_NEW)

            async def resolve_previous(
                self, tenant: TenantIdentity | None
            ) -> KeyRef | None:
                _ = tenant
                return self.previous

        tenant = _tenant()
        written = await _keyring(StaticKeyDirectory(KeyRef(key_id=_OLD))).encrypt(
            b"secret", tenant=tenant
        )

        directory = _Mutable()
        keyring = _keyring(directory)  # one long-lived keyring throughout

        # No overlap yet — refused, and that absence must not be remembered.
        with pytest.raises(CoreException):
            await keyring.decrypt(written, tenant=tenant)

        # The operator opens the overlap: the very next read must see it.
        directory.previous = KeyRef(key_id=_OLD)
        assert await keyring.decrypt(written, tenant=tenant) == b"secret"

        # ...and once the sweep is done and the overlap is dropped, the guard must close
        # again immediately — not at the next cache eviction or restart.
        directory.previous = None

        with pytest.raises(CoreException) as ei:
            await keyring.decrypt(written, tenant=tenant)

        assert ei.value.code == "core.crypto.key_id_unauthorized"

    async def test_no_previous_key_set_is_not_an_overlap(self) -> None:
        tenant = _tenant()
        written = await _keyring(StaticKeyDirectory(KeyRef(key_id=_OLD))).encrypt(
            b"secret", tenant=tenant
        )

        # previous_key_ref defaults to None — the guard is unchanged.
        with pytest.raises(CoreException):
            await _keyring(StaticKeyDirectory(KeyRef(key_id=_NEW))).decrypt(
                written, tenant=tenant
            )
