"""RFC 0017 §2's headline claim on the mock: export decrypts, import re-seals under the
**target's** key — the KEK-brick escape, now provable without a database.

# covers: forze_kits.integrations.portability.export_archive
# covers: forze_kits.integrations.portability.import_archive

This claim originally had to be proven against real Postgres because the mock sealed
nothing. The mock now resolves the same encrypting codecs the real factories do, so the
same envelope-level observables — the CMK id read straight off the bytes at rest, and the
keyring's refusal to open a foreign key id — hold in a unit test. One real-backend
differential leg remains (``tests/integration/test_portability/test_pg_field_encryption.py``
and the mock≡Postgres parity case) so the mock is never its own proof.
"""

from __future__ import annotations

import base64
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from forze import build_runtime
from forze.application.contracts.crypto import (
    AeadDepKey,
    FieldEncryption,
    KeyDirectoryDepKey,
    KeyManagementDepKey,
    KeyRef,
    KeyringDepKey,
    StaticKeyDirectory,
)
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.inventory import SpecRegistry
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import CryptoDepsModule, ExecutionRuntime
from forze.base.crypto import ENVELOPE_B64_PREFIX, unpack_envelope
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.dto import ImportTimestamps
from forze_kits.integrations.portability import TenantScope, export_archive, import_archive
from forze_mock import MockDepsModule, MockKeyManagement
from forze_mock.state import MockState

# ----------------------- #


class _VaultDoc(Document):
    holder: str
    secret: str


class _VaultRead(ReadDocument):
    holder: str
    secret: str


class _VaultCreate(ImportTimestamps):
    holder: str
    secret: str


class _VaultUpdate(BaseDTO):
    secret: str | None = None


VAULT_SPEC: DocumentSpec[_VaultRead, _VaultDoc, _VaultCreate, _VaultUpdate] = DocumentSpec(
    name="vault",
    read=_VaultRead,
    write=DocumentWriteTypes(domain=_VaultDoc, create_cmd=_VaultCreate, update_cmd=_VaultUpdate),
    encryption=FieldEncryption(encrypted=frozenset({"secret"})),
)


def _runtime(state: MockState, key_id: str) -> ExecutionRuntime:
    """A mock runtime whose deployment key is *key_id*, modelling two deployments with
    distinct CMKs. Merge is conflict-strict, so the module's own crypto registrations
    are dropped before the replacement CryptoDepsModule is merged in."""

    base = MockDepsModule(state=state)()
    store = base.store

    for key in (KeyringDepKey, KeyManagementDepKey, AeadDepKey, KeyDirectoryDepKey):
        store = store.without(key)

    deps = type(base)(store=store).merge(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id=key_id)),
        )()
    )

    return build_runtime(
        deps=[deps],
        specs=SpecRegistry().register(VAULT_SPEC),
        allow_unregistered=True,
    )


def _stored_key_id(state: MockState, pk: UUID) -> str:
    raw = state.documents["vault"][pk]["secret"]
    assert isinstance(raw, str) and raw.startswith(ENVELOPE_B64_PREFIX), raw
    return unpack_envelope(base64.b64decode(raw)).key_id


# ....................... #


@pytest.mark.asyncio
async def test_import_re_seals_under_the_targets_key(tmp_path: Path) -> None:
    tenant = uuid4()
    source_state = MockState()
    source = _runtime(source_state, "cmk-source")
    pk = uuid4()

    async with source.scope():
        ctx = source.get_context()

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            await ctx.document.command(VAULT_SPEC).ensure(
                pk, _VaultCreate(holder="ada", secret="s3cret")
            )

    # Sealed at rest under the source's CMK — read straight off the envelope bytes.
    assert _stored_key_id(source_state, pk) == "cmk-source"

    async with source.scope():
        await export_archive(
            source, tmp_path, scope=TenantScope(tenant_id=tenant), acknowledge_plaintext=True
        )

    target_state = MockState()
    target = _runtime(target_state, "cmk-target")

    async with target.scope():
        await import_archive(target, tmp_path, tenant=tenant)

    # Re-sealed under the target's CMK: fresh ciphertext, the target's key id, and the
    # value reads back in the target deployment.
    assert _stored_key_id(target_state, pk) == "cmk-target"

    async with target.scope():
        ctx = target.get_context()

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            restored = await ctx.document.query(VAULT_SPEC).get(pk)

    assert restored.secret == "s3cret"
    assert restored.holder == "ada"


@pytest.mark.asyncio
async def test_a_deployment_cannot_read_rows_sealed_under_a_foreign_key(
    tmp_path: Path,
) -> None:
    """Why the re-seal matters: copying the raw rows instead of migrating them leaves
    ciphertext the target's keyring refuses to unwrap (``key_id_unauthorized``)."""

    tenant = uuid4()
    source_state = MockState()
    source = _runtime(source_state, "cmk-source")
    pk = uuid4()

    async with source.scope():
        ctx = source.get_context()

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            await ctx.document.command(VAULT_SPEC).ensure(
                pk, _VaultCreate(holder="ada", secret="s3cret")
            )

    # Byte-copy the source's sealed rows into the target — the anti-pattern that
    # migrate exists to replace.
    target_state = MockState()
    target_state.documents["vault"] = {
        k: dict(v) for k, v in source_state.documents["vault"].items()
    }

    target = _runtime(target_state, "cmk-target")

    async with target.scope():
        ctx = target.get_context()

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            with pytest.raises(CoreException) as excinfo:
                await ctx.document.query(VAULT_SPEC).get(pk)

    assert excinfo.value.code == "core.crypto.key_id_unauthorized"


@pytest.mark.asyncio
async def test_unsealed_export_of_encrypted_fields_is_refused(tmp_path: Path) -> None:
    # The registry declares sealed fields, and export decrypts them into the archive by
    # design — writing them as plaintext without a sealer must be a stated decision.
    source = _runtime(MockState(), "cmk-source")

    with pytest.raises(CoreException, match="declares encrypted fields"):
        async with source.scope():
            await export_archive(source, tmp_path, scope=TenantScope(tenant_id=uuid4()))
