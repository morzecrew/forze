"""RFC 0017 §2's headline claim, proven: export decrypts through the codec, import re-seals under
the **target's** keys — so key migration falls out of data migration (the KEK-brick escape).

# covers: forze_kits.integrations.portability.ArchiveExporter
# covers: forze_kits.integrations.portability.ArchiveImporter
# covers: forze_kits.integrations.portability.migrate

Until now this was asserted in docstrings and never exercised: no portability test wired an
encrypting codec, and the shared corpus declares no encrypted field. It is also the claim that
justifies `migrate` as the recommended migration path, so it is the one most worth checking.

**Real Postgres on both sides, because that is the only place field encryption happens.** The mock
document adapter never resolves an encrypting codec — it stores what it is handed — so a mock-backed
version of this test would be a tautology: nothing sealed on either side, "re-sealed under the
target's key" trivially true. Only `resolve_document_codecs` (Postgres, Mongo) wraps the spec's
codecs, so only a real backend puts a ciphertext at rest at all.

**The observable is the envelope's own `key_id`.** A Forze envelope is self-describing — it carries
the CMK id and the wrapped DEK — so "re-sealed under the target's CMK" is a fact readable straight
off the bytes at rest, not an inference from behavior.

Two guards make that fact meaningful, and both are pinned here because each is a way this file could
otherwise pass while proving nothing. First, holding a KMS that *could* derive a key is not enough
to open an envelope: the keyring refuses one whose `key_id` is not the key its directory resolves
for the active tenant (`core.crypto.key_id_unauthorized`), so a deployment wired with only
`cmk-target` genuinely cannot read `cmk-source`-sealed rows — moving the data is what makes them
readable. Second, with the source's CMK actually refused by the KMS, the imported rows still read
while the source's own rows no longer do: the failure mode §2 says this kills, stated as an
observable rather than a promise.
"""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    FieldEncryption,
    KeyRef,
    KeyringDepKey,
    StaticKeyDirectory,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.inventory import FrozenSpecRegistry, SpecRegistry
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext
from forze.application.integrations.crypto import Keyring
from forze.base.crypto import unpack_envelope
from forze.base.exceptions import CoreException, exc
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze.testing import context_from_deps
from forze_kits.dto import ImportTimestamps
from forze_kits.integrations.portability import (
    ArchiveExporter,
    ArchiveImporter,
    ArchiveMigrator,
    TenantScope,
)
from forze_kits.integrations.portability.format import read_rows
from forze_mock import MockKeyManagement
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient

# ----------------------- #

SOURCE_CMK = "cmk-source"
TARGET_CMK = "cmk-target"


class _SecretDoc(Document):
    holder: str
    secret: str


class _SecretRead(ReadDocument):
    holder: str
    secret: str


class _SecretCreate(ImportTimestamps):
    holder: str
    secret: str


class _SecretUpdate(BaseDTO):
    secret: str | None = None


# ``secret`` is sealed at rest; ``holder`` stays plaintext, so every assertion below can tell
# "the row is encrypted" from "the row is simply absent/garbled".
VAULT_SPEC: DocumentSpec[_SecretRead, _SecretDoc, _SecretCreate, _SecretUpdate] = DocumentSpec(
    name="vault",
    read=_SecretRead,
    write=DocumentWriteTypes(
        domain=_SecretDoc, create_cmd=_SecretCreate, update_cmd=_SecretUpdate
    ),
    encryption=FieldEncryption(encrypted=frozenset({"secret"})),
)

# ``secret text``: a sealed field is stored as the base64 of a self-describing envelope.
_PG_COLUMNS = """
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    holder text NOT NULL,
    secret text NOT NULL
"""


def _registry() -> FrozenSpecRegistry:
    return SpecRegistry().register(VAULT_SPEC).freeze()


# ....................... #


class _BrickedKms:
    """A KMS whose *bricked* CMK no longer resolves — the failure mode RFC §2 names.

    Stands in for a key that was deleted, disabled, or rotated out from under its ciphertext
    ([[kek-migration-and-blob-reencrypt-plan]]): every other key still works, so a read that fails
    here fails for exactly one reason.
    """

    def __init__(self, bricked: str) -> None:
        self._bricked = bricked
        self._inner = MockKeyManagement()

    def _guard(self, key_ref: KeyRef) -> None:
        if key_ref.key_id == self._bricked:
            raise exc.precondition(f"CMK {key_ref.key_id!r} is unavailable (simulated brick).")

    async def generate_data_key(self, key_ref: KeyRef) -> Any:
        self._guard(key_ref)
        return await self._inner.generate_data_key(key_ref)

    async def unwrap_data_key(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        self._guard(key_ref)
        return await self._inner.unwrap_data_key(wrapped=wrapped, key_ref=key_ref)


def _keyring(key_id: str, *, kms: Any = None) -> Keyring:
    """A keyring that seals new values under *key_id*; decrypt follows each envelope's own key."""

    return Keyring(
        kms=kms if kms is not None else MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id=key_id)),
    )


def _pg_ctx(
    pg_client: PostgresClient, table: str, *, key_id: str, kms: Any = None
) -> ExecutionContext:
    configurable = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
                KeyringDepKey: _keyring(key_id, kms=kms),
            }
        )
    )


async def _create_table(pg_client: PostgresClient, table: str) -> None:
    await pg_client.execute(f"CREATE TABLE {table} ({_PG_COLUMNS});")


# ....................... #


async def _seed(
    ctx: ExecutionContext, tenant: UUID, rows: list[tuple[UUID, str, str]]
) -> dict[UUID, _SecretRead]:
    """Write rows inside the tenant binding — the tenant is part of the encryption AAD, so seed,
    export and import all have to agree on it."""

    seeded: dict[UUID, _SecretRead] = {}

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        command = ctx.document.command(VAULT_SPEC)

        for doc_id, holder, secret in rows:
            seeded[doc_id] = await command.ensure(
                doc_id, _SecretCreate(holder=holder, secret=secret)
            )

    return seeded


async def _read(ctx: ExecutionContext, tenant: UUID, ids: list[UUID]) -> dict[UUID, _SecretRead]:
    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        found = await ctx.document.query(VAULT_SPEC).get_many(ids)

    return {doc.id: doc for doc in found}


async def _raw_secret(pg_client: PostgresClient, table: str, doc_id: UUID) -> str:
    """The ``secret`` column exactly as it sits on disk — read past the port, so the port's own
    decrypt cannot make an unencrypted row look encrypted."""

    row = await pg_client.fetch_one(f"SELECT secret FROM {table} WHERE id = %s", (doc_id,))
    assert row is not None
    return str(row["secret"])


def _envelope_key_id(raw: str) -> str:
    """The CMK named by a stored ciphertext — the re-seal observable."""

    return unpack_envelope(base64.b64decode(raw, validate=True)).key_id


async def _archive_rows(archive: Path) -> dict[str, Any]:
    rows: dict[str, Any] = {}

    async for row in read_rows(archive / "documents" / "vault.jsonl.gz"):
        rows[str(row["id"])] = row

    return rows


_ROWS = [
    (uuid4(), "ada", "hunter2-ada"),
    (uuid4(), "grace", "hunter2-grace"),
]


# ....................... #


@pytest.mark.asyncio
async def test_source_seals_the_field_at_rest_under_its_own_cmk(
    pg_client: PostgresClient,
) -> None:
    """The precondition every claim below rests on: the field really is encrypted at rest, under
    the source's CMK. Without this the rest of the file would pass vacuously on plaintext rows —
    which is exactly how a mock-backed version of this test would lie."""

    tenant = uuid4()
    table = f"vault_{uuid4().hex[:8]}"
    await _create_table(pg_client, table)

    source = _pg_ctx(pg_client, table, key_id=SOURCE_CMK)
    await _seed(source, tenant, _ROWS)

    doc_id, _, secret = _ROWS[0]
    raw = await _raw_secret(pg_client, table, doc_id)

    assert secret not in raw, "the plaintext secret must not be on disk"
    assert _envelope_key_id(raw) == SOURCE_CMK

    # The port still reads it back — sealed at rest, plaintext in the read model.
    restored = await _read(source, tenant, [doc_id])
    assert restored[doc_id].secret == secret


@pytest.mark.asyncio
async def test_export_archive_carries_plaintext_not_ciphertext(
    pg_client: PostgresClient,
    tmp_path: Path,
) -> None:
    """Export decrypts through the codec path: the archive row holds the plaintext, not the
    source's envelope. This is what makes the artifact portable — it depends on none of the
    source's keys — and it is exactly why §9 calls the artifact a credential store.

    The ciphertext-at-rest assertion is what gives the plaintext one its meaning: on a spec that
    did not encrypt, "the archive carries plaintext" would be true for free. Here the same value is
    provably sealed on disk and provably clear in the archive.
    """

    tenant = uuid4()
    table = f"vault_{uuid4().hex[:8]}"
    await _create_table(pg_client, table)

    source = _pg_ctx(pg_client, table, key_id=SOURCE_CMK)
    await _seed(source, tenant, _ROWS)

    archive = tmp_path / "archive"
    report = await ArchiveExporter()(
        source, _registry(), archive, scope=TenantScope(tenant_id=tenant)
    )
    assert report.total_rows == 2

    rows = await _archive_rows(archive)

    for doc_id, holder, secret in _ROWS:
        # Sealed on disk...
        raw = await _raw_secret(pg_client, table, doc_id)
        assert _envelope_key_id(raw) == SOURCE_CMK

        # ...and clear in the archive, decrypted on the way out.
        row = rows[str(doc_id)]
        assert row["secret"] == secret, "the archive carries the decrypted value"
        assert row["holder"] == holder


@pytest.mark.asyncio
async def test_import_reseals_under_the_targets_cmk(
    pg_client: PostgresClient,
    tmp_path: Path,
) -> None:
    """The claim itself: import seals every encrypted field under whatever keyring the *target*
    wires. The rows land under ``cmk-target`` though they left under ``cmk-source`` — key migration
    as a side effect of data migration, with no bespoke rewrap protocol."""

    tenant = uuid4()
    source_table = f"vault_src_{uuid4().hex[:8]}"
    target_table = f"vault_dst_{uuid4().hex[:8]}"
    await _create_table(pg_client, source_table)
    await _create_table(pg_client, target_table)

    source = _pg_ctx(pg_client, source_table, key_id=SOURCE_CMK)
    await _seed(source, tenant, _ROWS)

    archive = tmp_path / "archive"
    await ArchiveExporter()(source, _registry(), archive, scope=TenantScope(tenant_id=tenant))

    target = _pg_ctx(pg_client, target_table, key_id=TARGET_CMK)
    result = await ArchiveImporter()(target, _registry(), archive)
    assert result.total_imported == 2

    for doc_id, _, secret in _ROWS:
        raw = await _raw_secret(pg_client, target_table, doc_id)

        assert secret not in raw, "the target seals it too — the import did not land plaintext"
        assert _envelope_key_id(raw) == TARGET_CMK, "re-sealed under the target's CMK"

    # And the values survive the re-seal.
    restored = await _read(target, tenant, [doc_id for doc_id, _, _ in _ROWS])
    assert {d.id: d.secret for d in restored.values()} == {
        doc_id: secret for doc_id, _, secret in _ROWS
    }


@pytest.mark.asyncio
async def test_imported_rows_survive_the_sources_cmk_being_bricked(
    pg_client: PostgresClient,
    tmp_path: Path,
) -> None:
    """The sharp form of the escape (RFC §2): once imported, the data no longer depends on the
    source's CMK at all. A KMS that refuses ``cmk-source`` still reads every imported row — and
    still cannot read the source's own table, which is what proves the brick is real and this
    assertion is not vacuous."""

    tenant = uuid4()
    source_table = f"vault_src_{uuid4().hex[:8]}"
    target_table = f"vault_dst_{uuid4().hex[:8]}"
    await _create_table(pg_client, source_table)
    await _create_table(pg_client, target_table)

    source = _pg_ctx(pg_client, source_table, key_id=SOURCE_CMK)
    await _seed(source, tenant, _ROWS)

    archive = tmp_path / "archive"
    await ArchiveExporter()(source, _registry(), archive, scope=TenantScope(tenant_id=tenant))

    target = _pg_ctx(pg_client, target_table, key_id=TARGET_CMK)
    await ArchiveImporter()(target, _registry(), archive)

    ids = [doc_id for doc_id, _, _ in _ROWS]

    # The source's CMK is now gone. The imported rows read anyway.
    bricked = _BrickedKms(bricked=SOURCE_CMK)
    after = _pg_ctx(pg_client, target_table, key_id=TARGET_CMK, kms=bricked)
    restored = await _read(after, tenant, ids)

    assert {d.id: d.secret for d in restored.values()} == {
        doc_id: secret for doc_id, _, secret in _ROWS
    }

    # The same brick genuinely strands the source's own rows. The directory names SOURCE_CMK here
    # on purpose: it gets the read past the keyring's key-ownership check (see below) so the read
    # fails on the *brick itself*, not on a key the caller was never allowed to unwrap. Matching
    # the brick's own message is what keeps this from passing for an incidental reason.
    stranded = _pg_ctx(pg_client, source_table, key_id=SOURCE_CMK, kms=_BrickedKms(SOURCE_CMK))

    with pytest.raises(CoreException, match="unavailable"):
        await _read(stranded, tenant, ids)


@pytest.mark.asyncio
async def test_a_target_keyed_reader_cannot_open_source_sealed_rows(
    pg_client: PostgresClient,
) -> None:
    """The key-ownership guard that gives the migration its point, pinned on its own.

    A keyring refuses to unwrap an envelope whose ``key_id`` is not the key its directory resolves
    for the active tenant (``core.crypto.key_id_unauthorized``) — it is not enough to hold a KMS
    that *could* derive the key. So a deployment configured with only ``cmk-target`` genuinely
    cannot read ``cmk-source``-sealed rows: moving the data is what makes them readable, which is
    the whole reason import re-seals rather than copying ciphertext across.
    """

    tenant = uuid4()
    table = f"vault_{uuid4().hex[:8]}"
    await _create_table(pg_client, table)

    source = _pg_ctx(pg_client, table, key_id=SOURCE_CMK)
    await _seed(source, tenant, _ROWS)

    # Same KMS, same rows — only the directory's key differs, and that alone is disqualifying.
    foreign = _pg_ctx(pg_client, table, key_id=TARGET_CMK)

    with pytest.raises(CoreException, match="does not belong to the active tenant"):
        await _read(foreign, tenant, [doc_id for doc_id, _, _ in _ROWS])


@pytest.mark.asyncio
async def test_sorting_on_a_sealed_field_is_refused(pg_client: PostgresClient) -> None:
    """Regression: ``ORDER BY`` on a sealed column sorted by **ciphertext**, silently.

    A randomized ciphertext has no order at all, so this returned rows in an arbitrary,
    nonce-dependent order and raised nothing — and a keyset cursor would additionally carry the
    field's raw value in its token. The search plane already refused this
    (``core.search.encrypted_sort_field``); the document plane never wired the same rule, so the
    helper that expresses it (``FieldEncryption.forbidden_sort_fields``) had no document caller.
    """

    tenant = uuid4()
    table = f"vault_{uuid4().hex[:8]}"
    await _create_table(pg_client, table)

    ctx = _pg_ctx(pg_client, table, key_id=SOURCE_CMK)
    await _seed(ctx, tenant, _ROWS)

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        query = ctx.document.query(VAULT_SPEC)

        with pytest.raises(CoreException, match="no order at rest") as excinfo:
            await query.find_many(sorts={"secret": "asc"})

        assert excinfo.value.code == "core.crypto.encrypted_sort_field"

        # A plaintext column on the same encrypting spec still sorts normally — the guard is
        # scoped to the declared fields, not to the spec.
        page = await query.find_many(sorts={"holder": "asc"})
        assert [row.holder for row in page.hits] == sorted(h for _, h, _ in _ROWS)


@pytest.mark.asyncio
async def test_migrate_reseals_under_the_targets_cmk(
    pg_client: PostgresClient,
) -> None:
    """The direct path — the one RFC §2 actually recommends as the KEK escape, since it never
    writes the decrypted rows anywhere. Same re-seal, no artifact in between."""

    tenant = uuid4()
    source_table = f"vault_src_{uuid4().hex[:8]}"
    target_table = f"vault_dst_{uuid4().hex[:8]}"
    await _create_table(pg_client, source_table)
    await _create_table(pg_client, target_table)

    source = _pg_ctx(pg_client, source_table, key_id=SOURCE_CMK)
    await _seed(source, tenant, _ROWS)

    target = _pg_ctx(pg_client, target_table, key_id=TARGET_CMK)
    report = await ArchiveMigrator()(
        source, target, _registry(), scope=TenantScope(tenant_id=tenant)
    )
    assert report.total_imported == 2

    for doc_id, _, secret in _ROWS:
        raw = await _raw_secret(pg_client, target_table, doc_id)

        assert secret not in raw
        assert _envelope_key_id(raw) == TARGET_CMK, "migrate re-seals under the target's CMK too"

    restored = await _read(target, tenant, [doc_id for doc_id, _, _ in _ROWS])
    assert {d.id: d.secret for d in restored.values()} == {
        doc_id: secret for doc_id, _, secret in _ROWS
    }
