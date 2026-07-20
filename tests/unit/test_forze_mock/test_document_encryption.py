"""Mock document field encryption — the mock seals what a real backend seals.

# covers: forze_mock.adapters.document, forze_mock.execution.factories

A spec that declares ``FieldEncryption`` gets the same treatment from the mock module
as from Postgres/Mongo: its codecs are wrapped through the shared fail-closed
resolution, randomized fields are envelopes at rest, searchable fields are
deterministic ciphertext (equality filters are rewritten to match them), reads
decrypt, and the keyring's key-ownership guard runs on every decode. No pre-pass is
needed anywhere: the mock key manager is a computation-only backend
(``SyncKeyManagementPort``), so the real ``Keyring`` fills its cache inline.
"""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest

from forze import build_runtime
from forze.application.contracts.crypto import (
    AesGcmAead,
    FieldEncryption,
    TenantTemplateKeyDirectory,
)
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.inventory import SpecRegistry
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import (
    DeterministicFieldCipher,
    Keyring,
    resolve_document_codecs,
)
from forze.base.crypto import ENVELOPE_B64_PREFIX
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockKeyManagement
from forze_mock.adapters import MockDocumentAdapter, MockState

# ----------------------- #


class _VaultDoc(Document):
    holder: str
    secret: str
    email: str


class _VaultRead(ReadDocument):
    holder: str
    secret: str
    email: str


class _VaultCreate(CreateDocumentCmd):
    holder: str
    secret: str
    email: str


class _VaultUpdate(BaseDTO):
    secret: str | None = None


def _spec(name: str = "vault") -> DocumentSpec[_VaultRead, _VaultDoc, _VaultCreate, _VaultUpdate]:
    return DocumentSpec(
        name=name,
        read=_VaultRead,
        write=DocumentWriteTypes(
            domain=_VaultDoc, create_cmd=_VaultCreate, update_cmd=_VaultUpdate
        ),
        encryption=FieldEncryption(
            encrypted=frozenset({"secret"}),
            searchable=frozenset({"email"}),
        ),
    )


# ....................... #
# through the module — the declaration is the whole wiring


@pytest.mark.asyncio
async def test_declared_encryption_round_trips_through_the_module() -> None:
    """Write → sealed in ``MockState`` → read → plaintext, with nothing but the spec's
    ``FieldEncryption`` declaration — the same one-declaration story the real backends
    give, now with the same at-rest behavior."""

    state = MockState()
    spec = _spec()
    runtime = build_runtime(
        MockDepsModule(state=state),
        specs=SpecRegistry().register(spec),
        allow_unregistered=True,
    )
    pk = uuid4()

    async with runtime.scope():
        ctx = runtime.get_context()
        await ctx.document.command(spec).ensure(
            pk, _VaultCreate(holder="ada", secret="s3cret", email="ada@example.com")
        )

        # At rest: the randomized field is a Forze envelope; the searchable field is
        # deterministic ciphertext (not plaintext); undeclared fields stay readable.
        raw = dict(state.documents["vault"])[pk]
        assert isinstance(raw["secret"], str)
        assert raw["secret"].startswith(ENVELOPE_B64_PREFIX)
        assert raw["email"] != "ada@example.com"
        assert raw["holder"] == "ada"

        # Reads decrypt.
        got = await ctx.document.query(spec).get(pk)
        assert got.secret == "s3cret"
        assert got.email == "ada@example.com"

        # Equality on the searchable field is rewritten to match the ciphertext at
        # rest — the query behaves exactly as it does on Postgres/Mongo.
        page = await ctx.document.query(spec).find_many(
            {"$values": {"email": "ada@example.com"}}
        )
        assert len(page.hits) == 1
        assert page.hits[0].secret == "s3cret"

        assert (
            await ctx.document.query(spec).count({"$values": {"email": "ada@example.com"}})
        ) == 1
        assert await ctx.document.query(spec).count({"$values": {"email": "nobody@x"}}) == 0

        # Updates merge on the decrypted domain and re-encrypt exactly once.
        updated = await ctx.document.command(spec).update(pk, 1, _VaultUpdate(secret="n3w"))
        assert updated.secret == "n3w"
        raw2 = dict(state.documents["vault"])[pk]
        assert raw2["secret"].startswith(ENVELOPE_B64_PREFIX)
        assert raw2["secret"] != raw["secret"]  # fresh envelope, not the old bytes

        # Projections of sealed fields decrypt before projecting.
        projected = await ctx.document.query(spec).project_many(
            ["secret", "email"], {"$values": {"holder": "ada"}}
        )
        assert projected.hits[0]["secret"] == "n3w"
        assert projected.hits[0]["email"] == "ada@example.com"


@pytest.mark.asyncio
async def test_membership_filter_on_a_searchable_field_matches_ciphertext() -> None:
    state = MockState()
    spec = _spec()
    runtime = build_runtime(
        MockDepsModule(state=state),
        specs=SpecRegistry().register(spec),
        allow_unregistered=True,
    )

    async with runtime.scope():
        ctx = runtime.get_context()
        for holder, email in (("ada", "ada@x.io"), ("bob", "bob@x.io"), ("eve", "eve@x.io")):
            await ctx.document.command(spec).ensure(
                uuid4(), _VaultCreate(holder=holder, secret="s", email=email)
            )

        page = await ctx.document.query(spec).find_many(
            {"$values": {"email": {"$in": ["ada@x.io", "eve@x.io"]}}}
        )

    assert sorted(hit.holder for hit in page.hits) == ["ada", "eve"]


@pytest.mark.asyncio
async def test_non_equality_operator_on_a_searchable_field_is_refused() -> None:
    """Deterministic encryption supports equality only — the rewrite refuses a range
    predicate with the same code a real backend raises."""

    state = MockState()
    spec = _spec()
    runtime = build_runtime(
        MockDepsModule(state=state),
        specs=SpecRegistry().register(spec),
        allow_unregistered=True,
    )

    async with runtime.scope():
        ctx = runtime.get_context()
        await ctx.document.command(spec).ensure(
            uuid4(), _VaultCreate(holder="ada", secret="s", email="ada@x.io")
        )

        with pytest.raises(CoreException) as excinfo:
            await ctx.document.query(spec).find_many({"$values": {"email": {"$like": "ada%"}}})

    assert excinfo.value.code == "core.crypto.searchable_op_unsupported"


# ....................... #
# key ownership — crypto isolation holds even where storage isolation does not


def _tenant_adapter(
    state: MockState,
    tenant_provider: Callable[[], TenantIdentity | None],
) -> MockDocumentAdapter[_VaultRead, _VaultDoc, _VaultCreate, _VaultUpdate]:
    """The adapter exactly as the factory builds it, with a per-tenant key directory
    and a controllable ambient tenant."""

    ring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=TenantTemplateKeyDirectory(
            template="tenant/{tenant_id}/cmk", default_key_id="default"
        ),
    )
    spec = _spec()
    codecs = resolve_document_codecs(
        spec.resolved_codecs,
        spec_name=str(spec.name),
        encryption=spec.encryption,
        keyring=ring,
        deterministic=DeterministicFieldCipher(root=b"mock-deterministic-root-secret!!"),
        tenant_provider=tenant_provider,
        integration="mock",
        code="mock.document.encryption_wiring",
    )
    return MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="vault",
        read_model=_VaultRead,
        codecs=codecs,
        domain_model=_VaultDoc,
        tenant_provider=tenant_provider,
    )


@pytest.mark.asyncio
async def test_cross_tenant_read_is_refused_by_the_key_ownership_guard() -> None:
    """Deliberately *without* document-level tenancy: tenant B can see tenant A's row in
    the shared namespace, but the keyring refuses to unwrap under A's key — the same
    ``key_id_unauthorized`` a real deployment raises, now enforced by the mock too."""

    state = MockState()
    current: dict[str, TenantIdentity | None] = {"id": TenantIdentity(tenant_id=uuid4())}
    adapter = _tenant_adapter(state, lambda: current["id"])

    created = await adapter.create(
        _VaultCreate(holder="ada", secret="s3cret", email="ada@x.io")
    )

    # Same process, same adapter — only the ambient tenant changes.
    current["id"] = TenantIdentity(tenant_id=uuid4())

    with pytest.raises(CoreException) as excinfo:
        await adapter.get(created.id)

    assert excinfo.value.code == "core.crypto.key_id_unauthorized"


@pytest.mark.asyncio
async def test_the_rightful_tenant_reads_its_own_sealed_row() -> None:
    state = MockState()
    tenant = TenantIdentity(tenant_id=uuid4())
    adapter = _tenant_adapter(state, lambda: tenant)

    created = await adapter.create(
        _VaultCreate(holder="ada", secret="s3cret", email="ada@x.io")
    )

    got = await adapter.get(created.id)
    assert got.secret == "s3cret"
    assert got.email == "ada@x.io"

    # And the tenant is bound into the AAD, not just the key id.
    raw = state.documents["vault"][created.id]["secret"]
    assert isinstance(raw, str)
    assert raw.startswith(ENVELOPE_B64_PREFIX)
