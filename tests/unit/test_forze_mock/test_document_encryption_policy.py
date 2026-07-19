"""The mock enforces a document's encryption *policy* at the declaration level.

# covers: forze_mock.adapters.document

A filter on a randomized-encrypted field matches nothing on Postgres/Mongo (the stored
value is non-deterministic ciphertext) and is refused there with
``core.crypto.encrypted_field_not_filterable``; a sort on any sealed field would order by
ciphertext. The rule is *policy* — it reads ``FieldEncryption`` off the spec, never the
data — so the shared validator enforces it identically on every backend. The mock now
also seals declared fields for real (see ``test_document_encryption.py``), but these
guards deliberately stay declaration-level: a policy that reads the spec fires on
backends that seal nothing and on ones not yet built, and does not depend on a cipher
being wired. Proven against real Postgres in
``tests/integration/test_portability/test_pg_field_encryption.py``.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze import build_runtime
from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.inventory import SpecRegistry
from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_mock import MockDepsModule
from forze_mock.state import MockState

# ----------------------- #


class _VaultDoc(Document):
    holder: str
    secret: str


class _VaultRead(ReadDocument):
    holder: str
    secret: str


class _VaultCreate(BaseDTO):
    holder: str
    secret: str


class _VaultUpdate(BaseDTO):
    secret: str | None = None


def _spec(*, encrypted: bool) -> DocumentSpec[_VaultRead, _VaultDoc, _VaultCreate, _VaultUpdate]:
    return DocumentSpec(
        name="vault",
        read=_VaultRead,
        write=DocumentWriteTypes(
            domain=_VaultDoc, create_cmd=_VaultCreate, update_cmd=_VaultUpdate
        ),
        encryption=FieldEncryption(encrypted=frozenset({"secret"})) if encrypted else None,
    )


def _runtime(spec: DocumentSpec[_VaultRead, _VaultDoc, _VaultCreate, _VaultUpdate]) -> ExecutionRuntime:
    return build_runtime(
        MockDepsModule(state=MockState()),
        specs=SpecRegistry().register(spec),
        allow_unregistered=True,
    )


# ....................... #


@pytest.mark.asyncio
async def test_filter_on_a_randomized_encrypted_field_is_refused() -> None:
    """A predicate on non-deterministic ciphertext can never match; without the policy guard
    the query would silently return nothing (or, pre-sealing, wrongly match) instead of raising
    the same code Postgres raises."""

    spec = _spec(encrypted=True)
    runtime = _runtime(spec)

    async with runtime.scope():
        ctx = runtime.get_context()
        await ctx.document.command(spec).ensure(uuid4(), _VaultCreate(holder="ada", secret="s3cret"))

        with pytest.raises(CoreException, match="randomized-encrypted") as excinfo:
            await ctx.document.query(spec).find_many({"$values": {"secret": "s3cret"}})

    # The same code a real backend raises — mock and Postgres are one rule, one code.
    assert excinfo.value.code == "core.crypto.encrypted_field_not_filterable"


@pytest.mark.asyncio
async def test_plaintext_fields_on_an_encrypting_spec_stay_filterable() -> None:
    """The guard is scoped to the declared fields — it must not make an encrypting spec
    unqueryable."""

    spec = _spec(encrypted=True)
    runtime = _runtime(spec)

    async with runtime.scope():
        ctx = runtime.get_context()
        await ctx.document.command(spec).ensure(uuid4(), _VaultCreate(holder="ada", secret="s3cret"))

        page = await ctx.document.query(spec).find_many({"$values": {"holder": "ada"}})

    assert len(page.hits) == 1
    assert page.hits[0].secret == "s3cret"  # sealed at rest, decrypted by the read codec


@pytest.mark.asyncio
async def test_sorting_on_a_sealed_field_is_refused() -> None:
    """Ciphertext has no meaningful order at rest on any backend, so the sort is refused from
    the declaration — same policy, same code, whether or not the store seals."""

    spec = _spec(encrypted=True)
    runtime = _runtime(spec)

    async with runtime.scope():
        ctx = runtime.get_context()
        await ctx.document.command(spec).ensure(uuid4(), _VaultCreate(holder="ada", secret="zzz"))
        await ctx.document.command(spec).ensure(uuid4(), _VaultCreate(holder="bob", secret="aaa"))

        with pytest.raises(CoreException, match="no order at rest") as excinfo:
            await ctx.document.query(spec).find_many(sorts={"secret": "asc"})

        assert excinfo.value.code == "core.crypto.encrypted_sort_field"

        # Plaintext sort keys on the same spec are unaffected.
        page = await ctx.document.query(spec).find_many(sorts={"holder": "asc"})

    assert [hit.holder for hit in page.hits] == ["ada", "bob"]


@pytest.mark.asyncio
async def test_a_default_sort_on_a_sealed_field_is_refused_at_spec_construction() -> None:
    """The author's error, caught at the earliest point — the same guard ``SearchSpec`` already
    applies to its own ``default_sort``. A spec is not a valid object if its default ordering can
    never work."""

    with pytest.raises(CoreException, match="no order at rest"):
        DocumentSpec(
            name="vault_bad_default",
            read=_VaultRead,
            write=DocumentWriteTypes(
                domain=_VaultDoc, create_cmd=_VaultCreate, update_cmd=_VaultUpdate
            ),
            encryption=FieldEncryption(encrypted=frozenset({"secret"})),
            default_sort={"secret": "asc"},
        )


@pytest.mark.asyncio
async def test_a_spec_without_encryption_filters_the_field_normally() -> None:
    """No declaration, no guard: the rule reads the spec's policy, not the field's name."""

    spec = _spec(encrypted=False)
    runtime = _runtime(spec)

    async with runtime.scope():
        ctx = runtime.get_context()
        await ctx.document.command(spec).ensure(uuid4(), _VaultCreate(holder="ada", secret="s3cret"))

        page = await ctx.document.query(spec).find_many({"$values": {"secret": "s3cret"}})

    assert len(page.hits) == 1
