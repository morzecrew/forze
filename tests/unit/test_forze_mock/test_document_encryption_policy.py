"""The mock enforces a document's encryption *policy* even though it stores plaintext.

# covers: forze_mock.adapters.document

The mock is an in-memory dict, not a disk: it does no field encryption, and that is defensible —
encryption is a persistence concern and the mock is not persistence. What is *not* defensible is
the mock answering a query that a real backend cannot. A filter on a randomized-encrypted field
matches nothing on Postgres/Mongo (the stored value is non-deterministic ciphertext) and is refused
there with ``core.crypto.encrypted_field_not_filterable``; against plaintext-in-a-dict the same
filter would happily match, so the query would pass every test and fail in production.

The rule is *policy* — it reads ``FieldEncryption.encrypted`` off the spec, never the data — so the
shared validator can enforce it on a backend that seals nothing, and the mock answers exactly as
the real one does. Proven against real Postgres in
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
    """The divergence this closes: the mock stores ``secret`` in the clear, so without the policy
    guard this filter would match and the test suite would bless a query that raises on Postgres."""

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
    assert page.hits[0].secret == "s3cret"  # readable — the mock seals nothing


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
