"""Field-encryption conformance battery: the oracle's crypto behaviour, checked per plane.

The deliverable is not "the mock encrypts" but that the mock's crypto behaviour is a
*checked* claim on every document backend, not just the one it was first compared against.
One scenario runs against a real store and against the mock wired with the same key
directory, and every observable must agree: the envelope at rest and the key id it names,
decrypt-on-read, searchable-equality and membership hits, and each refusal code.

The refusals matter as much as the round-trip, and one of them is why this exists. A sort
on a sealed field must be refused rather than answered — a store that sorts the ciphertext
returns rows in an order that looks plausible and is meaningless, which is the failure mode
that shipped once already and was caught only on real Postgres. That refusal is a *wiring*
property, so a helper wired for one plane says nothing about the next: it has to be
asserted per backend, which is exactly what this battery makes cheap.
"""

from __future__ import annotations

import base64
from collections.abc import Awaitable, Callable
from typing import Any, final

import attrs
import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.integrations.crypto import (
    DeterministicFieldCipher,
    Keyring,
    resolve_document_codecs,
)
from forze.base.crypto import ENVELOPE_B64_PREFIX, unpack_envelope
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockKeyManagement
from forze_mock.adapters import MockDocumentAdapter, MockState

# ----------------------- #

KEY_ID = "parity-cmk"
DETERMINISTIC_ROOT = b"parity-deterministic-root-32byte"

SPEC_NAME = "people_parity"
"""Also the collection/table name every leg provisions."""


class Person(Document):
    name: str
    secret: str
    email: str


class PersonCreate(CreateDocumentCmd):
    name: str
    secret: str
    email: str


class PersonUpdate(BaseDTO):
    secret: str | None = None


class PersonRead(ReadDocument):
    name: str
    secret: str
    email: str


def spec(*, encrypted: bool = True) -> DocumentSpec[PersonRead, Person, PersonCreate, PersonUpdate]:
    """One randomized field and one searchable field — the two halves behave differently.

    ``encrypted=False`` builds the *same* spec without the encryption declaration, which is
    how each leg reads what was really stored: the adapter then decodes nothing, so the
    values come back exactly as they sit at rest. Dropping to each backend's driver would
    work too, but it would need per-plane code for the one assertion that has to mean the
    same thing on every plane.
    """

    return DocumentSpec(
        name=SPEC_NAME,
        read=PersonRead,
        write={  # type: ignore[arg-type]
            "domain": Person,
            "create_cmd": PersonCreate,
            "update_cmd": PersonUpdate,
        },
        encryption=(
            FieldEncryption(
                encrypted=frozenset({"secret"}),
                searchable=frozenset({"email"}),
            )
            if encrypted
            else None
        ),
    )


def mock_adapter(state: MockState) -> MockDocumentAdapter[Any, Any, Any, Any]:
    """The oracle under the *same* key directory as the backend it is compared against.

    Same keyring, same deterministic root, same AEAD: a difference in what comes back is
    then a difference in behaviour rather than in wiring.
    """

    resolved = spec()
    codecs = resolve_document_codecs(
        resolved.resolved_codecs,
        spec_name=str(resolved.name),
        encryption=resolved.encryption,
        keyring=Keyring(
            kms=MockKeyManagement(),
            aead=AesGcmAead(),  # the CryptoDepsModule default, matching the real side
            directory=StaticKeyDirectory(KeyRef(key_id=KEY_ID)),
        ),
        deterministic=DeterministicFieldCipher(root=DETERMINISTIC_ROOT),
        tenant_provider=lambda: None,
        integration="mock",
        code="mock.document.encryption_wiring",
    )

    return MockDocumentAdapter(
        spec=resolved,
        state=state,
        namespace=SPEC_NAME,
        read_model=PersonRead,
        codecs=codecs,
        domain_model=Person,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class FieldEncryptionHarness:
    """One document plane under test, plus a way to read what it actually stored."""

    query: Any
    command: Any

    plain_query: Any
    """A query port over the same rows built from ``spec(encrypted=False)``.

    The at-rest witness. Without it the battery could only prove the plane round-trips its
    own writes, which a plane storing plaintext also does — this is what separates
    "encrypted" from merely "consistent".
    """

    backend: str


Check = Callable[[FieldEncryptionHarness], Awaitable[None]]


# ....................... #


async def observe(query: Any, command: Any) -> dict[str, Any]:
    """Run the shared scenario against one plane and collect every observable."""

    out: dict[str, Any] = {}
    people = [
        ("alice", "alpha", "alice@x.io"),
        ("bob", "beta", "bob@x.io"),
        ("eve", "gamma", "eve@x.io"),
    ]
    ids = {}

    for name, secret, email in people:
        created = await command.create(PersonCreate(name=name, secret=secret, email=email))
        ids[name] = created.id

    out["ids"] = ids

    got = await query.get(ids["alice"])
    out["read"] = (got.name, got.secret, got.email)

    # Searchable equality and membership: rewritten to ciphertext, same hits.
    eq_page = await query.find_many({"$values": {"email": "bob@x.io"}})
    out["eq_hits"] = sorted(hit.name for hit in eq_page.hits)

    in_page = await query.find_many({"$values": {"email": {"$in": ["alice@x.io", "eve@x.io"]}}})
    out["in_hits"] = sorted(hit.name for hit in in_page.hits)

    # Refusals: a randomized field cannot be filtered, a sealed field cannot be sorted,
    # and a searchable field supports equality only — never a pattern.
    for label, call in (
        ("randomized_filter", lambda: query.find_many({"$values": {"secret": "alpha"}})),
        ("sealed_sort", lambda: query.find_many(sorts={"secret": "asc"})),
        ("searchable_like", lambda: query.find_many({"$values": {"email": {"$like": "a%"}}})),
    ):
        with pytest.raises(CoreException) as refused:
            await call()

        out[label] = refused.value.code

    return out


def assert_at_rest(raw_secret: str, raw_email: str) -> None:
    """The stored shape: a Forze envelope naming the key, and no plaintext email."""

    assert raw_secret.startswith(ENVELOPE_B64_PREFIX), raw_secret

    envelope = unpack_envelope(base64.b64decode(raw_secret))

    assert envelope.key_id == KEY_ID
    assert raw_email != "alice@x.io"


# ....................... #


async def check_field_encryption_matches_the_oracle(h: FieldEncryptionHarness) -> None:
    """Every observable agrees with the mock, and both really did encrypt at rest."""

    observed = await observe(h.query, h.command)

    state = MockState()
    oracle = mock_adapter(state)
    expected = await observe(oracle, oracle)

    stored = await h.plain_query.get(observed["ids"]["alice"])
    assert_at_rest(str(stored.secret), str(stored.email))

    mock_row = state.documents[SPEC_NAME][expected["ids"]["alice"]]
    assert_at_rest(str(mock_row["secret"]), str(mock_row["email"]))

    for key in ("read", "eq_hits", "in_hits", "randomized_filter", "sealed_sort", "searchable_like"):
        assert observed[key] == expected[key], (
            f"{h.backend}.{key}: real={observed[key]!r} mock={expected[key]!r}"
        )


FIELD_ENCRYPTION_BATTERY: tuple[Check, ...] = (check_field_encryption_matches_the_oracle,)
