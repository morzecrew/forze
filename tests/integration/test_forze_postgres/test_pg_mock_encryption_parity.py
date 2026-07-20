"""Field-encryption conformance: mock ≡ real Postgres, provably, for one encrypted spec.

The deliverable of the mock field-encryption work: not "the mock encrypts", but that the
mock's crypto behavior is a *checked* claim. One scenario runs against both backends,
wired with the same key directory, and every observable — the envelope at rest and the
key id it names, decrypt-on-read, searchable-equality hits, and each refusal code — must
agree. A future divergence in either backend fails this file, not production.
"""

import base64
from typing import Any

import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
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
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

# ----------------------- #

_KEY_ID = "parity-cmk"
_DET_ROOT = b"parity-deterministic-root-32byte"


class _Person(Document):
    name: str
    secret: str
    email: str


class _PersonCreate(CreateDocumentCmd):
    name: str
    secret: str
    email: str


class _PersonUpdate(BaseDTO):
    secret: str | None = None


class _PersonRead(ReadDocument):
    name: str
    secret: str
    email: str


def _spec() -> DocumentSpec[_PersonRead, _Person, _PersonCreate, _PersonUpdate]:
    return DocumentSpec(
        name="people_parity",
        read=_PersonRead,
        write={  # type: ignore[arg-type]
            "domain": _Person,
            "create_cmd": _PersonCreate,
            "update_cmd": _PersonUpdate,
        },
        encryption=FieldEncryption(
            encrypted=frozenset({"secret"}),
            searchable=frozenset({"email"}),
        ),
    )


def _pg_ctx(pg_client: PostgresClient) -> ExecutionContext:
    configurable = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", "people_parity"),
            write=("public", "people_parity"),
            bookkeeping_strategy="application",
        )
    )
    deps = Deps.merge(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id=_KEY_ID)),
            deterministic_root=_DET_ROOT,
        )(),
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        ),
    )
    return context_from_deps(deps)


def _mock_adapter(
    state: MockState,
) -> MockDocumentAdapter[_PersonRead, _Person, _PersonCreate, _PersonUpdate]:
    """The mock document adapter under the *same* key directory as the Postgres side."""

    spec = _spec()
    codecs = resolve_document_codecs(
        spec.resolved_codecs,
        spec_name=str(spec.name),
        encryption=spec.encryption,
        keyring=Keyring(
            kms=MockKeyManagement(),
            aead=AesGcmAead(),  # the CryptoDepsModule default, matching the Postgres side
            directory=StaticKeyDirectory(KeyRef(key_id=_KEY_ID)),
        ),
        deterministic=DeterministicFieldCipher(root=_DET_ROOT),
        tenant_provider=lambda: None,
        integration="mock",
        code="mock.document.encryption_wiring",
    )
    return MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="people_parity",
        read_model=_PersonRead,
        codecs=codecs,
        domain_model=_Person,
    )


# ....................... #


async def _observe(query: Any, command: Any) -> dict[str, Any]:
    """Run the shared scenario against one backend and collect every observable."""

    out: dict[str, Any] = {}

    people = [
        ("alice", "alpha", "alice@x.io"),
        ("bob", "beta", "bob@x.io"),
        ("eve", "gamma", "eve@x.io"),
    ]
    ids = {}
    for name, secret, email in people:
        created = await command.create(_PersonCreate(name=name, secret=secret, email=email))
        ids[name] = created.id

    out["ids"] = ids

    # Decrypt-on-read.
    got = await query.get(ids["alice"])
    out["read"] = (got.name, got.secret, got.email)

    # Searchable equality and membership: rewritten to ciphertext, same hits.
    eq_page = await query.find_many({"$values": {"email": "bob@x.io"}})
    out["eq_hits"] = sorted(hit.name for hit in eq_page.hits)

    in_page = await query.find_many({"$values": {"email": {"$in": ["alice@x.io", "eve@x.io"]}}})
    out["in_hits"] = sorted(hit.name for hit in in_page.hits)

    # Refusals: randomized filter, sealed sort, non-equality on searchable.
    for label, call in (
        ("randomized_filter", lambda: query.find_many({"$values": {"secret": "alpha"}})),
        ("sealed_sort", lambda: query.find_many(sorts={"secret": "asc"})),
        ("searchable_like", lambda: query.find_many({"$values": {"email": {"$like": "a%"}}})),
    ):
        with pytest.raises(CoreException) as excinfo:
            await call()
        out[label] = excinfo.value.code

    return out


def _assert_at_rest(raw_secret: str, raw_email: str) -> None:
    assert raw_secret.startswith(ENVELOPE_B64_PREFIX), raw_secret
    envelope = unpack_envelope(base64.b64decode(raw_secret))
    assert envelope.key_id == _KEY_ID
    assert raw_email != "alice@x.io"


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mock_and_postgres_agree_on_field_encryption(
    pg_client: PostgresClient,
) -> None:
    await pg_client.execute("DROP TABLE IF EXISTS people_parity CASCADE;")
    await pg_client.execute(
        """
        CREATE TABLE people_parity (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            secret text NOT NULL,
            email text NOT NULL
        );
        """
    )

    spec = _spec()
    pg = _pg_ctx(pg_client)
    pg_obs = await _observe(pg.document.query(spec), pg.document.command(spec))

    state = MockState()
    mock = _mock_adapter(state)
    mock_obs = await _observe(mock, mock)

    # The at-rest shape agrees: a Forze envelope naming the same key id, and a
    # non-plaintext searchable value — on the dict exactly as on the table.
    pg_row = await pg_client.fetch_one(
        "SELECT secret, email FROM people_parity WHERE id = %s",
        [pg_obs["ids"]["alice"]],
        row_factory="dict",
    )
    assert pg_row is not None
    _assert_at_rest(pg_row["secret"], pg_row["email"])

    mock_row = state.documents["people_parity"][mock_obs["ids"]["alice"]]
    _assert_at_rest(mock_row["secret"], mock_row["email"])

    # Every behavioral observable is identical.
    for key in ("read", "eq_hits", "in_hits", "randomized_filter", "sealed_sort", "searchable_like"):
        assert pg_obs[key] == mock_obs[key], f"{key}: pg={pg_obs[key]!r} mock={mock_obs[key]!r}"
