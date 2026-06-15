"""Meilisearch field encryption: encrypted_fields sealed in the index document, plaintext
searchable fields untouched, decrypted on read."""

from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel, computed_field

from forze.application.contracts.crypto import (
    AesGcmAead,
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.search import SearchSpec
from forze.application.integrations.crypto import EncryptingModelCodec, Keyring
from forze.base.crypto import is_envelope
from forze.base.serialization import PydanticModelCodec
from forze_meilisearch.adapters.search._command import MeilisearchSearchCommandAdapter
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig
from forze_mock import MockKeyManagement

# ----------------------- #


class _Doc(BaseModel):
    id: str
    title: str
    secret: str

    @computed_field  # type: ignore[prop-decorator]
    @property
    def title_upper(self) -> str:
        return self.title.upper()


def _encrypting_adapter(client: MagicMock) -> MeilisearchSearchCommandAdapter[_Doc]:
    keyring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )
    wrapped = EncryptingModelCodec(
        inner=PydanticModelCodec(_Doc),
        cipher=keyring,
        fields=frozenset({"secret"}),
        tenant_provider=lambda: None,
    )
    spec = SearchSpec(
        name="items",
        model_type=_Doc,
        fields=["title"],
        read_codec=wrapped,
        encryption=FieldEncryption(encrypted=frozenset({"secret"})),
    )
    return MeilisearchSearchCommandAdapter(
        spec=spec,
        config=MeilisearchSearchConfig(index_uid="items_idx", wait_for_tasks=False),
        client=client,
    )


def _client() -> MagicMock:
    client = MagicMock()
    index = MagicMock()
    index.add_documents = AsyncMock(return_value=MagicMock(task_uid=1))
    client.index = MagicMock(return_value=index)
    client.wait_for_task = AsyncMock()
    return client


# ....................... #


@pytest.mark.asyncio
async def test_upsert_seals_encrypted_field_keeps_searchable_plaintext() -> None:
    client = _client()
    adapter = _encrypting_adapter(client)

    await adapter.upsert([_Doc(id="1", title="hello", secret="hunter2")])

    [chunk] = client.index.return_value.add_documents.call_args.args
    doc = chunk[0]

    # Searchable field stays plaintext (so Meilisearch can index it)...
    assert doc["title"] == "hello"
    # ...the confidential field is a sealed envelope, not the plaintext.
    assert doc["secret"] != "hunter2"
    assert is_envelope(base64.b64decode(doc["secret"]))


@pytest.mark.asyncio
async def test_encrypted_route_still_indexes_computed_fields() -> None:
    # Regression: the encrypting index path uses the persistence encode, which defaults to
    # dropping ``@computed_field`` values. The plain index path keeps them, so the encrypted
    # path must too — otherwise searchable/filterable computed data silently vanishes.
    client = _client()
    adapter = _encrypting_adapter(client)

    await adapter.upsert([_Doc(id="1", title="hello", secret="hunter2")])

    [chunk] = client.index.return_value.add_documents.call_args.args
    doc = chunk[0]

    assert doc["title_upper"] == "HELLO"
    # ...and the encrypted field is still sealed.
    assert doc["secret"] != "hunter2"


@pytest.mark.asyncio
async def test_read_round_trip_decrypts_sealed_field() -> None:
    client = _client()
    adapter = _encrypting_adapter(client)

    await adapter.upsert([_Doc(id="1", title="hello", secret="hunter2")])
    [chunk] = client.index.return_value.add_documents.call_args.args
    index_doc = chunk[0]

    # Simulate a search hit coming back, then the read decrypt pre-pass + decode (the
    # shared search executor warms the codec before its synchronous decode).
    row = adapter.from_hit(index_doc)
    await adapter.spec.resolved_read_codec.prepare_decrypt([row])
    model = adapter.spec.resolved_read_codec.decode_mapping(row)

    assert model == _Doc(id="1", title="hello", secret="hunter2")


@pytest.mark.asyncio
async def test_plaintext_spec_indexes_plaintext() -> None:
    client = _client()
    spec = SearchSpec(name="items", model_type=_Doc, fields=["title"])
    adapter = MeilisearchSearchCommandAdapter(
        spec=spec,
        config=MeilisearchSearchConfig(index_uid="items_idx", wait_for_tasks=False),
        client=client,
    )

    await adapter.upsert([_Doc(id="1", title="hello", secret="plain")])

    [chunk] = client.index.return_value.add_documents.call_args.args
    assert chunk[0]["secret"] == "plain"
