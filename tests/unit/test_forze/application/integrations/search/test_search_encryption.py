"""Shared search field-encryption resolver: wraps the read codec so in-place search
results decrypt, fail-closed without a keyring."""

from __future__ import annotations

import base64

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.search import SearchSpec
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.search import resolve_search_read_codec_spec
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement

# ----------------------- #


class _Doc(BaseModel):
    id: str
    title: str
    secret: str


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _spec(**kw: object) -> SearchSpec[_Doc]:
    return SearchSpec(name="docs", model_type=_Doc, fields=["title"], **kw)  # type: ignore[arg-type]


# ....................... #


def test_no_encrypted_fields_returns_spec_unchanged() -> None:
    spec = _spec()
    assert (
        resolve_search_read_codec_spec(
            spec, keyring=_keyring(), deterministic=None, tenant_provider=lambda: None
        )
        is spec
    )


def test_encrypted_fields_without_keyring_fails_closed() -> None:
    spec = _spec(encrypted_fields=frozenset({"secret"}))

    with pytest.raises(CoreException) as ei:
        resolve_search_read_codec_spec(
            spec, keyring=None, deterministic=None, tenant_provider=lambda: None
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.search.encryption_wiring"


@pytest.mark.asyncio
async def test_wrapped_codec_decrypts_in_place_search_rows() -> None:
    """The resolver yields an encrypting codec that the search read path warms + decodes —
    decrypting the document table's ciphertext out of search results."""

    spec = resolve_search_read_codec_spec(
        _spec(encrypted_fields=frozenset({"secret"})),
        keyring=_keyring(),
        deterministic=None,
        tenant_provider=lambda: None,
    )
    codec = spec.resolved_read_codec

    # The document gateway would have written this ciphertext; emulate via the same codec.
    await codec.prepare_encrypt()
    row = codec.encode_persistence_mapping(_Doc(id="1", title="t", secret="s3cr3t"))
    assert is_envelope(base64.b64decode(row["secret"]))  # sealed at rest

    # The search read path: warm the decrypt cache, then the synchronous decode.
    await codec.prepare_decrypt([row])
    assert codec.decode_mapping(row) == _Doc(id="1", title="t", secret="s3cr3t")
