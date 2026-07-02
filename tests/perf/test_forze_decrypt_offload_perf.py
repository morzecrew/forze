"""Micro-benchmark for encrypting-codec batch field decrypt — the ``run_cpu_map`` target.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via::

    just perf tests/perf/test_forze_decrypt_offload_perf.py

Measures the **event-loop stall** of decrypting a batch of encrypted rows inline — the work
the document gateway now offloads off the loop (via ``run_cpu_map``) once a batch reaches
``_DECRYPT_OFFLOAD_THRESHOLD`` rows. Read the per-size timings to justify/tune that threshold:
offloading wins once the inline stall exceeds a worker hand-off (~tens of µs) plus a context
copy. The second benchmark confirms the frozen decrypt snapshot (thread-local key dicts) has
no per-row overhead versus the live cache path, so the offload's only cost is the hand-off.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import AesGcmAead, KeyRef, StaticKeyDirectory
from forze.application.integrations.crypto import EncryptingModelCodec, Keyring
from forze.base.serialization import default_model_codec

from forze_mock import MockKeyManagement

# ----------------------- #


class _Doc(BaseModel):
    id: str
    a: str
    b: str
    c: str
    d: str


_ENCRYPTED = frozenset({"a", "b", "c", "d"})  # 4 randomized fields → 4 AEAD opens/row


def _codec() -> EncryptingModelCodec[_Doc]:
    return EncryptingModelCodec(
        inner=default_model_codec(_Doc),
        cipher=Keyring(
            kms=MockKeyManagement(),
            aead=AesGcmAead(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        fields=_ENCRYPTED,
        tenant_provider=lambda: None,
    )


def _encrypted_rows(codec: EncryptingModelCodec[_Doc], n: int) -> list[dict[str, Any]]:
    asyncio.run(codec.prepare_encrypt())
    rows = [
        codec.encode_persistence_mapping(_Doc(id=str(i), a="x", b="y", c="z", d="w"))
        for i in range(n)
    ]
    asyncio.run(codec.prepare_decrypt(rows))
    return rows


# ----------------------- #


@pytest.mark.perf
@pytest.mark.parametrize("n_rows", [16, 64, 256, 1024])
def test_inline_batch_decrypt_stall(benchmark: Any, n_rows: int) -> None:
    """Inline sync decrypt of *n_rows* encrypted rows — the loop stall the offload removes."""

    codec = _codec()
    rows = _encrypted_rows(codec, n_rows)

    benchmark(lambda: codec.decode_mapping_many([dict(r) for r in rows]))


@pytest.mark.perf
@pytest.mark.parametrize("n_rows", [256, 1024])
def test_frozen_snapshot_decode_cost(benchmark: Any, n_rows: int) -> None:
    """Frozen-snapshot per-row decode cost — confirms no overhead vs the live cache path."""

    codec = _codec()
    rows = _encrypted_rows(codec, n_rows)
    frozen = codec.freeze_for_decrypt(rows)
    assert frozen is not None

    benchmark(lambda: [frozen.decode_mapping(dict(r)) for r in rows])
