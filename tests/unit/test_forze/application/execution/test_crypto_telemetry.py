"""`instrument_crypto` exports keyring KMS + cache counters as OTel metrics."""

from __future__ import annotations

from typing import Any

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.crypto import AesGcmAead, KeyRef, StaticKeyDirectory
from forze.application.execution.observability import (
    CRYPTO_CACHE_HITS_COUNTER,
    CRYPTO_COLD_MISS_COUNTER,
    CRYPTO_DATA_KEYS_GENERATED_COUNTER,
    CRYPTO_DATA_KEYS_UNWRAPPED_COUNTER,
    instrument_crypto,
)
from forze.application.integrations.crypto import Keyring
from forze_mock import MockKeyManagement

# ----------------------- #


def _ring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _meter() -> tuple[Any, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    return MeterProvider(metric_readers=[reader]).get_meter("test"), reader


def _points(reader: InMemoryMetricReader, name: str) -> list[tuple[dict[str, Any], Any]]:
    data = reader.get_metrics_data()
    out: list[tuple[dict[str, Any], Any]] = []

    if data is None:
        return out

    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    for dp in metric.data.data_points:
                        out.append((dict(dp.attributes), dp))

    return out


# ....................... #


async def test_instrument_crypto_reports_generate_unwrap_hits() -> None:
    producer = _ring()
    await producer.warm(None)
    blob = producer.encrypt_sync(b"secret", tenant=None)  # 1 generate, 1 enc hit
    producer.decrypt_sync(blob)  # 1 dec hit (seeded cache)

    reader_ring = _ring()
    await reader_ring.decrypt(blob)  # 1 unwrap

    meter, reader = _meter()
    instrument_crypto({"producer": producer, "reader": reader_ring}, meter=meter)

    generated = dict(
        (a["forze.keyring"], dp.value)
        for a, dp in _points(reader, CRYPTO_DATA_KEYS_GENERATED_COUNTER)
    )
    assert generated["producer"] == 1
    assert generated["reader"] == 0

    unwrapped = dict(
        (a["forze.keyring"], dp.value)
        for a, dp in _points(reader, CRYPTO_DATA_KEYS_UNWRAPPED_COUNTER)
    )
    assert unwrapped["reader"] == 1

    # Cache hits are split by path via the ``forze.crypto.path`` label.
    hits = {
        (a["forze.keyring"], a["forze.crypto.path"]): dp.value
        for a, dp in _points(reader, CRYPTO_CACHE_HITS_COUNTER)
    }
    assert hits[("producer", "encrypt")] == 1
    assert hits[("producer", "decrypt")] == 1


async def test_instrument_crypto_reports_cold_misses() -> None:
    cold = _ring()
    producer = _ring()
    await producer.warm(None)
    blob = producer.encrypt_sync(b"x", tenant=None)

    try:
        cold.decrypt_sync(blob)
    except Exception:
        pass

    meter, reader = _meter()
    instrument_crypto({"cold": cold}, meter=meter)

    cold_points = {
        a["forze.keyring"]: dp.value
        for a, dp in _points(reader, CRYPTO_COLD_MISS_COUNTER)
    }
    assert cold_points["cold"] == 1
