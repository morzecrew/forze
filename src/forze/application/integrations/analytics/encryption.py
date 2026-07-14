"""Shared field-encryption resolution for warehouse analytics adapters.

Every analytics backend (Postgres, ClickHouse, BigQuery, DuckDB, mock) uses this to wrap an
:class:`AnalyticsSpec`'s read & ingest codecs with an :class:`EncryptingModelCodec`, so
encrypted columns are sealed on ingest and decrypted out of every read path.

Encrypted analytics columns are **confidential, not analyzable**: randomized ciphertext has
no numeric/linguistic structure, so it cannot be aggregated, grouped, or range-filtered — only
stored and returned. Encrypt columns you carry but never query (e.g. PII alongside the
dimensions/measures). ``binds_record_id`` is unsupported (analytics rows have no stable id).
"""

from collections.abc import Callable
from typing import Any, Literal

import attrs
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.contracts.crypto import (
    DeterministicFieldCipherPort,
    FieldCipherPort,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import EncryptingModelCodec
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec

# ----------------------- #

_WIRING_CODE = "core.analytics.encryption_wiring"


def resolve_analytics_codecs_spec(
    spec: AnalyticsSpec[Any, Any],
    *,
    keyring: FieldCipherPort | None,
    deterministic: DeterministicFieldCipherPort | None,
    tenant_provider: Callable[[], TenantIdentity | None],
) -> AnalyticsSpec[Any, Any]:
    """Return *spec* with its read & ingest codecs wrapped for field encryption, or unchanged.

    Fail-closed: declaring encrypted/searchable fields without the matching cipher wired
    raises rather than silently persisting / returning plaintext.
    """

    encryption = spec.encryption
    if encryption is None or encryption.is_empty:
        return spec

    # ``binds_record_id`` is rejected at spec construction (validate_analytics_spec) — analytics
    # rows have no stable id — so it is always False here.

    if keyring is None:
        raise exc.configuration(
            f"AnalyticsSpec {spec.name!r} declares encrypted/searchable fields but no keyring "
            "is wired. Register a CryptoDepsModule or clear the encrypted fields.",
            code=_WIRING_CODE,
        )

    if encryption.searchable and deterministic is None:
        raise exc.configuration(
            f"AnalyticsSpec {spec.name!r} declares searchable fields but no deterministic "
            "cipher is wired (CryptoDepsModule(deterministic_root=...)).",
            code=_WIRING_CODE,
        )

    def _wrap(inner: ModelCodec[Any, Any]) -> EncryptingModelCodec[Any]:
        return EncryptingModelCodec(
            inner=inner,
            cipher=keyring,
            fields=encryption.encrypted,
            searchable_fields=encryption.searchable,
            deterministic=deterministic,
            tenant_provider=tenant_provider,
            reject_plaintext=encryption.reject_plaintext,
        )

    read_codec = _wrap(spec.resolved_read_codec)
    ingest_codec = _wrap(ic) if (ic := spec.resolved_ingest_codec) is not None else None

    return attrs.evolve(spec, read_codec=read_codec, ingest_codec=ingest_codec)


# ....................... #


async def encode_ingest_payloads(
    ingest_codec: ModelCodec[Any, Any],
    rows: list[Any],
    *,
    mode: Literal["json", "python"] = "python",
) -> list[JsonDict]:
    """Encode ingest *rows* to property maps, sealing encrypted columns when the codec encrypts.

    Shared by every backend's ``append`` path: when *ingest_codec* is an encrypting codec it is
    warmed once and rows go through ``encode_persistence_mapping`` (which seals the fields); a
    plain codec keeps the existing ``encode_mapping`` behaviour unchanged. Rows must be
    instances of the ingest model (or any ``BaseModel``, re-decoded through the codec).

    **The caller picks *mode*, because the right answer is a property of the transport, not of
    the rows.** An ingest row is not JSON — it is a row of typed warehouse columns, and how a
    ``UUID`` or a ``datetime`` should be spelled depends entirely on what carries it there:

    - ``"python"`` (the default) keeps them as Python objects, which is what a driver that binds
      values natively wants — Postgres (psycopg) and ClickHouse (clickhouse-connect) both map
      them straight onto a ``UUID`` / ``DateTime64`` / ``Decimal`` column, and handing those
      drivers strings instead would be a lossy round-trip through the column's own parser.
    - ``"json"`` is for a backend whose transport **is** JSON. BigQuery's streaming insert is an
      HTTP ``insertAll`` — the client hands the row map to a JSON serializer, which cannot encode
      a ``UUID``, a ``datetime`` or a ``Decimal`` at all, so those rows raised ``TypeError``
      before the request was ever sent. BigQuery accepts RFC-3339 strings for ``TIMESTAMP`` and
      decimal strings for ``NUMERIC``, so the JSON encode is exactly what that wire wants.

    A single default for both would be wrong for one of them, which is why this is a parameter
    and not a decision made here.
    """

    # ``prepare_encrypt`` is the encrypting-codec discriminator (only ``EncryptingModelCodec``
    # defines it). ``encode_persistence_mapping`` is *not* — it is a base ``ModelCodec`` method
    # present on every codec, so it cannot tell encrypting from plain; gating on it would route
    # plain ingest through the persistence path too. The two are always co-present on the
    # encrypting codec, so the ``encrypts`` branch below safely calls the latter.
    prepare_encrypt = getattr(ingest_codec, "prepare_encrypt", None)
    encrypts = prepare_encrypt is not None
    if prepare_encrypt is not None:
        await prepare_encrypt()

    out: list[JsonDict] = []
    for row in rows:
        if isinstance(row, ingest_codec.model_type):
            model = row
        elif isinstance(row, BaseModel):
            model = ingest_codec.decode_mapping(row.model_dump())
        else:
            raise exc.internal("Analytics ingest rows must be Pydantic model instances.")

        out.append(
            ingest_codec.encode_persistence_mapping(model, mode=mode)
            if encrypts
            else ingest_codec.encode_mapping(model, mode=mode)
        )

    return out
