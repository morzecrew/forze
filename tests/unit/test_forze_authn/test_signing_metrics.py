"""Access-token sign/verify counters + `instrument_signing` OTel export."""

from __future__ import annotations

import secrets
from typing import Any
from uuid import uuid4

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.base.exceptions import CoreException
from forze_identity.authn import instrument_signing
from forze_identity.authn.observability import (
    TOKENS_SIGNED_COUNTER,
    TOKENS_VERIFIED_COUNTER,
    TOKENS_VERIFY_FAILED_COUNTER,
)
from forze_identity.authn.services import AccessTokenConfig, AccessTokenService, Hs256Signer

# ----------------------- #

_CFG = AccessTokenConfig(issuer="it", audience="api")


def _svc() -> AccessTokenService:
    return AccessTokenService(signer=Hs256Signer(secret=secrets.token_bytes(32)), config=_CFG)


def _meter() -> tuple[Any, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    return MeterProvider(metric_readers=[reader]).get_meter("test"), reader


def _value(reader: InMemoryMetricReader, name: str) -> int:
    data = reader.get_metrics_data()
    assert data is not None

    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    return sum(dp.value for dp in metric.data.data_points)

    return 0


def _attrs(reader: InMemoryMetricReader, name: str) -> dict[str, Any]:
    data = reader.get_metrics_data()
    assert data is not None

    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    return dict(metric.data.data_points[0].attributes)

    return {}


# ....................... #


async def test_stats_count_sign_verify_and_failures() -> None:
    svc = _svc()

    token = await svc.issue_token(principal_id=uuid4())
    await svc.issue_token(principal_id=uuid4())
    await svc.verify_token(token)

    with pytest.raises(CoreException):
        await svc.verify_token("not.a.valid.token")

    stats = svc.signing_stats()
    assert stats.signed == 2
    assert stats.verified == 1
    assert stats.verify_failed == 1
    assert stats.algorithm == "HS256"
    assert stats.kid is None


async def test_instrument_signing_exports_counters() -> None:
    svc = _svc()
    await svc.issue_token(principal_id=uuid4())
    await svc.issue_token(principal_id=uuid4())

    with pytest.raises(CoreException):
        await svc.verify_token("bad")

    meter, reader = _meter()
    instrument_signing({"default": svc}, meter=meter)

    assert _value(reader, TOKENS_SIGNED_COUNTER) == 2
    assert _value(reader, TOKENS_VERIFIED_COUNTER) == 0
    assert _value(reader, TOKENS_VERIFY_FAILED_COUNTER) == 1

    attributes = _attrs(reader, TOKENS_SIGNED_COUNTER)
    assert attributes["forze.signer"] == "default"
    assert attributes["forze.signer.algorithm"] == "HS256"
    # Symmetric signer has no kid → label omitted.
    assert "forze.signer.kid" not in attributes
