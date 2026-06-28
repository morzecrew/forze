"""Sanity checks for codec benchmark tier fixtures."""

from __future__ import annotations

import pytest

from forze.base.serialization.pydantic import pydantic_validate_many
from tests.support.codec_benchmark_models import (
    CODEC_BENCHMARK_TIERS,
    CodecBenchmarkTier,
    SimpleCodecRow,
)


@pytest.mark.parametrize("tier", CODEC_BENCHMARK_TIERS, ids=lambda t: t.name)
def test_tier_sample_rows_pass_strict_decode(tier: CodecBenchmarkTier) -> None:
    rows = tier.sample_rows(8)
    decoded = pydantic_validate_many(tier.pydantic_model, rows, trust_source=False)
    assert len(decoded) == 8


@pytest.mark.parametrize("tier", CODEC_BENCHMARK_TIERS, ids=lambda t: t.name)
def test_tier_trusted_decodes_fixture_rows(tier: CodecBenchmarkTier) -> None:
    rows = tier.sample_rows(8)
    decoded = pydantic_validate_many(tier.pydantic_model, rows, trust_source=True)
    assert len(decoded) == 8


def test_simple_tier_trusted_matches_strict() -> None:
    """Scalars only: trusted construct matches strict validation."""

    try:
        simple_tier = next(t for t in CODEC_BENCHMARK_TIERS if t.name == "simple")
    except StopIteration as exc:
        msg = 'codec benchmark tier "simple" is not registered'
        raise AssertionError(msg) from exc

    rows = simple_tier.sample_rows(8)
    strict = pydantic_validate_many(SimpleCodecRow, rows, trust_source=False)
    trusted = pydantic_validate_many(SimpleCodecRow, rows, trust_source=True)
    assert strict == trusted
