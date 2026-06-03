"""Sanity checks for codec benchmark tier fixtures."""

from __future__ import annotations

import pytest

from forze.base.serialization import MsgspecModelCodec, PydanticModelCodec
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


@pytest.mark.parametrize("tier", CODEC_BENCHMARK_TIERS, ids=lambda t: t.name)
def test_tier_msgspec_codec_decodes_fixture_rows(tier: CodecBenchmarkTier) -> None:
    rows = tier.sample_rows(8)
    codec = MsgspecModelCodec(tier.msgspec_struct)
    decoded = codec.decode_mapping_many(rows, trust_source=True)
    assert len(decoded) == 8


@pytest.mark.parametrize("tier", CODEC_BENCHMARK_TIERS, ids=lambda t: t.name)
def test_tier_msgspec_trust_source_matches_convert_many(tier: CodecBenchmarkTier) -> None:
    from forze.base.serialization.msgspec import (
        msgspec_convert_many,
        msgspec_validate_many,
    )

    rows = tier.sample_rows(8)
    via_codec = MsgspecModelCodec(tier.msgspec_struct).decode_mapping_many(
        rows,
        trust_source=True,
    )
    via_convert = msgspec_convert_many(tier.msgspec_struct, rows)
    assert via_codec == via_convert
    with_forbid = msgspec_validate_many(
        tier.msgspec_struct,
        rows,
        forbid_extra=True,
    )
    assert len(with_forbid) == 8


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
