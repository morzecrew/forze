"""Micro-benchmarks for Pydantic and msgspec :class:`~forze.base.serialization.ModelCodec` decode paths.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``.

Run **only** codec benchmarks (no full ``tests/perf`` suite, no Docker)::

    just perf tests/perf/test_forze_codec_perf.py

Compare backends on one tier::

    just perf tests/perf/test_forze_codec_perf.py -k "simple"

Compare decode modes (Pydantic strict/trusted, msgspec convert/forbid_extra)::

    just perf tests/perf/test_forze_codec_perf.py -k "simple and decode"

Filter msgspec only::

    just perf tests/perf/test_forze_codec_perf.py -k msgspec

Save a baseline (optional)::

    just perf tests/perf/test_forze_codec_perf.py --benchmark-save=codec-decode

Tiers: :mod:`tests.perf.support.codec_benchmark_models` (shared ``JsonDict`` fixtures).
"""

from __future__ import annotations

from typing import Any

import pytest

from forze.base.serialization import MsgspecModelCodec, PydanticModelCodec
from tests.perf.support.codec_benchmark_models import (
    CODEC_BENCHMARK_TIERS,
    CodecBenchmarkTier,
    CodecTierName,
)

_ROWS = 1_000


@pytest.fixture(params=CODEC_BENCHMARK_TIERS, ids=lambda t: t.name)
def codec_tier(request: pytest.FixtureRequest) -> CodecBenchmarkTier:
    return request.param


# ----------------------- #
# Pydantic
# ----------------------- #


@pytest.mark.perf
def test_codec_pydantic_strict_decode_benchmark(
    benchmark: Any,
    codec_tier: CodecBenchmarkTier,
) -> None:
    """Pydantic strict ``decode_mapping_many`` (batched TypeAdapter)."""

    codec = PydanticModelCodec(codec_tier.pydantic_model)
    rows = codec_tier.sample_rows(_ROWS)

    benchmark(lambda: codec.decode_mapping_many(rows, trust_source=False))


@pytest.mark.perf
def test_codec_pydantic_trusted_decode_benchmark(
    benchmark: Any,
    codec_tier: CodecBenchmarkTier,
) -> None:
    """Pydantic trusted ``decode_mapping_many`` (construct loop)."""

    codec = PydanticModelCodec(codec_tier.pydantic_model)
    rows = codec_tier.sample_rows(_ROWS)

    benchmark(lambda: codec.decode_mapping_many(rows, trust_source=True))


# ----------------------- #
# Msgspec
# ----------------------- #


@pytest.mark.perf
def test_codec_msgspec_decode_benchmark(
    benchmark: Any,
    codec_tier: CodecBenchmarkTier,
) -> None:
    """Msgspec bulk ``msgspec.convert`` (``trust_source=True`` / default fast path)."""

    codec = MsgspecModelCodec(codec_tier.msgspec_struct)
    rows = codec_tier.sample_rows(_ROWS)

    benchmark(lambda: codec.decode_mapping_many(rows, trust_source=True))


@pytest.mark.perf
def test_codec_msgspec_forbid_extra_decode_benchmark(
    benchmark: Any,
    codec_tier: CodecBenchmarkTier,
) -> None:
    """Msgspec decode with per-row unknown-field scan before convert."""

    codec = MsgspecModelCodec(codec_tier.msgspec_struct)
    rows = codec_tier.sample_rows(_ROWS)

    benchmark(
        lambda: codec.decode_mapping_many(rows, trust_source=False, forbid_extra=True),
    )


CODEC_TIER_NAMES: tuple[CodecTierName, ...] = tuple(t.name for t in CODEC_BENCHMARK_TIERS)
