"""Micro-benchmarks for the Pydantic :class:`~forze.base.serialization.ModelCodec` decode paths.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``.

Run **only** codec benchmarks (no full ``tests/perf`` suite, no Docker)::

    just perf tests/perf/test_forze_codec_perf.py

Run one tier::

    just perf tests/perf/test_forze_codec_perf.py -k "simple"

Compare decode modes (strict / trusted)::

    just perf tests/perf/test_forze_codec_perf.py -k "simple and decode"

Save a baseline (optional)::

    just perf tests/perf/test_forze_codec_perf.py --benchmark-save=codec-decode

Tiers: :mod:`tests.support.codec_benchmark_models` (shared ``JsonDict`` fixtures).
"""

from __future__ import annotations

from typing import Any

import pytest

from forze.base.serialization import PydanticModelCodec
from tests.support.codec_benchmark_models import (
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
    """Pydantic trusted ``decode_mapping_many`` (unknown-column precheck + batched TypeAdapter)."""

    codec = PydanticModelCodec(codec_tier.pydantic_model)
    rows = codec_tier.sample_rows(_ROWS)

    benchmark(lambda: codec.decode_mapping_many(rows, trust_source=True))


# ----------------------- #
# Model hash + transform + per-item trace gate
# ----------------------- #


@pytest.mark.perf
def test_codec_pydantic_model_hash_benchmark(
    benchmark: Any,
    codec_tier: CodecBenchmarkTier,
) -> None:
    """``pydantic_model_hash`` over a batch.

    Uses the ``orjson`` ``default=`` path (no recursive normalization deep-copy).
    """

    from forze.base.serialization.pydantic import pydantic_model_hash

    codec = PydanticModelCodec(codec_tier.pydantic_model)
    models = codec.decode_mapping_many(codec_tier.sample_rows(_ROWS), trust_source=True)

    benchmark(lambda: [pydantic_model_hash(m) for m in models])


@pytest.mark.perf
def test_codec_pydantic_transform_many_benchmark(
    benchmark: Any,
    codec_tier: CodecBenchmarkTier,
) -> None:
    """``pydantic_transform_many`` (dump + revalidate, two passes) for visibility."""

    from forze.base.serialization.pydantic import pydantic_transform_many

    codec = PydanticModelCodec(codec_tier.pydantic_model)
    models = codec.decode_mapping_many(codec_tier.sample_rows(_ROWS), trust_source=True)

    benchmark(lambda: pydantic_transform_many(codec_tier.pydantic_model, models))


@pytest.mark.perf
def test_codec_pydantic_single_decode_info_logging_benchmark(
    benchmark: Any,
    codec_tier: CodecBenchmarkTier,
) -> None:
    """Single-item ``decode_mapping`` with logging configured at INFO.

    Exercises the per-item ``logger.trace`` fast-skip on the hot codec path (the
    gate short-circuits before building the event when trace is below the level).
    """

    from forze.base.logging import configure_logging

    configure_logging(level="info", render_mode="json")

    codec = PydanticModelCodec(codec_tier.pydantic_model)
    row = codec_tier.sample_rows(1)[0]

    benchmark(lambda: codec.decode_mapping(row, trust_source=False))


CODEC_TIER_NAMES: tuple[CodecTierName, ...] = tuple(t.name for t in CODEC_BENCHMARK_TIERS)


# In-process and deterministic: participates in the CI perf regression gate.
pytestmark = pytest.mark.perf_gate
