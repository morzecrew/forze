"""Mechanical depth extraction — the locked 1-minimal choice-vector procedure on real corpus cases.

The labels these produce are the axis the seed-statistics experiments plot over, so the
extractor itself is exercised on the shipped corpus: a duplicate-delivery mutant must come out
depth-1 with an *empty* minimized schedule (the workload alone suffices), and the result must
reproduce — same vector, same seed — because the labels are reviewed data.
"""

from __future__ import annotations

import pytest

from forze_dst.depth import DepthEvidence, extract_depth
from forze_dst.misuse import MisuseCase
from tests.support.misuse import CORPUS
from tests.support.misuse.idempotency import i1_retry_without_key

# ----------------------- #


class TestDepthEvidence:
    def test_depth_must_match_the_vector(self) -> None:
        DepthEvidence(depth=2, choices=(0, 1), seed=0, act_count=2, concurrency=2)

        with pytest.raises(ValueError, match="does not match"):
            DepthEvidence(depth=3, choices=(0, 1), seed=0, act_count=2, concurrency=2)


class TestExtractDepth:
    def test_duplicate_delivery_is_depth_one_with_an_empty_schedule(self) -> None:
        evidence = extract_depth(i1_retry_without_key(), act_count=4, concurrency=1)

        assert evidence.depth == 1
        assert evidence.choices == ()  # plain FIFO already violates — zero ordering constraints
        assert "mechanical" in evidence.note()

    def test_extraction_reproduces(self) -> None:
        first = extract_depth(i1_retry_without_key(), act_count=4, concurrency=1)
        second = extract_depth(i1_retry_without_key(), act_count=4, concurrency=1)

        assert (first.depth, first.choices, first.seed) == (
            second.depth,
            second.choices,
            second.seed,
        )

    def test_registry_depth_matches_a_fresh_extraction_for_the_i_family(self) -> None:
        # The reviewed label and the tool must agree — the smoke-priced spot check; the full
        # corpus re-derivation is the offline pass that produced the registry evidence.
        mutant = next(m for m in CORPUS if m.mutant_id == "I1-retry-without-key")
        evidence = extract_depth(
            i1_retry_without_key(),
            act_count=4,
            concurrency=1,
        )

        assert evidence.depth == mutant.depth

    def test_requires_an_explicit_scenario(self) -> None:
        case = i1_retry_without_key()
        with pytest.raises(ValueError, match="explicit scenario"):
            extract_depth(
                MisuseCase(simulation=case.simulation, scenario=None),
                act_count=2,
                concurrency=1,
            )
