"""The fidelity matrix — pairing, direction-split classification, explanation gating, rendering.

The two divergence directions have opposite costs, so the matrix must keep them apart and must
never offer a collapsed score; a divergence is admissible only when a reviewed engine-scoped
catalog entry explains it.
"""

from __future__ import annotations

import json

import pytest

from forze.application.contracts.transaction import IsolationLevel
from forze_dst.conformance import (
    BATTERY,
    CellVerdict,
    Classification,
    FidelityMatrix,
    Verdict,
    collect_verdicts,
    render_markdown,
    write_matrix,
)
from tests.support.isolation_conformance import MockConformanceBackend

# ----------------------- #

_RC = IsolationLevel.READ_COMMITTED
_SI = IsolationLevel.SNAPSHOT


def _cell(case: str, level: IsolationLevel, verdict: Verdict, *, adya: str = "G-single") -> CellVerdict:
    return CellVerdict(case=case, adya=adya, level=level, verdict=verdict)


# ....................... #


class TestPairingAndClassification:
    def test_agreement_is_agree_with_nothing_to_explain(self) -> None:
        mock = [_cell("read_skew", _RC, Verdict.PERMITTED)]
        real = [_cell("read_skew", _RC, Verdict.PERMITTED)]

        matrix = FidelityMatrix.pair(mock, real, engine="postgres")

        (cell,) = matrix.cells
        assert cell.classification is Classification.AGREE
        assert cell.explained_by is None
        assert not cell.unexplained
        assert matrix.unexplained == ()

    def test_mock_prevents_real_permits_is_the_strict_direction(self) -> None:
        # The ship-a-bug direction: simulation green, production bleeds. No catalog entry covers
        # a fabricated case, so it must land in unexplained.
        mock = [_cell("write_skew", _SI, Verdict.PREVENTED, adya="G2-item")]
        real = [_cell("write_skew", _SI, Verdict.PERMITTED, adya="G2-item")]

        matrix = FidelityMatrix.pair(mock, real, engine="postgres")

        (cell,) = matrix.cells
        assert cell.classification is Classification.MOCK_STRICT
        assert cell.unexplained
        assert matrix.mock_strict == (cell,)
        assert matrix.unexplained == (cell,)

    def test_mongo_rc_snapshot_reads_are_weak_and_explained(self) -> None:
        # The real catalog entry: Mongo transactions read one snapshot regardless of read concern,
        # so textbook-RC anomalies the mock permits are prevented on Mongo — divergence in the
        # false-alarm direction, explained by the reviewed engine-scoped strengthening.
        mock = [_cell("non_repeatable_read", _RC, Verdict.PERMITTED)]
        real = [_cell("non_repeatable_read", _RC, Verdict.PREVENTED)]

        matrix = FidelityMatrix.pair(mock, real, engine="mongo")

        (cell,) = matrix.cells
        assert cell.classification is Classification.MOCK_WEAK
        assert cell.explained_by == "strengthening:non_repeatable_read@READ_COMMITTED[mongo]"
        assert not cell.unexplained
        assert matrix.mock_weak == (cell,)
        assert matrix.unexplained == ()

    def test_engine_scoped_entry_never_explains_another_engine(self) -> None:
        # The same divergence on Postgres has no catalog entry — it must stay unexplained: an
        # engine-scoped strengthening must not leak into another backend's matrix.
        mock = [_cell("non_repeatable_read", _RC, Verdict.PERMITTED)]
        real = [_cell("non_repeatable_read", _RC, Verdict.PREVENTED)]

        matrix = FidelityMatrix.pair(mock, real, engine="postgres")

        assert matrix.unexplained == matrix.cells

    def test_missing_mock_counterpart_raises(self) -> None:
        with pytest.raises(ValueError, match="no mock verdict collected"):
            FidelityMatrix.pair([], [_cell("read_skew", _RC, Verdict.PERMITTED)], engine="postgres")

    def test_levels_come_from_the_real_leg(self) -> None:
        # An engine without a level contributes no cells for it (Mongo has no SERIALIZABLE) — the
        # matrix's level set is the real leg's, not the mock's.
        mock = [
            _cell("read_skew", _RC, Verdict.PERMITTED),
            _cell("read_skew", _SI, Verdict.PREVENTED),
            _cell("read_skew", IsolationLevel.SERIALIZABLE, Verdict.PREVENTED),
        ]
        real = [
            _cell("read_skew", _RC, Verdict.PERMITTED),
            _cell("read_skew", _SI, Verdict.PREVENTED),
        ]

        matrix = FidelityMatrix.pair(mock, real, engine="mongo")

        assert matrix.levels == (_RC, _SI)
        assert len(matrix.cells) == 2


# ....................... #


class TestMockAgainstItself:
    async def test_mock_vs_mock_is_full_agreement(self) -> None:
        # The degenerate differential: two collections from identical code must agree on every
        # cell — the sanity floor under the pairing logic.
        levels = tuple(IsolationLevel)
        mock_a = await collect_verdicts(MockConformanceBackend(), levels=levels)
        mock_b = await collect_verdicts(MockConformanceBackend(), levels=levels)

        matrix = FidelityMatrix.pair(mock_a, mock_b, engine="mock")

        assert len(matrix.cells) == len(BATTERY) * len(levels)
        assert all(cell.classification is Classification.AGREE for cell in matrix.cells)
        assert matrix.unexplained == ()


# ....................... #


class TestArtifactAndRendering:
    def _payload(self) -> dict[str, object]:
        mock = [
            _cell("read_skew", _RC, Verdict.PERMITTED),
            _cell("non_repeatable_read", _RC, Verdict.PERMITTED),
            _cell("write_skew", _SI, Verdict.PREVENTED, adya="G2-item"),
        ]
        real = [
            _cell("read_skew", _RC, Verdict.PERMITTED),
            _cell("non_repeatable_read", _RC, Verdict.PREVENTED),
            _cell("write_skew", _SI, Verdict.PERMITTED, adya="G2-item"),
        ]
        return FidelityMatrix.pair(mock, real, engine="mongo").to_payload()

    def test_write_matrix_writes_the_per_engine_json(self, tmp_path) -> None:
        matrix = FidelityMatrix.pair(
            [_cell("read_skew", _RC, Verdict.PERMITTED)],
            [_cell("read_skew", _RC, Verdict.PERMITTED)],
            engine="postgres",
        )

        path = write_matrix(matrix, tmp_path)

        assert path.name == "fidelity_postgres.json"
        payload = json.loads(path.read_text())
        assert payload["engine"] == "postgres"
        assert payload["cells"][0]["classification"] == "agree"

    def test_markdown_splits_directions_and_flags_unexplained(self) -> None:
        out = render_markdown([self._payload()])

        assert "# DST fidelity matrix" in out
        assert "## mock ↔ mongo" in out
        # Direction counts, never a single score.
        assert "mock-stricter (▲): **1**" in out
        assert "mock-weaker (△): **1**" in out
        assert "unexplained: **1**" in out
        # The explained divergence and the unexplained one render differently.
        assert "(explained)" in out
        assert "**UNEXPLAINED**" in out
        # Both catalogs are rendered from the live module.
        assert "## Contract strengthenings (reviewed)" in out
        assert "`lost_update`" in out
        assert "## Mechanism divergences" in out
        assert "`lock-block-vs-abort-conductor`" in out
