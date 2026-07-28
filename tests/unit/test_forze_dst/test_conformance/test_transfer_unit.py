"""The transfer seam's fast tier — classification logic and the mock-vs-mock degenerate run.

The real differential is the Docker-gated Postgres leg; this tier pins the pieces that need no
container: direction classification, the parity property, artifact writing, and the sanity floor
that every script agrees with itself when both "backends" are the mock.
"""

from __future__ import annotations

import json

import pytest

from forze_dst.conformance import run_transfer, write_transfer
from forze_dst.conformance.transfer import (
    Detection,
    TransferClassification,
    TransferRecord,
    TransferScript,
    divergences,
)
from tests.support.isolation_conformance import MockConformanceBackend
from tests.support.misuse.transfer import SCRIPTS

# ----------------------- #


def _record(mock: Detection, real: Detection) -> TransferRecord:
    return TransferRecord(
        mutant_id="x",
        engine="postgres",
        expect_detected=True,
        mock=mock,
        real=real,
        classification=TransferClassification.AGREE,  # overwritten below where relevant
    )


class TestClassification:
    async def test_directions_are_classified_and_folded(self) -> None:
        async def detected(_backend) -> Detection:  # type: ignore[no-untyped-def]
            return Detection.DETECTED

        async def clean(_backend) -> Detection:  # type: ignore[no-untyped-def]
            return Detection.CLEAN

        class _Fixed:
            def __init__(self, name: str) -> None:
                self.scope_name = name

            def contexts(self, n: int):  # type: ignore[no-untyped-def]
                raise AssertionError("unused")

        scripts = (
            TransferScript(mutant_id="agree", expect_detected=True, run=detected),
            TransferScript(mutant_id="weak-side", expect_detected=False, run=clean),
        )
        records = await run_transfer(
            scripts, mock_backend=_Fixed("mock"), real_backend=_Fixed("postgres")
        )

        assert all(r.classification is TransferClassification.AGREE for r in records)
        assert all(r.mock_parity for r in records)
        assert divergences(records) == ()

    def test_parity_reads_the_corpus_expectation(self) -> None:
        killed = _record(Detection.DETECTED, Detection.DETECTED)
        missed = TransferRecord(
            mutant_id="x",
            engine="postgres",
            expect_detected=True,
            mock=Detection.CLEAN,
            real=Detection.CLEAN,
            classification=TransferClassification.AGREE,
        )

        assert killed.mock_parity
        assert not missed.mock_parity  # agreement alone is not enough — the mutant must detect


class TestMockAgainstItself:
    async def test_every_script_agrees_with_itself_on_the_mock(self) -> None:
        # The degenerate differential and the fast parity guard: both legs are the mock, so
        # every script must agree, and the mock verdicts must match the corpus expectations.
        records = await run_transfer(
            SCRIPTS,
            mock_backend=MockConformanceBackend(),
            real_backend=MockConformanceBackend(),
        )

        assert len(records) == len(SCRIPTS)
        assert divergences(records) == ()
        for record in records:
            assert record.mock_parity, f"{record.mutant_id}: {record.mock}"


class TestArtifact:
    def test_write_transfer_writes_per_engine_json(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        path = write_transfer([_record(Detection.DETECTED, Detection.DETECTED)], tmp_path)

        assert path.name == "transfer_postgres.json"
        payload = json.loads(path.read_text())
        assert payload[0]["classification"] == "agree"

    def test_write_transfer_refuses_empty(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        with pytest.raises(ValueError, match="no transfer records"):
            write_transfer([], tmp_path)
