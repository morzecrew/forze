"""The transfer seam's fast tier — classification logic and the mock-vs-mock degenerate run.

The real differential is the Docker-gated Postgres leg; this tier pins the pieces that need no
container: direction classification, the parity property, artifact writing, and the sanity floor
that every script agrees with itself when both "backends" are the mock.
"""

from __future__ import annotations

import json

import pytest

from forze_dst.conformance import render_transfer_markdown, run_transfer, write_transfer
from forze_dst.conformance.transfer import (
    Detection,
    TransferClassification,
    TransferRecord,
    TransferScript,
    divergences,
)
from tests.support.isolation_conformance import MockConformanceBackend
from tests.support.misuse import CONTROLS, CORPUS
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

    def test_parity_loss_is_a_divergence_finding_even_when_backends_agree(self) -> None:
        # A mutant BOTH backends cleared: classification AGREE, yet the corpus expectation was
        # never reproduced — folding it into a green differential would evidence nothing.
        missed = TransferRecord(
            mutant_id="x",
            engine="postgres",
            expect_detected=True,
            mock=Detection.CLEAN,
            real=Detection.CLEAN,
            classification=TransferClassification.AGREE,
        )

        assert divergences([missed]) == (missed,)
        assert divergences([_record(Detection.DETECTED, Detection.DETECTED)]) == ()


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


class TestRenderer:
    async def _payload(self, tmp_path) -> list[dict[str, object]]:  # type: ignore[no-untyped-def]
        # End-to-end through the artifact: run -> write_transfer -> parsed JSON -> renderer.
        records = await run_transfer(
            SCRIPTS,
            mock_backend=MockConformanceBackend(),
            real_backend=MockConformanceBackend(),
        )
        path = write_transfer(records, tmp_path)
        return json.loads(path.read_text())  # type: ignore[no-any-return]

    async def test_renders_registry_join_and_not_transferable_fraction(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        payload = await self._payload(tmp_path)

        document = render_transfer_markdown(payload, corpus=CORPUS, controls=CONTROLS)

        assert "## mock ↔ mock" in document
        assert "mock artifacts (▲): **0** · mock blind spots (△): **0**" in document
        for script in SCRIPTS:
            assert f"`{script.mutant_id}`" in document
        # The declared not-transferable fraction, with its stated reason — never dropped.
        assert f"### Not transferable — 1/{len(CORPUS)} mutants" in document
        assert "`T2-charge-before-guard`" in document
        assert "trace-level marker" in document

    def test_record_without_registry_entry_is_an_error(self) -> None:
        ghost = {
            "mutant_id": "ghost",
            "engine": "postgres",
            "expect_detected": True,
            "mock": "detected",
            "real": "detected",
            "classification": "agree",
        }

        with pytest.raises(ValueError, match="no corpus/control entry"):
            render_transfer_markdown([ghost], corpus=CORPUS, controls=CONTROLS)

    async def test_missing_transferable_record_is_an_error(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        payload = await self._payload(tmp_path)
        partial = [record for record in payload if record["mutant_id"] != SCRIPTS[0].mutant_id]

        with pytest.raises(ValueError, match="missing transfer records"):
            render_transfer_markdown(partial, corpus=CORPUS, controls=CONTROLS)


class TestDivergentClassifications:
    async def test_both_directions_classify_and_surface(self) -> None:
        # Backends that disagree by name: the mock-only detection is the false-alarm
        # direction (MOCK_STRICT), the real-only detection the dangerous one (MOCK_WEAK).
        async def mock_only(backend) -> Detection:  # type: ignore[no-untyped-def]
            return Detection.DETECTED if backend.scope_name == "mock" else Detection.CLEAN

        async def real_only(backend) -> Detection:  # type: ignore[no-untyped-def]
            return Detection.CLEAN if backend.scope_name == "mock" else Detection.DETECTED

        class _Named:
            def __init__(self, name: str) -> None:
                self.scope_name = name

            def contexts(self, n: int):  # type: ignore[no-untyped-def]
                raise AssertionError("unused")

        records = await run_transfer(
            (
                TransferScript(mutant_id="artifact", expect_detected=True, run=mock_only),
                TransferScript(mutant_id="blind-spot", expect_detected=True, run=real_only),
            ),
            mock_backend=_Named("mock"),
            real_backend=_Named("postgres"),
        )

        strict, weak = records
        assert strict.classification is TransferClassification.MOCK_STRICT
        assert weak.classification is TransferClassification.MOCK_WEAK
        assert divergences(records) == records
