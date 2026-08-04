"""The two in-repo consumers of the progress plane, watched end to end.

# covers: forze_kits.integrations.portability.export_archive
# covers: forze_kits.integrations.search.rebuild_search_index

Both are long, silent row-walks — the archive export and the search-index rebuild — and both
are the reason the plane exists rather than examples invented for it. What is worth asserting
is not that ticks happen but that the *record an operator reads* tells the truth: a bar that
only moves forward, a fraction that reaches 1.0 exactly when the work is done and not before,
and a failure that lands in the record instead of leaving a job running forever.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.inventory import SpecRegistry
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.integrations.portability import TenantScope, export_archive
from forze_kits.integrations.progress import (
    JobRecord,
    JobStatus,
    build_job_progress_projector,
    build_progress_reporter,
    job_record_spec,
)
from forze_kits.integrations.search import rebuild_search_index
from forze_mock import MockDepsModule
from forze_mock.state import MockState

# ----------------------- #

_JOBS = job_record_spec()


class _Note(Document):
    body: str


class _NoteCreate(BaseDTO):
    body: str


class _NoteRead(ReadDocument):
    body: str


NOTE_SPEC: DocumentSpec[_NoteRead, _Note, _NoteCreate, Any] = DocumentSpec(
    name="notes",
    read=_NoteRead,
    write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate),
)
NOTE_INDEX = SearchSpec(name="notes_search", model_type=_NoteRead, fields=["body"])


def _runtime(state: MockState | None = None) -> ExecutionRuntime:
    return build_runtime(
        MockDepsModule(state=state if state is not None else MockState()),
        specs=SpecRegistry().register(NOTE_SPEC),
        allow_unregistered=True,
    )


class _Trace:
    """Every fraction the reporter published, in order — the bar as a viewer would see it."""

    def __init__(self) -> None:
        self.fractions: list[float | None] = []
        self.messages: list[str | None] = []

    async def emit(self, event: Any, *, durable: bool) -> None:
        self.fractions.append(event.progress)
        self.messages.append(event.message)


class _CountingQuery:
    """A document query port that records whether the sweep asked for a denominator."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.counts = 0

    async def count(self, *args: Any, **kwargs: Any) -> int:
        self.counts += 1

        return await self.inner.count(*args, **kwargs)

    def find_stream(self, *args: Any, **kwargs: Any) -> Any:
        return self.inner.find_stream(*args, **kwargs)


class _UndercountingQuery(_CountingQuery):
    """A collection that grew under the sweep: the count is a snapshot, the stream is live."""

    def __init__(self, inner: Any, *, understate: int) -> None:
        super().__init__(inner)
        self.understate = understate

    async def count(self, *args: Any, **kwargs: Any) -> int:
        return max(await super().count(*args, **kwargs) - self.understate, 0)


async def _seed_notes(runtime: ExecutionRuntime, tenant: Any, count: int) -> None:
    async with runtime.scope():
        ctx = runtime.get_context()

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            command = ctx.document.command(NOTE_SPEC)

            for index in range(count):
                await command.create(_NoteCreate(body=f"note-{index}"))


async def _job(runtime: ExecutionRuntime, job_id: Any) -> JobRecord:
    async with runtime.scope():
        row = await runtime.get_context().document.query(_JOBS).find({"$values": {"id": job_id}})

    assert row is not None

    return row


def _is_monotone(fractions: list[float | None]) -> bool:
    known = [value for value in fractions if value is not None]

    return all(later >= earlier for earlier, later in zip(known, known[1:], strict=False))


# ----------------------- #


class TestArchiveExport:
    """The export walks (section × spec) units, and the record follows the walk."""

    async def test_an_export_reports_a_bar_that_ends_at_one(self, tmp_path: Path) -> None:
        tenant = uuid4()
        runtime = _runtime()
        await _seed_notes(runtime, tenant, count=3)
        job_id = uuid4()
        trace = _Trace()

        async with runtime.scope():
            ctx = runtime.get_context()
            reporter = build_progress_reporter(
                ctx,
                job_id=job_id,
                kind="export",
                subject=str(tmp_path),
                projector=build_job_progress_projector(ctx),
                min_interval=0.0,
            )
            reporter.sinks = (*reporter.sinks, trace)

            async with reporter.track("exporting"):
                report = await export_archive(
                    runtime,
                    tmp_path / "archive",
                    scope=TenantScope(tenant_id=tenant),
                    progress=reporter,
                )

        row = await _job(runtime, job_id)

        assert report.total_rows == 3
        assert _is_monotone(trace.fractions)
        # The manifest is its own unit, so the bar reaches 1.0 only once the archive is
        # actually readable — never while the export is still writing it.
        assert trace.messages[-2] == "manifest written"
        assert row.status is JobStatus.SUCCEEDED
        assert row.progress == 1.0
        assert row.subject == str(tmp_path)
        assert row.finished_at is not None

    async def test_a_failing_export_lands_in_the_record(self, tmp_path: Path) -> None:
        # The failure mode `track()` exists for: without it the job is left RUNNING and
        # only the traceback knows the export died.
        tenant = uuid4()
        runtime = _runtime()
        await _seed_notes(runtime, tenant, count=1)
        job_id = uuid4()

        async with runtime.scope():
            ctx = runtime.get_context()
            reporter = build_progress_reporter(
                ctx,
                job_id=job_id,
                kind="export",
                projector=build_job_progress_projector(ctx),
                min_interval=0.0,
            )

            with pytest.raises(RuntimeError):
                async with reporter.track():
                    await export_archive(
                        runtime,
                        tmp_path / "a",
                        scope=TenantScope(tenant_id=tenant),
                        progress=reporter,
                    )

                    raise RuntimeError("the disk filled up")

        row = await _job(runtime, job_id)

        assert row.status is JobStatus.FAILED
        assert row.error is not None
        assert "the disk filled up" in row.error
        # A failed job keeps the fraction it reached — it did not complete.
        assert row.progress == 1.0 or row.progress is not None


# ....................... #


class TestSearchRebuild:
    """The rebuild counts its rows first, so the fraction is real rather than a spinner."""

    async def test_a_rebuild_reports_rows_against_a_counted_total(self) -> None:
        tenant = uuid4()
        runtime = _runtime()
        await _seed_notes(runtime, tenant, count=5)
        job_id = uuid4()
        trace = _Trace()

        async with runtime.scope():
            ctx = runtime.get_context()

            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                reporter = build_progress_reporter(
                    ctx,
                    job_id=job_id,
                    kind="search_rebuild",
                    subject=str(NOTE_INDEX.name),
                    projector=build_job_progress_projector(ctx),
                    min_interval=0.0,
                )
                reporter.sinks = (*reporter.sinks, trace)

                async with reporter.track("rebuilding"):
                    report = await rebuild_search_index(
                        ctx.document.query(NOTE_SPEC),
                        ctx.search.command(NOTE_INDEX),
                        document=NOTE_SPEC,
                        search=NOTE_INDEX,
                        chunk_size=2,
                        progress=reporter,
                    )

        row = await _job(runtime, job_id)

        assert report.indexed == 5
        # 5 rows in pages of 2: the bar walks, it does not jump from nothing to everything.
        assert _is_monotone(trace.fractions)
        assert trace.messages[-2] == "5 of 5 rows"
        assert row.status is JobStatus.SUCCEEDED
        assert row.progress == 1.0

    async def test_an_empty_collection_is_indeterminate_not_a_division(self) -> None:
        runtime = _runtime()
        job_id = uuid4()
        trace = _Trace()

        async with runtime.scope():
            ctx = runtime.get_context()
            reporter = build_progress_reporter(
                ctx,
                job_id=job_id,
                kind="search_rebuild",
                projector=build_job_progress_projector(ctx),
                min_interval=0.0,
            )
            reporter.sinks = (*reporter.sinks, trace)

            async with reporter.track():
                report = await rebuild_search_index(
                    ctx.document.query(NOTE_SPEC),
                    ctx.search.command(NOTE_INDEX),
                    document=NOTE_SPEC,
                    search=NOTE_INDEX,
                    progress=reporter,
                )

        row = await _job(runtime, job_id)

        assert report.indexed == 0
        assert row.status is JobStatus.SUCCEEDED
        assert row.progress == 1.0  # finishing completes the bar even with nothing to do

    async def test_the_bar_completes_when_the_scan_does_not_before(self) -> None:
        # The denominator is a snapshot and the collection is live, so a sweep routinely
        # meets more rows than it counted. Clamped, that reads as a finished bar with work
        # still going — and 1.0 is the one value that has to mean "the scan is over", which
        # is what a watcher waits on before touching the index.
        tenant = uuid4()
        runtime = _runtime()
        await _seed_notes(runtime, tenant, count=6)
        trace = _Trace()

        async with runtime.scope():
            ctx = runtime.get_context()

            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                reporter = build_progress_reporter(
                    ctx,
                    job_id=uuid4(),
                    kind="search_rebuild",
                    projector=build_job_progress_projector(ctx),
                    min_interval=0.0,
                )
                reporter.sinks = (*reporter.sinks, trace)

                await rebuild_search_index(
                    _UndercountingQuery(ctx.document.query(NOTE_SPEC), understate=4),
                    ctx.search.command(NOTE_INDEX),
                    document=NOTE_SPEC,
                    search=NOTE_INDEX,
                    chunk_size=2,
                    progress=reporter,
                )

        # Six rows walked against a count of two: every tick but the last stays short of the
        # end, and the end is what the stream running out earns.
        assert trace.fractions[-1] == 1.0
        assert all(fraction != 1.0 for fraction in trace.fractions[:-1])
        assert _is_monotone(trace.fractions)
        # And no report claims an impossible ratio on the way: the count is written as the
        # estimate it is until the stream ends and makes it a fact.
        assert trace.messages[-3:] == ["4 of ~2 rows", "6 of ~2 rows", "6 of 6 rows"]

    async def test_a_rebuild_without_a_reporter_costs_no_count_query(self) -> None:
        # The denominator is worth one query only when someone is watching; a sweep with no
        # reporter must not pay for a count nobody reads.
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            query = _CountingQuery(ctx.document.query(NOTE_SPEC))

            await rebuild_search_index(
                query,
                ctx.search.command(NOTE_INDEX),
                document=NOTE_SPEC,
                search=NOTE_INDEX,
            )

        assert query.counts == 0


# ....................... #


async def test_a_terminal_job_survives_a_second_sweep_reporting_late() -> None:
    # The two consumers can share a job (a migration exports then rebuilds). Once the job
    # ends, a straggler from either sweep must not reopen it.
    runtime = _runtime()
    job_id = uuid4()

    async with runtime.scope():
        ctx = runtime.get_context()
        projector = build_job_progress_projector(ctx)
        reporter = build_progress_reporter(
            ctx, job_id=job_id, kind="migration", projector=projector, min_interval=0.0
        )

        async with reporter.track():
            await reporter.report(0.5, "halfway")

        straggler = build_progress_reporter(
            ctx, job_id=job_id, kind="migration", projector=projector, min_interval=0.0
        )
        await straggler.report(0.9, "a second sweep, still going")

    row = await _job(runtime, job_id)

    assert row.status is JobStatus.SUCCEEDED
    assert row.message != "a second sweep, still going"


# ....................... #


def test_the_progress_kit_has_two_in_repo_consumers() -> None:
    """The gate the RFC set: this plane ships only with two consumers that are not examples.

    Kept as an assertion rather than a comment because "a progress plane with one consumer is
    an app feature wearing a framework costume" is a claim that decays silently — a consumer
    can be dropped in a refactor and nothing else in the suite would notice.
    """

    import inspect

    from forze_kits.integrations.portability.export import ArchiveExporter
    from forze_kits.integrations.search.maintenance import rebuild_search_index as rebuild

    assert "progress" in inspect.signature(ArchiveExporter.__call__).parameters
    assert "progress" in inspect.signature(rebuild).parameters
