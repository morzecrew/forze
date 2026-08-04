"""Mock ≡ Postgres for the job-progress merge — the differential, not another mock proof.

The merge rules are exactly the kind of logic a code read clears wrongly: a compare-and-swap
loop, a lexicographic key over ``(terminal, at, seq, rank)``, a max over a nullable float, and
a partial update whose *unset* fields must stay untouched. Each of those is a place a real
adapter can legitimately behave differently from the in-memory one — a float that round-trips
through ``double precision``, a ``timestamptz`` compared at microsecond resolution, an update
DTO whose ``None`` means "clear" on one side and "leave alone" on the other — and every unit
test in the suite runs against the mock, so none of them could see it.

So this drives the *same* event battery through both projectors and compares the records field
for field. A rule that holds only in memory fails here.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from itertools import permutations
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext
from forze_kits.integrations.progress import (
    JobProgress,
    JobProgressProjector,
    JobRecord,
    JobStatus,
    build_job_progress_projector,
    build_progress_reporter,
    job_record_spec,
)
from forze_mock import MockDepsModule
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps, context_from_modules

pytestmark = pytest.mark.integration

_SPEC = job_record_spec()
_ROUTE = str(_SPEC.name)
_TENANT = UUID("11111111-1111-1111-1111-111111111111")
_T0 = datetime(2026, 8, 4, 12, 0, 0, tzinfo=UTC)

# tenant_id is the adapter-managed scoping column (no model field); the rest mirrors the
# job models exactly — this is the DDL the docs page hands to applications.
_JOBS_DDL = """
CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    tenant_id uuid NOT NULL,
    kind text NOT NULL,
    status text NOT NULL,
    progress double precision,
    message text,
    subject text,
    durable_run_id text,
    error text,
    heartbeat_at timestamptz NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    event_at timestamptz NOT NULL,
    event_seq integer NOT NULL
);
"""

_STALENESS_INDEX = """
CREATE INDEX jobs_staleness ON jobs (tenant_id, status, heartbeat_at);
"""


def _configurable() -> ConfigurablePostgresDocument:
    return ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", "jobs"),
            write=("public", "jobs"),
            bookkeeping_strategy="application",
            tenant_aware=True,
        )
    )


@pytest.fixture
def pg_ctx(pg_client: PostgresClient) -> ExecutionContext:
    return context_from_deps(
        Deps.merge(
            Deps.plain(
                {
                    PostgresClientDepKey: pg_client,
                    PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                }
            ),
            Deps.routed(
                {
                    DocumentQueryDepKey: {_ROUTE: _configurable()},
                    DocumentCommandDepKey: {_ROUTE: _configurable()},
                }
            ),
        )
    )


@pytest.fixture
def mock_ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule())


@pytest_asyncio.fixture(autouse=True)
async def _table(pg_client: PostgresClient):
    await pg_client.execute("DROP TABLE IF EXISTS jobs;")
    await pg_client.execute(_JOBS_DDL)
    await pg_client.execute(_STALENESS_INDEX)
    yield


# ....................... #


def _event(
    job_id: UUID,
    status: JobStatus,
    *,
    seconds: float,
    seq: int,
    progress: float | None = None,
    message: str | None = None,
    error: str | None = None,
    kind: str = "export",
) -> JobProgress:
    return JobProgress(
        job_id=job_id,
        kind=kind,
        status=status,
        at=_T0 + timedelta(seconds=seconds),
        seq=seq,
        progress=progress,
        message=message,
        error=error,
    )


def _observable(row: JobRecord) -> dict[str, Any]:
    return row.model_dump(exclude={"id", "rev", "created_at", "last_update_at"})


async def _project(projector: JobProgressProjector, events: list[JobProgress]) -> dict[str, Any]:
    for event in events:
        await projector.apply(event)

    row = await projector.query.find({"$values": {"id": events[0].job_id}})
    assert row is not None

    return _observable(row)


# A job's whole life, including the shapes ordering makes matter: a fraction that goes
# backwards, a pause, a resume, and a terminal report.
# The fractions are deliberately not representable in binary: a store that narrows the
# column (``real`` instead of ``double precision``) hands back a *different* number than the
# one the merge compared, and the two sides diverge here rather than in production.
_SCRIPT = [
    (JobStatus.RUNNING, 0.0, 1, None, "starting"),
    (JobStatus.RUNNING, 1.0, 2, 1 / 3, "a third"),
    (JobStatus.WAITING, 2.0, 3, 1 / 3, "waiting"),
    (JobStatus.RUNNING, 3.0, 4, 2 / 3, "resumed"),
    (JobStatus.SUCCEEDED, 4.0, 5, 1.0, "done"),
]


def _events(job_id: UUID, order: tuple[int, ...]) -> list[JobProgress]:
    return [
        _event(
            job_id,
            _SCRIPT[index][0],
            seconds=_SCRIPT[index][1],
            seq=_SCRIPT[index][2],
            progress=_SCRIPT[index][3],
            message=_SCRIPT[index][4],
        )
        for index in order
    ]


# ----------------------- #


@pytest.mark.asyncio
async def test_every_arrival_order_projects_identically_on_both(
    pg_ctx: ExecutionContext, mock_ctx: ExecutionContext
) -> None:
    with pg_ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
        pg = build_job_progress_projector(pg_ctx)
        mock = build_job_progress_projector(mock_ctx)
        outcomes: list[dict[str, Any]] = []

        for order in permutations(range(len(_SCRIPT))):
            job_id = uuid4()
            on_pg = await _project(pg, _events(job_id, order))
            on_mock = await _project(mock, _events(job_id, order))

            # The differential: the two stores agree on this ordering...
            assert on_pg == on_mock, f"mock and Postgres diverged on order {order}"
            outcomes.append(on_pg)

    # ...and every ordering agrees with every other, on the real adapter too.
    assert all(outcome == outcomes[0] for outcome in outcomes)
    assert outcomes[0]["status"] is JobStatus.SUCCEEDED
    assert outcomes[0]["progress"] == 1.0
    assert outcomes[0]["started_at"] == _T0
    assert outcomes[0]["finished_at"] == _T0 + timedelta(seconds=4)


@pytest.mark.asyncio
async def test_a_tick_after_the_end_changes_nothing_on_both(
    pg_ctx: ExecutionContext, mock_ctx: ExecutionContext
) -> None:
    with pg_ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
        for projector in (
            build_job_progress_projector(pg_ctx),
            build_job_progress_projector(mock_ctx),
        ):
            job_id = uuid4()

            await projector.apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=1, progress=0.5))
            await projector.apply(_event(job_id, JobStatus.FAILED, seconds=1, seq=2, error="boom"))
            before = _observable(
                await projector.query.find({"$values": {"id": job_id}})  # type: ignore[arg-type]
            )
            after = await _project(
                projector,
                [
                    _event(
                        job_id,
                        JobStatus.RUNNING,
                        seconds=9,
                        seq=99,
                        progress=0.9,
                        message="still going",
                    )
                ],
            )

            assert after == before


@pytest.mark.asyncio
async def test_clearing_a_message_clears_it_on_both(
    pg_ctx: ExecutionContext, mock_ctx: ExecutionContext
) -> None:
    # The merge sets `message` on every accepted transition, `None` included — a job that
    # finishes without one must not keep narrating whatever it last said. That rides on the
    # update payload applying by *set field* rather than by non-``None`` value, which is one
    # shared mixin today and two adapters that could drift tomorrow.
    with pg_ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
        for projector in (
            build_job_progress_projector(pg_ctx),
            build_job_progress_projector(mock_ctx),
        ):
            job_id = uuid4()

            await projector.apply(
                _event(job_id, JobStatus.RUNNING, seconds=0, seq=1, message="halfway through")
            )
            row = await projector.apply(
                _event(job_id, JobStatus.SUCCEEDED, seconds=1, seq=2, message=None)
            )

            assert row is not None
            assert row.message is None
            assert row.error is None


@pytest.mark.asyncio
async def test_a_fraction_survives_the_float_column(pg_ctx: ExecutionContext) -> None:
    # The max-merge compares what the store gave back, so a fraction that does not survive
    # the round-trip would make the bar stutter — accepted, re-read smaller, accepted again.
    with pg_ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
        projector = build_job_progress_projector(pg_ctx)
        job_id = uuid4()
        fraction = 1 / 3

        await projector.apply(
            _event(job_id, JobStatus.RUNNING, seconds=0, seq=1, progress=fraction)
        )
        row = await projector.apply(
            _event(job_id, JobStatus.RUNNING, seconds=1, seq=2, progress=fraction)
        )

        assert row is not None
        assert row.progress == fraction
        assert row.rev == 2  # the second event moved the heartbeat, not the fraction


@pytest.mark.asyncio
async def test_staleness_is_answered_by_the_index_on_postgres(
    pg_ctx: ExecutionContext,
) -> None:
    with pg_ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
        projector = build_job_progress_projector(pg_ctx)
        stuck, alive, done = (uuid4() for _ in range(3))

        await projector.apply(_event(stuck, JobStatus.RUNNING, seconds=0, seq=1))
        await projector.apply(_event(alive, JobStatus.RUNNING, seconds=500, seq=1))
        await projector.apply(_event(done, JobStatus.SUCCEEDED, seconds=0, seq=1))

        stalled = await projector.find_stalled(silent_since=_T0 + timedelta(seconds=60))

        assert [row.id for row in stalled] == [stuck]


@pytest.mark.asyncio
async def test_a_job_stays_in_its_own_tenant_while_the_work_moves_on(
    pg_ctx: ExecutionContext,
) -> None:
    # Only a real tenant-partitioned table can show this one. The work a job watches runs
    # under bindings of its own — a full-system export walks one bound section per tenant —
    # and every write here decides its partition from the ambient tenant at write time. The
    # reporter captures the identity that opened the job and restores it per emit; without
    # that, one job scatters a row into every partition its work touched.
    other = UUID("22222222-2222-2222-2222-222222222222")
    job_id = uuid4()

    with pg_ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
        reporter = build_progress_reporter(
            pg_ctx,
            job_id=job_id,
            kind="export",
            projector=build_job_progress_projector(pg_ctx),
            min_interval=0.0,
        )
        await reporter.start("walking the first tenant")

    with pg_ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=other)):
        await reporter.report(0.5, "walking the second")
        await reporter.finish("done")

        # Nothing of this job was filed under the tenant whose data it happened to be
        # reading when it reported.
        assert (
            await build_job_progress_projector(pg_ctx).query.count({"$values": {"id": job_id}}) == 0
        )

    with pg_ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
        row = await build_job_progress_projector(pg_ctx).query.find({"$values": {"id": job_id}})

    assert row is not None
    assert row.status is JobStatus.SUCCEEDED
    assert row.progress == 1.0
    assert row.message == "done"


@pytest.mark.asyncio
async def test_the_catch_all_bucket_asks_the_complement_on_postgres(
    pg_ctx: ExecutionContext,
) -> None:
    # The monitor's ``__other__`` bucket is a `$nin` on kind — a filter shape no other
    # progress query uses, so this is the only place it meets a real renderer. The buckets
    # must also *partition*: what the named kinds counted plus what the complement counted
    # is the whole, or the fleet-wide sum quietly under- or double-counts.
    with pg_ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
        projector = build_job_progress_projector(pg_ctx)
        exports, other = uuid4(), uuid4()
        cutoff = _T0 + timedelta(seconds=60)

        await projector.apply(_event(exports, JobStatus.RUNNING, seconds=0, seq=1))
        await projector.apply(_event(other, JobStatus.RUNNING, seconds=0, seq=1, kind="reencrypt"))

        named = await projector.count_stalled(silent_since=cutoff, kind="export")
        rest = await projector.count_stalled(silent_since=cutoff, exclude_kinds=("export",))
        quietest = await projector.find_stalled(
            silent_since=cutoff, exclude_kinds=("export",), limit=1
        )

        assert (named, rest) == (1, 1)
        assert [row.id for row in quietest] == [other]
        assert named + rest == await projector.count_stalled(silent_since=cutoff)
