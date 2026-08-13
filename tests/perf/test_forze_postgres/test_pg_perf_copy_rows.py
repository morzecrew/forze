"""``COPY`` against the paths it is meant to replace, measured rather than assumed.

The decision to move analytics ingest onto ``COPY`` is conditional on this file: the RFC's
rule is that the switch ships only if an interleaved A/B confirms the win, so a result that
failed to show one would stop the change rather than be explained away.

Interleaved on purpose. Running all of one strategy and then all of another attributes any
drift in the container — page cache warming, autovacuum, a noisy neighbour on the host — to
whichever strategy ran second. Alternating rounds spreads that drift across all three, which
is the same reason the repo's perf gate interleaves against its merge base.

Reported as the median of per-round minima: the minimum within a round is the cleanest sample
(least interference), and the median across rounds keeps one unlucky round from setting the
number.
"""

from __future__ import annotations

import statistics
import time
from typing import Any

import pytest

pytest.importorskip("psycopg")

from psycopg import sql

from forze_postgres.kernel.client.client import PostgresClient

# ----------------------- #

ROUNDS = 5
"""Alternating rounds per configuration. Each round times every strategy once."""

CONFIGURATIONS = [
    (10_000, 6),
    (10_000, 20),
    (100_000, 6),
    (100_000, 20),
]
"""``(rows, columns)`` — the RFC's grid: two scales × two widths."""


def _rows(count: int, columns: int) -> list[tuple[Any, ...]]:
    return [tuple(f"v{index}-{col}" for col in range(columns)) for index in range(count)]


async def _reset(client: PostgresClient, table: str, columns: int) -> None:
    cols = sql.SQL(", ").join(sql.Identifier(f"c{index}") + sql.SQL(" text") for index in range(columns))
    await client.execute(sql.SQL("DROP TABLE IF EXISTS {t}").format(t=sql.Identifier(table)))
    await client.execute(sql.SQL("CREATE TABLE {t} ({c})").format(t=sql.Identifier(table), c=cols))


async def _time_copy(client: PostgresClient, table: str, rows: list[tuple[Any, ...]], columns: int) -> float:
    names = [f"c{index}" for index in range(columns)]
    started = time.perf_counter()
    await client.copy_rows(("public", table), names, rows)

    return time.perf_counter() - started


async def _time_multi_values(
    client: PostgresClient,
    table: str,
    rows: list[tuple[Any, ...]],
    columns: int,
) -> float:
    """One multi-VALUES INSERT — the analytics adapter's current execution.

    Chunked at the bind-parameter ceiling rather than sent whole: the extended protocol tops
    out at 65 535 parameters, so the wide configurations physically cannot go in one
    statement. That ceiling is the reason the 10k row cap exists, and pretending it away
    would make this comparison flattering rather than informative.
    """

    names = [sql.Identifier(f"c{index}") for index in range(columns)]
    chunk = max(1, 65_535 // columns)
    started = time.perf_counter()

    for offset in range(0, len(rows), chunk):
        batch = rows[offset : offset + chunk]
        template = sql.SQL("(") + sql.SQL(", ").join(sql.Placeholder() * columns) + sql.SQL(")")
        statement = sql.SQL("INSERT INTO {t} ({c}) VALUES {v}").format(
            t=sql.Identifier(table),
            c=sql.SQL(", ").join(names),
            v=sql.SQL(", ").join([template] * len(batch)),
        )
        flat: list[Any] = [value for row in batch for value in row]
        await client.execute(statement, flat)

    return time.perf_counter() - started


async def _time_execute_many(
    client: PostgresClient,
    table: str,
    rows: list[tuple[Any, ...]],
    columns: int,
) -> float:
    names = [sql.Identifier(f"c{index}") for index in range(columns)]
    statement = sql.SQL("INSERT INTO {t} ({c}) VALUES ({v})").format(
        t=sql.Identifier(table),
        c=sql.SQL(", ").join(names),
        v=sql.SQL(", ").join(sql.Placeholder() * columns),
    )
    started = time.perf_counter()
    await client.execute_many(statement, [list(row) for row in rows])

    return time.perf_counter() - started


# ----------------------- #


@pytest.mark.perf
@pytest.mark.asyncio
@pytest.mark.parametrize(("row_count", "columns"), CONFIGURATIONS)
async def test_copy_beats_the_paths_it_replaces(
    pg_client: PostgresClient,
    row_count: int,
    columns: int,
) -> None:
    """``COPY`` vs multi-VALUES vs ``execute_many`` at one point of the grid."""

    rows = _rows(row_count, columns)
    samples: dict[str, list[float]] = {"copy": [], "multi_values": [], "execute_many": []}

    for _ in range(ROUNDS):
        for name, timer in (
            ("copy", _time_copy),
            ("multi_values", _time_multi_values),
            ("execute_many", _time_execute_many),
        ):
            table = f"perf_copy_{name}"
            await _reset(pg_client, table, columns)
            samples[name].append(await timer(pg_client, table, rows, columns))

    medians = {name: statistics.median(values) for name, values in samples.items()}
    copy_seconds = medians["copy"]

    print(  # noqa: T201 — the measurement is the deliverable; it goes in the RFC
        f"\n{row_count:>7} rows × {columns:>2} cols | "
        + " | ".join(
            f"{name}={medians[name] * 1000:8.1f}ms ({medians[name] / copy_seconds:5.2f}×)"
            for name in ("copy", "multi_values", "execute_many")
        ),
    )

    # The RFC's condition, as an assertion rather than a note: if COPY is not faster than
    # both paths it is meant to replace, the ingest switch does not ship.
    assert copy_seconds < medians["multi_values"]
    assert copy_seconds < medians["execute_many"]
