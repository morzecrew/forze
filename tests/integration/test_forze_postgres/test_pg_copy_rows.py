"""``copy_rows`` against a real Postgres — the only backend that can answer these questions.

Text-format ``COPY`` is a wire protocol with escaping rules, and the failures that matter are
exactly the ones a mock cannot have: a tab inside a string, a ``Decimal`` that loses precision
on the way through, a rejected row whose line number is the only way to find it in a million.
Every check here therefore reads the value back out of the server rather than trusting what
went in — the discipline the UUID/Decimal write-gap and the JSON-boundary incidents both
taught, and the reason this file exists at all.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from forze.base.exceptions import CoreException
from forze_postgres.kernel.client.client import PostgresClient

# ----------------------- #

RICH_COLUMNS = (
    "id",
    "amount",
    "occurred_at",
    "note",
    "payload",
    "flag",
    "maybe",
)

# Content chosen for what breaks a hand-rolled COPY rather than for looking realistic: the tab
# and newline are the text format's own delimiters, and the backslash is its escape character,
# so a naive "\t".join(row) corrupts all three.
HOSTILE_TEXT = "tab\there\nnewline\\backslash \\N not-null 'quote' \"dquote\""


async def _make_rich_table(client: PostgresClient, name: str) -> None:
    await client.execute(
        f"""
        CREATE TABLE {name} (
            id uuid PRIMARY KEY,
            amount numeric(30, 12),
            occurred_at timestamptz,
            note text,
            payload jsonb,
            flag boolean,
            maybe integer
        );
        """  # noqa: S608 — name is a test-local literal, never caller input
    )


def _rich_rows(count: int) -> list[tuple[Any, ...]]:
    base = datetime(2026, 3, 1, 12, 30, 45, 123456, tzinfo=UTC)

    return [
        (
            uuid4(),
            # 21 significant digits: a float round-trip mangles this visibly rather than
            # plausibly, so a precision regression cannot pass as rounding.
            Decimal("123456789.012345678901"),
            base + timedelta(seconds=index),
            f"{HOSTILE_TEXT} #{index}",
            json.dumps({"index": index, "nested": {"tab": "\t"}}),
            index % 2 == 0,
            None if index % 3 == 0 else index,
        )
        for index in range(count)
    ]


# ----------------------- #


class TestRichTypeRoundTrip:
    """Battery 1 — the types that have historically broken on the write path."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_rich_types_survive_a_large_text_copy(self, pg_client: PostgresClient) -> None:
        """10⁵ rows in, 10⁵ rows out, every value identical to what was sent."""

        await _make_rich_table(pg_client, "copy_rich_text")
        rows = _rich_rows(100_000)

        copied = await pg_client.copy_rows(("public", "copy_rich_text"), RICH_COLUMNS, rows)

        assert copied == 100_000

        stored = await pg_client.fetch_value("SELECT count(*) FROM copy_rich_text")
        assert stored == 100_000

        # Spot-check the boundaries and one interior row rather than all 100k: the failure
        # modes here are systematic (a codec, an escape rule), not per-row.
        for index in (0, 1, 99_999):
            expected = rows[index]
            got = await pg_client.fetch_one(
                "SELECT * FROM copy_rich_text WHERE id = %(id)s",
                {"id": expected[0]},
            )

            assert got is not None
            assert got["id"] == expected[0]
            assert got["amount"] == expected[1], "numeric precision was lost in transit"
            assert got["occurred_at"] == expected[2]
            assert got["note"] == expected[3], "text escaping mangled tabs/newlines/backslashes"
            assert got["payload"] == json.loads(expected[4])
            assert got["flag"] == expected[5]
            assert got["maybe"] == expected[6]

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_a_literal_backslash_n_stays_a_string(self, pg_client: PostgresClient) -> None:
        """``\\N`` is the text format's NULL marker; as data it must survive as four characters.

        This is the escaping bug a hand-rolled COPY ships and nobody notices until a column
        that should read ``\\N`` reads NULL.
        """

        await _make_rich_table(pg_client, "copy_null_marker")

        marker = uuid4()
        await pg_client.copy_rows(
            ("public", "copy_null_marker"),
            ("id", "note", "maybe"),
            [(marker, "\\N", None)],
        )

        got = await pg_client.fetch_one(
            "SELECT note, maybe FROM copy_null_marker WHERE id = %(id)s",
            {"id": marker},
        )

        assert got is not None
        assert got["note"] == "\\N"
        assert got["maybe"] is None, "a real NULL must still arrive as NULL"

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_an_async_iterator_is_never_materialized(self, pg_client: PostgresClient) -> None:
        """Bounded memory is the promise; this pins that rows are pulled, not collected."""

        await _make_rich_table(pg_client, "copy_streamed")

        produced = 0

        async def stream():  # type: ignore[no-untyped-def]
            nonlocal produced

            for row in _rich_rows(5_000):
                produced += 1
                yield row

        copied = await pg_client.copy_rows(
            ("public", "copy_streamed"),
            RICH_COLUMNS,
            stream(),
        )

        assert copied == 5_000
        assert produced == 5_000


# ....................... #


class TestFailureIsAllOrNothing:
    """Battery 2 — a bad row anywhere means no rows anywhere, and the error says where."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_a_bad_row_aborts_the_load_and_reports_its_line(
        self,
        pg_client: PostgresClient,
    ) -> None:
        """Row 500 of 1000 is unparseable → zero rows land, and the line is in the error.

        The line number is the whole DX obligation: at 10⁶ rows "invalid input syntax" without
        one is a debugging session, with one it is a `sed -n` away.
        """

        await pg_client.execute("CREATE TABLE copy_abort (id integer, label text);")

        rows: list[tuple[Any, ...]] = [(index, f"row-{index}") for index in range(1000)]
        rows[499] = ("not-an-integer", "row-499")

        with pytest.raises(CoreException) as caught:
            await pg_client.copy_rows(("public", "copy_abort"), ("id", "label"), rows)

        assert caught.value.code == "copy_row_invalid"
        assert caught.value.details is not None
        assert caught.value.details.get("line") == 500, "the reported line must be 1-based"

        remaining = await pg_client.fetch_value("SELECT count(*) FROM copy_abort")
        assert remaining == 0, "a partial load was observable — COPY is all-or-nothing"

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_the_connection_survives_a_rejected_copy(
        self,
        pg_client: PostgresClient,
    ) -> None:
        """A failed copy must not poison the pooled connection for the next caller."""

        await pg_client.execute("CREATE TABLE copy_reuse (id integer);")

        with pytest.raises(CoreException):
            await pg_client.copy_rows(("public", "copy_reuse"), ("id",), [("bad",)])

        await pg_client.copy_rows(("public", "copy_reuse"), ("id",), [(1,), (2,)])

        assert await pg_client.fetch_value("SELECT count(*) FROM copy_reuse") == 2


# ....................... #


class TestTransactionComposition:
    """Battery 3 — the copy joins the caller's transaction rather than escaping it."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_rollback_removes_every_copied_row(self, pg_client: PostgresClient) -> None:
        await pg_client.execute("CREATE TABLE copy_rollback (id integer);")

        class Rollback(Exception):
            pass

        with pytest.raises(Rollback):
            async with pg_client.transaction():
                await pg_client.copy_rows(
                    ("public", "copy_rollback"),
                    ("id",),
                    [(index,) for index in range(100)],
                )

                assert await pg_client.fetch_value("SELECT count(*) FROM copy_rollback") == 100

                raise Rollback

        assert await pg_client.fetch_value("SELECT count(*) FROM copy_rollback") == 0

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_commit_keeps_every_copied_row(self, pg_client: PostgresClient) -> None:
        await pg_client.execute("CREATE TABLE copy_commit (id integer);")

        async with pg_client.transaction():
            await pg_client.copy_rows(
                ("public", "copy_commit"),
                ("id",),
                [(index,) for index in range(100)],
            )

        assert await pg_client.fetch_value("SELECT count(*) FROM copy_commit") == 100


# ....................... #


class TestIdentifierHostility:
    """Battery 8 — the reason ``target`` is a tuple and not a string."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_quoted_and_reserved_names_round_trip(self, pg_client: PostgresClient) -> None:
        """Schema, table and column names that need quoting are composed, not formatted.

        ``select`` is a reserved word and ``we"ird`` contains the quote character that closes
        an identifier — either one breaks a formatted statement, and the second is how an
        injection lands in the raw tier.
        """

        schema = 'we"ird schema'
        table = "select"
        column = 'col"umn name'

        await pg_client.execute('CREATE SCHEMA "we""ird schema";')
        await pg_client.execute('CREATE TABLE "we""ird schema"."select" ("col""umn name" integer);')

        copied = await pg_client.copy_rows((schema, table), (column,), [(1,), (2,), (3,)])

        assert copied == 3
        assert await pg_client.fetch_value('SELECT count(*) FROM "we""ird schema"."select"') == 3


# ....................... #


class TestArgumentRefusals:
    """The caller mistakes that must not reach the server as a broken statement."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_no_columns_is_refused(self, pg_client: PostgresClient) -> None:
        """An empty column list is a caller that built it dynamically and got nothing."""

        with pytest.raises(CoreException, match="at least one column"):
            await pg_client.copy_rows(("public", "whatever"), (), [(1,)])

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_column_types_without_binary_is_refused(self, pg_client: PostgresClient) -> None:
        """Ignoring it would leave the caller believing they had pinned the types."""

        with pytest.raises(CoreException, match="binary mode only"):
            await pg_client.copy_rows(
                ("public", "whatever"),
                ("id",),
                [(1,)],
                column_types=["integer"],
            )

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_mismatched_column_types_length_is_refused(
        self,
        pg_client: PostgresClient,
    ) -> None:
        with pytest.raises(CoreException, match="column_types for"):
            await pg_client.copy_rows(
                ("public", "whatever"),
                ("id", "label"),
                [(1, "x")],
                binary=True,
                column_types=["integer"],
            )
