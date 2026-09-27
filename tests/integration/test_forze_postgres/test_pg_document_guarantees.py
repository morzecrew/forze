"""Integration: a declared storage guarantee against a real Postgres.

Two halves, and both need the database.

**Validation** — reconciliation at wiring said Postgres *can* keep a uniqueness guarantee; only
the catalog can say whether this deployment's migration did. The refusal has to name the DDL,
because an operator reading "guarantee not satisfied" with no statement to run is an operator
who has to read the framework's source.

**Parity** — the in-memory store is the canonical superset, which is exactly the declaration most
easily wrong in the optimistic direction: a mock stricter than the backend makes a green
simulation meaningless. So the violating write is made against *both* stores and the two
refusals are compared, rather than asserted separately in files that never meet.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import NonOverlapping, UniqueTogether
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.lifecycle import PostgresDocumentSchemaValidationHook
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.catalog.validation.validate_schema import PostgresDocumentSchemaSpec
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps, context_from_modules

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

ONE_CURRENT = UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}})
ONE_EVER = UniqueTogether(fields=("root_id",))
NO_OVERLAP = NonOverlapping(key=("root_id",), period=("valid_from", "valid_to"), bounds="[]")
NO_OVERLAP_WHILE_CURRENT = NonOverlapping(
    key=("root_id",),
    period=("valid_from", "valid_to"),
    bounds="[]",
    where={"$values": {"is_current": True}},
)


class _Read(BaseModel):
    id: UUID
    root_id: str
    is_current: bool
    valid_from: date = date(2026, 1, 1)
    valid_to: date | None = None


class _Domain(Document):
    root_id: str
    is_current: bool = True
    valid_from: date = date(2026, 1, 1)
    valid_to: date | None = None


class _Create(CreateDocumentCmd):
    root_id: str
    is_current: bool = True
    valid_from: date = date(2026, 1, 1)
    valid_to: date | None = None


async def _table(pg_client: PostgresClient) -> str:
    name = f"guarantee_{uuid4().hex[:12]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {name} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            root_id text NOT NULL,
            is_current boolean NOT NULL,
            valid_from date,
            valid_to date
        );
        """
    )

    return name


async def _validate(
    pg_client: PostgresClient,
    table: str,
    *guarantees: UniqueTogether | NonOverlapping,
) -> None:
    intro = PostgresIntrospector(client=pg_client)
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))
    hook = PostgresDocumentSchemaValidationHook(
        specs=(
            PostgresDocumentSchemaSpec(
                name="fact",
                read_model=_Read,
                read_relation=("public", table),
                write_domain_model=_Domain,
                write_create_model=_Create,
                write_relation=("public", table),
                bookkeeping_strategy="application",
                guarantees=guarantees,
            ),
        ),
    )

    await hook(ctx)


# ....................... #


async def _insert_period(
    pg_client: PostgresClient,
    table: str,
    *,
    start: str,
    end: str | None,
) -> None:
    """One row under key ``r1`` holding the given period."""

    await pg_client.execute(
        f"INSERT INTO {table} (id, rev, created_at, last_update_at, root_id, is_current,"
        " valid_from, valid_to) VALUES (%s, 1, now(), now(), 'r1', true, %s, %s);",
        [uuid4(), start, end],
    )


async def _exclude(
    pg_client: PostgresClient,
    table: str,
    *,
    bounds: str,
    where: str | None = None,
) -> None:
    """Add the constraint a non-overlap guarantee over (valid_from, valid_to) asks for."""

    restriction = f" WHERE ({where})" if where is not None else ""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;")
    await pg_client.execute(
        f"ALTER TABLE {table} ADD EXCLUDE USING gist "
        f"(root_id WITH =, daterange(valid_from, valid_to, '{bounds}') WITH &&){restriction};"
    )


# ....................... #


class TestStartupValidation:
    async def test_a_missing_partial_index_refuses_and_names_the_ddl(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)

        with pytest.raises(CoreException) as caught:
            await _validate(pg_client, table, ONE_CURRENT)

        message = caught.value.summary

        assert "CREATE UNIQUE INDEX" in message
        assert table in message
        assert "root_id" in message
        # The doctrine, in the message a deployment actually reads.
        assert "The migration is what satisfies a guarantee" in message

    async def test_a_present_partial_index_passes(self, pg_client: PostgresClient) -> None:
        table = await _table(pg_client)
        await pg_client.execute(f"CREATE UNIQUE INDEX ON {table} (root_id) WHERE is_current;")

        await _validate(pg_client, table, ONE_CURRENT)

    async def test_a_plain_index_does_not_satisfy_a_filtered_guarantee(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The axis the capability declaration splits, proved against the catalog: a plain
        # unique index is the wrong mechanism for "one *current* row per fact", and a check
        # that accepted it would pass a deployment that refuses every second history row.
        table = await _table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD UNIQUE (root_id);")

        with pytest.raises(CoreException, match="no valid partial unique index"):
            await _validate(pg_client, table, ONE_CURRENT)

    async def test_a_plain_index_satisfies_an_unfiltered_guarantee(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD UNIQUE (root_id);")

        await _validate(pg_client, table, ONE_EVER)

    async def test_declaring_nothing_validates_nothing(self, pg_client: PostgresClient) -> None:
        # The inert case: a table with no unique index at all is fine for a spec that asks
        # for nothing, which is every spec that has not opted in.
        await _validate(pg_client, await _table(pg_client))

    async def test_a_missing_exclusion_constraint_refuses_and_names_the_ddl(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)

        with pytest.raises(CoreException) as caught:
            await _validate(pg_client, table, NO_OVERLAP)

        message = caught.value.summary

        assert "EXCLUDE USING gist" in message
        assert "daterange(valid_from, valid_to, '[]')" in message
        assert table in message
        assert "The migration is what satisfies a guarantee" in message

    async def test_a_present_exclusion_constraint_passes(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)
        await _exclude(pg_client, table, bounds="[]")

        await _validate(pg_client, table, NO_OVERLAP)

    async def test_a_constraint_with_other_bounds_does_not_count(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The check this pass exists for. `[)` and `[]` agree about every day but one, and the
        # day they disagree about is the boundary — so a constraint built on the other
        # convention leaves exactly the case a reader would trust the guarantee for.
        table = await _table(pg_client)
        await _exclude(pg_client, table, bounds="[)")

        with pytest.raises(CoreException) as caught:
            await _validate(pg_client, table, NO_OVERLAP)

        assert "bounds" in caught.value.summary

    async def test_a_two_argument_range_reads_as_the_default_convention(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # `daterange(a, b)` is `[)` by Postgres's own definition, so a migration that omits the
        # literal is a correct migration for a `[)` guarantee and must not be refused for
        # spelling it the shorter way.
        table = await _table(pg_client)
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;")
        await pg_client.execute(
            f"ALTER TABLE {table} ADD EXCLUDE USING gist "
            f"(root_id WITH =, daterange(valid_from, valid_to) WITH &&);"
        )

        await _validate(
            pg_client,
            table,
            NonOverlapping(key=("root_id",), period=("valid_from", "valid_to"), bounds="[)"),
        )

    async def test_a_constraint_over_other_period_columns_does_not_count(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The detection branch: the constraint carries the right key and the right convention
        # over the *wrong* dates, so the periods the guarantee is about are unconstrained while
        # the catalog shows an exclusion constraint that looks like the mechanism.
        table = await _table(pg_client)
        await pg_client.execute(
            f"ALTER TABLE {table} ADD COLUMN other_from date, ADD COLUMN other_to date;"
        )
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;")
        await pg_client.execute(
            f"ALTER TABLE {table} ADD EXCLUDE USING gist "
            f"(root_id WITH =, daterange(other_from, other_to, '[]') WITH &&);"
        )

        with pytest.raises(CoreException, match="no EXCLUDE constraint"):
            await _validate(pg_client, table, NO_OVERLAP)

    async def test_an_inverted_range_does_not_count(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The constraint names both declared fields, in the wrong order. An inverted range is
        # empty, `&&` never matches an empty range, and so the constraint excludes nothing —
        # while the catalog shows an exclusion constraint over exactly the right columns.
        table = await _table(pg_client)
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;")
        await pg_client.execute(
            f"ALTER TABLE {table} ADD EXCLUDE USING gist "
            f"(root_id WITH =, daterange(valid_to, valid_from, '[]') WITH &&);"
        )

        with pytest.raises(CoreException, match="no EXCLUDE constraint"):
            await _validate(pg_client, table, NO_OVERLAP)

    async def test_the_inverted_constraint_refuses_every_ordinary_row(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The reason the order check is not pedantry, and it is worse than it looked: an
        # inverted range is not merely empty, it cannot be constructed. Every row whose period
        # runs forwards fails on insert, so the constraint does not weaken the guarantee — it
        # breaks the relation, at the first write rather than at startup.
        table = await _table(pg_client)
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;")
        await pg_client.execute(
            f"ALTER TABLE {table} ADD EXCLUDE USING gist "
            f"(root_id WITH =, daterange(valid_to, valid_from, '[]') WITH &&);"
        )

        with pytest.raises(CoreException):
            await _insert_period(pg_client, table, start="2026-01-01", end="2026-06-30")

    async def test_a_filtered_guarantee_wants_a_partial_constraint(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The bitemporal shape against the real mechanism: the constraint covers only the rows
        # in force, so a corrected version may keep its predecessor's period.
        table = await _table(pg_client)
        await _exclude(pg_client, table, bounds="[]", where="is_current")

        await _validate(pg_client, table, NO_OVERLAP_WHILE_CURRENT)

    async def test_a_full_constraint_does_not_satisfy_a_filtered_guarantee(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # It covers *more* rows than the declaration, which is not the same property: it
        # refuses the correction the filter exists to permit.
        table = await _table(pg_client)
        await _exclude(pg_client, table, bounds="[]")

        with pytest.raises(CoreException, match="no EXCLUDE constraint"):
            await _validate(pg_client, table, NO_OVERLAP_WHILE_CURRENT)

    async def test_a_partial_constraint_does_not_satisfy_a_full_guarantee(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The mirror, and the one that leaves rows unconstrained: a guarantee over every row is
        # not kept by a constraint covering some of them.
        table = await _table(pg_client)
        await _exclude(pg_client, table, bounds="[]", where="is_current")

        with pytest.raises(CoreException, match="no EXCLUDE constraint"):
            await _validate(pg_client, table, NO_OVERLAP)

    async def test_a_predicate_over_another_column_does_not_count_either(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The failure the column check exists for, on this member too: the constraint restricts
        # the right columns to the wrong rows, so the rows the guarantee covers are free.
        table = await _table(pg_client)
        await pg_client.execute(
            f"ALTER TABLE {table} ADD COLUMN is_verified boolean NOT NULL DEFAULT false;"
        )
        await _exclude(pg_client, table, bounds="[]", where="is_verified")

        with pytest.raises(CoreException, match="no EXCLUDE constraint"):
            await _validate(pg_client, table, NO_OVERLAP_WHILE_CURRENT)

    async def test_the_filtered_refusal_names_the_where_clause(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)

        with pytest.raises(CoreException) as caught:
            await _validate(pg_client, table, NO_OVERLAP_WHILE_CURRENT)

        assert "WHERE (<a condition over is_current>)" in caught.value.summary

    async def test_extra_key_columns_do_not_count(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # An extra scalar key column *weakens* the constraint: two rows must now match on that
        # column too before they conflict, so a pair the declaration refuses is accepted.
        table = await _table(pg_client)
        await pg_client.execute(
            f"ALTER TABLE {table} ADD COLUMN tenant_id text NOT NULL DEFAULT 't';"
        )
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;")
        await pg_client.execute(
            f"ALTER TABLE {table} ADD EXCLUDE USING gist "
            f"(tenant_id WITH =, root_id WITH =, daterange(valid_from, valid_to, '[]') WITH &&);"
        )

        with pytest.raises(CoreException, match="no EXCLUDE constraint"):
            await _validate(pg_client, table, NO_OVERLAP)

    async def test_a_range_element_that_does_not_overlap_does_not_count(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The columns, the order and the bounds are all right and the operator is not: `=` on
        # the range refuses two *identical* periods and takes every other overlapping pair,
        # which is a different property wearing this one's columns.
        table = await _table(pg_client)
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;")
        await pg_client.execute(
            f"ALTER TABLE {table} ADD EXCLUDE USING gist "
            f"(root_id WITH =, daterange(valid_from, valid_to, '[]') WITH =);"
        )

        with pytest.raises(CoreException, match="no EXCLUDE constraint"):
            await _validate(pg_client, table, NO_OVERLAP)

    async def test_a_key_element_compared_with_another_operator_does_not_count(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # `<>` on the key groups the opposite rows: it conflicts where the declaration says two
        # rows are unrelated, and lets through every pair the declaration is about.
        table = await _table(pg_client)
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;")
        await pg_client.execute(
            f"ALTER TABLE {table} ADD EXCLUDE USING gist "
            f"(root_id WITH <>, daterange(valid_from, valid_to, '[]') WITH &&);"
        )

        with pytest.raises(CoreException, match="no EXCLUDE constraint"):
            await _validate(pg_client, table, NO_OVERLAP)

    async def test_a_constraint_over_another_key_does_not_count(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD COLUMN other text NOT NULL DEFAULT '';")
        await pg_client.execute(
            f"ALTER TABLE {table} ADD EXCLUDE USING gist "
            f"(other WITH =, daterange(valid_from, valid_to, '[]') WITH &&);"
        )

        with pytest.raises(CoreException, match="no EXCLUDE constraint"):
            await _validate(pg_client, table, NO_OVERLAP)

    async def test_a_partial_index_on_other_columns_does_not_count(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)
        await pg_client.execute(f"CREATE UNIQUE INDEX ON {table} (id) WHERE is_current;")

        with pytest.raises(CoreException, match="no valid partial unique index"):
            await _validate(pg_client, table, ONE_CURRENT)

    async def test_a_predicate_over_another_column_does_not_count(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The failure the column check exists for: the index restricts the right columns to the
        # wrong rows. Two rows with the same root_id, both current and both unverified, sit
        # outside this index entirely — so Postgres takes them and the guarantee is a comment.
        table = await _table(pg_client)
        await pg_client.execute(
            f"ALTER TABLE {table} ADD COLUMN is_verified boolean NOT NULL DEFAULT false;"
        )
        await pg_client.execute(f"CREATE UNIQUE INDEX ON {table} (root_id) WHERE is_verified;")

        with pytest.raises(CoreException, match="no valid partial unique index"):
            await _validate(pg_client, table, ONE_CURRENT)

    async def test_a_reversed_compound_index_counts(self, pg_client: PostgresClient) -> None:
        # `UniqueTogether.fields` says order is not significant to the property, and it is not
        # significant to the index either: (a, b) and (b, a) refuse exactly the same pairs. A
        # check that required the declared order would fail a correct migration at startup.
        table = await _table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD UNIQUE (is_current, root_id);")

        await _validate(pg_client, table, UniqueTogether(fields=("root_id", "is_current")))

    async def test_an_include_payload_does_not_disqualify_an_index(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # INCLUDE columns are stored in the index and take no part in uniqueness, so this
        # enforces the declared property exactly. Reading them as key columns would reject it.
        table = await _table(pg_client)
        await pg_client.execute(
            f"CREATE UNIQUE INDEX ON {table} (root_id) INCLUDE (last_update_at);"
        )

        await _validate(pg_client, table, ONE_EVER)

    async def test_a_partial_index_does_not_satisfy_an_unfiltered_guarantee(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The mirror of the filtered case, and the one that leaves rows unconstrained rather
        # than over-constrained: a guarantee covering every row is not kept by an index that
        # covers some of them.
        table = await _table(pg_client)
        await pg_client.execute(f"CREATE UNIQUE INDEX ON {table} (root_id) WHERE is_current;")

        with pytest.raises(CoreException, match="no valid unique index"):
            await _validate(pg_client, table, ONE_EVER)

    async def test_an_invalid_index_does_not_count(self, pg_client: PostgresClient) -> None:
        # The state a failed migration leaves. A CREATE UNIQUE INDEX CONCURRENTLY over existing
        # duplicates fails and leaves an invalid index row behind; it enforces nothing on new
        # writes, so reading it as a satisfied guarantee is the worst reading of the catalog.
        table = await _table(pg_client)

        await pg_client.execute(f"CREATE UNIQUE INDEX {table}_bad ON {table} (root_id);")
        # Marked invalid directly rather than through a failed CONCURRENTLY build, which cannot
        # run inside the test's transaction. The state is what matters: this is the row a
        # failed concurrent build leaves, and it enforces nothing on new writes.
        await pg_client.execute(
            "UPDATE pg_index SET indisvalid = false WHERE indexrelid = %s::regclass;",
            [f"{table}_bad"],
        )

        invalid = await pg_client.fetch_all(
            "SELECT indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid"
            " WHERE c.relname = %s;",
            [f"{table}_bad"],
            row_factory="dict",
        )

        assert invalid and invalid[0]["indisvalid"] is False, "the index was not marked invalid"

        with pytest.raises(CoreException, match="no valid unique index"):
            await _validate(pg_client, table, ONE_EVER)


# ....................... #


class _NullableRead(BaseModel):
    id: UUID
    root_id: str | None
    is_current: bool
    valid_from: date = date(2026, 1, 1)
    valid_to: date | None = None


class _NullableDomain(Document):
    root_id: str | None = None
    is_current: bool = True
    valid_from: date = date(2026, 1, 1)
    valid_to: date | None = None


class _NullableReadDoc(ReadDocument):
    root_id: str | None = None
    is_current: bool = True
    valid_from: date = date(2026, 1, 1)
    valid_to: date | None = None


class _NullableCreate(CreateDocumentCmd):
    root_id: str | None = None
    is_current: bool = True
    valid_from: date = date(2026, 1, 1)
    valid_to: date | None = None


class _NullableUpdate(BaseDTO):
    is_current: bool | None = None


def _nullable_spec(
    *guarantees: UniqueTogether | NonOverlapping,
) -> DocumentSpec[Any, _NullableDomain, _NullableCreate, _NullableUpdate]:
    return DocumentSpec[Any, _NullableDomain, _NullableCreate, _NullableUpdate](
        name="fact",
        read=_NullableReadDoc,
        write=DocumentWriteTypes(
            domain=_NullableDomain, create_cmd=_NullableCreate, update_cmd=_NullableUpdate
        ),
        guarantees=guarantees,
    )


async def _validate_nullable(
    pg_client: PostgresClient,
    table: str,
    *guarantees: UniqueTogether | NonOverlapping,
) -> None:
    """:func:`_validate` over a model whose guaranteed column may hold a null.

    A separate model because the relation validator that runs first refuses a required field
    over a nullable column — correctly, and unrelated to what these legs are about.
    """

    intro = PostgresIntrospector(client=pg_client)
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))
    hook = PostgresDocumentSchemaValidationHook(
        specs=(
            PostgresDocumentSchemaSpec(
                name="fact",
                read_model=_NullableRead,
                read_relation=("public", table),
                write_domain_model=_NullableDomain,
                write_create_model=_NullableCreate,
                write_relation=("public", table),
                bookkeeping_strategy="application",
                guarantees=guarantees,
            ),
        ),
    )

    await hook(ctx)


async def _nullable_table(pg_client: PostgresClient) -> str:
    name = f"guarantee_null_{uuid4().hex[:12]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {name} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            root_id text,
            is_current boolean NOT NULL,
            valid_from date NOT NULL DEFAULT '2026-01-01',
            valid_to date
        );
        """
    )

    return name


class TestNullsAreValuesUnlessExempted:
    """`skip_null=False` counts a null as a value; Postgres does not, unless told to.

    The divergence is silent in both directions if unchecked: an ordinary unique index passes
    validation and then admits two rows sharing a tuple containing a null, which the in-memory
    store refuses. A parity break that only appears for nullable data is the kind a test suite
    over non-null fixtures never sees.
    """

    async def test_an_ordinary_index_over_a_nullable_column_is_refused(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _nullable_table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD UNIQUE (root_id);")

        with pytest.raises(CoreException, match="NULLS NOT DISTINCT"):
            await _validate_nullable(pg_client, table, ONE_EVER)

    async def test_nulls_not_distinct_satisfies_it(self, pg_client: PostgresClient) -> None:
        table = await _nullable_table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD UNIQUE NULLS NOT DISTINCT (root_id);")

        await _validate_nullable(pg_client, table, ONE_EVER)

    async def test_a_not_null_column_needs_no_such_index(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The column cannot hold a null, so the two readings cannot differ and demanding the
        # stricter index would refuse a correct migration.
        table = await _table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD UNIQUE (root_id);")

        await _validate(pg_client, table, ONE_EVER)

    async def test_skip_null_asks_for_a_partial_index_instead(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # With the exemption declared, the mechanism is a predicate that drops the null rows —
        # so the predicate has to mention the guarantee's own field, not just any field.
        table = await _nullable_table(pg_client)
        await pg_client.execute(
            f"CREATE UNIQUE INDEX ON {table} (root_id) WHERE root_id IS NOT NULL;"
        )

        await _validate_nullable(
            pg_client, table, UniqueTogether(fields=("root_id",), skip_null=True)
        )

    async def test_skip_null_is_not_satisfied_by_a_plain_index(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _nullable_table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD UNIQUE (root_id);")

        with pytest.raises(CoreException, match="no valid partial unique index"):
            await _validate_nullable(
                pg_client, table, UniqueTogether(fields=("root_id",), skip_null=True)
            )


# ....................... #


class _FactRead(ReadDocument):
    root_id: str
    is_current: bool = True
    valid_from: date = date(2026, 1, 1)
    valid_to: date | None = None


class _FactUpdate(BaseDTO):
    is_current: bool | None = None


def _spec(
    *guarantees: UniqueTogether | NonOverlapping,
) -> DocumentSpec[_FactRead, _Domain, _Create, _FactUpdate]:
    return DocumentSpec[_FactRead, _Domain, _Create, _FactUpdate](
        name="fact",
        read=_FactRead,
        write=DocumentWriteTypes(domain=_Domain, create_cmd=_Create, update_cmd=_FactUpdate),
        guarantees=guarantees or (ONE_CURRENT,),
    )


class TestMockAndPostgresRefuseTheSameWay:
    async def test_both_stores_raise_conflict_for_the_same_violation(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)
        await pg_client.execute(f"CREATE UNIQUE INDEX ON {table} (root_id) WHERE is_current;")

        # Postgres: the index refuses the second current row.
        await pg_client.execute(
            f"INSERT INTO {table} (id, rev, created_at, last_update_at, root_id, is_current)"
            " VALUES (%s, 1, now(), now(), 'r1', true);",
            [uuid4()],
        )

        with pytest.raises(CoreException) as from_postgres:
            await pg_client.execute(
                f"INSERT INTO {table} (id, rev, created_at, last_update_at, root_id, is_current)"
                " VALUES (%s, 1, now(), now(), 'r1', true);",
                [uuid4()],
            )

        # The in-memory store: the declared guarantee refuses it.
        command = context_from_modules(MockDepsModule()).doc.command(_spec())
        await command.create(_Create(root_id="r1"))

        with pytest.raises(CoreException) as from_mock:
            await command.create(_Create(root_id="r1"))

        # The comparison is the point: same kind, so a caller cannot tell the stores apart by
        # how they refuse, and a simulation that handles one handles the other.
        assert from_mock.value.kind is from_postgres.value.kind
        assert from_mock.value.kind.value == "conflict"

    async def test_neither_store_refuses_a_row_outside_the_filter(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The other direction, and the one that catches an over-strict mock: a non-current
        # duplicate is legal in both stores.
        table = await _table(pg_client)
        await pg_client.execute(f"CREATE UNIQUE INDEX ON {table} (root_id) WHERE is_current;")
        await pg_client.execute(
            f"INSERT INTO {table} (id, rev, created_at, last_update_at, root_id, is_current)"
            " VALUES (%s, 1, now(), now(), 'r1', true);",
            [uuid4()],
        )
        await pg_client.execute(
            f"INSERT INTO {table} (id, rev, created_at, last_update_at, root_id, is_current)"
            " VALUES (%s, 1, now(), now(), 'r1', false);",
            [uuid4()],
        )

        command = context_from_modules(MockDepsModule()).doc.command(_spec())
        await command.create(_Create(root_id="r1"))
        row = await command.create(_Create(root_id="r1", is_current=False))

        assert row.is_current is False

    async def test_both_stores_refuse_an_overlap_the_same_way(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The parity leg for the other member, and the one that found the divergence: an
        # exclusion violation used to reach the caller as `precondition` while the in-memory
        # store raised `conflict`, so the same violated declaration answered with two kinds.
        table = await _table(pg_client)
        await _exclude(pg_client, table, bounds="[]")
        await _insert_period(pg_client, table, start="2026-01-01", end="2026-03-31")

        with pytest.raises(CoreException) as from_postgres:
            await _insert_period(pg_client, table, start="2026-03-01", end="2026-05-01")

        command = context_from_modules(MockDepsModule()).doc.command(_spec(NO_OVERLAP))
        await command.create(
            _Create(root_id="r1", valid_from=date(2026, 1, 1), valid_to=date(2026, 3, 31))
        )

        with pytest.raises(CoreException) as from_mock:
            await command.create(
                _Create(root_id="r1", valid_from=date(2026, 3, 1), valid_to=date(2026, 5, 1))
            )

        assert from_mock.value.kind is from_postgres.value.kind
        assert from_mock.value.kind.value == "conflict"

    async def test_neither_store_conflicts_on_a_null_key(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # An exclusion constraint compares key parts with `=`, and `NULL = NULL` is unknown
        # rather than true — so two rows with a null key and overlapping periods both land. The
        # in-memory store used to refuse them, which is a mock stricter than the backend.
        table = await _nullable_table(pg_client)
        await _exclude(pg_client, table, bounds="[]")

        for start, end in (("2026-01-01", "2026-06-30"), ("2026-03-01", "2026-09-30")):
            await pg_client.execute(
                f"INSERT INTO {table} (id, rev, created_at, last_update_at, root_id,"
                " is_current, valid_from, valid_to)"
                " VALUES (%s, 1, now(), now(), NULL, true, %s, %s);",
                [uuid4(), start, end],
            )

        command = context_from_modules(MockDepsModule()).doc.command(_nullable_spec(NO_OVERLAP))
        await command.create(
            _NullableCreate(valid_from=date(2026, 1, 1), valid_to=date(2026, 6, 30))
        )
        row = await command.create(
            _NullableCreate(valid_from=date(2026, 3, 1), valid_to=date(2026, 9, 30))
        )

        rows = await pg_client.fetch_all(
            f"SELECT count(*) AS n FROM {table};", [], row_factory="dict", commit=False
        )

        assert rows[0]["n"] == 2
        assert row.root_id is None

    async def test_both_stores_agree_on_the_touching_boundary(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The day the two bounds conventions disagree about, asked of both stores under `[]`.
        # A mock reading the shared endpoint as excluded would accept a row Postgres refuses,
        # and every simulation over a contract ledger would be answering a different question.
        table = await _table(pg_client)
        await _exclude(pg_client, table, bounds="[]")
        await _insert_period(pg_client, table, start="2026-01-01", end="2026-04-01")

        with pytest.raises(CoreException) as from_postgres:
            await _insert_period(pg_client, table, start="2026-04-01", end="2026-06-01")

        command = context_from_modules(MockDepsModule()).doc.command(_spec(NO_OVERLAP))
        await command.create(
            _Create(root_id="r1", valid_from=date(2026, 1, 1), valid_to=date(2026, 4, 1))
        )

        with pytest.raises(CoreException) as from_mock:
            await command.create(
                _Create(root_id="r1", valid_from=date(2026, 4, 1), valid_to=date(2026, 6, 1))
            )

        assert from_mock.value.kind is from_postgres.value.kind

    async def test_neither_store_refuses_a_period_that_ended_first(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The over-strict direction: consecutive non-touching periods are legal in both.
        table = await _table(pg_client)
        await _exclude(pg_client, table, bounds="[]")
        await _insert_period(pg_client, table, start="2026-06-01", end=None)
        await _insert_period(pg_client, table, start="2025-01-01", end="2025-12-31")

        command = context_from_modules(MockDepsModule()).doc.command(_spec(NO_OVERLAP))
        await command.create(_Create(root_id="r1", valid_from=date(2026, 6, 1)))
        row = await command.create(
            _Create(root_id="r1", valid_from=date(2025, 1, 1), valid_to=date(2025, 12, 31))
        )

        assert row.valid_to == date(2025, 12, 31)


# ....................... #

ONE_EVER_AT_COMMIT = UniqueTogether(fields=("root_id",), holds="commit")
NO_OVERLAP_AT_COMMIT = NonOverlapping(
    key=("root_id",), period=("valid_from", "valid_to"), bounds="[]", holds="commit"
)


class TestWhenTheGuaranteeHolds:
    """A constraint's deferral has to agree with the declaration's ``holds``.

    Disagreement either way is the mock and the database answering differently: a deferred
    constraint behind a guarantee that holds after every write accepts writes the in-memory store
    refuses, and an immediate one behind a guarantee that holds at commit refuses the transaction
    the declaration lets through. Keyed on ``condeferred``, not ``condeferrable``: ``DEFERRABLE
    INITIALLY IMMEDIATE`` checks per statement unless a transaction asks otherwise.
    """

    async def test_a_deferred_constraint_keeps_a_commit_guarantee(
        self, pg_client: PostgresClient
    ) -> None:
        table = await _table(pg_client)
        await pg_client.execute(
            f"ALTER TABLE {table} ADD UNIQUE (root_id) DEFERRABLE INITIALLY DEFERRED;"
        )

        await _validate(pg_client, table, ONE_EVER_AT_COMMIT)

    async def test_a_deferred_constraint_does_not_keep_an_immediate_guarantee(
        self, pg_client: PostgresClient
    ) -> None:
        # The bug the moment was found through: accepted today, and the mock refused a write
        # the database took.
        table = await _table(pg_client)
        await pg_client.execute(
            f"ALTER TABLE {table} ADD UNIQUE (root_id) DEFERRABLE INITIALLY DEFERRED;"
        )

        with pytest.raises(CoreException, match="INITIALLY DEFERRED") as caught:
            await _validate(pg_client, table, ONE_EVER)

        assert 'holds="commit"' in caught.value.summary
        assert "has no valid unique index" not in caught.value.summary

    @pytest.mark.parametrize(
        "constraint",
        ["UNIQUE (root_id)", "UNIQUE (root_id) DEFERRABLE INITIALLY IMMEDIATE"],
        ids=["not-deferrable", "initially-immediate"],
    )
    async def test_an_immediate_constraint_keeps_only_an_immediate_guarantee(
        self, pg_client: PostgresClient, constraint: str
    ) -> None:
        table = await _table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD {constraint};")

        await _validate(pg_client, table, ONE_EVER)

        with pytest.raises(CoreException, match="checked per statement") as caught:
            await _validate(pg_client, table, ONE_EVER_AT_COMMIT)

        assert "ADD UNIQUE (root_id) DEFERRABLE INITIALLY DEFERRED;" in caught.value.summary

    async def test_a_missing_constraint_prints_the_deferred_ddl(
        self, pg_client: PostgresClient
    ) -> None:
        table = await _table(pg_client)

        with pytest.raises(CoreException, match="no valid unique index") as caught:
            await _validate(pg_client, table, ONE_EVER_AT_COMMIT)

        assert "ADD UNIQUE (root_id) DEFERRABLE INITIALLY DEFERRED;" in caught.value.summary

    async def test_a_partial_uniqueness_at_commit_has_no_mechanism(
        self, pg_client: PostgresClient
    ) -> None:
        # Reached only when startup runs without the port being resolved: wiring refuses it
        # first. A partial index is right in every other respect and still cannot be deferred.
        table = await _table(pg_client)
        await pg_client.execute(f"CREATE UNIQUE INDEX ON {table} (root_id) WHERE is_current;")
        at_commit = UniqueTogether(
            fields=("root_id",), where={"$values": {"is_current": True}}, holds="commit"
        )

        with pytest.raises(CoreException, match="has no mechanism for that") as caught:
            await _validate(pg_client, table, at_commit)

        assert "None" not in caught.value.summary

    async def test_a_deferred_foreign_key_elsewhere_says_nothing_about_the_index(
        self, pg_client: PostgresClient
    ) -> None:
        # `conindid` is set on a foreign key referencing the index too; joined carelessly, the
        # referencing table's deferral would be read as the referenced index's.
        table = await _table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD UNIQUE (root_id);")
        referencing = f"{table}_ref"
        await pg_client.execute(
            f"CREATE TABLE {referencing} (root_id text REFERENCES {table} (root_id) "
            "DEFERRABLE INITIALLY DEFERRED);"
        )

        await _validate(pg_client, table, ONE_EVER)

        with pytest.raises(CoreException, match="checked per statement"):
            await _validate(pg_client, table, ONE_EVER_AT_COMMIT)

    async def test_a_deferred_partial_exclusion_keeps_a_filtered_commit_guarantee(
        self, pg_client: PostgresClient
    ) -> None:
        # Unlike a uniqueness, an EXCLUDE constraint can be partial and deferred at once — and
        # the deparsed text then ends in the deferral clause, after the predicate.
        table = await _table(pg_client)
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;")
        await pg_client.execute(
            f"ALTER TABLE {table} ADD EXCLUDE USING gist "
            "(root_id WITH =, daterange(valid_from, valid_to, '[]') WITH &&) "
            "WHERE (is_current) DEFERRABLE INITIALLY DEFERRED;"
        )
        filtered = NonOverlapping(
            key=("root_id",),
            period=("valid_from", "valid_to"),
            bounds="[]",
            where={"$values": {"is_current": True}},
            holds="commit",
        )

        await _validate(pg_client, table, filtered)

    async def test_the_deferral_clause_is_not_read_as_part_of_the_predicate(
        self, pg_client: PostgresClient
    ) -> None:
        # A deferred partial constraint deparses as `... WHERE (...) DEFERRABLE INITIALLY
        # DEFERRED`. Read as predicate, the clause "mentions" a quoted column named
        # `DEFERRABLE`, and a constraint filtered on another column would pass for one on it.
        table = await _table(pg_client)
        await pg_client.execute(f'ALTER TABLE {table} ADD COLUMN "DEFERRABLE" boolean;')
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;")
        await pg_client.execute(
            f"ALTER TABLE {table} ADD EXCLUDE USING gist "
            "(root_id WITH =, daterange(valid_from, valid_to, '[]') WITH &&) "
            "WHERE (is_current) DEFERRABLE INITIALLY DEFERRED;"
        )
        on_the_other_column = NonOverlapping(
            key=("root_id",),
            period=("valid_from", "valid_to"),
            bounds="[]",
            where={"$values": {"DEFERRABLE": True}},
            holds="commit",
        )

        with pytest.raises(CoreException, match="no EXCLUDE constraint"):
            await _validate(pg_client, table, on_the_other_column)

    async def test_an_exclusion_constraint_must_agree_on_deferral_too(
        self, pg_client: PostgresClient
    ) -> None:
        table = await _table(pg_client)
        await _exclude(pg_client, table, bounds="[]")

        await _validate(pg_client, table, NO_OVERLAP)

        with pytest.raises(CoreException, match="checked per statement") as caught:
            await _validate(pg_client, table, NO_OVERLAP_AT_COMMIT)

        assert "WITH &&) DEFERRABLE INITIALLY DEFERRED;" in caught.value.summary

        deferred = await _table(pg_client)
        await pg_client.execute(
            f"ALTER TABLE {deferred} ADD EXCLUDE USING gist "
            "(root_id WITH =, daterange(valid_from, valid_to, '[]') WITH &&) "
            "DEFERRABLE INITIALLY DEFERRED;"
        )

        await _validate(pg_client, deferred, NO_OVERLAP_AT_COMMIT)

        with pytest.raises(CoreException, match="INITIALLY DEFERRED"):
            await _validate(pg_client, deferred, NO_OVERLAP)


# ....................... #


class _ItemRead(ReadDocument):
    order_id: str
    position: int


class _Item(Document):
    order_id: str
    position: int


class _ItemCreate(CreateDocumentCmd):
    order_id: str
    position: int


class _ItemUpdate(BaseDTO):
    position: int | None = None


POSITIONS_AT_COMMIT = UniqueTogether(fields=("order_id", "position"), holds="commit")


class TestBothStoresLetATransactionPassThroughAViolation:
    """The case that asked for the moment: an item inserted at the top of a positioned list.

    Written while the row it displaces still holds the position, and legal once the displaced
    rows have moved down — Postgres settles a deferred constraint at commit, and the in-memory
    store has to settle a commit guarantee the same way, or the application's ordinary
    reconciliation fails in the one place it is tested.
    """

    @staticmethod
    async def _postgres_list(pg_client: PostgresClient) -> tuple[str, UUID, UUID]:
        table = f"items_{uuid4().hex[:12]}"
        await pg_client.execute(
            f"CREATE TABLE {table} (id uuid PRIMARY KEY, order_id text NOT NULL, "
            "position integer NOT NULL, UNIQUE (order_id, position) "
            "DEFERRABLE INITIALLY DEFERRED);"
        )
        first, second = uuid4(), uuid4()
        await pg_client.execute(
            f"INSERT INTO {table} VALUES (%s, 'o1', 0), (%s, 'o1', 1);", [first, second]
        )

        return table, first, second

    @staticmethod
    def _mock_spec() -> DocumentSpec[Any, Any, Any, Any]:
        return DocumentSpec[_ItemRead, _Item, _ItemCreate, _ItemUpdate](
            name="items",
            read=_ItemRead,
            write=DocumentWriteTypes(
                domain=_Item, create_cmd=_ItemCreate, update_cmd=_ItemUpdate
            ),
            guarantees=(POSITIONS_AT_COMMIT,),
        )

    async def test_both_commit_an_insert_at_the_top(self, pg_client: PostgresClient) -> None:
        table, first, second = await self._postgres_list(pg_client)

        async with pg_client.transaction():
            await pg_client.execute(f"INSERT INTO {table} VALUES (%s, 'o1', 0);", [uuid4()])
            await pg_client.execute(f"UPDATE {table} SET position = 1 WHERE id = %s;", [first])
            await pg_client.execute(f"UPDATE {table} SET position = 2 WHERE id = %s;", [second])

        spec = self._mock_spec()
        ctx = context_from_modules(MockDepsModule())
        command = ctx.doc.command(spec)
        a = await command.create(_ItemCreate(order_id="o1", position=0))
        b = await command.create(_ItemCreate(order_id="o1", position=1))

        async with ctx.tx_ctx.scope("mock"):
            await command.create(_ItemCreate(order_id="o1", position=0))
            await command.update(a.id, a.rev, _ItemUpdate(position=1))
            await command.update(b.id, b.rev, _ItemUpdate(position=2))

        rows = await pg_client.fetch_all(
            f"SELECT position FROM {table} ORDER BY position;", [], row_factory="dict", commit=False
        )
        page = await ctx.doc.query(spec).find_many({"$values": {"order_id": "o1"}})

        assert [row["position"] for row in rows] == [0, 1, 2]
        assert sorted(row.position for row in page.hits) == [0, 1, 2]

    async def test_both_refuse_the_same_way_when_the_violation_is_left_at_commit(
        self, pg_client: PostgresClient
    ) -> None:
        table, first, _ = await self._postgres_list(pg_client)

        with pytest.raises(CoreException) as from_postgres:
            async with pg_client.transaction():
                await pg_client.execute(f"INSERT INTO {table} VALUES (%s, 'o1', 0);", [uuid4()])
                await pg_client.execute(
                    f"UPDATE {table} SET position = 1 WHERE id = %s;", [first]
                )

        spec = self._mock_spec()
        ctx = context_from_modules(MockDepsModule())
        command = ctx.doc.command(spec)
        a = await command.create(_ItemCreate(order_id="o1", position=0))
        await command.create(_ItemCreate(order_id="o1", position=1))

        with pytest.raises(CoreException) as from_mock:
            async with ctx.tx_ctx.scope("mock"):
                await command.create(_ItemCreate(order_id="o1", position=0))
                await command.update(a.id, a.rev, _ItemUpdate(position=1))

        assert from_mock.value.kind is from_postgres.value.kind
        assert from_mock.value.kind.value == "conflict"
