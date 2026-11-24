"""Owned table shapes, their render and their diff over a recorded catalog (RFC 0080 §6).

Each test of the diff builds the catalog a database in step with the declaration would report,
then changes one thing and checks the one row of RFC 0080 §5.3 that change selects.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

import attrs
import pytest

from forze.base.exceptions import CoreException
from forze_postgres import PostgresClient, PostgresDepsModule, owned_relations
from forze_postgres.adapters.durable.schema import (
    durable_run_shape,
    durable_schedule_shape,
    durable_step_shape,
)
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
)
from forze_postgres.kernel.catalog.introspect import (
    IndexShapeInfo,
    PostgresRelationKind,
    PostgresType,
)
from forze_postgres.kernel.catalog.introspect.utils import normalize_pg_type
from forze_postgres.kernel.catalog.owned import (
    BoundOwnedRelation,
    CatalogRelation,
    OwnedColumn,
    OwnedIndex,
    OwnedRelationShape,
    RelationDelta,
    Retired,
    comment,
    diff_outcome,
    diff_relation,
    is_constant_default,
    quote_ident,
    render_diff,
    render_sql,
)
from forze_postgres.kernel.catalog.owned.matching import declared_key, normalize_expression
from tests.support.sql_statements import split_sql

pytestmark = pytest.mark.unit

# ----------------------- #

_CATALOG_PREDICATES = {
    "status = 'pending' AND attempts = 0": "((status = 'pending'::text) AND (attempts = 0))",
    "enabled": "enabled",
}
"""Each declared predicate as Postgres deparses it, from ``pg_get_expr`` on Postgres 18."""


def _run(
    *, tenant_aware: bool = True, schema: str = "bi", name: str = "durable_run"
) -> BoundOwnedRelation:
    config = PostgresDurableRunConfig(relation=(schema, name), tenant_aware=tenant_aware)

    return BoundOwnedRelation(
        route="durable_run", schema=schema, name=name, shape=durable_run_shape(config)
    )


def _found(
    keys: Iterable[str],
    *,
    name: str,
    unique: bool = False,
    predicate: str | None = None,
    valid: bool = True,
    backs_constraint: bool = False,
    included: int = 0,
    method: str = "btree",
    primary: bool = False,
    default_opclasses: bool = True,
    default_collations: bool = True,
    immediate: bool = True,
) -> IndexShapeInfo:
    """An index as the catalog reports it: each key deparsed bare, its order in flags."""

    bare: list[str] = []
    descending: list[bool] = []

    for key in keys:
        column, _, order = key.rpartition(" ") if key.upper().endswith(" DESC") else (key, "", "")
        bare.append(column)
        descending.append(order.upper() == "DESC")

    return IndexShapeInfo(
        name=name,
        schema="bi",
        keys=tuple(bare),
        descending=tuple(descending),
        nulls_first=tuple(descending),
        included=included,
        method=method,
        unique=unique,
        primary=primary,
        nulls_not_distinct=False,
        predicate=predicate,
        valid=valid,
        backs_constraint=backs_constraint,
        default_opclasses=default_opclasses,
        default_collations=default_collations,
        immediate=immediate,
    )


def _in_sync(relation: BoundOwnedRelation) -> CatalogRelation:
    """The catalog of a table created from the declaration."""

    shape = relation.shape
    indexes = [
        _found(shape.primary_key, name="pkey", unique=True, primary=True, backs_constraint=True)
    ]
    indexes.extend(
        _found(
            index.keys,
            name=f"idx_{position}",
            unique=index.unique,
            predicate=_CATALOG_PREDICATES.get(index.where or ""),
        )
        for position, index in enumerate(shape.indexes)
    )

    return CatalogRelation(
        kind="table",
        columns={
            column.name: PostgresType(
                base=normalize_pg_type(column.type), is_array=False, not_null=not column.nullable
            )
            for column in shape.columns
        },
        primary_key=shape.primary_key,
        indexes=tuple(indexes),
    )


def _with(
    catalog: CatalogRelation, *, drop: Iterable[str] = (), add: Iterable[IndexShapeInfo] = ()
) -> CatalogRelation:
    """*catalog* with the indexes named in *drop* removed and *add* appended."""

    dropped = set(drop)

    return attrs.evolve(
        catalog,
        indexes=(*(i for i in catalog.indexes if i.name not in dropped), *add),
    )


def _index_named(relation: BoundOwnedRelation, keys: tuple[str, ...]) -> str:
    position = next(i for i, index in enumerate(relation.shape.indexes) if index.keys == keys)
    return f"idx_{position}"


def _statements(delta: RelationDelta) -> list[str]:
    return [block.statement for block in delta.statements if block.statement is not None]


def _text(*deltas: RelationDelta) -> str:
    return render_diff(list(deltas), version="9.9.9")


# ....................... #


class TestDeclarations:
    @pytest.mark.parametrize("tenant_aware", [True, False])
    def test_each_declaration_renders(self, tenant_aware: bool) -> None:
        deps = PostgresDepsModule(
            client=PostgresClient(),
            durable_run=PostgresDurableRunConfig(
                relation=("app", "runs"), tenant_aware=tenant_aware
            ),
            durable_step=PostgresDurableStepConfig(
                relation=("app", "steps"), tenant_aware=tenant_aware
            ),
            durable_schedule=PostgresDurableScheduleConfig(
                relation=("app", "schedules"), tenant_aware=tenant_aware
            ),
        )

        text = render_sql(owned_relations(deps).bound, version="9.9.9")

        assert text.count("CREATE TABLE") == 3
        assert "-- forze 9.9.9 · app.runs (PostgresDurableRunStore)" in text
        assert ("(tenant_id, status, name, created_at)" in text) is tenant_aware
        assert ("(tenant_id, next_fire_at)" in text) is tenant_aware
        assert "CREATE INDEX ON app.runs (status, name, created_at);" in text

    def test_no_declared_default_is_volatile(self) -> None:
        shapes = [
            durable_run_shape(PostgresDurableRunConfig(relation=("a", "b"))),
            durable_step_shape(PostgresDurableStepConfig(relation=("a", "b"))),
            durable_schedule_shape(PostgresDurableScheduleConfig(relation=("a", "b"))),
        ]

        defaults = [c.default for s in shapes for c in s.columns if c.default is not None]

        assert defaults
        assert all(is_constant_default(default) for default in defaults)

    @pytest.mark.parametrize(
        "default", ["now()", "gen_random_uuid()", "clock_timestamp()", "1 + nextval('s')"]
    )
    def test_a_volatile_default_is_refused_at_construction(self, default: str) -> None:
        with pytest.raises(CoreException) as caught:
            OwnedRelationShape(
                owner="X",
                columns=(
                    OwnedColumn(name="a", type="timestamptz", nullable=False, default=default),
                ),
                primary_key=("a",),
                indexes=(),
            )

        assert caught.value.code == "postgres_owned_shape_invalid"

    @pytest.mark.parametrize("default", ["0", "true", "'pending'", "'{}'::jsonb", "-1.5", "NULL"])
    def test_a_literal_default_is_constant(self, default: str) -> None:
        assert is_constant_default(default)

    @pytest.mark.parametrize(
        ("columns", "primary_key"),
        [
            ((OwnedColumn(name="a", type="text"), OwnedColumn(name="a", type="text")), ("a",)),
            ((OwnedColumn(name="a", type="text"),), ("b",)),
            ((OwnedColumn(name="a", type="text"),), ()),
        ],
        ids=["column-twice", "key-on-undeclared-column", "no-key"],
    )
    def test_an_inconsistent_shape_is_refused_at_construction(
        self, columns: tuple[OwnedColumn, ...], primary_key: tuple[str, ...]
    ) -> None:
        with pytest.raises(CoreException) as caught:
            OwnedRelationShape(owner="X", columns=columns, primary_key=primary_key, indexes=())

        assert caught.value.code == "postgres_owned_shape_invalid"

    def test_no_relation_renders_nothing(self) -> None:
        assert render_sql([], version="9.9.9") == ""


class TestOwnedRelations:
    def test_the_tenant_switch_adds_the_tenant_leading_variants_and_keeps_the_plain_ones(
        self,
    ) -> None:
        plain = {index.keys for index in _run(tenant_aware=False).shape.indexes}
        tagged = {index.keys for index in _run(tenant_aware=True).shape.indexes}

        assert plain < tagged
        assert tagged - plain == {
            ("tenant_id", "status", "name", "created_at"),
            ("tenant_id", "created_at DESC", "run_id DESC"),
        }

    def test_retired_shapes_are_declared_whatever_the_config(self) -> None:
        assert _run(tenant_aware=False).shape.retired == _run(tenant_aware=True).shape.retired

    def test_a_tenant_scoped_relation_is_skipped_and_reported(self) -> None:
        deps = PostgresDepsModule(
            client=PostgresClient(),
            durable_run=PostgresDurableRunConfig(relation=lambda tenant: ("t", "runs")),
            durable_step=PostgresDurableStepConfig(relation=("app", "steps")),
        )

        relations = owned_relations(deps)

        assert relations.skipped == ("durable_run",)
        assert [(r.route, r.schema, r.name) for r in relations.bound] == [
            ("durable_step", "app", "steps")
        ]

    def test_a_module_without_owned_tables_binds_nothing(self) -> None:
        relations = owned_relations(PostgresDepsModule(client=PostgresClient()))

        assert relations.bound == () and relations.skipped == ()


class TestMatching:
    @pytest.mark.parametrize(("declared", "catalog"), list(_CATALOG_PREDICATES.items()))
    def test_a_declared_predicate_equals_its_catalog_form(
        self, declared: str, catalog: str
    ) -> None:
        assert normalize_expression(declared) == normalize_expression(catalog)

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            ("a = 1 OR b = 2 AND c = 3", "(a = 1 OR b = 2) AND c = 3"),
            ("NOT (a AND b)", "NOT a AND b"),
            ("status = 'pending'", "status = 'done'"),
            ("status = 'pending'", "\"Status\" = 'pending'"),
            # A cast on a column or an expression changes the value: varchar(1) truncates.
            ("(status)::character varying(1)", "status"),
            ("(idempotency_key)::character varying(10)", "idempotency_key"),
            ("((status)::character varying(1) = 'x'::text)", "status = 'x'"),
        ],
    )
    def test_spellings_that_differ_in_meaning_stay_apart(self, left: str, right: str) -> None:
        assert normalize_expression(left) != normalize_expression(right)

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            ("a = 1 AND (b = 2 AND c = 3)", "((a = 1) AND (b = 2) AND (c = 3))"),
            ("x = 'a'::character varying", "x='a'"),
            ("ts < '2020-01-01'::timestamp with time zone", "ts < '2020-01-01'"),
            ("x = 'a'::character varying(10)", "x = 'a'"),
            ("tags && '{a}'::text[]", "tags && '{a}'"),
            ("x = 'a'::public.citext", "x = 'a'"),
            ('"status" = 1', "STATUS = 1"),
        ],
    )
    def test_spellings_that_differ_only_in_form_are_equal(self, left: str, right: str) -> None:
        assert normalize_expression(left) == normalize_expression(right)

    @pytest.mark.parametrize(
        ("key", "descending", "nulls_first"),
        [
            ("created_at", False, False),
            ("created_at DESC", True, True),
            ("created_at DESC NULLS LAST", True, False),
            ("created_at NULLS FIRST", False, True),
            ("created_at ASC", False, False),
        ],
    )
    def test_a_declared_key_reads_its_direction_and_nulls_order_as_postgres_does(
        self, key: str, descending: bool, nulls_first: bool
    ) -> None:
        assert declared_key(key) == (normalize_expression("created_at"), descending, nulls_first)

    def test_unbalanced_parentheses_are_compared_as_written(self) -> None:
        assert normalize_expression("(a = 1") != normalize_expression("a = 1")


class TestQuoting:
    @pytest.mark.parametrize(
        ("name", "quoted"),
        [
            ("durable_run", "durable_run"),
            ("name", "name"),
            ("user", '"user"'),
            ("Runs", '"Runs"'),
            ("a$b", '"a$b"'),
            ("1x", '"1x"'),
            ('a"b', '"a""b"'),
            ("x;\nDROP TABLE t; --", '"x;\nDROP TABLE t; --"'),
        ],
    )
    def test_quote_ident_follows_postgres(self, name: str, quoted: str) -> None:
        assert quote_ident(name) == quoted

    def test_a_comment_stays_one_line_whatever_it_carries(self) -> None:
        line = comment("a\r\nDROP TABLE t; b\x85c")

        assert "\n" not in line and "\r" not in line
        assert line == "-- a  DROP TABLE t; b c"


class TestHostileNames:
    _HOSTILE = 'x";\nDROP TABLE victims; --\r'

    def test_a_hostile_relation_name_stays_inside_its_identifier(self) -> None:
        relation = _run(schema="Bad\nSchema", name=self._HOSTILE)

        statements = split_sql(render_sql([relation], version="9.9.9"))

        assert len(statements) == 1 + len(relation.shape.indexes)
        assert all(
            s.startswith(("CREATE TABLE", "CREATE UNIQUE INDEX", "CREATE INDEX"))
            for s in statements
        )

    def test_hostile_catalog_names_reach_the_diff_only_quoted(self) -> None:
        relation = _run()
        catalog = _in_sync(relation)
        hostile_type = PostgresType(base=self._HOSTILE, is_array=False, not_null=True)
        catalog = attrs.evolve(
            catalog,
            columns={**catalog.columns, "status": hostile_type},
            indexes=(
                *catalog.indexes,
                _found(("status", "created_at"), name=self._HOSTILE),
                _found(("tenant_id", "status", "created_at"), name="Hostile\n", unique=True),
            ),
        )

        text = _text(diff_relation(relation, catalog))
        statements = split_sql(text)

        assert statements == [
            f'DROP INDEX CONCURRENTLY bi."{self._HOSTILE.replace(chr(34), chr(34) * 2)}"'
        ]
        assert "DROP TABLE" not in re.sub(r'"(?:""|[^"])*"', "", text.replace("\n--", "\n"))


class TestDiff:
    def test_a_table_in_step_with_the_declaration_prints_nothing(self) -> None:
        relation = _run()
        delta = diff_relation(relation, _in_sync(relation))

        assert delta.blocks == ()
        assert diff_outcome([delta]) == "in_sync"
        assert _text(delta) == ""

    def test_a_missing_table_is_created_with_every_declared_index(self) -> None:
        relation = _run()
        statements = _statements(diff_relation(relation, None))

        assert statements[0].startswith("CREATE TABLE bi.durable_run (")
        assert len(statements) == 1 + len(relation.shape.indexes)
        assert all("CONCURRENTLY" not in s for s in statements)

    def test_a_primary_key_on_other_columns_is_refused(self) -> None:
        relation = _run()
        catalog = attrs.evolve(_in_sync(relation), primary_key=("run_id", "name"))

        delta = diff_relation(relation, catalog)

        assert diff_outcome([delta]) == "refusal"
        assert "ON CONFLICT (run_id)" in _text(delta)

    def test_a_missing_nullable_column_is_added(self) -> None:
        relation = _run()
        catalog = _in_sync(relation)
        columns = {k: v for k, v in catalog.columns.items() if k != "coalesce_key"}

        delta = diff_relation(relation, attrs.evolve(catalog, columns=columns))

        assert _statements(delta) == ["ALTER TABLE bi.durable_run ADD COLUMN coalesce_key text;"]

    def test_a_missing_not_null_column_with_a_literal_default_is_added(self) -> None:
        relation = _run()
        catalog = _in_sync(relation)
        columns = {k: v for k, v in catalog.columns.items() if k != "attempts"}

        delta = diff_relation(relation, attrs.evolve(catalog, columns=columns))

        assert _statements(delta) == [
            "ALTER TABLE bi.durable_run ADD COLUMN attempts integer NOT NULL DEFAULT 0;"
        ]

    def test_a_missing_not_null_column_without_a_default_is_refused(self) -> None:
        relation = _run()
        catalog = _in_sync(relation)
        columns = {k: v for k, v in catalog.columns.items() if k != "created_at"}

        delta = diff_relation(relation, attrs.evolve(catalog, columns=columns))

        assert _statements(delta) == []
        assert diff_outcome([delta]) == "refusal"
        assert "-- ALTER TABLE bi.durable_run ADD COLUMN created_at timestamptz NOT NULL;" in _text(
            delta
        )

    def test_a_column_of_another_type_family_is_refused_and_never_altered(self) -> None:
        relation = _run()
        catalog = _in_sync(relation)
        columns = {
            **catalog.columns,
            "input": PostgresType(base="text", is_array=False, not_null=False),
        }

        delta = diff_relation(relation, attrs.evolve(catalog, columns=columns))

        assert diff_outcome([delta]) == "refusal"
        assert "ALTER" not in _text(delta)

    @pytest.mark.parametrize(
        ("column", "base", "formatted"),
        [
            ("status", "citext", "citext"),
            ("status", "varchar", "character varying"),
            ("attempts", "int8", "bigint"),
        ],
    )
    def test_a_column_of_the_same_family_is_the_applications_call(
        self, column: str, base: str, formatted: str
    ) -> None:
        relation = _run()
        catalog = _in_sync(relation)
        columns = {
            **catalog.columns,
            column: PostgresType(base=base, is_array=False, not_null=True, formatted=formatted),
        }

        assert diff_relation(relation, attrs.evolve(catalog, columns=columns)).blocks == ()

    @pytest.mark.parametrize(
        ("column", "base", "formatted"),
        [
            ("attempts", "int2", "smallint"),
            ("run_id", "varchar", "character varying(36)"),
            ("status", "character(8)", "character(8)"),
        ],
    )
    def test_a_narrower_column_of_the_same_family_is_refused(
        self, column: str, base: str, formatted: str
    ) -> None:
        # A value forze writes may not fit: smallint overflows, varchar(36) cuts a scoped key.
        relation = _run()
        catalog = _in_sync(relation)
        columns = {
            **catalog.columns,
            column: PostgresType(base=base, is_array=False, not_null=True, formatted=formatted),
        }

        delta = diff_relation(relation, attrs.evolve(catalog, columns=columns))

        assert diff_outcome([delta]) == "refusal"
        assert formatted in _text(delta)

    def test_a_declared_nullable_column_found_not_null_is_refused(self) -> None:
        # enqueue(input_json=None) writes NULL, which the stricter column refuses.
        relation = _run()
        catalog = _in_sync(relation)
        columns = {
            **catalog.columns,
            "input": PostgresType(base="jsonb", is_array=False, not_null=True),
        }

        delta = diff_relation(relation, attrs.evolve(catalog, columns=columns))

        assert diff_outcome([delta]) == "refusal"
        assert "-- ALTER TABLE bi.durable_run ALTER COLUMN input DROP NOT NULL;" in _text(delta)

    def test_a_declared_not_null_column_found_nullable_is_refused_with_the_statement_commented(
        self,
    ) -> None:
        relation = _run()
        catalog = _in_sync(relation)
        columns = {
            **catalog.columns,
            "name": PostgresType(base="text", is_array=False, not_null=False),
        }

        delta = diff_relation(relation, attrs.evolve(catalog, columns=columns))

        assert _statements(delta) == []
        assert "-- ALTER TABLE bi.durable_run ALTER COLUMN name SET NOT NULL;" in _text(delta)

    def test_a_missing_declared_index_is_built_concurrently(self) -> None:
        relation = _run()
        keys = ("status", "name", "created_at")
        catalog = _with(_in_sync(relation), drop=[_index_named(relation, keys)])

        delta = diff_relation(relation, catalog)

        assert _statements(delta) == [
            "CREATE INDEX CONCURRENTLY ON bi.durable_run (status, name, created_at);"
        ]
        assert diff_outcome([delta]) == "delta"

    def test_an_index_on_the_keys_in_another_order_does_not_count(self) -> None:
        relation = _run()
        keys = ("created_at DESC", "run_id DESC")
        catalog = _with(
            _in_sync(relation),
            drop=[_index_named(relation, keys)],
            add=[_found(("created_at", "run_id"), name="asc_listing")],
        )

        assert _statements(diff_relation(relation, catalog)) == [
            "CREATE INDEX CONCURRENTLY ON bi.durable_run (created_at DESC, run_id DESC);"
        ]

    def test_a_unique_constraint_backs_the_declared_unique_index(self) -> None:
        relation = _run()
        keys = ("idempotency_key",)
        catalog = _with(
            _in_sync(relation),
            drop=[_index_named(relation, keys)],
            add=[
                _found(
                    keys, name="durable_run_idempotency_key_key", unique=True, backs_constraint=True
                )
            ],
        )

        assert diff_relation(relation, catalog).blocks == ()

    @pytest.mark.parametrize("beside_an_immediate_one", [False, True])
    def test_a_deferrable_unique_constraint_on_the_conflict_keys_is_refused(
        self, beside_an_immediate_one: bool
    ) -> None:
        # ON CONFLICT infers every unique index on its keys and refuses a deferrable one,
        # INITIALLY IMMEDIATE included, even when an immediate index stands beside it.
        relation = _run()
        keys = ("idempotency_key",)
        deferred = _found(
            keys, name="deferred", unique=True, backs_constraint=True, immediate=False
        )
        drop = [] if beside_an_immediate_one else [_index_named(relation, keys)]
        delta = diff_relation(relation, _with(_in_sync(relation), drop=drop, add=[deferred]))

        assert diff_outcome([delta]) == "refusal"
        assert "-- ALTER TABLE bi.durable_run DROP CONSTRAINT deferred;" in _text(delta)
        assert _statements(delta) == (
            []
            if beside_an_immediate_one
            else ["CREATE UNIQUE INDEX CONCURRENTLY ON bi.durable_run (idempotency_key);"]
        )

    def test_a_deferrable_primary_key_is_refused(self) -> None:
        relation = _run()
        catalog = _with(
            _in_sync(relation),
            drop=["pkey"],
            add=[
                _found(
                    ("run_id",),
                    name="pkey",
                    unique=True,
                    primary=True,
                    backs_constraint=True,
                    immediate=False,
                )
            ],
        )

        delta = diff_relation(relation, catalog)

        assert diff_outcome([delta]) == "refusal"
        assert "deferrable" in _text(delta)

    def test_a_cast_key_is_not_the_retired_column_key(self) -> None:
        relation = _run()
        cast_index = attrs.evolve(
            _found(("status", "created_at"), name="app_cast"),
            keys=("(status)::character varying(1)", "created_at"),
        )

        assert diff_relation(relation, _with(_in_sync(relation), add=[cast_index])).blocks == ()

    def test_an_invalid_index_is_dropped_and_rebuilt(self) -> None:
        relation = _run()
        keys = ("status", "updated_at")
        name = _index_named(relation, keys)
        catalog = _with(_in_sync(relation), drop=[name], add=[_found(keys, name=name, valid=False)])

        assert _statements(diff_relation(relation, catalog)) == [
            f"DROP INDEX CONCURRENTLY bi.{name};",
            "CREATE INDEX CONCURRENTLY ON bi.durable_run (status, updated_at);",
        ]

    def test_an_invalid_index_beside_a_valid_one_is_only_dropped(self) -> None:
        relation = _run()
        keys = ("status", "updated_at")
        catalog = _with(_in_sync(relation), add=[_found(keys, name="leftover", valid=False)])

        assert _statements(diff_relation(relation, catalog)) == [
            "DROP INDEX CONCURRENTLY bi.leftover;"
        ]

    def test_an_invalid_index_during_a_build_is_a_warning_and_nothing_else(self) -> None:
        relation = _run()
        keys = ("status", "updated_at")
        name = _index_named(relation, keys)
        catalog = attrs.evolve(
            _with(_in_sync(relation), drop=[name], add=[_found(keys, name=name, valid=False)]),
            building=True,
        )

        delta = diff_relation(relation, catalog)

        assert _statements(delta) == []
        assert len(delta.warnings) == 1
        assert diff_outcome([delta]) == "refusal"

    @pytest.mark.parametrize(
        "found_name",
        ["durable_run_status_created_at_idx", "idx_durable_run_status", "ix_durable_run_scan"],
    )
    def test_a_retired_index_is_dropped_under_the_name_the_catalog_holds_after_every_create(
        self, found_name: str
    ) -> None:
        relation = _run()
        new = ("status", "name", "created_at")
        catalog = _with(
            _in_sync(relation),
            drop=[_index_named(relation, new)],
            add=[_found(("status", "created_at"), name=found_name)],
        )

        assert _statements(diff_relation(relation, catalog)) == [
            "CREATE INDEX CONCURRENTLY ON bi.durable_run (status, name, created_at);",
            f"DROP INDEX CONCURRENTLY bi.{found_name};",
        ]

    @pytest.mark.parametrize(
        ("index", "reason"),
        [
            (_found(("status", "created_at"), name="u", unique=True), "it is unique"),
            (
                _found(("status", "created_at"), name="c", backs_constraint=True),
                "a constraint is backed by it",
            ),
            (
                _found(("status", "created_at"), name="p", predicate="(status = 'pending'::text)"),
                "its predicate differs",
            ),
            (_found(("status", "created_at"), name="i", included=1), "INCLUDE columns"),
            (_found(("status", "created_at"), name="o", default_opclasses=False), "operator class"),
            (_found(("status", "created_at"), name="l", default_collations=False), "collation"),
        ],
    )
    def test_an_index_with_the_retired_keys_that_is_not_a_retired_match_is_refused(
        self, index: IndexShapeInfo, reason: str
    ) -> None:
        relation = _run()
        delta = diff_relation(relation, _with(_in_sync(relation), add=[index]))

        assert _statements(delta) == []
        assert diff_outcome([delta]) == "refusal"
        assert reason in _text(delta)

    def test_an_undeclared_index_or_column_is_left_alone(self) -> None:
        relation = _run()
        catalog = _in_sync(relation)
        catalog = attrs.evolve(
            _with(
                catalog,
                add=[
                    _found(("name",), name="app_by_name"),
                    _found(("status", "created_at"), name="gin", method="gin"),
                ],
            ),
            columns={
                **catalog.columns,
                "note": PostgresType(base="text", is_array=False, not_null=False),
            },
        )

        assert diff_relation(relation, catalog).blocks == ()

    @pytest.mark.parametrize(
        ("kind", "said"),
        [("view", "a view"), ("partitioned_table", "a partitioned table")],
    )
    def test_anything_but_a_plain_table_under_the_name_is_refused(
        self, kind: PostgresRelationKind, said: str
    ) -> None:
        # A partitioned table takes no CREATE INDEX CONCURRENTLY.
        delta = diff_relation(_run(), CatalogRelation(kind=kind))

        assert diff_outcome([delta]) == "refusal"
        assert _statements(delta) == []
        assert f"is {said}, not a plain table" in _text(delta)

    def test_the_bi_upgrade_from_0_17_prints_what_the_rfc_shows(self) -> None:
        relation = _run()
        catalog = _with(
            _in_sync(relation),
            drop=[
                _index_named(relation, ("status", "name", "created_at")),
                _index_named(relation, ("tenant_id", "status", "name", "created_at")),
            ],
            add=[
                _found(("status", "created_at"), name="durable_run_status_created_at_idx"),
                _found(
                    ("tenant_id", "status", "created_at"),
                    name="durable_run_tenant_id_status_created_at_idx",
                ),
            ],
        )

        text = _text(diff_relation(relation, catalog))

        assert split_sql(text) == [
            "CREATE INDEX CONCURRENTLY ON bi.durable_run (status, name, created_at)",
            "CREATE INDEX CONCURRENTLY ON bi.durable_run (tenant_id, status, name, created_at)",
            "DROP INDEX CONCURRENTLY bi.durable_run_status_created_at_idx",
            "DROP INDEX CONCURRENTLY bi.durable_run_tenant_id_status_created_at_idx",
        ]
        assert text.startswith(
            "-- Shape only: data rewrites are announced as **Migration:** SQL in CHANGELOG.md.\n"
            "-- CONCURRENTLY cannot run in a transaction (Flyway: executeInTransaction=false).\n\n"
            "-- forze 9.9.9 · bi.durable_run (PostgresDurableRunStore)\n"
        )

    def test_a_refusal_wins_over_a_delta(self) -> None:
        relation = _run()
        delta = diff_relation(relation, None)
        refused = diff_relation(relation, CatalogRelation(kind="view"))

        assert diff_outcome([delta]) == "delta"
        assert diff_outcome([delta, refused]) == "refusal"

    def test_a_retired_column_still_present_is_a_note_that_asks_nothing(self) -> None:
        shape = attrs.evolve(
            _run().shape,
            retired=(
                *_run().shape.retired,
                Retired(shape=OwnedColumn(name="legacy", type="text"), comment="retired in 1.0"),
            ),
        )
        relation = attrs.evolve(_run(), shape=shape)
        catalog = _in_sync(relation)
        catalog = attrs.evolve(
            catalog,
            columns={
                **catalog.columns,
                "legacy": PostgresType(base="text", is_array=False, not_null=False),
            },
        )

        delta = diff_relation(relation, catalog)

        assert diff_outcome([delta]) == "in_sync"
        assert "-- ALTER TABLE bi.durable_run DROP COLUMN legacy;" in _text(delta)

    def test_an_index_that_serves_a_declared_shape_is_never_dropped_as_retired(self) -> None:
        # A shape retired as non-unique and declared again as unique: the unique index stays.
        relation = _run()
        shape = attrs.evolve(
            relation.shape,
            indexes=(
                *relation.shape.indexes,
                OwnedIndex(keys=("status", "created_at"), unique=True, comment="declared"),
            ),
        )
        relation = attrs.evolve(relation, shape=shape)
        catalog = _with(
            _in_sync(_run()),
            add=[_found(("status", "created_at"), name="kept", unique=True)],
        )

        assert diff_relation(relation, catalog).blocks == ()

    def test_declared_shapes_are_checked_against_the_index_type(self) -> None:
        # A hash index on the declared keys is not the declared b-tree.
        relation = _run()
        keys = ("status", "updated_at")
        catalog = _with(
            _in_sync(relation),
            drop=[_index_named(relation, keys)],
            add=[_found(keys, name="hash", method="hash")],
        )

        assert _statements(diff_relation(relation, catalog)) == [
            "CREATE INDEX CONCURRENTLY ON bi.durable_run (status, updated_at);"
        ]
