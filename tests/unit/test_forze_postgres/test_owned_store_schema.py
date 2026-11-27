"""The store tables' shapes, their config switches, and the rules every declaration keeps
(RFC 0080 §5.1, §6, P2)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, cast

import attrs
import pytest

from forze.base.exceptions import CoreException
from forze_postgres import PostgresClient, PostgresDepsModule, owned_relations
from forze_postgres.adapters.durable.schema import (
    durable_run_shape,
    durable_schedule_shape,
    durable_step_shape,
)
from forze_postgres.adapters.store_schema import (
    counter_shape,
    hlc_checkpoint_shape,
    idempotency_shape,
    inbox_shape,
    outbox_delivery_log_shape,
    outbox_shape,
    principal_epoch_shape,
    rotating_credentials_shape,
)
from forze_postgres.execution.deps.configs import (
    PostgresCounterConfig,
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
    PostgresHlcCheckpointConfig,
    PostgresIdempotencyConfig,
    PostgresInboxConfig,
    PostgresOutboxConfig,
    PostgresPrincipalEpochConfig,
    PostgresRotatingCredentialsConfig,
)
from forze_postgres.kernel.catalog.introspect import IndexShapeInfo, PostgresType
from forze_postgres.kernel.catalog.introspect.utils import normalize_pg_type
from forze_postgres.kernel.catalog.owned import (
    BoundOwnedRelation,
    CatalogRelation,
    OwnedColumn,
    OwnedIndex,
    OwnedRelationShape,
    Retired,
    diff_outcome,
    diff_relation,
    is_constant_default,
    render_diff,
    render_sql,
)
from forze_postgres.kernel.catalog.owned.matching import normalize_expression
from forze_postgres.kernel.catalog.owned.render import default_partition_name
from tests.support.sql_statements import split_sql

pytestmark = pytest.mark.unit

# ----------------------- #

_EXCHANGER: Any = object()
"""Stands in for the token endpoint; no shape reads it."""


def _outbox(**kwargs: Any) -> PostgresOutboxConfig:
    return PostgresOutboxConfig(**{"relation": ("app", "outbox"), **kwargs})


def _credentials() -> PostgresRotatingCredentialsConfig:
    return PostgresRotatingCredentialsConfig(relation=("app", "credentials"), exchanger=_EXCHANGER)


def _every_shape() -> Iterator[OwnedRelationShape]:
    """Every declared shape, under every switch that changes it."""

    for tenant_aware in (False, True):
        yield durable_run_shape(
            PostgresDurableRunConfig(relation=("a", "b"), tenant_aware=tenant_aware)
        )
        yield durable_schedule_shape(
            PostgresDurableScheduleConfig(relation=("a", "b"), tenant_aware=tenant_aware)
        )

        for hlc in (False, True):
            for trace in (False, True):
                for storage in ("retain", "consume"):
                    yield outbox_shape(
                        _outbox(
                            tenant_aware=tenant_aware,
                            hlc_ordering=hlc,
                            propagate_trace=trace,
                            storage=storage,
                        )
                    )

    yield durable_step_shape(PostgresDurableStepConfig(relation=("a", "b")))
    yield outbox_delivery_log_shape(_outbox(storage="consume", delivery_log=("app", "log")))
    yield hlc_checkpoint_shape(PostgresHlcCheckpointConfig(relation=("a", "b")))
    yield inbox_shape(PostgresInboxConfig(relation=("a", "b")))
    yield idempotency_shape(PostgresIdempotencyConfig(relation=("a", "b")))
    yield counter_shape(PostgresCounterConfig(relation=("a", "b")))
    yield principal_epoch_shape(PostgresPrincipalEpochConfig(relation=("a", "b")))
    yield rotating_credentials_shape(_credentials())


def _every_store(**outbox: Any) -> PostgresDepsModule:
    return PostgresDepsModule(
        client=PostgresClient(),
        durable_run=PostgresDurableRunConfig(relation=("app", "runs")),
        durable_step=PostgresDurableStepConfig(relation=("app", "steps")),
        durable_schedule=PostgresDurableScheduleConfig(relation=("app", "schedules")),
        outboxes={
            "orders": _outbox(storage="consume", delivery_log=("app", "outbox_delivered"), **outbox)
        },
        hlc_checkpoint=PostgresHlcCheckpointConfig(relation=("app", "hlc_checkpoint")),
        inboxes={"orders": PostgresInboxConfig(relation=("app", "inbox"))},
        idempotencies={"orders": PostgresIdempotencyConfig(relation=("app", "idempotency"))},
        counters={"orders": PostgresCounterConfig(relation=("app", "counters"))},
        principal_epochs=PostgresPrincipalEpochConfig(relation=("app", "principal_epochs")),
        rotating_credentials=_credentials(),
    )


def _columns(shape: OwnedRelationShape) -> list[str]:
    return [column.name for column in shape.columns]


def _keys(shape: OwnedRelationShape) -> set[tuple[str, ...]]:
    return {index.keys for index in shape.indexes}


def _shape(**kwargs: Any) -> OwnedRelationShape:
    defaults: dict[str, Any] = {
        "owner": "X",
        "since": "1.0.0",
        "columns": (OwnedColumn(name="a", type="text", nullable=False),),
        "primary_key": ("a",),
        "indexes": (),
    }

    return OwnedRelationShape(**{**defaults, **kwargs})


def _found(keys: tuple[str, ...], *, name: str, valid: bool = True) -> IndexShapeInfo:
    """An index as the catalog reports it: each key bare, its direction in the flags."""

    descending = tuple(key.endswith(" DESC") for key in keys)

    return IndexShapeInfo(
        name=name,
        schema="app",
        keys=tuple(key.removesuffix(" DESC") for key in keys),
        descending=descending,
        nulls_first=descending,
        included=0,
        method="btree",
        unique=False,
        primary=False,
        nulls_not_distinct=False,
        predicate=None,
        valid=valid,
        backs_constraint=False,
        owns_constraint=False,
        referenced_by_foreign_key=False,
        default_opclasses=True,
        default_collations=True,
        immediate=True,
    )


def _columns_of(shape: OwnedRelationShape) -> dict[str, PostgresType]:
    return {
        column.name: PostgresType(
            base=normalize_pg_type(column.type), is_array=False, not_null=not column.nullable
        )
        for column in shape.columns
    }


# ....................... #


class TestEveryDeclaration:
    def test_every_store_table_renders_from_one_deps_module(self) -> None:
        relations = owned_relations(_every_store())
        text = render_sql(relations.bound, version="9.9.9")

        assert [relation.route for relation in relations.bound] == [
            "durable_run",
            "durable_step",
            "durable_schedule",
            "outboxes.orders",
            "outboxes.orders.delivery_log",
            "hlc_checkpoint",
            "inboxes.orders",
            "idempotencies.orders",
            "counters.orders",
            "principal_epochs",
            "rotating_credentials",
        ]
        assert relations.skipped == ()
        # Eleven tables and the delivery log's DEFAULT partition.
        assert text.count("CREATE TABLE") == 12
        assert all(
            statement.startswith(("CREATE TABLE", "CREATE INDEX", "CREATE UNIQUE INDEX"))
            for statement in split_sql(text)
        )

    def test_no_declared_default_is_volatile(self) -> None:
        defaults = [c.default for s in _every_shape() for c in s.columns if c.default is not None]

        assert defaults
        assert all(is_constant_default(default) for default in defaults)

    def test_every_column_added_after_its_table_shipped_is_safe_to_add(self) -> None:
        # The shapes refuse to build otherwise; this keeps the rule from going unexercised.
        later = [
            column
            for shape in _every_shape()
            for column in shape.columns
            if column.since is not None and column.since != shape.since
        ]

        assert later
        assert all(column.nullable or column.default is not None for column in later)

    def test_every_index_comment_is_one_line(self) -> None:
        labels = [index.label for shape in _every_shape() for index in shape.indexes]

        assert all("\n" not in label for label in labels)


class TestRules:
    @pytest.mark.parametrize("since", [None, "1.1.0"], ids=["shipped", "added-later"])
    @pytest.mark.parametrize("default", ["NULL", " null "])
    def test_a_not_null_column_defaulting_to_null_is_refused(
        self, since: str | None, default: str
    ) -> None:
        # ADD COLUMN … NOT NULL DEFAULT NULL fails on a table that holds rows.
        column = OwnedColumn(name="b", type="text", nullable=False, default=default, since=since)

        with pytest.raises(CoreException) as caught:
            _shape(columns=(OwnedColumn(name="a", type="text", nullable=False), column))

        assert caught.value.code == "postgres_owned_shape_invalid"

    def test_a_column_added_later_as_not_null_without_a_default_is_refused(self) -> None:
        late = OwnedColumn(name="b", type="text", nullable=False, since="1.1.0")

        with pytest.raises(CoreException) as caught:
            _shape(columns=(OwnedColumn(name="a", type="text", nullable=False), late))

        assert caught.value.code == "postgres_owned_shape_invalid"

    @pytest.mark.parametrize(
        "column",
        [
            OwnedColumn(name="b", type="text", since="1.1.0"),
            OwnedColumn(name="b", type="integer", nullable=False, default="0", since="1.1.0"),
            OwnedColumn(name="b", type="text", nullable=False, since="1.0.0"),
        ],
        ids=["nullable", "literal-default", "shipped-with-the-table"],
    )
    def test_a_column_safe_to_add_is_accepted(self, column: OwnedColumn) -> None:
        _shape(columns=(OwnedColumn(name="a", type="text", nullable=False), column))

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"since": "1.0"},
            {"columns": (OwnedColumn(name="a", type="text", nullable=False, since="0.9.0"),)},
            {"indexes": (OwnedIndex(keys=("a",), since="0.9.0", comment="early"),)},
            {"partitioned_by": "missing"},
        ],
        ids=["not-x-y-z", "column-before-the-table", "index-before-the-table", "partition-key"],
    )
    def test_an_inconsistent_release_or_partition_is_refused(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(CoreException) as caught:
            _shape(**kwargs)

        assert caught.value.code == "postgres_owned_shape_invalid"

    @pytest.mark.parametrize(
        ("keys", "where"),
        [
            (("ts",), "ts > '2024-01-01 12:00:00'::date"),
            (("ts",), "attempts = 0::bigint"),
            (("coalesce(a, '0'::text)",), None),
        ],
    )
    def test_a_cast_on_a_declared_literal_is_refused(
        self, keys: tuple[str, ...], where: str | None
    ) -> None:
        with pytest.raises(CoreException) as caught:
            OwnedIndex(keys=keys, where=where, comment="cast")

        assert caught.value.code == "postgres_owned_shape_invalid"

    def test_a_cast_on_a_column_stays_declarable(self) -> None:
        OwnedIndex(keys=("(status)::text",), where="status::text = 'pending'", comment="cast")

    def test_a_bare_literal_matches_its_catalog_form_and_a_folded_one_does_not(self) -> None:
        declared = normalize_expression("ts > '2024-01-01 12:00:00'", predicate=True)

        assert declared == normalize_expression(
            "(ts > '2024-01-01 12:00:00'::timestamp without time zone)", predicate=True
        )
        # Postgres stores a cast to date folded: midnight, not noon.
        assert declared != normalize_expression("(ts > '2024-01-01'::date)", predicate=True)


class TestOutbox:
    def test_the_base_shape_claims_by_created_at(self) -> None:
        shape = outbox_shape(_outbox())

        assert "published_at" in _columns(shape) and "processing_at" in _columns(shape)
        assert {"hlc", "traceparent"}.isdisjoint(_columns(shape))
        assert _keys(shape) == {
            ("outbox_route", "event_id"),
            ("outbox_route", "status", "created_at"),
        }

    def test_hlc_ordering_adds_hlc_and_claims_by_it(self) -> None:
        shape = outbox_shape(_outbox(hlc_ordering=True))

        assert "hlc" in _columns(shape)
        assert ("outbox_route", "status", "hlc", "created_at", "id") in _keys(shape)
        assert ("outbox_route", "status", "created_at") not in _keys(shape)

    def test_propagate_trace_adds_traceparent(self) -> None:
        assert "traceparent" in _columns(outbox_shape(_outbox(propagate_trace=True)))

    def test_consume_storage_leaves_out_published_at_and_keeps_processing_at(self) -> None:
        # The relay's stale reclaim reads processing_at in either mode.
        columns = _columns(outbox_shape(_outbox(storage="consume")))

        assert "published_at" not in columns
        assert "processing_at" in columns

    @pytest.mark.parametrize("hlc", [False, True])
    def test_a_tagged_table_adds_the_tenant_leading_claim(self, hlc: bool) -> None:
        plain = _keys(outbox_shape(_outbox(hlc_ordering=hlc)))
        tagged = _keys(outbox_shape(_outbox(hlc_ordering=hlc, tenant_aware=True)))

        assert plain < tagged
        assert all(keys[0] == "tenant_id" for keys in tagged - plain)

    def test_a_tenant_scoped_outbox_adds_no_tenant_variant(self) -> None:
        config = _outbox(relation=lambda tenant: ("t", "outbox"), tenant_aware=True)

        assert _keys(outbox_shape(config)) == _keys(outbox_shape(_outbox()))

    def test_the_claim_indexes_the_pages_and_docstring_printed_are_retired(self) -> None:
        retired = {item.shape.keys for item in outbox_shape(_outbox()).retired}

        assert retired == {
            ("outbox_route", "status", "available_at", "created_at"),
            ("outbox_route", "status", "available_at", "hlc", "created_at", "id"),
            ("outbox_route", "status", "hlc", "created_at"),
        }

    def test_the_page_claim_index_is_dropped_after_the_new_one_is_built(self) -> None:
        relation = BoundOwnedRelation(
            route="outboxes.orders", schema="app", name="outbox", shape=outbox_shape(_outbox())
        )
        shape = relation.shape
        catalog = CatalogRelation(
            kind="table",
            columns=_columns_of(shape),
            primary_key=shape.primary_key,
            indexes=(
                attrs.evolve(
                    _found(("id",), name="outbox_pkey"),
                    unique=True,
                    primary=True,
                    backs_constraint=True,
                    owns_constraint=True,
                ),
                attrs.evolve(
                    _found(("outbox_route", "event_id"), name="outbox_uq"),
                    unique=True,
                    backs_constraint=True,
                    owns_constraint=True,
                ),
                _found(
                    ("outbox_route", "status", "available_at", "created_at"),
                    name="outbox_claim_idx",
                ),
            ),
        )

        delta = diff_relation(relation, catalog)

        assert [block.statement for block in delta.statements] == [
            "CREATE INDEX CONCURRENTLY ON app.outbox (outbox_route, status, created_at);",
            "DROP INDEX CONCURRENTLY app.outbox_claim_idx;",
        ]
        assert "-- retired in 0.25.0: replaced by" in render_diff([delta], version="9.9.9")


class TestDeliveryLog:
    def _relation(self) -> BoundOwnedRelation:
        shape = outbox_delivery_log_shape(_outbox(storage="consume", delivery_log=("app", "log")))

        return BoundOwnedRelation(
            route="outboxes.orders.delivery_log", schema="app", name="log", shape=shape
        )

    def _catalog(self, kind: Any, *indexes: IndexShapeInfo) -> CatalogRelation:
        shape = self._relation().shape

        # A partitioned table as forze creates it: with its DEFAULT partition.
        return CatalogRelation(
            kind=kind,
            columns=_columns_of(shape),
            indexes=indexes,
            partitions=1,
            default_partition=True,
        )

    def test_sql_creates_the_keyless_partitioned_table_and_its_default_partition(self) -> None:
        statements = split_sql(render_sql([self._relation()], version="9.9.9"))

        assert statements[0].endswith(") PARTITION BY RANGE (published_at)")
        assert "PRIMARY KEY" not in statements[0]
        assert statements[1] == "CREATE TABLE app.log_default PARTITION OF app.log DEFAULT"
        assert statements[2] == "CREATE INDEX ON app.log (event_id)"

    def test_a_partitioned_log_with_its_index_is_in_sync(self) -> None:
        catalog = self._catalog("partitioned_table", _found(("event_id",), name="log_event"))

        assert diff_relation(self._relation(), catalog).blocks == ()

    @pytest.mark.parametrize("valid", [None, False], ids=["missing", "invalid"])
    def test_a_partitioned_log_without_a_valid_index_gets_a_warning(
        self, valid: bool | None
    ) -> None:
        indexes = () if valid is None else (_found(("event_id",), name="log_event", valid=False),)
        delta = diff_relation(self._relation(), self._catalog("partitioned_table", *indexes))
        text = render_diff([delta], version="9.9.9")

        assert diff_outcome([delta]) == "refusal"
        assert delta.statements == ()
        assert "-- CREATE INDEX ON app.log (event_id);" in text
        assert "CONCURRENTLY ON" not in text

    def test_a_plain_log_takes_its_index_concurrently(self) -> None:
        delta = diff_relation(self._relation(), self._catalog("table"))

        assert [block.statement for block in delta.statements] == [
            "CREATE INDEX CONCURRENTLY ON app.log (event_id);"
        ]

    def test_a_retired_index_on_a_partitioned_table_is_a_warning_with_its_drop_commented(
        self,
    ) -> None:
        old = OwnedIndex(keys=("outbox_route",), comment="an old scan")
        relation = self._relation()
        relation = attrs.evolve(
            relation,
            shape=attrs.evolve(
                relation.shape,
                retired=(Retired(shape=old, retired_in="0.25.0", comment="unused"),),
            ),
        )
        catalog = self._catalog(
            "partitioned_table",
            _found(("event_id",), name="log_event"),
            _found(("outbox_route",), name="log_route"),
        )

        delta = diff_relation(relation, catalog)
        text = render_diff([delta], version="9.9.9")

        assert delta.statements == ()
        assert diff_outcome([delta]) == "refusal"
        assert "-- DROP INDEX app.log_route;" in text
        assert "-- retired in 0.25.0: unused" in text

    def test_a_partitioned_log_missing_an_optional_column_and_its_index_starts(self) -> None:
        from forze_postgres.execution.lifecycle.owned_schema import grade

        relation = self._relation()
        relation = attrs.evolve(
            relation,
            shape=attrs.evolve(
                relation.shape,
                columns=(*relation.shape.columns, OwnedColumn(name="tag", type="text", required=False)),
                indexes=(
                    *relation.shape.indexes,
                    OwnedIndex(keys=("tag",), required=True, comment="tag lookups"),
                ),
            ),
        )
        catalog = self._catalog("partitioned_table", _found(("event_id",), name="log_event"))

        findings = grade([relation], [diff_relation(relation, catalog)])

        assert findings.refusals == ()
        assert any("tag" in warning for warning in findings.warnings)

    @pytest.mark.parametrize(
        "name",
        ["l" * 63, "l" * 55 + "_default", "ж" * 31],
        ids=["63-bytes", "ends-in-the-suffix", "multibyte"],
    )
    def test_the_default_partition_name_stays_distinct_within_63_bytes(self, name: str) -> None:
        relation = attrs.evolve(self._relation(), name=name)
        statement = split_sql(render_sql([relation], version="9.9.9"))[1]
        partition = statement.removeprefix("CREATE TABLE app.").split(" PARTITION OF ")[0]
        bare = partition.strip('"')

        assert bare == default_partition_name(name)
        assert len(bare.encode()) <= 63
        assert bare != name
        assert bare.endswith("_default")

    def test_a_short_name_keeps_its_plain_default_partition_name(self) -> None:
        assert default_partition_name("outbox_delivered") == "outbox_delivered_default"

    def test_the_default_partition_never_takes_another_bound_tables_name(self) -> None:
        # The outbox holds events_default, so the log's partition takes the hashed form.
        deps = PostgresDepsModule(
            client=PostgresClient(),
            outboxes={
                "events": _outbox(
                    relation=("app", "events_default"),
                    storage="consume",
                    delivery_log=("app", "events"),
                )
            },
        )

        text = render_sql(owned_relations(deps).bound, version="9.9.9")
        partitions = [s for s in split_sql(text) if " PARTITION OF " in s]

        assert partitions == [
            f"CREATE TABLE app.{default_partition_name('events', hashed=True)} "
            "PARTITION OF app.events DEFAULT"
        ]

    def test_two_partitions_never_take_one_name(self) -> None:
        # events takes the hashed name, which is the plain one of the log events_<hash>.
        digest = default_partition_name("events", hashed=True).removesuffix("_default")
        deps = PostgresDepsModule(
            client=PostgresClient(),
            outboxes={
                "a": _outbox(
                    relation=("app", "events_default"),
                    storage="consume",
                    delivery_log=("app", "events"),
                ),
                "b": _outbox(
                    relation=("app", "other"), storage="consume", delivery_log=("app", digest)
                ),
            },
        )

        text = render_sql(owned_relations(deps).bound, version="9.9.9")
        partitions = [
            s.split(" PARTITION OF ")[0] for s in split_sql(text) if " PARTITION OF " in s
        ]

        assert len(partitions) == 2
        assert len(set(partitions)) == 2

    def test_bound_tables_holding_both_partition_names_are_refused(self) -> None:
        hashed = default_partition_name("events", hashed=True)
        deps = PostgresDepsModule(
            client=PostgresClient(),
            outboxes={
                "events": _outbox(
                    relation=("app", "events_default"),
                    storage="consume",
                    delivery_log=("app", "events"),
                ),
                "other": _outbox(relation=("app", hashed)),
            },
        )

        with pytest.raises(CoreException) as caught:
            owned_relations(deps)

        assert caught.value.code == "postgres_owned_partition_name_taken"

    def test_two_long_names_sharing_a_prefix_get_two_partitions(self) -> None:
        # Cutting both to the same prefix would make the second CREATE TABLE fail.
        assert default_partition_name("a" * 55 + "x") != default_partition_name("a" * 55 + "y")

    def test_a_partitioned_log_with_no_partition_gets_a_warning(self) -> None:
        catalog = attrs.evolve(
            self._catalog("partitioned_table", _found(("event_id",), name="log_event")),
            partitions=0,
            default_partition=False,
        )

        delta = diff_relation(self._relation(), catalog)
        text = render_diff([delta], version="9.9.9")

        assert diff_outcome([delta]) == "refusal"
        assert "-- CREATE TABLE app.log_default PARTITION OF app.log DEFAULT;" in text

    def test_a_partitioned_log_without_a_default_partition_gets_a_note(self) -> None:
        catalog = attrs.evolve(
            self._catalog("partitioned_table", _found(("event_id",), name="log_event")),
            partitions=3,
            default_partition=False,
        )

        delta = diff_relation(self._relation(), catalog)

        assert diff_outcome([delta]) == "in_sync"
        assert "-- CREATE TABLE app.log_default PARTITION OF app.log DEFAULT;" in render_diff(
            [delta], version="9.9.9"
        )

    def test_a_view_under_the_log_is_still_refused(self) -> None:
        delta = diff_relation(self._relation(), self._catalog("view"))

        assert diff_outcome([delta]) == "refusal"

    def test_a_key_the_application_adds_is_its_own(self) -> None:
        catalog = attrs.evolve(
            self._catalog("table", _found(("event_id",), name="e")), primary_key=("id",)
        )

        assert diff_relation(self._relation(), catalog).blocks == ()


class TestSharedTables:
    def test_routes_on_one_table_bind_it_once_with_every_column_either_needs(self) -> None:
        deps = PostgresDepsModule(
            client=PostgresClient(),
            outboxes={
                "billing": _outbox(storage="consume"),
                "orders": _outbox(hlc_ordering=True),
            },
        )

        (relation,) = owned_relations(deps).bound
        columns = _columns(relation.shape)

        assert relation.route == "outboxes.billing, outboxes.orders"
        # billing never writes published_at, but orders does, so the table keeps it.
        assert "published_at" in columns and "hlc" in columns
        assert {
            ("outbox_route", "status", "created_at"),
            ("outbox_route", "status", "hlc", "created_at", "id"),
        } <= _keys(relation.shape)

    def test_counter_routes_on_one_table_share_it(self) -> None:
        deps = PostgresDepsModule(
            client=PostgresClient(),
            counters={
                "a": PostgresCounterConfig(relation=("app", "counters")),
                "b": PostgresCounterConfig(relation=("app", "counters"), tenant_aware=True),
            },
        )

        (relation,) = owned_relations(deps).bound

        assert relation.route == "counters.a, counters.b"

    def test_two_kinds_of_store_on_one_table_are_refused(self) -> None:
        deps = PostgresDepsModule(
            client=PostgresClient(),
            outboxes={"orders": _outbox()},
            inboxes={"orders": PostgresInboxConfig(relation=("app", "outbox"))},
        )

        with pytest.raises(CoreException) as caught:
            owned_relations(deps)

        assert caught.value.code == "postgres_owned_relation_shared"

    def test_a_column_declared_two_ways_is_refused(self) -> None:
        one = _shape(columns=(OwnedColumn(name="a", type="text", nullable=False),))
        other = _shape(columns=(OwnedColumn(name="a", type="bigint", nullable=False),))

        with pytest.raises(CoreException) as caught:
            one.union(other)

        assert caught.value.code == "postgres_owned_relation_shared"

    def test_a_column_either_route_requires_is_required(self) -> None:
        optional = OwnedColumn(name="b", type="text", required=False)
        one = _shape(columns=(OwnedColumn(name="a", type="text", nullable=False), optional))
        other = _shape(
            columns=(
                OwnedColumn(name="a", type="text", nullable=False),
                attrs.evolve(optional, required=True),
            )
        )

        assert one.union(other).columns[1].required

    def test_a_tenant_scoped_route_is_skipped_by_name(self) -> None:
        deps = PostgresDepsModule(
            client=PostgresClient(),
            counters={"a": PostgresCounterConfig(relation=lambda tenant: ("t", "counters"))},
        )

        assert owned_relations(deps).skipped == ("counters.a",)


class TestKeepRetired:
    def _run(self) -> BoundOwnedRelation:
        shape = durable_run_shape(PostgresDurableRunConfig(relation=("app", "runs")))

        return BoundOwnedRelation(route="durable_run", schema="app", name="runs", shape=shape)

    def _catalog(self, *extra: IndexShapeInfo) -> CatalogRelation:
        relation = self._run()
        shape = relation.shape
        indexes = [
            attrs.evolve(
                _found(shape.primary_key, name="pkey"),
                unique=True,
                primary=True,
                backs_constraint=True,
                owns_constraint=True,
            )
        ]
        indexes.extend(
            attrs.evolve(
                _found(index.keys, name=f"i{position}"),
                unique=index.unique,
                predicate="((status = 'pending'::text) AND (attempts = 0))"
                if index.where
                else None,
            )
            for position, index in enumerate(shape.indexes)
        )

        return CatalogRelation(
            kind="table",
            columns=_columns_of(shape),
            primary_key=shape.primary_key,
            indexes=(*indexes, *extra),
        )

    def test_a_retired_match_is_dropped_unless_retired_indexes_are_kept(self) -> None:
        catalog = self._catalog(_found(("status", "created_at"), name="old_scan"))

        assert diff_outcome([diff_relation(self._run(), catalog)]) == "delta"
        assert diff_relation(self._run(), catalog, keep_retired=True).blocks == ()

    def test_a_retired_index_is_kept_while_a_declared_one_is_still_being_built(self) -> None:
        # The retired scan serves the recovery scan until its replacement is usable.
        replacement = ("status", "name", "created_at")
        catalog = self._catalog(_found(("status", "created_at"), name="old_scan"))
        catalog = attrs.evolve(
            catalog,
            indexes=tuple(
                attrs.evolve(index, valid=False) if index.keys == replacement else index
                for index in catalog.indexes
            ),
            building=True,
        )

        delta = diff_relation(self._run(), catalog)
        text = render_diff([delta], version="9.9.9")

        assert delta.statements == ()
        assert "DROP INDEX CONCURRENTLY app.old_scan;" not in text.replace("-- DROP", "")
        assert "old_scan" in text

    def test_an_ambiguous_retired_index_is_not_refused_when_retired_indexes_are_kept(self) -> None:
        ambiguous = attrs.evolve(_found(("status", "created_at"), name="app_scan"), unique=True)
        catalog = self._catalog(ambiguous)

        assert diff_outcome([diff_relation(self._run(), catalog)]) == "refusal"
        assert diff_relation(self._run(), catalog, keep_retired=True).blocks == ()


class TestLabels:
    def test_an_index_comment_names_its_grade_and_release(self) -> None:
        text = render_sql([TestKeepRetired()._run()], version="9.9.9")

        assert "-- recommended since 0.18.0: the recovery scan (claim_abandoned)" in text
        assert "-- required: two submits of one key converge on one run" in text
        assert "-- recommended: the admin listing (list_runs)" in text

    def test_a_late_column_names_its_release_where_it_is_added(self) -> None:
        relation = TestKeepRetired()._run()
        catalog = TestKeepRetired()._catalog()
        catalog = attrs.evolve(
            catalog,
            columns={k: v for k, v in catalog.columns.items() if k != "cancel_requested_at"},
        )

        text = render_diff([diff_relation(relation, catalog)], version="9.9.9")

        assert "-- since 0.6.0: starting a run, the recovery scan and every read return it" in text


def test_every_shape_function_is_reachable_from_a_deps_module() -> None:
    # A shape no walker reaches is a table forze schema never prints.
    owners = {relation.shape.owner for relation in owned_relations(_every_store()).bound}

    assert owners == {shape.owner for shape in _every_shape()}


def test_the_exchanger_stub_is_unused_by_the_shape() -> None:
    assert rotating_credentials_shape(_credentials()) == rotating_credentials_shape(
        attrs.evolve(_credentials(), exchanger=cast(Any, None))
    )
