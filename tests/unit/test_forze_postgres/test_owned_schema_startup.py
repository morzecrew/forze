"""The owned-table startup check's grading, its wiring, and tenant binding (RFC 0080 P2b)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.execution import Deps
from forze.base.exceptions import CoreException, exc
from forze_postgres import (
    PostgresClient,
    PostgresDepsModule,
    owned_relations,
    resolve_owned_relations,
)
from forze_postgres.execution.deps.configs import (
    PostgresCounterConfig,
    PostgresInboxConfig,
)
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.lifecycle import (
    PostgresLifecycleModule,
    PostgresOwnedSchemaValidationHook,
)
from forze_postgres.execution.lifecycle.owned_schema import REFUSED_FINDINGS, grade
from forze_postgres.kernel.catalog.introspect import IndexShapeInfo, PostgresType
from forze_postgres.kernel.catalog.introspect.utils import normalize_pg_type
from forze_postgres.kernel.catalog.owned import (
    BoundOwnedRelation,
    CatalogRelation,
    diff_relation,
)
from forze_postgres.kernel.client.value_objects import PostgresTransactionOptions
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit

# ----------------------- #


def _counter() -> BoundOwnedRelation:
    (relation,) = owned_relations(
        PostgresDepsModule(
            client=PostgresClient(),
            counters={"orders": PostgresCounterConfig(relation=("app", "counters"))},
        )
    ).bound

    return relation


def _pkey(keys: tuple[str, ...]) -> IndexShapeInfo:
    return IndexShapeInfo(
        name="pkey",
        schema="app",
        keys=keys,
        descending=(False,) * len(keys),
        nulls_first=(False,) * len(keys),
        included=0,
        method="btree",
        unique=True,
        primary=True,
        nulls_not_distinct=False,
        predicate=None,
        valid=True,
        backs_constraint=True,
        owns_constraint=True,
        referenced_by_foreign_key=False,
        default_opclasses=True,
        default_collations=True,
        immediate=True,
    )


def _catalog(relation: BoundOwnedRelation, **columns: PostgresType | None) -> CatalogRelation:
    """The table as created from its shape, with *columns* replaced, or dropped when None."""

    found: dict[str, PostgresType] = {
        column.name: PostgresType(
            base=normalize_pg_type(column.type), is_array=False, not_null=not column.nullable
        )
        for column in relation.shape.columns
    }

    for name, value in columns.items():
        if value is None:
            found.pop(name)
        else:
            found[name] = value

    return CatalogRelation(
        kind="table",
        columns=found,
        primary_key=relation.shape.primary_key,
        indexes=(_pkey(relation.shape.primary_key),),
    )


def _two_tables() -> Any:
    return owned_relations(
        PostgresDepsModule(
            client=PostgresClient(),
            inboxes={"events": PostgresInboxConfig(relation=("app", "inbox"))},
            counters={"orders": PostgresCounterConfig(relation=("app", "counters"))},
        )
    )


def _run_shape() -> Any:
    from forze_postgres.adapters.durable.schema import durable_run_shape
    from forze_postgres.execution.deps.configs import PostgresDurableRunConfig

    return durable_run_shape(PostgresDurableRunConfig(relation=("app", "runs")))


class _Client:
    """Records the transactions the hook opens."""

    def __init__(self) -> None:
        self.options: list[PostgresTransactionOptions | None] = []

    @asynccontextmanager
    async def transaction(
        self, *, options: PostgresTransactionOptions | None = None
    ) -> AsyncIterator[None]:
        self.options.append(options)
        yield None


class _Introspector:
    """An introspector over one recorded catalog, every relation missing by default."""

    def __init__(
        self,
        *,
        partition: bool = True,
        catalog: CatalogRelation | None = None,
        error: Exception | None = None,
    ) -> None:
        self.client = _Client()
        self.partition = partition
        self.catalog = catalog
        self.error = error

    def require_partition(self) -> None:
        if not self.partition:
            raise exc.internal("no partition", code="introspection_partition_required")

    async def find_relation(self, **_: Any) -> str | None:
        if self.error is not None:
            raise self.error

        return None if self.catalog is None else "table"

    async def get_column_types(self, **_: Any) -> Any:
        assert self.catalog is not None
        return self.catalog.columns

    async def get_primary_key_columns(self, **_: Any) -> Any:
        assert self.catalog is not None
        return self.catalog.primary_key

    async def indexes(self, **_: Any) -> Any:
        assert self.catalog is not None
        return self.catalog.indexes

    async def index_build_in_progress(self, **_: Any) -> bool:
        return False


# ....................... #


class TestGrade:
    def test_a_table_created_from_its_shape_has_nothing_to_report(self) -> None:
        relation = _counter()

        findings = grade([relation], [diff_relation(relation, _catalog(relation))])

        assert findings.refusals == () and findings.warnings == ()

    def test_a_missing_table_is_refused(self) -> None:
        findings = grade([_counter()], [None])

        assert findings.refusals == ("app.counters is missing (PostgresCounterAdapter).",)

    def test_a_missing_required_column_is_refused(self) -> None:
        relation = _counter()

        findings = grade([relation], [diff_relation(relation, _catalog(relation, value=None))])

        assert findings.refusals == (
            "column value is missing from app.counters, and it is NOT NULL with no literal "
            "default.",
        )

    def test_a_missing_optional_column_only_warns(self) -> None:
        relation = _counter()

        findings = grade(
            [relation], [diff_relation(relation, _catalog(relation, expires_at=None))]
        )

        assert findings.refusals == ()
        assert findings.warnings == (
            "app.counters needs: ALTER TABLE app.counters ADD COLUMN expires_at timestamptz;",
        )

    def test_a_type_of_another_family_or_a_smaller_integer_is_refused(self) -> None:
        relation = _counter()
        other = PostgresType(base="uuid", is_array=False, not_null=True)
        narrow = PostgresType(base="int4", is_array=False, not_null=True, formatted="integer")

        refused = grade([relation], [diff_relation(relation, _catalog(relation, suffix=other))])
        smaller = grade([relation], [diff_relation(relation, _catalog(relation, value=narrow))])

        assert len(refused.refusals) == 1 and "uuid" in refused.refusals[0]
        assert len(smaller.refusals) == 1 and "narrower" in smaller.refusals[0]

    def test_a_length_limited_text_column_warns(self) -> None:
        relation = _counter()
        limited = PostgresType(
            base="varchar", is_array=False, not_null=True, formatted="character varying(64)"
        )

        warned = grade([relation], [diff_relation(relation, _catalog(relation, suffix=limited))])

        assert warned.refusals == () and "narrower" in warned.warnings[0]

    def test_a_column_found_not_null_where_forze_writes_null_is_refused(self) -> None:
        relation = _counter()
        not_null = PostgresType(base="timestamptz", is_array=False, not_null=True)

        findings = grade(
            [relation], [diff_relation(relation, _catalog(relation, expires_at=not_null))]
        )

        assert len(findings.refusals) == 1 and "NOT NULL" in findings.refusals[0]

    def test_a_narrower_column_that_refuses_null_is_refused(self) -> None:
        # A width warning must not hide a column that refuses the NULL forze writes.
        relation = BoundOwnedRelation(
            route="durable_run", schema="app", name="runs", shape=_run_shape()
        )
        narrow_not_null = PostgresType(
            base="varchar", is_array=False, not_null=True, formatted="character varying(36)"
        )

        findings = grade(
            [relation],
            [diff_relation(relation, _catalog(relation, idempotency_key=narrow_not_null))],
        )

        assert any(
            "idempotency_key" in line and "NOT NULL" in line for line in findings.refusals
        )
        assert any("narrower" in line for line in findings.warnings)

    def test_a_required_index_still_being_built_is_refused(self) -> None:
        # ON CONFLICT never infers an invalid index, so writes fail until the build ends.
        relation = BoundOwnedRelation(
            route="inboxes.events",
            schema="app",
            name="runs",
            shape=_run_shape(),
        )
        catalog = _catalog(relation)
        unique = _pkey(("idempotency_key",))
        catalog = attrs.evolve(
            catalog,
            indexes=(
                *catalog.indexes,
                attrs.evolve(
                    unique, name="building", primary=False, owns_constraint=False, valid=False
                ),
            ),
            building=True,
        )

        findings = grade([relation], [diff_relation(relation, catalog)])

        assert any("idempotency_key" in line for line in findings.refusals)

    def test_a_required_index_rebuilt_beside_a_valid_one_only_warns(self) -> None:
        # The valid index still serves ON CONFLICT while its replacement builds.
        relation = BoundOwnedRelation(
            route="durable_run", schema="app", name="runs", shape=_run_shape()
        )
        catalog = _catalog(relation)
        unique = attrs.evolve(
            _pkey(("idempotency_key",)), primary=False, owns_constraint=False
        )
        catalog = attrs.evolve(
            catalog,
            indexes=(
                *catalog.indexes,
                attrs.evolve(unique, name="serving"),
                attrs.evolve(unique, name="building", valid=False),
            ),
            building=True,
        )

        findings = grade([relation], [diff_relation(relation, catalog)])

        assert not any("idempotency_key" in line for line in findings.refusals)
        assert any("idempotency_key" in line for line in findings.warnings)

    def test_the_refused_findings_are_each_produced_by_the_diff(self) -> None:
        # A code the diff never sets would refuse nothing; keep the set and the diff in step.
        import inspect

        from forze_postgres.kernel.catalog.owned import diff

        source = inspect.getsource(diff)

        assert all(f'"{finding}"' in source for finding in REFUSED_FINDINGS)


class TestWiring:
    def test_the_lifecycle_module_registers_the_step_when_given_relations(self) -> None:
        client = PostgresClient()
        relations = owned_relations(PostgresDepsModule(client=client))

        steps = PostgresLifecycleModule(client=client, dsn="postgresql://x", owned_schema=relations)()

        assert [step.id for step in steps][-1] == "postgres_owned_schema_validate"
        assert "postgres_owned_schema_validate" not in [
            step.id for step in PostgresLifecycleModule(client=client, dsn="postgresql://x")()
        ]

    async def test_the_hook_refuses_with_one_error_naming_every_table(self) -> None:
        relations = owned_relations(
            PostgresDepsModule(
                client=PostgresClient(),
                inboxes={"events": PostgresInboxConfig(relation=("app", "inbox"))},
                counters={"orders": PostgresCounterConfig(relation=("app", "counters"))},
            )
        )

        introspector = _Introspector()
        ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: introspector}))

        with pytest.raises(CoreException) as caught:
            await PostgresOwnedSchemaValidationHook(relations=relations)(ctx)

        assert caught.value.code == "postgres_owned_schema_invalid"
        assert caught.value.details == {
            "refusals": [
                "app.inbox is missing (PostgresInboxStore).",
                "app.counters is missing (PostgresCounterAdapter).",
            ]
        }
        # Every relation is read in one read-only snapshot.
        assert introspector.client.options == [
            PostgresTransactionOptions(read_only=True, isolation="repeatable_read")
        ]

    async def test_without_its_partition_the_check_is_skipped_before_any_read(self) -> None:
        # A routed client has no connection to give before a tenant is bound.
        introspector = _Introspector(partition=False)
        ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: introspector}))

        await PostgresOwnedSchemaValidationHook(relations=_two_tables())(ctx)

        assert introspector.client.options == []

    async def test_an_error_other_than_a_missing_partition_is_raised(self) -> None:
        introspector = _Introspector(error=exc.infrastructure("catalog unreachable"))
        ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: introspector}))

        with pytest.raises(CoreException) as caught:
            await PostgresOwnedSchemaValidationHook(relations=_two_tables())(ctx)

        assert "catalog unreachable" in str(caught.value)

    async def test_skipped_routes_and_warnings_are_logged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from forze_postgres.execution.lifecycle import owned_schema

        logged: list[str] = []

        class _Logger:
            def warning(self, message: str, *args: object) -> None:
                logged.append(message % args)

        monkeypatch.setattr(owned_schema, "logger", _Logger())
        relations = owned_relations(
            PostgresDepsModule(
                client=PostgresClient(),
                counters={
                    "orders": PostgresCounterConfig(relation=("app", "counters")),
                    "scoped": PostgresCounterConfig(relation=lambda tenant: ("t", "counters")),
                },
            )
        )
        relation = relations.bound[0]
        catalog = _catalog(relation, expires_at=None)
        ctx = context_from_deps(
            Deps.plain({PostgresIntrospectorDepKey: _Introspector(catalog=catalog)})
        )

        await PostgresOwnedSchemaValidationHook(relations=relations)(ctx)

        assert any("skips route 'counters.scoped'" in line for line in logged)
        assert any("ADD COLUMN expires_at" in line for line in logged)


class TestTenants:
    def _deps(self, resolver: Any) -> PostgresDepsModule:
        return PostgresDepsModule(
            client=PostgresClient(),
            counters={"orders": PostgresCounterConfig(relation=resolver)},
        )

    async def test_each_tenant_binds_the_table_its_resolver_names(self) -> None:
        a, b = uuid4(), uuid4()
        deps = self._deps(lambda tenant: (f"t_{tenant.hex[:6]}", "counters"))

        relations = await resolve_owned_relations(deps, tenants=(a, b))

        assert relations.skipped == ()
        assert [(r.route, r.schema) for r in relations.bound] == [
            (f"counters.orders[{a}]", f"t_{a.hex[:6]}"),
            (f"counters.orders[{b}]", f"t_{b.hex[:6]}"),
        ]

    async def test_an_async_resolver_is_awaited(self) -> None:
        tenant = uuid4()

        async def resolver(tenant: UUID | None) -> tuple[str, str]:
            return ("tenant", "counters")

        relations = await resolve_owned_relations(self._deps(resolver), tenants=(tenant,))

        assert [(r.schema, r.name) for r in relations.bound] == [("tenant", "counters")]

    async def test_tenants_whose_resolver_names_one_table_bind_it_once(self) -> None:
        a, b = uuid4(), uuid4()
        deps = self._deps(lambda tenant: ("shared", "counters"))

        (relation,) = (await resolve_owned_relations(deps, tenants=(a, b))).bound

        assert relation.route == f"counters.orders[{a}], counters.orders[{b}]"

    async def test_with_no_tenant_a_resolver_is_skipped(self) -> None:
        deps = self._deps(lambda tenant: ("t", "counters"))

        relations = await resolve_owned_relations(deps, tenants=())

        assert relations.bound == () and relations.skipped == ("counters.orders",)

    def test_the_sync_binding_still_skips_a_resolver(self) -> None:
        relations = owned_relations(self._deps(lambda tenant: ("t", "counters")))

        assert attrs.evolve(relations).skipped == ("counters.orders",)
