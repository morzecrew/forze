"""The owned-table startup check's grading, its wiring, and tenant binding (RFC 0080 P2b)."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.execution import Deps
from forze.base.exceptions import CoreException
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

    def test_a_type_of_another_family_is_refused_and_a_narrower_one_warns(self) -> None:
        relation = _counter()
        other = PostgresType(base="uuid", is_array=False, not_null=True)
        narrow = PostgresType(base="int4", is_array=False, not_null=True, formatted="integer")

        refused = grade([relation], [diff_relation(relation, _catalog(relation, suffix=other))])
        warned = grade([relation], [diff_relation(relation, _catalog(relation, value=narrow))])

        assert len(refused.refusals) == 1 and "uuid" in refused.refusals[0]
        assert warned.refusals == () and "narrower" in warned.warnings[0]

    def test_a_column_found_not_null_where_forze_writes_null_is_refused(self) -> None:
        relation = _counter()
        not_null = PostgresType(base="timestamptz", is_array=False, not_null=True)

        findings = grade(
            [relation], [diff_relation(relation, _catalog(relation, expires_at=not_null))]
        )

        assert len(findings.refusals) == 1 and "NOT NULL" in findings.refusals[0]

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

        class _Missing:
            async def find_relation(self, **_: Any) -> None:
                return None

        ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: _Missing()}))

        with pytest.raises(CoreException) as caught:
            await PostgresOwnedSchemaValidationHook(relations=relations)(ctx)

        assert caught.value.code == "postgres_owned_schema_invalid"
        assert caught.value.details == {
            "refusals": [
                "app.inbox is missing (PostgresInboxStore).",
                "app.counters is missing (PostgresCounterAdapter).",
            ]
        }


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
