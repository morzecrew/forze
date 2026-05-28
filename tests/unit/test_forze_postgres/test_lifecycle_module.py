"""Tests for PostgresLifecycleModule and lifecycle step ordering."""

from forze.application.execution.lifecycle import LifecyclePlan
from forze_postgres import (
    PostgresClient,
    PostgresLifecycleModule,
    PostgresSearchConfig,
    postgres_catalog_warmup_lifecycle_step,
    postgres_lifecycle_step,
)

# ----------------------- #


def _pgroonga_search() -> PostgresSearchConfig:
    return PostgresSearchConfig(
        engine="pgroonga",
        index=("public", "idx"),
        read=("public", "src"),
    )


class TestPostgresLifecycleModule:
    def test_build_orders_pool_before_warmup(self) -> None:
        module = PostgresLifecycleModule(
            client=PostgresClient(),
            dsn="postgresql://u:p@localhost/db",
            searches={"main": _pgroonga_search()},
        )
        built = LifecyclePlan.from_modules(module).build()

        assert [s.id for s in built.steps] == [
            "postgres_lifecycle",
            "postgres_catalog_warmup",
        ]

    def test_capability_metadata_reorders_plain_steps(self) -> None:
        pool = postgres_lifecycle_step(dsn="postgresql://u:p@localhost/db")
        warmup = postgres_catalog_warmup_lifecycle_step(
            searches={"main": _pgroonga_search()},
        )
        built = LifecyclePlan.from_steps(warmup, pool).build()

        assert [s.id for s in built.steps] == [
            "postgres_lifecycle",
            "postgres_catalog_warmup",
        ]
