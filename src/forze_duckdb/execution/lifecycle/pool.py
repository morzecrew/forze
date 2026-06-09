"""DuckDB client lifecycle hooks and step factory."""

from typing import Any, Mapping, Sequence, cast, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle.builtin import ClientShutdownHook

from ...kernel.client import DuckDbClient, DuckDbConfig
from ..deps import DuckDbClientDepKey

# ----------------------- #

_DEFAULT_EXTENSIONS: tuple[str, ...] = ("httpfs",)

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DuckDbStartupHook(LifecycleHook):
    """Startup hook that opens and configures the DuckDB connection."""

    database: str = ":memory:"
    """DuckDB database path, or ``:memory:`` for an ephemeral database."""

    config: DuckDbConfig | None = attrs.field(default=None, repr=False)
    """Optional engine/executor configuration."""

    extensions: Sequence[str] = _DEFAULT_EXTENSIONS
    """Extensions to ``INSTALL`` + ``LOAD`` (e.g. ``httpfs``, ``iceberg``, ``delta``)."""

    secrets: Sequence[str] = attrs.field(default=(), repr=False)
    """Raw ``CREATE SECRET ...`` statements (object-storage credentials)."""

    sources: Mapping[str, str] | None = None
    """Optional ``name -> scan expression`` views registered at startup."""

    bootstrap_sql: Sequence[str] = ()
    """Additional raw statements run after extensions, secrets, and sources."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(DuckDbClient, ctx.deps.provide(DuckDbClientDepKey))
        await client.initialize(
            self.database,
            config=self.config,
            extensions=self.extensions,
            secrets=self.secrets,
            sources=self.sources,
            bootstrap_sql=self.bootstrap_sql,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DuckDbShutdownHook(ClientShutdownHook):
    """Shutdown hook that closes the DuckDB client."""

    dep_key: DepKey[Any] = attrs.field(default=DuckDbClientDepKey, init=False)


# ....................... #


def duckdb_lifecycle_step(
    name: str = "duckdb_lifecycle",
    *,
    database: str = ":memory:",
    config: DuckDbConfig | None = None,
    extensions: Sequence[str] = _DEFAULT_EXTENSIONS,
    secrets: Sequence[str] = (),
    sources: Mapping[str, str] | None = None,
    bootstrap_sql: Sequence[str] = (),
) -> LifecycleStep:
    """Build a lifecycle step for DuckDB client init and shutdown."""

    return LifecycleStep(
        id=name,
        startup=DuckDbStartupHook(
            database=database,
            config=config,
            extensions=extensions,
            secrets=secrets,
            sources=sources,
            bootstrap_sql=bootstrap_sql,
        ),
        shutdown=DuckDbShutdownHook(),
    )
