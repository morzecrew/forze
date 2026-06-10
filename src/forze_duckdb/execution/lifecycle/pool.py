"""DuckDB client lifecycle hooks and step factory."""

from typing import Any, Mapping, Sequence, cast, final

import attrs

from pydantic import BaseModel

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.secrets import (
    SecretsDepKey,
    SecretsPort,
    resolve_structured,
)
from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle.builtin import ClientShutdownHook
from forze.base.exceptions import exc

from ...kernel.client import DuckDbClient, DuckDbConfig
from ...kernel.credentials import ObjectStoreCredentials
from ...kernel.sources import DuckDbSource, compile_source, source_extensions
from ..deps import DuckDbClientDepKey

# ----------------------- #

_DEFAULT_EXTENSIONS: tuple[str, ...] = ("httpfs",)

# ....................... #


def _merge_extensions(*groups: Sequence[str]) -> tuple[str, ...]:
    """Union extension names across *groups*, de-duplicated and order-preserving."""

    seen: dict[str, None] = {}

    for group in groups:
        for ext in group:
            seen.setdefault(ext, None)

    return tuple(seen)


# ....................... #


async def _resolve_secret_statements(
    ctx: ExecutionContext,
    object_stores: Sequence[ObjectStoreCredentials],
) -> tuple[str, ...]:
    """Compile each object-store credential to a ``CREATE SECRET`` statement.

    Credentials carrying a ``secret_ref`` are resolved through the wired
    :class:`SecretsPort`; the port is only required when at least one credential needs it.
    """

    statements: list[str] = []
    secrets_port: SecretsPort | None = None

    for store in object_stores:
        if store.secret_ref is not None:
            if secrets_port is None:
                secrets_port = ctx.deps.provide(SecretsDepKey)

            payload: BaseModel = await resolve_structured(
                secrets_port,
                store.secret_ref,
                store.payload_model(),
            )

        else:
            inline = store.inline_payload()

            if inline is None:
                raise exc.configuration(
                    f"Object-store credential {store.name!r} has neither "
                    "an inline payload nor a secret_ref.",
                )

            payload = inline

        statements.append(store.render(payload))

    return tuple(statements)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DuckDbStartupHook(LifecycleHook):
    """Startup hook that opens and configures the DuckDB connection.

    Typed :class:`DuckDbSource` views and :class:`ObjectStoreCredentials` are compiled here:
    sources become scan-expression views, credentials become ``CREATE SECRET`` statements
    (resolved through the secrets backend when given a ``secret_ref``), and the extensions
    each one needs are merged into the load set automatically.
    """

    database: str = ":memory:"
    """DuckDB database path, or ``:memory:`` for an ephemeral database."""

    config: DuckDbConfig | None = attrs.field(default=None, repr=False)
    """Optional engine/executor configuration."""

    extensions: Sequence[str] = _DEFAULT_EXTENSIONS
    """Extensions to ``INSTALL`` + ``LOAD`` (merged with those required by sources/credentials)."""

    secrets: Sequence[str] = attrs.field(default=(), repr=False)
    """Raw ``CREATE SECRET ...`` statements (escape hatch alongside ``object_stores``)."""

    object_stores: Sequence[ObjectStoreCredentials] = attrs.field(default=(), repr=False)
    """Typed object-storage credentials, resolved via the secrets backend when referenced."""

    sources: Mapping[str, str | DuckDbSource] | None = None
    """``name -> source`` views registered at startup; a value may be a typed
    :class:`DuckDbSource` or a raw scan-expression string (escape hatch)."""

    bootstrap_sql: Sequence[str] = ()
    """Additional raw statements run after extensions, secrets, and sources."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(DuckDbClient, ctx.deps.provide(DuckDbClientDepKey))

        derived_exts: list[str] = []
        compiled_sources: dict[str, str] | None = None

        if self.sources is not None:
            compiled_sources = {}

            for name, source in self.sources.items():
                compiled_sources[name] = compile_source(source)
                derived_exts.extend(source_extensions(source))

        secret_statements = list(self.secrets)

        if self.object_stores:
            secret_statements.extend(
                await _resolve_secret_statements(ctx, self.object_stores)
            )

            for store in self.object_stores:
                derived_exts.extend(store.required_extensions())

        extensions = _merge_extensions(self.extensions, derived_exts)

        await client.initialize(
            self.database,
            config=self.config,
            extensions=extensions,
            secrets=tuple(secret_statements),
            sources=compiled_sources,
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
    object_stores: Sequence[ObjectStoreCredentials] = (),
    sources: Mapping[str, str | DuckDbSource] | None = None,
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
            object_stores=object_stores,
            sources=sources,
            bootstrap_sql=bootstrap_sql,
        ),
        shutdown=DuckDbShutdownHook(),
    )
