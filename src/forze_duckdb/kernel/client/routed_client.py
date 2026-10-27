"""DuckDB client that gives each tenant its own locked-down instance over its own lake warehouse.

One unnamed ``:memory:`` instance per tenant is the ``dedicated`` tier (RFC 0078 §1): the
instance holds one tenant-scoped catalog principal and only the credentials the catalog vends
to it. Every instance is built by the same fixed sequence and refuses to serve until it attests
that the sequence took:

.. code-block:: text

    connect(":memory:", allow_persistent_secrets, autoload/autoinstall/community off,
            temp_directory '', extension_directory)
    LOAD httpfs; LOAD iceberg                                -- never INSTALL
    SET GLOBAL enable_global_s3_configuration=false; s3_* = ''  -- no ambient credentials
    SET GLOBAL allowed_directories=[<storage_prefix>]
    SET GLOBAL enable_external_access=false                  -- no path outside the prefix
    CREATE SECRET (TYPE iceberg, client credentials)
    ATTACH <warehouse> AS lake (TYPE iceberg, vended_credentials, READ_ONLY)
    SET GLOBAL disabled_filesystems='LocalFileSystem,HTTPFileSystem'
    SET GLOBAL http_timeout, caches off
    SET GLOBAL lock_configuration=true
    attest

``GLOBAL`` is load-bearing, not style. Each statement runs on its own cursor, and DuckDB keeps
an extension option set without it (``enable_global_s3_configuration``, ``s3_*``,
``http_timeout``) on the cursor that set it, so every later statement would run with ambient
S3 credentials back on.
"""

from forze_duckdb._compat import require_duckdb

require_duckdb()

# ....................... #

import asyncio
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, cast
from uuid import UUID

import attrs
import duckdb
from pydantic import BaseModel, SecretStr

from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import JsonDict
from forze.base.primitives.fingerprint import build_routing_fingerprint

from .._logger import logger
from .client import DuckDbClient
from .port import DuckDbDynamicReadClientPort
from .value_objects import DuckDbConfig, DuckDbQueryResult

# ----------------------- #

LAKE_CATALOG = "lake"
"""The alias every tenant's warehouse is attached under. A wiring constant: plans never name it."""

_CATALOG_SECRET = "lake_cat"  # nosec B105 - the secret entry's name, not a secret
_EXTENSIONS = ("httpfs", "iceberg")
_DATABASES = frozenset({"memory", "system", "temp", LAKE_CATALOG})
_DISABLED_FILESYSTEMS = ("LocalFileSystem", "HTTPFileSystem")

# ....................... #


class LakeCatalogCredentials(BaseModel):
    """JSON shape of a tenant's catalog secret: its OAuth2 client-credentials principal.

    Never a static ``TOKEN``: a token disables refresh (RFC 0078 §3.1).
    """

    client_id: str
    """The principal's client id. Must be non-empty: the pinned ``iceberg`` build never
    refreshes a token issued for an empty one (EXECUTION-LOG D-030)."""

    client_secret: SecretStr


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class LakeTenantCatalog:
    """Where a tenant's warehouse lives, resolved per tenant by ``catalog_for_tenant``."""

    uri: str
    """The Iceberg REST catalog's base URI (e.g. ``http://lakekeeper:8181/catalog``)."""

    warehouse: str
    """The tenant's warehouse name."""

    oauth2_server_uri: str
    """The IdP's token endpoint."""

    oauth2_scope: str
    """Scope requested with the client credentials. Required: left out, the ``iceberg``
    extension sends its own Polaris default, which other IdPs refuse (EXECUTION-LOG D-024)."""

    storage_prefix: str
    """The warehouse's storage prefix (``s3://bucket/prefix/``), the one path tree the session
    may open (``allowed_directories``, EXECUTION-LOG D-027)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not (self.storage_prefix.startswith("s3://") and self.storage_prefix.endswith("/")):
            raise exc.configuration(
                "LakeTenantCatalog.storage_prefix must be an s3:// prefix ending in '/', or "
                "the allowlist also admits every sibling that shares its spelling.",
                code="duckdb_lake_catalog_invalid",
                details={"storage_prefix": self.storage_prefix},
            )


# ....................... #


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _connect_config(extension_directory: str | None) -> dict[str, Any]:
    """Options that only take effect at connect time."""

    config: dict[str, Any] = {
        "allow_persistent_secrets": False,
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
        "allow_community_extensions": False,
        "temp_directory": "",
    }

    if extension_directory is not None:
        config["extension_directory"] = extension_directory

    return config


def _session_statements(
    catalog: LakeTenantCatalog,
    creds: LakeCatalogCredentials,
    *,
    http_timeout_seconds: int,
) -> list[str]:
    """The hardening sequence after ``LOAD``, in order (RFC 0078 §3.4 steps 2-7)."""

    return [
        "SET GLOBAL enable_global_s3_configuration=false",
        "SET GLOBAL s3_access_key_id=''",
        "SET GLOBAL s3_secret_access_key=''",
        "SET GLOBAL s3_session_token=''",
        # Allowlist first: it cannot change once external access is off.
        f"SET GLOBAL allowed_directories=[{_literal(catalog.storage_prefix)}]",
        "SET GLOBAL enable_external_access=false",
        (
            f"CREATE SECRET {_CATALOG_SECRET} (TYPE iceberg, "
            f"CLIENT_ID {_literal(creds.client_id)}, "
            f"CLIENT_SECRET {_literal(creds.client_secret.get_secret_value())}, "
            f"OAUTH2_SERVER_URI {_literal(catalog.oauth2_server_uri)}, "
            f"OAUTH2_SCOPE {_literal(catalog.oauth2_scope)})"
        ),
        (
            f"ATTACH {_literal(catalog.warehouse)} AS {LAKE_CATALOG} (TYPE iceberg, "
            f"ENDPOINT {_literal(catalog.uri)}, SECRET {_CATALOG_SECRET}, "
            "ACCESS_DELEGATION_MODE 'vended_credentials', READ_ONLY)"
        ),
        f"SET GLOBAL disabled_filesystems='{','.join(_DISABLED_FILESYSTEMS)}'",
        f"SET GLOBAL http_timeout={http_timeout_seconds}",
        "SET GLOBAL enable_http_metadata_cache=false",
        "SET GLOBAL enable_external_file_cache=false",
        "SET GLOBAL lock_configuration=true",
    ]


def _expected_settings(
    catalog: LakeTenantCatalog,
    *,
    http_timeout_seconds: int,
    engine: DuckDbConfig,
    extension_directory: str | None,
) -> dict[str, str]:
    """What ``duckdb_settings()`` must report on a fresh cursor once the sequence ran.

    Two settings are checked another way or not at all. ``disabled_filesystems`` always reads
    back empty, so :func:`_attest` probes it by effect. ``memory_limit`` reads back reformatted
    (``1GB`` as ``953.6 MiB``) and bounds resources, not reach.
    """

    expected = {
        "allow_persistent_secrets": "false",
        "autoinstall_known_extensions": "false",
        "autoload_known_extensions": "false",
        "allow_community_extensions": "false",
        "allow_unredacted_secrets": "false",
        "temp_directory": "",
        "enable_global_s3_configuration": "false",
        "s3_access_key_id": "",
        "s3_secret_access_key": "",  # nosec B105 - the cleared setting the sequence asserts
        "s3_session_token": "",  # nosec B105 - likewise
        "allowed_directories": f"[{_literal(catalog.storage_prefix)}]",
        "enable_external_access": "false",
        "http_timeout": str(http_timeout_seconds),
        "enable_http_metadata_cache": "false",
        "enable_external_file_cache": "false",
        "lock_configuration": "true",
    }

    if engine.threads is not None:
        expected["threads"] = str(engine.threads)

    if extension_directory is not None:
        expected["extension_directory"] = extension_directory

    return expected


async def _filesystem_disabled(client: DuckDbClient, filesystem: str, path: str) -> bool:
    try:
        await client.run_query(f"SELECT 1 FROM read_text({_literal(path)})")  # nosec B608

    except duckdb.PermissionException as error:
        return f"{filesystem} has been disabled" in str(error)

    except duckdb.Error:
        return False

    return False


async def _attest(client: DuckDbClient, expected: dict[str, str]) -> list[str]:
    """Read the session back from a fresh cursor, the view a statement gets; list what is off."""

    problems: list[str] = []

    settings = await client.run_query(
        "SELECT name, value FROM duckdb_settings() WHERE list_contains($names, name)",
        {"names": sorted(expected)},
    )
    actual = {row["name"]: row["value"] for row in settings.rows}

    problems.extend(
        f"setting {name}" for name, value in sorted(expected.items()) if actual.get(name) != value
    )

    for filesystem, path in zip(
        _DISABLED_FILESYSTEMS, ("/forze-attest", "http://forze-attest.invalid/"), strict=True
    ):
        if not await _filesystem_disabled(client, filesystem, path):
            problems.append(f"filesystem {filesystem} enabled")

    secrets = await client.run_query("SELECT DISTINCT type FROM duckdb_secrets()")
    problems.extend(
        f"secret type {row['type']}" for row in secrets.rows if row["type"] != "iceberg"
    )

    databases = await client.run_query("SELECT database_name, readonly FROM duckdb_databases()")
    attached = {row["database_name"]: row["readonly"] for row in databases.rows}

    if set(attached) != _DATABASES:
        problems.append(f"databases {sorted(attached)}")

    elif attached[LAKE_CATALOG] is not True:
        problems.append(f"{LAKE_CATALOG} attached writable")

    return problems


# ....................... #


@attrs.define(slots=True, kw_only=True)
class RoutedDuckDbClient(
    StructuredSecretRoutedTenantClientBase[DuckDbClient],
    DuckDbDynamicReadClientPort,
):
    """Routes each call to a lazily built, locked-down :class:`DuckDbClient` for the tenant.

    The tenant's secret (:class:`LakeCatalogCredentials`) and its :class:`LakeTenantCatalog`
    are resolved on first use; the instance then runs the module's hardening sequence and an
    attestation, and serves nothing if a step or the attestation fails (both configuration
    errors, never a statement's). It has no field for static
    object-store credentials, raw secrets, sources or bootstrap statements. An instance holds
    the tenant's catalog principal for its life, and the S3 credentials vended for a read
    statement until that statement ends. An instance that fails to drop them is retired: it
    refuses statements, and the scope that saw it evicts it so the next one rebuilds.

    Register it under :data:`~forze_duckdb.execution.deps.DuckDbClientDepKey` and use
    :func:`~forze.application.execution.lifecycle.builtin.routed_client_lifecycle_step`, not
    :func:`~forze_duckdb.execution.lifecycle.duckdb_lifecycle_step`.

    An instance resolves a namespace once: a namespace it found missing stays missing, even
    after the grant or the namespace appears (EXECUTION-LOG D-026). Call :meth:`evict_tenant`
    after restoring a grant or creating a namespace, as after rotating credentials.

    An evicted or drained instance closes in the background: closing waits for its workers, and
    one stuck on a hung request takes about 120 s (D-028). Until then it parks a thread of the
    loop's default executor. :meth:`close` waits for every such teardown.
    """

    catalog_for_tenant: Callable[[UUID], LakeTenantCatalog]
    """Resolve where a tenant's warehouse lives."""

    engine: DuckDbConfig = attrs.field(
        factory=lambda: DuckDbConfig(threads=2, memory_limit="1GB", max_concurrent_queries=2),
    )
    """Per-instance engine and executor sizing."""

    max_concurrent_queries_total: int = 8
    """Statements running at once across every tenant instance in the process.

    Peak memory is about this times ``engine.memory_limit``. A statement whose catalog or store
    request hangs holds its slot for about 120 s whatever ``http_timeout`` says
    (EXECUTION-LOG D-028), so size this with hung requests counted in."""

    http_timeout: timedelta = timedelta(seconds=5)
    """``http_timeout`` for the store, in whole seconds. Must not exceed the smallest
    ``statement_timeout`` among the routes on this client (checked by the deps module)."""

    extension_directory: str | None = None
    """Directory ``httpfs`` and ``iceberg`` are loaded from, pre-installed in the image.
    ``None`` keeps DuckDB's default (``~/.duckdb/extensions``)."""

    guarded: bool = True
    """Guarded LRU eviction: an evicted instance closes after its last statement. ``False`` is
    refused."""

    creds_type: type[BaseModel] = attrs.field(default=LakeCatalogCredentials, init=False)
    backend: str = attrs.field(default="DuckDB lake catalog", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed DuckDB access",
        init=False,
    )

    _admission: asyncio.Semaphore = attrs.field(init=False)
    _teardowns: set[asyncio.Task[None]] = attrs.field(factory=set, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.guarded:
            raise exc.configuration(
                "RoutedDuckDbClient requires guarded eviction: an unguarded evict closes an "
                "instance under a running statement.",
                code="duckdb_routed_client_unguarded",
            )

        if self.max_concurrent_queries_total < 1:
            raise exc.configuration(
                "RoutedDuckDbClient.max_concurrent_queries_total must be >= 1.",
                code="duckdb_routed_client_invalid",
            )

        seconds = self.http_timeout.total_seconds()

        if seconds < 1 or not seconds.is_integer():
            raise exc.configuration(
                "RoutedDuckDbClient.http_timeout must be a whole number of seconds, at least 1: "
                "DuckDB takes it in seconds.",
                code="duckdb_routed_client_invalid",
                details={"http_timeout": str(self.http_timeout)},
            )

        super().__attrs_post_init__()
        self._admission = asyncio.Semaphore(self.max_concurrent_queries_total)

    # ....................... #

    def credential_fingerprint(self, creds: BaseModel) -> str:
        lake = cast(LakeCatalogCredentials, creds)

        return build_routing_fingerprint(public=[lake.client_id], secret=[lake.client_secret])

    def access_fingerprint_extra_parts(self, tenant_id: UUID) -> Sequence[str]:
        # Captured at build alongside the secret: a remapped catalog must rebuild the instance.
        catalog = self.catalog_for_tenant(tenant_id)

        return (
            catalog.uri,
            catalog.warehouse,
            catalog.oauth2_server_uri,
            catalog.oauth2_scope,
            catalog.storage_prefix,
        )

    # ....................... #

    async def _dispose_client(self, client: DuckDbClient) -> None:
        task = asyncio.get_running_loop().create_task(client.close())
        self._teardowns.add(task)
        task.add_done_callback(self._teardown_done)

    def _teardown_done(self, task: asyncio.Task[None]) -> None:
        self._teardowns.discard(task)

        if not task.cancelled() and task.exception() is not None:
            logger.warning("A DuckDB tenant instance failed to close", exc_info=task.exception())

    async def close(self) -> None:
        await super().close()
        await asyncio.gather(*self._teardowns, return_exceptions=True)

    # ....................... #

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: LakeCatalogCredentials,
    ) -> DuckDbClient:
        if not creds.client_id:
            raise exc.configuration(
                "The tenant's lake catalog secret has an empty client_id; the catalog token "
                "would never refresh.",
                code="duckdb_lake_client_id_empty",
                details={"tenant_id": str(tenant_id)},
            )

        catalog = self.catalog_for_tenant(tenant_id)
        http_timeout_seconds = int(self.http_timeout.total_seconds())
        client = DuckDbClient()

        try:
            try:
                await client.initialize(
                    ":memory:",
                    config=self.engine,
                    extensions=_EXTENSIONS,
                    install_extensions=False,
                    connect_config=_connect_config(self.extension_directory),
                    admission=self._admission,
                    drop_vended_secrets=True,
                )

                for statement in _session_statements(
                    catalog, creds, http_timeout_seconds=http_timeout_seconds
                ):
                    await client.run_command(statement)

            except CoreException:
                raise

            except Exception as error:
                # A failed step is the deployment's fault (an extension missing from the image,
                # a principal the catalog refuses), never that of the statement waiting on it.
                raise exc.configuration(
                    "The tenant's DuckDB lake session could not be set up; it serves nothing.",
                    code="duckdb_session_setup_failed",
                    details={"tenant_id": str(tenant_id), "error": str(error)},
                ) from error

            problems = await _attest(
                client,
                _expected_settings(
                    catalog,
                    http_timeout_seconds=http_timeout_seconds,
                    engine=self.engine,
                    extension_directory=self.extension_directory,
                ),
            )

            if problems:
                raise exc.configuration(
                    "The tenant's DuckDB lake session did not lock down as configured; it "
                    "serves nothing.",
                    code="duckdb_session_attestation_failed",
                    details={"tenant_id": str(tenant_id), "problems": problems},
                )

        except BaseException:
            await client.close()
            raise

        return client

    # ....................... #

    @asynccontextmanager
    async def client_scope(self, tenant_id: UUID | None = None) -> AsyncGenerator[DuckDbClient]:
        if tenant_id is None:
            tenant_id = self._require_tenant_id()

        inner: DuckDbClient | None = None

        try:
            # Zero-argument super() does not reach through asynccontextmanager in a slots class.
            base = StructuredSecretRoutedTenantClientBase.client_scope

            async with base(self, tenant_id) as inner:
                yield inner

        finally:
            # A failed vended-secret drop retired it (EXECUTION-LOG D-049). Evict it only while
            # the pool still holds it, so a later scope on the same instance spares its successor.
            if inner is not None and inner.retired and self._peek_client(tenant_id) is inner:
                await self.evict_tenant(tenant_id)

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        async with self.client_scope() as inner:
            return await inner.health()

    async def run_query(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
    ) -> DuckDbQueryResult:
        async with self.client_scope() as inner:
            return await inner.run_query(
                sql,
                params,
                limit=limit,
                offset=offset,
                max_rows=max_rows,
                timeout=timeout,
            )

    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> list[JsonDict]:
        async with self.client_scope() as inner:
            return await inner.run_query_all_pages(
                sql,
                params,
                max_rows=max_rows,
                timeout=timeout,
                fetch_batch_size=fetch_batch_size,
            )

    async def run_query_streamed(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        async with self.client_scope() as inner:
            async for batch in inner.run_query_streamed(
                sql,
                params,
                max_rows=max_rows,
                timeout=timeout,
                fetch_batch_size=fetch_batch_size,
            ):
                yield batch

    async def run_command(
        self,
        command: str,
        params: BaseModel | JsonDict | None = None,
        *,
        timeout: timedelta | None = None,
    ) -> None:
        async with self.client_scope() as inner:
            await inner.run_command(command, params, timeout=timeout)

    async def run_dynamic(
        self,
        statement: str,
        params: JsonDict | None = None,
        *,
        row_probe: int,
        timeout: timedelta,
        use: str | None = None,
    ) -> list[JsonDict]:
        async with self.client_scope() as inner:
            return await inner.run_dynamic(
                statement,
                params,
                row_probe=row_probe,
                timeout=timeout,
                use=use,
            )
