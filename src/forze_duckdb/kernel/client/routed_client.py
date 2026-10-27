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
    SET GLOBAL ca_cert_file=<ca_cert_file>                   -- if configured
    SET GLOBAL allowed_directories=[<storage_prefix>, <catalog uri>/]
    SET GLOBAL allowed_paths=[<token endpoint>, <ca_cert_file>]
    SET GLOBAL enable_external_access=false                  -- nothing outside the allowlist
    CREATE SECRET (TYPE iceberg, client credentials)
    ATTACH <warehouse> AS lake (TYPE iceberg, vended_credentials, READ_ONLY)
    SET GLOBAL http_timeout, caches off
    SET GLOBAL lock_configuration=true
    attest

``GLOBAL`` is load-bearing, not style. Each statement runs on its own cursor, and DuckDB keeps
an extension option set without it (``enable_global_s3_configuration``, ``s3_*``,
``http_timeout``) on the cursor that set it, so every later statement would run with ambient
S3 credentials back on.

The allowlist is the one in-engine bound on what a statement can open, local or remote. It
names the catalog and the IdP because DuckDB 2 checks the ``iceberg`` extension's own requests
against it, and the CA bundle because DuckDB 2 reads that bundle through the same check for
every HTTPS request. No filesystem is disabled: DuckDB 2 cannot read the bundle with
``LocalFileSystem`` off, nor reach the catalog with ``HTTPFileSystem`` off. The same profile
runs on DuckDB 1.5 (RFC 0078 EXECUTION-LOG D-062, D-063).
"""

from forze_duckdb._compat import require_duckdb

require_duckdb()

# ....................... #

import asyncio
import os
import re
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit
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
_STATEMENT_ECHO = re.compile(r"\n\s*LINE \d+:.*", re.DOTALL)
"""DuckDB's echo of a failing statement: a window of its text after ``LINE n:``, cut at either
end, so a secret in it can show only in part."""
_PEM_CERTIFICATE = "-----BEGIN CERTIFICATE-----"

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
    """The Iceberg REST catalog's base URI (e.g. ``http://lakekeeper:8181/catalog``). The
    session allow-lists the tree under it, so a statement can also send unauthenticated GETs
    there (EXECUTION-LOG D-064)."""

    warehouse: str
    """The tenant's warehouse name."""

    oauth2_server_uri: str
    """The IdP's token endpoint, allow-listed as one exact path."""

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

        for name in ("uri", "oauth2_server_uri"):
            value = getattr(self, name)
            parts = urlsplit(value)

            if parts.scheme not in ("http", "https") or not parts.hostname:
                raise exc.configuration(
                    f"LakeTenantCatalog.{name} must be an http(s) URL: the session allow-lists "
                    "it, and anything else would widen what a statement can open.",
                    code="duckdb_lake_catalog_invalid",
                    details={name: value},
                )


# ....................... #


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _redacted(text: str, secret: str) -> str:
    """*text* without DuckDB's statement echo, and without *secret* in either spelling.

    Setup embeds the tenant's client secret in ``CREATE SECRET``, and an error from that
    statement can quote it.
    """

    text = _STATEMENT_ECHO.sub("", text)

    if not secret:
        return text

    # The SQL-escaped spelling first: the raw one can be a substring of it.
    for spelling in (secret.replace("'", "''"), secret):
        text = text.replace(spelling, "***")

    return text


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


def _allowlist(catalog: LakeTenantCatalog, ca_cert_file: str | None) -> tuple[list[str], list[str]]:
    """``allowed_directories`` and ``allowed_paths``, sorted as DuckDB reports them back.

    The allowlist bounds what the engine opens; it is not what keeps credentials in. A statement
    that may open the catalog's tree could carry data out in a URL, if it could read a secret.
    It cannot, and that is the exfiltration boundary: ``duckdb_secrets()`` stays redacted
    (``allow_unredacted_secrets`` off), the ``s3_*`` options are cleared, no secret persists, and
    ``lock_configuration`` keeps all of it so. Step 8 attests each one (EXECUTION-LOG D-075).
    """

    directories = [catalog.storage_prefix, catalog.uri.rstrip("/") + "/"]
    paths = [catalog.oauth2_server_uri]

    if ca_cert_file is not None:
        paths.append(ca_cert_file)

    return sorted(directories), sorted(paths)


def _session_statements(
    catalog: LakeTenantCatalog,
    creds: LakeCatalogCredentials,
    *,
    http_timeout_seconds: int,
    ca_cert_file: str | None,
) -> list[str]:
    """The hardening sequence after ``LOAD``, in order (RFC 0078 §3.4 steps 2-7)."""

    directories, paths = _allowlist(catalog, ca_cert_file)
    ca = [] if ca_cert_file is None else [f"SET GLOBAL ca_cert_file={_literal(ca_cert_file)}"]

    return [
        "SET GLOBAL enable_global_s3_configuration=false",
        "SET GLOBAL s3_access_key_id=''",
        "SET GLOBAL s3_secret_access_key=''",
        "SET GLOBAL s3_session_token=''",
        *ca,
        # Allowlist first: it cannot change once external access is off.
        f"SET GLOBAL allowed_directories=[{', '.join(map(_literal, directories))}]",
        f"SET GLOBAL allowed_paths=[{', '.join(map(_literal, paths))}]",
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
    ca_cert_file: str | None,
) -> dict[str, object]:
    """What ``current_setting()`` must return on a fresh cursor once the sequence ran.

    ``current_setting()``, not ``duckdb_settings()``: DuckDB 2 no longer lists the ``s3_*``
    options or ``extension_directory`` there (EXECUTION-LOG D-060). ``memory_limit`` is left
    out: it reads back reformatted (``1GB`` as ``953.6 MiB``) and bounds resources, not reach.
    """

    directories, paths = _allowlist(catalog, ca_cert_file)
    expected: dict[str, object] = {
        "allow_persistent_secrets": False,
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
        "allow_community_extensions": False,
        "allow_unredacted_secrets": False,
        "temp_directory": "",
        "enable_global_s3_configuration": False,
        "s3_access_key_id": "",
        "s3_secret_access_key": "",  # nosec B105 - the cleared setting the sequence asserts
        "s3_session_token": "",  # nosec B105 - likewise
        "allowed_directories": directories,
        "allowed_paths": paths,
        "enable_external_access": False,
        "http_timeout": http_timeout_seconds,
        "enable_http_metadata_cache": False,
        "enable_external_file_cache": False,
        "lock_configuration": True,
    }

    if engine.threads is not None:
        expected["threads"] = engine.threads

    if extension_directory is not None:
        expected["extension_directory"] = extension_directory

    if ca_cert_file is not None:
        expected["ca_cert_file"] = ca_cert_file

    return expected


def _require_pem(path: str) -> None:
    """Refuse a ``ca_cert_file`` that holds no PEM certificate: DuckDB 2 hands it to curl for
    every request, plain HTTP included, and a wrong file fails each one obscurely (D-063)."""

    try:
        text = Path(path).read_text(encoding="utf-8", errors="replace")

    except OSError as error:
        raise exc.configuration(
            "RoutedDuckDbClient.ca_cert_file cannot be read.",
            code="duckdb_routed_client_invalid",
            details={"ca_cert_file": path, "error": str(error)},
        ) from error

    if _PEM_CERTIFICATE not in text:
        raise exc.configuration(
            f"RoutedDuckDbClient.ca_cert_file holds no PEM certificate ({_PEM_CERTIFICATE}).",
            code="duckdb_routed_client_invalid",
            details={"ca_cert_file": path},
        )


def _outside_the_allowlist(catalog: LakeTenantCatalog, ca_cert_file: str | None) -> list[str]:
    """Paths next to each allowlist entry that a statement must not open (EXECUTION-LOG D-075).

    A local path and an unlisted URL; the token endpoint with a path or a query added, since a
    path entry must match exactly; a sibling of the prefix and of the catalog tree that shares
    their spelling; and a file beside the CA bundle. No dot-segment probe: the engine resolves
    none, and the classification refuses every path instead (D-073).
    """

    paths = [
        "/forze-attest",
        "http://forze-attest.invalid/",
        f"{catalog.oauth2_server_uri}/forze-attest",
        f"{catalog.oauth2_server_uri}?forze-attest",
        f"{catalog.storage_prefix.rstrip('/')}-forze-attest/x",
        f"{catalog.uri.rstrip('/')}-forze-attest/",
    ]

    if ca_cert_file is not None:
        paths.append(os.path.join(os.path.dirname(ca_cert_file), "forze-attest"))

    return paths


async def _refused(client: DuckDbClient, sql: str, error: type[duckdb.Error], phrase: str) -> bool:
    """Whether *sql* fails with *error*. The class decides; a message without *phrase* is only
    logged, since DuckDB rewords its messages between releases (it did in 2.0, D-060)."""

    try:
        await client.run_query(sql)

    except error as refusal:
        if phrase not in str(refusal):
            logger.warning(
                "DuckDB refused an attestation probe in unexpected words", exc_info=refusal
            )

        return True

    except duckdb.Error:
        return False

    return False


async def _setting(client: DuckDbClient, name: str) -> object:
    """``current_setting(name)`` on a fresh cursor, lists sorted; ``None`` if it cannot be read.

    One query per setting: an extension option the session never set is not in a fresh
    cursor's catalog, and reading it raises rather than returning ``NULL``.
    """

    try:
        result = await client.run_query(f"SELECT current_setting({_literal(name)}) AS v")  # nosec B608

    except duckdb.Error:
        return None

    value = result.rows[0]["v"]

    return sorted(value) if isinstance(value, list) else value  # pyright: ignore[reportUnknownArgumentType]


async def _attest(
    client: DuckDbClient, expected: dict[str, object], outside: Sequence[str]
) -> list[str]:
    """Read the session back from a fresh cursor, the view a statement gets; list what is off."""

    actual = {name: await _setting(client, name) for name in sorted(expected)}
    problems = [f"setting {name}" for name, value in actual.items() if value != expected[name]]

    # Only with external access off: on, each probe would really go out (public AWS, the IdP).
    for path in outside if actual.get("enable_external_access") is False else ():
        read = f"SELECT 1 FROM read_text({_literal(path)})"  # nosec B608

        if not await _refused(client, read, duckdb.PermissionException, "by configuration"):
            problems.append(f"path {path} not refused")

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

    # By effect too, and only once the flag says read-only: refused locally, it sends nothing.
    elif not await _refused(
        client,
        f"CREATE SCHEMA {LAKE_CATALOG}.forze_attest",
        duckdb.InvalidInputException,
        "read-only mode",
    ):
        problems.append(f"{LAKE_CATALOG} accepted a write")

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
    refuses new statements, and the scope that saw it, or else the tenant's next scope, evicts
    it so a fresh one is built.

    Register it under :data:`~forze_duckdb.execution.deps.DuckDbClientDepKey` and use
    :func:`~forze.application.execution.lifecycle.builtin.routed_client_lifecycle_step`, not
    :func:`~forze_duckdb.execution.lifecycle.duckdb_lifecycle_step`.

    An instance resolves a namespace once: a namespace it found missing stays missing, even
    after the grant or the namespace appears (EXECUTION-LOG D-026). Call :meth:`evict_tenant`
    after restoring a grant or creating a namespace, as after rotating credentials.

    An evicted or drained instance closes in the background: closing waits for its workers, and
    one stuck on a hung request takes about 120 s on DuckDB 1.x (D-028), or ``http_timeout``
    times one plus ``http_retries`` (default 3) on DuckDB 2 (D-057). Until then it parks a
    thread of the loop's default executor. :meth:`close` waits for every such teardown.
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
    request hangs holds its slot for about 120 s on DuckDB 1.x, whatever ``http_timeout`` says
    (EXECUTION-LOG D-028), and for ``http_timeout`` times one plus ``http_retries`` on DuckDB 2
    (D-057). Size this with hung requests counted in."""

    http_timeout: timedelta = timedelta(seconds=5)
    """``http_timeout`` for the store, in whole seconds. Must not exceed the smallest
    ``statement_timeout`` among the routes on this client (checked by the deps module)."""

    extension_directory: str | None = None
    """Directory ``httpfs`` and ``iceberg`` are loaded from, pre-installed in the image with
    ``avro``, which ``iceberg`` loads itself (EXECUTION-LOG D-071). ``None`` keeps DuckDB's
    default (``~/.duckdb/extensions``)."""

    ca_cert_file: str | None = attrs.field(
        default=None, converter=attrs.converters.optional(os.path.realpath)
    )
    """CA bundle for HTTPS to the catalog, the IdP and the store, set as ``ca_cert_file`` and
    allow-listed as one path. Required on DuckDB 2 when any of them is HTTPS: it reads the
    bundle through the allowlist, and without it the session fails to build. Resolved to its
    real path at construction, the form DuckDB reports back, and refused unless it holds a PEM
    certificate. Every statement on the instance can then read the file: analytics SQL by path,
    while dynamic reads refuse paths. ``None`` sets nothing (EXECUTION-LOG D-063, D-075)."""

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

        if self.ca_cert_file is not None:
            _require_pem(self.ca_cert_file)

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
        ca_cert_file = self.ca_cert_file
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
                    catalog,
                    creds,
                    http_timeout_seconds=http_timeout_seconds,
                    ca_cert_file=ca_cert_file,
                ):
                    await client.run_command(statement)

                problems = await _attest(
                    client,
                    _expected_settings(
                        catalog,
                        http_timeout_seconds=http_timeout_seconds,
                        engine=self.engine,
                        extension_directory=self.extension_directory,
                        ca_cert_file=ca_cert_file,
                    ),
                    _outside_the_allowlist(catalog, ca_cert_file),
                )

            except CoreException:
                raise

            except Exception as error:
                # A failed step is the deployment's fault (an extension missing from the image,
                # a principal the catalog refuses), never that of the statement waiting on it.
                # Not chained: the engine's error can quote the secret, and a server-error log
                # prints the cause. Its text survives, redacted, in the details.
                raise exc.configuration(
                    "The tenant's DuckDB lake session could not be set up; it serves nothing.",
                    code="duckdb_session_setup_failed",
                    details={
                        "tenant_id": str(tenant_id),
                        "error": _redacted(str(error), creds.client_secret.get_secret_value()),
                    },
                ) from None

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

        # A worker that outlived its timed-out scope may have retired the pooled instance since
        # (EXECUTION-LOG D-049): rebuild before serving, so no caller starts on a retired one.
        await self._evict_if_retired(tenant_id, self._peek_client(tenant_id))
        inner: DuckDbClient | None = None

        try:
            # Zero-argument super() does not reach through asynccontextmanager in a slots class.
            base = StructuredSecretRoutedTenantClientBase.client_scope

            async with base(self, tenant_id) as inner:
                yield inner

        finally:
            await self._evict_if_retired(tenant_id, inner)

    async def _evict_if_retired(self, tenant_id: UUID, inner: DuckDbClient | None) -> None:
        # Only while the pool still holds it, so a later scope on the same instance spares its
        # successor.
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
