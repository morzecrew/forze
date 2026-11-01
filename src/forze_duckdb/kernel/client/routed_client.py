"""DuckDB client that gives each tenant its own locked-down instance over its own lake.

One unnamed ``:memory:`` instance per tenant is the ``dedicated`` tier (RFC 0078 §1): the
instance holds one tenant-scoped catalog principal and only the credentials that principal
reaches. A tenant's lake is an Iceberg warehouse behind a REST catalog
(:class:`LakeTenantCatalog`) or a DuckLake (:class:`DuckLakeTenantCatalog`). Every instance is
built by a fixed sequence for its kind and refuses to serve until it attests that the sequence
took. For an Iceberg warehouse:

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

For a DuckLake, whose catalog is a Postgres schema read with the tenant's own role:

.. code-block:: text

    connect(":memory:", allow_persistent_secrets, autoload/autoinstall/community off,
            temp_directory '', extension_directory)
    LOAD ducklake; LOAD postgres [; LOAD httpfs]             -- never INSTALL; httpfs for s3://
    [SET GLOBAL enable_global_s3_configuration=false; s3_* = '']
    SET GLOBAL ca_cert_file=<ca_cert_file>                   -- if configured
    SET GLOBAL allowed_directories=[<data path>]
    SET GLOBAL allowed_paths=[<ca_cert_file>]
    CREATE SECRET (TYPE postgres, host, port, database, role, password)   -- the unnamed one
    [CREATE SECRET lake_store (TYPE s3, ..., SCOPE <data path>)]
    ATTACH 'ducklake:postgres:' AS lake (DATA_PATH, METADATA_SCHEMA, READ_ONLY)
    SET GLOBAL enable_external_access=false
    [SET GLOBAL http_timeout; HTTP metadata cache off]
    SET GLOBAL enable_external_file_cache=false
    SET GLOBAL lock_configuration=true
    attest                                                   -- also the role's grants

It attaches before external access goes off, since Postgres refuses to attach after. The
unnamed secret keeps the password out of the attach string, which ``duckdb_databases()`` hands
any statement (EXECUTION-LOG D-099), and it is what lets persistent secrets stay off: an attach
that finds no secret in memory looks in their storage, and fails with it off (D-098).
"""

from forze_duckdb._compat import require_duckdb

require_duckdb()

# ....................... #

import asyncio
import os
import re
import ssl
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import timedelta
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
from ..credentials import S3Credentials, S3SecretPayload
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

_DUCKLAKE_EXTENSIONS = ("ducklake", "postgres")
_STORE_SECRET = "lake_store"  # nosec B105 - the secret entry's name, not a secret
_METADATA_DATABASE = f"__ducklake_metadata_{LAKE_CATALOG}"
"""Where DuckLake attaches the tenant's metadata database: addressable by name, though
``duckdb_databases()`` does not list it."""
_METADATA_PATH = "postgres:"
"""The metadata path the lake attaches with: the unnamed secret supplies every part of it."""
_ATTACHED_FILES = ("", ".wal", ".wal.checkpoint", ".wal.recovery")
"""What ``ATTACH`` appends to ``allowed_paths`` for the metadata path, read as a file in the
working directory. A later ``SET`` does not remove them (EXECUTION-LOG D-100)."""

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


class DuckLakeCatalogCredentials(BaseModel):
    """JSON shape of a tenant's DuckLake secret: its metadata role and, for an ``s3://`` data
    path, its object-store key."""

    user: str
    """The tenant's role on the metadata database. Grant it ``SELECT`` on its metadata schema's
    tables and nothing else: the session attests that it can write none of them."""

    password: SecretStr

    store: S3SecretPayload | None = None
    """The object-store key for an ``s3://`` data path, ``None`` for a local one. Scope it to the
    data path, read-only: the engine admits writes under the path (EXECUTION-LOG D-103)."""


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class DuckLakeTenantCatalog:
    """Where a tenant's DuckLake lives, resolved per tenant by ``catalog_for_tenant``: a metadata
    schema in a Postgres database, read with the tenant's role, and a data path."""

    host: str
    """The metadata database's host."""

    port: int = 5432
    """The metadata database's port."""

    database: str
    """The metadata database."""

    metadata_schema: str
    """The tenant's metadata schema (DuckLake's ``METADATA_SCHEMA``)."""

    data_path: str
    """The tenant's data: an absolute local directory, normalized, or an ``s3://`` prefix, either
    ending in ``/``. The one path tree the session may open (``allowed_directories``), and the
    ``DATA_PATH`` the lake attaches with, whatever its metadata records."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        path = self.data_path
        local = os.path.isabs(path) and os.path.normpath(path) + "/" == path

        if not (path.endswith("/") and (local or re.fullmatch(r"s3://[^/]+/.*", path))):
            raise exc.configuration(
                "DuckLakeTenantCatalog.data_path must be an absolute, normalized local directory "
                "or an s3:// prefix, ending in '/': the allowlist matches it by spelling, and "
                "without the slash it also admits every sibling that shares it.",
                code="duckdb_lake_catalog_invalid",
                details={"data_path": path},
            )

        if not (self.host and self.database and self.metadata_schema):
            raise exc.configuration(
                "DuckLakeTenantCatalog needs a host, a database and a metadata schema.",
                code="duckdb_lake_catalog_invalid",
            )

        if not 0 < self.port < 65536:
            raise exc.configuration(
                "DuckLakeTenantCatalog.port must be a TCP port.",
                code="duckdb_lake_catalog_invalid",
                details={"port": self.port},
            )

    # ....................... #

    @property
    def on_s3(self) -> bool:
        """Whether the data lives in object storage rather than a local directory."""

        return self.data_path.startswith("s3://")


# ....................... #


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _redacted(text: str, *secrets: str) -> str:
    """*text* without DuckDB's statement echo, and without any of *secrets* in either spelling.

    Setup embeds the tenant's secrets in ``CREATE SECRET``, and an error from that statement can
    quote them.
    """

    text = _STATEMENT_ECHO.sub("", text)

    for secret in filter(None, secrets):
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
        # OpenSSL parses it, as curl will: a marker alone or a truncated body loads nothing.
        ssl.create_default_context(cafile=path)

    except ssl.SSLError as error:
        raise exc.configuration(
            "RoutedDuckDbClient.ca_cert_file holds no PEM certificate OpenSSL can load.",
            code="duckdb_routed_client_invalid",
            details={"ca_cert_file": path},
        ) from error

    except OSError as error:
        raise exc.configuration(
            "RoutedDuckDbClient.ca_cert_file cannot be read.",
            code="duckdb_routed_client_invalid",
            details={"ca_cert_file": path, "error": str(error)},
        ) from error


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
    client: DuckDbClient,
    expected: dict[str, object],
    outside: Sequence[str],
    secret_types: frozenset[str] = frozenset({"iceberg"}),
) -> list[str]:
    """Read the session back from a fresh cursor, the view a statement gets; list what is off.

    *secret_types* are the types of secret the sequence creates; any other is a problem.
    """

    actual = {name: await _setting(client, name) for name in sorted(expected)}
    problems = [f"setting {name}" for name, value in actual.items() if value != expected[name]]

    # Only with external access off: on, each probe would really go out (public AWS, the IdP).
    for path in outside if actual.get("enable_external_access") is False else ():
        read = f"SELECT 1 FROM read_text({_literal(path)})"  # nosec B608

        if not await _refused(client, read, duckdb.PermissionException, "by configuration"):
            problems.append(f"path {path} not refused")

    secrets = await client.run_query("SELECT DISTINCT type FROM duckdb_secrets()")
    problems.extend(
        f"secret type {row['type']}" for row in secrets.rows if row["type"] not in secret_types
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


def _ducklake_session_statements(
    catalog: DuckLakeTenantCatalog,
    creds: DuckLakeCatalogCredentials,
    *,
    http_timeout_seconds: int,
    ca_cert_file: str | None,
) -> list[str]:
    """The DuckLake hardening sequence after ``LOAD``, in order (module docstring)."""

    store: list[str] = []
    timeouts: list[str] = []

    if creds.store is not None:
        store = [
            "SET GLOBAL enable_global_s3_configuration=false",
            "SET GLOBAL s3_access_key_id=''",
            "SET GLOBAL s3_secret_access_key=''",
            "SET GLOBAL s3_session_token=''",
        ]
        timeouts = [
            f"SET GLOBAL http_timeout={http_timeout_seconds}",
            "SET GLOBAL enable_http_metadata_cache=false",
        ]

    ca = [] if ca_cert_file is None else [f"SET GLOBAL ca_cert_file={_literal(ca_cert_file)}"]
    paths = [] if ca_cert_file is None else [ca_cert_file]
    secret = (
        "CREATE SECRET (TYPE postgres, "
        f"HOST {_literal(catalog.host)}, PORT {int(catalog.port)}, "
        f"DATABASE {_literal(catalog.database)}, USER {_literal(creds.user)}, "
        f"PASSWORD {_literal(creds.password.get_secret_value())})"
    )

    if creds.store is not None:
        rendered = S3Credentials(name=_STORE_SECRET, scope=catalog.data_path, inline=creds.store)
        store_secret = [rendered.render(creds.store)]

    else:
        store_secret = []

    return [
        *store,
        *ca,
        # Allowlist first: it cannot change once external access is off.
        f"SET GLOBAL allowed_directories=[{_literal(catalog.data_path)}]",
        f"SET GLOBAL allowed_paths=[{', '.join(map(_literal, paths))}]",
        secret,
        *store_secret,
        # Before external access goes off: Postgres refuses to attach after it.
        (
            f"ATTACH {_literal('ducklake:' + _METADATA_PATH)} AS {LAKE_CATALOG} ("
            f"DATA_PATH {_literal(catalog.data_path)}, "
            f"METADATA_SCHEMA {_literal(catalog.metadata_schema)}, READ_ONLY)"
        ),
        "SET GLOBAL enable_external_access=false",
        *timeouts,
        "SET GLOBAL enable_external_file_cache=false",
        "SET GLOBAL lock_configuration=true",
    ]


def _ducklake_expected_settings(
    catalog: DuckLakeTenantCatalog,
    *,
    on_s3: bool,
    working_directory: str,
    http_timeout_seconds: int,
    engine: DuckDbConfig,
    extension_directory: str | None,
    ca_cert_file: str | None,
) -> dict[str, object]:
    """What ``current_setting()`` must return on a fresh cursor once the DuckLake sequence ran.

    ``allowed_paths`` holds, beside the CA bundle, what ``ATTACH`` appended for the metadata
    path in *working_directory* (D-100).
    """

    attached = [os.path.join(working_directory, _METADATA_PATH + s) for s in _ATTACHED_FILES]
    expected: dict[str, object] = {
        "allow_persistent_secrets": False,
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
        "allow_community_extensions": False,
        "allow_unredacted_secrets": False,
        "temp_directory": "",
        "allowed_directories": [catalog.data_path],
        "allowed_paths": sorted([*([] if ca_cert_file is None else [ca_cert_file]), *attached]),
        "enable_external_access": False,
        "enable_external_file_cache": False,
        "lock_configuration": True,
    }

    if on_s3:
        expected |= {
            "enable_global_s3_configuration": False,
            "s3_access_key_id": "",
            "s3_secret_access_key": "",  # nosec B105 - the cleared setting the sequence asserts
            "s3_session_token": "",  # nosec B105 - likewise
            "http_timeout": http_timeout_seconds,
            "enable_http_metadata_cache": False,
        }

    if engine.threads is not None:
        expected["threads"] = engine.threads

    if extension_directory is not None:
        expected["extension_directory"] = extension_directory

    if ca_cert_file is not None:
        expected["ca_cert_file"] = ca_cert_file

    return expected


def _ducklake_outside(catalog: DuckLakeTenantCatalog, ca_cert_file: str | None) -> list[str]:
    """Paths next to the DuckLake allowlist that a statement must not open: a local path, a URL,
    a sibling of the data path that shares its spelling, and a file beside the CA bundle.

    A local data path also gets a dot-segment probe, which the engine refuses there. Under an
    ``s3://`` prefix it does not: the path goes to the store, whose key must refuse it.
    """

    paths = [
        "/forze-attest",
        "http://forze-attest.invalid/",
        f"{catalog.data_path.rstrip('/')}-forze-attest/x",
    ]

    if not catalog.on_s3:
        paths.append(f"{catalog.data_path}../forze-attest/x")

    if ca_cert_file is not None:
        paths.append(os.path.join(os.path.dirname(ca_cert_file), "forze-attest"))

    return paths


async def _ducklake_problems(client: DuckDbClient, catalog: DuckLakeTenantCatalog) -> list[str]:
    """What the generic attestation cannot see on a DuckLake session.

    The attach string must carry nothing a statement could read back but the bare metadata
    path. The metadata role must be unable to write its schema, and unable to read another
    lake's metadata, which can hold that lake's inlined rows: the role's grants are what bound
    the metadata database, which a statement can query through ``postgres_query``
    (EXECUTION-LOG D-102).
    """

    problems: list[str] = []
    databases = await client.run_query(
        f"SELECT path FROM duckdb_databases() WHERE database_name = {_literal(LAKE_CATALOG)}"  # nosec B608
    )

    if [row["path"] for row in databases.rows] != [_METADATA_PATH]:
        problems.append(f"{LAKE_CATALOG} attached with more than the metadata path")

    schema = _literal(catalog.metadata_schema)
    grants = (
        "SELECT (SELECT count(*) FROM pg_catalog.pg_class c "  # nosec B608 - schema is a literal
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE n.nspname = {schema} AND ("
        "(c.relkind IN ('r', 'p', 'v', 'm', 'f') "
        "AND has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE')) "
        "OR (c.relkind = 'S' AND has_sequence_privilege(c.oid, 'USAGE, UPDATE')))) AS writable, "
        f"has_schema_privilege({schema}, 'CREATE') AS creates, "
        "(SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user) AS super, "
        "(SELECT count(DISTINCT n.nspname) FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE n.nspname <> {schema} AND c.relkind IN ('r', 'p', 'v', 'm', 'f') "
        "AND has_schema_privilege(n.oid, 'USAGE') AND has_table_privilege(c.oid, 'SELECT') "
        "AND EXISTS (SELECT 1 FROM pg_catalog.pg_class m "
        "WHERE m.relnamespace = n.oid AND m.relname = 'ducklake_metadata')) AS others"
    )

    try:
        result = await client.run_query(
            f"SELECT * FROM postgres_query({_literal(_METADATA_DATABASE)}, {_literal(grants)})"  # nosec B608
        )

    except duckdb.Error:
        return [*problems, "metadata role's grants unreadable"]

    row = result.rows[0]

    if row["writable"]:
        problems.append(f"metadata role can write {row['writable']} tables")

    if row["creates"]:
        problems.append("metadata role can create in its schema")

    if row["super"]:
        problems.append("metadata role is a superuser")

    if row["others"]:
        problems.append(f"metadata role can read {row['others']} other lakes' metadata")

    return problems


# ....................... #


@attrs.define(slots=True, kw_only=True)
class RoutedDuckDbClient(
    StructuredSecretRoutedTenantClientBase[DuckDbClient],
    DuckDbDynamicReadClientPort,
):
    """Routes each call to a lazily built, locked-down :class:`DuckDbClient` for the tenant.

    The tenant's catalog and its secret are resolved on first use: a :class:`LakeTenantCatalog`
    with :class:`LakeCatalogCredentials`, or a :class:`DuckLakeTenantCatalog` with
    :class:`DuckLakeCatalogCredentials`. The instance then runs the module's hardening sequence
    for that kind and an attestation, and serves nothing if a step or the attestation fails
    (both configuration errors, never a statement's). It has no field for static
    object-store credentials, raw secrets, sources or bootstrap statements. An instance holds
    the tenant's catalog principal for its life: for Iceberg, with the S3 credentials vended for
    a read statement until that statement ends; for DuckLake, with the store key in its secret.
    An instance that fails to drop vended credentials is retired: it refuses new statements, and
    the scope that saw it, or else the tenant's next scope, evicts it so a fresh one is built.

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

    catalog_for_tenant: Callable[[UUID], LakeTenantCatalog | DuckLakeTenantCatalog]
    """Resolve where a tenant's lake lives. Its kind picks the shape of the tenant's secret."""

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
    """``http_timeout`` for the store, in whole seconds; a DuckLake on a local path sets none.
    Must not exceed the smallest ``statement_timeout`` among the routes on this client (checked
    by the deps module)."""

    extension_directory: str | None = None
    """Directory the extensions are loaded from, pre-installed in the image: ``httpfs`` and
    ``iceberg`` with ``avro``, which ``iceberg`` loads itself (EXECUTION-LOG D-071), for an
    Iceberg warehouse; ``ducklake`` and ``postgres``, with ``httpfs`` for an ``s3://`` data path,
    for a DuckLake. ``None`` keeps DuckDB's default (``~/.duckdb/extensions``)."""

    ca_cert_file: str | None = attrs.field(
        default=None, converter=attrs.converters.optional(os.path.realpath)
    )
    """CA bundle for HTTPS to the catalog, the IdP and the store, set as ``ca_cert_file`` and
    allow-listed as one path; on a DuckLake, only for an ``s3://`` data path. Required on DuckDB 2 when any of them is HTTPS: it reads the
    bundle through the allowlist, and without it the session fails to build. Resolved to its
    real path at construction, the form DuckDB reports back, and refused unless it holds a PEM
    certificate. Every statement on the instance can then read the file: analytics SQL by path,
    while dynamic reads refuse paths. ``None`` sets nothing (EXECUTION-LOG D-063, D-075)."""

    guarded: bool = True
    """Guarded LRU eviction: an evicted instance closes after its last statement. ``False`` is
    refused."""

    # The Iceberg shape; :meth:`resolve_credentials` picks the tenant's from its catalog.
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

    async def resolve_credentials(self, tenant_id: UUID) -> BaseModel:
        catalog = self.catalog_for_tenant(tenant_id)
        model = (
            DuckLakeCatalogCredentials
            if isinstance(catalog, DuckLakeTenantCatalog)
            else LakeCatalogCredentials
        )

        return await self._resolver.resolve_structured(model, tenant_id)

    def credential_fingerprint(self, creds: BaseModel) -> str:
        if isinstance(creds, DuckLakeCatalogCredentials):
            store = creds.store
            public = [creds.user]
            secret: list[SecretStr | None] = [creds.password]

            if store is not None:
                public += [
                    store.access_key_id,
                    *(str(v) for v in (store.region, store.endpoint, store.url_style)),
                    str(store.use_ssl),
                ]
                secret += [store.secret_access_key, store.session_token]

            return build_routing_fingerprint(public=public, secret=secret)

        lake = cast(LakeCatalogCredentials, creds)

        return build_routing_fingerprint(public=[lake.client_id], secret=[lake.client_secret])

    def access_fingerprint_extra_parts(self, tenant_id: UUID) -> Sequence[str]:
        # Captured at build alongside the secret: a remapped catalog must rebuild the instance.
        catalog = self.catalog_for_tenant(tenant_id)

        if isinstance(catalog, DuckLakeTenantCatalog):
            return (
                catalog.host,
                str(catalog.port),
                catalog.database,
                catalog.metadata_schema,
                catalog.data_path,
            )

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
        creds: LakeCatalogCredentials | DuckLakeCatalogCredentials,
    ) -> DuckDbClient:
        catalog = self.catalog_for_tenant(tenant_id)

        if isinstance(catalog, DuckLakeTenantCatalog) != isinstance(
            creds, DuckLakeCatalogCredentials
        ):
            # The catalog changed kind between resolving the secret and now: build nothing.
            raise exc.configuration(
                "The tenant's lake catalog and its secret are of different kinds.",
                code="duckdb_lake_catalog_kind_changed",
                details={"tenant_id": str(tenant_id)},
            )

        if isinstance(catalog, DuckLakeTenantCatalog):
            return await self._initialize_ducklake(
                tenant_id, catalog, cast(DuckLakeCatalogCredentials, creds)
            )

        creds = cast(LakeCatalogCredentials, creds)

        if not creds.client_id:
            raise exc.configuration(
                "The tenant's lake catalog secret has an empty client_id; the catalog token "
                "would never refresh.",
                code="duckdb_lake_client_id_empty",
                details={"tenant_id": str(tenant_id)},
            )

        http_timeout_seconds = int(self.http_timeout.total_seconds())
        ca_cert_file = self.ca_cert_file

        async def attest(client: DuckDbClient) -> list[str]:
            return await _attest(
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

        return await self._build_session(
            tenant_id,
            extensions=_EXTENSIONS,
            connect_config=_connect_config(self.extension_directory),
            statements=lambda: _session_statements(
                catalog,
                creds,
                http_timeout_seconds=http_timeout_seconds,
                ca_cert_file=ca_cert_file,
            ),
            attest=attest,
            drop_vended_secrets=True,
            hidden=[creds.client_secret.get_secret_value()],
        )

    async def _initialize_ducklake(
        self,
        tenant_id: UUID,
        catalog: DuckLakeTenantCatalog,
        creds: DuckLakeCatalogCredentials,
    ) -> DuckDbClient:
        if catalog.on_s3 != (creds.store is not None):
            raise exc.configuration(
                "A DuckLake on an s3:// data path needs the store key in the tenant's secret, "
                "and one on a local path takes none.",
                code="duckdb_lake_store_credentials_mismatch",
                details={"tenant_id": str(tenant_id), "data_path": catalog.data_path},
            )

        if not (creds.user and creds.password.get_secret_value()):
            # Left empty, libpq would fall back to the OS user or to PGPASSWORD and ~/.pgpass.
            raise exc.configuration(
                "The tenant's DuckLake secret needs a metadata role and its password.",
                code="duckdb_lake_role_credentials_empty",
                details={"tenant_id": str(tenant_id)},
            )

        http_timeout_seconds = int(self.http_timeout.total_seconds())
        # Only the store needs it, and every statement on the instance could read it.
        ca_cert_file = self.ca_cert_file if catalog.on_s3 else None
        working_directory = os.getcwd()
        hidden = [creds.password.get_secret_value()]

        if creds.store is not None:
            hidden.append(creds.store.secret_access_key.get_secret_value())

            if creds.store.session_token is not None:
                hidden.append(creds.store.session_token.get_secret_value())

        async def attest(client: DuckDbClient) -> list[str]:
            problems = await _attest(
                client,
                _ducklake_expected_settings(
                    catalog,
                    on_s3=catalog.on_s3,
                    working_directory=working_directory,
                    http_timeout_seconds=http_timeout_seconds,
                    engine=self.engine,
                    extension_directory=self.extension_directory,
                    ca_cert_file=ca_cert_file,
                ),
                _ducklake_outside(catalog, ca_cert_file),
                frozenset({"postgres", "s3"} if catalog.on_s3 else {"postgres"}),
            )

            return problems + await _ducklake_problems(client, catalog)

        return await self._build_session(
            tenant_id,
            extensions=(*_DUCKLAKE_EXTENSIONS, *(("httpfs",) if catalog.on_s3 else ())),
            connect_config=_connect_config(self.extension_directory),
            statements=lambda: _ducklake_session_statements(
                catalog,
                creds,
                http_timeout_seconds=http_timeout_seconds,
                ca_cert_file=ca_cert_file,
            ),
            attest=attest,
            drop_vended_secrets=False,
            hidden=hidden,
        )

    async def _build_session(
        self,
        tenant_id: UUID,
        *,
        extensions: Sequence[str],
        connect_config: dict[str, Any],
        statements: Callable[[], list[str]],
        attest: Callable[[DuckDbClient], Awaitable[list[str]]],
        drop_vended_secrets: bool,
        hidden: Sequence[str],
    ) -> DuckDbClient:
        """Open an instance, run *statements* on it and *attest* it; serve it only if every step
        ran and the attestation found nothing. *hidden* are the secrets the sequence embeds."""

        client = DuckDbClient()

        try:
            try:
                await client.initialize(
                    ":memory:",
                    config=self.engine,
                    extensions=extensions,
                    install_extensions=False,
                    connect_config=connect_config,
                    admission=self._admission,
                    drop_vended_secrets=drop_vended_secrets,
                )

                for statement in statements():
                    await client.run_command(statement)

                problems = await attest(client)

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
                    details={"tenant_id": str(tenant_id), "error": _redacted(str(error), *hidden)},
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
