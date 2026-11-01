"""Per-tenant DuckLakes on one Postgres, for the routed DuckLake reads (issue #800).

Each tenant gets what the application would provision, outside forze:

* a metadata schema in one Postgres database, owned by the tenant's **writer** role;
* a **reader** role granted ``SELECT`` on that schema's tables and nothing else — the role a
  ``RoutedDuckDbClient`` instance reads with;
* a data path: a directory of its own under one root, or a prefix of its own in one bucket.

The writer builds ``gold.orders`` and ``silver.raw`` through DuckDB, read-write, as the build
would. Every password is generated per stack and never written to a file.
"""

from __future__ import annotations

import secrets
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import psycopg
from psycopg import sql
from testcontainers.postgres import PostgresContainer

# ----------------------- #

POSTGRES_IMAGE = "ghcr.io/morzecrew/postgres:18"
DATABASE = "lakemeta"
BUCKET = "ducklake"
EXTENSIONS = ("ducklake", "postgres", "httpfs")


@dataclass(frozen=True, kw_only=True)
class S3Store:
    """An S3-compatible store the stack may put a tenant's data in."""

    endpoint: str
    access_key: str
    secret_key: str

    def secret_sql(self, scope: str) -> str:
        return (
            f"CREATE OR REPLACE SECRET forze_store (TYPE s3, KEY_ID '{self.access_key}', "
            f"SECRET '{self.secret_key}', REGION 'us-east-1', ENDPOINT '{self.endpoint}', "
            f"URL_STYLE 'path', USE_SSL false, SCOPE '{scope}')"
        )


@dataclass(frozen=True, kw_only=True)
class DuckLakeTenant:
    """One tenant's DuckLake and the two roles that reach it."""

    name: str
    schema: str
    reader: str
    reader_password: str = field(repr=False)
    writer: str
    writer_password: str = field(repr=False)
    data_path: str
    rows: int
    """Rows in ``gold.orders``, each tagged with the tenant's name."""


@dataclass(frozen=True, kw_only=True)
class DuckLakeStack:
    """The Postgres every tenant's metadata lives in, and the tenants."""

    host: str
    port: int
    admin_dsn: str = field(repr=False)
    root: Path
    store: S3Store | None
    tenants: dict[str, DuckLakeTenant]

    def admin(self, statement: str, *params: Any) -> list[tuple[Any, ...]]:
        """Run *statement* as the superuser on the metadata database."""

        with psycopg.connect(self.admin_dsn, dbname=DATABASE, autocommit=True) as conn:
            cur = conn.execute(statement, params or None)
            return cur.fetchall() if cur.description else []

    @contextmanager
    def writer(self, tenant: DuckLakeTenant) -> Iterator[duckdb.DuckDBPyConnection]:
        """The tenant's lake attached read-write with its writer role, as the build has it."""

        con = duckdb.connect(":memory:")

        try:
            con.execute("LOAD ducklake; LOAD postgres; LOAD httpfs")

            if tenant.data_path.startswith("s3://") and self.store is not None:
                con.execute(self.store.secret_sql(tenant.data_path))

            dsn = (
                f"host={self.host} port={self.port} dbname={DATABASE} "
                f"user={tenant.writer} password={tenant.writer_password}"
            )
            con.execute(
                f"ATTACH 'ducklake:postgres:{dsn}' AS lake "
                f"(DATA_PATH '{tenant.data_path}', METADATA_SCHEMA '{tenant.schema}')"
            )
            yield con

        finally:
            con.close()

        # A table the writer created is the writer's: the reader sees it once granted.
        self.admin(
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}")
            .format(sql.Identifier(tenant.schema), sql.Identifier(tenant.reader))
            .as_string()
        )


def install_extensions() -> None:
    """Install the extensions a session loads, with autoinstall off, into the default directory."""

    con = duckdb.connect(":memory:")

    try:
        for name in EXTENSIONS:
            con.execute(f"INSTALL {name}")

    finally:
        con.close()


def _provision(stack: DuckLakeStack, name: str, data_path: str, rows: int) -> DuckLakeTenant:
    tenant = DuckLakeTenant(
        name=name,
        schema=f"{name}_meta",
        reader=f"{name}_reader",
        reader_password=secrets.token_hex(16),
        writer=f"{name}_writer",
        writer_password=secrets.token_hex(16),
        data_path=data_path,
        rows=rows,
    )
    schema, reader, writer = (
        sql.Identifier(x) for x in (tenant.schema, tenant.reader, tenant.writer)
    )

    for statement in (
        sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            writer, sql.Literal(tenant.writer_password)
        ),
        sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            reader, sql.Literal(tenant.reader_password)
        ),
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}, {}").format(
            sql.Identifier(DATABASE), writer, reader
        ),
        sql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(schema, writer),
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema, reader),
    ):
        stack.admin(statement.as_string())

    if not data_path.startswith("s3://"):
        Path(data_path).mkdir(parents=True)

    with stack.writer(tenant) as con:
        con.execute("CREATE SCHEMA lake.gold; CREATE SCHEMA lake.silver")
        con.execute(
            f"CREATE TABLE lake.gold.orders AS SELECT i AS id, '{name}' AS tenant "  # nosec B608
            f"FROM range({rows}) t(i)"
        )
        con.execute("CREATE TABLE lake.silver.raw AS SELECT i AS id FROM range(3) t(i)")

    return tenant


@contextmanager
def ducklake_stack(*, store: S3Store | None = None) -> Iterator[DuckLakeStack]:
    """Start Postgres, then provision tenants ``a`` (5 rows), ``b`` (7), ``c`` (2) and ``d`` (1)
    on local data paths, and ``s`` (4) on an ``s3://`` prefix when *store* is given.

    ``c`` and ``d`` hold deployment mistakes the attestation must catch: ``c``'s reader can
    insert into one metadata table; ``d``'s can insert through a view in its own schema, and
    read tenant ``a``'s metadata.
    """

    install_extensions()

    with (
        PostgresContainer(image=POSTGRES_IMAGE, driver="psycopg") as pg,
        tempfile.TemporaryDirectory() as root,
    ):
        host, port = pg.get_container_host_ip(), int(pg.get_exposed_port(5432))
        admin_dsn = (
            f"host={host} port={port} user={pg.username} password={pg.password} dbname={pg.dbname}"
        )

        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DATABASE)))

        stack = DuckLakeStack(
            host=host, port=port, admin_dsn=admin_dsn, root=Path(root), store=store, tenants={}
        )
        layout = {"a": 5, "b": 7, "c": 2, "d": 1}

        for name, rows in layout.items():
            stack.tenants[name] = _provision(stack, name, f"{root}/{name}/", rows)

        if store is not None:
            stack.tenants["s"] = _provision(stack, "s", f"s3://{BUCKET}/s/", 4)

        a, c, d = (stack.tenants[name] for name in "acd")
        stack.admin(
            sql.SQL("GRANT INSERT ON {}.ducklake_tag TO {}")
            .format(sql.Identifier(c.schema), sql.Identifier(c.reader))
            .as_string()
        )

        for statement in (
            sql.SQL("CREATE VIEW {}.tags AS SELECT * FROM {}.ducklake_tag").format(
                sql.Identifier(d.schema), sql.Identifier(d.schema)
            ),
            sql.SQL("GRANT INSERT ON {}.tags TO {}").format(
                sql.Identifier(d.schema), sql.Identifier(d.reader)
            ),
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(a.schema), sql.Identifier(d.reader)
            ),
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                sql.Identifier(a.schema), sql.Identifier(d.reader)
            ),
        ):
            stack.admin(statement.as_string())

        try:
            yield stack

        finally:
            shutil.rmtree(root, ignore_errors=True)
