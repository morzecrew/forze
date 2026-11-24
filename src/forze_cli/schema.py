"""``forze schema``: the SQL for the tables forze owns (RFC 0080).

``sql`` prints the ``CREATE`` statements a fresh install needs and reads no database. ``diff``
reads a live catalog and prints the forward statements that bring it to the installed forze
version. Forze never runs either: the output goes into the application's next migration file,
applied by its own tool (RFC 0080 row 1).

Exit codes are a contract for CI:

* ``0``: in sync, nothing printed;
* ``10``: statements were printed;
* ``11``: something needs a person (a refusal or a warning), which wins over ``10``;
* ``2``: a usage error, such as the DSN passed as a flag;
* ``75``: the database could not be reached or read, safe to retry;
* ``78``: a configuration error: ``--deps`` does not load or name a ``PostgresDepsModule``,
  the DSN is unset or does not parse, the role lacks access, or the ``postgres`` extra is
  missing;
* ``1``: an internal error.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Final, NoReturn

import typer

from forze._version import __version__
from forze_cli._compat import require_postgres
from forze_cli.loader import load_object

if TYPE_CHECKING:
    from forze.base.exceptions import CoreException
    from forze_postgres import OwnedRelations, PostgresDepsModule
    from forze_postgres.kernel.catalog.owned import BoundOwnedRelation, RelationDelta

# ----------------------- #

EXIT_INTERNAL: Final = 1
EXIT_USAGE: Final = 2
EXIT_DELTA: Final = 10
EXIT_REFUSAL: Final = 11
EXIT_TEMPFAIL: Final = 75
EXIT_CONFIG: Final = 78

DSN_ENV: Final = "FORZE_SCHEMA_DSN"
"""The only place ``diff`` reads its DSN from: a flag would leak it to shell history and
``ps``."""

_ROLE_NOTE: Final = (
    f"{DSN_ENV} must name a read-only role: USAGE on the schemas, catalog access, and no "
    "write privilege. That role is what keeps the diff from writing; its READ ONLY "
    "transaction only guards against forze's own mistakes, since a session can switch one "
    "back."
)

_DEPS_HELP: Final = (
    "The PostgresDepsModule, or a sequence holding exactly one, as 'module:attribute'."
)

schema_app = typer.Typer(
    no_args_is_help=True,
    help=(
        "The SQL for the tables forze owns. Forze prints it and never runs it: apply it "
        "with your migration tool. Covers the durable run, step and schedule tables."
    ),
)

# ....................... #


def _fail(message: str, code: int) -> NoReturn:
    typer.echo(message, err=True)
    raise typer.Exit(code=code)


def _load_deps(ref: str) -> PostgresDepsModule:
    from forze_postgres import PostgresDepsModule

    try:
        obj = load_object(ref)

    except (ImportError, AttributeError, ValueError) as error:
        _fail(f"Cannot load --deps {ref!r}: {error}", EXIT_CONFIG)

    if isinstance(obj, PostgresDepsModule):
        return obj

    if isinstance(obj, Sequence) and not isinstance(obj, str):
        found = [item for item in obj if isinstance(item, PostgresDepsModule)]  # pyright: ignore[reportUnknownVariableType]

        if len(found) == 1:  # pyright: ignore[reportUnknownArgumentType]
            return found[0]  # pyright: ignore[reportUnknownVariableType]

        _fail(
            f"--deps {ref!r} holds {len(found)} PostgresDepsModule objects; "  # pyright: ignore[reportUnknownArgumentType]
            "point at exactly one.",
            EXIT_CONFIG,
        )

    _fail(f"--deps {ref!r} is not a PostgresDepsModule.", EXIT_CONFIG)


def _relations(ref: str) -> OwnedRelations:
    from forze_postgres import owned_relations

    relations = owned_relations(_load_deps(ref))

    for route in relations.skipped:
        typer.echo(
            f"warning: skipped {route}: its relation is a tenant-scoped resolver, so no single "
            "table is known.",
            err=True,
        )

    return relations


# ....................... #


@schema_app.command("sql")
def sql_command(
    deps: str = typer.Option(..., "--deps", metavar="MODULE:ATTR", help=_DEPS_HELP),
) -> None:
    """Print the CREATE statements for every owned table the deps module binds.

    Reads no database. The output is a fresh install's first forze migration.
    """

    require_postgres()

    from forze_postgres.kernel.catalog.owned import render_sql

    relations = _relations(deps)
    typer.echo(render_sql(relations.bound, version=__version__), nl=False)


# ....................... #


async def _read_deltas(
    dsn: str,
    relations: Sequence[BoundOwnedRelation],
) -> list[RelationDelta]:
    from forze_postgres import PostgresClient, PostgresConfig
    from forze_postgres.kernel.catalog import PostgresIntrospector
    from forze_postgres.kernel.catalog.owned import diff_relation, read_catalog
    from forze_postgres.kernel.client.value_objects import PostgresTransactionOptions

    await _probe(dsn)
    client = PostgresClient()
    await client.initialize(dsn, config=PostgresConfig(min_size=0, max_size=1, pool_headroom=0))

    try:
        introspector = PostgresIntrospector(client=client)
        options = PostgresTransactionOptions(read_only=True, isolation="repeatable_read")

        # One snapshot for every relation, so the output describes one moment.
        async with client.transaction(options=options):
            return [
                diff_relation(relation, await read_catalog(introspector, relation))
                for relation in relations
            ]

    finally:
        await client.close()


async def _probe(dsn: str) -> None:
    """Connect once, so a bad DSN fails at once and says why rather than as a pool timeout.

    A connection string psycopg cannot parse is reported without its text, which may hold the
    password. A refused connection keeps libpq's message, which names the host and user only.
    """

    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    try:
        conninfo_to_dict(dsn)

    except psycopg.ProgrammingError:
        _fail(f"{DSN_ENV} is not a valid Postgres connection string.", EXIT_CONFIG)

    try:
        connection = await psycopg.AsyncConnection.connect(dsn, connect_timeout=10)

    except psycopg.OperationalError as error:
        _fail(f"Cannot connect with {DSN_ENV}: {str(error).strip()}", EXIT_TEMPFAIL)

    await connection.close()


def _exit_for(error: CoreException) -> int:
    from forze.base.exceptions import ExceptionKind, exception_egress_policy

    kind = error.kind

    if kind in (ExceptionKind.AUTHENTICATION, ExceptionKind.AUTHORIZATION):
        return EXIT_CONFIG

    if exception_egress_policy(kind).retryable or kind is ExceptionKind.INFRASTRUCTURE:
        return EXIT_TEMPFAIL

    if kind is ExceptionKind.CONFIGURATION:
        return EXIT_CONFIG

    return EXIT_INTERNAL


@schema_app.command(
    "diff", help=f"Print the SQL that brings a live database to this forze version.\n\n{_ROLE_NOTE}"
)
def diff_command(
    deps: str = typer.Option(..., "--deps", metavar="MODULE:ATTR", help=_DEPS_HELP),
    check: bool = typer.Option(
        False,
        "--check",
        help="Print nothing; exit 0 when in sync, 10 on a delta, 11 when a person is needed.",
    ),
    dsn: str | None = typer.Option(None, "--dsn", hidden=True),
) -> None:
    if dsn is not None:
        _fail(
            f"Pass the DSN in {DSN_ENV}, never as a flag: a flag leaks it to shell history and ps.",
            EXIT_USAGE,
        )

    require_postgres()

    from forze.base.exceptions import CoreException
    from forze_postgres.kernel.catalog.owned import diff_outcome, render_diff

    url = os.environ.get(DSN_ENV)

    if not url:
        _fail(f"{DSN_ENV} is not set. {_ROLE_NOTE}", EXIT_CONFIG)

    relations = _relations(deps)

    try:
        deltas = asyncio.run(_read_deltas(url, relations.bound))

    except CoreException as error:
        _fail(f"Cannot read the catalog: {error.summary}", _exit_for(error))

    if not check:
        typer.echo(render_diff(deltas, version=__version__), nl=False)

    outcome = diff_outcome(deltas)

    if outcome == "refusal":
        raise typer.Exit(code=EXIT_REFUSAL)

    if outcome == "delta":
        raise typer.Exit(code=EXIT_DELTA)
