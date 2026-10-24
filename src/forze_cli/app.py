"""The ``forze`` CLI root.

A thin Typer surface over the framework's *introspectable* assets — the operation catalog
and the deterministic-simulation engine. It deliberately does not wrap development tasks
(tests, quality, docs); those live in the ``justfile``. Commands are grouped by subject
(``ops``, ``dst``, ``mock``), and every command points at an object the user exposes via a
``module:attribute`` import string.
"""

from __future__ import annotations

import typer

from forze._version import __version__
from forze_cli.dst import dst_app
from forze_cli.mock import mock_app_cli
from forze_cli.ops.commands import ops_cli

# ----------------------- #

app = typer.Typer(
    no_args_is_help=True,
    # Typer's default pretty exceptions render a rich locals panel for every frame
    # of an unhandled error — the raw values, straight past the framework's log
    # scrubbing. A CLI command driving application code (a simulation, an operation)
    # has OAuth secrets, DSNs and tokens sitting in those locals; one crash prints
    # them to the terminal and to whatever captures it. Keep the pretty traceback,
    # never the locals.
    pretty_exceptions_show_locals=False,
    help=(
        "Forze — run, simulate and serve your application's operations.\n\n"
        "Point a command at an object you expose as 'module:attribute': an OpsApp for "
        "'ops' (call operations, governed as over HTTP), a Simulation for 'dst' (hunt "
        "concurrency and consistency bugs; needs the 'dst' extra), a MockApp for 'mock' "
        "(serve on in-memory backends; needs the 'mock-server' extra)."
    ),
)


def _show_version(value: bool) -> None:
    if value:
        typer.echo(f"forze {__version__}")
        raise typer.Exit()


@app.callback()
def root(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_show_version,
        is_eager=True,
        help="Show the installed Forze version and exit.",
    ),
) -> None:
    """Forze command-line interface."""

    del version


app.add_typer(ops_cli, name="ops")
app.add_typer(dst_app, name="dst")
app.add_typer(mock_app_cli, name="mock")
