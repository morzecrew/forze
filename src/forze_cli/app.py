"""The ``forze`` CLI root.

A thin Typer surface over the framework's *introspectable* assets — the operation catalog
and the deterministic-simulation engine. It deliberately does not wrap development tasks
(tests, quality, docs); those live in the ``justfile``. Commands are grouped by subject
(``dst`` today; room for ``ops`` and more), and every command points at an object the user
exposes via a ``module:attribute`` import string.
"""

from __future__ import annotations

import typer

from forze._version import __version__
from forze_cli.dst import dst_app

# ----------------------- #

app = typer.Typer(
    no_args_is_help=True,
    help=(
        "Forze — exercise your domain operations under deterministic simulation.\n\n"
        "Point a command at a Simulation you expose (e.g. 'myapp.sim:simulation') to "
        "hunt concurrency and consistency bugs, or to inspect the reactive topology and "
        "the auto-derived scenario. Install the engine with the 'dst' extra."
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


app.add_typer(dst_app, name="dst")
