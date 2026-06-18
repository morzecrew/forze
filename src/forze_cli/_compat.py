"""Compatibility helpers for the optional CLI extra."""

from __future__ import annotations

from importlib.util import find_spec

import typer

# ----------------------- #


def require_dst() -> None:
    """Exit with a clear message when the ``dst`` extra (needed by ``forze dst``) is missing.

    The CLI's ``dst`` commands generate inputs (polyfactory) and shrink (hypothesis); those
    live in the ``dst`` extra, not ``cli`` — so a user running ``forze dst …`` needs both.
    """

    if find_spec("polyfactory") is None or find_spec("hypothesis") is None:
        typer.echo(
            "The 'forze dst' commands require the DST extra. "
            "Install it with:  pip install 'forze[cli,dst]'",
            err=True,
        )
        raise typer.Exit(code=1)
