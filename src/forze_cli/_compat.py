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


# ....................... #


def require_mock_server() -> None:
    """Exit with a clear message when the ``mock-server`` extra is missing.

    Only what *serving* needs. A ``MockApp`` with **no seed plan** never imports
    ``forze_mock.seeding``, so refusing to serve it over the generator would be a gate on the
    wrong thing. Any plan at all does import it — fixtures-only included, because the gate
    there is on the module rather than on the call — and ``require_seeding()`` is what says
    so, at the point where it is true.
    """

    missing = [name for name in ("starlette", "uvicorn") if find_spec(name) is None]

    if missing:
        typer.echo(
            "The 'forze mock' commands require the mock-server extra "
            f"(missing: {', '.join(missing)}). "
            "Install it with:  pip install 'forze[cli,mock-server]'",
            err=True,
        )
        raise typer.Exit(code=1)
