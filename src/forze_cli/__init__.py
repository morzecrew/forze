"""The optional ``forze`` command-line interface.

The ``forze`` console script is always installed, but its machinery (Typer) ships only with
the ``cli`` extra. So this module imports nothing heavy at load time: :func:`main` checks the
extra is present and fails with a clear install hint otherwise, then hands off to the app.
"""

from __future__ import annotations

from importlib.util import find_spec

__all__ = ["main"]


def main() -> None:
    """Console-script entry point — fail gracefully when the ``cli`` extra is absent."""

    if find_spec("typer") is None:
        import sys

        sys.stderr.write(
            "The 'forze' command requires the CLI extra. "
            "Install it with:  pip install 'forze[cli]'\n"
        )
        raise SystemExit(1)

    from forze_cli.app import app

    app()
