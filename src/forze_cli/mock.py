"""``forze mock`` — serve an app on in-memory backends.

Takes the same ``module:attribute`` import string ``forze dst`` does, pointing at a
:class:`~forze_mock.server.MockApp` the user exposes. Nothing about the app is inverted or
re-declared: the declaration carries the app's own factory, and the CLI supplies a runtime.
"""

from __future__ import annotations

import typer

from forze_cli._compat import require_mock_server
from forze_cli.loader import load_object

# ----------------------- #

mock_app_cli = typer.Typer(
    no_args_is_help=True,
    help=(
        "Serve your app on the in-memory mock — real routes, real identity, no infrastructure. "
        "Install with the 'mock-server' extra."
    ),
)

# ....................... #


@mock_app_cli.command("serve")
def serve(
    ref: str = typer.Argument(
        ...,
        help="Import string of a MockApp, e.g. 'myapp.mock:mock_app'.",
        metavar="MODULE:ATTR",
    ),
    host: str = typer.Option("127.0.0.1", help="Interface to bind."),
    port: int = typer.Option(8000, help="Port to bind."),
    log_level: str = typer.Option("info", help="uvicorn log level."),
) -> None:
    """Serve the declared ``MockApp``.

    Refuses unless ``FORZE_MOCK_SERVER=1`` is set, and unless the composed deps actually
    contain a mock module — this server keeps every byte in memory and enforces none of the
    guarantees a real backend does.
    """

    require_mock_server()

    from forze_mock.server import MockApp
    from forze_mock.server import serve as serve_mock

    target = load_object(ref)

    if callable(target) and not isinstance(target, MockApp):  # pyright: ignore[reportUnnecessaryIsInstance]
        try:
            target = target()

        # The `MockApp` class itself, or a factory that takes arguments: both are callable and
        # both are the same user mistake as pointing at the wrong attribute, so they get the
        # same guidance rather than a traceback.
        except TypeError as error:
            typer.echo(
                f"Calling {ref!r} to get a MockApp failed: {error}. Expose a MockApp "
                "instance, or a zero-argument callable returning one.",
                err=True,
            )
            raise typer.Exit(code=1) from error

    if not isinstance(target, MockApp):
        typer.echo(
            f"{ref!r} is not a MockApp (got {type(target).__name__}). Expose one, or a "
            "zero-argument callable returning one.",
            err=True,
        )
        raise typer.Exit(code=1)

    serve_mock(target, host=host, port=port, log_level=log_level)
