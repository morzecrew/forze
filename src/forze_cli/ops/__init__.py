"""``forze ops``: governed operations from the command line (RFC 0077).

Declare an :class:`OpsApp` in your application, then point ``forze ops`` at it with
``--app module:attr`` or ``FORZE_OPS_APP``, or mount :func:`build_ops_cli` in your own Typer
CLI. Importing this package needs neither Typer nor the ``cli`` extra, so the module that
declares an :class:`OpsApp` stays importable from the web process.
"""

from typing import TYPE_CHECKING

from .declaration import (
    AnonymousIdentity,
    ApiKeyIdentity,
    OpsApp,
    OpsIdentity,
    StaticIdentity,
)

if TYPE_CHECKING:
    import typer

# ----------------------- #

__all__ = [
    "AnonymousIdentity",
    "ApiKeyIdentity",
    "OpsApp",
    "OpsIdentity",
    "StaticIdentity",
    "build_ops_cli",
]

# ....................... #


def build_ops_cli(app: OpsApp, *, name: str = "ops") -> "typer.Typer":
    """The ``ops`` command group with *app* bound, to mount in an application's own CLI.

    ``myapp ops call orders.get --input ...`` then needs no ``--app``. Requires the ``cli``
    extra.

    .. warning::
       Set ``pretty_exceptions_show_locals=False`` on the root ``typer.Typer`` you mount
       this group in. Typer reads it from the app that was invoked, never from a mounted
       group, and a crash that prints locals prints the credentials an operation's frames
       hold.

    :param app: The declaration every command uses.
    :param name: The group's name, when mounted without one.
    """

    from .commands import build_ops_cli as build

    return build(app, name=name)
