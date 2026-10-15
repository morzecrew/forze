"""A local TCP port for a test server to bind."""

import socket

# ----------------------- #


def free_port() -> int:
    """A port on ``127.0.0.1`` the OS just handed out and took back, so nothing listens there.

    Another process can take it before the caller binds it; a test that starts a server on it
    accepts that small race.
    """

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))

        return probe.getsockname()[1]
