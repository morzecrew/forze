"""One canonical id for this process, for every subsystem that needs to name it."""

import os
import socket
from typing import Final

from .entropy_source import current_entropy_source

# ----------------------- #

_IDS: Final[dict[int, str]] = {}
"""Process id → the id minted for it. Keyed by pid, so a forked child mints its own."""

# ....................... #


def instance_id() -> str:
    """This process's id, ``host:pid:nonce``.

    The nonce is random per process, so a restarted process that reuses a pid is still a
    new instance. The id is stable for the life of the process, and a forked child gets
    its own.

    :returns: The instance id.
    """

    pid = os.getpid()
    found = _IDS.get(pid)

    if found is None:
        nonce = current_entropy_source().randbits(32)
        # ``setdefault`` keeps the first id two racing threads minted.
        found = _IDS.setdefault(pid, f"{socket.gethostname()}:{pid}:{nonce:08x}")

    return found
