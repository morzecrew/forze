"""One canonical id for this process, for every subsystem that needs to name it."""

import os
import socket
from typing import Final

from .entropy_source import SystemEntropySource

# ----------------------- #

_IDS: Final[dict[int, str]] = {}
"""Process id → the id minted for it. Keyed by pid, so a forked child mints its own."""

# A child forgets its ancestors' ids, so a descendant that reuses a dead ancestor's pid
# still mints a new one. Platforms without ``fork`` (Windows) have no such hook, and a
# spawned process starts with an empty cache anyway.
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_IDS.clear)

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
        # The system source, not the bound one: a seeded source would spend a draw on the
        # first call only, and two runs with one seed would diverge after it.
        nonce = SystemEntropySource().randbits(32)
        # ``setdefault`` keeps the first id two racing threads minted.
        found = _IDS.setdefault(pid, f"{socket.gethostname()}:{pid}:{nonce:08x}")

    return found
