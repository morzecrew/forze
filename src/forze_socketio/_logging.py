"""Logger names for the forze_socketio package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeSocketIOLogger(StrEnum):
    """Forze Socket.IO logger names."""

    ERRORS = "socketio.errors"


# ....................... #

FORZE_SOCKETIO_LOGGER_NAMES: Final = list(map(str, ForzeSocketIOLogger))
