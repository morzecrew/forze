"""Constants for the forze_kits package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeKitsLogger(StrEnum):
    """Forze kits logger names.

    Kits log under their own namespace rather than borrowing ``forze.application`` so
    pre-built wiring (outbox relay, consumer, distributed-lock scope, lifecycle) is
    distinguishable from the framework core in the logs.
    """

    INTEGRATIONS = "forze_kits.integrations"
    SCOPES = "forze_kits.scopes"
    LIFECYCLE = "forze_kits.lifecycle"


# ....................... #

FORZE_KITS_LOGGER_NAMES: Final = list(map(str, ForzeKitsLogger))
