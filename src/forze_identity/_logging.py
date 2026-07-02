"""Constants for the forze_identity package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeIdentityLogger(StrEnum):
    """Forze identity logger names."""

    AUTHN = "forze_identity.authn"
    AUTHZ = "forze_identity.authz"


# ....................... #

FORZE_IDENTITY_LOGGER_NAMES: Final = list(map(str, ForzeIdentityLogger))
