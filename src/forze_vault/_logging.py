"""Logger names for the forze_vault package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeVaultLogger(StrEnum):
    """Forze Vault logger names."""

    ADAPTERS = "forze_vault.adapters"
    EXECUTION = "forze_vault.execution"
    KERNEL = "forze_vault.kernel"


# ....................... #

FORZE_VAULT_LOGGER_NAMES: Final = list(map(str, ForzeVaultLogger))
