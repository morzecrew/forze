"""Logger names for the forze_mcp package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeMCPLogger(StrEnum):
    """Forze MCP logger names."""

    ACCESS = "mcp.access"
    ERRORS = "mcp.errors"
    MIDDLEWARES = "mcp.middlewares"


# ....................... #

FORZE_MCP_LOGGER_NAMES: Final = list(map(str, ForzeMCPLogger))
