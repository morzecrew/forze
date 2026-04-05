from typing import Literal

# ----------------------- #

PostgresBookkeepingStrategy = Literal["database", "application"]
"""Strategy for bookkeeping: ``"database"`` (trigger) or ``"application"``.

Related to revision bump and history write strategies.
"""
