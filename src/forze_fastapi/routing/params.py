from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Annotated
from uuid import UUID

from fastapi import Query

# ----------------------- #

UUIDQuery = Annotated[
    UUID,
    Query(description="Unique identifier of the document."),
]
"""Unique identifier of the document."""

RevQuery = Annotated[
    int,
    Query(description="Revision number of the document."),
]
"""Revision number of the document."""
