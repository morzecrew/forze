from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Annotated, final
from uuid import UUID

import attrs
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


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Pagination:
    """Pagination parameters extracted from query string."""

    page: int
    size: int


def pagination(
    page: int = Query(default=1, ge=1, description="Page number."),
    size: int = Query(default=10, ge=1, le=100, description="Size of the page."),
) -> Pagination:
    """Return a :class:`Pagination` instance from query parameters."""

    return Pagination(page=page, size=size)
