"""Request and response DTOs for the versioned-facts operations."""

from datetime import datetime
from uuid import UUID

from forze.domain.models import BaseDTO
from forze_kits.dto.paginated import Pagination

# ----------------------- #


class CorrectDocumentDTO[In: BaseDTO](BaseDTO):
    """What a correction asserts: which version it read, what it changes, and why.

    ``expected_version`` rather than ``rev`` deliberately — a caller correcting a fact is asserting
    *which version of the fact* it saw, and an unrelated column update that bumped ``rev`` must not
    invalidate that. The two answer different questions and a correction asks the first.
    """

    id: UUID
    """The version being corrected; must be the fact's current one."""

    expected_version: int
    """The version number the caller read. A mismatch is a ``conflict``."""

    dto: In
    """The patch the successor carries."""

    reason: str
    """Why the fact was corrected, in the author's own vocabulary.

    A free string: a framework-owned enum would be wrong for every domain, and an app that wants
    one declares it on its own patch model."""


# ....................... #


class FactIdDTO(Pagination):
    """A fact, by the identity its versions share.

    Paginated because a chain has no bound but the number of corrections, and a read that asked
    for all of it would meet the store's implicit cap — which truncates with a warning the
    caller never sees, so a long chain would come back quietly incomplete.
    """

    root_id: UUID
    """The fact, not one version of it."""


# ....................... #


class FactAsOfDTO(BaseDTO):
    """A fact and the instant to read it at."""

    root_id: UUID
    """The fact, not one version of it."""

    at: datetime
    """The instant; the version current then is the one returned."""
