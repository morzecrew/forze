"""Request DTOs for the temporal-validity reads."""

from datetime import date

from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO
from forze_kits.dto.paginated import Pagination

# ----------------------- #


class EffectiveOnDTO(BaseDTO):
    """Which key to read, and the day to read it on."""

    key: JsonDict
    """The key's fields and their values — exactly the fields the policy names."""

    on: date
    """The day. The row whose period is in force then is the one returned."""


# ....................... #


class TimelineDTO(Pagination):
    """Which key to read, and the window its rows must meet.

    Paginated, unlike the "one query" the design sketches. A timeline is unbounded by nature —
    one row per period, for all time — and an unbounded read meets the store's implicit cap,
    which truncates with a warning the caller never receives. A page says how much it holds; a
    silently partial timeline says nothing and reads like the whole of it.
    """

    key: JsonDict
    """The key's fields and their values — exactly the fields the policy names."""

    start: date
    """The first day of the window."""

    end: date | None = None
    """The last day, or ``None`` for a window with no end."""
