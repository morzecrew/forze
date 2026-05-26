"""Scrub sensitive values before exposing data in logs or API error context."""

from typing import Any, Literal

from ._walk import walk_value
from .policy import DEFAULT_MAX_DEPTH

# ----------------------- #

SanitizeContext = Literal["egress", "log"]
"""Where scrubbed data is headed: API/errors (``egress``) or structured logs (``log``)."""

# ....................... #


def sanitize(
    value: Any,
    *,
    context: SanitizeContext = "egress",
    depth: int = 0,
    max_depth: int = DEFAULT_MAX_DEPTH,
    text_scrub: bool | None = None,
) -> Any:
    """Return a scrubbed copy of *value* safe for the given *context*.

    Does not mutate the input. ``egress`` masks sensitive keys only (``**********``).
    ``log`` applies the same key policy plus log string rules on string leaves when
    *text_scrub* is true (default for ``log``).

    :param value: Arbitrary value (mappings, sequences, models, scalars).
    :param context: ``egress`` for clients and :class:`~forze.base.errors.exc.internal`
        details; ``log`` for structlog extras.
    :param depth: Current recursion depth (internal).
    :param max_depth: Maximum nesting depth before values are replaced with a sentinel.
    :param text_scrub: Override log string rule scrubbing (``log`` context only).
    :returns: Scrubbed copy.
    """

    use_text_scrub = text_scrub if text_scrub is not None else context == "log"

    return walk_value(
        value,
        text_scrub=use_text_scrub,
        depth=depth,
        max_depth=max_depth,
    )
