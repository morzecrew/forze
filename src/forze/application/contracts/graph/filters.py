"""Property-filter key rule shared by every graph adapter.

A ``property_filter`` key ends up embedded in adapter query machinery — e.g. a Cypher
``$pf_<key>`` parameter *name*, which cannot be backtick-quoted — so it is restricted to
plain identifiers and anything else fails closed before evaluation. The rule lives here
(not per adapter) so the in-memory mock rejects exactly what a real engine rejects and a
test cannot pass with a filter key that production would refuse.
"""

import re
from collections.abc import Mapping

from forze.base.exceptions import exc

# ----------------------- #

_FILTER_KEY_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def is_valid_filter_key(key: str) -> bool:
    """Whether *key* is a plain identifier usable as a property-filter key."""

    return _FILTER_KEY_RE.fullmatch(key) is not None


# ....................... #


def validate_property_filter_keys(property_filter: Mapping[str, object] | None) -> None:
    """Fail closed on any non-identifier key in *property_filter* (``exc.validation``)."""

    if not property_filter:
        return

    malformed = sorted(k for k in property_filter if not is_valid_filter_key(k))

    if malformed:
        raise exc.validation(
            f"Invalid graph property-filter keys {malformed}: a filter key must be "
            "an identifier (letters, digits, underscores; not starting with a digit).",
            code="graph_filter_key_invalid",
        )
