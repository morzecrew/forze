"""Property-filter rules shared by every graph adapter — keys *and* values.

A ``property_filter`` key ends up embedded in adapter query machinery — e.g. a Cypher
``$pf_<key>`` parameter *name*, which cannot be backtick-quoted — so it is restricted to
plain identifiers and anything else fails closed before evaluation. The rule lives here
(not per adapter) so the in-memory mock rejects exactly what a real engine rejects and a
test cannot pass with a filter key that production would refuse.

Values need the same treatment for the same reason. Vertex and edge properties are written
through ``model_dump(mode="json")``, so a ``UUID`` is *stored* as a string — and a filter
carrying the ``UUID`` itself therefore matched nothing on the mock while the Neo4j driver
refused the parameter type outright. Two different wrong answers, and the mock's was the
worse one: an empty result reads as "no such vertex" rather than as a bug. Normalizing the
value the same way it was stored makes the filter mean what a caller intends on every
backend.
"""

import re
from collections.abc import Mapping

from pydantic_core import to_jsonable_python

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


# ....................... #


def normalize_property_filter(
    property_filter: Mapping[str, object] | None,
) -> dict[str, object] | None:
    """Coerce filter values into the form properties are stored in.

    Properties are persisted through ``model_dump(mode="json")``, so this applies the same
    conversion to the values being matched against them — a ``UUID``, ``datetime`` or
    ``Decimal`` compares equal to what was written instead of silently matching nothing.
    Values already JSON-native pass through untouched.

    Keys are left alone; :func:`validate_property_filter_keys` owns those.

    A ``None`` value is not a way to ask for "unset": equality against null matches nothing
    on every backend, following Cypher's three-valued logic. Filtering for absent properties
    needs a predicate the equality filter does not express.
    """

    if not property_filter:
        return None

    return {key: to_jsonable_python(value) for key, value in property_filter.items()}
