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
from decimal import Decimal

from pydantic_core import to_jsonable_python

from forze.base.exceptions import exc
from forze.base.serialization import decimal_text, sorted_set_items

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
    """Coerce filter values into the form a plain pydantic model stores them in.

    Properties are persisted through ``model_dump(mode="json")``, so this applies the same
    conversion to the values being matched against them — a ``UUID`` or ``datetime``
    compares equal to what was written instead of silently matching nothing. Values already
    JSON-native pass through untouched.

    Keys are left alone; :func:`validate_property_filter_keys` owns those. A forze model
    writes a ``Decimal`` (and a set) differently, so an adapter matches with
    :func:`property_filter_forms`, which gives both forms.

    A ``None`` value is not a way to ask for "unset": equality against null matches nothing
    on every backend, following Cypher's three-valued logic. Filtering for absent properties
    needs a predicate the equality filter does not express.
    """

    if not property_filter:
        return None

    return {key: to_jsonable_python(value) for key, value in property_filter.items()}


# ....................... #


def _model_text(value: object) -> object:
    """*value* as a forze model writes it in JSON: a ``Decimal`` in fixed point and a set
    sorted, at any depth of a list or mapping; anything else as pydantic writes it."""

    if isinstance(value, Decimal):
        return decimal_text(value)

    if isinstance(value, (list, tuple)):
        return [_model_text(item) for item in value]

    if isinstance(value, Mapping):
        return {key: _model_text(item) for key, item in value.items()}

    if isinstance(value, (set, frozenset)):
        return to_jsonable_python(sorted_set_items(value))

    return to_jsonable_python(value)


def _stored_forms(value: object) -> list[object]:
    """The texts *value* may be stored as: as a forze model writes it, and as a plain
    pydantic model (and data written before forze did) does — one when they agree. A whole
    write is one or the other, so two forms cover a value of any depth."""

    forms = [_model_text(value), to_jsonable_python(value)]

    return forms[:1] if forms[0] == forms[1] else forms


def property_filter_forms(
    property_filter: Mapping[str, object] | None,
) -> dict[str, list[object]] | None:
    """Each filter key with the stored values it matches, as :func:`normalize_property_filter`
    normalizes them; a ``Decimal`` matches either of its two texts.

    A property matches when it equals any of its key's values. What an adapter evaluates;
    ``None`` for an empty filter.
    """

    if not property_filter:
        return None

    return {key: _stored_forms(value) for key, value in property_filter.items()}
