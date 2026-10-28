"""Enum members as the values a store holds."""

from collections.abc import Sequence
from enum import Enum
from typing import Any, cast

# ----------------------- #


def enum_values(value: Any) -> Any:
    """*value* with each ``Enum`` member in it, in lists, tuples and dicts too, as its value.

    A store holds a member as its value, and the read model decodes the value back to the
    member. A ``StrEnum`` or ``IntEnum`` value is the text or number the member already was. A
    member whose value is ``None`` becomes ``None``: SQL ``NULL`` in a scalar column.

    A container holding no member is returned as it is; one holding a member is rebuilt as a
    ``list``, ``tuple`` or ``dict``.

    :param value: A value prepared for a store driver.
    :returns: *value* with its members replaced by their values.
    """

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, (list, tuple)):
        seq = cast(Sequence[Any], value)
        items = [enum_values(item) for item in seq]

        if all(new is old for new, old in zip(items, seq, strict=True)):
            return seq

        return tuple(items) if isinstance(seq, tuple) else items

    if isinstance(value, dict):
        mapping: dict[Any, Any] = value
        mapped = {key: enum_values(item) for key, item in mapping.items()}

        if all(mapped[key] is item for key, item in mapping.items()):
            return mapping

        return mapped

    return value
