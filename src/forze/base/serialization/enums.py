"""Enum members as the values a store holds."""

from collections.abc import Sequence
from enum import Enum
from typing import Any, cast

from ..exceptions import exc

# ----------------------- #


def enum_values(value: Any) -> Any:
    """*value* with each ``Enum`` member in it, in lists, tuples, dict keys and values and a
    member's own value too, as its value.

    A store holds a member as its value, and the read model decodes the value back to the
    member. A ``StrEnum`` or ``IntEnum`` value is the text or number the member already was. A
    member whose value is ``None`` becomes ``None``: SQL ``NULL`` in a scalar column.

    A container holding no member is returned as it is; one holding a member is rebuilt as a
    ``list``, ``tuple`` or ``dict``.

    :param value: A value prepared for a store driver.
    :returns: *value* with its members replaced by their values.
    :raises CoreException: ``precondition`` when two keys of one dict become the same value.
    """

    if isinstance(value, Enum):
        return enum_values(value.value)

    if isinstance(value, (list, tuple)):
        seq = cast(Sequence[Any], value)
        items = [enum_values(item) for item in seq]

        if all(new is old for new, old in zip(items, seq, strict=True)):
            return seq

        return tuple(items) if isinstance(seq, tuple) else items

    if isinstance(value, dict):
        mapping: dict[Any, Any] = value
        pairs = [(enum_values(key), enum_values(item)) for key, item in mapping.items()]

        if all(
            new_key is key and new is item
            for (new_key, new), (key, item) in zip(pairs, mapping.items(), strict=True)
        ):
            return mapping

        mapped = dict(pairs)

        # Last-wins, as a JSON object would keep it, drops an entry with nothing said.
        if len(mapped) < len(mapping):
            raise exc.precondition("Two keys of one mapping share a value once enums are values.")

        return mapped

    return value
