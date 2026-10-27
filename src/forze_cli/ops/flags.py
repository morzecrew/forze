"""Typed flags: ``forze ops call OP --field VALUE`` for an input of flat scalars.

The flags only spell the raw input object ``--input`` would carry: every value but a
switch's stays the string the caller typed, and the shared ingress validation coerces it,
so there is no second validation path. An input whose shape has no clean flag mapping takes
no flags at all, rather than some of them.
"""

from datetime import date
from decimal import Decimal
from enum import Enum
from types import NoneType, UnionType
from typing import Annotated, Any, Final, Literal, Union, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel

# ----------------------- #

CALL_OPTIONS: Final = frozenset(
    {
        "--app",
        "--input",
        "--batch",
        "--tenant",
        "--idempotency-key",
        "--timeout",
        "--grace",
        "--correlation-id",
        "--causation-id",
        "--api-key-file",
        "--pretty",
        "--help",
    }
)
"""The spellings ``call`` keeps for itself; an input with a field spelled like one takes no
flags, so a call option never changes meaning with the operation."""

# ``bool`` is an ``int`` and ``datetime`` a ``date``; pydantic coerces each from a string.
_SCALARS: Final = (str, int, float, Decimal, UUID, date, Enum)

type FlagTable = dict[str, tuple[str, bool | None]]
"""Flag spelling → the field it sets, and the value a switch sets (``None`` takes a value)."""

# ....................... #


def declares_secret(schema: Any) -> bool:
    """Whether a JSON Schema declares a ``SecretStr`` or ``SecretBytes`` anywhere in it."""

    if isinstance(schema, dict):
        return schema.get("format") == "password" or any(  # pyright: ignore[reportUnknownMemberType]
            declares_secret(value)
            for value in schema.values()  # pyright: ignore[reportUnknownVariableType]
        )

    if isinstance(schema, list):
        return any(declares_secret(value) for value in schema)  # pyright: ignore[reportUnknownVariableType]

    return False


# ....................... #


def flag_table(input_type: type[BaseModel]) -> tuple[FlagTable, str | None]:
    """The typed flags *input_type* takes, or the reason it takes none.

    Every field must be a scalar (or an optional one), so a flag is one value; a ``bool``
    becomes the switch pair ``--name`` / ``--no-name``. The input must declare no secret,
    which argv would show, and no spelling may be a ``call`` option or another field's.
    """

    if declares_secret(input_type.model_json_schema()):
        return {}, "it declares a secret, which argv would show"

    table: FlagTable = {}

    for name, field in input_type.model_fields.items():
        annotation = _unwrap(field.annotation)

        if not _scalar(annotation):
            return {}, f"field {name!r} is not a scalar"

        flag = "--" + name.replace("_", "-")
        spellings = {flag: True, "--no-" + flag[2:]: False} if annotation is bool else {flag: None}

        for spelling, constant in spellings.items():
            if spelling in CALL_OPTIONS:
                return {}, f"field {name!r} is spelled {spelling}, a call option"

            if spelling in table:
                return {}, f"fields {table[spelling][0]!r} and {name!r} share {spelling}"

            table[spelling] = (name, constant)

    return table, None


def describe_flags(table: FlagTable) -> list[str]:
    """One entry per field, a switch as ``--name/--no-name``."""

    by_field: dict[str, list[str]] = {}

    for spelling, (name, _) in table.items():
        by_field.setdefault(name, []).append(spelling)

    return ["/".join(spellings) for spellings in by_field.values()]


# ....................... #


def parse_flags(args: list[str], table: FlagTable) -> dict[str, Any]:
    """The raw input object *args* spell, keyed by field name.

    :raises ValueError: When *args* do not parse; the message names flags, never values.
    """

    raw: dict[str, Any] = {}
    tokens = iter(args)

    for token in tokens:
        spelling, equals, value = token.partition("=")

        if not spelling.startswith("--"):
            raise ValueError("an argument that is not a flag; spell each as --name VALUE")

        if spelling not in table:
            raise ValueError(f"no field takes {spelling}")

        name, constant = table[spelling]

        if name in raw:
            raise ValueError(f"field {name!r} is given more than once")

        if constant is not None:
            if equals:
                raise ValueError(f"{spelling} takes no value")

            raw[name] = constant

        elif equals:
            raw[name] = value

        else:
            following = next(tokens, None)

            if following is None:
                raise ValueError(f"{spelling} needs a value")

            # A forgotten value would swallow the next flag; --name=VALUE spells one that
            # looks like a flag.
            if following.partition("=")[0] in table:
                raise ValueError(f"{spelling} needs a value, not the flag after it")

            raw[name] = following

    return raw


# ....................... #


def _unwrap(annotation: Any) -> Any:
    """*annotation* without ``Annotated`` metadata, and without ``None`` when optional."""

    if get_origin(annotation) is Annotated:
        return _unwrap(get_args(annotation)[0])

    if get_origin(annotation) in (Union, UnionType):
        members = [member for member in get_args(annotation) if member is not NoneType]

        # A union of two types has no single coercion from a string.
        return _unwrap(members[0]) if len(members) == 1 else annotation

    return annotation


def _scalar(annotation: Any) -> bool:
    # Pydantic matches a literal, or a plain enum's value, exactly: only a string one ever
    # comes from a string. A ``StrEnum`` or an ``IntEnum`` coerces as its base type does.
    if get_origin(annotation) is Literal:
        return all(isinstance(value, str) for value in get_args(annotation))

    if not isinstance(annotation, type) or not issubclass(annotation, _SCALARS):
        return False

    if issubclass(annotation, Enum) and not issubclass(annotation, (str, int)):
        return all(isinstance(member.value, str) for member in annotation)

    return True
