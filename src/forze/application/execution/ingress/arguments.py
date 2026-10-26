"""Validate a caller's raw arguments into an operation's input DTO."""

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ValidationError

from forze.base.exceptions import exc
from forze.base.scrubbing import sanitize_pydantic_errors

from ..operations import OperationDescriptor

# ----------------------- #


def validate_operation_input(
    descriptor: OperationDescriptor | None,
    raw: Mapping[str, Any],
    *,
    code: str,
    forbid_unknown: bool,
    summary: str = "Invalid input",
    by_name: bool = False,
) -> Any:
    """Validate *raw* into the operation's input DTO (``None`` if it has no DTO).

    A rejection is raised as a ``validation`` error with *code*, a fixed *summary* and
    field-level errors with the raw values stripped. Pydantic's own error text embeds the
    offending ``input_value``, so passing it through would echo whatever the caller sent,
    including a secret typed into the wrong field.

    :param descriptor: The operation's descriptor, if it has one.
    :param raw: The caller's arguments.
    :param code: The error code of a rejection; each surface keeps its own.
    :param forbid_unknown: Refuse an argument the DTO does not accept, naming it, instead
        of letting Pydantic drop it. An operation with no input then refuses any argument.
    :param summary: The rejection's message.
    :param by_name: Read every field by its own name too, whatever the DTO's config says,
        for a surface whose schema advertises field names rather than aliases.
    :raises CoreException: When *raw* does not validate, or names an unknown argument while
        *forbid_unknown* is on.
    """

    if descriptor is None or descriptor.input_type is None:
        if forbid_unknown:
            _refuse_unknown(sorted(raw), code=code, summary=summary)

        return None

    if forbid_unknown:
        accepted = accepted_input_names(descriptor.input_type)

        if accepted is not None:
            if by_name:
                accepted |= set(descriptor.input_type.model_fields)

            _refuse_unknown(sorted(set(raw) - accepted), code=code, summary=summary)

    try:
        return descriptor.input_type.model_validate(dict(raw), by_name=by_name or None)

    except ValidationError as error:
        raise exc.validation(
            summary,
            code=code,
            details={"errors": sanitize_pydantic_errors(error.errors())},
        ) from error


# ....................... #


def accepted_input_names(input_type: type[BaseModel]) -> set[str] | None:
    """Every name the input DTO accepts, or ``None`` when it accepts names not listed here.

    The names the DTO's config lets validation read: an aliased field's own name only under
    ``validate_by_name`` (or ``populate_by_name``), its aliases unless ``validate_by_alias``
    is off and ``populate_by_name`` does not turn it back on. A name the DTO would not read
    is dropped by pydantic without a word, so listing it would run the operation without
    the caller's value. ``Field(alias=...)`` sets ``validation_alias`` too, so reading that
    one covers both.

    ``None`` means "do not check": a DTO configured ``extra="allow"`` takes extension
    fields on purpose, and enumerating what it accepts is impossible by construction. The
    same answer covers an alias shape a future pydantic introduces that
    :func:`_alias_names` cannot read — declining to refuse is the safe direction, and
    declining to accept is not.
    """

    config = input_type.model_config

    if config.get("extra") == "allow":
        return None

    by_name = config.get("validate_by_name")
    by_alias = config.get("validate_by_alias")

    # Pydantic's own back-compat rule: ``populate_by_name`` with ``validate_by_name`` unset
    # reads both spellings, whatever ``validate_by_alias`` says.
    if by_name is None and config.get("populate_by_name") is not None:
        by_name, by_alias = config.get("populate_by_name"), True

    by_name = bool(by_name)
    by_alias = by_alias is not False
    accepted: set[str] = set()

    for name, field in input_type.model_fields.items():
        alias = field.validation_alias

        if alias is None:
            accepted.add(name)
            continue

        names = _alias_names(alias)

        if not names:
            return None

        if by_name:
            accepted.add(name)

        if by_alias:
            accepted |= names

    return accepted


# ....................... #


def _alias_names(alias: object) -> set[str]:
    """Top-level names a single ``validation_alias`` accepts.

    Pydantic offers three shapes. A plain string is itself. An ``AliasPath`` reaches into a
    nested structure, so the name a caller sends is the *head* of the path. An
    ``AliasChoices`` is a list of either, and accepts all of them. Read structurally rather
    than by importing the classes: a shape this does not recognise contributes nothing, and
    the caller decides what that means.
    """

    if isinstance(alias, str):
        return {alias}

    choices = getattr(alias, "choices", None)

    if choices is not None:
        return {name for choice in choices for name in _alias_names(choice)}

    path = getattr(alias, "path", None)

    if path:
        head = path[0]

        return {head} if isinstance(head, str) else set()

    return set()


# ....................... #


def _refuse_unknown(unknown: list[str], *, code: str, summary: str) -> None:
    # Pydantic ignores a key it does not know: the caller believes it passed a filter, the
    # operation runs without one, and nothing records that anything was dropped.
    if not unknown:
        return

    raise exc.validation(
        f"{summary}: unknown argument(s) {', '.join(repr(name) for name in unknown)}",
        code=code,
        details={"unknown": unknown},
    )
