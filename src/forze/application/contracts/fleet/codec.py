"""The checks and the report encoding every fleet backend shares.

A beat is refused before anything is sent, and every backend refuses the same beats: the
checks live here, not in each adapter, so two backends cannot drift on them. Reports are
encoded with the standard library's JSON, which writes integers of any size and floats in
their shortest round-tripping form, so a number reads back exactly as it was reported.
"""

import json
import math
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Final, get_args

from forze.base.exceptions import CoreException, exc

from .specs import FleetSpec
from .value_objects import MemberState, Report

# ----------------------- #

MEMBER_ID_MAX_BYTES: Final[int] = 256
"""Longest member id, in UTF-8 bytes."""

_MEMBER_ID_FORBIDDEN: Final[str] = "\t\n\r{}"
"""Characters a member id may not hold: the reply's separators, and the key's hash-tag braces."""

_STATES: Final[frozenset[str]] = frozenset(get_args(MemberState))

# ....................... #


def validate_member_id(member_id: str) -> None:
    """Refuse a member id a backend could not store and read back unambiguously.

    :param member_id: The id to check.
    :raises CoreException: ``validation`` (``fleet.member_id_invalid``) when the id is
        empty, longer than :data:`MEMBER_ID_MAX_BYTES`, or holds a tab, a newline or a brace.
    """

    if (
        not isinstance(member_id, str)  # pyright: ignore[reportUnnecessaryIsInstance]
        or not member_id
        or len(member_id.encode()) > MEMBER_ID_MAX_BYTES
        or any(char in member_id for char in _MEMBER_ID_FORBIDDEN)
    ):
        raise exc.validation(
            f"Fleet member id {member_id!r} must be 1 to {MEMBER_ID_MAX_BYTES} bytes "
            "with no tab, newline or brace",
            code="fleet.member_id_invalid",
        )


# ....................... #


def _refuse_report(reason: str) -> CoreException:
    return exc.validation(f"Fleet report refused: {reason}", code="fleet.report_invalid")


def encode_report(report: Report, *, max_bytes: int) -> str:
    """Encode *report* as compact JSON, refusing what a backend must not store.

    :param report: A flat map of string keys to int, float or string values.
    :param max_bytes: The largest encoding allowed, in UTF-8 bytes.
    :returns: The encoded report.
    :raises CoreException: ``validation`` (``fleet.report_invalid``) for a non-string key,
        a ``bool`` or other non-int/float/str value, a NaN or infinite float, or an encoding
        longer than *max_bytes*.
    """

    if not isinstance(report, Mapping):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise _refuse_report("a report is a mapping")

    for key, value in report.items():
        if not isinstance(key, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise _refuse_report(f"key {key!r} is not a string")

        if isinstance(value, bool) or not isinstance(value, int | float | str):
            raise _refuse_report(f"{key!r} is {type(value).__name__}, not int, float or str")

        if isinstance(value, float) and not math.isfinite(value):
            raise _refuse_report(f"{key!r} is {value}, not a finite number")

    try:
        encoded = json.dumps(dict(report), ensure_ascii=False, separators=(",", ":"))

    except ValueError as error:  # an integer past Python's digit limit
        raise _refuse_report(str(error)) from error

    size = len(encoded.encode())

    if size > max_bytes:
        raise _refuse_report(f"{size} bytes encoded, above the {max_bytes}-byte cap")

    return encoded


# ....................... #


def decode_report(raw: str) -> Report:
    """Decode a report written by :func:`encode_report`.

    :param raw: The encoded report.
    :returns: The report, with every number as it was encoded.
    :raises ValueError: When *raw* is not an encoded report.
    """

    decoded: Any = json.loads(raw)

    if not isinstance(decoded, dict):
        raise ValueError("an encoded report is a JSON object")

    return decoded  # pyright: ignore[reportUnknownVariableType]


# ....................... #


def prepare_beat(
    spec: FleetSpec,
    member_id: str,
    state: MemberState,
    report: Report,
    ttl: timedelta | None,
) -> tuple[str, int]:
    """Check a beat and return what a backend stores for it.

    :param spec: The fleet.
    :param member_id: The beating member's id.
    :param state: The member's state.
    :param report: The member's report.
    :param ttl: The entry's lifetime, or ``None`` for the spec's.
    :returns: The encoded report and the lifetime in whole milliseconds.
    :raises CoreException: ``validation`` for a bad id, state, report or lifetime.
    """

    validate_member_id(member_id)

    if state not in _STATES:
        raise exc.validation(
            f"Fleet member state {state!r} is not one of {sorted(_STATES)}",
            code="fleet.state_invalid",
        )

    ttl_ms = (spec.ttl if ttl is None else ttl) // timedelta(milliseconds=1)

    if ttl_ms < 1:
        raise exc.validation(
            "A fleet beat's ttl must be at least 1 millisecond",
            code="fleet.ttl_invalid",
        )

    return encode_report(report, max_bytes=spec.report_max_bytes), ttl_ms
