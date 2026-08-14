"""``DynamicReadSpec`` validation and its simulation-capture policy."""

from __future__ import annotations

import pytest

from forze.application.contracts.dynamic_read import (
    STATEMENT_CAPTURE_KEY,
    DynamicReadSpec,
)
from forze.base.exceptions import CoreException, ExceptionKind


def test_the_defaults_ship_on() -> None:
    """Caps are real values out of the box, and there is no way to spell "unlimited"."""

    spec = DynamicReadSpec(name="widgets")

    assert spec.row_cap > 0
    assert spec.max_statement_bytes > 0
    assert spec.capture_statements is False


@pytest.mark.parametrize(
    ("kwargs", "code"),
    [
        ({"row_cap": 0}, "dynamic_read_row_cap_invalid"),
        ({"row_cap": -1}, "dynamic_read_row_cap_invalid"),
        ({"max_statement_bytes": 0}, "dynamic_read_statement_bytes_invalid"),
    ],
)
def test_a_nonsensical_cap_is_refused_at_construction(
    kwargs: dict[str, int],
    code: str,
) -> None:
    """A cap of zero would make every call fail; a negative one is meaningless."""

    with pytest.raises(CoreException) as ei:
        DynamicReadSpec(name="widgets", **kwargs)

    assert ei.value.code == code
    assert ei.value.kind == ExceptionKind.CONFIGURATION


def test_statement_text_is_masked_in_capture_by_default() -> None:
    """The statement embeds the literals it was compiled with, so it is PII-shaped by default."""

    spec = DynamicReadSpec(name="widgets")

    assert spec.sensitive_capture_fields == frozenset({STATEMENT_CAPTURE_KEY})
    assert spec.trace_text_arg_key == STATEMENT_CAPTURE_KEY


def test_opting_in_unmasks_the_statement() -> None:
    """``capture_statements=True`` is the author's acknowledgement, the inference twin."""

    spec = DynamicReadSpec(name="widgets", capture_statements=True)

    assert spec.sensitive_capture_fields == frozenset()
    assert spec.trace_text_arg_key == STATEMENT_CAPTURE_KEY


@pytest.mark.parametrize("bad", [True, False, 1.5, 0, -1, None, "10"])
@pytest.mark.parametrize("field", ["row_cap", "max_statement_bytes"])
def test_a_cap_that_is_not_a_positive_int_is_refused(field: str, bad: object) -> None:
    """``attrs`` does not enforce annotations, so the validator has to.

    Each of these fails differently without a type check, and none of them fails usefully:
    ``True`` is an ``int`` and would wire as a one-row cap, a ``float`` compares fine and then
    reaches the fetch probe as ``cap + 1``, and ``None`` or a string raises ``TypeError`` from
    inside the validator instead of the configuration error that names the field.
    """

    with pytest.raises(CoreException) as ei:
        DynamicReadSpec(name="widgets", **{field: bad})  # type: ignore[arg-type]

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == (
        "dynamic_read_row_cap_invalid"
        if field == "row_cap"
        else "dynamic_read_statement_bytes_invalid"
    )
