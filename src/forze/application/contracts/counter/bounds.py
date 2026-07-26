"""The counter plane's value domain, and the one error that leaving it produces.

Every backend that stores a counter stores it as a signed 64-bit integer: Postgres in a
``bigint``, MongoDB in an int64, Redis in an integer-valued string, Firestore in an
``integerValue``. So int64 *is* the plane's domain — not a backend detail, a contract fact,
and one the contract used to leave unsaid.

Leaving it unsaid had a specific cost. The in-memory mock counts with Python integers, which
have no bound at all, so it accepted values no real backend can hold; code that looked
correct against the mock failed only in production. And where the real backends did refuse,
they disagreed about what kind of failure it was — one calling it a precondition, the others
infrastructure — so a caller could not branch on it portably.

Overflow is **caller-caused and permanent**: the value asked for cannot be represented, and
retrying changes nothing. That makes it a ``precondition``, never an ``infrastructure``
error, whose kind invites exactly the retry that will never work.
"""

from typing import Final

from forze.base.exceptions import CoreException, exc

# ----------------------- #

COUNTER_MAX_VALUE: Final[int] = 2**63 - 1
"""Largest value a counter can hold — signed 64-bit, the domain every backend shares."""

COUNTER_MIN_VALUE: Final[int] = -(2**63)
"""Smallest value a counter can hold. Counters are not required to be non-negative:
``decr`` below zero is legal and every backend agrees on it."""

COUNTER_VALUE_OUT_OF_RANGE_CODE: Final[str] = "counter_value_out_of_range"
"""Error code for an allocation or reset that would leave the int64 domain.

Raised identically by every adapter — including the in-memory mock, which could represent
the value and refuses anyway, because a mock that accepts what production rejects is how a
bug reaches production."""


def counter_out_of_range(value: int, *, operation: str) -> CoreException:
    """The refusal itself, so every backend words it the same way."""

    return exc.precondition(
        f"Counter {operation} would put the value at {value}, outside the "
        f"[{COUNTER_MIN_VALUE}, {COUNTER_MAX_VALUE}] range a counter can hold.",
        code=COUNTER_VALUE_OUT_OF_RANGE_CODE,
        details={"operation": operation},
    )


def validate_counter_value(value: int, *, operation: str) -> None:
    """Refuse a value outside the domain *before* it reaches a backend.

    Usable wherever the resulting value is known up front — ``reset``, and the ``by`` of an
    allocation. An atomic increment's result is not known without a read, so adapters also
    translate the backend's own overflow refusal; this is the half that can be checked
    early, and checking it early is what stops a backend accepting a write that only breaks
    the *next* caller.
    """

    if not COUNTER_MIN_VALUE <= value <= COUNTER_MAX_VALUE:
        raise counter_out_of_range(value, operation=operation)
