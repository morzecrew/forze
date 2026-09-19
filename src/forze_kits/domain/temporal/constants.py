"""Shared field names for the temporal-validity mixin."""

from typing import Final

# ----------------------- #

VALID_FROM_FIELD: Final = "valid_from"
"""The day the row's assertion comes into force."""

VALID_TO_FIELD: Final = "valid_to"
"""The day it stops, or null while it is still in force.

Null is open-ended, never a sentinel date. A ``9999-12-31`` simplifies one predicate and then
lies in every export, report and column a human reads."""

VALIDITY_PERIOD: Final = (VALID_FROM_FIELD, VALID_TO_FIELD)
"""The pair, in the order the guarantee's ``period`` takes them."""
