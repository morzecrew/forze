"""Plan patch types for operation registry selectors."""

import attrs

from forze.base.primitives import StrKeySelector

from ..planning import OperationPlan

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PlanPatch:
    """Plan patch keyed by a string key selector."""

    selector: StrKeySelector.Spec
    """Selector matching registered operation keys."""

    plan: OperationPlan
    """Partial plan merged for matching operations at freeze."""
