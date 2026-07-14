"""The plane-completeness doctrine's enforcement point.

Every plane an application binds declares itself **exportable**, **rebuildable**, **drained** —
or **refused**. A refused plane is one the framework can neither carry faithfully nor safely
skip, and the distinction matters because *the artifact cannot tell you which happened*: a
plane that was left out and a plane that was empty look exactly alike once it is written.

So an export asks first, and refuses rather than produce something that looks complete and is
not. The refusal names each plane and what to do about it, because "your app is not exportable"
is not an actionable thing to tell someone.
"""

from typing import Any, cast

from forze.base.exceptions import exc

from ..analytics import AnalyticsProvenance, AnalyticsSpec
from .registry import FrozenSpecRegistry
from .value_objects import PlaneDisposition, SpecPlane, SpecRegistryEntry

# ----------------------- #

_COUNTER_REASON = (
    "counters are durable state with no read path, so an export cannot copy them and a "
    "migrated application would reissue sequence numbers it has already handed out. There is "
    "no way to declare around this — the plane needs a read port before it can travel."
)

_UNDECLARED_ANALYTICS_REASON = (
    "provenance is undeclared. Set provenance=AnalyticsProvenance.PROJECTED if these rows are "
    "recomputed from a plane that *is* exported (the export then rebuilds them on the target), "
    "or provenance=AnalyticsProvenance.SYSTEM_OF_RECORD if this warehouse is the only place "
    "they exist. The framework cannot tell the two apart, and guessing wrong in one direction "
    "silently drops the only copy of the data."
)

_SYSTEM_OF_RECORD_REASON = (
    "this warehouse is declared the system of record for its rows, and the analytics port "
    "exposes only the application's named queries — there is no full-scan read to export it "
    "with, and nothing to rebuild it from. Export it with your warehouse's own tooling."
)


# ....................... #


def refusal_reason(entry: SpecRegistryEntry) -> str:
    """Why this plane cannot be carried, and what (if anything) the author can do about it."""

    if entry.plane is SpecPlane.COUNTER:
        return _COUNTER_REASON

    if isinstance(entry.spec, AnalyticsSpec):
        s = cast(AnalyticsSpec[Any, Any], entry.spec)  # type: ignore[redundant-cast]

        if s.provenance is AnalyticsProvenance.SYSTEM_OF_RECORD:
            return _SYSTEM_OF_RECORD_REASON

        return _UNDECLARED_ANALYTICS_REASON

    return "the plane is declared REFUSED and cannot be carried or safely skipped."


# ....................... #


def assert_exportable(registry: FrozenSpecRegistry) -> None:
    """Raise unless every plane in the inventory can be carried, rebuilt, or drained.

    The guard a portable export runs before it writes a single row. It is deliberately the
    *whole* inventory and not just the planes an export would touch: a plane it would skip is
    exactly the one worth refusing over, because skipping it is indistinguishable from it
    having been empty.

    Nothing else calls this — a runtime is free to bind refused planes and run perfectly well.
    An application only has to be *exportable* when someone tries to export it.
    """

    refused = registry.of_disposition(PlaneDisposition.REFUSED)

    if not refused:
        return

    lines = "\n".join(
        f"  - {entry.ref.label()} ({entry.source.value}): {refusal_reason(entry)}"
        for entry in refused
    )

    raise exc.precondition(
        f"{len(refused)} plane(s) cannot be exported, so the export would produce an artifact "
        f"that looks complete and is not:\n{lines}"
    )
