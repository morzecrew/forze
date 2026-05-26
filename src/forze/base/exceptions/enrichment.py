from typing import TYPE_CHECKING, Any, Final

import attrs

if TYPE_CHECKING:
    from ..primitives import JsonDict
    from .model import CoreException

# ----------------------- #

_RESERVED_KEYS: Final = frozenset({"callsite", "resource", "cause"})
_SKIP_BOUND_KEYS: Final = frozenset({"callsite", "resource", "cause", "self", "cls"})

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class CallsiteFrame:
    """Callsite frame."""

    domain: str
    """The domain."""

    site: str
    """The site."""

    surface: str | None = None
    """The surface."""

    route: str | None = None
    """The route."""

    phase: str | None = None
    """The phase."""


# ....................... #


def _frame_to_mapping(frame: "CallsiteFrame | JsonDict") -> "JsonDict":
    if isinstance(frame, CallsiteFrame):
        return {k: v for k, v in attrs.asdict(frame).items() if v is not None}

    return {k: v for k, v in frame.items() if v is not None}


# ....................... #


def pick_semantic_details(bound: "JsonDict | None") -> "JsonDict":
    """Return scrubbed bound args safe to merge as non-reserved detail keys."""

    from ..scrubbing import sanitize

    if bound is None:
        return {}

    out: JsonDict = {}

    for key, value in bound.items():
        if key in _SKIP_BOUND_KEYS:
            continue

        out[key] = sanitize(value, context="egress")

    return out


# ....................... #


def enrich[X: "CoreException"](
    exc: X,
    *,
    callsite: "CallsiteFrame | JsonDict | None" = None,
    resource: "JsonDict | None" = None,
    cause: "JsonDict | None" = None,
    **semantic: Any,
) -> X:
    """Return a copy of *exc* with missing reserved/semantic detail keys filled.

    Existing reserved/semantic detail keys are not overwritten.
    """

    from ..scrubbing import sanitize
    from ..serialization import apply_dict_patch

    details = dict(exc.details or {})

    if callsite is not None and "callsite" not in details:
        details["callsite"] = sanitize(_frame_to_mapping(callsite), context="egress")

    if resource is not None and "resource" not in details:
        details["resource"] = sanitize(dict(resource), context="egress")

    if cause is not None and "cause" not in details:
        details["cause"] = sanitize(dict(cause), context="egress")

    semantic_details = pick_semantic_details(semantic)
    details = apply_dict_patch(details, semantic_details)

    return attrs.evolve(exc, details=details or None)
