"""Normalize a domain ``(before[, after[, diff]])`` callable to a canonical 3-arg form.

Shared by the ``@event_emitter`` and ``@update_validator`` decorators: both accept a
method declaring one to three of ``before`` / ``after`` / ``diff`` and normalize it to a
``(before, after, diff)`` callable run after an update. Their only structural difference
is the return value and the metadata they attach — that lives in each decorator; the
arity normalization lives here.
"""

import inspect
from collections.abc import Callable
from typing import Any, TypeVar

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #

R = TypeVar("R")


def normalize_before_after_diff(
    f: Callable[..., R], *, kind: str
) -> Callable[[Any, Any, JsonDict], R]:
    """Wrap *f* as a canonical ``(before, after, diff) -> R`` callable.

    *f* may declare one (``before``), two (``+after``), or three (``+diff``) parameters;
    the returned wrapper always takes all three and forwards only the ones *f* declares.
    *kind* names the decorator in error messages (e.g. ``"Event emitter"``). Raises
    ``exc.internal`` if *f* takes zero or more than three parameters.
    """

    params = list(inspect.signature(f).parameters.values())

    if not params:
        raise exc.internal(
            f"{kind} must have at least one parameter (state before update)"
        )

    extra = len(params) - 1

    if extra == 0:

        def wrapper(before: Any, after: Any, diff: JsonDict) -> R:
            return f(before)

    elif extra == 1:

        def wrapper(before: Any, after: Any, diff: JsonDict) -> R:
            return f(before, after)

    elif extra == 2:

        def wrapper(before: Any, after: Any, diff: JsonDict) -> R:
            return f(before, after, diff)

    else:
        raise exc.internal(
            f"{kind} must have at most three parameters "
            "(state before update, state after update, diff)"
        )

    wrapper.__name__ = getattr(f, "__name__", kind)
    wrapper.__qualname__ = getattr(f, "__qualname__", wrapper.__name__)

    return wrapper
