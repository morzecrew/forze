"""Declarative domain invariants — always-true state rules.

An ``@invariant`` is a ``(self) -> None`` predicate that raises on violation. Unlike a raw
Pydantic ``@model_validator`` — which Forze's merge-patch update path bypasses via
``model_copy`` — an ``@invariant`` is enforced on **both** create and update, so the rule is
declared once and always holds.

Use ``@invariant`` for state rules; use :func:`~forze.domain.validation.update_validator`
when you need the transition (``before``/``after``/``diff``).
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, TypeVar

from forze.base.exceptions import exc

from .._logger import logger

# ----------------------- #

_INVARIANT_ATTR = "__forze_invariant__"

F = TypeVar("F", bound=Callable[..., None])


def invariant(func: F) -> F:
    """Mark a method as a domain invariant.

    The method takes only ``self`` and raises on violation. It runs on create (model
    construction) and after every :meth:`~forze.domain.models.Document.update`, so the rule
    is always enforced — including across merge-patch updates, which a raw
    ``@model_validator`` silently skips.
    """

    required = [
        p
        for p in inspect.signature(func).parameters.values()
        if p.name != "self"
        and p.default is p.empty
        and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
    ]

    if required:
        raise exc.configuration(
            f"@invariant {func.__qualname__!r} must take only 'self' "
            f"(unexpected required parameter(s): {[p.name for p in required]})."
        )

    setattr(func, _INVARIANT_ATTR, True)
    logger.trace("Registered invariant %s", getattr(func, "__qualname__", func))

    return func


# ....................... #


def is_invariant(member: Any) -> bool:
    """Whether *member* was marked by :func:`invariant`."""

    return callable(member) and getattr(member, _INVARIANT_ATTR, False)


# ....................... #


def collect_invariants(cls: type) -> list[str]:
    """Collect the names of ``@invariant`` methods on ``cls`` and its bases (MRO order).

    A name is collected once; calls resolve via ``getattr(type(self), name)`` so a subclass
    override is honoured.
    """

    names: list[str] = []
    seen: set[str] = set()

    for klass in cls.__mro__:
        for name, member in vars(klass).items():
            if name in seen or not is_invariant(member):
                continue

            names.append(name)
            seen.add(name)

    return names
