"""String key selectors for matching opaque operation keys."""

from __future__ import annotations

import fnmatch
from collections.abc import Callable, Iterable, Iterator
from typing import TypeAlias, final

import attrs

from ..errors import CoreError
from .types import StrKey

# ----------------------- #


def _normalize_key(key: StrKey) -> str:
    return str(key)


def _require_non_empty(value: str, *, label: str) -> None:
    if not value:
        raise CoreError(f"{label} must be non-empty")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _AllKeys:
    """Select every key."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _ExactKeys:
    """Select keys that match one of the given literals exactly."""

    keys: frozenset[str]

    def __attrs_post_init__(self) -> None:
        if not self.keys:
            raise CoreError("ExactKeys.keys must be non-empty")


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _Prefix:
    """Select keys whose string form starts with ``value``."""

    value: str

    def __attrs_post_init__(self) -> None:
        _require_non_empty(self.value, label="Prefix.value")


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _Suffix:
    """Select keys whose string form ends with ``value``."""

    value: str

    def __attrs_post_init__(self) -> None:
        _require_non_empty(self.value, label="Suffix.value")


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _Glob:
    """Select keys that match a :mod:`fnmatch` pattern (case-sensitive)."""

    pattern: str

    def __attrs_post_init__(self) -> None:
        _require_non_empty(self.pattern, label="Glob.pattern")


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _When:
    """Select keys that satisfy a custom predicate."""

    predicate: Callable[[str], bool]


# ....................... #


@final
class StrKeySelector:
    """Factory and matching API for opaque string key selectors.

    Use the module singleton :data:`str_key_selector` for call sites, or this
    class directly. Selector values are :class:`Spec` variants (frozen attrs
    instances) produced by the factory methods below.

    Glob patterns use :mod:`fnmatch` metacharacters (``*``, ``?``); they do not
    imply a canonical key separator.
    """

    Spec: TypeAlias = _AllKeys | _ExactKeys | _Prefix | _Suffix | _Glob | _When
    """Tagged union of selector strategies returned by factory methods."""

    # ....................... #

    def all_keys(self) -> Spec:
        """Build a selector that matches every key."""

        return _AllKeys()

    # ....................... #

    def exact(self, *keys: StrKey) -> Spec:
        """Build a selector that matches the given literal keys."""

        if not keys:
            raise CoreError("exact() requires at least one key")

        return _ExactKeys(keys=frozenset(map(_normalize_key, keys)))

    # ....................... #

    def prefix(self, value: str) -> Spec:
        """Build a prefix selector."""

        return _Prefix(value=value)

    # ....................... #

    def suffix(self, value: str) -> Spec:
        """Build a suffix selector."""

        return _Suffix(value=value)

    # ....................... #

    def glob(self, pattern: str) -> Spec:
        """Build a case-sensitive :mod:`fnmatch` selector."""

        return _Glob(pattern=pattern)

    # ....................... #

    def when(self, predicate: Callable[[str], bool]) -> Spec:
        """Build a custom predicate selector."""

        return _When(predicate=predicate)

    # ....................... #

    def matches(self, selector: Spec, key: StrKey) -> bool:
        """Return whether ``key`` is selected by ``selector``."""

        normalized = _normalize_key(key)

        match selector:
            case _AllKeys():
                return True

            case _ExactKeys(keys=keys):
                return normalized in keys

            case _Prefix(value=value):
                return normalized.startswith(value)

            case _Suffix(value=value):
                return normalized.endswith(value)

            case _Glob(pattern=pattern):
                return fnmatch.fnmatchcase(normalized, pattern)

            case _When(predicate=predicate):
                return predicate(normalized)

    # ....................... #

    def iter_matching(
        self,
        selector: Spec,
        keys: Iterable[StrKey],
    ) -> Iterator[str]:
        """Yield normalized keys from ``keys`` that match ``selector`` (stable order)."""

        for key in keys:
            if self.matches(selector, key):
                yield _normalize_key(key)

    # ....................... #

    def specificity(self, selector: Spec) -> int:
        """Return a specificity rank for ordering selectors (higher = more specific).

        Intended for layering patches: apply lower specificity first, higher last.
        Custom ``when`` selectors are intentionally low; prefer structural selectors
        when merge order must be predictable.
        """

        match selector:
            case _AllKeys():
                return 0

            case _When():
                return 1

            case _Glob(pattern=pattern):
                return len(pattern)

            case _Prefix(value=value) | _Suffix(value=value):
                return len(value)

            case _ExactKeys(keys=keys):
                return 1000 + len(keys)

    # ....................... #

    def sort_by_specificity(
        self,
        selectors: Iterable[Spec],
    ) -> tuple[Spec, ...]:
        """Return selectors sorted from lowest to highest :meth:`specificity`."""

        return tuple(sorted(selectors, key=self.specificity))


# ....................... #

str_key_selector: StrKeySelector = StrKeySelector()
"""Singleton :class:`StrKeySelector` API."""
