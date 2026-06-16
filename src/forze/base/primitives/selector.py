"""String key selectors for matching opaque operation keys."""

from __future__ import annotations

import fnmatch
from typing import Callable, Iterable, Iterator, TypeAlias, final

import attrs

from ..exceptions import exc
from .namespace import StrKeyNamespace
from .types import StrKey

# ----------------------- #


def _normalize_key(key: StrKey) -> str:
    """
    Normalize a key to its string form.
    
    Returns:
    	str: The string representation of the key.
    """
    return str(key)


# ....................... #


def _require_non_empty(value: str, *, label: str) -> None:
    """
    Validate that a string value is non-empty.
    
    Parameters:
        value (str): The string to validate.
        label (str): A label used in the error message if validation fails.
    
    Raises:
        exc.internal: If value is empty.
    """
    if not value:
        raise exc.internal(f"{label} must be non-empty")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _AllKeys:
    """Select every key."""

    except_keys: frozenset[str] = attrs.field(factory=frozenset)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _ExactKeys:
    """Select keys that match one of the given literals exactly."""

    keys: frozenset[str]

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.keys:
            raise exc.internal("ExactKeys.keys must be non-empty")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _Prefix:
    """Select keys whose string form starts with ``value``."""

    value: str

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_non_empty(self.value, label="Prefix.value")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _Suffix:
    """Select keys whose string form ends with ``value``."""

    value: str

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_non_empty(self.value, label="Suffix.value")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _Glob:
    """Select keys that match a :mod:`fnmatch` pattern (case-sensitive)."""

    pattern: str

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_non_empty(self.pattern, label="Glob.pattern")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _When:
    """Select keys that satisfy a custom predicate."""

    predicate: Callable[[str], bool]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _Namespaced:
    """Scope an inner selector to a namespace, matching against the relative key.

    A key matches only when it starts with ``prefix + sep`` and the remainder
    (the namespace-relative portion) matches ``inner``. This lets a selector be
    authored in the same relative terms as operation keys and remounted under a
    namespace without hardcoding the absolute prefix.
    """

    prefix: str
    sep: str
    inner: StrKeySelector.Spec

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_non_empty(self.prefix, label="Namespaced.prefix")
        _require_non_empty(self.sep, label="Namespaced.sep")


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

    Spec: TypeAlias = (
        _AllKeys | _ExactKeys | _Prefix | _Suffix | _Glob | _When | _Namespaced
    )
    """Tagged union of selector strategies returned by factory methods."""

    # ....................... #

    @staticmethod
    def all_keys(*except_: StrKey) -> Spec:
        """
        Creates a selector matching all keys except those specified.
        
        Parameters:
            *except_: Keys to exclude from matching.
        """

        return _AllKeys(except_keys=frozenset(map(_normalize_key, except_)))

    # ....................... #

    @staticmethod
    def exact(*keys: StrKey) -> Spec:
        """
        Create a selector matching keys by exact literal equality.
        
        Parameters:
            *keys: One or more keys to match exactly.
        
        Returns:
            A Spec selector that matches only the specified keys.
        """

        if not keys:
            raise exc.internal("exact() requires at least one key")

        return _ExactKeys(keys=frozenset(map(_normalize_key, keys)))

    # ....................... #

    @staticmethod
    def prefix(value: str) -> Spec:
        """
        Create a selector that matches keys starting with a given prefix.
        
        Parameters:
        	value (str): The prefix string that keys must start with. Must be non-empty.
        
        Returns:
        	Spec: A selector spec for prefix matching.
        """

        return _Prefix(value=value)

    # ....................... #

    @staticmethod
    def suffix(value: str) -> Spec:
        """
        Create a selector that matches keys ending with a given value.
        
        Returns:
            Spec: A suffix selector.
        """

        return _Suffix(value=value)

    # ....................... #

    @staticmethod
    def glob(pattern: str) -> Spec:
        """Build a case-sensitive :mod:`fnmatch` selector."""

        return _Glob(pattern=pattern)

    # ....................... #

    @staticmethod
    def when(predicate: Callable[[str], bool]) -> Spec:
        """
        Creates a selector that matches keys where the predicate returns true.
        
        Parameters:
            predicate (Callable[[str], bool]): A function that takes a normalized key string and returns `true` if the key matches.
        
        Returns:
            Spec: A selector spec applying the predicate to normalized keys.
        """

        return _When(predicate=predicate)

    # ....................... #

    @staticmethod
    def in_namespace(namespace: StrKeyNamespace, selector: Spec) -> Spec:
        """
        Scope selector to a namespace, matching the namespace-relative key.
        
        The resulting selector matches a key only when it starts with the namespace
        boundary (prefix + sep); the inner selector is then tested against the
        namespace-relative remainder.
        """

        return _Namespaced(
            prefix=str(namespace.prefix),
            sep=namespace.sep,
            inner=selector,
        )

    # ....................... #

    def matches(self, selector: Spec, key: StrKey) -> bool:
        """
        Determine whether a key matches the given selector specification.
        
        Parameters:
            selector: A selector specification.
            key: The key to test against the selector.
        
        Returns:
            True if the key matches the selector, False otherwise.
        """

        normalized = _normalize_key(key)

        match selector:
            case _AllKeys(except_keys=except_keys):
                return key not in except_keys

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

            case _Namespaced(prefix=prefix, sep=sep, inner=inner):
                boundary = prefix + sep

                if not normalized.startswith(boundary):
                    return False

                return self.matches(inner, normalized[len(boundary) :])

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
        """
        Compute a specificity rank for a selector.
        
        Higher ranks indicate more specific selectors. Rankings are determined by selector type and content characteristics (e.g., exact matches rank highest, generic selectors rank lowest).
        
        Returns:
            An integer rank, where higher values indicate more specific selectors.
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

            case _Namespaced(prefix=prefix, sep=sep, inner=inner):
                # The namespace boundary is an added constraint, so a scoped
                # selector is strictly more specific than its bare inner.
                return len(prefix) + len(sep) + self.specificity(inner)

    # ....................... #

    def sort_by_specificity(
        self,
        selectors: Iterable[Spec],
    ) -> tuple[Spec, ...]:
        """
        Orders selectors by increasing specificity.
        
        Returns:
            tuple[Spec, ...]: The input selectors as a tuple, ordered from lowest to highest specificity.
        """

        return tuple(sorted(selectors, key=self.specificity))


# ....................... #

str_key_selector: StrKeySelector = StrKeySelector()
"""Singleton :class:`StrKeySelector` API."""
