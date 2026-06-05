"""Lazy memoization cell for frozen owners."""

from typing import Awaitable, Callable

import attrs

# ----------------------- #


@attrs.define(slots=True, eq=False, repr=False)
class OnceCell[T]:
    """Mutable single-value memo cell for lazy initialization on frozen owners.

    A frozen :mod:`attrs` object holds the cell in an ``init=False`` field; the
    value is memoized by mutating the *cell* (not rebinding the owner's
    attribute), so no frozen-attribute bypass is needed and the owner stays
    field-immutable.

    ``None`` is treated as "unset"; cached values are therefore expected to be
    non-``None`` (as for resolved relation/namespace/index targets, or a computed
    boolean flag — ``False`` is a set value, distinct from ``None``).
    """

    _value: T | None = attrs.field(default=None, init=False)

    # ....................... #

    def peek(self) -> T | None:
        """Return the memoized value, or ``None`` when not yet set."""

        return self._value

    # ....................... #

    def set(self, value: T) -> T:
        """Memoize *value* and return it."""

        self._value = value

        return value

    # ....................... #

    def get_or_compute(self, factory: Callable[[], T]) -> T:
        """Return the memoized value, computing and caching it via *factory* once."""

        if self._value is None:
            self._value = factory()

        return self._value

    # ....................... #

    async def resolve(
        self,
        factory: Callable[[], Awaitable[T]],
        *,
        cache: bool = True,
    ) -> T:
        """Resolve via *factory*, memoizing only when ``cache`` is true.

        With ``cache=False`` the value is resolved fresh on every call — used for
        tenant-scoped (dynamic) resolvers on adapters shared across tenants, where
        memoizing one tenant's result would leak it to others.
        """

        if cache and self._value is not None:
            return self._value

        value = await factory()

        if cache:
            self._value = value

        return value
