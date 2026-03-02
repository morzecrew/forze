from typing import TypeVar, final

import attrs

# ----------------------- #

T = TypeVar("T")

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class DepKey[T]:
    """Typed key used to identify dependencies in the kernel.

    The ``name`` is only used for diagnostics; type information is carried
    through the type parameter ``T``.
    """

    name: str
    """Name of the dependency."""
