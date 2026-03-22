from typing import TYPE_CHECKING, Protocol, Sequence, runtime_checkable

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


@runtime_checkable
class MapperPort[In, Out](Protocol):
    """Protocol for a mapper that maps a single source to a single output."""

    async def __call__(
        self,
        ctx: "ExecutionContext",
        source: In,  # noqa: F841
        /,
    ) -> Out: ...


# ....................... #


@runtime_checkable
class LocalMapperPort[In, Out](Protocol):  # pragma: no cover
    """Protocol for a mapper that maps a single source to a single output."""

    async def __call__(self, source: In, /) -> Out: ...  # noqa: F841


# ....................... #


@runtime_checkable
class BatchMapperPort[In, Out](Protocol):  # pragma: no cover
    """Protocol for a mapper that maps a sequence of sources to a sequence of outputs."""

    async def __call__(
        self,
        ctx: "ExecutionContext",
        sources: Sequence[In],  # noqa: F841
        /,
    ) -> Sequence[Out]: ...


# ....................... #


@runtime_checkable
class LocalBatchMapperPort[In, Out](Protocol):  # pragma: no cover
    """Protocol for a mapper that maps a sequence of sources to a sequence of outputs."""

    async def __call__(
        self,
        sources: Sequence[In],  # noqa: F841
        /,
    ) -> Sequence[Out]: ...


# ....................... #


@runtime_checkable
class FanOutMapperPort[In, Out](Protocol):  # pragma: no cover
    """Protocol for a mapper that maps a single source to a sequence of outputs."""

    async def __call__(
        self,
        ctx: "ExecutionContext",
        source: In,  # noqa: F841
        /,
    ) -> Sequence[Out]: ...


# ....................... #


@runtime_checkable
class LocalFanOutMapperPort[In, Out](Protocol):  # pragma: no cover
    """Protocol for a mapper that maps a single source to a sequence of outputs."""

    async def __call__(self, source: In, /) -> Sequence[Out]: ...  # noqa: F841


# ....................... #


@runtime_checkable
class ReducerMapperPort[In, Out](Protocol):  # pragma: no cover
    """Protocol for a mapper that maps a sequence of sources to a single output."""

    async def __call__(
        self,
        ctx: "ExecutionContext",
        sources: Sequence[In],  # noqa: F841
        /,
    ) -> Out: ...


# ....................... #


@runtime_checkable
class LocalReducerMapperPort[In, Out](Protocol):  # pragma: no cover
    """Protocol for a mapper that maps a sequence of sources to a single output."""

    async def __call__(
        self,
        sources: Sequence[In],  # noqa: F841
        /,
    ) -> Out: ...
