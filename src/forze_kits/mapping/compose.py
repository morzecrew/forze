"""Stacking mapper factories that share one slot."""

from typing import Any

from forze.application.contracts.mapping import Mapper, MapperFactory

# ----------------------- #


def compose_mapper_factories(
    first: MapperFactory[Any, Any] | None,
    second: MapperFactory[Any, Any],
) -> MapperFactory[Any, Any]:
    """A mapper factory running *first* then *second*, or *second* alone when there is none.

    The kit's arms share the document factory's mapper slots, so an arm that assigned its own
    would drop the one before it — the author's mapper, or another arm's restriction. Each
    mapper works on what the previous one produced, which is what makes stacked filters mean
    "both restrictions".
    """

    if first is None:
        return second

    def _factory(ctx: Any) -> Mapper[Any, Any]:
        before = first(ctx)
        after = second(ctx)

        async def _map(source: Any) -> Any:
            return await after(await before(source))

        return _map

    return _factory
