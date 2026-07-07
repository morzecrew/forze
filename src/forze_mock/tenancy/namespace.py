"""Resolve mock adapter namespaces from static or tenant-scoped specs."""

from uuid import UUID

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    RelationSpec,
    ValueResolver,
    resolve_value,
)

# ----------------------- #


def resolve_mock_namespace_sync(
    *,
    default: str,
    relation: RelationSpec | None = None,
    namespace: NamedResourceSpec | None = None,
    tenant_id: UUID | None = None,
) -> str:
    """Resolve namespace at DI time for static relation/namespace specs.

    Dynamic :class:`~forze.application.contracts.resolution.ValueResolver` values
    require :func:`resolve_mock_namespace` from async adapter entrypoints.
    """

    del tenant_id

    if relation is not None:
        if isinstance(relation, tuple):
            ns, name = relation  # pyright: ignore[reportUnknownVariableType]
            return f"{ns}/{name}"

        if isinstance(relation, str):
            return relation

    if namespace is not None and isinstance(namespace, str):
        return namespace

    return default


async def resolve_mock_namespace(
    spec: str | ValueResolver[str] | tuple[str, str],
    tenant_id: UUID | None,
) -> str:
    """Resolve a namespace string from a static name, resolver, or relation pair."""

    if isinstance(spec, tuple):
        ns, name = spec  # pyright: ignore[reportUnknownVariableType]
        return f"{ns}/{name}"

    return await resolve_value(spec, tenant_id)
