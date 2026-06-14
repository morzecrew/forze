"""Static or tenant-scoped resource specs for integration configs."""

from typing import Any, TypeGuard

from forze.base.exceptions import exc

from .types import ValueResolver

# ----------------------- #

RelationSpec = tuple[str, str] | ValueResolver[tuple[str, str]]
"""Static ``(namespace, name)`` pair or tenant-scoped resolver (schema/table, db/collection, etc.)."""

NamedResourceSpec = str | ValueResolver[str]
"""Static resource name or tenant-scoped resolver (bucket, queue namespace, index UID, etc.)."""

# ....................... #


def coerce_relation_spec(value: Any) -> RelationSpec:
    """Normalize config input to :data:`RelationSpec`."""

    if callable(value):
        return value  # type: ignore[return-value]

    if isinstance(value, tuple) and len(value) == 2:  # type: ignore[arg-type]
        namespace, name = value  # type: ignore[misc]
        return (str(namespace), str(name))  # type: ignore[return-value]

    raise exc.configuration(
        "Relation must be a (namespace, name) tuple or a callable resolver",
    )


# ....................... #


def coerce_named_resource_spec(value: Any) -> NamedResourceSpec:
    """Normalize config input to :data:`NamedResourceSpec`."""

    if callable(value):
        return value  # type: ignore[return-value]

    if isinstance(value, str):
        return value

    enum_value = getattr(value, "value", None)

    if isinstance(enum_value, str):
        return enum_value

    raise exc.configuration(
        "Named resource must be a str or a callable resolver",
    )


def coerce_optional_named_resource_spec(value: Any) -> NamedResourceSpec | None:
    """Like :func:`coerce_named_resource_spec`, but passes ``None`` through unchanged."""

    return None if value is None else coerce_named_resource_spec(value)


# ....................... #


def is_static_relation(spec: RelationSpec) -> TypeGuard[tuple[str, str]]:
    return isinstance(spec, tuple)


# ....................... #


def is_static_named_resource(spec: NamedResourceSpec) -> TypeGuard[str]:
    return isinstance(spec, str)


# ....................... #


def require_static_relation(
    spec: RelationSpec,
    *,
    route_name: str,
    field: str,
    integration: str = "integration",
    omit_hint: str = "Omit startup validation for this route, or use static relations.",
) -> tuple[str, str]:
    """Return *spec* when static; fail for dynamic resolvers at startup validation."""

    if is_static_relation(spec):
        return spec

    raise exc.configuration(
        f"{integration} route {route_name!r}: {field} uses a dynamic RelationSpec resolver; "
        f"startup validation requires static (namespace, name) tuples. {omit_hint}",
        code="dynamic_relation_startup_validation",
        details={"route": route_name, "field": field, "integration": integration},
    )


# ....................... #


def require_static_named_resource(
    spec: NamedResourceSpec,
    *,
    route_name: str,
    field: str,
    integration: str = "integration",
    omit_hint: str = "Omit startup validation for this route, or use a static name.",
) -> str:
    """Return *spec* when static; fail for dynamic resolvers at startup validation."""

    if is_static_named_resource(spec):
        return spec

    raise exc.configuration(
        f"{integration} route {route_name!r}: {field} uses a dynamic NamedResourceSpec "
        f"resolver; startup validation requires a static name. {omit_hint}",
        code="dynamic_named_resource_startup_validation",
        details={"route": route_name, "field": field, "integration": integration},
    )
