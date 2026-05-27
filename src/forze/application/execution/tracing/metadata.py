"""Derive tracing dimensions from dependency keys and specs."""

from forze.application.contracts.deps import DepKey
from forze.base.primitives import StrKey

# ----------------------- #


def infer_port_metadata(
    key: DepKey[object],
    spec: object,
    *,
    route: StrKey | None,
) -> tuple[str, str, str | None, str | None]:
    """Return ``(domain, surface, route, phase)`` for a configurable port resolution."""

    surface = key.name
    phase: str | None = None

    if surface.endswith("_query"):
        phase = "query"
    elif surface.endswith("_command"):
        phase = "command"

    domain = surface.split("_", 1)[0] if "_" in surface else surface
    route_name = getattr(spec, "name", None)

    if route_name is None and route is not None:
        route_name = str(getattr(route, "value", route))

    return domain, surface, route_name, phase
