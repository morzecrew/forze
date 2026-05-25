"""HTTP route policy protocol and merge helpers."""

from dataclasses import dataclass, field
from typing import Any, Protocol, Sequence, runtime_checkable

from fastapi.routing import APIRoute

# ----------------------- #


@runtime_checkable
class Policy(Protocol):
    """Cross-cutting behavior applied when registering a Forze HTTP route."""

    def route_dependencies(self) -> Sequence[Any]:
        """FastAPI dependencies (for example ``Depends``, ``Security``)."""
        ...

    def openapi_extra(self) -> dict[str, Any] | None:
        """Fragment merged into the route ``openapi_extra``."""
        ...

    def route_class(self) -> type[APIRoute] | None:
        """Optional route class override (for example idempotency in a later PR)."""
        ...


# ....................... #


def deep_merge_openapi_extra(
    base: dict[str, Any],
    fragment: dict[str, Any],
) -> dict[str, Any]:
    out = dict(base)

    for key, value in fragment.items():
        if key == "components" and "components" in out:
            components = dict(out["components"])
            frag_components = dict(value)
            schemes = dict(components.get("securitySchemes", {}))
            schemes.update(frag_components.get("securitySchemes", {}))
            components["securitySchemes"] = schemes
            for sub_key, sub_val in frag_components.items():
                if sub_key != "securitySchemes":
                    components[sub_key] = sub_val
            out["components"] = components
        elif key == "security" and "security" in out:
            existing = list(out["security"])
            existing.extend(value)
            out["security"] = existing
        else:
            out[key] = value

    return out


# ....................... #


@dataclass(frozen=True, slots=True)
class MergedPolicies:
    """Result of merging router-level and route-level policies."""

    dependencies: list[Any] = field(default_factory=list)
    openapi_extra: dict[str, Any] = field(default_factory=dict)
    route_class: type[APIRoute] | None = None
    policies: tuple[Policy, ...] = ()


# ....................... #


def merge_policies(*policy_groups: Sequence[Policy]) -> MergedPolicies:
    """Merge policy sequences (router defaults first, route overrides last)."""

    dependencies: list[Any] = []
    openapi_extra: dict[str, Any] = {}
    route_class: type[APIRoute] | None = None
    flat: list[Policy] = []

    for group in policy_groups:
        for policy in group:
            flat.append(policy)
            dependencies.extend(policy.route_dependencies())
            fragment = policy.openapi_extra()

            if fragment:
                openapi_extra = deep_merge_openapi_extra(openapi_extra, fragment)

            override = policy.route_class()

            if override is not None:
                route_class = override

    return MergedPolicies(
        dependencies=dependencies,
        openapi_extra=openapi_extra,
        route_class=route_class,
        policies=tuple(flat),
    )
