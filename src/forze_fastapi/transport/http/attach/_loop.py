"""Shared catalog iteration and route resolution for attach_*_routes."""

from collections.abc import Iterable, Iterator, Mapping
from logging import Logger
from typing import TypeVar

from forze.base.errors import CoreError
from forze_fastapi.transport.http._path import path_coerce
from forze_fastapi.transport.http.attach._common import route_opts_from_entry
from forze_fastapi.transport.http.options.route_opts import RouteOpts

# ----------------------- #

TEntry = TypeVar("TEntry")


def resolve_route_path(
    name: str,
    *,
    paths: Mapping[str, str],
    per_route: Mapping[str, RouteOpts | bool | None],
    default_path: str,
) -> str:
    """Resolve final path for operation *name* (bulk override, then per-route, then default)."""

    route_opts = route_opts_from_entry(per_route.get(name))
    override = paths.get(name)
    if override is None and route_opts is not None:
        override = route_opts.get("path_override")
    return path_coerce(override or default_path)


def resolve_include_in_schema(route_opts: RouteOpts | None) -> bool:
    """Return whether the route appears in OpenAPI schema."""

    if route_opts is None or "include_in_schema" not in route_opts:
        return True

    return bool(route_opts["include_in_schema"])


def iter_catalog_operations(
    enabled: Iterable[str],
    operations: Mapping[str, TEntry],
    *,
    strict: bool,
    logger: Logger,
    domain_label: str,
) -> Iterator[tuple[str, TEntry]]:
    """Yield ``(enable_name, catalog_entry)`` for names in *enabled* present in *operations*."""

    for name in enabled:
        if name not in operations:
            if strict:
                raise CoreError(f"Unknown {domain_label} route '{name}'")
            logger.warning("Unknown %s route '%s', skipping", domain_label, name)
            continue
        yield name, operations[name]
