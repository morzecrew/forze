"""Expose per-aggregate field schemas as MCP resources for LLM grounding.

The querying DSL (see :func:`~forze_mcp.prompts.register_dsl_query_prompts`) references field
names, so an agent needs to know *which* fields exist and which are filterable / sortable to
build a valid ``list`` / ``search`` query. :func:`register_schema_resources` publishes one
MCP **resource** per :class:`DocumentSpec` — the read model's JSON schema annotated with the
spec's queryable-field allow-sets — so the model can read the contract directly rather than
guessing. Resources are client-pulled context (URI-addressed), the natural MCP primitive for
this static grounding data.
"""

import json
from typing import Any, Callable

from fastmcp import FastMCP
from fastmcp.resources import Resource

from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import exc

# ----------------------- #


def _schema_payload(spec: DocumentSpec[Any, Any, Any, Any]) -> dict[str, Any]:
    """Build the discovery payload for one aggregate spec."""

    return {
        "aggregate": str(spec.name),
        "read_schema": spec.read.model_json_schema(),
        "filterable_fields": sorted(spec.filterable_fields()),
        "sortable_fields": sorted(spec.sortable_fields()),
        "aggregatable_fields": sorted(spec.aggregatable_fields()),
        "default_sort": dict(spec.default_sort) if spec.default_sort else None,
    }


def _resource_provider(payload: dict[str, Any]) -> Callable[[], str]:
    """Bind *payload* into a zero-arg resource reader (JSON text)."""

    def _read() -> str:
        return json.dumps(payload)

    return _read


# ....................... #


def register_schema_resources(
    server: FastMCP,
    *specs: DocumentSpec[Any, Any, Any, Any],
    prefix: str = "schema",
) -> list[str]:
    """Attach a field-schema resource per aggregate spec to *server*.

    Each spec becomes a resource at ``{prefix}://{spec.name}`` whose JSON body carries the
    read model's schema plus the resolved filterable / sortable field sets and default sort —
    everything an LLM needs to construct valid queries for that aggregate. ``add_resource`` is
    additive, so these coexist with any resources you registered.

    :param server: A FastMCP server the caller owns.
    :param specs: The document specs to publish (supply specs, not models — capability
        metadata lives on the spec).
    :param prefix: URI scheme for the resources (default ``"schema"``).
    :returns: The list of registered resource URIs.
    :raises CoreException: When a spec is marked ``sensitive`` — its read model
        carries credential/secret material and must not be published.
    """

    # Refuse sensitive specs up front (before any resource is added).
    for spec in specs:
        if spec.sensitive:
            raise exc.configuration(
                f"Refusing to register schema resource for spec '{spec.name}': "
                "it is marked sensitive=True (its read model carries "
                "credential/secret material that must not be exposed on "
                "generated external surfaces)"
            )

    uris: list[str] = []

    for spec in specs:
        uri = f"{prefix}://{spec.name}"

        server.add_resource(
            Resource.from_function(
                _resource_provider(_schema_payload(spec)),
                uri=uri,
                name=f"{spec.name} field schema",
                description=(
                    f"Read-model field schema for the {str(spec.name)!r} aggregate, with the "
                    "fields that are filterable / sortable in list/search queries."
                ),
                mime_type="application/json",
            )
        )
        uris.append(uri)

    return uris
