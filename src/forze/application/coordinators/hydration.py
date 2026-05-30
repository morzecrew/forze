"""Helpers for post-write read-model hydration in :class:`~forze.application.coordinators.document.coordinator.DocumentCoordinator`."""

from pydantic import BaseModel

from forze.base.serialization import pydantic_field_names

# ----------------------- #


def can_hydrate_read_from_write_domain(
    *,
    read_model: type[BaseModel],
    domain_model: type[BaseModel],
    read_source_key: str,
    write_source_key: str,
) -> bool:
    """Return whether a domain row from the write path can validate as the read model.

    Requires the same physical source and read fields that are a subset of domain fields
    (excluding computed read-only fields).
    """

    if read_source_key != write_source_key:
        return False

    read_fields = pydantic_field_names(read_model, include_computed=False)
    domain_fields = pydantic_field_names(domain_model, include_computed=False)

    return read_fields <= domain_fields
