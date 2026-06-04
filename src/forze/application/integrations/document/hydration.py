"""Helpers for post-write read-model hydration in :class:`~forze.application.integrations.document.adapter.DocumentAdapter`."""

from typing import Protocol

from pydantic import BaseModel

from forze.application.contracts.codecs import stored_field_names_for
from forze.base.exceptions import exc

# ----------------------- #


class _CompatGateway(Protocol):
    @property
    def client(self) -> object: ...

    @property
    def tenant_aware(self) -> bool: ...


# ....................... #


def validate_read_write_gateway_compat(
    read_gw: _CompatGateway,
    write_gw: _CompatGateway,
) -> None:
    """Reject read/write document gateways that cannot share a write path.

    The write gateway must use the same client instance and tenant-awareness as
    the read gateway; otherwise a post-write read would target a different
    connection or tenant partition.

    :raises CoreException: When the gateways use different clients or differ in
        tenant awareness.
    """

    if write_gw.client is not read_gw.client:
        raise exc.internal("Write and read gateways must use the same client")

    if write_gw.tenant_aware != read_gw.tenant_aware:
        raise exc.internal(
            "Write and read gateways must have the same tenant awareness."
        )


# ....................... #


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

    read_fields = stored_field_names_for(read_model, include_computed=False)
    domain_fields = stored_field_names_for(domain_model, include_computed=False)

    return read_fields <= domain_fields
