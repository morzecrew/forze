"""GCS storage execution configs."""

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    is_static_named_resource,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GCSStorageConfig(TenantAwareIntegrationConfig):
    """Configuration for a GCS-backed storage route."""

    bucket: NamedResourceSpec = attrs.field(converter=coerce_named_resource_spec)
    """GCS bucket name (static or tenant-scoped resolver)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if is_static_named_resource(self.bucket) and not self.bucket:
            raise exc.configuration("GCS storage config requires bucket.")
