"""S3 storage execution configs."""

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
class S3StorageConfig(TenantAwareIntegrationConfig):
    """Configuration for the S3 storage."""

    bucket: NamedResourceSpec = attrs.field(converter=coerce_named_resource_spec)
    """The name of the bucket to use for the storage (static or tenant-scoped resolver)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if is_static_named_resource(self.bucket) and not self.bucket:
            raise exc.configuration("S3 storage config requires bucket.")
