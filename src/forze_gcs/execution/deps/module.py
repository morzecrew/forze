"""GCS dependency module for the application kernel."""

from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.storage import StorageDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import GCSClientPort
from .configs import GCSStorageConfig
from .deps import ConfigurableGCSStorage
from .keys import GCSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GCSDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers GCS client and storage port."""

    client: GCSClientPort
    """Pre-constructed GCS client (initialized via :func:`gcs_lifecycle_step`)."""

    storages: Mapping[K, GCSStorageConfig] | None = attrs.field(default=None)
    """Mapping from storage route names to GCS bucket configuration."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        plain_deps = Deps[K].plain({GCSClientDepKey: self.client})
        storage_deps = Deps[K]()

        if self.storages:
            storage_deps = storage_deps.merge(
                Deps[K].routed(
                    {
                        StorageDepKey: {
                            name: ConfigurableGCSStorage(config=config)
                            for name, config in self.storages.items()
                        }
                    }
                )
            )

        return plain_deps.merge(storage_deps)
