"""S3 dependency module for the application kernel."""

from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.storage import StorageDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import S3Client
from .configs import S3StorageConfig
from .deps import ConfigurableS3Storage
from .keys import S3ClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class S3DepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers S3 client and storage port.

    Invoke to produce a :class:`Deps` container with S3-backed storage
    dependencies. The client must be initialized separately (e.g. via
    :func:`s3_lifecycle_step`) before usecases run.
    """

    client: S3Client
    """Pre-constructed S3 client (session not yet initialized)."""

    storages: Mapping[K, S3StorageConfig] | None = attrs.field(default=None)
    """Mapping from storage names to their S3-specific configurations."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        """Build a dependency container with S3-backed storage port.

        :returns: Deps with client and storage port factory.
        """

        plain_deps = Deps[K].plain({S3ClientDepKey: self.client})
        storage_deps = Deps[K]()

        if self.storages:
            storage_deps = storage_deps.merge(
                Deps[K].routed(
                    {
                        StorageDepKey: {
                            name: ConfigurableS3Storage(config=config)
                            for name, config in self.storages.items()
                        }
                    }
                )
            )

        return plain_deps.merge(storage_deps)
