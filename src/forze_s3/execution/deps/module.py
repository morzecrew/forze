"""S3 dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.storage import StorageDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import S3Client
from .deps import s3_storage
from .keys import S3ClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class S3DepsModule(DepsModule):
    """Dependency module that registers S3 client and storage port.

    Invoke to produce a :class:`Deps` container with S3-backed storage
    dependencies. The client must be initialized separately (e.g. via
    :func:`s3_lifecycle_step`) before usecases run.
    """

    client: S3Client
    """Pre-constructed S3 client (session not yet initialized)."""

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with S3-backed storage port.

        :returns: Deps with client and storage port factory.
        """
        return Deps(
            {
                S3ClientDepKey: self.client,
                StorageDepKey: s3_storage,
            }
        )
