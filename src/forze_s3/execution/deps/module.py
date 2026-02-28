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
    client: S3Client

    # ....................... #

    def __call__(self) -> Deps:
        return Deps(
            {
                S3ClientDepKey: self.client,
                StorageDepKey: s3_storage,
            }
        )
