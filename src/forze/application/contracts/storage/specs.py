from typing import final

import attrs

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StorageSpec(BaseSpec):
    """Storage specification."""

    direct_transfer: bool = False
    """The ports resolved for this spec hand the client the store directly: presigned URLs and
    multipart upload sessions, whose bytes never pass through the application. A
    client-side-encrypting route can serve neither, so it refuses to resolve such a spec with a
    ``configuration`` error. That surfaces at ``check_wiring`` or on the first resolution, not
    on the first transfer."""
