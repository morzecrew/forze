from datetime import timedelta

import attrs

from forze.base.exceptions import exc

from ..base import BaseSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class IdempotencySpec(BaseSpec):
    """Specification for idempotency behavior."""

    ttl: timedelta = timedelta(seconds=30)
    """Time-to-live for the idempotency snapshot."""

    encrypt_result: bool = False
    """Seal the cached operation **result** at rest (default ``False``).

    The idempotency store caches an operation's full return value for replay — potentially
    sensitive business data (e.g. a created order, a user record), in plaintext in a
    Forze-owned store (Redis/Postgres). When ``True``, the result bytes are sealed on commit
    and opened on replay (AAD binds tenant + operation/key); the status/hash metadata stays
    plaintext. Requires a wired keyring (``KeyringDepKey``) — fail-closed otherwise. Records
    written before enabling this still replay (envelope sniff)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("TTL must be positive")
