"""Redis-backed revocation states: one script per call, on the server's clock.

A principal's keys share one hash tag, so its epoch and a state are read in one script. Every
expiry is an instant on the server's ``TIME`` or a ``PX`` relative to it, never a local clock.
"""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Final, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    EPOCH_STATE_KEY,
    REVOKED_STATE,
    RevocationStatesPort,
    RevocationStatesRead,
)
from forze.base.exceptions import exc

from ..kernel.scripts import (
    REVOCATION_FILL,
    REVOCATION_NOW,
    REVOCATION_RAISE_EPOCH,
    REVOCATION_READ,
)
from .base import RedisBaseAdapter

# ----------------------- #

_SCOPE: Final[str] = "revocation"

# ....................... #


def _present(field: str) -> str | None:
    return field[1:] if field.startswith("+") else None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisRevocationStatesAdapter(RedisBaseAdapter, RevocationStatesPort):
    """Cached revocation states in Redis, under ``{<principal id>}`` hash tags."""

    def _key(self, principal_id: UUID, key: str) -> str:
        return self.construct_key(_SCOPE, f"{{{principal_id}}}", key)

    # ....................... #

    async def read(self, principal_id: UUID, key: str) -> RevocationStatesRead:
        await self._prepare_keys()
        raw = await self.client.run_script(
            REVOCATION_READ,
            [self._key(principal_id, EPOCH_STATE_KEY), self._key(principal_id, key)],
            [],
        )

        try:
            now_ms, epoch, state = raw.split("\n", 2)

            return RevocationStatesRead(
                now_ms=int(now_ms),
                epoch=_present(epoch),
                state=_present(state),
            )

        except ValueError as error:
            raise exc.infrastructure("Unreadable revocation states reply") from error

    async def fill(
        self,
        principal_id: UUID,
        states: Mapping[str, str],
        *,
        expire_at_ms: int,
    ) -> None:
        if not states:
            return

        await self._prepare_keys()
        await self.client.run_script(
            REVOCATION_FILL,
            [self._key(principal_id, key) for key in states],
            [expire_at_ms, *states.values()],
        )

    async def revoke(self, principal_id: UUID, keys: Sequence[str], *, ttl: timedelta) -> None:
        if not keys:
            return

        await self._prepare_keys()
        await self.client.mset(
            {self._key(principal_id, key): REVOKED_STATE for key in keys},
            px=ttl // timedelta(milliseconds=1),
        )

    async def now_ms(self) -> int:
        return int(await self.client.run_script(REVOCATION_NOW, [], []))

    async def raise_epoch(
        self,
        principal_id: UUID,
        epoch: int,
        *,
        expire_at_ms: int,
    ) -> None:
        await self._prepare_keys()
        await self.client.run_script(
            REVOCATION_RAISE_EPOCH,
            [self._key(principal_id, EPOCH_STATE_KEY)],
            [str(epoch), expire_at_ms],
        )
