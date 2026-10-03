"""An opt-in, per-process cache of the catalog grants a principal resolves to."""

import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from datetime import timedelta
from uuid import UUID

import attrs

from forze.application.contracts.authz import AuthzScope, PermissionRef, RoleRef
from forze.base.exceptions import exc

# ----------------------- #

CatalogGrants = tuple[frozenset[RoleRef], frozenset[PermissionRef]]
"""The roles and permissions the catalog bindings grant, without provider-derived ones."""

GrantsCacheKey = tuple[UUID, UUID | None, AuthzScope | None]
"""Principal, the invocation tenant read on the call, and the requested scope."""


@attrs.define(slots=True, kw_only=True, eq=False)
class GrantsCache:
    """Remembers each principal's catalog grants for :attr:`ttl`, per process.

    Pass one to ``AuthzKernelConfig(grants_cache=...)``; without it nothing is cached. What it
    holds is the part of a decision read from role, group and permission bindings. The
    principal's active flag and the permissions providers derive are read on every decision,
    so a deactivation, or a provider's change, applies at once.

    Decisions inside a transaction read the bindings and leave the cache alone, since what they
    read may still roll back.

    The TTL is the revocation delay for every other change: a binding removed by a plain
    document command, or by another process, keeps granting here until the entry expires.
    ``RoleAssignmentPort`` forgets the principal in this process when its write commits. After
    committing another change, call :meth:`forget` when it touches one principal's own bindings
    (its role, permission or group membership bindings), and :meth:`clear` when it touches what
    many principals reach: a role's permissions or parent, a group's roles, permissions or active
    flag, or a deleted role or permission.

    One cache serves one catalog: its key holds no catalog, so two kernels over different
    catalogs need a cache each.

    Its methods do not await and hold a lock, so decisions and forgets in any number of event
    loops or threads cannot interleave inside one.
    """

    ttl: timedelta
    """How long an entry is served after it was read."""

    max_entries: int = 10_000
    """Entries kept; the least recently used goes first."""

    clock: Callable[[], float] = time.monotonic
    """Seconds, for expiry."""

    _entries: OrderedDict[GrantsCacheKey, tuple[float, CatalogGrants]] = attrs.field(
        factory=OrderedDict[GrantsCacheKey, tuple[float, CatalogGrants]],
        init=False,
    )
    _epoch: int = attrs.field(default=0, init=False)
    _lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not isinstance(self.ttl, timedelta) or self.ttl <= timedelta(0):
            raise exc.configuration(
                f"GrantsCache.ttl must be a positive timedelta, not {self.ttl!r}"
            )

        if (
            not isinstance(self.max_entries, int)
            or isinstance(self.max_entries, bool)
            or self.max_entries < 1
        ):
            raise exc.configuration(
                f"GrantsCache.max_entries must be an integer of at least 1, not {self.max_entries!r}"
            )

    # ....................... #

    @property
    def epoch(self) -> int:
        """Advances on every :meth:`forget` and :meth:`clear`; pass it back to :meth:`put`."""

        return self._epoch

    # ....................... #

    def get(self, key: GrantsCacheKey) -> CatalogGrants | None:
        """The grants remembered under *key*, unless absent or expired."""

        with self._lock:
            entry = self._entries.get(key)

            if entry is None:
                return None

            expires_at, grants = entry

            if self.clock() >= expires_at:
                del self._entries[key]
                return None

            self._entries.move_to_end(key)

            return grants

    # ....................... #

    def put(self, key: GrantsCacheKey, grants: CatalogGrants, *, epoch: int) -> None:
        """Remember *grants*, read when :attr:`epoch` was *epoch*.

        Dropped when a forget ran since: the grants may have been read before the change it
        announces.
        """

        # ponytail: one epoch for every principal, so any forget voids every read in flight;
        # per-principal generations if forgets ever become frequent.
        with self._lock:
            if epoch != self._epoch:
                return

            self._entries[key] = (self.clock() + self.ttl.total_seconds(), grants)
            self._entries.move_to_end(key)

            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)

    # ....................... #

    def forget(self, principal_id: UUID) -> None:
        """Drop *principal_id*'s entries in every tenant and scope."""

        with self._lock:
            self._epoch += 1

            for key in [key for key in self._entries if key[0] == principal_id]:
                del self._entries[key]

    # ....................... #

    def clear(self) -> None:
        """Drop every entry."""

        with self._lock:
            self._epoch += 1
            self._entries.clear()
