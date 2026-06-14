from forze_redis._compat import require_redis

require_redis()

# ....................... #

from contextvars import ContextVar
from typing import Any

import attrs

from forze.application.contracts.resolution import NamedResourceSpec, is_static_named_resource
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import OnceCell

from ..kernel.client import RedisClientPort
from ..kernel.relation import resolve_redis_namespace
from .codecs import KEY_SEP, RedisKeyCodec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisBaseAdapter(TenancyMixin):
    """Base adapter class for Redis integration."""

    client: RedisClientPort
    """Redis client instance."""

    namespace: NamedResourceSpec
    """Static or tenant-scoped Redis key namespace."""

    key_sep: str = KEY_SEP
    """Separator between key parts."""

    _namespace_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )
    """Memo for a static namespace (tenant-independent; resolved once)."""

    _namespace_cv: ContextVar[str | None] = attrs.field(
        factory=lambda: ContextVar[str | None]("redis_namespace", default=None),
        init=False,
        eq=False,
        repr=False,
        hash=False,
    )
    """Task-local scratchpad for a dynamic (tenant-scoped) namespace.

    The adapter may be shared across tenants/requests (per-scope port cache), and
    operations build keys via the sync :attr:`key_codec` after ``await`` points, so a
    shared instance field would be clobbered by concurrent other-tenant operations. A
    context var keeps each task reading its own resolved namespace."""

    # ....................... #

    async def _prepare_keys(self) -> None:
        """Resolve a dynamic namespace before key construction."""

        if is_static_named_resource(self.namespace):
            return

        await self._resolved_namespace()


    # ....................... #

    async def _resolved_namespace(self) -> str:
        if is_static_named_resource(self.namespace):

            async def _factory() -> str:
                return await resolve_redis_namespace(
                    self.namespace,
                    self._tenant_id_for_resolve(),
                )

            return await self._namespace_cell.resolve(_factory)

        # Dynamic: resolve per call and stash in a task-local var so the shared
        # adapter's sync key_codec reads the current task's tenant namespace, even
        # across awaits and concurrent operations for other tenants.
        resolved = await resolve_redis_namespace(
            self.namespace,
            self._tenant_id_for_resolve(),
        )
        self._namespace_cv.set(resolved)

        return resolved

    # ....................... #

    @property
    def key_codec(self) -> RedisKeyCodec:
        """Key codec using the static memo or the task-local resolved namespace."""

        if is_static_named_resource(self.namespace):
            cached = self._namespace_cell.peek()
            static_ns = cached if cached is not None else self.namespace
            return RedisKeyCodec(namespace=static_ns, sep=self.key_sep)

        dynamic_ns = self._namespace_cv.get()

        if dynamic_ns is None:
            raise exc.internal(
                "key_codec requires a resolved namespace; await _resolved_namespace() first",
            )

        return RedisKeyCodec(namespace=dynamic_ns, sep=self.key_sep)

    # ....................... #

    def __tenant_prefix(self) -> tuple[str, ...] | None:
        """Construct a tenant prefix from attached tenant ID if any."""

        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
            return ("tenant", str(tenant_id))

        return None

    # ....................... #

    def construct_key(self, scope: str | tuple[str, ...], *parts: Any) -> str:
        """Construct a key for the given scope and parts."""

        tenant_prefix = self.__tenant_prefix()

        return self.key_codec.join(tenant_prefix, scope, *parts)
