"""ContextVar-scoped resource nesting for integration kernel clients."""

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncGenerator, Awaitable, Callable, final

import attrs

from .._logger import logger

# ----------------------- #


@final
@attrs.define(slots=True, eq=False, repr=False)
class ContextScopedResource[R]:
    """Depth-tracked, task-local resource scope over a pair of ContextVars.

    The outermost :meth:`scope` entry acquires the resource via ``factory``
    and binds it to the current context; nested entries reuse the bound
    resource as long as the optional ``reusable`` predicate accepts it (e.g.
    a channel that is still open) — otherwise a fresh resource is acquired
    and bound for the inner scope. Whichever entry created a binding resets
    the context vars in ``finally`` (exception-safe via tokens) and then
    invokes the optional ``closer``. Concurrent tasks see independent scopes
    thanks to :class:`contextvars.ContextVar` isolation.
    """

    name: str
    """Name of the resource."""

    _ctx_resource: ContextVar[R | None] = attrs.field(
        default=attrs.Factory(
            lambda self: ContextVar(f"{self.name}_resource", default=None),
            takes_self=True,
        ),
        init=False,
    )
    """Context variable for the resource."""

    _ctx_depth: ContextVar[int] = attrs.field(
        default=attrs.Factory(
            lambda self: ContextVar(f"{self.name}_depth", default=0), takes_self=True
        ),
        init=False,
    )
    """Context variable for the depth."""

    # ....................... #

    def current(self) -> R | None:
        """Return the resource bound to the current context, if any."""

        return self._ctx_resource.get()

    # ....................... #

    @asynccontextmanager
    async def scope(
        self,
        factory: Callable[[], Awaitable[R]],
        *,
        closer: Callable[[R], Awaitable[None]] | None = None,
        reusable: Callable[[R], bool] | None = None,
    ) -> AsyncGenerator[R]:
        """Yield the scope's resource (outermost acquires, nested reuse)."""

        depth = self._ctx_depth.get()
        bound = self._ctx_resource.get()

        if depth > 0 and bound is not None and (reusable is None or reusable(bound)):
            token_depth = self._ctx_depth.set(depth + 1)

            try:
                yield bound

            finally:
                self._ctx_depth.reset(token_depth)

            return

        resource = await factory()
        token_resource = self._ctx_resource.set(resource)
        token_depth = self._ctx_depth.set(1)

        body_failed = False

        try:
            yield resource

        except BaseException:
            body_failed = True
            raise

        finally:
            self._ctx_depth.reset(token_depth)
            self._ctx_resource.reset(token_resource)

            if closer is not None:
                try:
                    await closer(resource)

                except Exception as e:
                    # Never mask the body's exception with a teardown error;
                    # on the happy path a failing closer still propagates.
                    if not body_failed:
                        raise

                    logger.warning(
                        "Suppressed %s closer error after scope body failure: %s",
                        self.name,
                        e,
                    )
