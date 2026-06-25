"""Shared dispatch skeleton for port-wrapping proxies.

A port proxy forwards attribute access to an inner port, wrapping its callable
methods to add one cross-cutting concern — tracing, interception, or resilience.
The dispatch is identical across all three: forward non-callables untouched, then
branch on async-generator / coroutine / plain-sync. Only the per-call behavior
differs. This base owns the dispatch; a subclass overrides the wrap hooks for the
method kinds it cares about (each defaults to passing the method through unchanged)
and optionally narrows which methods are wrapped via :meth:`_should_wrap`.
"""

import inspect
from typing import Any

import attrs

# ----------------------- #


@attrs.define(slots=True)
class PortProxy:
    """Forward attribute access to ``inner``, dispatching callables to wrap hooks.

    Non-callable attributes, and methods :meth:`_should_wrap` rejects, are returned
    from ``inner`` untouched. Each remaining callable is routed to the hook for its
    kind; the default hooks passthrough, so a subclass only writes the kinds it wraps.
    """

    inner: Any
    """The wrapped port."""

    _wrapped_cache: dict[str, Any] = attrs.field(
        factory=dict, init=False, repr=False, eq=False
    )
    """Per-attribute memo of wrapped methods. The inner port and the wrap decision are
    fixed for a proxy's lifetime, so each method is classified and wrapped once instead
    of on every access (every ``ctx.x.method(...)`` goes through :meth:`__getattr__`)."""

    # ....................... #

    def _should_wrap(self, name: str, attr: Any) -> bool:
        """Whether *attr* (already known callable) should be wrapped. Default: always."""

        del name, attr
        return True

    # ....................... #

    def _wrap_async_gen(self, name: str, attr: Any) -> Any:
        """Wrap an async-generator method. Default: passthrough."""

        del name
        return attr

    # ....................... #

    def _wrap_async(self, name: str, attr: Any) -> Any:
        """Wrap a coroutine method. Default: passthrough."""

        del name
        return attr

    # ....................... #

    def _wrap_sync(self, name: str, attr: Any) -> Any:
        """Wrap a plain synchronous method. Default: passthrough."""

        del name
        return attr

    # ....................... #

    def __getattr__(self, name: str) -> Any:
        # Guard the cache slot itself: if it is read before ``__init__`` set it (an
        # unset slot routes here), raise rather than recursing on ``self._wrapped_cache``.
        if name == "_wrapped_cache":
            raise AttributeError(name)

        cache = self._wrapped_cache

        if name in cache:
            return cache[name]

        attr = getattr(self.inner, name)

        # Non-callables and not-wrapped methods stay live (uncached): a non-callable may
        # read mutable inner state, and a bare bound method is already cheap.
        if not callable(attr) or not self._should_wrap(name, attr):
            return attr

        if inspect.isasyncgenfunction(attr):
            wrapped = self._wrap_async_gen(name, attr)
        elif inspect.iscoroutinefunction(attr):
            wrapped = self._wrap_async(name, attr)
        else:
            wrapped = self._wrap_sync(name, attr)

        cache[name] = wrapped
        return wrapped
