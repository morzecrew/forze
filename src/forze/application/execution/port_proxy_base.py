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
        attr = getattr(self.inner, name)

        if not callable(attr) or not self._should_wrap(name, attr):
            return attr

        if inspect.isasyncgenfunction(attr):
            return self._wrap_async_gen(name, attr)

        if inspect.iscoroutinefunction(attr):
            return self._wrap_async(name, attr)

        return self._wrap_sync(name, attr)
