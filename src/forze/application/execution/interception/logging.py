"""A production port interceptor that logs every port call, uniformly.

Instrumenting the interception seam logs all outbound I/O in one place — no per-adapter
log calls, one consistent shape ``(surface, route, op, duration)``. It rides the same
chain as the simulation interceptors, **inside** the resilience wrap, so a logged call is
the attempt that actually reached the backend (a retried call logs once per attempt).

Volume-safe by construction: a successful call logs at ``trace`` — a single integer
compare in production unless ``configure_logging(level="trace")`` is set — so per-call
logging on hot paths costs nothing by default. An expected domain failure logs at
``debug``; only an unexpected non-domain exception logs at ``warning``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing
from time import perf_counter
from typing import Any, cast

import attrs

from forze.base.exceptions import CoreException
from forze.base.logging import Logger, resolve_logger

from .protocol import PortCall, PortNext, StreamPortNext

# ----------------------- #


def _domain_of(surface: str | None) -> str:
    """The port family from its surface (``document_command`` -> ``document``)."""

    if not surface:
        return "port"

    return surface.split("_", 1)[0]


# ....................... #


@attrs.define(slots=True, frozen=True)
class LoggingInterceptor:
    """Log each port call with its surface, route, op, and duration.

    Logs under ``forze.integrations.<domain>`` by default (per the port's surface), or
    under *logger* when supplied. Success at ``trace``, an expected :class:`CoreException`
    at ``debug`` (the backend's own outcome — the operation boundary owns error logging),
    and an unexpected exception at ``warning`` with its traceback.
    """

    logger: Logger | None = None
    """Optional logger override; defaults to ``forze.integrations.<domain>``."""

    # ....................... #

    async def around(self, call: PortCall, nxt: PortNext) -> Any:
        log = resolve_logger(self.logger, domain=_domain_of(call.surface))
        start = perf_counter()

        try:
            result = await nxt(call)

        except CoreException as error:
            log.debug(
                "port call failed",
                surface=call.surface,
                route=call.route,
                op=call.op,
                duration_ms=round((perf_counter() - start) * 1000, 2),
                error_kind=error.kind.value,
            )
            raise

        except Exception:
            log.warning(
                "port call raised",
                surface=call.surface,
                route=call.route,
                op=call.op,
                duration_ms=round((perf_counter() - start) * 1000, 2),
                exc_info=True,
            )
            raise

        log.trace(
            "port call",
            surface=call.surface,
            route=call.route,
            op=call.op,
            duration_ms=round((perf_counter() - start) * 1000, 2),
        )

        return result

    # ....................... #

    async def around_stream(self, call: PortCall, nxt: StreamPortNext) -> AsyncIterator[Any]:
        """Log a streamed port call once for the *whole* stream — its item count and total
        duration, and a mid-stream failure at the item it broke on — rather than timing only
        the (near-instant) iterator acquisition and reporting success for a stream that later
        fails. Same levels as :meth:`around`: success ``trace``, expected ``CoreException``
        ``debug``, unexpected exception ``warning`` with its traceback."""

        log = resolve_logger(self.logger, domain=_domain_of(call.surface))
        start = perf_counter()
        items = 0

        # ``aclosing`` closes the inner stream deterministically on any exit (consumer
        # ``aclose``, early break, a thrown-in exception) — a backend cursor is released
        # at scope exit, not whenever GC finalizes an abandoned generator. Abandonment is
        # not an error: ``GeneratorExit`` bypasses the handlers below, so nothing is logged.
        async with aclosing(cast("AsyncGenerator[Any]", nxt(call))) as stream:
            try:
                async for item in stream:
                    items += 1
                    yield item

            except CoreException as error:
                log.debug(
                    "port stream failed",
                    surface=call.surface,
                    route=call.route,
                    op=call.op,
                    items=items,
                    duration_ms=round((perf_counter() - start) * 1000, 2),
                    error_kind=error.kind.value,
                )
                raise

            except Exception:
                log.warning(
                    "port stream raised",
                    surface=call.surface,
                    route=call.route,
                    op=call.op,
                    items=items,
                    duration_ms=round((perf_counter() - start) * 1000, 2),
                    exc_info=True,
                )
                raise

            log.trace(
                "port stream",
                surface=call.surface,
                route=call.route,
                op=call.op,
                items=items,
                duration_ms=round((perf_counter() - start) * 1000, 2),
            )
