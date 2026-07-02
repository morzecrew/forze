"""Opt-in per-instance logger override for adapters built on shared bases."""

import attrs

from .logger import Logger

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class LoggerAware:
    """Mixin giving a component an optional per-instance logger override.

    Concrete adapters keep logging under their package-local module logger (e.g.
    ``forze_postgres.adapters``) by default. When an instance needs its own identity —
    a routed or per-tenant adapter that wants a distinct logger name or pre-bound
    fields — construct it with ``logger=...`` and call :meth:`logger_or` with the
    module default; the override wins when present.

    The default is passed in per call rather than stored on the mixin so the base has
    no dependency on any package's logger name; the owning adapter supplies it.
    """

    logger: Logger | None = None
    """Optional per-instance override; ``None`` falls back to the caller-supplied default."""

    # ....................... #

    def logger_or(self, default: Logger) -> Logger:
        """Return the per-instance override if set, else *default*."""

        return self.logger if self.logger is not None else default
