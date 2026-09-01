"""Process-level runtime settings — the argument lists of the bootstrap helpers.

:func:`~forze.base.logging.bootstrap_logging` and
:func:`~forze.base.telemetry.bootstrap_telemetry` take their configuration as keyword
arguments, which every deploying application then re-declares as settings fields and
re-picks defaults for. :class:`RuntimeSettings` is that declaration, once.

It is a plain :class:`pydantic.BaseModel` rather than a ``BaseSettings``: the environment
prefix, nesting delimiter, case sensitivity and extra-key policy are deployment decisions,
so the root settings class stays in the application and mounts this as a field.
"""

from typing import Any

from pydantic import BaseModel, computed_field, field_validator

from forze.base.logging import AccessLogMode, LogLevel, RenderMode
from forze.base.telemetry import ExporterChoice

# ----------------------- #

_NORMALIZED_FIELDS = ("log_level", "log_render", "access_log", "telemetry")

# ....................... #


class RuntimeSettings(BaseModel):
    """Logging, access-log volume, telemetry and build identity for one process.

    Feed the fields straight through::

        bootstrap_logging(level=rt.log_level, render_mode=rt.log_render, ...)
        bootstrap_telemetry(service_version=rt.full_version, exporter=rt.telemetry, ...)
        AccessLogSampler(mode=rt.access_log, exclude=DEFAULT_HEALTH_PATHS)
    """

    version: str = "local"
    """Application version — ``service.version``, minus the build id."""

    build_id: str = "unknown"
    """CI build identifier, appended to :attr:`version` by :attr:`full_version`."""

    git_sha: str = "unknown"
    """Commit the image was built from. Bind it onto the log context, not the resource:
    it answers "which source produced this line", which is unanswerable after the fact."""

    log_level: LogLevel = "info"

    log_render: RenderMode = "json"
    """Deliberately **not** ``bootstrap_logging``'s own default.

    That function defaults to ``console`` because its caller might be a script or a test.
    A ``RuntimeSettings`` exists because something is being deployed, and a deployed
    process logs to a collector — so ``console`` is the thing a developer opts into here,
    the same direction as every other field on this model.
    """

    access_log: AccessLogMode = AccessLogMode.SAMPLED
    """Per-request access-log volume. Feeds ``AccessLogSampler(mode=...)``."""

    telemetry: ExporterChoice = "otlp"
    """OTel exporter choice. Endpoint, headers and timeouts stay with the standard
    ``OTEL_EXPORTER_OTLP_*`` environment variables the SDK reads itself — a second way to
    set the same endpoint is a second thing to get out of step. ``"none"`` installs real
    providers with nothing attached, which is what a hermetic test suite wants: a counter
    still records, and nothing opens a socket to a collector that is not there."""

    # ....................... #

    @computed_field  # type: ignore[prop-decorator]
    @property
    def full_version(self) -> str:
        """``service.version`` as reported to telemetry: version plus build id."""

        return f"{self.version}+{self.build_id}"

    # ....................... #

    @field_validator(*_NORMALIZED_FIELDS, mode="before")
    @classmethod
    def _normalize_choice(cls, v: Any) -> Any:
        """Case-fold the string choices.

        These arrive from the environment, where ``INFO`` and ``JSON`` are what an
        operator types. A rejected boot naming a case mismatch is a bad trade for a
        value that has exactly one spelling.
        """

        return v.lower().strip() if isinstance(v, str) else v


# ....................... #

__all__ = ["RuntimeSettings"]
