"""Settings pieces every integration package builds its own connection model from.

Two things live here, and only because they are genuinely shared. :class:`EndpointSettings`
carries the ``host``/``port`` pair and the RFC 3986 authority grammar that applies to every
scheme built from it — bracketing a bare IPv6 literal, joining the port, refusing a blank
host by name. The *scheme* and the query parameters stay in the integration package, which
is the part that is actually per-backend. :class:`RuntimeSettings` is unrelated to either:
it is the argument lists of the bootstrap helpers.

``RuntimeSettings`` exists because :func:`~forze.base.logging.bootstrap_logging` and
:func:`~forze.base.telemetry.bootstrap_telemetry` take their configuration as keyword
arguments, which every deploying application then re-declares as settings fields and
re-picks defaults for. :class:`RuntimeSettings` is that declaration, once.

It is a plain :class:`pydantic.BaseModel` rather than a ``BaseSettings``: the environment
prefix, nesting delimiter, case sensitivity and extra-key policy are deployment decisions,
so the root settings class stays in the application and mounts this as a field.
"""

from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, Field, computed_field, field_validator

from forze.base.exceptions import exc
from forze.base.logging import AccessLogMode, LogLevel, RenderMode
from forze.base.telemetry import ExporterChoice

# ----------------------- #

_NORMALIZED_FIELDS = ("log_level", "log_render", "access_log", "telemetry")

# ....................... #


def require(value: str | None, *, service: str, setting: str) -> str:
    """*value*, stripped and known non-empty, or a configuration refusal naming *setting*.

    The family rule behind every ``require_*`` accessor on an integration settings model:
    a value the wiring needs as a ``str`` is declared ``str | None`` so the model still
    constructs with nothing in the environment, and the refusal happens where the value is
    read. That refusal names the setting an operator can act on, which a downstream DNS or
    credential error does not.

    Stripping first is part of it: a ``HOST=" "`` out of a hand-edited env file is unset,
    and treating it as a value turns a boot failure into a lookup failure.

    :param value: The optional value to demand.
    :param service: Backend name for the message, e.g. ``"Postgres"``.
    :param setting: What is missing, e.g. ``"host"``, spelled as the field is.
    :raises CoreException: ``configuration`` when *value* is ``None``, empty or blank.
    """

    text = (value or "").strip()

    if not text:
        raise exc.configuration(f"{service} {setting} is required.")

    return text


# ....................... #


def configured_fields(model: BaseModel, names: Sequence[str]) -> dict[str, Any]:
    """The named fields the operator actually set, as constructor keyword arguments.

    Integration settings mirror a subset of their client's ``attrs`` config, and every
    mirrored knob defaults to ``None`` meaning "whatever the config defaults to". Dropping
    the unset ones instead of forwarding ``None`` is what keeps the defaults living in one
    place: a second copy of them on the settings model is a second copy to drift.

    :param model: The settings model to read.
    :param names: Field names to forward, spelled as the target config spells them.
    :returns: ``{name: value}`` for every named field that is not ``None``.
    """

    return {name: value for name in names if (value := getattr(model, name)) is not None}


# ....................... #


class EndpointSettings(BaseModel):
    """One network service's ``host`` and ``port``, and the URL grammar they obey.

    Subclassed by every integration whose connection string is built rather than supplied.
    It deliberately carries no scheme, no credentials and no query parameters: those differ
    per backend, and a base class guessing at them is how a shared helper starts lying.
    """

    host: str | None = None
    """No default on purpose: an unset host is a boot failure naming the setting, never a
    silent connection to a ``localhost`` that happens to be listening."""

    port: int | None = Field(default=None, ge=1, le=65535)
    """Omitted from the authority when unset, leaving the scheme's own default port."""

    # ....................... #

    def require_host(self, *, service: str) -> str:
        """The host, stripped and known non-empty.

        For a backend whose client takes host and port separately, so there is no
        authority to build. :meth:`authority` is the one to use when there is.

        :param service: Backend name for the error message, e.g. ``"ClickHouse"``.
        :raises CoreException: ``configuration`` when the host is unset or blank.
        """

        return require(self.host, service=service, setting="host")

    # ....................... #

    def authority(self, *, service: str) -> str:
        """``host``, or ``host:port`` when a port is set — the URL's authority component.

        :param service: Backend name for the error message, e.g. ``"Postgres"``.
        :raises CoreException: ``configuration`` when the host is unset or blank.
        """

        host = self.require_host(service=service)

        # A bare IPv6 literal has to be bracketed or the first colon reads as the port
        # separator.
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"

        return f"{host}:{self.port}" if self.port else host


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

__all__ = ["EndpointSettings", "RuntimeSettings", "configured_fields", "require"]
