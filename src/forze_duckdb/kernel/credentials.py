"""Object-storage credentials for DuckDB lake / lakehouse sources.

Each credential value object compiles to a DuckDB ``CREATE SECRET`` statement. Credentials
are supplied in one of two ways (adapter-preferred):

* ``secret_ref`` — a :class:`~forze.application.contracts.secrets.SecretRef` resolved at
  startup through the wired :class:`~forze.application.contracts.secrets.SecretsPort` (Vault,
  env, directory, …). The secret payload is JSON matching the provider's payload model.
* ``inline`` — the payload supplied directly in code. Convenient for tests and local runs;
  prefer ``secret_ref`` for anything sensitive.

Exactly one of the two must be set. Secret values are :class:`~pydantic.SecretStr` and never
appear in ``repr``. Path/string fields are single-quote-escaped when rendered.
"""

from __future__ import annotations

import abc
import re
from typing import final

import attrs

from pydantic import BaseModel, SecretStr

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import exc

# ----------------------- #

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _sql_str(value: str) -> str:
    """Render *value* as a single-quoted SQL string literal, escaping embedded quotes."""

    return "'" + value.replace("'", "''") + "'"


def _require_identifier(name: str) -> str:
    """Validate *name* is a bare SQL identifier (used unquoted in ``CREATE SECRET <name>``)."""

    if not _IDENTIFIER.match(name):
        raise exc.configuration(
            f"Object-store secret name {name!r} must be a bare SQL identifier "
            "(letters, digits, underscore; not starting with a digit).",
        )

    return name


# ----------------------- #


class ObjectStoreCredentials(abc.ABC):
    """Provider-agnostic object-storage credential that renders a ``CREATE SECRET`` statement."""

    name: str
    """DuckDB secret name (a bare identifier)."""

    secret_ref: SecretRef | None
    """When set, the payload is resolved from the secrets backend at startup."""

    # ....................... #

    @staticmethod
    @abc.abstractmethod
    def payload_model() -> type[BaseModel]:
        """Return the Pydantic model the ``secret_ref`` JSON payload is validated against."""

    # ....................... #

    @abc.abstractmethod
    def inline_payload(self) -> BaseModel | None:
        """Return the inline payload, or ``None`` when this credential uses ``secret_ref``."""

    # ....................... #

    @abc.abstractmethod
    def render(self, payload: BaseModel) -> str:
        """Render the ``CREATE SECRET`` statement from a resolved or inline *payload*."""

    # ....................... #

    @abc.abstractmethod
    def required_extensions(self) -> tuple[str, ...]:
        """Return the DuckDB extensions this credential needs (e.g. ``httpfs``)."""


# ....................... #


def _render_secret(name: str, type_: str, parts: list[str], scope: str | None) -> str:
    """Assemble a ``CREATE OR REPLACE SECRET`` statement from rendered key/value *parts*."""

    fields = [f"TYPE {type_}", *parts]

    if scope is not None:
        fields.append(f"SCOPE {_sql_str(scope)}")

    body = ",\n  ".join(fields)

    return f"CREATE OR REPLACE SECRET {name} (\n  {body}\n)"


# ----------------------- #


class S3SecretPayload(BaseModel):
    """JSON payload shape for :class:`S3Credentials` (also reused for the inline form)."""

    access_key_id: str
    secret_access_key: SecretStr
    region: str | None = None
    endpoint: str | None = None
    url_style: str | None = None
    """``path`` or ``vhost``; use ``path`` for most S3-compatible stores (MinIO, R2)."""
    use_ssl: bool | None = None
    session_token: SecretStr | None = None


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class S3Credentials(ObjectStoreCredentials):
    """S3 (and S3-compatible: MinIO, Cloudflare R2 via ``endpoint``) credentials."""

    name: str = attrs.field(default="forze_s3")
    scope: str | None = None
    """Optional ``SCOPE`` (e.g. ``s3://bucket``) limiting which paths use this secret."""

    inline: S3SecretPayload | None = attrs.field(default=None, repr=False)
    secret_ref: SecretRef | None = None

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_identifier(self.name)

        if (self.inline is None) == (self.secret_ref is None):
            raise exc.configuration(
                "S3Credentials requires exactly one of `inline` or `secret_ref`.",
            )

    # ....................... #

    @staticmethod
    def payload_model() -> type[BaseModel]:
        return S3SecretPayload

    # ....................... #

    def inline_payload(self) -> BaseModel | None:
        return self.inline

    # ....................... #

    def render(self, payload: BaseModel) -> str:
        if not isinstance(payload, S3SecretPayload):
            raise exc.internal("S3Credentials.render expects an S3SecretPayload.")

        parts = [
            f"KEY_ID {_sql_str(payload.access_key_id)}",
            f"SECRET {_sql_str(payload.secret_access_key.get_secret_value())}",
        ]

        if payload.region is not None:
            parts.append(f"REGION {_sql_str(payload.region)}")

        if payload.endpoint is not None:
            parts.append(f"ENDPOINT {_sql_str(payload.endpoint)}")

        if payload.url_style is not None:
            parts.append(f"URL_STYLE {_sql_str(payload.url_style)}")

        if payload.use_ssl is not None:
            parts.append(f"USE_SSL {'true' if payload.use_ssl else 'false'}")

        if payload.session_token is not None:
            parts.append(
                f"SESSION_TOKEN {_sql_str(payload.session_token.get_secret_value())}"
            )

        return _render_secret(self.name, "S3", parts, self.scope)

    # ....................... #

    def required_extensions(self) -> tuple[str, ...]:
        return ("httpfs",)


# ----------------------- #


class GcsSecretPayload(BaseModel):
    """JSON payload shape for :class:`GcsCredentials` (HMAC interoperability keys)."""

    key_id: str
    secret: SecretStr


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GcsCredentials(ObjectStoreCredentials):
    """Google Cloud Storage credentials (HMAC interoperability keys)."""

    name: str = attrs.field(default="forze_gcs")
    scope: str | None = None
    """Optional ``SCOPE`` (e.g. ``gs://bucket``) limiting which paths use this secret."""

    inline: GcsSecretPayload | None = attrs.field(default=None, repr=False)
    secret_ref: SecretRef | None = None

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_identifier(self.name)

        if (self.inline is None) == (self.secret_ref is None):
            raise exc.configuration(
                "GcsCredentials requires exactly one of `inline` or `secret_ref`.",
            )

    # ....................... #

    @staticmethod
    def payload_model() -> type[BaseModel]:
        return GcsSecretPayload

    # ....................... #

    def inline_payload(self) -> BaseModel | None:
        return self.inline

    # ....................... #

    def render(self, payload: BaseModel) -> str:
        if not isinstance(payload, GcsSecretPayload):
            raise exc.internal("GcsCredentials.render expects a GcsSecretPayload.")

        parts = [
            f"KEY_ID {_sql_str(payload.key_id)}",
            f"SECRET {_sql_str(payload.secret.get_secret_value())}",
        ]

        return _render_secret(self.name, "GCS", parts, self.scope)

    # ....................... #

    def required_extensions(self) -> tuple[str, ...]:
        return ("httpfs",)
