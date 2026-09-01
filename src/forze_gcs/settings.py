"""Connection settings for one GCS client.

No endpoint and no credentials in the usual sense — Google's client resolves both from
Application Default Credentials. What an application still has to carry is which project
it is talking to, and where the service-account file is when ADC is not enough.
"""

from datetime import timedelta

from pydantic import BaseModel

from forze.base.settings import configured_fields, require

from .kernel.client import GCSConfig

# ----------------------- #

CLIENT_FIELDS = ("service_file", "timeout", "signing_service_account_email")
"""Knobs :class:`GCSSettings` forwards, by their :class:`GCSConfig` name. Every entry is
``None`` by default and dropped when unset, so the defaults live in :class:`GCSConfig` and
cannot drift out of a second copy here.
"""

# ....................... #


class GCSSettings(BaseModel):
    """Project, credentials file and client tuning for one GCS client."""

    project_id: str | None = None
    """Required when read — see :meth:`require_project_id`."""

    service_file: str | None = None
    """Path to a service-account JSON key. Unset uses Application Default Credentials —
    the workload identity a GKE or Cloud Run process already has, and the shape that needs
    no key material on disk at all."""

    timeout: timedelta | None = None

    signing_service_account_email: str | None = None
    """Identity used to sign URLs when the runtime credential cannot sign for itself —
    the case on every metadata-server credential, which has no private key."""

    # ....................... #

    def require_project_id(self) -> str:
        """The project id, refused by name when unset.

        :raises CoreException: ``configuration`` when :attr:`project_id` is unset or blank.
        """

        return require(self.project_id, service="GCS", setting="project_id")

    # ....................... #

    @property
    def config(self) -> GCSConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`GCSConfig` is an attrs class,
        and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return GCSConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "GCSSettings"]
