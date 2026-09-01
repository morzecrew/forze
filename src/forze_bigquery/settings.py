"""Connection settings for one BigQuery client.

The twin of :mod:`forze_gcs.settings` — same project and credentials-file shape — plus the
two knobs that decide what a runaway query costs.
"""

from datetime import timedelta

from pydantic import BaseModel

from forze.base.settings import configured_fields, require

from .kernel.client import BigQueryConfig

# ----------------------- #

CLIENT_FIELDS = (
    "timeout",
    "maximum_bytes_billed",
    "use_legacy_sql",
    "poll_interval",
    "max_poll_attempts",
    "insert_batch_size",
)
"""Knobs :class:`BigQuerySettings` forwards, by their :class:`BigQueryConfig` name. Every
entry is ``None`` by default and dropped when unset, so the defaults live in
:class:`BigQueryConfig` and cannot drift out of a second copy here.
"""

# ....................... #


class BigQuerySettings(BaseModel):
    """Project, credentials file and client tuning for one BigQuery client."""

    project_id: str | None = None
    """Required when read — see :meth:`require_project_id`."""

    service_file: str | None = None
    """Path to a service-account JSON key. Unset uses Application Default Credentials."""

    timeout: timedelta | None = None

    maximum_bytes_billed: int | None = None
    """Hard ceiling on one query's scanned bytes; BigQuery fails the job rather than
    billing past it. The one setting here that is about money, and the reason an operator
    wants any of these in the environment at all."""

    use_legacy_sql: bool | None = None
    poll_interval: timedelta | None = None
    max_poll_attempts: int | None = None
    insert_batch_size: int | None = None

    # ....................... #

    def require_project_id(self) -> str:
        """The project id, refused by name when unset.

        :raises CoreException: ``configuration`` when :attr:`project_id` is unset or blank.
        """

        return require(self.project_id, service="BigQuery", setting="project_id")

    # ....................... #

    @property
    def config(self) -> BigQueryConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`BigQueryConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return BigQueryConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "BigQuerySettings"]
