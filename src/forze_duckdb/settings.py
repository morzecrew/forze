"""Settings for one DuckDB client.

The odd one out of the family: DuckDB is in-process, so there is no endpoint and no
credentials — ``database`` is a filesystem path or ``:memory:``. What is worth carrying in
the environment is which file (or none) and the two resource limits, because those are the
difference between a query that is slow and a container the kernel kills.

Extensions, object-store credentials and named sources stay arguments to
:func:`~forze_duckdb.duckdb_lifecycle_step`: they describe what the process reads, which is
the application's wiring, not a deployment's dials.
"""

from pydantic import BaseModel, Field

from forze.base.settings import configured_fields

from .kernel.client import DuckDbConfig

# ----------------------- #

CLIENT_FIELDS = ("threads", "memory_limit", "max_concurrent_queries", "read_only")
"""Knobs :class:`DuckDbSettings` forwards, by their :class:`DuckDbConfig` name. Every
entry is ``None`` by default and dropped when unset, so the defaults live in
:class:`DuckDbConfig` and cannot drift out of a second copy here.
"""

# ....................... #


class DuckDbSettings(BaseModel):
    """Database path and resource limits for one DuckDB client."""

    database: str = Field(default=":memory:", min_length=1)
    """A file path, or ``:memory:`` for an ephemeral database. The default is in-memory
    because that is the analytics-over-a-lake shape DuckDB is wired for here — a path is
    what you set when the process needs its results to outlive it."""

    threads: int | None = Field(default=None, ge=1)
    memory_limit: str | None = Field(default=None, min_length=1)
    """DuckDB's own spelling, e.g. ``"4GB"``. Empty is refused rather than forwarded: an
    unset environment variable is ``None``, and an empty one is a typo that DuckDB would
    only reject at startup. Unset means DuckDB sizes it from the host,
    which in a container is the *host's* memory rather than the cgroup limit — so this is
    the setting that stops a query getting the process OOM-killed."""

    max_concurrent_queries: int | None = Field(default=None, ge=1)
    read_only: bool | None = None

    # ....................... #

    @property
    def config(self) -> DuckDbConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`DuckDbConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return DuckDbConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "DuckDbSettings"]
