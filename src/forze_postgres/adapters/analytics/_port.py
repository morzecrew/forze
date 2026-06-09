"""Postgres analytics query-port delegation (inherited from the core mixin)."""

from pydantic import BaseModel

from forze.application.integrations.analytics import AnalyticsQueryPortMixin

from ._mixin_base import PostgresAnalyticsMixinBase

# ----------------------- #


class PostgresAnalyticsPortMixin[R: BaseModel, Ing: BaseModel](
    AnalyticsQueryPortMixin[R],
    PostgresAnalyticsMixinBase[R, Ing],
):
    """Postgres :class:`~forze.application.contracts.analytics.AnalyticsQueryPort` delegation.

    The run/projection/select/chunked/cursor methods are inherited from
    :class:`~forze.application.integrations.analytics.AnalyticsQueryPortMixin`;
    the concrete adapter supplies ``_offset_page`` / ``_cursor_page`` /
    ``run_chunked`` via its sibling mixins.
    """
