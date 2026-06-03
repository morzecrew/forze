"""Backend alias for the shared :class:`SimpleSearchPortMixin`."""

from pydantic import BaseModel

from forze.application.integrations.search import SimpleSearchPortMixin

# ----------------------- #


class MongoSearchPortMixin[M: BaseModel](SimpleSearchPortMixin[M]):
    """Mongo :class:`~forze.application.contracts.search.SearchQueryPort` delegation.

    All offset/projection/select/cursor variants are inherited from
    :class:`~forze.application.integrations.search.SimpleSearchPortMixin`;
    adapters implement only ``_offset_search_impl`` / ``_cursor_search_impl``.
    """
