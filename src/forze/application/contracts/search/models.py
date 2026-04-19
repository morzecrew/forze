from pydantic import BaseModel

# ----------------------- #
#! TODO: review


class FederatedSearchReadModel[X: BaseModel](BaseModel):
    """Canonical federated search read model."""

    hit: X
    """Search hit data."""

    member: str
    """Leg :class:`~forze.application.contracts.search.SearchSpec` name (``spec.name``)."""
