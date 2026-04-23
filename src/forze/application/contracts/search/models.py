from pydantic import BaseModel

# ----------------------- #


class FederatedSearchReadModel[X: BaseModel](BaseModel):
    """Canonical federated search read model."""

    hit: X
    """Search hit data."""

    member: str
    """Member :class:`~forze.application.contracts.search.SearchSpec` name (``spec.name``)."""
