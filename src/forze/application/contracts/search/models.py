from pydantic import BaseModel

# ----------------------- #
#! TODO: review


class FederatedSearchReadModel[X: BaseModel](BaseModel):
    """Canonical federated search read model."""

    hit: X
    """Search hit data."""

    branch_name: str
    """Search branch name."""
