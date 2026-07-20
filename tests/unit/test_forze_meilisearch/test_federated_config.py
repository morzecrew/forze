"""Validation tests for Meilisearch federated search configuration."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import FederatedSearchSpec, HubSearchSpec, SearchSpec
from forze.base.exceptions import CoreException
from forze_meilisearch.execution.deps.configs import (
    MeilisearchFederatedSearchConfig,
    MeilisearchSearchConfig,
)


class _Hit(BaseModel):
    id: str
    label: str = ""


def _mem(name: str) -> SearchSpec[_Hit]:
    return SearchSpec(name=name, model_type=_Hit, fields=["label"])


def test_federated_requires_two_members() -> None:
    with pytest.raises(CoreException):
        MeilisearchFederatedSearchConfig(
            members={"a": MeilisearchSearchConfig(index_uid="a")},
        )


def test_federated_rejects_hub_member() -> None:
    hub = HubSearchSpec(
        name="hub",
        model_type=_Hit,
        members=[_mem("leg")],
    )
    spec = FederatedSearchSpec(name="fed", members=(_mem("a"), hub))
    config = MeilisearchFederatedSearchConfig(
        members={
            "a": MeilisearchSearchConfig(index_uid="idx_a"),
            "hub": MeilisearchSearchConfig(index_uid="idx_hub"),
        },
    )

    with pytest.raises(CoreException):
        config.validate_against_spec(spec)
