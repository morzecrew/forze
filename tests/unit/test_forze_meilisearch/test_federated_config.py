"""Validation tests for Meilisearch federated search configuration."""

import pytest

from forze.application.contracts.search import FederatedSearchSpec, HubSearchSpec, SearchSpec
from forze.base.exceptions import CoreException
from pydantic import BaseModel

from forze_meilisearch.execution.deps.configs import (
    validate_meilisearch_federated_search_conf,
)


class _Hit(BaseModel):
    id: str
    label: str = ""


def _mem(name: str) -> SearchSpec[_Hit]:
    return SearchSpec(name=name, model_type=_Hit, fields=["label"])


def test_federated_requires_two_members() -> None:
    spec = FederatedSearchSpec(name="fed", members=(_mem("a"), _mem("b")))

    with pytest.raises(CoreException):
        validate_meilisearch_federated_search_conf(
            {"members": {"a": {"index_uid": "a"}}},
            spec,
        )


def test_federated_rejects_hub_member() -> None:
    hub = HubSearchSpec(
        name="hub",
        model_type=_Hit,
        members=[_mem("leg")],
    )
    spec = FederatedSearchSpec(name="fed", members=(_mem("a"), hub))

    with pytest.raises(CoreException):
        validate_meilisearch_federated_search_conf(
            {
                "members": {
                    "a": {"index_uid": "idx_a"},
                    "hub": {"index_uid": "idx_hub"},
                }
            },
            spec,
        )
