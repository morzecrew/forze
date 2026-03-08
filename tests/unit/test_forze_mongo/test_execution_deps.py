"""Unit tests for ``forze_mongo.execution.deps`` wiring."""

from unittest.mock import MagicMock

from forze.application.execution import Deps
from forze_mongo.execution.deps import MongoClientDepKey, MongoDepsModule
from forze_mongo.kernel.platform import MongoClient


def test_mongo_deps_module_registers_expected_keys() -> None:
    client = MagicMock(spec=MongoClient)
    module = MongoDepsModule(client=client)

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(MongoClientDepKey)
