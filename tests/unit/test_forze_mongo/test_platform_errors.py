"""Unit tests for the Mongo error handler."""

from forze.base.errors import (
    ConcurrencyError,
    ConflictError,
    CoreError,
    InfrastructureError,
)
from forze_mongo.kernel.platform.errors import _mongo_eh


class TestMongoErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = CoreError("original")
        result = _mongo_eh(original, "op")
        assert result is original

    def test_duplicate_key_error(self) -> None:
        from pymongo.errors import DuplicateKeyError

        e = DuplicateKeyError("dup", code=11000, details={})
        result = _mongo_eh(e, "insert")
        assert isinstance(result, ConflictError)

    def test_write_timeout_error(self) -> None:
        from pymongo.errors import WTimeoutError

        e = WTimeoutError("timeout", code=50, details={})
        result = _mongo_eh(e, "update")
        assert isinstance(result, ConcurrencyError)
        assert result.code == "write_concern_timeout"

    def test_auto_reconnect_error(self) -> None:
        from pymongo.errors import AutoReconnect

        e = AutoReconnect("reconnect")
        result = _mongo_eh(e, "query")
        assert isinstance(result, ConcurrencyError)
        assert result.code == "auto_reconnect"

    def test_not_primary_error(self) -> None:
        from pymongo.errors import NotPrimaryError

        e = NotPrimaryError("not primary")
        result = _mongo_eh(e, "write")
        assert isinstance(result, ConcurrencyError)
        assert result.code == "not_primary"

    def test_connection_failure(self) -> None:
        from pymongo.errors import ConnectionFailure

        e = ConnectionFailure("no connection")
        result = _mongo_eh(e, "op")
        assert isinstance(result, InfrastructureError)

    def test_server_selection_timeout(self) -> None:
        from pymongo.errors import ServerSelectionTimeoutError

        e = ServerSelectionTimeoutError("timeout")
        result = _mongo_eh(e, "op")
        assert isinstance(result, InfrastructureError)

    def test_configuration_error(self) -> None:
        from pymongo.errors import ConfigurationError

        e = ConfigurationError("bad config")
        result = _mongo_eh(e, "op")
        assert isinstance(result, InfrastructureError)

    def test_unknown_exception_fallback(self) -> None:
        e = RuntimeError("unexpected")
        result = _mongo_eh(e, "some_op")
        assert isinstance(result, InfrastructureError)
        assert "some_op" in result.message
