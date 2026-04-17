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

    def test_bulk_write_duplicate_key(self) -> None:
        from pymongo.errors import BulkWriteError

        e = BulkWriteError({"writeErrors": [{"code": 11000}]})
        result = _mongo_eh(e, "bulk")
        assert isinstance(result, ConflictError)

    def test_bulk_write_non_duplicate_maps_to_infra(self) -> None:
        from pymongo.errors import BulkWriteError

        e = BulkWriteError({"writeErrors": [{"code": 121}]})
        result = _mongo_eh(e, "bulk")
        assert isinstance(result, InfrastructureError)
        assert "bulk" in result.message

    def test_write_error_duplicate_key(self) -> None:
        from pymongo.errors import WriteError

        e = WriteError("dup", code=11000)
        result = _mongo_eh(e, "ins")
        assert isinstance(result, ConflictError)

    def test_write_error_other_code(self) -> None:
        from pymongo.errors import WriteError

        e = WriteError("other", code=2)
        result = _mongo_eh(e, "ins")
        assert isinstance(result, InfrastructureError)

    def test_operation_failure_interrupted(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure("rs", code=11600)
        result = _mongo_eh(e, "x")
        assert isinstance(result, ConcurrencyError)
        assert result.code == "interrupted"

    def test_operation_failure_transaction_conflict(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure("abort", code=251)
        result = _mongo_eh(e, "x")
        assert isinstance(result, ConcurrencyError)
        assert result.code == "transaction_conflict"

    def test_operation_failure_unauthorized(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure("not authorized to run aggregate", code=13)
        result = _mongo_eh(e, "x")
        assert isinstance(result, InfrastructureError)
        assert "authorization" in result.message

    def test_operation_failure_generic(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure("some server error", code=999)
        result = _mongo_eh(e, "my_op")
        assert isinstance(result, InfrastructureError)
        assert "my_op" in result.message
