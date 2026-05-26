"""Unit tests for the Mongo error handler."""

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_mongo.kernel.platform.errors import _mongo_eh

class TestMongoErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("original")
        result = _mongo_eh(original, site="op")
        assert result is original

    def test_duplicate_key_error(self) -> None:
        from pymongo.errors import DuplicateKeyError

        e = DuplicateKeyError("dup", code=11000, details={})
        result = _mongo_eh(e, site="insert")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.CONFLICT

    def test_write_timeout_error(self) -> None:
        from pymongo.errors import WTimeoutError

        e = WTimeoutError("timeout", code=50, details={})
        result = _mongo_eh(e, site="update")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.CONCURRENCY
        assert "timeout" in result.summary.lower()

    def test_auto_reconnect_error(self) -> None:
        from pymongo.errors import AutoReconnect

        e = AutoReconnect("reconnect")
        result = _mongo_eh(e, site="query")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.CONCURRENCY
        assert "reconnect" in result.summary.lower()

    def test_not_primary_error(self) -> None:
        from pymongo.errors import NotPrimaryError

        e = NotPrimaryError("not primary")
        result = _mongo_eh(e, site="write")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.CONCURRENCY
        assert "primary" in result.summary.lower()

    def test_connection_failure(self) -> None:
        from pymongo.errors import ConnectionFailure

        e = ConnectionFailure("no connection")
        result = _mongo_eh(e, site="op")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.INFRASTRUCTURE

    def test_server_selection_timeout(self) -> None:
        from pymongo.errors import ServerSelectionTimeoutError

        e = ServerSelectionTimeoutError("timeout")
        result = _mongo_eh(e, site="op")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.INFRASTRUCTURE

    def test_configuration_error(self) -> None:
        from pymongo.errors import ConfigurationError

        e = ConfigurationError("bad config")
        result = _mongo_eh(e, site="op")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.INFRASTRUCTURE

    def test_unknown_exception_fallback(self) -> None:
        e = RuntimeError("unexpected")
        result = _mongo_eh(e, site="some_op")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.INFRASTRUCTURE
        assert "some_op" in result.summary

    def test_bulk_write_duplicate_key(self) -> None:
        from pymongo.errors import BulkWriteError

        e = BulkWriteError({"writeErrors": [{"code": 11000}]})
        result = _mongo_eh(e, site="bulk")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.CONFLICT

    def test_bulk_write_non_duplicate_maps_to_infra(self) -> None:
        from pymongo.errors import BulkWriteError

        e = BulkWriteError({"writeErrors": [{"code": 121}]})
        result = _mongo_eh(e, site="bulk")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.INFRASTRUCTURE
        assert "bulk" in result.summary

    def test_write_error_duplicate_key(self) -> None:
        from pymongo.errors import WriteError

        e = WriteError("dup", code=11000)
        result = _mongo_eh(e, site="ins")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.CONFLICT

    def test_write_error_other_code(self) -> None:
        from pymongo.errors import WriteError

        e = WriteError("other", code=2)
        result = _mongo_eh(e, site="ins")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.INFRASTRUCTURE

    def test_operation_failure_interrupted(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure("rs", code=11600)
        result = _mongo_eh(e, site="x")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.CONCURRENCY
        assert "interrupted" in result.summary.lower()

    def test_operation_failure_transaction_conflict(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure("abort", code=251)
        result = _mongo_eh(e, site="x")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.CONCURRENCY
        assert "transaction" in result.summary.lower()

    def test_operation_failure_unauthorized(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure("not authorized to run aggregate", code=13)
        result = _mongo_eh(e, site="x")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.INFRASTRUCTURE
        assert "authorization" in result.summary

    def test_operation_failure_generic(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure("some server error", code=999)
        result = _mongo_eh(e, site="my_op")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.INFRASTRUCTURE
        assert "my_op" in result.summary
