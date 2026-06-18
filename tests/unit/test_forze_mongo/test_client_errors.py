"""Unit tests for the Mongo error handler."""

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_mongo.kernel.client.errors import _mongo_eh

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
        # raw driver text must not leak into the summary, only into details
        assert "unexpected" not in result.summary
        assert result.details is not None
        assert result.details["error"] == "unexpected"

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

    def test_operation_failure_write_conflict(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure("WriteConflict", code=112)
        result = _mongo_eh(e, site="x")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.CONCURRENCY
        assert "conflict" in result.summary.lower()

    def test_operation_failure_transient_transaction_label(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure(
            "some transient error",
            code=999,
            details={"errorLabels": ["TransientTransactionError"]},
        )
        result = _mongo_eh(e, site="x")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.CONCURRENCY
        assert "transient" in result.summary.lower()

    def test_operation_failure_unauthorized(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure("not authorized to run aggregate", code=13)
        result = _mongo_eh(e, site="x")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.INFRASTRUCTURE
        assert "authorization" in result.summary

    def test_operation_failure_message_mentioning_authorization_without_code_13(
        self,
    ) -> None:
        from pymongo.errors import OperationFailure

        # A non-auth failure whose message merely contains "not authorized"
        # must not be classified as an authorization error (code-based, not
        # substring-based).
        e = OperationFailure("validator: 'not authorized' is invalid", code=121)
        result = _mongo_eh(e, site="x")
        assert isinstance(result, CoreException)
        assert "authorization" not in result.summary

    def test_operation_failure_generic(self) -> None:
        from pymongo.errors import OperationFailure

        e = OperationFailure("some server error", code=999)
        result = _mongo_eh(e, site="my_op")
        assert isinstance(result, CoreException) and result.kind == ExceptionKind.INFRASTRUCTURE
        assert "my_op" in result.summary


class TestAssembledChain:
    """Drive the actual chain wired into ``exc_interceptor``.

    Regression: the nested default chain used to make ``_mongo_eh``
    unreachable, so in-transaction write conflicts (code 112) surfaced as
    INTERNAL "Unhandled exception" instead of CONCURRENCY and were never
    retried by OCC machinery.
    """

    def test_write_conflict_code_112_maps_to_concurrency(self) -> None:
        from pymongo.errors import OperationFailure

        from forze_mongo.kernel.client.errors import exc_interceptor

        out = exc_interceptor.mapper(
            OperationFailure("WriteConflict", code=112),
            site="tx",
        )
        assert out is not None
        assert out.kind == ExceptionKind.CONCURRENCY

    def test_unknown_exception_reaches_package_fallback(self) -> None:
        from forze_mongo.kernel.client.errors import exc_interceptor

        out = exc_interceptor.mapper(RuntimeError("weird"), site="op")
        assert out is not None
        assert out.kind == ExceptionKind.INFRASTRUCTURE
        assert out.code != "core.unhandled"
