"""Unit tests for Postgres RelationSpec wiring warnings."""

from unittest.mock import MagicMock, patch

from forze_postgres.kernel.catalog.validation.validate_relation_specs import (
    warn_dynamic_relation_with_tenant_aware,  # postgres wrapper
)


def test_warn_dynamic_relation_with_tenant_aware_logs() -> None:
    def _resolver(_tenant_id: object) -> tuple[str, str]:
        return ("tenant_schema", "items")

    mock_logger = MagicMock()
    with patch(
        "forze_postgres.kernel.catalog.validation.validate_relation_specs.logger",
        mock_logger,
    ):
        warn_dynamic_relation_with_tenant_aware(
            route_name="items",
            kind="document",
            tenant_aware=True,
            fields=[("read", _resolver)],
        )

    mock_logger.warning.assert_called_once()
    args = mock_logger.warning.call_args.args
    message = args[1] if len(args) > 1 else args[0]
    assert "dynamic RelationSpec" in str(message)


def test_warn_skipped_for_static_relation() -> None:
    mock_logger = MagicMock()
    with patch(
        "forze_postgres.kernel.catalog.validation.validate_relation_specs.logger",
        mock_logger,
    ):
        warn_dynamic_relation_with_tenant_aware(
            route_name="items",
            kind="document",
            tenant_aware=True,
            fields=[("read", ("public", "items"))],
        )

    mock_logger.warning.assert_not_called()


def test_warn_skipped_when_not_tenant_aware() -> None:
    def _resolver(_tenant_id: object) -> tuple[str, str]:
        return ("tenant_schema", "items")

    mock_logger = MagicMock()
    with patch(
        "forze_postgres.kernel.catalog.validation.validate_relation_specs.logger",
        mock_logger,
    ):
        warn_dynamic_relation_with_tenant_aware(
            route_name="items",
            kind="document",
            tenant_aware=False,
            fields=[("read", _resolver)],
        )

    mock_logger.warning.assert_not_called()
