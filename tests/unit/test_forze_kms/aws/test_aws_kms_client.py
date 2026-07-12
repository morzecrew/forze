"""The AWS KMS kernel client and its config (mocked boto).

The emulator-backed integration suite covers the happy paths; these pin the branches a live backend will
not readily produce — the un-initialized guards, the credential/endpoint wiring handed to
botocore, and the orphan-CMK cleanup when aliasing a freshly minted key fails.
"""

from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("aioboto3")

from botocore import exceptions as boto_errors

from forze.base.exceptions import CoreException, ExceptionKind
from forze_kms.aws import AwsKmsClient, AwsKmsConfig
from forze_kms.aws.kernel.client.value_objects import AwsKmsConnectionOpts

# ----------------------- #

_DEK = b"0123456789abcdef0123456789abcdef"
_BLOB = b"wrapped"


def _boto() -> MagicMock:
    """A stand-in for the aiobotocore KMS client."""

    c = MagicMock()
    c.exceptions.ClientError = boto_errors.ClientError
    c.generate_data_key = AsyncMock(
        return_value={"Plaintext": _DEK, "CiphertextBlob": _BLOB}
    )
    c.decrypt = AsyncMock(return_value={"Plaintext": _DEK})
    c.create_key = AsyncMock(return_value={"KeyMetadata": {"KeyId": "cmk-1"}})
    c.create_alias = AsyncMock()
    c.delete_alias = AsyncMock()
    c.describe_key = AsyncMock(return_value={"KeyMetadata": {"KeyId": "cmk-1"}})
    c.schedule_key_deletion = AsyncMock()
    c.list_keys = AsyncMock()

    return c


def _client_with(boto: MagicMock) -> AwsKmsClient:
    """An AwsKmsClient whose `client()` scope yields *boto*."""

    client = AwsKmsClient()

    @asynccontextmanager
    async def _scope() -> AsyncIterator[MagicMock]:
        yield boto

    client.client = _scope  # type: ignore[method-assign]

    return client


def _not_found() -> boto_errors.ClientError:
    return boto_errors.ClientError({"Error": {"Code": "NotFoundException"}}, "DescribeKey")


# ....................... #


class TestConfig:
    def test_defaults_apply_an_adaptive_retry_policy(self) -> None:
        cfg = AwsKmsConfig().to_aio_config()

        assert cfg.retries == {"max_attempts": 3, "mode": "adaptive"}

    def test_timeouts_are_rendered_as_seconds(self) -> None:
        cfg = AwsKmsConfig(
            region_name="eu-central-1",
            connect_timeout=timedelta(seconds=2),
            read_timeout=timedelta(seconds=5),
        ).to_aio_config()

        assert cfg.region_name == "eu-central-1"
        assert cfg.connect_timeout == 2
        assert cfg.read_timeout == 5

    @pytest.mark.parametrize("field", ["connect_timeout", "read_timeout"])
    def test_a_non_positive_timeout_fails_closed(self, field: str) -> None:
        with pytest.raises(CoreException) as ei:
            AwsKmsConfig(**{field: timedelta(seconds=0)})

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_a_half_supplied_credential_pair_fails_closed(self) -> None:
        """Half a static credential silently falls back to the chain — refuse it."""

        with pytest.raises(CoreException) as ei:
            AwsKmsConnectionOpts(access_key_id="only-one")

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_neither_credential_defers_to_the_botocore_chain(self) -> None:
        assert AwsKmsConnectionOpts().access_key_id is None


# ....................... #


class TestLifecycle:
    async def test_operations_before_initialize_fail_closed(self) -> None:
        with pytest.raises(CoreException):
            await AwsKmsClient().generate_data_key("cmk")

    async def test_close_before_initialize_is_a_no_op(self) -> None:
        await AwsKmsClient().close()  # must not raise

    async def test_initialize_is_idempotent_and_close_releases(self) -> None:
        client = AwsKmsClient()
        await client.initialize(
            endpoint="http://localhost:1",
            region_name="us-east-1",
            access_key_id="k",
            secret_access_key="s",
        )
        await client.initialize(endpoint="http://other:2")  # no-op, keeps the first

        async with client.client() as c:
            assert c is not None

        await client.close()

        # Closed → the scope falls back to building a transient client, which needs opts.
        with pytest.raises(CoreException):
            await client.generate_data_key("cmk")

    async def test_a_failed_initialize_leaves_no_half_built_state(self) -> None:
        """Otherwise a retry would find a session but no client and never rebuild."""

        client = AwsKmsClient()
        session = MagicMock()
        session.client = MagicMock(side_effect=RuntimeError("no endpoint"))

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("aioboto3.Session", lambda: session)

            with pytest.raises(RuntimeError, match="no endpoint"):
                await client.initialize(endpoint="http://localhost:1")

        # The session was rolled back, so a later call fails closed rather than half-working.
        with pytest.raises(CoreException):
            await client.generate_data_key("cmk")

    async def test_health_reports_a_failure_without_raising(self) -> None:
        boto = _boto()
        boto.list_keys = AsyncMock(side_effect=RuntimeError("down"))

        message, ok = await _client_with(boto).health()

        assert ok is False
        assert "down" in message

    async def test_health_reports_ok(self) -> None:
        message, ok = await _client_with(_boto()).health()

        assert (message, ok) == ("ok", True)


# ....................... #


class TestEnvelopeOperations:
    async def test_generate_data_key_passes_the_key_spec(self) -> None:
        boto = _boto()

        plaintext, blob = await _client_with(boto).generate_data_key(
            "cmk", key_spec="AES_128"
        )

        assert (plaintext, blob) == (_DEK, _BLOB)
        boto.generate_data_key.assert_awaited_once_with(KeyId="cmk", KeySpec="AES_128")

    async def test_missing_key_material_is_an_internal_error(self) -> None:
        boto = _boto()
        boto.generate_data_key = AsyncMock(return_value={})

        with pytest.raises(CoreException):
            await _client_with(boto).generate_data_key("cmk")

    async def test_decrypt_binds_the_key_id_when_given(self) -> None:
        """Passing KeyId makes KMS itself reject a blob wrapped under another CMK."""

        boto = _boto()

        assert await _client_with(boto).decrypt(_BLOB, key_id="cmk") == _DEK
        boto.decrypt.assert_awaited_once_with(CiphertextBlob=_BLOB, KeyId="cmk")

    async def test_decrypt_without_a_key_id_omits_the_constraint(self) -> None:
        boto = _boto()

        await _client_with(boto).decrypt(_BLOB)

        boto.decrypt.assert_awaited_once_with(CiphertextBlob=_BLOB)

    async def test_missing_plaintext_is_an_internal_error(self) -> None:
        boto = _boto()
        boto.decrypt = AsyncMock(return_value={})

        with pytest.raises(CoreException):
            await _client_with(boto).decrypt(_BLOB)


# ....................... #


class TestKeyAdministration:
    async def test_find_key_id_by_alias_returns_the_cmk(self) -> None:
        assert await _client_with(_boto()).find_key_id_by_alias("alias/a") == "cmk-1"

    async def test_an_absent_alias_is_none_not_an_error(self) -> None:
        boto = _boto()
        boto.describe_key = AsyncMock(side_effect=_not_found())

        assert await _client_with(boto).find_key_id_by_alias("alias/gone") is None

    async def test_an_unexpected_describe_failure_still_raises(self) -> None:
        boto = _boto()
        boto.describe_key = AsyncMock(
            side_effect=boto_errors.ClientError(
                {"Error": {"Code": "AccessDeniedException"}}, "DescribeKey"
            )
        )

        with pytest.raises(CoreException):
            await _client_with(boto).find_key_id_by_alias("alias/a")

    async def test_create_key_with_alias_points_the_alias_at_the_new_cmk(self) -> None:
        boto = _boto()

        key_id = await _client_with(boto).create_key_with_alias(
            "alias/a", description="d"
        )

        assert key_id == "cmk-1"
        boto.create_alias.assert_awaited_once_with(
            AliasName="alias/a", TargetKeyId="cmk-1"
        )

    async def test_a_failed_alias_retires_the_orphaned_cmk(self) -> None:
        """The CMK is only reachable through its alias, so an un-aliased key is an orphan
        — billable, unaddressable, and re-created on every retry."""

        boto = _boto()
        boto.create_alias = AsyncMock(side_effect=RuntimeError("alias exists"))

        with pytest.raises(CoreException) as ei:
            await _client_with(boto).create_key_with_alias("alias/a")

        assert isinstance(ei.value.__cause__, RuntimeError)  # the aliasing failure
        boto.schedule_key_deletion.assert_awaited_once()
        assert boto.schedule_key_deletion.await_args.kwargs["KeyId"] == "cmk-1"

    async def test_cleanup_failure_does_not_mask_the_aliasing_error(self) -> None:
        boto = _boto()
        boto.create_alias = AsyncMock(side_effect=RuntimeError("alias exists"))
        boto.schedule_key_deletion = AsyncMock(side_effect=RuntimeError("cleanup down"))

        with pytest.raises(CoreException) as ei:
            await _client_with(boto).create_key_with_alias("alias/a")

        # The aliasing failure is what surfaces — a failed cleanup must not replace it.
        assert str(ei.value.__cause__) == "alias exists"

    async def test_create_key_without_a_key_id_is_an_internal_error(self) -> None:
        boto = _boto()
        boto.create_key = AsyncMock(return_value={})

        with pytest.raises(CoreException):
            await _client_with(boto).create_key_with_alias("alias/a")

    async def test_delete_alias_tolerates_an_absent_one(self) -> None:
        boto = _boto()
        boto.delete_alias = AsyncMock(side_effect=_not_found())

        await _client_with(boto).delete_alias("alias/gone")  # idempotent teardown

    async def test_an_unexpected_delete_failure_still_raises(self) -> None:
        boto = _boto()
        boto.delete_alias = AsyncMock(
            side_effect=boto_errors.ClientError(
                {"Error": {"Code": "AccessDeniedException"}}, "DeleteAlias"
            )
        )

        with pytest.raises(CoreException):
            await _client_with(boto).delete_alias("alias/a")

    async def test_schedule_key_deletion_passes_the_window(self) -> None:
        boto = _boto()

        await _client_with(boto).schedule_key_deletion("cmk-1", pending_window_days=7)

        boto.schedule_key_deletion.assert_awaited_once_with(
            KeyId="cmk-1", PendingWindowInDays=7
        )


# ....................... #


class TestBotoClientWiring:
    async def test_credentials_and_endpoint_reach_botocore(self) -> None:
        client = AwsKmsClient()
        captured: dict[str, Any] = {}

        def _capture(_service: str, **kwargs: Any) -> Any:
            captured.update(kwargs)

            @asynccontextmanager
            async def _cm() -> AsyncIterator[MagicMock]:
                yield _boto()

            return _cm()

        session = MagicMock()
        session.client = _capture

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("aioboto3.Session", lambda: session)
            await client.initialize(
                endpoint="http://localhost:1",
                region_name="eu-west-1",
                access_key_id="k",
                secret_access_key="s",
            )

        assert captured["endpoint_url"] == "http://localhost:1"
        assert captured["aws_access_key_id"] == "k"
        assert captured["aws_secret_access_key"] == "s"
        assert captured["config"].region_name == "eu-west-1"

    async def test_no_endpoint_or_credentials_defers_to_the_chain(self) -> None:
        client = AwsKmsClient()
        captured: dict[str, Any] = {}

        def _capture(_service: str, **kwargs: Any) -> Any:
            captured.update(kwargs)

            @asynccontextmanager
            async def _cm() -> AsyncIterator[MagicMock]:
                yield _boto()

            return _cm()

        session = MagicMock()
        session.client = _capture

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("aioboto3.Session", lambda: session)
            await client.initialize()

        assert "endpoint_url" not in captured
        assert "aws_access_key_id" not in captured
