"""The Yandex Cloud KMS client's lifecycle and key administration (mocked SDK).

Yandex Cloud publishes no KMS emulator, so nothing here is reachable by an integration
test: these pin the control plane (create / list / delete), its long-running-operation
handling, and that the configured deadline covers both the RPC and the wait.
"""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("yandexcloud")

from forze.base.exceptions import CoreException
from forze_kms.yc import YcKmsClient, YcKmsConfig

# ----------------------- #

_FOLDER = "fldr-1"


def _admin(client: YcKmsClient, sdk: Any, key_stub: Any) -> YcKmsClient:
    client._YcKmsClient__sdk = sdk  # type: ignore[attr-defined]  # noqa: SLF001
    client._YcKmsClient__key_stub = key_stub  # type: ignore[attr-defined]  # noqa: SLF001
    client._YcKmsClient__stub = MagicMock()  # type: ignore[attr-defined]  # noqa: SLF001

    return client


def _page(keys: list[Any], next_token: str = "") -> SimpleNamespace:
    return SimpleNamespace(keys=keys, next_page_token=next_token)


def _key(name: str, key_id: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, id=key_id)


# ....................... #


class TestConfig:
    def test_a_non_positive_timeout_fails_closed(self) -> None:
        with pytest.raises(CoreException):
            YcKmsConfig(request_timeout=0)


# ....................... #


class TestLifecycle:
    async def test_admin_calls_before_initialize_fail_closed(self) -> None:
        with pytest.raises(CoreException):
            await YcKmsClient().find_key_id_by_name(_FOLDER, "n")

    async def test_close_before_initialize_is_a_no_op(self) -> None:
        await YcKmsClient().close()

    async def test_health_reports_a_failure_without_raising(self) -> None:
        message, ok = await YcKmsClient().health()

        assert ok is False
        assert message

    async def test_initialize_builds_the_stubs_and_close_releases_them(self) -> None:
        sdk = MagicMock()
        sdk.client = MagicMock(side_effect=lambda _stub: MagicMock())
        client = YcKmsClient()

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "forze_kms.yc.kernel.client.client.yandexcloud.SDK",
                MagicMock(return_value=sdk),
            )
            await client.initialize(iam_token="t")
            await client.initialize(iam_token="other")  # idempotent

        assert await client.health() == ("ok", True)

        await client.close()

        with pytest.raises(CoreException):
            await client.find_key_id_by_name(_FOLDER, "n")

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({"iam_token": "t"}, "iam_token"),
            ({"oauth_token": "o"}, "token"),
            ({"service_account_key": {"id": "k"}}, "service_account_key"),
        ],
    )
    async def test_each_credential_form_reaches_the_sdk(
        self, kwargs: dict[str, Any], expected: str
    ) -> None:
        built: dict[str, Any] = {}

        def _sdk(**sdk_kwargs: Any) -> MagicMock:
            built.update(sdk_kwargs)
            sdk = MagicMock()
            sdk.client = MagicMock(side_effect=lambda _stub: MagicMock())

            return sdk

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("forze_kms.yc.kernel.client.client.yandexcloud.SDK", _sdk)
            await YcKmsClient().initialize(
                config=YcKmsConfig(endpoint="api.example"), **kwargs
            )

        assert expected in built
        assert built["endpoint"] == "api.example"

    async def test_no_credential_defers_to_the_metadata_service(self) -> None:
        built: dict[str, Any] = {}

        def _sdk(**sdk_kwargs: Any) -> MagicMock:
            built.update(sdk_kwargs)
            sdk = MagicMock()
            sdk.client = MagicMock(side_effect=lambda _stub: MagicMock())

            return sdk

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("forze_kms.yc.kernel.client.client.yandexcloud.SDK", _sdk)
            await YcKmsClient().initialize()

        assert built == {}


# ....................... #


class TestKeyAdministration:
    async def test_find_key_id_by_name_pages_until_it_matches(self) -> None:
        """Yandex Cloud has no get-by-name, so the folder is paged and matched."""

        key_stub = MagicMock()
        key_stub.List = MagicMock(
            side_effect=[
                _page([_key("other", "abj-other")], next_token="p2"),
                _page([_key("wanted", "abj-wanted")]),
            ]
        )
        client = _admin(YcKmsClient(), MagicMock(), key_stub)

        assert await client.find_key_id_by_name(_FOLDER, "wanted") == "abj-wanted"
        assert key_stub.List.call_count == 2

    async def test_an_unknown_name_is_none(self) -> None:
        key_stub = MagicMock()
        key_stub.List = MagicMock(return_value=_page([_key("other", "abj-other")]))
        client = _admin(YcKmsClient(), MagicMock(), key_stub)

        assert await client.find_key_id_by_name(_FOLDER, "missing") is None

    async def test_create_key_awaits_the_operation_and_returns_the_minted_id(
        self,
    ) -> None:
        """The id is minted by the service, so it comes back through the operation."""

        sdk = MagicMock()
        sdk.wait_operation_and_get_result = MagicMock(
            return_value=SimpleNamespace(response=SimpleNamespace(id="abj-new"))
        )
        key_stub = MagicMock()
        client = _admin(YcKmsClient(), sdk, key_stub)

        key_id = await client.create_key(_FOLDER, "tenant-x", algorithm="AES_128")

        assert key_id == "abj-new"
        request = key_stub.Create.call_args[0][0]
        assert request.folder_id == _FOLDER
        assert request.name == "tenant-x"

    async def test_a_create_that_yields_no_id_is_an_internal_error(self) -> None:
        sdk = MagicMock()
        sdk.wait_operation_and_get_result = MagicMock(
            return_value=SimpleNamespace(response=SimpleNamespace(id=""))
        )
        client = _admin(YcKmsClient(), sdk, MagicMock())

        with pytest.raises(CoreException):
            await client.create_key(_FOLDER, "tenant-x")

    async def test_delete_key_awaits_the_operation(self) -> None:
        sdk = MagicMock()
        key_stub = MagicMock()
        client = _admin(YcKmsClient(), sdk, key_stub)

        await client.delete_key("abj-1")

        assert key_stub.Delete.call_args[0][0].key_id == "abj-1"
        sdk.wait_operation_and_get_result.assert_called_once()

    async def test_the_deadline_covers_the_rpc_and_the_operation_wait(self) -> None:
        """A long-running create must not outlive the configured timeout."""

        sdk = MagicMock()
        sdk.wait_operation_and_get_result = MagicMock(
            return_value=SimpleNamespace(response=SimpleNamespace(id="abj-new"))
        )
        key_stub = MagicMock()
        client = _admin(YcKmsClient(), sdk, key_stub)
        client._YcKmsClient__request_timeout = 9.0  # type: ignore[attr-defined]  # noqa: SLF001

        await client.create_key(_FOLDER, "tenant-x")

        assert key_stub.Create.call_args.kwargs["timeout"] == 9.0
        assert sdk.wait_operation_and_get_result.call_args.kwargs["timeout"] == 9.0
