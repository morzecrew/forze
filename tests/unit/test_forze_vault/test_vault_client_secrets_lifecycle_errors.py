"""Error translation for the Vault client's secrets-lifecycle methods (mocked hvac):
versioned reads, metadata, writes, and the database-engine lease surface."""

from unittest.mock import MagicMock

import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("hvac")

from hvac.exceptions import InvalidPath, VaultError

from forze_vault.kernel.client import VaultClient, VaultConfig

# ----------------------- #


def _client(mock_hvac: MagicMock) -> VaultClient:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    client._client = mock_hvac

    return client


class TestReadKvDataVersioned:
    async def test_returns_data_and_version_from_one_response(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"value": "dsn"}, "metadata": {"version": 3}},
        }

        data, version = await _client(mock_hvac).read_kv_data_versioned("p")

        assert (data, version) == ({"value": "dsn"}, 3)

    async def test_not_found(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.read_secret_version.side_effect = InvalidPath()

        with pytest.raises(CoreException, match="No secret"):
            await _client(mock_hvac).read_kv_data_versioned("missing")

    async def test_vault_error(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.read_secret_version.side_effect = VaultError("down")

        with pytest.raises(CoreException, match="read failed"):
            await _client(mock_hvac).read_kv_data_versioned("p")

    async def test_malformed_payload(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"value": "dsn"}, "metadata": {}},  # no version
        }

        with pytest.raises(CoreException, match="unexpected payload"):
            await _client(mock_hvac).read_kv_data_versioned("p")


class TestReadKvMetadata:
    async def test_returns_metadata_dict(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.read_secret_metadata.return_value = {
            "data": {"current_version": 7},
        }

        metadata = await _client(mock_hvac).read_kv_metadata("p")

        assert metadata["current_version"] == 7

    async def test_not_found(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.read_secret_metadata.side_effect = InvalidPath()

        with pytest.raises(CoreException, match="No secret"):
            await _client(mock_hvac).read_kv_metadata("missing")

    async def test_vault_error(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.read_secret_metadata.side_effect = VaultError("down")

        with pytest.raises(CoreException, match="metadata read failed"):
            await _client(mock_hvac).read_kv_metadata("p")

    async def test_malformed_payload(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.read_secret_metadata.return_value = {"data": None}

        with pytest.raises(CoreException, match="unexpected payload"):
            await _client(mock_hvac).read_kv_metadata("p")


class TestWriteKvData:
    async def test_returns_new_version(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.create_or_update_secret.return_value = {
            "data": {"version": 5},
        }

        assert await _client(mock_hvac).write_kv_data("p", {"value": "x"}) == 5

    async def test_vault_error(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.create_or_update_secret.side_effect = VaultError("down")

        with pytest.raises(CoreException, match="write failed"):
            await _client(mock_hvac).write_kv_data("p", {"value": "x"})

    async def test_malformed_payload(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.create_or_update_secret.return_value = {"data": {}}

        with pytest.raises(CoreException, match="unexpected payload"):
            await _client(mock_hvac).write_kv_data("p", {"value": "x"})

    async def test_cas_is_passed_through(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.create_or_update_secret.return_value = {
            "data": {"version": 8},
        }

        assert await _client(mock_hvac).write_kv_data("p", {"value": "x"}, cas=7) == 8
        mock_hvac.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
            path="p", secret={"value": "x"}, cas=7, mount_point="secret"
        )

    async def test_cas_mismatch_is_a_concurrency_error(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.create_or_update_secret.side_effect = VaultError(
            "check-and-set parameter did not match the current version"
        )

        with pytest.raises(CoreException, match="changed since") as excinfo:
            await _client(mock_hvac).write_kv_data("p", {"value": "x"}, cas=7)

        assert excinfo.value.code == "secret_version_conflict"

    async def test_unconditional_write_never_maps_to_concurrency(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.create_or_update_secret.side_effect = VaultError(
            "check-and-set parameter did not match the current version"
        )

        # Without a fence the same message is an infrastructure failure (e.g. a
        # cas_required mount rejecting a bare write) — never a silent retry signal.
        with pytest.raises(CoreException, match="write failed"):
            await _client(mock_hvac).write_kv_data("p", {"value": "x"})


class TestDatabaseLeases:
    async def test_generate_credentials_passes_the_response_through(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.database.generate_credentials.return_value = {
            "lease_id": "database/creds/app/1",
            "lease_duration": 300,
            "renewable": True,
            "data": {"username": "u", "password": "p"},
        }

        response = await _client(mock_hvac).db_generate_credentials("app")

        assert response["lease_id"] == "database/creds/app/1"
        mock_hvac.secrets.database.generate_credentials.assert_called_once_with(
            name="app", mount_point="database"
        )

    async def test_generate_credentials_unknown_role(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.database.generate_credentials.side_effect = InvalidPath()

        with pytest.raises(CoreException, match="No database role"):
            await _client(mock_hvac).db_generate_credentials("ghost")

    async def test_generate_credentials_vault_error(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.database.generate_credentials.side_effect = VaultError("down")

        with pytest.raises(CoreException, match="credential mint failed"):
            await _client(mock_hvac).db_generate_credentials("app")

    async def test_generate_credentials_malformed_payload(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.secrets.database.generate_credentials.return_value = {"data": "nope"}

        with pytest.raises(CoreException, match="unexpected payload"):
            await _client(mock_hvac).db_generate_credentials("app")

    async def test_renew_lease_returns_granted_seconds(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.sys.renew_lease.return_value = {"lease_duration": 120}

        assert await _client(mock_hvac).renew_lease("lease-1", 300) == 120
        mock_hvac.sys.renew_lease.assert_called_once_with(lease_id="lease-1", increment=300)

    async def test_renew_lease_vault_error(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.sys.renew_lease.side_effect = VaultError("down")

        with pytest.raises(CoreException, match="renewal failed"):
            await _client(mock_hvac).renew_lease("lease-1", 300)

    async def test_renew_lease_malformed_payload(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.sys.renew_lease.return_value = {}

        with pytest.raises(CoreException, match="unexpected payload"):
            await _client(mock_hvac).renew_lease("lease-1", 300)

    async def test_revoke_lease(self) -> None:
        mock_hvac = MagicMock()

        await _client(mock_hvac).revoke_lease("lease-1")
        mock_hvac.sys.revoke_lease.assert_called_once_with(lease_id="lease-1")

    async def test_revoke_lease_vault_error(self) -> None:
        mock_hvac = MagicMock()
        mock_hvac.sys.revoke_lease.side_effect = VaultError("down")

        with pytest.raises(CoreException, match="revoke failed"):
            await _client(mock_hvac).revoke_lease("lease-1")


class TestNonVaultErrors:
    """The generic arm: transport-level failures (not hvac exceptions) wrap too."""

    async def test_each_lifecycle_method_wraps_unexpected_errors(self) -> None:
        boom = ConnectionResetError("wire cut")

        cases = [
            ("secrets.kv.v2.read_secret_version", lambda c: c.read_kv_data_versioned("p")),
            ("secrets.kv.v2.read_secret_metadata", lambda c: c.read_kv_metadata("p")),
            (
                "secrets.kv.v2.create_or_update_secret",
                lambda c: c.write_kv_data("p", {"value": "x"}),
            ),
            (
                "secrets.database.generate_credentials",
                lambda c: c.db_generate_credentials("r"),
            ),
            ("sys.renew_lease", lambda c: c.renew_lease("l", 1)),
            ("sys.revoke_lease", lambda c: c.revoke_lease("l")),
        ]

        for attribute, call in cases:
            mock_hvac = MagicMock()
            target = mock_hvac

            for part in attribute.split("."):
                target = getattr(target, part)

            target.side_effect = boom

            with pytest.raises(CoreException, match="failed"):
                await call(_client(mock_hvac))


class TestUninitializedClient:
    async def test_lifecycle_methods_require_initialization(self) -> None:
        client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))

        for call in (
            client.read_kv_data_versioned("p"),
            client.read_kv_metadata("p"),
            client.write_kv_data("p", {}),
            client.db_generate_credentials("r"),
            client.renew_lease("l", 1),
            client.revoke_lease("l"),
        ):
            with pytest.raises(CoreException, match="not initialized"):
                await call
