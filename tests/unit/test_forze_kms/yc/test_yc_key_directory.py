"""`YcKmsKeyDirectory` — resolving a tenant to a key id Yandex Cloud minted itself.

Yandex Cloud has no get-by-name, so every resolve is a List call. That makes the memo
load-bearing (the keyring asks for the previous key once per envelope a migration sweep
reads) — and makes *what is not memoized* load-bearing too: an absence must never stick,
or opening an overlap would not be seen.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("yandexcloud")

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_kms.yc import YcKmsClientPort, YcKmsKeyDirectory

# ----------------------- #

_FOLDER = "fldr-1"


def _tenant() -> TenantIdentity:
    return TenantIdentity(tenant_id=uuid4())


def _client(**lookups: str | None) -> MagicMock:
    """A client resolving each key *name* to the id it was given."""

    client = MagicMock(spec=YcKmsClientPort)
    client.find_key_id_by_name = AsyncMock(
        side_effect=lambda _folder, name: lookups.get(name)
    )

    return client


# ....................... #


class TestResolve:
    async def test_a_tenant_resolves_to_the_id_behind_its_name(self) -> None:
        tenant = _tenant()
        client = _client(**{f"tenant-{tenant.tenant_id}": "abj-1"})
        directory = YcKmsKeyDirectory(client=client, folder_id=_FOLDER)

        assert (await directory.resolve(tenant)).key_id == "abj-1"

    async def test_an_unprovisioned_tenant_fails_closed(self) -> None:
        directory = YcKmsKeyDirectory(client=_client(), folder_id=_FOLDER)

        with pytest.raises(CoreException) as ei:
            await directory.resolve(_tenant())

        assert ei.value.code == "core.crypto.tenant_key_not_provisioned"

    async def test_an_unbound_tenant_uses_the_default_key(self) -> None:
        directory = YcKmsKeyDirectory(
            client=_client(), folder_id=_FOLDER, default_key_id="abj-shared"
        )

        assert (await directory.resolve(None)).key_id == "abj-shared"

    async def test_an_unbound_tenant_without_a_default_fails_closed(self) -> None:
        directory = YcKmsKeyDirectory(client=_client(), folder_id=_FOLDER)

        with pytest.raises(CoreException) as ei:
            await directory.resolve(None)

        assert ei.value.code == "core.crypto.default_key_missing"

    async def test_the_name_template_is_honored(self) -> None:
        tenant = _tenant()
        client = _client(**{f"kek::{tenant.tenant_id}": "abj-1"})
        directory = YcKmsKeyDirectory(
            client=client, folder_id=_FOLDER, template="kek::{tenant_id}"
        )

        assert (await directory.resolve(tenant)).key_id == "abj-1"


# ....................... #


class TestResolvePrevious:
    async def test_no_previous_template_means_no_overlap(self) -> None:
        directory = YcKmsKeyDirectory(client=_client(), folder_id=_FOLDER)

        assert await directory.resolve_previous(_tenant()) is None

    async def test_an_unbound_tenant_has_no_overlap(self) -> None:
        directory = YcKmsKeyDirectory(
            client=_client(), folder_id=_FOLDER, previous_template="old-{tenant_id}"
        )

        assert await directory.resolve_previous(None) is None

    async def test_the_previous_key_resolves_through_its_own_template(self) -> None:
        tenant = _tenant()
        client = _client(**{f"old-{tenant.tenant_id}": "abj-old"})
        directory = YcKmsKeyDirectory(
            client=client, folder_id=_FOLDER, previous_template="old-{tenant_id}"
        )

        previous = await directory.resolve_previous(tenant)

        assert previous is not None
        assert previous.key_id == "abj-old"

    async def test_a_missing_previous_key_is_none(self) -> None:
        directory = YcKmsKeyDirectory(
            client=_client(), folder_id=_FOLDER, previous_template="old-{tenant_id}"
        )

        assert await directory.resolve_previous(_tenant()) is None

    async def test_a_found_previous_key_is_memoized(self) -> None:
        """The keyring asks once per envelope a sweep reads — one List, not thousands."""

        tenant = _tenant()
        client = _client(**{f"old-{tenant.tenant_id}": "abj-old"})
        directory = YcKmsKeyDirectory(
            client=client, folder_id=_FOLDER, previous_template="old-{tenant_id}"
        )

        for _ in range(3):
            assert (await directory.resolve_previous(tenant)) is not None

        assert client.find_key_id_by_name.await_count == 1

    async def test_an_absent_previous_key_is_never_memoized(self) -> None:
        """A remembered absence would strand a migration opened moments later."""

        tenant = _tenant()
        client = MagicMock(spec=YcKmsClientPort)
        client.find_key_id_by_name = AsyncMock(return_value=None)
        directory = YcKmsKeyDirectory(
            client=client, folder_id=_FOLDER, previous_template="old-{tenant_id}"
        )

        assert await directory.resolve_previous(tenant) is None

        # The overlap is opened: the very next resolve must see it.
        client.find_key_id_by_name = AsyncMock(return_value="abj-old")
        previous = await directory.resolve_previous(tenant)

        assert previous is not None
        assert previous.key_id == "abj-old"
