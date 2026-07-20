"""The shipped key directories, including the migration overlap they expose.

`resolve` names the key writes go to; `resolve_previous` names the one reads still accept
while a KEK is being replaced. Both must resolve **per tenant**, and an overlap that was
never configured must stay absent.
"""

from uuid import uuid4

from forze.application.contracts.crypto import (
    KeyDirectoryWithPrevious,
    KeyRef,
    StaticKeyDirectory,
    TenantTemplateKeyDirectory,
)
from forze.application.contracts.tenancy import TenantIdentity

# ----------------------- #


def _tenant() -> TenantIdentity:
    return TenantIdentity(tenant_id=uuid4())


# ....................... #


class TestStaticKeyDirectory:
    async def test_every_tenant_gets_the_one_key(self) -> None:
        directory = StaticKeyDirectory(KeyRef(key_id="kek"))

        assert (await directory.resolve(_tenant())).key_id == "kek"
        assert (await directory.resolve(None)).key_id == "kek"

    async def test_there_is_no_overlap_by_default(self) -> None:
        assert await StaticKeyDirectory(KeyRef(key_id="kek")).resolve_previous(None) is None

    async def test_a_previous_key_opens_the_overlap(self) -> None:
        directory = StaticKeyDirectory(
            KeyRef(key_id="new"), previous_key_ref=KeyRef(key_id="old")
        )
        previous = await directory.resolve_previous(_tenant())

        assert previous is not None
        assert previous.key_id == "old"


# ....................... #


class TestTenantTemplateKeyDirectory:
    async def test_each_tenant_gets_its_own_key(self) -> None:
        directory = TenantTemplateKeyDirectory(
            template="tenant/{tenant_id}/kek", default_key_id="shared"
        )
        tenant = _tenant()

        assert (await directory.resolve(tenant)).key_id == f"tenant/{tenant.tenant_id}/kek"
        assert (await directory.resolve(None)).key_id == "shared"

    async def test_a_fixed_version_rides_on_every_reference(self) -> None:
        directory = TenantTemplateKeyDirectory(
            template="t/{tenant_id}", default_key_id="shared", version="v2"
        )

        assert (await directory.resolve(_tenant())).version == "v2"
        assert (await directory.resolve(None)).version == "v2"

    async def test_there_is_no_overlap_by_default(self) -> None:
        directory = TenantTemplateKeyDirectory(
            template="t/{tenant_id}", default_key_id="shared"
        )

        assert await directory.resolve_previous(_tenant()) is None
        assert await directory.resolve_previous(None) is None

    async def test_the_previous_template_resolves_per_tenant(self) -> None:
        directory = TenantTemplateKeyDirectory(
            template="t/{tenant_id}/v2",
            default_key_id="shared",
            previous_template="t/{tenant_id}/v1",
        )
        tenant = _tenant()

        previous = await directory.resolve_previous(tenant)

        assert previous is not None
        assert previous.key_id == f"t/{tenant.tenant_id}/v1"

    async def test_an_unbound_tenant_overlaps_only_with_a_previous_default(self) -> None:
        without = TenantTemplateKeyDirectory(
            template="t/{tenant_id}",
            default_key_id="new-shared",
            previous_template="old/{tenant_id}",
        )
        assert await without.resolve_previous(None) is None

        with_default = TenantTemplateKeyDirectory(
            template="t/{tenant_id}",
            default_key_id="new-shared",
            previous_default_key_id="old-shared",
        )
        previous = await with_default.resolve_previous(None)

        assert previous is not None
        assert previous.key_id == "old-shared"


# ....................... #


def test_both_shipped_directories_advertise_the_overlap_capability() -> None:
    """The keyring duck-types on this, so a directory without it simply has no overlap."""

    assert isinstance(StaticKeyDirectory(KeyRef(key_id="k")), KeyDirectoryWithPrevious)
    assert isinstance(
        TenantTemplateKeyDirectory(template="t/{tenant_id}", default_key_id="s"),
        KeyDirectoryWithPrevious,
    )

    class _Plain:
        async def resolve(self, tenant: TenantIdentity | None) -> KeyRef:
            _ = tenant
            return KeyRef(key_id="k")

    assert not isinstance(_Plain(), KeyDirectoryWithPrevious)
