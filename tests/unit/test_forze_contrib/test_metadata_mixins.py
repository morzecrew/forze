from forze_patterns.metadata import (
    MetadataCreateCmdMixin,
    MetadataMixin,
    MetadataUpdateCmdMixin,
)


class NameDoc(MetadataMixin): ...


def test_metadata_mixin_requires_name() -> None:
    doc = NameDoc(name="Main")
    assert doc.name == "Main"
    assert doc.display_name is None


def test_metadata_create_cmd_mixin_includes_required_name() -> None:
    cmd = MetadataCreateCmdMixin(name="Create")
    assert cmd.name == "Create"


def test_metadata_update_cmd_mixin_allows_optional_fields() -> None:
    cmd = MetadataUpdateCmdMixin(name="Updated", description="Desc")
    assert cmd.name == "Updated"
    assert cmd.description == "Desc"
