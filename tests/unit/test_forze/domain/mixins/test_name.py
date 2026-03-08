from forze.domain.mixins import NameCreateCmdMixin, NameMixin, NameUpdateCmdMixin


class NameDoc(NameMixin): ...


def test_name_mixin_requires_name() -> None:
    doc = NameDoc(name="Main")
    assert doc.name == "Main"
    assert doc.display_name is None


def test_name_create_cmd_mixin_includes_required_name() -> None:
    cmd = NameCreateCmdMixin(name="Create")
    assert cmd.name == "Create"


def test_name_update_cmd_mixin_allows_optional_fields() -> None:
    cmd = NameUpdateCmdMixin(name="Updated", description="Desc")
    assert cmd.name == "Updated"
    assert cmd.description == "Desc"
