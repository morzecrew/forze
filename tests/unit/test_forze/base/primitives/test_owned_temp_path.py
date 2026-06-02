"""Unit tests for :class:`~forze.base.primitives.owned_temp_path.OwnedTempPath`."""

from pathlib import Path

from forze.base.primitives.owned_temp_path import OwnedTempPath

# ----------------------- #

_SA_JSON = '{"type":"service_account","project_id":"p"}'


def test_materialize_and_release_owned_file() -> None:
    owned = OwnedTempPath.materialize_text(_SA_JSON, prefix="forze-test-")

    assert owned.owned is True
    assert owned.path is not None
    assert Path(owned.path).read_text(encoding="utf-8") == _SA_JSON

    path = owned.path
    owned.release()

    assert owned.path is None
    assert owned.owned is False
    assert not Path(path).exists()


def test_release_does_not_delete_unowned_path(tmp_path: Path) -> None:
    key_file = tmp_path / "key.json"
    key_file.write_text(_SA_JSON, encoding="utf-8")

    external = OwnedTempPath.unowned(str(key_file))
    external.release()

    assert key_file.exists()
