"""Unit tests for GCP service-account temp file helpers."""

from pathlib import Path

from forze.base.primitives.gcp_service_file import (
    materialize_service_account_json,
    release_service_file,
)

# ----------------------- #

_SA_JSON = '{"type":"service_account","project_id":"p"}'


def test_materialize_and_release_owned_file() -> None:
    path, owned = materialize_service_account_json(_SA_JSON, prefix="forze-test-")

    assert owned is True
    assert Path(path).read_text(encoding="utf-8") == _SA_JSON

    release_service_file(path, owned=True)

    assert not Path(path).exists()


def test_release_does_not_delete_unowned_path(tmp_path: Path) -> None:
    key_file = tmp_path / "key.json"
    key_file.write_text(_SA_JSON, encoding="utf-8")

    release_service_file(str(key_file), owned=False)

    assert key_file.exists()
