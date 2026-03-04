import io
from pathlib import Path

from forze.base.files import iter_file, read_text, read_yaml


def test_read_yaml_empty_file(tmp_path: Path) -> None:
    p = tmp_path / "empty.yaml"
    p.write_text("")

    data = read_yaml(p)
    assert data == {}


def test_read_yaml_non_empty(tmp_path: Path) -> None:
    p = tmp_path / "data.yaml"
    p.write_text("a: 1\nb: test\n")

    data = read_yaml(p)
    assert data == {"a": 1, "b": "test"}


def test_read_text_returns_full_contents(tmp_path: Path) -> None:
    p = tmp_path / "file.txt"
    p.write_text("hello\nworld")

    assert read_text(p) == "hello\nworld"


def test_iter_file_from_bytes() -> None:
    b = b"abcdef"
    chunks = list(iter_file(b))
    # default chunk size is large, for small bytes we expect a single chunk
    assert chunks == [b"abcdef"]


def test_iter_file_from_bytesio() -> None:
    """iter_file with file-like object yields chunks and closes when exhausted."""
    data = b"x" * (64 * 1024)  # 64 KB to get multiple chunks
    bio = io.BytesIO(data)
    chunks = list(iter_file(bio))
    assert len(chunks) >= 2
    assert b"".join(chunks) == data
    assert bio.closed

