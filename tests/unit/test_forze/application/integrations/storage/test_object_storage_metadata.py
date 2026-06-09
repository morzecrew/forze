"""Unit tests for object metadata decoding."""

from forze.application.integrations.storage import object_metadata_from_user_metadata


def test_object_metadata_from_user_metadata_coerces_string_size() -> None:
    meta = object_metadata_from_user_metadata(
        {
            "filename": "Zm9v.txt",
            "size": "42",
            "created_at": "2025-01-15T12:00:00+00:00",
        },
    )
    assert meta.size == 42
    assert meta.filename == "Zm9v.txt"


def test_object_metadata_from_user_metadata_accepts_zulu_timestamp() -> None:
    meta = object_metadata_from_user_metadata(
        {
            "filename": "x",
            "size": "1",
            "created_at": "2025-01-15T12:00:00Z",
        },
    )
    assert meta.created_at.isoformat().startswith("2025-01-15T12:00:00")
