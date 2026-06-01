"""Smoke tests for S3 storage adapter MIME guessing."""

import magic

from forze_s3.adapters.storage import S3StorageAdapter


def test_s3_storage_adapter_uses_magic_for_content_type() -> None:
    content_type = S3StorageAdapter._guess_content_type("file.bin", b"\x89PNG\r\n\x1a\n")
    assert content_type == magic.from_buffer(b"\x89PNG\r\n\x1a\n", mime=True)
