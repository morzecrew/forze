"""Compatibility helpers for the optional Yandex Cloud KMS extra."""


def require_kms_yc() -> None:
    """Raise a clear error when the ``kms-yc`` extra is not installed."""

    try:
        import grpc  # pyright: ignore[reportUnusedImport]  # noqa: F401
        import yandexcloud  # pyright: ignore[reportUnusedImport]  # noqa: F401
        from yandex.cloud.kms.v1 import (  # pyright: ignore[reportUnusedImport]  # noqa: F401
            symmetric_crypto_service_pb2_grpc,  # pyright: ignore[reportUnusedImport]
        )
    except ImportError as e:
        raise RuntimeError("forze_kms.yc requires 'forze[kms-yc]' extra") from e
