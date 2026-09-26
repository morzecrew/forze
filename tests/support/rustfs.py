"""RustFS: the S3 server the S3 and DuckDB suites run against in MinIO's place.

MinIO stopped publishing images for its free edition, so the suites ran a frozen mirror of its
last release. RustFS is an Apache-2.0 S3 server that is still released, answers MinIO's health
path, and takes its credentials from its own environment variables.
"""

from __future__ import annotations

from minio import Minio
from testcontainers.core.container import DockerContainer

RUSTFS_IMAGE = "rustfs/rustfs:1.0.0"

_S3_PORT = 9000


class RustfsContainer(DockerContainer):
    """A RustFS server, constructed the way ``testcontainers.minio.MinioContainer`` is."""

    def __init__(
        self,
        image: str = RUSTFS_IMAGE,
        port: int = _S3_PORT,
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
    ) -> None:
        super().__init__(image)
        self.port = port
        self.access_key = access_key
        self.secret_key = secret_key
        self.with_exposed_ports(port)
        self.with_env("RUSTFS_ACCESS_KEY", access_key)
        self.with_env("RUSTFS_SECRET_KEY", secret_key)
        self.with_env("RUSTFS_ADDRESS", f":{port}")

    def get_client(self) -> Minio:
        """An S3 client for the server — the MinIO SDK speaks plain S3 to any implementation."""

        return Minio(
            endpoint=f"{self.get_container_host_ip()}:{self.get_exposed_port(self.port)}",
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
        )
