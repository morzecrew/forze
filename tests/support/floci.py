"""Shared floci container for AWS-shaped integration fixtures (SQS, KMS, S3).

floci (https://github.com/floci-io/floci) is an independent, MIT-licensed
reimplementation of the AWS wire protocol behind a single edge port. It
replaced LocalStack here when the LocalStack community image was sunset
(2026-03: ``localstack:latest`` began requiring an account and auth token,
and the last free image, 3.8.1, froze without security updates or new API
coverage such as KMS ``RotateKeyOnDemand``).

Because floci is an independent implementation — not the backing OSS engine
wrapped in an AWS façade — behavioral differences against it are findings,
which is what makes it admissible as a test double (see the emulator-fidelity
policy in the integration-test docs).

Known, accepted infidelities (spiked/observed 2026-07 against 1.5.32):

* KMS: the ``KeyId`` parameter on ``Decrypt`` is not enforced — real AWS
  raises ``IncorrectKeyException`` for a blob wrapped under a different CMK,
  floci decrypts it (reported: https://github.com/floci-io/floci/issues/1844). No test relies on that server-side guard: the keyring's
  own confused-deputy check rejects a foreign ``key_id`` before KMS is
  reached. Everything else the adapters use was verified faithful:
  ``EncryptionContext`` mismatch rejection, ``RotateKeyOnDemand``
  transparency, alias lifecycle and alias-form key ids, ``NotFoundException``
  shapes, and key-deletion scheduling.
* S3: presigned-URL verification is immature (tracked:
  https://github.com/floci-io/floci/issues/1841). SigV4 signed-header binding
  is not verified on presigned PUTs (a mismatched ``Content-Type`` upload is
  accepted; real S3 and MinIO return 403), and expiry enforcement is
  environment-dependent (a 1s-expiry URL dies locally but never expires on CI
  runners). Both negative presign tests skip on floci and assert the
  properties against MinIO.
"""

from __future__ import annotations

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

FLOCI_IMAGE = "floci/floci:1.5.32"

_EDGE_PORT = 4566
_READY_LOG = r"EmulatorLifecycle\] Ready\."
_READY_TIMEOUT_S = 120


class FlociContainer(DockerContainer):
    """A floci AWS-emulator container, ready when its lifecycle log says so."""

    def __init__(self, image: str = FLOCI_IMAGE) -> None:
        super().__init__(image)
        self.with_exposed_ports(_EDGE_PORT)

    def start(self) -> "FlociContainer":
        super().start()
        wait_for_logs(self, _READY_LOG, timeout=_READY_TIMEOUT_S)
        return self

    def get_url(self) -> str:
        """AWS endpoint URL (every emulated service on the one edge port)."""

        host = self.get_container_host_ip()
        port = self.get_exposed_port(_EDGE_PORT)
        return f"http://{host}:{port}"
