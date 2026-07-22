"""The three KMS error mappers — every branch of the backend→`CoreException` translation.

A backend failure has to land on the right side of two lines. The client/server one: a
corrupt or foreign wrapped data key is caller-caused (``validation``). And the
transient/permanent one, which decides whether a consumer retries at all.

That second line runs through the key's *verified* state, not through how the failure
reads. Only a state the backend names outright — deleted, disabled, destroyed — is
``configuration`` (non-retryable); retrying one of those forever is what turned a revoked
key into a consumer that crash-restarted every few seconds while alerting nobody.

Everything else stays ``infrastructure``, including three that read permanent but are not:

* a **denied permission** — IAM and key policies propagate, so a freshly granted principal
  is denied for seconds before it is allowed;
* AWS **KMSInvalidStateException** — it reports only that the state forbids the call, which
  covers ``Creating`` / ``Updating`` as much as ``PendingDeletion``;
* a **precondition failure** that does not name a dead key-version state — an unreachable
  external key manager raises the same code, as does a key still being created. Only GCP
  names the state at all; Yandex Cloud's status carries no discriminator, so every
  precondition failure there stays retryable.

Ambiguity resolves toward retrying: a fault that never clears still terminates by
exhausting the supervisor's crash ceiling, whereas a wrongly-permanent classification
strands a consumer on an outage that would have fixed itself.

These paths are hard to provoke against a live service, so they are pinned here.
"""

from typing import Any

import pytest

pytest.importorskip("aioboto3")
pytest.importorskip("google.cloud.kms")
pytest.importorskip("yandexcloud")

import grpc
from botocore import exceptions as boto_errors
from google.api_core import exceptions as gcp_errors

from forze.base.exceptions import CoreException, ExceptionKind, exception_egress_policy
from forze_kms.aws.kernel.client.errors import _awskms_eh
from forze_kms.gcp.kernel.client.errors import _gcpkms_eh
from forze_kms.yc.kernel.client.errors import _yckms_eh

# ----------------------- #

_DETAILS = {"key_id": "cmk"}


def _client_error(code: str) -> boto_errors.ClientError:
    return boto_errors.ClientError({"Error": {"Code": code}}, "Decrypt")


class _RpcError(grpc.RpcError):
    def __init__(self, status: grpc.StatusCode) -> None:
        self._status = status

    def code(self) -> grpc.StatusCode:
        return self._status


# ....................... #


class TestAwsErrorMapper:
    @pytest.mark.parametrize(
        "error",
        [
            boto_errors.EndpointConnectionError(endpoint_url="http://x"),
            boto_errors.ConnectTimeoutError(endpoint_url="http://x"),
            boto_errors.ReadTimeoutError(endpoint_url="http://x"),
            boto_errors.NoCredentialsError(),
            boto_errors.PartialCredentialsError(provider="p", cred_var="v"),
            boto_errors.SSLError(endpoint_url="http://x", error="bad"),
        ],
    )
    def test_transport_failures_are_infrastructure(self, error: BaseException) -> None:
        mapped = _awskms_eh(error, site="awskms.decrypt", details=_DETAILS)

        assert mapped is not None
        assert mapped.kind is ExceptionKind.INFRASTRUCTURE

    @pytest.mark.parametrize("code", ["InvalidCiphertextException", "IncorrectKeyException"])
    def test_a_bad_wrapped_key_is_caller_caused(self, code: str) -> None:
        """A corrupt or foreign blob must not be masked as a server fault."""

        mapped = _awskms_eh(_client_error(code), site="awskms.decrypt")

        assert mapped is not None
        assert mapped.kind is ExceptionKind.VALIDATION
        assert mapped.code == "core.crypto.wrapped_key_invalid"

    @pytest.mark.parametrize(
        "code",
        [
            "NotFoundException",
            "DisabledException",
        ],
    )
    def test_permanent_key_faults_are_not_retryable(self, code: str) -> None:
        """A revoked, deleted or disabled key never comes back on its own.

        Classified as configuration so the egress policy reports it non-retryable: as
        infrastructure these drove a decrypt loop to crash-restart forever.
        """

        mapped = _awskms_eh(_client_error(code), site="awskms.decrypt")

        assert mapped is not None
        assert mapped.kind is ExceptionKind.CONFIGURATION
        assert exception_egress_policy(mapped.kind).retryable is False
        # Still an internal fault: never expose key/ARN details to a caller.
        assert exception_egress_policy(mapped.kind).expose_details is False

    @pytest.mark.parametrize(
        "code",
        [
            # IAM / key policies propagate, so a denial can clear on its own.
            "AccessDeniedException",
            "KMSAccessDeniedException",
            "KeyUnavailableException",  # AWS documents this one as retryable
            # Ambiguous: also covers Creating / Updating / Unavailable, which self-clear.
            "KMSInvalidStateException",
            "ThrottlingException",
            "LimitExceededException",
            "KMSInternalException",
            "InternalError",
            "SomethingUnmapped",
        ],
    )
    def test_transient_faults_stay_retryable(self, code: str) -> None:
        mapped = _awskms_eh(_client_error(code), site="awskms.decrypt")

        assert mapped is not None
        assert mapped.kind is ExceptionKind.INFRASTRUCTURE
        assert exception_egress_policy(mapped.kind).retryable is True

    def test_generic_botocore_error_is_infrastructure(self) -> None:
        mapped = _awskms_eh(boto_errors.BotoCoreError(), site="awskms.decrypt")

        assert mapped is not None
        assert mapped.kind is ExceptionKind.INFRASTRUCTURE

    def test_an_unrelated_exception_is_not_claimed(self) -> None:
        """Returning None lets the caller's own error surface untouched."""

        assert _awskms_eh(ValueError("nope"), site="awskms.decrypt") is None


# ....................... #


class TestGcpErrorMapper:
    def test_invalid_argument_on_a_crypto_call_names_the_ciphertext(self) -> None:
        mapped = _gcpkms_eh(
            gcp_errors.InvalidArgument("bad"), site="gcpkms.decrypt", details=_DETAILS
        )

        assert mapped is not None
        assert mapped.kind is ExceptionKind.VALIDATION
        assert mapped.code == "core.crypto.wrapped_key_invalid"

    @pytest.mark.parametrize(
        "site", ["gcpkms.ensure_crypto_key", "gcpkms.destroy_crypto_key_versions"]
    )
    def test_invalid_argument_on_provisioning_is_not_dressed_as_a_ciphertext_fault(
        self, site: str
    ) -> None:
        """A malformed key ring is not a bad wrapped key — it must not borrow that code."""

        mapped = _gcpkms_eh(gcp_errors.InvalidArgument("bad"), site=site)

        assert mapped is not None
        assert mapped.kind is ExceptionKind.VALIDATION
        assert mapped.code != "core.crypto.wrapped_key_invalid"
        assert "ciphertext" not in str(mapped).lower()

    @pytest.mark.parametrize(
        "error",
        [
            gcp_errors.NotFound("x"),
            gcp_errors.FailedPrecondition(
                "Resource p has value DESTROYED in field crypto_key_version.state."
            ),
            gcp_errors.FailedPrecondition(
                "Resource p has value DISABLED in field crypto_key_version.state."
            ),
            # Terminal outcomes of a generation/import that will never complete on its own.
            gcp_errors.FailedPrecondition(
                "Resource p has value IMPORT_FAILED in field crypto_key_version.state."
            ),
            gcp_errors.FailedPrecondition(
                "Resource p has value GENERATION_FAILED in field crypto_key_version.state."
            ),
            gcp_errors.FailedPrecondition(
                "Resource p has value PENDING_EXTERNAL_DESTRUCTION in field "
                "crypto_key_version.state."
            ),
            gcp_errors.FailedPrecondition(
                "Resource p has value EXTERNAL_DESTRUCTION_FAILED in field "
                "crypto_key_version.state."
            ),
        ],
    )
    def test_permanent_key_faults_are_not_retryable(self, error: BaseException) -> None:
        mapped = _gcpkms_eh(error, site="gcpkms.decrypt")

        assert mapped is not None
        assert mapped.kind is ExceptionKind.CONFIGURATION
        assert exception_egress_policy(mapped.kind).retryable is False
        assert exception_egress_policy(mapped.kind).expose_details is False

    @pytest.mark.parametrize(
        "error",
        [
            # A precondition failure that does not name a dead key-version state: an
            # unreachable external key manager, or anything unrecognized.
            gcp_errors.FailedPrecondition("Cannot make request to EKM: connection failed"),
            gcp_errors.FailedPrecondition("x"),
            # Mid-generation / mid-import: these clear on their own within seconds.
            gcp_errors.FailedPrecondition(
                "Resource p has value PENDING_GENERATION in field crypto_key_version.state."
            ),
            gcp_errors.FailedPrecondition(
                "Resource p has value PENDING_IMPORT in field crypto_key_version.state."
            ),
            # IAM bindings propagate, so a denial can clear on its own.
            gcp_errors.PermissionDenied("x"),
            gcp_errors.Unauthenticated("x"),
            gcp_errors.ResourceExhausted("x"),
            gcp_errors.ServiceUnavailable("x"),
            gcp_errors.DeadlineExceeded("x"),
            gcp_errors.RetryError("x", cause=None),
            gcp_errors.GoogleAPICallError("x"),
        ],
    )
    def test_transient_faults_stay_retryable(self, error: BaseException) -> None:
        mapped = _gcpkms_eh(error, site="gcpkms.decrypt")

        assert mapped is not None
        assert mapped.kind is ExceptionKind.INFRASTRUCTURE
        assert exception_egress_policy(mapped.kind).retryable is True

    def test_an_unrelated_exception_is_not_claimed(self) -> None:
        assert _gcpkms_eh(ValueError("nope"), site="gcpkms.decrypt") is None


# ....................... #


class TestYcErrorMapper:
    def test_invalid_argument_is_caller_caused(self) -> None:
        mapped = _yckms_eh(
            _RpcError(grpc.StatusCode.INVALID_ARGUMENT),
            site="yckms.decrypt",
            details=_DETAILS,
        )

        assert mapped is not None
        assert mapped.kind is ExceptionKind.VALIDATION
        assert mapped.code == "core.crypto.wrapped_key_invalid"

    @pytest.mark.parametrize("status", [grpc.StatusCode.NOT_FOUND])
    def test_permanent_key_faults_are_not_retryable(self, status: Any) -> None:
        mapped = _yckms_eh(_RpcError(status), site="yckms.decrypt")

        assert mapped is not None
        assert mapped.kind is ExceptionKind.CONFIGURATION
        assert exception_egress_policy(mapped.kind).retryable is False
        assert exception_egress_policy(mapped.kind).expose_details is False

    @pytest.mark.parametrize(
        "status",
        [
            # IAM bindings propagate, so a denial can clear on its own.
            grpc.StatusCode.PERMISSION_DENIED,
            grpc.StatusCode.UNAUTHENTICATED,
            # Ambiguous: the status names no state, so this covers a key still being
            # created or propagating a change as much as a disabled one.
            grpc.StatusCode.FAILED_PRECONDITION,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.INTERNAL,
        ],
    )
    def test_transient_faults_stay_retryable(self, status: Any) -> None:
        mapped = _yckms_eh(_RpcError(status), site="yckms.decrypt")

        assert mapped is not None
        assert mapped.kind is ExceptionKind.INFRASTRUCTURE
        assert exception_egress_policy(mapped.kind).retryable is True

    def test_an_rpc_error_without_a_code_still_maps(self) -> None:
        """`grpc.RpcError` is only *usually* a `Call`; a bare one must not crash."""

        mapped = _yckms_eh(grpc.RpcError(), site="yckms.decrypt")

        assert mapped is not None
        assert mapped.kind is ExceptionKind.INFRASTRUCTURE

    def test_an_unrelated_exception_is_not_claimed(self) -> None:
        assert _yckms_eh(ValueError("nope"), site="yckms.decrypt") is None


# ....................... #


def test_every_mapper_returns_a_core_exception() -> None:
    """The interceptor contract: a claimed error is always a CoreException."""

    for mapped in (
        _awskms_eh(_client_error("AccessDeniedException"), site="s"),
        _gcpkms_eh(gcp_errors.NotFound("x"), site="s"),
        _yckms_eh(_RpcError(grpc.StatusCode.UNAVAILABLE), site="s"),
    ):
        assert isinstance(mapped, CoreException)
