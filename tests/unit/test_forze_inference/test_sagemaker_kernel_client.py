"""The SageMaker runtime client wrapper, with only the AWS SDK boundary faked.

The moto integration test proves the wire; this proves the wrapper's own logic — request
assembly, the error-taxonomy translation, body decoding and teardown — without Docker, and
covers the failure branches a live endpoint will not produce on demand (throttling, model
errors, transport failures, malformed bodies).
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from pydantic import SecretStr

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_inference.sagemaker import (
    RoutedSageMakerRuntimeClient,
    SageMakerInferenceConfig,
    SageMakerInferenceDepsModule,
    SageMakerRuntimeClient,
    routed_sagemaker_inference_lifecycle_step,
    sagemaker_inference_lifecycle_step,
)
from forze_inference.sagemaker.kernel import client as client_module
from forze_inference.sagemaker.kernel.client import _translate_client_error

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")


class _FakeBody:
    def __init__(self, raw: bytes) -> None:
        self._raw = raw

    async def read(self) -> bytes:
        return self._raw


class _FakeRuntime:
    """Stands in for the aioboto3 ``sagemaker-runtime`` client."""

    def __init__(self, *, body: bytes = b"{}", raises: Exception | None = None) -> None:
        self.body = body
        self.raises = raises
        self.calls: list[dict[str, Any]] = []

    async def invoke_endpoint(self, **request: Any) -> dict[str, Any]:
        self.calls.append(request)

        if self.raises is not None:
            raise self.raises

        return {"Body": _FakeBody(self.body)}


class _FakeClientCM:
    def __init__(self, runtime: _FakeRuntime) -> None:
        self.runtime = runtime
        self.exited = False

    async def __aenter__(self) -> _FakeRuntime:
        return self.runtime

    async def __aexit__(self, *exc_info: object) -> None:
        self.exited = True


class _FakeSession:
    """Captures the kwargs the wrapper builds for ``session.client(...)``."""

    last: _FakeSession | None = None

    def __init__(self, runtime: _FakeRuntime) -> None:
        self.runtime = runtime
        self.client_kwargs: dict[str, Any] = {}
        self.service: str | None = None
        self.cm: _FakeClientCM | None = None

    def client(self, service: str, **kwargs: Any) -> _FakeClientCM:
        self.service = service
        self.client_kwargs = kwargs
        self.cm = _FakeClientCM(self.runtime)
        return self.cm


def _fake_aws(monkeypatch: pytest.MonkeyPatch, runtime: _FakeRuntime) -> _FakeSession:
    session = _FakeSession(runtime)
    monkeypatch.setattr(client_module.aioboto3, "Session", lambda: session)
    return session


def _client_error(
    code: str, message: str = "boom", *, original_status: int | None = None
) -> ClientError:
    response: dict[str, Any] = {"Error": {"Code": code, "Message": message}}

    if original_status is not None:
        response["OriginalStatusCode"] = original_status

    return ClientError(response, "InvokeEndpoint")


# ....................... #


class TestErrorTranslation:
    """Every AWS failure class maps onto the port's taxonomy — the table a caller's
    retry policy keys off, so a wrong mapping silently changes retry behavior."""

    @pytest.mark.parametrize(
        ("code", "expected"),
        [
            ("ThrottlingException", "inference_throttled"),
            ("TooManyRequestsException", "inference_throttled"),
            ("ThrottledException", "inference_throttled"),
            ("ValidationError", "inference_route_mismatch"),
            ("ValidationException", "inference_route_mismatch"),
            ("ServiceUnavailable", "inference_endpoint_unavailable"),
            ("", "inference_endpoint_unavailable"),
        ],
    )
    def test_code_maps_to_taxonomy(self, code: str, expected: str) -> None:
        translated = _translate_client_error(_client_error(code))

        assert isinstance(translated, CoreException)
        assert translated.code == expected

    def test_model_error_is_classified_by_the_container_status(self) -> None:
        # A ModelError wraps whatever the container returned. Only a container 4xx is
        # a payload the model rejects (caller-shaped); a 5xx — or no status at all —
        # is the container *failing*, which as a caller error would surface as a
        # permanent 422 with no 5xx alert and no retry.
        rejected = _translate_client_error(_client_error("ModelError", original_status=422))
        crashed = _translate_client_error(_client_error("ModelError", original_status=500))
        bare = _translate_client_error(_client_error("ModelError"))

        assert isinstance(rejected, CoreException)
        assert rejected.code == "inference_output_mismatch"
        assert isinstance(crashed, CoreException)
        assert crashed.code == "inference_endpoint_unavailable"
        assert isinstance(bare, CoreException)
        assert bare.code == "inference_endpoint_unavailable"

    def test_throttling_is_retryable_and_model_rejection_is_not(self) -> None:
        from forze.base.exceptions.egress import exception_egress_policy

        throttled = _translate_client_error(_client_error("ThrottlingException"))
        rejected = _translate_client_error(_client_error("ModelError", original_status=400))

        assert exception_egress_policy(throttled.kind).retryable  # type: ignore[attr-defined]
        assert not exception_egress_policy(rejected.kind).retryable  # type: ignore[attr-defined]

    def test_upstream_message_is_never_embedded(self) -> None:
        # The container's message can carry the offending feature values or its
        # traceback, on the plane declared PII-dense by construction; it is never
        # placed in the summary the API caller sees verbatim.
        leaked = "Traceback: rejected feature ssn=078-05-1120"

        for error in (
            _client_error("ModelError", leaked, original_status=422),
            _client_error("ModelError", leaked),
            _client_error("ThrottlingException", leaked),
            _client_error("ServiceUnavailable", leaked),
        ):
            translated = _translate_client_error(error)

            assert isinstance(translated, CoreException)
            assert "078-05-1120" not in translated.summary
            assert "078-05-1120" not in str(translated.details or {})

    def test_upstream_message_is_never_logged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The log scrubber recognizes credential-shaped patterns, not arbitrary PII —
        # the message is withheld from the log line itself. The log carries the AWS
        # error code, the message's size, and the container's LogStreamArn (a pointer
        # to where the content already lives).
        class _Recorder:
            def __init__(self) -> None:
                self.lines: list[str] = []

            def warning(self, msg: str, *args: object, **kwargs: object) -> None:
                del kwargs
                self.lines.append(str(msg) % args if args else str(msg))

        recorder = _Recorder()
        monkeypatch.setattr(client_module, "logger", recorder)

        leaked = "Traceback: rejected feature ssn=078-05-1120"
        response: dict[str, Any] = {
            "Error": {"Code": "ModelError", "Message": leaked},
            "OriginalStatusCode": 422,
            "LogStreamArn": "arn:aws:logs:x:1:log-group:/sm/endpoint",
        }

        _translate_client_error(ClientError(response, "InvokeEndpoint"))

        assert recorder.lines
        assert all("078-05-1120" not in line for line in recorder.lines)
        assert any("arn:aws:logs" in line for line in recorder.lines)  # the pointer survives


# ....................... #


class TestRuntimeClient:
    @pytest.mark.asyncio
    async def test_initialize_passes_only_the_supplied_credentials(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime = _FakeRuntime(body=b'{"predictions": []}')
        session = _fake_aws(monkeypatch, runtime)
        client = SageMakerRuntimeClient()

        await client.initialize(
            region_name="eu-west-1",
            endpoint_url="http://local",
            access_key_id="AKIA",
            secret_access_key=SecretStr("secret"),
        )

        try:
            assert session.service == "sagemaker-runtime"
            assert session.client_kwargs == {
                "region_name": "eu-west-1",
                "endpoint_url": "http://local",
                "aws_access_key_id": "AKIA",
                "aws_secret_access_key": "secret",
            }
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_initialize_omits_unset_credentials_for_the_default_chain(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unset values must be absent, not ``None`` — botocore falls back to its own
        credential chain only when the kwargs are missing entirely."""

        session = _fake_aws(monkeypatch, _FakeRuntime())
        client = SageMakerRuntimeClient()

        await client.initialize()

        try:
            assert session.client_kwargs == {}
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_invoke_builds_the_request_and_decodes_the_body(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime = _FakeRuntime(body=b'{"predictions": [{"y": 1.0}]}')
        _fake_aws(monkeypatch, runtime)
        client = SageMakerRuntimeClient()
        await client.initialize(region_name="eu-west-1")

        try:
            payload = await client.invoke_endpoint(
                "doubler-prod",
                body=b'{"instances": []}',
                target_variant="blue",
            )
        finally:
            await client.close()

        assert payload == {"predictions": [{"y": 1.0}]}
        assert runtime.calls == [
            {
                "EndpointName": "doubler-prod",
                "Body": b'{"instances": []}',
                "ContentType": "application/json",
                "Accept": "application/json",
                "TargetVariant": "blue",
            }
        ]

    @pytest.mark.asyncio
    async def test_target_variant_is_omitted_when_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime = _FakeRuntime(body=b"{}")
        _fake_aws(monkeypatch, runtime)
        client = SageMakerRuntimeClient()
        await client.initialize()

        try:
            await client.invoke_endpoint("doubler-prod", body=b"{}")
        finally:
            await client.close()

        assert "TargetVariant" not in runtime.calls[0]

    @pytest.mark.asyncio
    async def test_invoke_before_initialize_is_an_internal_error(self) -> None:
        client = SageMakerRuntimeClient()

        with pytest.raises(CoreException, match="not initialized"):
            await client.invoke_endpoint("doubler-prod", body=b"{}")

    @pytest.mark.asyncio
    async def test_timeout_maps_to_the_inference_timeout_code(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _fake_aws(monkeypatch, _FakeRuntime(raises=TimeoutError()))
        client = SageMakerRuntimeClient()
        await client.initialize()

        try:
            with pytest.raises(CoreException) as ei:
                await client.invoke_endpoint("doubler-prod", body=b"{}", timeout=5.0)
        finally:
            await client.close()

        assert ei.value.code == "inference_timeout"

    @pytest.mark.asyncio
    async def test_client_error_is_translated(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _fake_aws(monkeypatch, _FakeRuntime(raises=_client_error("ThrottlingException")))
        client = SageMakerRuntimeClient()
        await client.initialize()

        try:
            with pytest.raises(CoreException) as ei:
                await client.invoke_endpoint("doubler-prod", body=b"{}")
        finally:
            await client.close()

        assert ei.value.code == "inference_throttled"

    @pytest.mark.asyncio
    async def test_transport_failure_is_infrastructure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _fake_aws(
            monkeypatch,
            _FakeRuntime(raises=EndpointConnectionError(endpoint_url="http://nope")),
        )
        client = SageMakerRuntimeClient()
        await client.initialize()

        try:
            with pytest.raises(CoreException) as ei:
                await client.invoke_endpoint("doubler-prod", body=b"{}")
        finally:
            await client.close()

        assert ei.value.code == "inference_endpoint_unavailable"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("raw", [b"not json at all", b"[1, 2, 3]"])
    async def test_malformed_body_fails_at_the_boundary(
        self, monkeypatch: pytest.MonkeyPatch, raw: bytes
    ) -> None:
        """Neither a non-JSON body nor a JSON array is a prediction envelope."""

        _fake_aws(monkeypatch, _FakeRuntime(body=raw))
        client = SageMakerRuntimeClient()
        await client.initialize()

        try:
            with pytest.raises(CoreException) as ei:
                await client.invoke_endpoint("doubler-prod", body=b"{}")
        finally:
            await client.close()

        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.asyncio
    async def test_close_releases_the_aws_context_and_is_idempotent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        session = _fake_aws(monkeypatch, _FakeRuntime())
        client = SageMakerRuntimeClient()
        await client.initialize()

        await client.close()
        await client.close()  # a second shutdown must not explode

        assert session.cm is not None
        assert session.cm.exited


# ....................... #


class TestRoutedRuntimeClient:
    @staticmethod
    def _secrets() -> Any:
        payload = json.dumps(
            {
                "region_name": "eu-west-1",
                "access_key_id": "AKIA",
                "secret_access_key": "secret",
            }
        )

        class _Secrets:
            async def resolve_str(self, ref: SecretRef) -> str:
                return payload

            async def exists(self, ref: SecretRef) -> bool:
                return True

        return _Secrets()

    @pytest.mark.asyncio
    async def test_invocation_uses_the_tenant_credentials(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime = _FakeRuntime(body=b'{"predictions": [{"y": 2.0}]}')
        session = _fake_aws(monkeypatch, runtime)
        routed = RoutedSageMakerRuntimeClient(
            secrets=self._secrets(),
            secret_ref_for_tenant=lambda t: SecretRef(path=f"tenants/{t}/inference"),
            tenant_provider=lambda: _T1,
        )
        await routed.startup()

        try:
            payload = await routed.invoke_endpoint("doubler-prod", body=b"{}")
        finally:
            await routed.close()

        assert payload == {"predictions": [{"y": 2.0}]}
        assert session.client_kwargs["aws_access_key_id"] == "AKIA"

    def test_credential_fingerprint_tracks_rotation(self) -> None:
        from forze_inference.sagemaker.kernel.routing_credentials import (
            SageMakerRoutingCredentials,
        )

        routed = RoutedSageMakerRuntimeClient(
            secrets=self._secrets(),
            secret_ref_for_tenant=lambda t: SecretRef(path=str(t)),
            tenant_provider=lambda: _T1,
        )

        def creds(secret: str) -> SageMakerRoutingCredentials:
            return SageMakerRoutingCredentials(
                region_name="eu-west-1",
                access_key_id="AKIA",
                secret_access_key=SecretStr(secret),
            )

        before = routed.credential_fingerprint(creds("old"))

        assert before == routed.credential_fingerprint(creds("old"))
        assert before != routed.credential_fingerprint(creds("rotated"))


# ....................... #


class TestLifecycleSteps:
    def _module(self, client: Any) -> SageMakerInferenceDepsModule:
        return SageMakerInferenceDepsModule(
            client=client,
            models={
                "doubler": SageMakerInferenceConfig(
                    endpoint_name="doubler-prod",
                    acknowledge_data_egress=True,
                )
            },
        )

    @pytest.mark.asyncio
    async def test_startup_initializes_the_registered_client(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from forze.testing import context_from_modules

        session = _fake_aws(monkeypatch, _FakeRuntime())
        client = SageMakerRuntimeClient()
        module = self._module(client)
        ctx = context_from_modules(module)
        step = sagemaker_inference_lifecycle_step(region_name="eu-west-1")

        await step.startup(ctx)

        try:
            assert session.client_kwargs == {"region_name": "eu-west-1"}
        finally:
            await step.shutdown(ctx)

    @pytest.mark.asyncio
    async def test_startup_leaves_a_custom_port_implementation_alone(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A caller may register their own ``SageMakerRuntimeClientPort``; the hook must
        not try to initialize something it does not own."""

        from forze.testing import context_from_modules

        session = _fake_aws(monkeypatch, _FakeRuntime())

        class _Custom:
            async def invoke_endpoint(self, endpoint_name: str, **kwargs: Any) -> dict[str, Any]:
                return {}

            async def close(self) -> None:
                return None

        ctx = context_from_modules(self._module(_Custom()))

        await sagemaker_inference_lifecycle_step().startup(ctx)

        assert session.client_kwargs == {}  # never touched

    def test_step_ids_are_stable_and_overridable(self) -> None:
        assert sagemaker_inference_lifecycle_step().id == "sagemaker_inference_client"
        assert sagemaker_inference_lifecycle_step(name="custom").id == "custom"

        routed = RoutedSageMakerRuntimeClient(
            secrets=TestRoutedRuntimeClient._secrets(),
            secret_ref_for_tenant=lambda t: SecretRef(path=str(t)),
            tenant_provider=lambda: _T1,
        )

        assert (
            routed_sagemaker_inference_lifecycle_step(routed).id
            == "routed_sagemaker_inference_client"
        )
