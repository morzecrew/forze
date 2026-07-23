"""Async SageMaker runtime client with inference error taxonomy."""

import asyncio
import json
from typing import Any, cast, final

from forze_inference.sagemaker._compat import require_inference_sagemaker

require_inference_sagemaker()

# ....................... #

import aioboto3
import attrs
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import SecretStr

from forze.base.exceptions import exc
from forze.base.logging import get_logger
from forze.base.primitives import GuardedLifecycle

from .port import SageMakerRuntimeClientPort

# ----------------------- #

logger = get_logger("forze_inference.sagemaker")

_THROTTLED_CODES = frozenset(
    {"ThrottlingException", "TooManyRequestsException", "ThrottledException"}
)
_ROUTE_CODES = frozenset({"ValidationError", "ValidationException"})


def _translate_client_error(error: ClientError) -> Exception:
    """Map a SageMaker runtime error to the inference error taxonomy.

    The upstream message is withheld everywhere — not embedded in the raised error (a
    summary renders verbatim to the API caller for every kind below 500) and **not
    logged either**: a ``ModelError`` message carries whatever the model container
    wrote — the offending feature values, a traceback — and the log scrubber
    recognizes credential-shaped patterns, not arbitrary PII, on the plane declared
    PII-dense by construction. The log records the AWS error code, the message's
    size, and the container's ``LogStreamArn`` — a *pointer* to where the content
    already lives, instead of a copy of it.
    """

    response = error.response or {}
    code = str(response.get("Error", {}).get("Code", ""))
    message = str(response.get("Error", {}).get("Message", ""))

    logger.warning(
        "SageMaker endpoint error %s (%d-char message withheld from logs; container logs: %s)",
        code or "<no code>",
        len(message),
        response.get("LogStreamArn") or "<no log stream>",
    )

    if code in _THROTTLED_CODES:
        return exc.throttled(
            "SageMaker endpoint throttled the request.",
            code="inference_throttled",
        )

    if code == "ModelError":
        # A ModelError wraps whatever the container returned, classified by the
        # container's own status: a 4xx is a payload the model rejects (caller-shaped),
        # while a 5xx — or no status at all — is the container *failing*, which as a
        # caller error would be a permanent 422 with no alert and no retry.
        original_status = response.get("OriginalStatusCode")

        if isinstance(original_status, int) and 400 <= original_status < 500:
            return exc.validation(
                f"SageMaker model rejected the payload ({original_status}).",
                code="inference_output_mismatch",
            )

        return exc.infrastructure(
            "SageMaker model container failed.",
            code="inference_endpoint_unavailable",
        )

    if code in _ROUTE_CODES:
        return exc.configuration(
            "SageMaker endpoint not found or invalid.",
            code="inference_route_mismatch",
        )

    return exc.infrastructure(
        f"SageMaker endpoint failed ({code}).",
        code="inference_endpoint_unavailable",
    )


# ....................... #


@final
@attrs.define(slots=True)
class SageMakerRuntimeClient(SageMakerRuntimeClientPort):
    """Thin wrapper around the aioboto3 ``sagemaker-runtime`` client."""

    __client_cm: Any = attrs.field(default=None, init=False)
    __client: Any = attrs.field(default=None, init=False)
    __lifecycle: GuardedLifecycle = attrs.field(factory=GuardedLifecycle, init=False)

    # ....................... #

    async def initialize(
        self,
        *,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: SecretStr | None = None,
    ) -> None:
        async def setup() -> None:
            session = aioboto3.Session()
            client_kwargs: dict[str, Any] = {}

            if region_name is not None:
                client_kwargs["region_name"] = region_name

            if endpoint_url is not None:
                client_kwargs["endpoint_url"] = endpoint_url

            if access_key_id is not None:
                client_kwargs["aws_access_key_id"] = access_key_id

            if secret_access_key is not None:
                client_kwargs["aws_secret_access_key"] = secret_access_key.get_secret_value()

            # aioboto3 ships no sagemaker-runtime stubs; the client is honestly untyped.
            cm = cast(Any, session).client("sagemaker-runtime", **client_kwargs)
            self.__client = await cm.__aenter__()
            self.__client_cm = cm

        await self.__lifecycle.initialize(
            setup,
            ready=lambda: self.__client is not None,
        )

    # ....................... #

    def _require_client(self) -> Any:
        if self.__client is None:
            raise exc.internal("SageMakerRuntimeClient is not initialized")

        return self.__client

    # ....................... #

    async def invoke_endpoint(
        self,
        endpoint_name: str,
        *,
        body: bytes,
        content_type: str = "application/json",
        accept: str = "application/json",
        target_variant: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        client = self._require_client()

        request: dict[str, Any] = {
            "EndpointName": endpoint_name,
            "Body": body,
            "ContentType": content_type,
            "Accept": accept,
        }

        if target_variant is not None:
            request["TargetVariant"] = target_variant

        try:
            if timeout is not None:
                async with asyncio.timeout(timeout):
                    response = await client.invoke_endpoint(**request)
            else:
                response = await client.invoke_endpoint(**request)

        except TimeoutError as e:
            raise exc.timeout(
                "SageMaker endpoint call exceeded its budget.",
                code="inference_timeout",
            ) from e

        except ClientError as e:
            raise _translate_client_error(e) from e

        except BotoCoreError as e:
            raise exc.infrastructure(
                f"SageMaker endpoint unreachable: {e}",
                code="inference_endpoint_unavailable",
            ) from e

        raw = await response["Body"].read()

        try:
            payload: Any = json.loads(raw)

        except ValueError as e:
            raise exc.validation(
                "SageMaker endpoint returned a non-JSON body.",
                code="inference_output_mismatch",
            ) from e

        if not isinstance(payload, dict):
            raise exc.validation(
                "SageMaker endpoint returned a non-object JSON body.",
                code="inference_output_mismatch",
            )

        return cast(dict[str, Any], payload)

    # ....................... #

    async def aclose(self) -> None:
        if self.__client_cm is not None:
            await self.__client_cm.__aexit__(None, None, None)
            self.__client_cm = None
            self.__client = None

    async def close(self) -> None:
        await self.__lifecycle.close(self.aclose)
