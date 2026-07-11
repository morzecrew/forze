"""Pytest configuration for forze_kms.yc integration tests (real Yandex Cloud KMS).

Yandex Cloud publishes no KMS emulator, so — unlike AWS (LocalStack) and GCP
(fake-cloud-kms) — these tests run against the real service and are **skipped
unless credentials are supplied**:

* ``FORZE_YC_KMS_KEY_ID``          — the symmetric key id to wrap data keys under
* ``FORZE_YC_IAM_TOKEN``           — a short-lived IAM token, **or**
* ``FORZE_YC_SERVICE_ACCOUNT_KEY`` — the authorized-key JSON (inline)

CI leaves these unset, so the suite skips there; the mocked-stub unit tests under
``tests/unit/test_forze_kms/yc`` are the CI-runnable coverage.
"""

import json
import os
from typing import AsyncGenerator

import pytest
import pytest_asyncio

pytest.importorskip("yandexcloud")

from forze_kms.yc import YcKmsClient

_KEY_ENV = "FORZE_YC_KMS_KEY_ID"
_IAM_ENV = "FORZE_YC_IAM_TOKEN"
_SA_ENV = "FORZE_YC_SERVICE_ACCOUNT_KEY"


@pytest.fixture(scope="session")
def yc_key_id() -> str:
    """The symmetric key id under test, or skip when not configured."""

    key_id = os.environ.get(_KEY_ENV)

    if not key_id:
        pytest.skip(
            f"Set {_KEY_ENV} (plus {_IAM_ENV} or {_SA_ENV}) to run the "
            f"Yandex Cloud KMS integration tests"
        )

    return key_id


@pytest_asyncio.fixture(scope="function")
async def yc_kms_client(yc_key_id: str) -> AsyncGenerator[YcKmsClient]:
    """Provide an initialized Yandex Cloud KMS client against the real service."""

    _ = yc_key_id  # ordering: skip on a missing key before touching credentials

    iam_token = os.environ.get(_IAM_ENV)
    raw_key = os.environ.get(_SA_ENV)

    if not iam_token and not raw_key:
        pytest.skip(f"Set {_IAM_ENV} or {_SA_ENV} to authenticate against Yandex Cloud")

    service_account_key = json.loads(raw_key) if raw_key else None

    client = YcKmsClient()
    await client.initialize(
        iam_token=iam_token,
        service_account_key=service_account_key,
    )

    yield client

    await client.close()
