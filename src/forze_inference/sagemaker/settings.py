"""Connection settings for one SageMaker runtime client.

The AWS shape again — region, optional endpoint override, optional static credentials —
with no ``config`` property: the step takes a botocore ``AioConfig``, which is a library
object rather than a value an environment carries.
"""

from typing import Self

from pydantic import BaseModel, SecretStr, model_validator

# ----------------------- #


class SageMakerSettings(BaseModel):
    """Region and credentials for one SageMaker runtime client."""

    region_name: str | None = None
    """``None`` lets botocore's own chain resolve it (``AWS_REGION``, profile, IMDS)."""

    endpoint_url: str | None = None
    """Override the service endpoint — a VPC endpoint or a local stub. Unset is the normal
    shape: botocore derives it from the region."""

    access_key_id: str | None = None
    secret_access_key: SecretStr | None = None
    """Both unset defers to botocore's default credential chain — the task or instance
    role, which is how a SageMaker caller usually authenticates."""

    # ....................... #

    @model_validator(mode="after")
    def _credentials_are_both_or_neither(self) -> Self:
        """An incomplete pair cannot sign a request, so the only question is where the
        failure appears. Refused here, it names the environment variable that is missing;
        left alone, it surfaces from inside the AWS client at the first call, naming that
        client's internals instead."""

        if (self.access_key_id is None) != (self.secret_access_key is None):
            raise ValueError(
                "SageMaker static credentials require both access_key_id and "
                "secret_access_key; set both or neither (neither uses the credential chain)"
            )

        return self


# ....................... #

__all__ = ["SageMakerSettings"]
