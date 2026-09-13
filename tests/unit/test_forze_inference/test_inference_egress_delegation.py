"""The inference configs now share the gate, and must refuse exactly as they did before.

An inference route is unconditionally sensitive — a served model is handed real feature
values or it cannot score them — so these configs expose no way to turn the gate off.
Delegating to the shared helper must not change that, nor the sentence an operator reads.
"""

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_inference.http.execution.deps.configs import HttpInferenceConfig
from forze_inference.sagemaker.execution.deps.configs import SageMakerInferenceConfig

pytestmark = pytest.mark.unit

# ----------------------- #

_DETAIL = (
    "this route sends feature values in plaintext to an external endpoint, and the "
    "operator must state that consciously."
)


class TestTheRefusalIsUnchanged:
    @pytest.mark.parametrize(
        ("build", "subject"),
        [
            (lambda ack: HttpInferenceConfig(
                protocol="mlflow", model_name="m", acknowledge_data_egress=ack
            ), "HttpInferenceConfig"),
            (lambda ack: SageMakerInferenceConfig(
                endpoint_name="e", acknowledge_data_egress=ack
            ), "SageMakerInferenceConfig"),
        ],
        ids=["http", "sagemaker"],
    )
    def test_an_unacknowledged_route_is_refused_with_the_same_sentence(
        self, build, subject: str
    ) -> None:
        with pytest.raises(CoreException) as caught:
            build(False)

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.summary == (
            f"{subject} requires acknowledge_data_egress=True: {_DETAIL}"
        )

    @pytest.mark.parametrize(
        "build",
        [
            lambda: HttpInferenceConfig(
                protocol="mlflow", model_name="m", acknowledge_data_egress=True
            ),
            lambda: SageMakerInferenceConfig(
                endpoint_name="e", acknowledge_data_egress=True
            ),
        ],
        ids=["http", "sagemaker"],
    )
    def test_an_acknowledged_route_wires(self, build) -> None:
        assert build().acknowledge_data_egress is True

    def test_there_is_no_way_to_declare_an_inference_route_insensitive(self) -> None:
        # The shared helper takes a sensitivity flag; these configs pin it True rather
        # than exposing it, so no wiring can opt an inference route out of the gate.
        assert not hasattr(
            HttpInferenceConfig(
                protocol="mlflow", model_name="m", acknowledge_data_egress=True
            ),
            "egress_sensitive",
        )
