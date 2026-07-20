"""Recipe: score payments through the inference seam — model in wiring, types in handlers.

One ``InferenceSpec`` names the task (``payment_risk``) with typed input/output models. The
handler resolves ``ctx.inference.model(spec)`` and predicts; *which* model answers is a
wiring fact. Here the wiring binds a hand-rolled in-process model through
``LocalInferenceDepsModule`` — the local adapter takes a **loader callable**, so the same
config would load a pickled sklearn pipeline or an ONNX session without the framework
depending on either. Swapping to a served or cloud model later changes the deps module,
never this file's scoring code.

Predictions run off the event loop under the CPU-offload seam (bounded pool, invocation
deadline, cancellation), and the model loads once at startup via the lifecycle step,
failing boot closed if the artifact is broken.

Run it:  uv run python -m examples.recipes.inference_scoring.app   (no infra)
Exercised by tests/unit/test_examples/test_inference_scoring.py.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Sequence

import structlog
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze.application.execution import (
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
)
from forze.application.integrations.inference import (
    LocalInferenceConfig,
    LocalInferenceDepsModule,
    local_inference_lifecycle_step,
)
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel

_LOGGER_NAME = "inference_scoring"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: LogLevel) -> None:
    # Render this example's narration cleanly **only when run as a script**, leaving
    # global logging untouched so imports/tests are unaffected.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])


# --8<-- [start:spec]
class PaymentFeatures(BaseModel):
    amount: float
    velocity_24h: int
    new_beneficiary: bool


class PaymentRisk(BaseModel):
    risk: float  # 0.0 (safe) .. 1.0 (block)


PAYMENT_RISK = InferenceSpec(
    name="payment_risk",
    input=PaymentFeatures,
    output=PaymentRisk,
)
# --8<-- [end:spec]


# --8<-- [start:model]
class LogisticRiskModel:
    """A stand-in artifact: any object with a sync ``predict_batch``.

    A real deployment returns this from the loader after ``joblib.load(...)`` /
    ``onnxruntime.InferenceSession(...)`` — the framework only sees the callable.
    """

    def predict_batch(self, instances: Sequence[PaymentFeatures]) -> list[PaymentRisk]:
        return [
            PaymentRisk(
                risk=1.0
                / (
                    1.0
                    + math.exp(
                        -(
                            0.002 * i.amount
                            + 0.35 * i.velocity_24h
                            + (1.5 if i.new_beneficiary else 0.0)
                            - 4.0
                        )
                    )
                )
            )
            for i in instances
        ]


def load_risk_model() -> LogisticRiskModel:
    return LogisticRiskModel()
# --8<-- [end:model]


# --8<-- [start:score]
async def score_payment(ctx: ExecutionContext, features: PaymentFeatures) -> PaymentRisk:
    """Handler-side code: typed in, typed out — no model URI, artifact, or protocol."""

    port = ctx.inference.model(PAYMENT_RISK)

    return await port.predict(features)
# --8<-- [end:score]


# --8<-- [start:wiring]
def inference_module() -> LocalInferenceDepsModule:
    return LocalInferenceDepsModule(
        models={"payment_risk": LocalInferenceConfig(loader=load_risk_model)},
    )
# --8<-- [end:wiring]


async def main() -> None:
    module = inference_module()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()

        # Warm the model the way an app boot would (fail-closed on a broken artifact).
        await local_inference_lifecycle_step(module).startup(ctx)

        safe = await score_payment(
            ctx,
            PaymentFeatures(amount=40.0, velocity_24h=1, new_beneficiary=False),
        )
        risky = await score_payment(
            ctx,
            PaymentFeatures(amount=950.0, velocity_24h=9, new_beneficiary=True),
        )

        log.info("scored", safe=round(safe.risk, 3), risky=round(risky.risk, 3))


if __name__ == "__main__":
    _setup_logging("info")
    asyncio.run(main())
