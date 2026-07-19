"""Inference-scoring recipe — a local model behind the seam, warm at boot, typed round-trip."""

from __future__ import annotations

from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.integrations.inference import local_inference_lifecycle_step

from examples.recipes.inference_scoring.app import (
    PAYMENT_RISK,
    PaymentFeatures,
    inference_module,
    score_payment,
)


async def test_score_payment_round_trips_through_the_local_model() -> None:
    module = inference_module()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await local_inference_lifecycle_step(module).startup(ctx)

        safe = await score_payment(
            ctx, PaymentFeatures(amount=40.0, velocity_24h=1, new_beneficiary=False)
        )
        risky = await score_payment(
            ctx, PaymentFeatures(amount=950.0, velocity_24h=9, new_beneficiary=True)
        )

    assert 0.0 <= safe.risk <= 1.0
    assert safe.risk < risky.risk  # the model orders an obviously safer payment lower


async def test_batch_scoring_is_order_preserving() -> None:
    module = inference_module()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        port = ctx.inference.model(PAYMENT_RISK)

        batch = [
            PaymentFeatures(amount=10.0 * n, velocity_24h=n, new_beneficiary=False)
            for n in range(1, 4)
        ]
        out = await port.predict_many(batch)

    assert len(out) == 3
    assert out[0].risk < out[1].risk < out[2].risk
