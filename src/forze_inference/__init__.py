"""Remote inference backends for the ``forze.application.contracts.inference`` seam.

One submodule per backend, each gated by its own extra (the ``forze_kms`` shape):

- :mod:`forze_inference.http` — served models over HTTP wire protocols
  (KServe V2 / Open Inference Protocol, MLflow ``/invocations``); extra
  ``forze[inference-http]``.
- :mod:`forze_inference.sagemaker` — AWS SageMaker realtime endpoints; extra
  ``forze[inference-sagemaker]``.

This top-level package imports nothing at runtime so installing one extra never
requires the others.
"""
