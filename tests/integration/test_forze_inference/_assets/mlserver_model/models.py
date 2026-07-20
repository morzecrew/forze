"""Custom MLServer runtime for the inference integration tests.

Reads the columnar V2 inputs the forze kserve_v2 protocol encodes (one named tensor per
input field) and answers with one named tensor per output field — a real Open Inference
Protocol server validating the wire shape end-to-end, with no ML framework involved.
"""

from mlserver import MLModel
from mlserver.types import InferenceRequest, InferenceResponse, ResponseOutput


class DoublerRuntime(MLModel):
    async def load(self) -> bool:
        return True

    async def predict(self, payload: InferenceRequest) -> InferenceResponse:
        by_name = {tensor.name: list(tensor.data) for tensor in payload.inputs}
        xs = by_name["x"]
        tags = by_name["tag"]

        return InferenceResponse(
            model_name=self.name,
            outputs=[
                ResponseOutput(
                    name="y",
                    shape=[len(xs)],
                    datatype="FP64",
                    data=[float(value) * 2.0 for value in xs],
                ),
                ResponseOutput(
                    name="tag_len",
                    shape=[len(tags)],
                    datatype="INT64",
                    data=[len(_as_text(tag)) for tag in tags],
                ),
            ],
        )


def _as_text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)
