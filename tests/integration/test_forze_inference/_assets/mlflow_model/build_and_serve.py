"""Build a tiny pyfunc model inside the mlflow container, then serve it.

Runs as the container command: saves a pure-python pyfunc (no ML framework) and execs
``mlflow models serve`` so the /invocations scoring protocol — including the
``instances`` records parsing — is the real thing.
"""

import os

import mlflow.pyfunc


def _records(model_input):
    # The scoring server hands `instances` data in different shapes depending on
    # mlflow version and signature presence: DataFrame, list of record dicts,
    # column-oriented dict, or a single record dict. Normalize to record dicts.
    if hasattr(model_input, "iterrows"):
        return [row for _, row in model_input.iterrows()]

    if isinstance(model_input, dict):
        first = next(iter(model_input.values()))

        # Column values arrive as lists or numpy arrays; a scalar dict is one record.
        if isinstance(first, str) or not hasattr(first, "__len__"):
            return [model_input]

        keys = list(model_input)
        count = len(first)
        return [{key: model_input[key][i] for key in keys} for i in range(count)]

    return list(model_input)


class Doubler(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input, params=None):
        return [
            {"y": float(record["x"]) * 2.0, "tag_len": len(str(record["tag"]))}
            for record in _records(model_input)
        ]


mlflow.pyfunc.save_model(path="/tmp/model", python_model=Doubler())

os.execvp(
    "mlflow",
    [
        "mlflow",
        "models",
        "serve",
        "-m",
        "/tmp/model",
        "--host",
        "0.0.0.0",
        "--port",
        "5000",
        "--env-manager",
        "local",
    ],
)
