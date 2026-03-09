## 2024-03-09 - Domain and Application Coupling with Infrastructure/Utils

Learning:
The architecture emphasizes strict separation of concerns, meaning the domain and application layers should ideally not depend on utility modules that might represent infrastructure or generic details (`forze.utils`). `src/forze/application/execution/plan.py` imports `get_callable_module` and `get_callable_name` from `forze.utils.debug`. This couples the execution plan (application layer) to the utils module. While seemingly minor, this blurs the architectural boundary.

Action:
Extract the introspection functions (`get_callable_module`, `get_callable_name`) directly into the application layer, or possibly the base primitives layer which is acceptable for cross-cutting core primitives. We will move the functions in `forze.utils.debug` to `forze.base.primitives.runtime` or a similar appropriate module, or create `forze.base.introspection` to remove the application layer's dependency on `forze.utils`.

## 2024-03-09 - Codecs Module Extraction

Learning:
The `forze.utils.codecs` module contains encoding, serialization, and path building logic (`JsonCodec`, `KeyCodec`, `PathCodec`, `TextCodec`, `AsciiB64Codec`). These are serialization and encoding mechanisms. They belong in `forze.base.serialization` or a similar core package, alongside `JsonDict` and serialization primitives. The `forze.utils` module is generally discouraged as a structural bin for functionality, as its boundaries are ill-defined.

Action:
Move the contents of `forze.utils.codecs` to `forze.base.serialization.codecs` and update all imports from adapters (`forze_redis`, `forze_s3`) and tests.
