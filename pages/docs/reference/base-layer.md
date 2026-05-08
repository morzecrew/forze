# Base Layer

The base layer (`forze.base`) provides cross-cutting utilities used by every other layer. It has no dependencies on domain or application code and contains errors, codecs, primitives, serialization helpers, file I/O, and introspection tools.

## Errors

All domain- and application-level errors derive from `CoreError`. Infrastructure and presentation layers catch `CoreError` to produce consistent error responses.

    :::python
    from forze.base.errors import CoreError, NotFoundError, ValidationError

    raise NotFoundError("Project not found", code="project_not_found")

### Error hierarchy

| Class | Default code | Purpose |
|-------|-------------|---------|
| `CoreError` | `internal_error` | Base class for all application errors |
| `NotFoundError` | `not_found` | Requested resource does not exist |
| `ConflictError` | `conflict` | State conflict (e.g. duplicate key) |
| `ValidationError` | `validation_error` | Input validation failure |
| `InfrastructureError` | `infrastructure_error` | Database, cache, or external service failure |
| `ConcurrencyError` | `concurrency_error` | Optimistic concurrency violation |

Every error carries three fields:

| Field | Type | Purpose |
|-------|------|---------|
| `message` | `str` | Human-readable description |
| `code` | `str` | Machine-readable error code |
| `details` | `Mapping[str, Any] | None` | Optional structured context |

### Error handling

The `handled` decorator converts raw exceptions into `CoreError` instances using a custom handler. It works with sync functions, async functions, context managers, and iterators:

    :::python
    from forze.base.errors import CoreError, error_handler, handled


    @error_handler
    def pg_error_handler(e: Exception, op: str, **kwargs) -> CoreError:
        if isinstance(e, UniqueViolationError):
            return ConflictError(f"Duplicate in {op}")

        return InfrastructureError(f"DB error in {op}: {e}")


    class PostgresAdapter:
        @handled(pg_error_handler)
        async def create(self, data):
            ...

The `@error_handler` decorator applies built-in mappings (e.g. Pydantic `ValidationError` → `CoreError`) before your custom handler runs. Exceptions that are already `CoreError` pass through without conversion.

## Codecs

Immutable codec classes for serialization, encoding, and key/path construction.

### JsonCodec

JSON serializer using `orjson` with deterministic key ordering:

    :::python
    from forze.base import JsonCodec

    codec = JsonCodec()
    raw = codec.dumps({"b": 2, "a": 1})   # b'{"a":1,"b":2}'
    data = codec.loads(raw)                 # {"a": 1, "b": 2}
    text = codec.dumps_as_str(data)         # '{"a":1,"b":2}'

### TextCodec

String-to-bytes encoder/decoder:

    :::python
    from forze.base import TextCodec

    codec = TextCodec()
    raw = codec.dumps("hello")   # b'hello'
    text = codec.loads(raw)      # 'hello'

### AsciiB64Codec

Transparent base64 codec for non-ASCII strings. ASCII-only strings pass through unchanged:

    :::python
    from forze.base import AsciiB64Codec

    codec = AsciiB64Codec()
    codec.dumps("hello")     # 'hello' (ASCII, unchanged)
    codec.dumps("привет")    # 'b64://0L/RgNC40LLQtdGC' (base64-encoded)
    codec.loads("hello")     # 'hello'

### KeyCodec

Namespace-prefixed key builder for Redis-style key schemes:

    :::python
    from forze.base import KeyCodec

    keys = KeyCodec(namespace="app")
    keys.join("users", "123")            # 'app:users:123'
    keys.cond_join("users", None, "pk")  # 'app:users:pk'
    keys.split("app:users:123")          # ['app', 'users', '123']

### PathCodec

Slash-separated path joiner (no namespace):

    :::python
    from forze.base import PathCodec

    paths = PathCodec()
    paths.join("uploads", "2024", "file.png")  # 'uploads/2024/file.png'
    paths.cond_join("a", None, "b")            # 'a/b'

## Primitives

Shared types, value generators, and context-scoped utilities importable from `forze.base.primitives`.

### Type aliases

| Type | Underlying | Constraints |
|------|-----------|-------------|
| `String` | `Annotated[str, ...]` | Min 2, max 4096 chars, stripped, NFC-normalized |
| `LongString` | `Annotated[str, ...]` | Max 16384 chars, stripped, NFC-normalized |
| `JsonDict` | `dict[str, Any]` | JSON-compatible dictionary |

`String` and `LongString` are Pydantic-aware annotated types. They apply `normalize_string` as a before-validator, which handles Unicode normalization, invisible character stripping, and whitespace collapsing.

    :::python
    from forze.base.primitives import String, LongString, JsonDict

### UUID generation

Forze provides two UUID generators:

**`uuid7()`** generates time-ordered UUIDv7 identifiers with nanosecond precision. These are sortable by creation time and suitable as primary keys:

    :::python
    from forze.base.primitives import uuid7

    pk = uuid7()                                  # current time
    pk = uuid7(timestamp_ms=1700000000000)         # specific millisecond
    pk = uuid7(timestamp_ns=1700000000000000000)   # specific nanosecond

The UUID layout embeds a 48-bit millisecond timestamp in bits 0–47 and 20 bits of sub-millisecond nanoseconds in bits 52–71, followed by 54 random bits.

**`uuid4()`** generates random UUIDv4 identifiers, optionally deterministic from a value:

    :::python
    from forze.base.primitives import uuid4

    random_id = uuid4()             # random
    stable_id = uuid4({"key": 1})   # SHA-256-based, deterministic

Additional helpers in `forze.base.primitives.uuid`:

| Function | Purpose |
|----------|---------|
| `uuid7_to_datetime(uuid, tz?, high_precision?)` | Extract the timestamp from a UUIDv7 |
| `datetime_to_uuid7(dt)` | Generate a UUIDv7 from a datetime |

### Datetime

    :::python
    from forze.base.primitives import utcnow

    now = utcnow()  # timezone-aware UTC datetime

### String normalization

`normalize_string` cleans user-provided text for consistent storage:

    :::python
    from forze.base.primitives import normalize_string

    normalize_string("  hello   world  ")  # 'hello world'
    normalize_string(None)                  # None

The normalization pipeline:

1. Normalize Unicode to NFC
2. Strip invisible/control characters (preserving emoji joiners)
3. Replace NBSP with space, strip BOM and zero-width spaces
4. Collapse whitespace (except newlines) to single spaces
5. Trim leading/trailing spaces on each line

### RuntimeVar

Thread-safe, set-once global variable for application-wide values initialized during startup:

    :::python
    from forze.base.primitives import RuntimeVar

    app_ctx: RuntimeVar[AppContext] = RuntimeVar("app_ctx")

    # During startup
    app_ctx.set_once(context)

    # Anywhere later
    ctx = app_ctx.get()

    # For testing
    app_ctx.reset()

| Method | Behavior |
|--------|----------|
| `set_once(value)` | Set the value; raises `CoreError` if already set or value is `None` |
| `get()` | Return the value; raises `CoreError` if not yet set |
| `reset()` | Clear the value so it can be set again |

`RuntimeVar` is used internally by `ExecutionRuntime` to store the execution context per scope.

### ContextualBuffer

Context-scoped buffer for collecting objects during async task execution. Each async task or thread gets its own buffer via `ContextVar`:

    :::python
    from forze.base.primitives import ContextualBuffer

    buffer: ContextualBuffer[str] = ContextualBuffer()

    buffer.push(["a", "b"])
    buffer.peek()    # ['a', 'b']
    items = buffer.pop()   # ['a', 'b'] (buffer is now empty)

Use `scope()` for nested isolation:

    :::python
    buffer.push(["outer"])

    with buffer.scope():
        buffer.push(["inner"])
        buffer.peek()  # ['inner']

    buffer.peek()  # ['outer']

| Method | Behavior |
|--------|----------|
| `push(items)` | Append items to the buffer |
| `peek()` | Return current items without clearing |
| `pop()` | Return all items and clear the buffer |
| `clear()` | Clear the buffer |
| `scope()` | Context manager providing an isolated buffer; restores previous state on exit |

The outbox feature uses `ContextualBuffer` to collect events during a usecase and flush them after commit.

## Serialization

Helpers for dict diffing, merging, and Pydantic model utilities. Import from `forze.base.serialization`.

### Dict diff and merge

    :::python
    from forze.base.serialization import (
        apply_dict_patch,
        calculate_dict_difference,
    )

    before = {"a": 1, "b": {"c": 2}}
    after = {"a": 1, "b": {"c": 3, "d": 4}}

    patch = calculate_dict_difference(before, after)
    # {"b": {"c": 3, "d": 4}}

    result = apply_dict_patch(before, patch)
    # {"a": 1, "b": {"c": 3, "d": 4}}

| Function | Purpose |
|----------|---------|
| `calculate_dict_difference(...)` | Compute a JSON-merge-style patch<br>from `before` to `after` |
| `apply_dict_patch(...)` | Apply a merge patch to a dict |
| `split_touches_from_merge_patch(...)` | Separate a patch into scalar changes and<br>container replacements |
| `has_hybrid_patch_conflict(...)` | Check if two patches conflict |

These are used internally by `Document.update()` to compute minimal diffs and by `validate_historical_consistency()` to detect concurrent update conflicts.

### Pydantic helpers

    :::python
    from forze.base.serialization import (
        pydantic_validate,
        pydantic_dump,
        pydantic_field_names,
        pydantic_model_hash,
    )

| Function | Purpose |
|----------|---------|
| `pydantic_validate(...)` | Validate raw data into a model instance |
| `pydantic_dump(...)` | Dump a model to a dict with fine-grained exclusion |
| `pydantic_field_names(...)` | Return the set of field names on a model class |
| `pydantic_model_hash(...)` | Compute a stable SHA-256 hash of a serialized model |

The `exclude` parameter for `pydantic_dump` accepts a `TypedDict` with optional keys: `unset`, `none`, `defaults`, `computed_fields` (all `bool`).

## File I/O

Simple helpers for reading YAML and text files:

    :::python
    from forze.base.files import read_yaml, read_text, iter_file

    config = read_yaml("config.yml")    # dict (empty dict for empty files)
    content = read_text("template.sql")  # str

    for chunk in iter_file(raw_bytes):
        process(chunk)

`iter_file` yields 32 KB chunks from raw bytes or a file-like object.

## Introspection

Utilities for extracting names and modules from callables and classes. Used internally for diagnostics and error messages:

    :::python
    from forze.base import (
        get_callable_name,
        get_callable_module,
        get_class_name,
        get_class_module,
    )

| Function | Returns |
|----------|---------|
| `get_callable_name(fn)` | Qualified name of a function or partial |
| `get_callable_module(fn)` | Module name of a function |
| `get_class_name(cls)` | Qualified name of a class |
| `get_class_module(cls)` | Module name of a class |
