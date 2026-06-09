---
title: Quickstart
icon: lucide/zap
---

## What you will build

A minimal REST service for a `User` aggregate:

| Method | Path | Action |
|--------|------|--------|
| `POST` | `/users` | Create a user |
| `GET` | `/users/{id}` | Get one user |
| `GET` | `/users` | List users |
| `DELETE` | `/users/{id}` | Delete a user |

Storage is **in-memory** - no Docker, no database migrations.

## Step 1: Create the project

```bash
uv init forze-quickstart
cd forze-quickstart
uv add 'forze[fastapi]'
```

Create `main.py` in the project root. The next steps add code to this file.

## Step 2: Define domain models

Every document aggregate needs a **domain model**, a **create command**, and a **read model**. `Document` gives you `id`, `rev`, and timestamps.

```python
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from pydantic import computed_field


class User(Document):
    name: str
    email: str | None = None


class CreateUserCmd(CreateDocumentCmd):
    name: str
    email: str | None = None


class ReadUser(ReadDocument):
    name: str
    email: str | None = None

    @computed_field
    @property
    def email_provided(self) -> bool:
        return self.email is not None
```

??? question "Why three types?"

    - **Domain model** - business entity with behavior and invariants
    - **Create command** - frozen input for `POST`
    - **Read model** - frozen projection for `GET` responses

    Update commands come later, this quickstart skips them on purpose.

## Step 3: Declare a document specification

The spec is the logical name adapters and routes share. Here it is `#!python "users"`.

```python
from forze.application.contracts.document import DocumentSpec

user_spec = DocumentSpec(
    name="users",
    read=ReadUser,
    write={
        "domain": User,
        "create_cmd": CreateUserCmd,
    },
)
```

## Step 4: Declare operations registry

```python
from forze_kits.aggregates.document import build_document_registry, DocumentDTOs

reg = build_document_registry(
    user_spec,
    DocumentDTOs(read=ReadUser, create=CreateUserCmd)
)

frozen_reg = reg.freeze()
```

## Step 5: Wire the runtime

`MockDepsModule` registers in-memory adapters for every contract. `ExecutionRuntime` builds an `ExecutionContext` during startup and stores it in a `RuntimeVar` for per-request access. Typically the best way to store the execution runtime is another `RuntimeVar` paired with a function to access the context.

```python
from forze.application.execution import DepsRegistry, ExecutionRuntime, ExecutionContext
from forze.base.primitives import RuntimeVar
from forze_mock import MockDepsModule


_rt = RuntimeVar[ExecutionRuntime]("rt")


def get_context() -> ExecutionContext:
    return _rt.get().get_context()


def construct_runtime() -> ExecutionRuntime:
    deps = DepsRegistry.from_modules(MockDepsModule()).freeze()
    crt = ExecutionRuntime(deps=deps)

    _rt.set_once(crt)

    return crt

```

At runtime, a request resolves document ports from that context.

*TODO: add diagram*

## Step 6: Attach FastAPI routes

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends


@asynccontextmanager
async def lifespan(app: FastAPI):
    crt = construct_runtime()

    async with crt.scope():
        yield


app = FastAPI(title="Users API", lifespan=lifespan)
```

!!! tip "Lifespan is required"

    Accessing the execution context outside the runtime scope is not possible.

## Step 7: Run the service

```bash
uv run uvicorn main:app --reload
```

Open [http://localhost:8000/docs](http://localhost:8000/docs) for the interactive API explorer.

**Try it:**

```bash
# Create
curl -s -X POST http://127.0.0.1:8000/users \
  -H 'Content-Type: application/json' \
  -d '{"name": "Ada", "email": "ada@example.com"}'

# List (use the id from the create response for get/delete)
curl -s http://127.0.0.1:8000/users

# Get a particular user
curl -s http://127.0.0.1:8000/users/{id}

# Delete a user
curl -s -X DELETE http://127.0.0.1:8000/users/{id}
```

## What you just did

...

## Complete example

??? example "Full code"

    ```python
    from contextlib import asynccontextmanager
    ```
