---
title: Quickstart
icon: lucide/zap
---

## What you will build

In this guide you will create:

- User domain models
- Document specification
- REST API
- In-memory storage backend

The final service exposes:

```
POST /users
GET /users/{id}
GET /users
DELETE /users/{id}
```

## Install packages

Install the core package and the FastAPI integration:

```bash
uv add 'forze[fastapi]'
```

## Define domain models

```python
from forze.domain.models import Document, CreateDocumentCmd

class User(Document):
    name: str
    email: str

class CreateUserCmd(CreateDocumentCmd):
    name: str
    email: str
```

## Define document specification

```python
from forze.application.contracts.document import DocumentSpec

user_spec = DocumentSpec(
    name="users",
    read=User,
    write={
        "domain": User,
        "create_cmd": CreateUserCmd,
    },
)
```
