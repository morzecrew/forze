"""Seed a fixed world and print a canonical snapshot — run in a subprocess.

Lives in its own module because the property under test is *cross-process*: a value that
reproduces inside one interpreter can still differ in the next one (hash seed, object
identity, `default=str` on a live object). Anything the parent process could share would
weaken the check, so this imports nothing from the test module.
"""

from __future__ import annotations

import asyncio
import json
import sys
from uuid import UUID

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze.testing import context_from_modules
from forze_mock import MockDepsModule
from forze_mock.seeding import SeedPlan, apply_seed, spec_seed

# ----------------------- #


class Project(Document):
    name: str = ""


class ProjectCreate(CreateDocumentCmd):
    name: str = ""


class ProjectUpdate(BaseDTO):
    name: str | None = None


class ProjectRead(ReadDocument):
    name: str = ""


class Task(Document):
    title: str = ""
    project_id: UUID | None = None


class TaskCreate(CreateDocumentCmd):
    title: str = ""
    project_id: UUID | None = None


class TaskUpdate(BaseDTO):
    title: str | None = None


class TaskRead(ReadDocument):
    title: str = ""
    project_id: UUID | None = None


PROJECTS = DocumentSpec(
    name="projects",
    read=ProjectRead,
    write=DocumentWriteTypes(domain=Project, create_cmd=ProjectCreate, update_cmd=ProjectUpdate),
)

TASKS = DocumentSpec(
    name="tasks",
    read=TaskRead,
    write=DocumentWriteTypes(domain=Task, create_cmd=TaskCreate, update_cmd=TaskUpdate),
)


async def main() -> None:
    ctx = context_from_modules(MockDepsModule())
    plan = SeedPlan(
        specs=(spec_seed(TASKS, count=5), spec_seed(PROJECTS, count=3)),
        rng_seed=11,
    )
    await apply_seed(ctx, plan)

    snapshot = {
        spec.name: [
            row.model_dump(mode="json") for row in (await ctx.doc.query(spec).find_many()).hits
        ]
        for spec in (PROJECTS, TASKS)
    }

    sys.stdout.write(json.dumps(snapshot, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
