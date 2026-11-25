"""The framework document pages print the render of their tables (RFC 0080 row 37).

Applications copy the identity, audit and progress tables from their pages. Each untitled SQL
block must be what ``forze schema sql --specs`` prints for those specs, which is also what the
integration tests create and validate, so a page cannot drift from the models. A block fenced
with a title (```` ```sql title="For the staleness query" ````) holds the application's own
statements, such as an index for its access patterns or a migration, and is not a render.
"""

import re
from pathlib import Path
from typing import Any, Final

import pytest

from forze.application.contracts.document import DocumentSpec
from forze_identity.authn.application.specs import (
    api_key_account_spec,
    password_account_spec,
    password_reset_spec,
    session_spec,
)
from forze_identity.authz.application.specs import (
    policy_principal_spec,
    principal_role_binding_spec,
)
from forze_kits.integrations.audit import audit_record_spec
from forze_kits.integrations.progress import job_record_spec
from forze_postgres.execution.deps.document_binding import (
    postgres_document_schema_spec_for_binding,
)
from forze_postgres.kernel.catalog.owned import BoundOwnedRelation, document_shape
from forze_postgres.kernel.catalog.owned.render import install_blocks
from tests.support.framework_documents import document_config

# ----------------------- #

_PAGES: Final[Path] = Path(__file__).resolve().parents[2] / "pages" / "docs"

_SQL_BLOCK: Final[re.Pattern[str]] = re.compile(r"```sql\n(.*?)```", re.DOTALL)
_TITLED_BLOCK: Final[re.Pattern[str]] = re.compile(r'```sql title="[^"]+"\n(.*?)```', re.DOTALL)


def _words(text: str) -> str:
    return " ".join(text.split())


def _statements(text: str) -> list[str]:
    """Every statement in *text*'s untitled SQL blocks, comments dropped, whitespace folded."""

    body = re.sub(r"--[^\n]*", "", "\n".join(_SQL_BLOCK.findall(text)))

    return [_words(statement) for statement in body.split(";") if statement.strip()]


def _rendered(*specs: DocumentSpec[Any, Any, Any, Any], tenant_aware: bool) -> list[str]:
    out: list[str] = []

    for spec in specs:
        name = str(spec.name)
        config = document_config("app", name, tenant_aware=tenant_aware)
        shape = document_shape(
            postgres_document_schema_spec_for_binding(name, spec=spec, config=config)
        )
        relation = BoundOwnedRelation(route=name, schema="app", name=name, shape=shape)

        for block in install_blocks(relation):
            assert block.statement is not None
            out.append(_words(block.statement.removesuffix(";")))

    return out


def _page(path: str) -> str:
    return (_PAGES / path).read_text(encoding="utf-8")


# ....................... #


def test_the_identity_recipe_prints_the_render_of_its_tables() -> None:
    assert _statements(_page("recipes/provision-identity-postgres.md")) == _rendered(
        password_account_spec,
        api_key_account_spec,
        session_spec,
        password_reset_spec,
        policy_principal_spec,
        principal_role_binding_spec,
        tenant_aware=False,
    )


def test_the_audit_page_prints_the_render_of_its_table() -> None:
    assert _statements(_page("identity-tenancy-enc/audit.md")) == _rendered(
        audit_record_spec(), tenant_aware=True
    )


def test_the_progress_page_prints_the_render_of_its_table() -> None:
    assert _statements(_page("running-in-prod/operation-progress.md")) == _rendered(
        job_record_spec(), tenant_aware=True
    )


@pytest.mark.parametrize(
    "path",
    [
        "recipes/provision-identity-postgres.md",
        "identity-tenancy-enc/audit.md",
        "running-in-prod/operation-progress.md",
    ],
)
def test_no_titled_block_hides_a_render(path: str) -> None:
    # A render behind a title would drop out of the comparison unseen.
    assert not any("CREATE TABLE" in block for block in _TITLED_BLOCK.findall(_page(path)))


def test_the_recipe_names_each_table_for_the_spec_its_binding_binds() -> None:
    # The binding comprehension names each table for its spec, so a table named for none
    # (the recipe's old authn_sessions) is never bound.
    tables = re.findall(
        r"CREATE TABLE app\.(\w+) \(", _page("recipes/provision-identity-postgres.md")
    )

    assert "authn_token_sessions" in tables
    assert "authn_sessions" not in tables
