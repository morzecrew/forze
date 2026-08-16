"""Integrity gates for the published skills corpus under ``skills/``.

The corpus is documentation that ships into other people's repositories and is read by
agents that write code from it, which gives it a property ordinary docs do not have: it
is executable in effect. An agent reading ``from forze_kits.aggregates.document import
DocumentFacade`` will emit that line. When an export is renamed the skill does not
degrade into vague-but-harmless prose — it instructs a downstream agent to write a
broken import, in a repository nobody here can see and which will never file a bug.

Until this package existed, none of the repository's gates read it: lint, import-linter,
dead-code and dependency checks cover ``src/`` and ``tests/``, and the docs build covers
``pages/``. The corpus sat outside CI entirely, maintained by review alone.

This package lives outside ``skills/`` so the installer never copies it into a
consumer's repository.

Usage (from the repository root)::

    python -m tools.skills_check           # the offline gates; `just skills-check`
    python -m tools.skills_check --links   # published-link liveness; scheduled only
"""

from __future__ import annotations

from .checks import (
    Result,
    check_census,
    check_imports,
    check_structure,
    check_syntax,
    load_extras,
    load_shipped_packages,
)
from .corpus import CodeBlock, Corpus, Document, Link, load_corpus
from .links import LinkOutcome, LinkPolicy, check_liveness, collect_published_urls
from .manifest import Manifest, Unit, default_manifest_path, load_manifest

__all__ = [
    "CodeBlock",
    "Corpus",
    "Document",
    "Link",
    "LinkOutcome",
    "LinkPolicy",
    "Manifest",
    "Result",
    "Unit",
    "check_census",
    "check_imports",
    "check_liveness",
    "check_structure",
    "check_syntax",
    "collect_published_urls",
    "default_manifest_path",
    "load_corpus",
    "load_extras",
    "load_manifest",
    "load_shipped_packages",
]
