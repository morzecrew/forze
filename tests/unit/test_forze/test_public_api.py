"""The curated top-level front doors (`forze` / `forze_kits`).

Locks the lazy PEP 562 re-export surface: every advertised name resolves, `__all__` stays
in sync with the resolver map, unknown names raise cleanly, and `import forze` stays lazy
(no execution kernel or integration package pulled in just by importing the namespace).
"""

from __future__ import annotations

import subprocess
import sys

import pytest

import forze
import forze_kits

# ----------------------- #


@pytest.mark.parametrize("module", [forze, forze_kits], ids=["forze", "forze_kits"])
def test_every_exported_name_resolves(module) -> None:
    for name in module.__all__:
        assert getattr(module, name) is not None


@pytest.mark.parametrize("module", [forze, forze_kits], ids=["forze", "forze_kits"])
def test_all_matches_resolver_map(module) -> None:
    # `__all__` and the `_EXPORTS` map must not drift apart.
    assert sorted(module.__all__) == sorted(module._EXPORTS)


@pytest.mark.parametrize("module", [forze, forze_kits], ids=["forze", "forze_kits"])
def test_all_appears_in_dir(module) -> None:
    listed = set(dir(module))
    assert set(module.__all__) <= listed


@pytest.mark.parametrize("module", [forze, forze_kits], ids=["forze", "forze_kits"])
def test_unknown_attribute_raises(module) -> None:
    with pytest.raises(AttributeError, match="has no attribute"):
        _ = module.DefinitelyNotAName


def test_import_forze_is_lazy_and_pulls_no_heavy_deps() -> None:
    # A fresh interpreter: importing the namespace must not eagerly load the execution
    # kernel or any integration package — the cost is deferred to first symbol access.
    code = (
        "import sys, forze\n"
        "kernel = 'forze.application.execution' in sys.modules\n"
        "integrations = [m for m in sys.modules if m.startswith('forze_') "
        "and m not in ('forze_kits',)]\n"
        "assert not kernel, 'execution kernel imported eagerly'\n"
        "assert not integrations, f'integration pkgs imported: {integrations}'\n"
        "forze.DocumentSpec  # touching a symbol resolves it\n"
        "assert 'forze.application.contracts.document' in sys.modules\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
