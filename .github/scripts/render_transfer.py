"""Render the corpus bug-transfer artifact: per-engine JSON records -> one markdown document.

Invoked by ``just dst-transfer`` after the integration transfer differential has written its
``transfer_<engine>.json`` records (with ``FORZE_FIDELITY_OUT`` set). All rendering logic lives
in ``forze_dst.conformance.transfer`` — this is a thin argv wrapper, mirroring
``render_fidelity.py``; the registry join (families, tiers, the not-transferable reasons) reads
the live corpus, so the document can't drift from the reviewed data it presents.

Usage: ``PYTHONPATH=. python render_transfer.py <records.json> [...] --out <transfer.md>``
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from forze_dst.conformance import render_transfer_markdown


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("records", nargs="+", type=Path, help="transfer_<engine>.json files")
    parser.add_argument("--out", required=True, type=Path, help="markdown output path")
    args = parser.parse_args(argv)

    from tests.support.misuse import CONTROLS, CORPUS

    flat = [record for path in args.records for record in json.loads(path.read_text())]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render_transfer_markdown(flat, corpus=CORPUS, controls=CONTROLS))

    print(f"rendered {args.out} from {len(flat)} transfer record(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
