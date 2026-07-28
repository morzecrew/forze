"""Render the DST fidelity matrix artifact: per-backend JSON payloads -> one markdown document.

Invoked by ``just dst-fidelity`` after the integration matrix tests have written their
``fidelity_<engine>.json`` payloads (with ``FORZE_FIDELITY_OUT`` set). All rendering logic lives in
``forze_dst.conformance.report`` — this is a thin argv wrapper, mirroring ``coverage_floors.py``.

Usage: ``python render_fidelity.py <payload.json> [<payload.json> ...] --out <fidelity.md>``
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from forze_dst.conformance import render_markdown


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("payloads", nargs="+", type=Path, help="fidelity_<engine>.json files")
    parser.add_argument("--out", required=True, type=Path, help="markdown output path")
    args = parser.parse_args(argv)

    payloads = [json.loads(path.read_text()) for path in args.payloads]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render_markdown(payloads))

    print(f"rendered {args.out} from {len(payloads)} backend payload(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
