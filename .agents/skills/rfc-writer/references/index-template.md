# INDEX.md template

Copy the skeleton below into `rfcs/INDEX.md` (or `rfc/INDEX.md`) when initializing an RFC directory. Replace `<placeholders>`; delete the gitignore sentence if the directory is committed.

---

```markdown
# RFCs

Design proposals for <project>. <If applicable: **This directory is
gitignored** (`.gitignore` → `rfcs/`) — these are local working notes, not
pushed to the repo.>

<Once EXECUTION-LOG.md exists — written by the first completed execution
unit, not by this template:
[EXECUTION-LOG.md](EXECUTION-LOG.md) holds what execution found wherever the
code and these designs disagreed, with the decision rows it puts forward in
response. Entries are only ever added. It is not a design, so it has no number
and no row in the table below.>

## Allocating a number

The next free number is **0001**. Before creating an RFC, glance at the table
below (or `ls` this directory) and take the next unused integer — numbers
collide when minted in parallel. Update this table in the same change.

Filename: `NNNN-kebab-title.md`. Keep the `# RFC NNNN — Title` H1 and the
number in the filename in sync.

## Index

| # | Title | Status | One-line routing description |
|---|---|---|---|

## Status legend

- 📝 **Draft** — proposed, not started
- 🚧 **In progress** — partially shipped
- ✅ **Complete** — fully shipped
- ❌ **Rejected / withdrawn**
```

---

## Notes

- **Row format:** `| [0001](0001-kebab-title.md) | Title | 📝 Draft | One-line routing description |` — number linked to the file, newest rows appended at the bottom.
- **The one-liner routes, it does not summarise.** One sentence naming the problem and the shape of the answer — enough to tell this design apart from the others, and no more. Aim for 200 characters, ceiling 300. What the RFC decides, how it works and what it excluded belong in the RFC.
- **Never record history here.** No shipped dates, no phase progress, no defects found, no amendments. The Status column carries state; the RFC carries its own story. An entry that grows each time work lands turns the index into a changelog that every lookup has to read.
- **Keep "next free number" honest.** Every RFC creation bumps it in the same change; when syncing a stale index, recompute it from the files actually present.
- **`EXECUTION-LOG.md` never gets a table row.** It is not a design and has no status; it is linked in prose above the table, and only once it exists. `rfc_index.py` ignores it — only `NNNN-*.md` files are RFCs.
