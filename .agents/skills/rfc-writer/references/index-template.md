# INDEX.md template

Copy the skeleton below into `rfcs/INDEX.md` (or `rfc/INDEX.md`) when initializing an RFC directory. Replace `<placeholders>`; delete the gitignore sentence if the directory is committed.

---

```markdown
# RFCs

Design proposals for <project>. <If applicable: **This directory is
gitignored** (`.gitignore` → `rfcs/`) — these are local working notes, not
pushed to the repo.>

## Allocating a number

The next free number is **0001**. Before creating an RFC, glance at the table
below (or `ls` this directory) and take the next unused integer — numbers
collide when minted in parallel. Update this table in the same change.

Filename: `NNNN-kebab-title.md`. Keep the `# RFC NNNN — Title` H1 and the
number in the filename in sync.

## Index

| # | Title | Status | One-line |
|---|---|---|---|

## Status legend

- 📝 **Draft** — proposed, not started
- 🚧 **In progress** — partially shipped
- ✅ **Complete** — fully shipped
- ❌ **Rejected / withdrawn**
```

---

## Notes

- **Row format:** `| [0001](0001-kebab-title.md) | Title | 📝 Draft | One-line summary |` — number linked to the file, newest rows appended at the bottom.
- **The one-liner is a summary, not a teaser.** Compress what the RFC decides — key mechanism, key exclusions, shipped-state notes — densely enough that scanning the index substitutes for opening most files.
- **Keep "next free number" honest.** Every RFC creation bumps it in the same change; when syncing a stale index, recompute it from the files actually present.
