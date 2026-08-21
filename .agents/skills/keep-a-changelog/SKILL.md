---
name: keep-a-changelog
description: Use when asked to update CHANGELOG.md, add an entry, write release notes, or cut a version section; or when user-facing changes land that it should record. Not when the file is auto-generated in another format, and not for performing the release itself.
roles: [author]
gate: validate-changelog
---

# Keep a Changelog Assistant

Changelogs are for humans. A good one lets a reader answer "what does upgrading
do to me?" without reading a diff. The agent maintains `## [Unreleased]` and, when
explicitly asked, prepares version sections; the human decides when to cut, tag
and publish.

Format, categories, breaking changes, reverts, yanked releases, entry style, this
repository's house rules, and both workflows are in
[references/spec-and-workflows.md](references/spec-and-workflows.md) — load it
before editing the file. `scripts/validate_changelog.py` enforces the mechanical
half of it:

```bash
python3 skills/keep-a-changelog/scripts/validate_changelog.py CHANGELOG.md
python3 skills/keep-a-changelog/scripts/validate_changelog.py --house-rules CHANGELOG.md
```

It never edits. What it cannot judge is the whole of what follows.

## Which changes earn an entry

Only changes meaningful to consumers. The test: **does this affect how users
install, import, use, or should trust the software?** If not, leave it out — even
when it was the largest thing in the release.

Typically in: public API changes, new or changed behavior, packaging and
installation changes, deprecations, security fixes.

Typically out: test changes, CI and workflow updates, internal tooling, docs-only
changes, formatting and lint-only changes, refactors with no observable impact,
trivial renames.

Read the product-code boundary from the repository's own layout — wherever the
shipped code lives (`src/`, `cmd/` plus `internal/`, `lib/`, a package directory)
— and treat user-relevant as changes inside it: public APIs and commands, domain
primitives, contracts and schemas, behaviors, plus packaging. Never carry one
layout's boundary into another repository.

Two judgements the validator will never make for you:

- **Which category a change dominantly belongs to** when it fits two. A rewrite that also fixes a bug is `Changed` if the rewrite is the story and `Fixed` if the fix is; putting it in both makes the release look twice as large as it was.
- **Whether an entry is true and outcome-shaped.** A well-formed entry that describes the implementation instead of the effect passes every check and still fails the reader.

Every commit marked breaking (`💥` / `!` / a `BREAKING CHANGE:` footer, see
`gitmoji-conventional`) must produce an entry that names the break, and implies
MAJOR for the next release.

## Related skills

- `gitmoji-conventional` — commit messages and PR titles; its breaking-change markers are the signal that an entry must call out a break. Absent it, read the same signal off the diff: a removed or altered public symbol is a break whatever the commit subject says.
