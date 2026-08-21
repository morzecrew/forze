---
name: gitmoji-conventional
description: Use whenever generating or suggesting a git commit message or Pull Request title, even when the user mentions neither gitmoji nor Conventional Commits. Not when the repository enforces a conflicting convention, and not when the user dictates the message verbatim.
roles: [implement, author]
gate: check-commit-msg
---

# Gitmoji + Conventional Commits

`<gitmoji> <type>[optional scope][!]: <description>`

```text
✨ feat(api): add OAuth login support
🐛 fix(auth): handle expired refresh tokens
♻️ refactor(cache): extract eviction policy
```

The shape, the breaking-change signals, revert form, footers, SemVer mapping and
PR-title rules are in [references/commit-format.md](references/commit-format.md).
`check_commit_msg.py` enforces the commit-message half of that; **PR titles are
unchecked** — it reads message files, literal messages and commit ranges, and a
title never passes through it. Load the reference when writing either. What
follows is the part no check can decide for you.

## Pick the dominant type

1. Identify the dominant change.
2. Take its gitmoji from [references/gitmoji-mapping.md](references/gitmoji-mapping.md) — load it when choosing.
3. Use the type mapped to that gitmoji. Never invent gitmoji or types.

Common pairs: ✨ feat, 🐛 fix, ♻️ refactor, ⚡️ perf, 📝 docs, ✅ test, 👷 ci, 📦️ build, 🔧 chore, ⏪️ revert. Breaking is not a type: 💥 rides the underlying type with `!`, so release grouping still reads the `feat`/`fix` underneath.

**One commit, one semantic story.** When a change spans types, choose the one that
would headline the release notes; the rest is body detail. Tie-breaker, by user
impact:

`fix > feat > perf > refactor > build > docs > test > chore`

Incidental edits don't count — a feature commit that also touches its tests is
`✨ feat`, because the tests exist for the feature. Two genuinely independent
changes are two commits; say so rather than blending the subject.

The type is what release tooling reads, so mislabeling has a price: a feature
filed as `chore` never reaches the release notes, and a refactor filed as `feat`
inflates the version. Choose for what the change does, not how it felt to write.

## Write the body for `git blame`, not for the reviewer

**12 non-blank lines is the target; 20 is a hard failure.** Footers and fenced
blocks are exempt, so evidence that must travel with the commit — a stack trace,
a failing config, a benchmark table — goes in a fence.

Belongs in a body: the motivation the diff cannot show (the constraint, the bug's
mechanism, the rejected alternative), a consequence a reader would not predict,
and what the change deliberately does *not* do where the omission looks like an
oversight.

Does not:

- **Session narrative** — "then I ran the tests, which found X". The tell is that the body describes the author's activity rather than the code's new state. That belongs in the PR description.
- **Evidence dumps** — test counts, coverage, mutation tallies. Current in a PR, fossils in `git log`. Exception: a number that *is* the reason ("p99 was 240ms against a 150ms bar").
- **Restating the subject** in longer words, or listing files the diff already names.
- **Process commentary** — which skill was followed, which pass found it, how many rounds it took.

If the explanation needs more room than that, it is not a commit body: put it in
an RFC, an issue, or the PR description and link to it in one line. And a body
that needs headings or a topic per paragraph is a batching smell — six
paragraphs usually means six commits, which is the dominant-type rule above
reporting a real finding.

## Checking

```bash
python3 skills/gitmoji-conventional/scripts/check_commit_msg.py --message "✨ feat(api): add OAuth login"
python3 skills/gitmoji-conventional/scripts/check_commit_msg.py --range main..HEAD   # audit a branch
python3 skills/gitmoji-conventional/scripts/check_commit_msg.py --file "$1"          # commit-msg hook
```

The body cap lives in the script rather than in prose here for a reason: it was
added *because* the prose rule was read, agreed with, and ignored in the same
session (`drift-to-gate`).

Output only the commit message or PR title — no explanations, no alternatives
unless asked.

## Related skills

- `keep-a-changelog` — turning the same changes into user-facing CHANGELOG.md entries; breaking commits here require explicit break entries there. Absent it, still write the entry: a breaking change with no changelog line is the failure, not the missing skill.
