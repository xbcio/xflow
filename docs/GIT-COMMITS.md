# Git Commit Conventions

## Format

```
<type>(<scope>): <what changed>
```

Single-line subject. No body — see [Why no body](#why-no-body) if that seems wrong.

## Type

One of: `feat` | `fix` | `refactor` | `test` | `docs` | `chore` | `style` | `ci`

## Scope

A package or module name, not a file name and not a topic. Use the top-level
or sub-package name the change lives in: `control`, `runner`, `protocol`,
`wasm`, `supply`, `trigger`, `script`, `node`, `graph`, `types`, `store`,
`sdk`, `server`, `test`, `api`, `cli`.

A change to repo-root or tooling files that belong to no package — `.gitignore`,
`Makefile`, CI workflows — takes no scope at all: `chore: ...`, `ci: ...`.
Do not invent a scope from the file name (`fix(gitignore)` names a file, not a
package).

If a change touches several unrelated packages, either pick the scope of the
most central one, or split into multiple commits (see
[One commit, one change](#one-commit-one-change)).

## Subject

Start with a verb describing the concrete change: "add X", "fix Y", "remove Z".
Not a vague label like "update stuff" or "misc fixes" — a reader should know
what changed without opening the diff.

**Hard limit: 70 characters**, counting the `type(scope): ` prefix. Treat it as
a title, not a sentence you can keep extending. If it does not fit, the commit
is usually too big — shorten the change, not the words.

Write it in English, ASCII only. The hook rejects a non-ASCII subject.

## One commit, one change

A commit does one thing. If a session ends up bundling unrelated fixes —
say, a port-discovery fix, a gofmt pass, and a doc update — split them into
separate commits instead of folding them into one with a bencyclopedic
subject or a bullet-list body.

**The " and " test.** If the subject needs " and " or a comma to cover what
you did, that is two commits. `feat(trigger): add schema validation and inject
supplies in the SDK path` is a schema commit and a supply commit that were
never separated.

An oversized subject and the urge to write a body are the same symptom: the
commit is doing more than one thing. Split it and both problems disappear.

## Why no body

Reasoning and background belong in the PR description or the review
discussion, not the commit message. A commit with a multi-paragraph body is
usually a sign the change should have been split per
[One commit, one change](#one-commit-one-change).

This means **no body at all** — not a shortened body, not a single line of
context, not a bullet list of what the diff touched. If a design decision
genuinely needs to be recorded, it goes in a `.claude/` doc or `docs/design/`,
where it stays readable and editable; a commit message is neither.

History was rewritten to this convention, so no commit in `git log` has a body.
Nothing was lost that belonged there: reasoning that is still worth keeping
lives in `docs/design/` or `.claude/`, where it can be edited as it ages.

## Examples

Real commits from this repo's history — all single-line, all body-free:

```
feat(runner): report activation failures through a callback          (59)
docs(supply): record that a declined activation now self-heals       (62)
feat(control): per-key jittered backoff for activation redispatch    (65)
feat(control): redispatch an activation a runner declined            (57)
test(supply): prove a declined activation self-heals without a restart (70)
```

## Counter-examples

All from this repo's pre-rewrite history. The rewrite replaced them, so they
are no longer in `git log`. Each violates a rule above:

```
fix(runner): make the panic-recovery ack test actually wait for the panic, document fire-and-forget ack shutdown
```
112 characters, and the comma joins two changes: a test fix and a doc change.
Two commits.

```
feat(supply): add AES-256-GCM content encryption with key delivery on registration
```
81 characters over 661 lines — encryption and key delivery are separable.

```
fix(trigger): validate kafka messages in every mode and make drops visible
```
Fits the pattern, but carried a 2100-character body across eight paragraphs and
1468 lines spanning validation, a drop policy, a DLQ writer, metrics, and a new
integration test. Five commits wearing one subject.
