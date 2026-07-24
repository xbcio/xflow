# Git Commit Conventions

## Format

```
<type>(<scope>): <what changed>
```

Single-line subject. No body — see [Why no body](#why-no-body) if that seems wrong.

## Type

One of: `feat` | `fix` | `refactor` | `test` | `docs` | `chore` | `style` | `ci`

## Scope

A package or module name, not a file name. Use the top-level or sub-package
name the change lives in: `asynq`, `control`, `runner`, `test`, `sdk`, `xflow`,
`node`, `graph`, `types`, `protocol`, `cmd/server`.

If a change touches several unrelated packages, either pick the scope of the
most central one, or split into multiple commits (see
[One commit, one change](#one-commit-one-change)).

## Subject

Start with a verb describing the concrete change: "add X", "fix Y", "remove Z".
Not a vague label like "update stuff" or "misc fixes" — a reader should know
what changed without opening the diff.

Keep it around 70 characters. Treat it as a title, not a sentence you can
keep extending.

## One commit, one change

A commit does one thing. If a session ends up bundling unrelated fixes —
say, a port-discovery fix, a gofmt pass, and a doc update — split them into
separate commits instead of folding them into one with a bencyclopedic
subject or a bullet-list body.

## Why no body

Reasoning and background belong in the PR description or the review
discussion, not the commit message. A commit with a multi-paragraph body is
usually a sign the change should have been split per
[One commit, one change](#one-commit-one-change).

Commits already in history with a body are grandfathered exceptions, not a
license to add more.

## Examples

Real commits from this repo's history:

```
feat(asynq): add RedisLeaderElector backed by SETNX lease
fix(control): recampaign after leadership loss
test(node): fix trigger test races and inflight limits
docs: move design spec to .claude/specs (gitignored, project convention)
chore(db): sync schema with node lease columns
```
