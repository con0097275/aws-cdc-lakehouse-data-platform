# What was changed to make this copy public

This repository is a **sanitized copy** of a private one. The original runs against a real
AWS account; this one names nobody and nothing you could target.

Nothing was removed to hide a defect. The engineering record — every defect found and how it
was fixed — is intact in `docs/ENGINEERING_JOURNAL.md` and `DECISION_LOG.md`.

## Identifiers replaced

| Original | Here | Why |
|---|---|---|
| a 12-digit AWS account id | `111122223333` | an account id plus a bucket name is a target list |
| the owner's CLI profile | `my-aws-profile` | names a person and their credential profile |
| the owner's email | `owner@example.com` | personal address |
| absolute home paths | `~` / `/path/to/aws-cdc-lakehouse` | leaks a username and local layout |
| the unix username | `operator` | same |
| a **second**, unrelated AWS account id (the workstation's `[default]` profile, named in the OPEN-13 defect notes) | `444455556666` | found 2026-10-06. It was never on the replacement list because it is not the project's account — the scan looked for one id, and a second real one in eight places went through. Both placeholders are AWS's own documentation values |

`111122223333` is AWS's own documentation placeholder, so it is unmistakable as a stand-in.

**The identity guard still works, and will refuse to run for you.** `scripts/lib.sh` asserts
`EXPECT_ACCOUNT` / `EXPECT_PROFILE` before any AWS call. Those now hold placeholders, so
every script stops rather than touching an account it was not built for. Set your own values
before running anything.

## Files not carried over

| Left out | Why |
|---|---|
| **git history** | This repo starts at one commit. Filtering history is error-prone and one missed blob undoes the exercise, so it was dropped rather than rewritten. |
| `terraform.tfvars`, any `*.tfstate` | real values; state is sensitive by policy |
| `tfplan*.json` | a plan JSON carries `prior_state` — a state dump by another name. The one in the private repo had every secret redacted by Terraform (294 `sensitive_values` markers) and it still does not get published. |
| `dbt/logs/`, `*.log` | real run logs, full of absolute paths and a username |
| `artifacts/validation/ai-ui-sessions.jsonl` | a log of what one operator actually typed |
| `.venv*`, `.terraform/`, `metastore_db/`, caches, jars | build and runtime artefacts (3.7 GB → 30 MB) |
| the sanitiser scripts | they carry the private identifiers in their rewrite table, and the verifier's self-test fixtures — a seeded access key, a seeded private-key header — are indistinguishable from real ones to any scanner, including its own |

## What was checked

A scanner runs over the whole tree for eight patterns — account id, profile, personal email,
home path, username, AWS access keys, private keys, state files — and exits non-zero on any
hit. It **self-tests first**: it seeds one example of every pattern and asserts each is
detected, because a scanner that cannot fail proves nothing.

Result: **clean on all eight.**

### The scan reads text, not pixels — and the screenshots prove it

Corrected 2026-10-06, during a visual audit of `docs/images/`. The eight-pattern scan covers
files it can decode as text. It never looked inside a PNG, and two identifiers survive there
that the same scan would have rejected in any `.md` file:

| In the screenshots | Where | Status |
|---|---|---|
| `van00972756` as the asset **owner** | 6 reliability screenshots | the governance inventory's real owner value, rendered by the UI |
| `"user": "vannguyen"` in the evidence pack JSON | 3 reliability screenshots | the unix username this document says was replaced with `operator` |

Both are the repository author's own handles on a repository that carries their name, so the
exposure is small — but the claim above was stated without qualification, and it was not
true of images. The clean fix is to regenerate those captures with a sanitized owner in the
governance inventory; until then this is recorded rather than hidden.

**Also removed in that audit**: the browser chrome was cropped off 11 screenshots. It
carried a personal bookmarks bar (a course registration, an apartment contract, a film site)
and a profile avatar, plus one stray hover tooltip. None of it was a credential; all of it
was personal and none of it belonged on a portfolio. Three screenshots had no chrome and
were left untouched. PNG metadata chunks were checked separately: **none of the 14 images
carries `tEXt`, `iTXt`, `zTXt`, `eXIf` or `tIME`.**

Worth stating plainly: **no credentials were found anywhere in the private repo.** Secrets
live in SSM and Secrets Manager and are read at runtime, which is why the saved Terraform
plans show parameter *paths* like `/kafka-dev-lab/dev/source-lab/oracle-password` rather than
values. The one file that trips a naive credential grep,
`spark/tests/test_ai_corpus.py`, assembles credential-shaped strings at runtime precisely so
the repo's own scanner can stay strict.

## Two defects the sanitized copy found

Building this copy reproduced a **fresh clone** — no `.terraform/`, no state — which is the
state every reader meets the repository in, and which the private working tree never is.

1. **`scripts/emr-submit.sh` never printed its usage.** The `[ $# -ge 3 ]` check sat *below*
   the `terraform output` and `sts get-caller-identity` calls, so running it with no
   arguments in a fresh clone died with `REFUSING: no lake bucket from terraform output` and
   exit 1 instead of usage and exit 64. Fixed upstream: usage is now the first thing the
   script does.
2. **The committed CDC plan artefact went stale.** `artifacts/cdc/table-plan.json` records a
   hash of the registry's content, and sanitising the registry changes that content. The
   repo's own test caught it — *"a stale one runs the previous config"* — so the build now
   recompiles the plan from the sanitised registry rather than copying a stale one.

## The one thing a text scan cannot check

**Screenshots.** `docs/images/` holds 14 PNGs, and text baked into pixels survives every
substitution above. They were kept because they are the clearest evidence of what the
platform does, and they show `127.0.0.1` URLs, Glue database and table names, an `owner`
field carrying the author's handle, and in some captures a browser bookmarks bar.

None of that is a credential, and the account id appears in none of them. If you are reusing
this repository rather than reading it, retake them against your own deployment.

## Reproducing this copy

It is a build output, not a place to edit — anything written here by hand is overwritten by
the next build. The sanitiser and its verifier live in the private repository.
