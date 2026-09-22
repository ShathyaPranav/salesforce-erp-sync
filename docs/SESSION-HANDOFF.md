# Where Relay stands — handoff, 22–23 Sep 2026

Written at the end of the first session so a fresh window can pick up without
re-deriving anything.

## Nothing outside this folder was changed

* **No GitHub activity at all.** No push, no repo created, no settings or
  secrets touched. There is nothing on GitHub to clean up.
* **The 4DGS repo was never written to.** That project was only ever this
  window's *starting* directory, which is why the session's temp and log paths
  carried its name. Its HEAD, reflog and untracked files are unchanged.
* This repo has **no commits and no remote** yet: `git init -b main` was run,
  nothing was committed, because CLAUDE.md asks for the git identity to be
  confirmed first (see below).

Two things were changed outside the folder, both deliberate:

* The Python virtualenv lives at `C:\Users\luckf\.venvs\relay`, **outside
  OneDrive**, so OneDrive doesn't sync ~10k library files.
* Docker Desktop had been failing to start since December 2025 on two stale
  zero-byte socket files (`%LOCALAPPDATA%\Docker\run\dockerInference` and
  `userAnalyticsOtlpHttp.sock`). They were deleted from WSL and Docker now
  starts normally. Unrelated to Relay; it just had to work first.

## What exists and passes

`pytest` → **23 passed, 3 skipped** (the 3 are the contract test, waiting on a
recorded response from your real org).

| Path | What it is |
| --- | --- |
| `fake_salesforce/` | Stub of Salesforce: OAuth client-credentials token endpoint, `/services/data/vNN.N/query` with `nextRecordsUrl` paging, and an `/__admin` API for tests (create/edit deals, force timestamps, inject faults, replay the SOQL log). Strict SOQL subset: a quoted datetime is rejected exactly as the real API rejects it. |
| `fake_salesforce/seed.json` | Sample data mirroring what you'll create in the real org, including the edge cases: no Amount, no Account, two deals sharing a SystemModstamp, one edited twice, plus non-won deals that must never sync. |
| `erp/schema.py` | The three DynamoDB tables (`customers`, `orders`, `invoices`), keyed off Salesforce IDs. Single source for the bootstrap, tests and (later) `template.yaml`. |
| `docs/data-model.md` | Item shapes and the reasoning behind the write condition. |
| `scripts/local_bootstrap.py` | Creates tables, the queue + DLQ (maxReceiveCount 5) and the SSM parameters. Idempotent; runs as the compose `bootstrap` service. |
| `scripts/sf_query.py` | One SOQL query from the command line. `--fake` for the stub, no flag for your real org via `.env`, `--record` to save a real response for the contract test. Never prints secrets. |
| `docker-compose.yml` | `localstack` + `fake-salesforce` + one-shot `bootstrap`. `docker compose up -d` works today. |
| `tests/unit/` | 17 tests for the stub, incl. the `>=` boundary case. |
| `tests/integration/` | Stack checks, plus two "emulator fidelity" tests proving LocalStack really does the DLQ move after N receives and really returns the old item on a failed transaction condition. |
| `docs/research/` | Findings from the background research workflow (see caveat below). |

Phase 1.1 is done except its second "done when": *a real Salesforce query works
from the command line*, which needs your org.

## Decisions taken, and the ones that are yours

1. **LocalStack is pinned to `4.14.0`.** The current image (2026.8.3) refuses
   to start without `LOCALSTACK_AUTH_TOKEN`; 4.14.0 (Feb 2026) is the last
   release that runs with no account, and it passes the fidelity tests above.
   The cost is that it's frozen — no future fixes. Alternatives if you'd rather
   not pin: a free LocalStack Hobby/Student token (a secret in CI, and their
   free tier is non-commercial), or `motoserver/moto`. **This deviates from
   PLAN.md's plain "LocalStack", so it's your call to confirm.**
2. **Python 3.12** everywhere (your `python` is 3.12.8), so the Lambda base
   image will be `public.ecr.aws/lambda/python:3.12`.
3. `settings.json` was moved to `.claude/settings.json`, where Claude Code
   actually reads it; `PLAN.md` moved to `docs/PLAN.md` to match CLAUDE.md.

## Blockers found, both needing you

1. **Salesforce connected apps.** New orgs can no longer create them; you
   create an **External Client App** instead and enable the client credentials
   flow there, with a Run As user. The token endpoint must be your **My Domain**
   URL, not `login.salesforce.com`.
2. **LocalStack licensing**, as above.

## Your machine

Present: git 2.51, Docker 28.3.2 + compose v2.39.1 (working now), Python 3.12.8,
AWS CLI 2.33.9 (no profile configured yet), gh 2.78.0, chocolatey.
**Missing: Go, AWS SAM CLI, golangci-lint** (and `winget` isn't on PATH in this
shell; it exists at `%LOCALAPPDATA%\Microsoft\WindowsApps\winget.exe`).
Go is needed for Phase 1.2, SAM for 2.1. Install commands are in
`docs/research/plan-research-2026-09-22.md` (§aws-account Q6, Q7).

Git identity for this repository (set in `.git/config`):
`ShathyaPranav <s.r.k.shathyapranav@gmail.com>`, pushed as the GitHub
account `ShathyaPranav`.

## Read the research with a pinch of salt

`docs/research/plan-research-2026-09-22.md` holds four topic reports
(LocalStack, Salesforce, AWS account/tooling, AWS semantics) and a 27-item
critique of PLAN.md. **The adversarial verification pass never ran** — the
session hit its usage limit — so those findings are single-pass. The two
blockers above were confirmed independently (LocalStack by running it, the two
emulator behaviours by the fidelity tests); the rest are not.

The critique's own headline items: nothing in the local stack triggers the
worker (the Lambda emulator only exposes an invoke endpoint, so a small
SQS-poller stand-in is needed); the watermark can miss late-committing
transactions; `>=` against "newest published stamp" republishes the boundary
forever; partial publish failure can skip a row; the OIDC role must exist
before CI can use it; and the repo sitting in a space-containing OneDrive path
is a real hazard on Windows.

## Starting the next window

1. Open **`C:\Users\luckf\OneDrive\Desktop\EverPure Relay`** as the project
   folder (not the 4DGS one). Everything here is self-contained.
2. Optional but recommended, and cheapest to do now while there are no commits:
   move the project out of OneDrive and out of a path with a space, e.g.
   `C:\dev\relay`. OneDrive syncing a live `.git` is a known source of index
   corruption, and the space in "EverPure Relay" trips some Windows tooling.
   ```
   robocopy "C:\Users\luckf\OneDrive\Desktop\EverPure Relay" "C:\dev\relay" /E /MOVE
   ```
3. The stack may still be running (`docker compose ps`). `docker compose up -d`
   is safe to re-run; `docker compose down` stops it.
4. Run the tests to confirm the state this document describes:
   ```
   C:\Users\luckf\.venvs\relay\Scripts\python -m pytest -q
   ```

Don't paste or attach any files. `CLAUDE.md` is loaded automatically from the
project root, and it already tells Claude to read `docs/PLAN.md` before
starting; `.claude/settings.json` is read by the tool itself. Naming a file is
enough for it to be read off disk. Copy this as the first message:

```
Read CLAUDE.md, docs/PLAN.md and docs/SESSION-HANDOFF.md fully first.

Phase 1.1 is already built and its tests pass - confirm that yourself, then:

1. Summarise the project back to me in 5-6 lines, and flag anything in the plan
   that looks wrong, risky or unclear. docs/research/ has a critique from the
   last session, but its verification pass never ran, so check its claims
   rather than trusting them.
2. Check my machine: git and my git user.name/email, Docker running, whichever
   Python and Go versions are installed, plus the AWS CLI and AWS SAM CLI.
   Tell me what's missing and how to install it on Windows 11.
3. Give me the manual checklist for everything only I can do, with exact steps:
   Salesforce Developer Edition signup, the app for the OAuth 2.0 client
   credentials flow (new orgs need an External Client App, not a connected
   app), sample Accounts and Opportunities including the edge cases (one with
   no Amount, one deal edited twice), the $1 AWS Budget alert, and configuring
   the AWS CLI locally. Tell me exactly which values go in .env.

Also confirm the LocalStack 4.14.0 pin, or tell me the better option.

Then carry on from Phase 1.2 through the phases in order, without waiting on me
except where CLAUDE.md says to stop and ask. Build against the fake Salesforce
stub until my real credentials are ready. Post an update after every phase.
```
