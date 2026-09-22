# Relay — instructions for Claude Code

Relay syncs Salesforce Closed Won opportunities into a mock ERP (DynamoDB) with
exactly-once effect under at-least-once delivery. The full design is in
`docs/PLAN.md`. Read it before starting any task, and treat it as the source of
truth. If something in the plan looks wrong, say so and ask. Don't silently
change the design.

## How we work

- Work autonomously through the phases in docs/PLAN.md ("2-day build plan"),
  in order. You don't need my approval for local work: writing code, running
  tests, installing project dependencies, building images, running docker
  compose, committing.
- A phase is finished only when its "Done when" is met and its tests pass.
  Don't move on with a failing test.
- Write tests first for every row of the failure table ("Failure handling").
  A feature is done only when its failure-injection test passes.
- Small commits, one logical change each, with clear messages.

## Stop and ask me before anything external

Stop, tell me exactly what you need and why, and wait before you:

- touch Salesforce in any way that needs me (org setup, connected app, credentials)
- run anything against the real AWS account (`sam deploy`, `sam delete`, any
  `aws` command that creates, changes or deletes resources)
- push to GitHub, create a repo, or change GitHub settings or secrets
- install system-wide tools (Docker, Go, Python, AWS CLI, SAM CLI)

When you need credentials, tell me where to put them (`.env`, which is
gitignored, or SSM Parameter Store). Never ask me to paste secrets into the
chat, and never print them.

## Git: everything is authored by me

- All commits must use my own git identity, the `user.name` and `user.email`
  already configured on this machine. Never change git config and never
  set an author.
- Never add "Co-Authored-By: Claude", "Generated with Claude Code", or any
  other AI attribution to commit messages, PR descriptions or code comments.
- Never push with, or create, any account other than mine. Pushes use my
  existing GitHub credentials, and only after I say yes.
- Before the first commit, run `git config user.name` and
  `git config user.email` and show me the values. If they're empty, stop and
  ask me to set them.

## Keep me updated so I learn the project

I'm going to explain this system in interviews, so the updates matter as much
as the code.

After each phase, write an update in the chat and append the same update to
`docs/LEARNING.md`. Each update has four parts:

1. **What I built:** the files and components, in 2–3 lines.
2. **How it works:** the flow in plain language, then the one key idea of this
   phase and why it's designed that way (e.g. why the write is conditional,
   why `>=` on the watermark). Name the real concept: idempotency,
   at-least-once delivery, visibility timeout. Explain it in a sentence or two
   the first time it appears.
3. **See it yourself:** one or two commands I can run to watch it work, with
   what I should see.
4. **Interview check:** one question an interviewer could ask about this
   phase, and a 2–3 sentence answer.

Pitch updates at a final-year CS student who knows Python, C++ and basic AWS
but is new to integration systems. Not too complicated: no walls of jargon,
no pasting large chunks of code. Not too abstract: always use real names of
files, functions, AWS services and fields, and point to the exact file and
function where the key logic lives. Aim for 150–300 words per phase.

Also give me a one-line heads-up when you start each phase, and tell me
straight away if something fails in a way that changes the plan.

When the build is complete, give me a reading guide: the four core pieces
(idempotent write, transient vs permanent error classification, watermark
logic, reconciliation comparison), with file paths, and what to understand in
each one.

## Stack and layout

```
ingest/          Go Lambda: Salesforce poller -> SQS
relayctl/        Go CLI: dlq list/redrive, stats, reconcile, chaos
worker/          Python Lambda: SQS -> ERP
reconciler/      Python Lambda: nightly drift check
erp/             Python library: DynamoDB data model + conditional writes
fake_salesforce/ Python stub of the Salesforce OAuth + query API
tests/           pytest integration + failure-injection suites
template.yaml    AWS SAM (all infrastructure)
docker-compose.yml
.github/workflows/
docs/PLAN.md, docs/LEARNING.md
```

- Use the Python and Go versions already installed on this machine. Check with
  `python3 --version` and `go version` at the start, pin those versions in the
  project (Dockerfiles, CI, `go.mod`), and use a Lambda base image that matches.
  Only ask me to install something if it's missing or too old for a
  dependency.
- Python: ruff and mypy. Go: golangci-lint.
- Every Lambda ships as a container image built from the AWS Lambda base images.
- Local runs use docker compose + LocalStack. Tests must never need a real AWS
  account.

## Hard rules: cost and safety

- Never create a NAT gateway, VPC, RDS, EC2, ECS, or anything that bills hourly.
- Every log group gets 7-day retention in template.yaml.
- Never commit secrets. `.env` is gitignored; commit `.env.example` with
  placeholder values only.
- CI deploys via GitHub OIDC only. No long-lived AWS keys anywhere.
- No frontend, no web UI, no JavaScript.
