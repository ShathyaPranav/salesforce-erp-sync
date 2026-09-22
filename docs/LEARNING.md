# Learning log

One entry per build phase: what was built, how it works, how to see it, and
one interview question. Newest at the bottom.

## Phase 1.1: local stack, fake Salesforce, ERP tables

**What I built.** `docker-compose.yml` starts three things: LocalStack (a
local imitation of SQS, DynamoDB and SSM), `fake_salesforce/` (a small Python
server that behaves like Salesforce's OAuth and query API), and a one-shot
`scripts/local_bootstrap.py` that creates the queue, its dead-letter queue,
the three ERP tables from `erp/schema.py`, and the SSM parameters.

**How it works.** Every ERP key is derived from a Salesforce ID:
`orders.order_id` is the Opportunity ID, `invoices.invoice_id` is
`INV-<OpportunityId>`, and `customers.customer_id` is the Account ID. So the
same deal always lands on the same DynamoDB item, however many times its
message arrives. That's where **idempotency** (doing something twice has the
same effect as doing it once) begins: a duplicate can't create a second order,
because there is no second key to create. The queue has a **redrive policy**:
after a message has been received 5 times without being deleted, SQS moves it
to `relay-events-dlq`, the **dead-letter queue**, where a human looks at it.
The fake Salesforce is deliberately strict. It rejects a quoted datetime in
SOQL exactly as the real API does, so poller bugs show up locally.

**See it yourself.**

```powershell
docker compose up -d --wait
C:\Users\luckf\.venvs\relay\Scripts\python scripts\sf_query.py --fake
```

You'll see `6 record(s)`: the Closed Won deals in `fake_salesforce/seed.json`,
including `006FAKE00000000003` with `amount=None`.
`docker compose logs bootstrap` shows the tables, queue and parameters being created.

**Interview check.** *Why key the order on the Salesforce Opportunity ID
instead of generating a new ID?* A generated ID would give every delivery of
the same message a fresh key, so duplicates would become duplicate orders.
Keying on the source ID makes the write target deterministic, and the
conditional write in Phase 1.3 can then decide whether the incoming version
is new, a duplicate or stale.

## Phase 1.2: the ingest poller (Go)

**What I built.** A Go Lambda in `ingest/`: `internal/salesforce/client.go`
(OAuth token, SOQL paging, Salesforce's clock), `internal/events/event.go`
(the SQS message, see `docs/event-schema.md`), and `ingest/poller/poller.go`
(the watermark logic), shipped as a container image (`ingest/Dockerfile`) that
docker compose runs on port 9001.

**How it works.** Each run, `Poller.Run` reads the **watermark** (the point in
time it has already read up to) from SSM `/relay/ingest/watermark`. It then
reads Salesforce's clock from the HTTP `Date` header, sets `upper` = that time
minus 2 minutes, and queries `IsWon = true AND SystemModstamp >= watermark AND
SystemModstamp < upper`. It sends one message per row, 10 per `SendMessageBatch`,
and finally saves `upper` as the new watermark. The key idea is this
**half-open window** `[watermark, upper)`. `>=` means two deals sharing a
timestamp at the edge can't be skipped. `<` plus "watermark = upper" means the
windows tile time exactly, so a quiet org sends nothing. The 2-minute lag exists
because Salesforce stamps a record when it's *written*, not when its transaction
*commits*: a slow transaction could appear behind a watermark that has already
moved on. One subtle bug is guarded in `ingest/poller/aws.go` `Publish`:
`SendMessageBatch` returns success even when some entries failed, so the
`Failed` list is checked, and the watermark only moves up to the first unsent
event. Re-sending is fine, because SQS is **at-least-once** anyway (a
message can arrive more than once) and the ERP write in Phase 1.3 is idempotent.

**See it yourself.**

```powershell
C:\Users\luckf\.venvs\relay\Scripts\python scripts\local_bootstrap.py --reset-state
curl.exe -s -d "{}" http://127.0.0.1:9001/2015-03-31/functions/function/invocations
curl.exe -s -d "{}" http://127.0.0.1:9001/2015-03-31/functions/function/invocations
```

The first call prints `"published":6`, and the second `"published":0`: nothing
changed, so nothing is re-sent. `docker compose logs ingest` shows one JSON
line per `event_key`.

**Interview check.** *Why does the poller use Salesforce's clock and stay 2
minutes behind it?* Because `SystemModstamp` comes from Salesforce's clock, so
measuring the window with the Lambda's clock would add clock skew as a new
failure mode. The lag covers commit delay: a record stamped at 10:00:00 may
only become visible at 10:00:40, so a window closing at "now" could step past
it. Anything slower than the lag is caught by the nightly reconciler.

## Phase 1.3: the sync worker and the idempotent write (Python)

**What I built.** `worker/mapping.py` (validate an event, map Salesforce
fields to ERP fields), `erp/writer.py` (`write_order`, the conditional
DynamoDB transaction) and `worker/app.py` (the Lambda handler), shipped as
`worker/Dockerfile`. `tests/harness/esm_pump.py` stands in locally for
Lambda's SQS trigger, which only exists on AWS.

**How it works.** For each message, `parse_body` reads the JSON with numbers
as `Decimal` (DynamoDB rejects floats), `to_order` checks the required fields,
and `write_order` sends **one `TransactWriteItems`** with three updates: the
order, its invoice and the customer. Only the order has a condition:
`attribute_not_exists(order_id) OR version < :v`, where `version` is
`SystemModstamp` in milliseconds. That single line is the **idempotent write**
(running it twice has the same effect as running it once). A new deal passes.
A newer edit passes and bumps `revision`. The same version again fails, so
it's a *duplicate*. An older version fails, so it's *stale*. Because it's a
**transaction** (all or nothing), a failed condition also cancels the invoice
and customer writes, so the three tables can never disagree. On failure,
`ReturnValuesOnConditionCheckFailure=ALL_OLD` hands back the stored order, so
`_classify` can tell duplicate from stale without another read. The handler
processes each record on its own and returns a **partial batch response**: only
the failed message IDs, so one bad message doesn't make the other nine retry.

**See it yourself.**

```powershell
C:\Users\luckf\.venvs\relay\Scripts\python -m pytest tests/integration/test_worker.py -v
docker compose logs worker | Select-String '"duplicate"|"stale"'
```

Four passing tests, then log lines showing the extra copies of one event
landing as `duplicate`, and the old v1 arriving after v2 as `stale`.

**Interview check.** *The worker wrote the order, then crashed before SQS
deleted the message. What happens?* The message becomes visible again after
its visibility timeout and is delivered a second time. The write's condition
sees the same version already stored and does nothing, so the retry is
harmless. This is exactly-once *effect* from at-least-once *delivery*, and
it's why Relay needs no outbox table.

## Phase 1.4: failure handling (the minimum credible project)

**What I built.** `worker/classify.py` (transient or permanent),
`worker/backoff.py` (retry delays), `worker/chaos.py` (failures you can switch
on), the three outcomes in `worker/app.py` `handle_record`, the reconciler's
core in `reconciler/reconcile.py`, and `tests/integration/test_failures.py`:
one test per row of the failure table.

**How it works.** When a record fails, `classify` decides its fate.
**Permanent** errors (no Amount, no Account, bad fields) can never succeed on
retry, so `dead_letter` copies the message to the DLQ with a `reason`
attribute and the original is deleted. **Transient** errors (throttling, a
transaction conflict, anything unexpected) might succeed later. So the worker
sets that message's **visibility timeout** (how long SQS hides a received
message before handing it out again) to `base × 2^(attempt-1) + jitter`, and
returns its ID in `batchItemFailures`. SQS redelivers it after that delay, and
the queue's redrive policy moves it to the DLQ after 5 receives. The key idea is
that **the queue does the waiting, not the Lambda**: no `sleep`, no paying for
idle time, and a crash mid-retry loses nothing. Unknown errors count as
transient on purpose, because 5 receives bounds the cost of being wrong. The
reconciler compares every Closed Won deal with the ERP: it re-sends missing or
out-of-date orders, and only *reports* invalid, reverted or deleted deals,
because those need a human.

**See it yourself.**

```powershell
C:\Users\luckf\.venvs\relay\Scripts\python -m pytest tests/integration/test_failures.py -v
docker compose logs worker | Select-String '"retry"|"dead_lettered"'
```

Nine passing tests named after the failure table. The logs show `retry` lines
with `attempt` 1 to 5 and `retry_in_s` roughly doubling, and a
`dead_lettered` line with `reason: missing_amount`.

**Interview check.** *How do you decide whether an error is retried or
dead-lettered?* Ask whether the same message could succeed later. A throttled
DynamoDB write can succeed a minute later, so it's transient and retried with
backoff. A deal with no Amount will fail forever, so it's permanent and
dead-lettered immediately with a reason. Retrying it would only burn the retry
budget and delay the alert.

## Phase 2.1: infrastructure as code (built and validated; first deploy waits on your approval)

**What I built.** `template.yaml` (AWS SAM: the whole app), `infra/bootstrap.yaml`
(a one-time stack with the image repository and, later, the GitHub deploy
role), and `scripts/deploy.py`, `scripts/put_sf_secret.py` and
`scripts/smoke.py`. `tests/unit/test_template.py` pins the cost and failure
rules, and both templates pass `sam validate --lint`.

**How it works.** SAM is a shorthand for CloudFormation: `sam build` builds the
three Docker images, and `sam deploy` pushes them to ECR and creates or updates
every resource as one stack called `relay-dev`. Every setting that the failure
handling relies on is written down, not clicked in a console:
- `FunctionResponseTypes: [ReportBatchItemFailures]` on the worker's SQS trigger;
- a queue `VisibilityTimeout` of 180 s (6 × the worker's 30 s timeout);
- `maxReceiveCount: 5`;
- a DLQ that keeps messages 14 days;
- `MaximumRetryAttempts: 0` on the poller, so a failed run doesn't overlap the next.

The key idea is **what the stack does *not* own**. The Salesforce secret is a
SecureString, which CloudFormation can't create, so `put_sf_secret.py` stores
it once. The watermark and chaos flags are written at runtime, so a redeploy
can never reset them. There's no `ReservedConcurrentExecutions`, because new
accounts only have 10 and reserving any fails the deploy. The worker is capped
with `ScalingConfig.MaximumConcurrency: 2` instead.

**See it yourself.**

```powershell
docker compose run --rm sam sam validate --lint
C:\Users\luckf\.venvs\relay\Scripts\python -m pytest tests/unit/test_template.py -v
```

`template.yaml is a valid SAM Template`, then 8 guard tests pass.

**Interview check.** *Why is the Salesforce secret not in your CloudFormation
template?* CloudFormation can't create SecureString parameters, and a secret
in a template ends up in version control and stack history. It's stored once in
SSM Parameter Store, encrypted with KMS, and the Lambdas read it at cold start
with `WithDecryption`. The stack only grants them permission to read that one path.

## Phase 2.2: CI/CD (built and linted; first run waits on the GitHub repo)

**What I built.** `.github/workflows/ci.yml`: jobs `lint`, `unit`,
`integration`, `deploy` and a smoke test, plus the OIDC role in
`infra/bootstrap.yaml` (`GitHubDeployRole`).

**How it works.** Every push and pull request runs the linters, the unit
tests, and the whole failure-injection suite on `docker compose` (which also
builds the three images). Only a push to `main` reaches the `deploy` job. That
job has `permissions: id-token: write`, so GitHub issues it a signed
**OIDC token** (a short-lived identity document saying "this is a workflow run
on main in your repo"). `configure-aws-credentials` trades that token for
temporary credentials of `relay-github-deploy`, whose trust policy accepts only
`repo:<you>/<repo>:ref:refs/heads/main`. The key idea is **no stored keys**:
nothing in GitHub can be leaked and used later, because the credentials expire
within the hour. After `sam deploy`, `scripts/smoke.py` pushes a unique
`006SMOKE…` deal through the live queue and waits for its order. Because it's
unique, it can't pass by finding an old one.

**See it yourself.**

```powershell
docker run --rm -v "${PWD}:/repo" -w /repo rhysd/actionlint:latest
```

No output means the workflow is valid (actionlint also runs shellcheck on every `run:` step).

**Interview check.** *How does your pipeline deploy without AWS keys?* GitHub
Actions signs a short-lived OIDC token for each workflow run. AWS is configured
to trust GitHub's token issuer, and my deploy role's trust policy only accepts
tokens for the `main` branch of my repository. The job exchanges the token for
temporary credentials, so there's no long-lived secret to rotate or leak.

## Phase 2.3: operating it (built and tested locally; the AWS demo waits on the deploy)

**What I built.** Metrics in `worker/metrics.py` (plus `emitMetric` in
`ingest/main.go`); three alarms, an SNS alert topic, a dashboard and a saved
Logs Insights query in `template.yaml`; and `relayctl`, a Go CLI
(`relayctl/main.go`, with the logic in `relayctl/ops/dlq.go`).

**How it works.** Each Lambda prints one **Embedded Metric Format** line per
invocation: a JSON log line with an `_aws` block that tells CloudWatch
"these fields are metrics". CloudWatch turns it into `OrdersWritten`,
`DuplicatesSkipped`, `TransientRetries` and the rest, with no API call.
Metrics are counted per dimension combination, so they all share a single
`Service` dimension: 7 metrics, inside the 10 free.

There are three alarms, and each emails when it fires and when it clears:
- **DLQ not empty.** A human has to look.
- **Oldest message over 20 minutes.** That's deliberately above the ~15-minute
  retry budget, so normal backoff never trips it.
- **Three failed polls in 10 minutes.** Usually a Salesforce auth problem.

`relayctl dlq redrive` shows the key operational idea: it only moves messages
*without* a `reason` back to the queue. Those are transient failures that ran
out of retries. A message with `reason: missing_amount` would just fail again,
so the fix is to correct the deal in Salesforce and then `relayctl dlq ack` the
old message. Redrive also deletes from the DLQ only *after* the copy is on the
main queue, so a crash in the middle leaves a harmless duplicate and never a
lost message.

**See it yourself.**

```powershell
docker compose run --rm go go run ./relayctl --local --endpoint http://localstack:4566 stats
docker compose run --rm go go run ./relayctl --local --endpoint http://localstack:4566 dlq list
```

The first prints queue depths, the ERP table counts, the watermark and the
chaos flags. The second lists each dead-letter with its kind (permanent or
transient) and its reason.

**Interview check.** *The ERP was down for two hours. What happened, and how do
you recover?* Every message retried with growing delays for about 15 minutes,
then SQS moved it to the DLQ. The DLQ alarm emailed me, and the queue-age alarm
didn't fire, because messages were moving, just failing. Once the ERP is back,
`relayctl dlq redrive` sends the transient failures back. Anything left over is
caught by the nightly reconciler, and duplicates are harmless because the
writes are idempotent.

## Phase 2.4: polish

**What I built.** `README.md` (architecture diagram, the failure table
linked to the test for each row, quick start, a two-minute demo, deploying,
operating, cost, known limits), the exact deploy and GitHub steps in
`docs/SETUP-CHECKLIST.md` (sections 7–8), and a clean-slate
`scripts/local_bootstrap.py --reset-state` so the demo always starts from zero.

**How it works.** The README is built around the same idea as the tests:
every failure the system claims to handle is a row with a named test that
triggers it. Someone can clone the repo, run `docker compose up` and `pytest`,
and check each claim without an AWS account. The key idea is
**reproducibility**. The demo resets to a known state, so it always shows the
same numbers: 4 orders, 4 invoices, 3 customers, 2 dead-letters with reasons,
and 0 events on the second poll.

**See it yourself.** Follow "Two-minute demo (local)" in the README from the top.

**Interview check.** *How would you convince me this system is correct?* I'd
point to the failure table: each row is a failure I trigger on purpose, with an
integration test that runs through the real queue, worker image and database.
Then I'd demo one live: break the ERP with the chaos flag, show the retries
backing off and the DLQ alarm firing, then recover with `relayctl dlq redrive`
and show that the order count is still right.

## Reading guide: the four core pieces

Read these until you can explain every line. That's what the interview tests.
For each one, read the code first, then the tests that pin it down.

1. **The idempotent write.** `erp/writer.py`: `write_order`, `_transaction`, `_classify`.
   Understand:
   - why the condition is `attribute_not_exists(order_id) OR version < :v`, and
     what happens for new, newer, the same and older versions;
   - why only the *order* is conditional, and why all three items share one
     transaction (a failed condition cancels the invoice and customer too);
   - why `version` is `SystemModstamp` in epoch milliseconds, stored as a Number
     (strings compare wrongly across formats);
   - how `ReturnValuesOnConditionCheckFailure=ALL_OLD` tells *duplicate* from
     *stale* without a second read.

   Tests: `tests/integration/test_erp_writer.py`, and rows 1, 4 and 6 of
   `test_failures.py`.

2. **Transient vs permanent errors.** `worker/classify.py` `classify`;
   `worker/app.py` `handle_record`, `dead_letter` and `_delay_redelivery`;
   `worker/backoff.py`. Understand:
   - the one question behind the split: could the same message succeed later?
   - why unknown errors default to transient (maxReceiveCount 5 caps the cost);
   - why a permanent error is sent to the DLQ *before* it's acknowledged, and
     what happens if that send fails;
   - how the visibility timeout replaces `sleep`;
   - why `ReportBatchItemFailures` must be on in `template.yaml`.

   Tests: `tests/unit/test_worker_failures.py`, and rows 2, 3 and 5 of
   `test_failures.py`.

3. **The watermark.** `ingest/poller/poller.go` `Run` and `Query`;
   `ingest/poller/aws.go` `Publish`. Understand:
   - the half-open window `[watermark, upper)`: why `>=` below and `<` above,
     and why the watermark becomes `upper`, not the newest record's stamp;
   - why `upper` is Salesforce's clock minus 2 minutes (commit lag, no clock skew);
   - the partial-failure rule (move only to the first unsent event);
   - what the design can still miss (a commit slower than the lag).

   Tests: `ingest/poller/poller_test.go`, especially
   `TestIdleOrgPublishesNothingOnTheNextPoll`,
   `TestEqualTimestampsAreBothPublishedExactlyOnce` and
   `TestPartialPublishFailureNeverSkipsAnUnsentEvent`; then
   `tests/integration/test_ingest.py`.

4. **The reconciliation comparison.** `reconciler/reconcile.py` `compare`.
   Understand:
   - the six drift kinds, and which get re-enqueued (`missing_order`,
     `stale_order`) versus only reported;
   - why a deal is validated before it's re-enqueued;
   - why recent changes are skipped (the grace period);
   - why reverts and deletes need a human;
   - how the golden file (`tests/contract/fixtures/event_golden.json`) keeps the
     Python and Go event builders identical.

   Tests: `tests/unit/test_reconcile.py`, and rows 7 and 8 of `test_failures.py`.
