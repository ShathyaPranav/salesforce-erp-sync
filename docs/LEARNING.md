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
