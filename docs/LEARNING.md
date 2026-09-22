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
