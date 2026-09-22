# The event message (`relay.opportunity.v1`)

One SQS message per version of a Closed Won deal. The Go poller builds it
(`internal/events/event.go`) and the Python worker consumes it. The message
format is the contract between the two languages, so any change bumps `schema`.

```json
{
  "schema": "relay.opportunity.v1",
  "event_key": "006FAKE00000000001:2026-09-15T09:00:00.000Z",
  "version": 1789462800000,
  "opportunity": {
    "id": "006FAKE00000000001",
    "name": "Acme - 50 robot arms",
    "stage_name": "Closed Won",
    "amount": 125000.0,
    "close_date": "2026-09-01",
    "account_id": "001FAKE00000000001",
    "account_name": "Acme Robotics",
    "system_modstamp": "2026-09-15T09:00:00.000Z"
  },
  "published_at": "2026-09-22T10:00:00Z"
}
```

| Field | Meaning |
| --- | --- |
| `event_key` | `OpportunityId:SystemModstamp` (UTC). Identifies one version of one deal; logged on every line by both Lambdas. Also sent as the `event_key` message attribute so DLQ tooling can read it without parsing the body |
| `version` | `SystemModstamp` as epoch milliseconds. The ERP stores it on the order and only accepts a strictly newer one |
| `amount`, `close_date`, `account_id`, `account_name` | May be `null`. The poller doesn't judge the data; the worker decides a missing Amount or Account is a permanent error |
| `amount` | A JSON number with Salesforce's exact decimal text. The worker parses it with `parse_float=Decimal`, because DynamoDB rejects Python floats |
| `published_at` | When the poller sent it. Informational only |

Delivery is at least once and unordered (SQS standard queue): the same
`event_key` can arrive more than once, and an older version can arrive after a
newer one. The worker's conditional write handles both.
