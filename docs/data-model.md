# Mock ERP data model (DynamoDB)

Three tables, as in the plan. The code-side definition is `erp/schema.py`; the
local bootstrap (`scripts/local_bootstrap.py`) and `template.yaml` both create
tables from these specs.

The rule behind every key: **derive it from a Salesforce ID**. The same deal
always maps to the same order item, so a repeated message targets the same
item instead of creating a second one. Idempotency starts with the key.

## `customers` (PK `customer_id` = Salesforce `AccountId`)

| Attribute | Type | Notes |
| --- | --- | --- |
| `customer_id` | S | Salesforce Account ID (18 chars) |
| `name` | S | `Account.Name` from the latest synced deal |
| `created_at` | S | ISO-8601, set once (`if_not_exists`) |
| `updated_at` | S | ISO-8601 |
| `last_event_key` | S | event key of the deal that last touched it |

Upserted **unconditionally** in the same transaction as the order. It must not
carry its own version condition: its "version" would come from an Opportunity,
not the Account, so a condition here could cancel a perfectly valid order
write. Last writer wins on `name`; the order keeps its own snapshot.

## `orders` (PK `order_id` = Salesforce `Opportunity.Id`)

| Attribute | Type | Notes |
| --- | --- | --- |
| `order_id` | S | Opportunity ID: one order per deal |
| `customer_id` | S | Account ID |
| `customer_name` | S | snapshot of `Account.Name` at this version |
| `opportunity_name` | S | `Opportunity.Name` |
| `total` | N | `Amount` |
| `order_date` | S | `CloseDate` (YYYY-MM-DD) |
| `version` | N | `SystemModstamp` as epoch milliseconds: the ordering guard |
| `source_modstamp` | S | raw `SystemModstamp` string from Salesforce |
| `event_key` | S | `OpportunityId:SystemModstamp` |
| `revision` | N | how many times this order was actually written (1 = never updated) |
| `created_at`, `updated_at` | S | ISO-8601 |

The write condition (Phase 1.3):

```
attribute_not_exists(order_id) OR version < :incoming_version
```

* Same version again: the condition fails, so it's a **duplicate** and nothing changes.
* Older version: the condition fails, so it's **stale** and gets dropped.
* Newer version: the condition passes, so the order is updated and `revision` goes up by 1.

## `invoices` (PK `invoice_id` = `"INV-" + Opportunity.Id`)

| Attribute | Type | Notes |
| --- | --- | --- |
| `invoice_id` | S | `INV-<OpportunityId>`: one invoice per order |
| `order_id` | S | |
| `customer_id` | S | |
| `amount` | N | equals the order total at this version |
| `issue_date` | S | `CloseDate` |
| `status` | S | `ISSUED` |
| `version` | N | same version as the order it belongs to |
| `event_key` | S | |
| `created_at`, `updated_at` | S | |

Written in the same transaction as the order, so it can never disagree with
it: if the order's condition fails, the whole transaction is cancelled and the
invoice isn't touched either.

## Why no GSIs

Every read in Relay is by primary key (worker, smoke test) or a full scan of a
tiny table (nightly reconciler). A global secondary index would add write cost
and a second consistency model for no benefit at this scale.
