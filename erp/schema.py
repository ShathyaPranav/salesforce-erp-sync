"""DynamoDB table design for the mock ERP.

Three tables, one item per business object. Every key is derived from a
Salesforce ID, so the same deal always lands on the same item no matter how
many times its message is delivered. That property is what makes the write
idempotent (see docs/data-model.md for the full item shapes).

Physical table names come from environment variables so the same code works
against LocalStack (plain names) and AWS (stack-prefixed names).
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class TableSpec:
    logical_name: str
    env_var: str
    partition_key: str

    @property
    def name(self) -> str:
        """Physical table name: the env var if set, else the logical name."""
        return os.environ.get(self.env_var, self.logical_name)


# customer_id = Salesforce AccountId
CUSTOMERS = TableSpec("customers", "CUSTOMERS_TABLE", "customer_id")
# order_id = Salesforce OpportunityId (one order per deal)
ORDERS = TableSpec("orders", "ORDERS_TABLE", "order_id")
# invoice_id = "INV-" + OpportunityId (one invoice per order)
INVOICES = TableSpec("invoices", "INVOICES_TABLE", "invoice_id")

ALL_TABLES: tuple[TableSpec, ...] = (CUSTOMERS, ORDERS, INVOICES)


def invoice_id_for(opportunity_id: str) -> str:
    return f"INV-{opportunity_id}"
