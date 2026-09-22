"""A deliberately small SOQL subset.

It supports exactly what Relay sends (SELECT ... FROM Opportunity WHERE a AND b
ORDER BY x, y LIMIT n) and is strict about types the way real Salesforce is:
comparing SystemModstamp with a quoted string is rejected, just like the real
API rejects it. That strictness catches poller bugs locally instead of on the
real org.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

Literal = str | bool | None | datetime | date | Decimal | tuple[str, ...]


class SoqlError(Exception):
    """Mirrors a Salesforce REST error: HTTP 400 with an errorCode."""

    def __init__(self, error_code: str, message: str) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message


# API name, field type. Keys are lower-case because SOQL field names are
# case-insensitive; responses always use the canonical casing.
OPPORTUNITY_FIELDS: dict[str, tuple[str, str]] = {
    "id": ("Id", "id"),
    "name": ("Name", "string"),
    "accountid": ("AccountId", "id"),
    "amount": ("Amount", "currency"),
    "closedate": ("CloseDate", "date"),
    "stagename": ("StageName", "picklist"),
    "iswon": ("IsWon", "boolean"),
    "isclosed": ("IsClosed", "boolean"),
    "systemmodstamp": ("SystemModstamp", "datetime"),
    "lastmodifieddate": ("LastModifiedDate", "datetime"),
    "createddate": ("CreatedDate", "datetime"),
    "account.id": ("Account.Id", "id"),
    "account.name": ("Account.Name", "string"),
}

_TOKEN_RE = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<string>'(?:[^'\\]|\\.)*')
  | (?P<datetime>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:?\d{2}))
  | (?P<date>\d{4}-\d{2}-\d{2})
  | (?P<number>-?\d+(?:\.\d+)?)
  | (?P<op>>=|<=|!=|<>|=|<|>)
  | (?P<punct>[(),])
  | (?P<ident>[A-Za-z_][A-Za-z0-9_.]*)
    """,
    re.VERBOSE,
)


@dataclass(frozen=True)
class Token:
    kind: str
    text: str


@dataclass(frozen=True)
class Condition:
    field: str  # canonical API name
    op: str  # = != < <= > >= IN
    value: Literal


@dataclass(frozen=True)
class OrderBy:
    field: str
    descending: bool


@dataclass(frozen=True)
class Query:
    fields: tuple[str, ...]
    sobject: str
    where: tuple[Condition, ...]
    order_by: tuple[OrderBy, ...]
    limit: int | None


def tokenize(soql: str) -> list[Token]:
    tokens: list[Token] = []
    pos = 0
    while pos < len(soql):
        match = _TOKEN_RE.match(soql, pos)
        if match is None:
            raise SoqlError("MALFORMED_QUERY", f"unexpected character at {pos}: {soql[pos]!r}")
        kind = match.lastgroup or "ws"
        if kind != "ws":
            tokens.append(Token(kind, match.group()))
        pos = match.end()
    return tokens


def parse_datetime_literal(text: str) -> datetime:
    """Parse 2026-09-22T10:00:00Z, ...00.000+0000 or ...00+05:30 into aware UTC."""
    normalised = text.replace("Z", "+00:00")
    if re.search(r"[+-]\d{4}$", normalised):
        normalised = normalised[:-2] + ":" + normalised[-2:]
    return datetime.fromisoformat(normalised).astimezone(UTC)


class _Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self.tokens = tokens
        self.i = 0

    def peek(self) -> Token | None:
        return self.tokens[self.i] if self.i < len(self.tokens) else None

    def next(self) -> Token:
        tok = self.peek()
        if tok is None:
            raise SoqlError("MALFORMED_QUERY", "unexpected end of query")
        self.i += 1
        return tok

    def keyword(self, word: str) -> bool:
        tok = self.peek()
        if tok is not None and tok.kind == "ident" and tok.text.upper() == word:
            self.i += 1
            return True
        return False

    def expect_keyword(self, word: str) -> None:
        if not self.keyword(word):
            got = self.peek()
            raise SoqlError("MALFORMED_QUERY", f"expected {word}, got {got.text if got else 'end'}")

    def field(self) -> tuple[str, str]:
        tok = self.next()
        if tok.kind != "ident":
            raise SoqlError("MALFORMED_QUERY", f"expected a field name, got {tok.text!r}")
        known = OPPORTUNITY_FIELDS.get(tok.text.lower())
        if known is None:
            raise SoqlError(
                "INVALID_FIELD",
                f"No such column '{tok.text}' on entity 'Opportunity'. If you are attempting "
                "to use a custom field, be sure to append the '__c' after the custom field name.",
            )
        return known

    def literal(self, field: str, ftype: str) -> Literal:
        tok = self.next()
        upper = tok.text.upper() if tok.kind == "ident" else ""
        if upper == "NULL":
            return None
        if ftype == "datetime":
            if tok.kind != "datetime":
                raise SoqlError(
                    "INVALID_FIELD",
                    f"value of filter criterion for field '{field}' must be of type dateTime "
                    "and should not be enclosed in quotes",
                )
            return parse_datetime_literal(tok.text)
        if ftype == "date":
            if tok.kind != "date":
                raise SoqlError(
                    "INVALID_FIELD",
                    f"value of filter criterion for field '{field}' must be of type date "
                    "and should not be enclosed in quotes",
                )
            return date.fromisoformat(tok.text)
        if ftype == "boolean":
            if upper not in ("TRUE", "FALSE"):
                raise SoqlError(
                    "INVALID_FIELD",
                    f"value of filter criterion for field '{field}' must be of type boolean "
                    "and should not be enclosed in quotes",
                )
            return upper == "TRUE"
        if ftype == "currency":
            if tok.kind != "number":
                raise SoqlError(
                    "INVALID_FIELD",
                    f"value of filter criterion for field '{field}' must be of type double "
                    "and should not be enclosed in quotes",
                )
            return Decimal(tok.text)
        if tok.kind != "string":
            raise SoqlError("MALFORMED_QUERY", f"expected a quoted string for '{field}'")
        return tok.text[1:-1].replace("\\'", "'")

    def condition(self) -> Condition:
        field, ftype = self.field()
        if self.keyword("IN"):
            if self.next().text != "(":
                raise SoqlError("MALFORMED_QUERY", "expected ( after IN")
            values: list[str] = []
            while True:
                value = self.literal(field, ftype)
                if not isinstance(value, str):
                    raise SoqlError("MALFORMED_QUERY", "IN supports quoted strings only")
                values.append(value)
                sep = self.next().text
                if sep == ")":
                    break
                if sep != ",":
                    raise SoqlError("MALFORMED_QUERY", "expected , or ) in IN list")
            return Condition(field, "IN", tuple(values))
        op_tok = self.next()
        if op_tok.kind != "op":
            raise SoqlError("MALFORMED_QUERY", f"expected an operator, got {op_tok.text!r}")
        op = "!=" if op_tok.text == "<>" else op_tok.text
        return Condition(field, op, self.literal(field, ftype))


def parse(soql: str) -> Query:
    p = _Parser(tokenize(soql))
    p.expect_keyword("SELECT")
    fields: list[str] = []
    while True:
        fields.append(p.field()[0])
        tok = p.peek()
        if tok is not None and tok.text == ",":
            p.next()
            continue
        break
    p.expect_keyword("FROM")
    sobject_tok = p.next()
    if sobject_tok.text.lower() != "opportunity":
        raise SoqlError(
            "INVALID_TYPE", f"sObject type '{sobject_tok.text}' is not supported by the fake"
        )

    where: list[Condition] = []
    if p.keyword("WHERE"):
        where.append(p.condition())
        while p.keyword("AND"):
            where.append(p.condition())
        tok = p.peek()
        if tok is not None and tok.text.upper() == "OR":
            raise SoqlError("MALFORMED_QUERY", "the fake Salesforce supports AND only")

    order_by: list[OrderBy] = []
    if p.keyword("ORDER"):
        p.expect_keyword("BY")
        while True:
            field, _ = p.field()
            descending = False
            if p.keyword("DESC"):
                descending = True
            else:
                p.keyword("ASC")
            order_by.append(OrderBy(field, descending))
            tok = p.peek()
            if tok is not None and tok.text == ",":
                p.next()
                continue
            break

    limit: int | None = None
    if p.keyword("LIMIT"):
        tok = p.next()
        if tok.kind != "number" or not tok.text.isdigit():
            raise SoqlError("MALFORMED_QUERY", "LIMIT needs a whole number")
        limit = int(tok.text)

    leftover = p.peek()
    if leftover is not None:
        raise SoqlError("MALFORMED_QUERY", f"unexpected token {leftover.text!r}")
    return Query(tuple(fields), "Opportunity", tuple(where), tuple(order_by), limit)
