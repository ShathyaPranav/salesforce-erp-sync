"""Local stand-in for Lambda's SQS event source mapping. Test harness only.

On AWS, Lambda's own pollers read the queue, invoke the worker with a batch,
and delete the messages it reports as done. Locally nothing does that: the
worker image's Runtime Interface Emulator only answers direct invokes. This
script plays the event source mapping, following AWS's documented rules:

  * receive up to 10 messages, with all system attributes (the worker reads
    ApproximateReceiveCount for its backoff) and all message attributes;
  * invoke the function with a Lambda-shaped SQS event;
  * the whole batch counts as failed (nothing deleted) if the function
    errored, the response isn't JSON, or any batchItemFailures entry is
    empty, malformed or names a message that wasn't in the batch;
  * otherwise delete every message not listed in batchItemFailures;
  * never touch the visibility of failed messages: they come back when their
    visibility timeout (possibly changed by the worker) runs out, and SQS's
    redrive policy moves them to the DLQ after maxReceiveCount receives.

Control API on :9100, used by tests that need the queue to themselves:
  POST /pause   returns once any in-flight batch has finished
  POST /resume
  GET  /stats
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import boto3

QUEUE_NAME = os.environ.get("QUEUE_NAME", "relay-events")
FUNCTION_URL = os.environ.get(
    "FUNCTION_URL", "http://worker:8080/2015-03-31/functions/function/invocations"
)
REGION = os.environ.get("AWS_REGION", "us-east-1")
BATCH_SIZE = 10

paused = threading.Event()
cycle_lock = threading.Lock()
stats = {"batches": 0, "deleted": 0, "item_failures": 0, "batch_failures": 0}


def log(msg: str, **fields: Any) -> None:
    print(json.dumps({"service": "esm-pump", "msg": msg, **fields}), flush=True)


def to_record(msg: dict[str, Any], arn: str) -> dict[str, Any]:
    attrs = {
        name: {
            "stringValue": value.get("StringValue"),
            "binaryValue": value.get("BinaryValue"),
            "stringListValues": [],
            "binaryListValues": [],
            "dataType": value["DataType"],
        }
        for name, value in msg.get("MessageAttributes", {}).items()
    }
    return {
        "messageId": msg["MessageId"],
        "receiptHandle": msg["ReceiptHandle"],
        "body": msg["Body"],
        "attributes": msg.get("Attributes", {}),
        "messageAttributes": attrs,
        "md5OfBody": hashlib.md5(msg["Body"].encode(), usedforsecurity=False).hexdigest(),
        "eventSource": "aws:sqs",
        "eventSourceARN": arn,
        "awsRegion": REGION,
    }


def invoke(records: list[dict[str, Any]]) -> set[str] | None:
    """Return the failed message IDs, or None if the whole batch failed."""
    request = urllib.request.Request(  # noqa: S310 - fixed local URL
        FUNCTION_URL,
        data=json.dumps({"Records": records}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=900) as resp:  # noqa: S310
            function_error = resp.headers.get("X-Amz-Function-Error")
            raw = resp.read()
    except (urllib.error.URLError, TimeoutError) as exc:
        log("invoke failed", error=str(exc))
        return None
    if function_error:
        log("function error", kind=function_error, body=raw[:500].decode(errors="replace"))
        return None
    try:
        body = json.loads(raw) if raw.strip() else None
    except json.JSONDecodeError:
        log("response is not JSON", body=raw[:200].decode(errors="replace"))
        return None
    if isinstance(body, dict) and "errorType" in body:  # the emulator's unhandled-exception shape
        log("function error", body=body)
        return None
    if not body or body.get("batchItemFailures") is None:
        return set()  # null or empty response: the whole batch succeeded
    items = body["batchItemFailures"]
    batch_ids = {r["messageId"] for r in records}
    if not isinstance(items, list):
        return None
    failed: set[str] = set()
    for item in items:
        ident = item.get("itemIdentifier") if isinstance(item, dict) else None
        if not isinstance(ident, str) or not ident or ident not in batch_ids:
            log("invalid batchItemFailures entry; failing the whole batch", item=item)
            return None
        failed.add(ident)
    return failed


def cycle(sqs: Any, url: str, arn: str) -> None:
    messages = sqs.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=BATCH_SIZE,
        WaitTimeSeconds=1,
        MessageSystemAttributeNames=["All"],
        MessageAttributeNames=["All"],
    ).get("Messages", [])
    if not messages:
        return
    stats["batches"] += 1
    failed = invoke([to_record(m, arn) for m in messages])
    if failed is None:
        stats["batch_failures"] += 1
        return
    stats["item_failures"] += len(failed)
    done = [m for m in messages if m["MessageId"] not in failed]
    if done:
        sqs.delete_message_batch(
            QueueUrl=url,
            Entries=[
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(done)
            ],
        )
        stats["deleted"] += len(done)


def pump() -> None:
    sqs = boto3.client("sqs", region_name=REGION)
    while True:
        try:
            url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
            arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"][
                "QueueArn"
            ]
            break
        except Exception as exc:
            log("waiting for queue", error=str(exc))
            time.sleep(1)
    log("polling", queue=url, function=FUNCTION_URL)
    while True:
        if paused.is_set():
            time.sleep(0.05)
            continue
        with cycle_lock:
            if paused.is_set():
                continue
            try:
                cycle(sqs, url, arn)
            except Exception as exc:
                log("cycle error", error=repr(exc))
                time.sleep(1)


class Control(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _reply(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path == "/pause":
            paused.set()
            with cycle_lock:  # wait for the in-flight batch, if any
                pass
            self._reply({"paused": True})
        elif self.path == "/resume":
            paused.clear()
            self._reply({"paused": False})
        else:
            self.send_error(404)

    def do_GET(self) -> None:
        self._reply({"paused": paused.is_set(), **stats})


if __name__ == "__main__":
    threading.Thread(target=pump, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 9100), Control).serve_forever()  # noqa: S104 - in a container
