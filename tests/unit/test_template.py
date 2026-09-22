"""Guard rails on template.yaml: the cost rules from CLAUDE.md and the settings
the failure handling depends on. A template change that breaks one of these
fails CI before it can reach AWS."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


class CfnLoader(yaml.SafeLoader):
    """Reads CloudFormation's short-form tags (!Ref, !GetAtt, !Sub ...) as plain dicts."""


def _tag(loader: yaml.SafeLoader, suffix: str, node: yaml.Node) -> dict[str, Any]:
    if isinstance(node, yaml.ScalarNode):
        value: Any = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)  # type: ignore[arg-type]
    return {f"!{suffix}": value}


CfnLoader.add_multi_constructor("!", _tag)


def load(path: str) -> dict[str, Any]:
    doc: dict[str, Any] = yaml.load((ROOT / path).read_text(encoding="utf-8"), Loader=CfnLoader)  # noqa: S506
    return doc


@pytest.fixture(scope="module")
def resources() -> dict[str, Any]:
    res: dict[str, Any] = load("template.yaml")["Resources"]
    return res


def of_type(resources: dict[str, Any], kind: str) -> dict[str, Any]:
    return {k: v for k, v in resources.items() if v["Type"] == kind}


def test_nothing_that_bills_by_the_hour(resources):
    banned = ("AWS::EC2::", "AWS::RDS::", "AWS::ECS::", "AWS::EKS::", "AWS::ElastiCache::")
    assert not [k for k, v in resources.items() if v["Type"].startswith(banned)]
    for name, fn in of_type(resources, "AWS::Serverless::Function").items():
        assert "VpcConfig" not in fn["Properties"], f"{name} must not be in a VPC (NAT costs)"


def test_every_log_group_keeps_7_days_and_every_function_uses_one(resources):
    groups = of_type(resources, "AWS::Logs::LogGroup")
    assert groups
    assert all(g["Properties"]["RetentionInDays"] == 7 for g in groups.values())
    for name, fn in of_type(resources, "AWS::Serverless::Function").items():
        ref = fn["Properties"]["LoggingConfig"]["LogGroup"]
        assert ref["!Ref"] in groups, f"{name} logs to an unmanaged log group"


def test_worker_reports_partial_batch_failures(resources):
    event = resources["WorkerFunction"]["Properties"]["Events"]["Queue"]["Properties"]
    assert event["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
    assert event["BatchSize"] <= 10


def test_queue_settings_match_the_failure_design(resources):
    queue = resources["EventsQueue"]["Properties"]
    dlq = resources["EventsDLQ"]["Properties"]
    worker_timeout = resources["WorkerFunction"]["Properties"]["Timeout"]
    assert queue["VisibilityTimeout"] >= 6 * worker_timeout
    assert queue["RedrivePolicy"]["maxReceiveCount"] == 5
    assert dlq["MessageRetentionPeriod"] > queue["MessageRetentionPeriod"]


def test_no_reserved_concurrency_and_no_async_retries_for_the_poller(resources):
    for name, fn in of_type(resources, "AWS::Serverless::Function").items():
        assert "ReservedConcurrentExecutions" not in fn["Properties"], name
    ingest = resources["IngestFunction"]["Properties"]
    assert ingest["EventInvokeConfig"]["MaximumRetryAttempts"] == 0
    assert ingest["Events"]["Poll"]["Properties"]["RetryPolicy"]["MaximumRetryAttempts"] == 0


def test_stack_owns_no_secrets_and_no_runtime_state(resources):
    for param in of_type(resources, "AWS::SSM::Parameter").values():
        props = param["Properties"]
        assert props["Type"] == "String"
        name = str(props["Name"])
        assert "client_secret" not in name and "watermark" not in name and "chaos" not in name


def test_dynamodb_stays_inside_the_always_free_capacity(resources):
    tables = of_type(resources, "AWS::DynamoDB::Table").values()
    reads = sum(t["Properties"]["ProvisionedThroughput"]["ReadCapacityUnits"] for t in tables)
    writes = sum(t["Properties"]["ProvisionedThroughput"]["WriteCapacityUnits"] for t in tables)
    assert reads <= 25 and writes <= 25


def test_ecr_lifecycle_never_expires_by_age_alone():
    policy = load("infra/bootstrap.yaml")["Resources"]["ImageRepository"]["Properties"]
    text = policy["LifecyclePolicy"]["LifecyclePolicyText"]
    assert '"imageCountMoreThan"' in text
    assert '"tagStatus": "untagged", "countType": "sinceImagePushed"' in text
