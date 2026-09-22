"""One place that decides which AWS the scripts talk to.

    --local          the docker compose emulator (LocalStack), dummy credentials
    --profile NAME   your real account via `aws login --profile NAME`
    (neither)        the ambient credentials, e.g. GitHub Actions' OIDC role

Real AWS is only ever reached with an explicit --profile or in CI.
"""

from __future__ import annotations

import os
from typing import Any

import boto3

LOCAL_ENDPOINT = "http://127.0.0.1:4566"


def session(local: bool, profile: str | None, region: str = "us-east-1") -> Any:
    if local:
        return boto3.session.Session(  # the emulator accepts any credentials
            aws_access_key_id="test",
            aws_secret_access_key="test",  # noqa: S106 - not a secret
            region_name=region,
        )
    return boto3.session.Session(profile_name=profile, region_name=region)


def client(sess: Any, service: str, local: bool) -> Any:
    return sess.client(service, endpoint_url=LOCAL_ENDPOINT if local else None)


def stack_outputs(sess: Any, stack: str) -> dict[str, str]:
    cfn = sess.client("cloudformation")
    stacks = cfn.describe_stacks(StackName=stack)["Stacks"]
    return {o["OutputKey"]: o["OutputValue"] for o in stacks[0].get("Outputs", [])}


def refuse_emulator_env() -> None:
    """A stray AWS_ENDPOINT_URL would silently send 'real' calls to the emulator."""
    if os.environ.get("AWS_ENDPOINT_URL"):
        raise SystemExit("AWS_ENDPOINT_URL is set; unset it before talking to real AWS.")
