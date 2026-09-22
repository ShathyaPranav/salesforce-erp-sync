"""Build the three Lambda images and deploy the app stack from your laptop.

    python scripts/deploy.py --profile relay            # shows the change set, asks to confirm
    python scripts/deploy.py --profile relay --poller DISABLED

Reads SF_LOGIN_URL, SF_CLIENT_ID and SF_API_VERSION from .env, and the image
repository from the relay-bootstrap stack's outputs. CI does the same with
GitHub variables (.github/workflows/ci.yml), so both paths deploy identically.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.aws_session import refuse_emulator_env, session, stack_outputs

ROOT = Path(__file__).resolve().parents[1]


def setting(name: str) -> str:
    value = os.environ.get(name, "")
    if not value or value.startswith("<"):
        sys.exit(f"{name} is not set in .env")
    return value


def optional(name: str) -> str:
    value = os.environ.get(name, "")
    return "" if value.startswith("<") else value


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--poller", choices=["ENABLED", "DISABLED"], default="ENABLED")
    parser.add_argument("--no-confirm", action="store_true", help="skip the change-set prompt")
    args = parser.parse_args()
    refuse_emulator_env()
    load_dotenv(ROOT / ".env")

    sam = shutil.which("sam")
    if sam is None:
        sys.exit("AWS SAM CLI not found: winget install -e --id Amazon.SAM-CLI")
    repo = stack_outputs(session(False, args.profile), "relay-bootstrap")["ImageRepositoryUri"]

    overrides = [
        f"SalesforceLoginUrl={setting('SF_LOGIN_URL')}",
        f"SalesforceClientId={setting('SF_CLIENT_ID')}",
        f"SalesforceApiVersion={os.environ.get('SF_API_VERSION', 'v66.0')}",
        f"PollerState={args.poller}",
        f"AlertEmail={optional('ALERT_EMAIL')}",
    ]
    subprocess.run([sam, "build"], cwd=ROOT, check=True)  # noqa: S603 - fixed argv
    deploy = [
        sam, "deploy",
        "--profile", args.profile,
        "--image-repository", repo,
        "--parameter-overrides", *overrides,
    ]  # fmt: skip
    if args.no_confirm:
        deploy.append("--no-confirm-changeset")
    subprocess.run(deploy, cwd=ROOT, check=True)  # noqa: S603 - fixed argv


if __name__ == "__main__":
    main()
