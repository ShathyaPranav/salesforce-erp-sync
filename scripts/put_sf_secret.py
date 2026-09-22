"""Store the Salesforce client secret in SSM Parameter Store as a SecureString.

    python scripts/put_sf_secret.py --profile relay

It reads SF_CLIENT_SECRET from .env and writes /relay/salesforce/client_secret,
encrypted with the AWS-managed aws/ssm key (no monthly key fee). The secret
never appears on a command line, in shell history, or in this script's output.
CloudFormation can't create SecureString parameters, which is why this is a
separate one-off step instead of part of template.yaml.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.aws_session import refuse_emulator_env, session


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--name", default="/relay/salesforce/client_secret")
    args = parser.parse_args()
    refuse_emulator_env()

    load_dotenv()
    secret = os.environ.get("SF_CLIENT_SECRET", "")
    if not secret or secret.startswith("<"):
        sys.exit("SF_CLIENT_SECRET is not set in .env")

    ssm = session(False, args.profile).client("ssm")
    result = ssm.put_parameter(
        Name=args.name, Value=secret, Type="SecureString", Overwrite=True, Tier="Standard"
    )
    print(f"stored {args.name} as SecureString (version {result['Version']})")


if __name__ == "__main__":
    main()
