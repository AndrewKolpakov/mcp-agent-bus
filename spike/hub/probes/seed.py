"""Create test agents: raw tokens go to SPIKE_STATE, only SHA-256 to the table.

Usage: seed.py <stack-name> <agent>...
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sys
from pathlib import Path

import boto3

stack, agents = sys.argv[1], sys.argv[2:]
cfn = boto3.client("cloudformation")
outputs = {
    o["OutputKey"]: o["OutputValue"]
    for o in cfn.describe_stacks(StackName=stack)["Stacks"][0]["Outputs"]
}
table = outputs["TableName"]
ddb = boto3.client("dynamodb")

path = Path(os.environ["SPIKE_STATE"])
current = json.loads(path.read_text()) if path.exists() else {"tokens": {}}
current.update(http_url=outputs["HttpUrl"], ws_url=outputs["WsUrl"], table=table)
for agent in agents:
    token = "tok_" + secrets.token_urlsafe(32)
    digest = hashlib.sha256(token.encode()).hexdigest()
    ddb.put_item(
        TableName=table,
        Item={"pk": {"S": f"TOKEN#{digest}"}, "sk": {"S": "AGENT"}, "agent": {"S": agent}},
    )
    ddb.put_item(
        TableName=table,
        Item={"pk": {"S": f"AGENT#{agent}"}, "sk": {"S": "PROFILE"}, "token_hash": {"S": digest}},
    )
    current["tokens"][agent] = token
path.write_text(json.dumps(current, indent=2))
path.chmod(0o600)
print(f"seeded {agents} into {table}; state in {path}")
