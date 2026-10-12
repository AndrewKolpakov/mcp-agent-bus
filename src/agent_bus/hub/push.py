"""Posting to WebSocket connections through the API Gateway management API."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Protocol

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_apigatewaymanagementapi import ApiGatewayManagementApiClient

log = logging.getLogger(__name__)


class Pusher(Protocol):
    def post(self, endpoint: str, conn: str, payload: dict[str, Any]) -> bool:
        """Send `payload`; False only when the connection is gone for good."""
        ...


class ApiGatewayPusher:
    def __init__(self) -> None:
        self._clients: dict[str, ApiGatewayManagementApiClient] = {}

    def _client(self, endpoint: str) -> ApiGatewayManagementApiClient:
        if endpoint not in self._clients:
            self._clients[endpoint] = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint)
        return self._clients[endpoint]

    def post(self, endpoint: str, conn: str, payload: dict[str, Any]) -> bool:
        try:
            self._client(endpoint).post_to_connection(
                ConnectionId=conn, Data=json.dumps(payload).encode()
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "GoneException":
                return False
            # A failed push costs only latency: the client catches up by seq
            # on its next read, so the request that caused it still succeeds.
            log.warning("push to %s failed: %s", conn, code)
        return True
