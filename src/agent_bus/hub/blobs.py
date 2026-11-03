"""Attachment bytes on the hub: an S3 bucket reached by presigned URLs.

The Lambda never carries a file through API Gateway (6 MB payload cap, and
base64 on top): a sender gets a presigned PUT, a recipient a presigned GET,
and the hub reads an object only to check it before the message that names
it is written. Keys are `<sender>/<sha256>`, so an agent can only attach
what it uploaded itself; knowing a hash is not enough to obtain the file.
The bucket's lifecycle rule deletes objects after a retention period
(hub/template.yaml); a message outlives its attachments.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Protocol

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

URL_SECONDS = 300


def blob_key(agent: str, sha256: str) -> str:
    return f"{agent}/{sha256}"


class Blobs(Protocol):
    def upload_url(self, key: str) -> str: ...

    def download_url(self, key: str) -> str: ...

    def read(self, key: str, max_bytes: int) -> bytes | None:
        """The object's bytes; None when there is none or it is larger than `max_bytes`."""
        ...


class S3Blobs:
    def __init__(self, bucket: str, client: S3Client | None = None) -> None:
        self.bucket = bucket
        if client is None:
            # Regional virtual-host URLs: the global endpoint answers a new
            # bucket outside us-east-1 with a redirect for up to a day.
            region = os.environ.get("AWS_REGION", "us-east-1")
            client = boto3.client(
                "s3",
                region_name=region,
                endpoint_url=f"https://s3.{region}.amazonaws.com",
                config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}),
            )
        self.client: S3Client = client

    def upload_url(self, key: str) -> str:
        url: str = self.client.generate_presigned_url(
            "put_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=URL_SECONDS
        )
        return url

    def download_url(self, key: str) -> str:
        url: str = self.client.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=URL_SECONDS
        )
        return url

    def read(self, key: str, max_bytes: int) -> bytes | None:
        try:
            head = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        if head["ContentLength"] > max_bytes:
            return None
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
