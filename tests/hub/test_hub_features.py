"""Secrets, status, activity and attachments on the hub (moto table, fake bucket)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from hubfakes import Clock, FakeBlobs, blobs, clock, hub, pusher, table, tokens
from moto import mock_aws

__all__ = ["blobs", "clock", "hub", "pusher", "table", "tokens"]  # fixtures

from agent_bus.hub.app import Hub, HubError
from agent_bus.hub.blobs import S3Blobs, blob_key
from agent_bus.models import MAX_ATTACHMENT_BYTES

AWS_ID = "AKIA" + "IOSFODNN7ABCDEFG"


def send(hub: Hub, sender: str, to: str, body: str = "hi", **extra: Any) -> dict[str, Any]:
    reply = hub.post_event(sender, {"to": to, "body": body, **extra})
    assert reply.status == 200
    record: dict[str, Any] = reply.body
    return record


def refused(call: Any, status: int, match: str) -> None:
    with pytest.raises(HubError, match=match) as info:
        call()
    assert info.value.status == status


def peer(hub: Hub, viewer: str, name: str) -> dict[str, Any]:
    found: dict[str, Any] = {p["name"]: p for p in hub.peers(viewer).body}[name]
    return found


def go_live(hub: Hub, agent: str) -> None:
    hub.connect(agent, f"c-{agent}", "https://ws.example/dev")
    hub.put_session(agent, {"session": "s", "connection": f"c-{agent}"})


# --- secrets ------------------------------------------------------------------


def test_the_hub_refuses_a_secret_a_client_did_not_check(hub: Hub) -> None:
    refused(lambda: send(hub, "alice", "bob", f"key {AWS_ID}"), 422, "line 1: AWS access key id")
    assert hub.events("bob", {}).body == ""


# --- status -------------------------------------------------------------------


def test_peers_show_last_sent_and_open_tasks_until_acked(hub: Hub, clock: Clock) -> None:
    go_live(hub, "bob")
    task = send(hub, "alice", "bob", "review", kind="task")
    clock.t += 60
    send(hub, "bob", "alice", "question")
    bob = peer(hub, "alice", "bob")
    assert bob["live"]
    assert bob["state"] == "working"  # no hooks yet: an open task reads as working
    assert bob["last_sent"].startswith("2027-01-15")
    assert [(t["msg_id"], t["from"], t["thread"]) for t in bob["tasks"]] == [
        (task["id"], "alice", task["thread"])
    ]
    hub.post_event("bob", {"type": "ack", "id": task["id"]})
    assert peer(hub, "alice", "bob")["tasks"] == []
    assert peer(hub, "alice", "bob")["state"] == "idle"


def test_await_human_waits_for_a_human_turn(hub: Hub, clock: Clock) -> None:
    go_live(hub, "bob")
    send(hub, "bob", "alice", "stuck, need a human", kind="await_human")
    assert peer(hub, "alice", "bob")["state"] == "waiting_human"
    clock.t += 30
    hub.post_event("bob", {"type": "human_turn", "session": "s"})
    assert peer(hub, "alice", "bob")["state"] == "idle"


def test_activity_reports_keep_the_latest(hub: Hub) -> None:
    go_live(hub, "bob")
    late = {"state": "idle", "at": "2027-01-15T08:00:10+00:00", "session": "s"}
    early = {"state": "working", "at": "2027-01-15T08:00:05+00:00", "session": "s"}
    assert hub.put_activity("bob", late).body == {"stored": True}
    assert hub.put_activity("bob", early).body == {"stored": False}
    bob = peer(hub, "alice", "bob")
    assert (bob["state"], bob["since"]) == ("idle", "2027-01-15T08:00:10+00:00")


def test_activity_reports_are_checked(hub: Hub) -> None:
    refused(lambda: hub.put_activity("bob", {"state": "x", "at": "", "session": "s"}), 400, "state")
    refused(
        lambda: hub.put_activity("bob", {"state": "idle", "at": "nope", "session": "s"}), 400, "ISO"
    )
    naive = {"state": "idle", "at": "2027-01-15T08:00:00", "session": "s"}
    refused(lambda: hub.put_activity("bob", naive), 400, "UTC offset")


def test_a_clock_running_ahead_cannot_pin_a_state(hub: Hub, clock: Clock) -> None:
    go_live(hub, "bob")
    hub.put_activity("bob", {"state": "working", "at": "2030-01-01T00:00:00+00:00", "session": "s"})
    clock.t += 600
    now = hub.now().isoformat()
    assert hub.put_activity("bob", {"state": "idle", "at": now, "session": "s"}).body["stored"]


def test_a_removed_agent_takes_its_status_along(hub: Hub) -> None:
    send(hub, "alice", "bob", "task", kind="task")
    hub.table.remove_agent("bob")
    assert hub.table.status_of("bob")[1].tasks == ()


# --- attachments ----------------------------------------------------------------


def upload(
    hub: Hub, blobs: FakeBlobs, agent: str, data: bytes, name: str = "f.py"
) -> dict[str, Any]:
    sha = hashlib.sha256(data).hexdigest()
    url = hub.upload_attachment(agent, {"sha256": sha, "size": len(data)}).body["url"]
    assert f"/{agent}/{sha}" in url
    blobs.objects[blob_key(agent, sha)] = data
    return {"sha256": sha, "name": name, "size": len(data)}


def test_a_message_names_only_files_its_sender_uploaded(hub: Hub, blobs: FakeBlobs) -> None:
    mine = upload(hub, blobs, "alice", b"print(1)\n")
    record = send(hub, "alice", "bob", "review", attachments=[mine])
    assert record["attachments"] == [mine]
    assert hub.table.message(record["id"]).attachments == {mine["sha256"]}  # type: ignore[union-attr]

    dave = upload(hub, blobs, "dave", b"dave's file\n")
    refused(lambda: send(hub, "alice", "bob", "x", attachments=[dave]), 400, "was not uploaded")


def test_a_file_that_differs_from_its_hash_is_refused(hub: Hub, blobs: FakeBlobs) -> None:
    named = upload(hub, blobs, "alice", b"original\n")
    blobs.objects[blob_key("alice", named["sha256"])] = b"swapped\n"
    refused(lambda: send(hub, "alice", "bob", "x", attachments=[named]), 400, "does not match")


def test_an_attached_secret_is_refused(hub: Hub, blobs: FakeBlobs) -> None:
    named = upload(hub, blobs, "alice", f"key = {AWS_ID}\n".encode(), "keys.txt")
    refused(
        lambda: send(hub, "alice", "bob", "x", attachments=[named]),
        422,
        "attachment keys.txt line 1",
    )


def test_bad_attachment_requests_are_refused(hub: Hub, blobs: FakeBlobs) -> None:
    refused(lambda: hub.upload_attachment("alice", {"sha256": "x", "size": 1}), 400, "sha256")
    big = {"sha256": "0" * 64, "size": MAX_ATTACHMENT_BYTES + 1}
    refused(lambda: hub.upload_attachment("alice", big), 400, "size")
    path = {"sha256": "0" * 64, "name": "../etc/passwd", "size": 1}
    refused(lambda: send(hub, "alice", "bob", "x", attachments=[path]), 400, "bad attachment")


def test_only_a_party_gets_a_download_url(hub: Hub, blobs: FakeBlobs) -> None:
    named = upload(hub, blobs, "alice", b"data\n")
    record = send(hub, "alice", "bob", "x", attachments=[named])
    query = {"msg": record["id"], "sha256": named["sha256"]}
    url = hub.download_attachment("bob", query).body["url"]
    assert f"/alice/{named['sha256']}" in url
    refused(lambda: hub.download_attachment("dave", query), 404, "unknown message id")
    other = {"msg": record["id"], "sha256": "1" * 64}
    refused(lambda: hub.download_attachment("bob", other), 404, "no attachment")


def test_a_hub_without_a_bucket_says_so(hub: Hub) -> None:
    hub.blobs = None
    refused(
        lambda: hub.upload_attachment("alice", {"sha256": "0" * 64, "size": 1}), 400, "no attach"
    )


@pytest.fixture
def bucket() -> Iterator[S3Blobs]:
    with mock_aws():
        client = boto3.client("s3", region_name="eu-central-1")
        client.create_bucket(
            Bucket="att", CreateBucketConfiguration={"LocationConstraint": "eu-central-1"}
        )
        yield S3Blobs("att", client)


def test_s3_blobs_read_presign_and_cap(bucket: S3Blobs) -> None:
    bucket.client.put_object(Bucket="att", Key="alice/abc", Body=b"12345")
    assert bucket.read("alice/abc", 10) == b"12345"
    assert bucket.read("alice/abc", 4) is None
    assert bucket.read("alice/missing", 10) is None
    assert "alice/abc" in bucket.upload_url("alice/abc")
    assert "X-Amz-Signature" in bucket.download_url("alice/abc")
