"""Turn the live-run logs into answers for stage-0 questions Q1, Q3-Q6.

Run after a live session: python3 spike/report.py
Reads spike/spike.log (every JSON-RPC message each spike server process saw
or sent), spike/live/hooks.log (hook inputs from the project hooks in live/
and the plugin hooks in hooks/hooks.json) and the session transcripts those
hook inputs point to.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
SPIKE_LOG = HERE / "spike.log"
HOOKS_LOG = HERE / "live" / "hooks.log"
CODEWORD = "HELIOTROPE"
CHANNEL_PREFIX = "<channel source="
TAG_IN_TRANSCRIPT = re.compile(r'<channel source=\\"[^>]*>')


def read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


@dataclass
class Process:
    pid: int
    started: float
    kind: str
    protocol: str | None = None
    events: list[dict] = field(default_factory=list)
    replies: list[dict] = field(default_factory=list)


spike = read(SPIKE_LOG)
hooks = read(HOOKS_LOG)
print(f"spike.log: {len(spike)} entries, hooks.log: {len(hooks)} entries")

processes: list[Process] = []
for e in spike:
    if e["dir"] == "start":
        env = e["msg"].get("mcp_env", [])
        kind = "plugin" if "CLAUDE_PLUGIN_ROOT" in env else "bare"
        processes.append(Process(pid=e["msg"]["pid"], started=e["t"], kind=kind))
        continue
    if not processes:
        continue
    proc, msg = processes[-1], e["msg"]
    if e["dir"] == "in" and msg.get("method") == "initialize":
        proc.protocol = msg["params"].get("protocolVersion")
    elif e["dir"] == "out" and msg.get("method") == "notifications/claude/channel":
        proc.events.append(e)
    elif e["dir"] == "tool":
        proc.replies.append(e)

# A process only logs after its own start line, but two live processes can
# interleave; entries are attributed to the latest start, which is exact for
# the sequential runs the runbook asks for.
prompts = [h for h in hooks if h["argv"][:1] == ["UserPromptSubmit"]]
channel_prompts = [p for p in prompts if (p["input"].get("prompt") or "").startswith(CHANNEL_PREFIX)]
human_prompts = [p for p in prompts if p not in channel_prompts]
permission = [h for h in hooks if h["argv"][:1] == ["PermissionRequest"]]
pretool = [h for h in hooks if h["argv"][:1] == ["PreToolUse"]]

print("\nServer processes:")
for p in processes:
    print(f"  pid {p.pid} {p.kind:6} protocol={p.protocol} events={len(p.events)} replies={len(p.replies)}")

print("\nQ1 protocol negotiated with Claude Code:", sorted({p.protocol for p in processes if p.protocol}))

print("\nQ4 wake: each channel event and what followed it")
for p in processes:
    for ev in p.events:
        t = ev["t"]
        content = ev["msg"]["params"]["content"]
        reply = next((r for r in p.replies if r["t"] > t), None)
        end = reply["t"] if reply else float("inf")
        typed = [h for h in human_prompts if t < h["t"] < end]
        asked = [h for h in permission if t < h["t"] < end]
        if reply is None:
            verdict = "no reply from the model"
        elif typed:
            verdict = f"reply after {reply['t'] - t:.0f}s, but a human prompt came first"
        else:
            verdict = f"woke: reply after {reply['t'] - t:.0f}s with no human prompt"
            if asked:
                verdict += f" (waited on a permission dialog opened {asked[0]['t'] - t:.0f}s after the event)"
        print(f"  {p.kind:6} pid {p.pid} {content!r}: {verdict}")

print("\nQ5 instructions reached the model:", any(CODEWORD in json.dumps(r["msg"]) for p in processes for r in p.replies))

print("\nQ6 UserPromptSubmit for channel events:")
print(f"  channel-shaped prompts seen by the hook: {len(channel_prompts)}")
print(f"  human prompts seen by the hook: {len(human_prompts)}")
if channel_prompts:
    print("  the hook fires for channel events; skip prompts starting with", repr(CHANNEL_PREFIX))
    for p in channel_prompts:
        print("   ", (p["input"].get("prompt") or "")[:120].replace("\n", " "))

print("\nQ4 from transcripts: what the session did after each channel record")
transcripts = sorted({h["input"]["transcript_path"] for h in hooks if h["input"].get("transcript_path")})
transcripts += [a for a in __import__("sys").argv[1:] if a not in transcripts]
for path in transcripts:
    transcript = Path(path)
    if not transcript.exists():
        continue
    records = [json.loads(line) for line in transcript.read_text(errors="replace").splitlines() if line.strip()]
    for i, rec in enumerate(records):
        if (rec.get("origin") or {}).get("kind") != "channel":
            continue
        tag = (rec.get("message") or {}).get("content")
        print(f"  {transcript.name[:8]} {rec.get('timestamp', '')[11:19]} channel record: {str(tag)[:70]!r}")
        for later in records[i + 1:]:
            kind = (later.get("origin") or {}).get("kind")
            msg = later.get("message") if isinstance(later.get("message"), dict) else {}
            if kind in ("human", "channel"):
                print(f"    {later.get('timestamp', '')[11:19]} next {kind} record arrives; the turn above ended")
                break
            if later.get("type") == "assistant":
                for part in msg.get("content") or []:
                    if part.get("type") == "tool_use":
                        print(f"    {later.get('timestamp', '')[11:19]} tool_use {part['name']}")
                    elif part.get("type") == "text" and part["text"].strip():
                        print(f"    {later.get('timestamp', '')[11:19]} text {part['text'][:70]!r}")

print("\nQ3 names:")
names = sorted({h["input"].get("tool_name", "?") for h in pretool + permission})
print("  MCP tool names from PreToolUse / PermissionRequest:", names or "none logged")
tags: set[str] = set()
for path in transcripts:
    transcript = Path(path)
    if transcript.exists():
        for tag in TAG_IN_TRANSCRIPT.findall(transcript.read_text(errors="replace")):
            if '\\"...\\"' not in tag:  # the instructions text quotes a placeholder tag
                tags.add(tag.replace('\\"', '"'))
print("  <channel> opening tags in transcripts:", sorted(tags) or "none found")
