"""Turn the live-run logs into answers for stage-0 questions Q1, Q3-Q6.

Run after a live session: python3 spike/report.py
Reads spike/spike.log (every JSON-RPC message the spike server saw or sent)
and spike/live/hooks.log (every UserPromptSubmit / Stop hook input).
"""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
SPIKE_LOG = HERE / "spike.log"
HOOKS_LOG = HERE / "live" / "hooks.log"
CODEWORD = "HELIOTROPE"


def read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


spike = read(SPIKE_LOG)
hooks = read(HOOKS_LOG)
print(f"spike.log: {len(spike)} entries, hooks.log: {len(hooks)} entries\n")

starts = [e for e in spike if e["dir"] == "start"]
print(f"server processes started: {len(starts)}")

inits = [e["msg"] for e in spike if e["dir"] == "in" and e["msg"].get("method") == "initialize"]
for init in inits:
    params = init.get("params", {})
    print("Q1 initialize from client:")
    print("   protocolVersion:", params.get("protocolVersion"))
    print("   clientInfo:", params.get("clientInfo"))
    print("   client capabilities:", json.dumps(params.get("capabilities")))
init_results = [e["msg"] for e in spike if e["dir"] == "out" and "result" in e["msg"] and "protocolVersion" in e["msg"]["result"]]
for res in init_results:
    print("   negotiated:", res["result"]["protocolVersion"])

channel_out = [e for e in spike if e["dir"] == "out" and e["msg"].get("method") == "notifications/claude/channel"]
tool_calls = [e for e in spike if e["dir"] == "tool"]
print(f"\nchannel notifications sent: {len(channel_out)}")
for e in channel_out:
    print("   ", e["t"], json.dumps(e["msg"]["params"]))
print(f"reply tool calls: {len(tool_calls)}")
for e in tool_calls:
    print("   ", e["t"], json.dumps(e["msg"]))

print("\nQ4 wake: a reply after a channel event with no human prompt in between")
prompts = [h for h in hooks if h["argv"][:1] == ["UserPromptSubmit"]]
for note in channel_out:
    replies = [c for c in tool_calls if c["t"] > note["t"]]
    humans = [p for p in prompts if note["t"] < p["t"] < (replies[0]["t"] if replies else float("inf"))]
    channel_prompts = [p for p in humans if "<channel" in json.dumps(p["input"])]
    typed = [p for p in humans if p not in channel_prompts]
    verdict = "woke" if replies and not typed else "no autonomous reply" if not replies else "human typed in between"
    print(f"    event {note['msg']['params']['meta'].get('msg_id')}: {verdict}")

print("\nQ5 instructions reached the model:", any(CODEWORD in json.dumps(c["msg"]) for c in tool_calls))

print("\nQ6 UserPromptSubmit fired for channel events:")
if not prompts:
    print("    no UserPromptSubmit entries at all")
for p in prompts:
    inp = p["input"]
    text = inp.get("prompt") or inp.get("user_input") or ""
    print(f"    {p['t']} channel={'<channel' in text} keys={sorted(inp)} text={text[:120]!r}")

print("\nQ3 names: source attribute and tool names as the model saw them")
for p in prompts:
    text = p["input"].get("prompt") or p["input"].get("user_input") or ""
    if "<channel" in text:
        print("   ", text.split(">", 1)[0] + ">")
for e in tool_calls:
    print("    tool call reached server as:", e["msg"].get("name"))
