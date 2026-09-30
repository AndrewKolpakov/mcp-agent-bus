"""Print a compact timeline of a Claude Code session transcript (spike only).

Usage: python3 spike/transcript_view.py <transcript.jsonl>
Shows each record's time, type, role, origin-like fields and a short summary
of its content, so a channel event can be told apart from a typed prompt.
"""

from __future__ import annotations

import json
import sys


def summarize(content: object) -> str:
    if isinstance(content, str):
        return content[:120]
    parts = []
    if isinstance(content, list):
        for c in content:
            kind = c.get("type")
            if kind == "text":
                parts.append("T:" + c["text"][:90])
            elif kind == "tool_use":
                parts.append("USE:" + c["name"] + " " + json.dumps(c.get("input"))[:60])
            elif kind == "tool_result":
                parts.append("RES:" + json.dumps(c.get("content"))[:60])
    return " | ".join(parts)


for line in open(sys.argv[1], encoding="utf-8"):
    rec = json.loads(line)
    msg = rec.get("message") if isinstance(rec.get("message"), dict) else {}
    extra = {k: rec[k] for k in ("origin", "isMeta", "subtype", "userType") if k in rec}
    text = summarize(msg.get("content")) if msg else json.dumps(rec)[:120]
    print(rec.get("timestamp", "")[11:19], rec.get("type"), msg.get("role"), json.dumps(extra)[:120], text.replace("\n", " ")[:170])
