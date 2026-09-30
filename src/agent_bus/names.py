"""Names Claude Code derives from the plugin manifest.

`.claude-plugin/plugin.json` declares plugin `agent-bus` with MCP server key
`bus`. Claude Code then tags channel events `source="plugin:agent-bus:bus"`
and names the tools `mcp__plugin_agent-bus_bus__<tool>` (both observed in the
stage-0 live runs). A test pins these constants to the manifest.
"""

PLUGIN_NAME = "agent-bus"
SERVER_KEY = "bus"
CHANNEL_SOURCE = f"plugin:{PLUGIN_NAME}:{SERVER_KEY}"
CHANNEL_PROMPT_PREFIX = f'<channel source="{CHANNEL_SOURCE}"'
TOOL_PREFIX = f"mcp__plugin_{PLUGIN_NAME}_{SERVER_KEY}__"
PERMISSION_RULE = f"mcp__plugin_{PLUGIN_NAME}_{SERVER_KEY}"
