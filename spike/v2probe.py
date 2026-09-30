# /// script
# requires-python = ">=3.12"
# dependencies = ["mcp==2.2.0"]
# ///
"""Answer one question: which protocol revision does an mcp 2.x server negotiate?"""

from mcp.server.mcpserver import MCPServer

mcp = MCPServer("probe", instructions="probe")


@mcp.tool()
def ping() -> str:
    return "pong"


mcp.run()
