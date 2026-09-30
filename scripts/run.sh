#!/bin/sh
# Single entry point for the plugin's MCP server and hooks.
#
# Stdout belongs to the caller: JSON-RPC for the server, hook JSON for hooks.
# Nothing in this script writes to it, and uv runs with --quiet.
set -eu

# Hooks of sessions that did not opt in to the bus exit before uv starts, so
# the other sessions of an account pay no process start-up per turn.
if [ "${1:-}" = "hook" ] && [ "${AGENT_BUS_SESSION:-}" != "1" ]; then
    exit 0
fi

ROOT="${CLAUDE_PLUGIN_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"

UV="$(command -v uv 2>/dev/null || true)"
for candidate in "${HOME:-}/.local/bin/uv" /opt/homebrew/bin/uv /usr/local/bin/uv; do
    if [ -n "$UV" ]; then
        break
    fi
    if [ -x "$candidate" ]; then
        UV="$candidate"
    fi
done
if [ -z "$UV" ]; then
    echo "agent-bus: uv not found on PATH or in ~/.local/bin, /opt/homebrew/bin, /usr/local/bin" >&2
    exit 1
fi

# The venv lives outside the plugin root, which moves on every plugin update.
# CLAUDE_PLUGIN_DATA is per account; the fallback serves a human running this
# script from a shell.
export UV_PROJECT_ENVIRONMENT="${CLAUDE_PLUGIN_DATA:-$ROOT/.plugin-data}/venv"
exec "$UV" run --quiet --frozen --no-dev --project "$ROOT" agent-bus "$@"
