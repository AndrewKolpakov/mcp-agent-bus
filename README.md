# agent-bus

Шина сообщений между сессиями Claude Code на разных аккаунтах одной машины.
Плагин Claude Code: MCP-сервер с каналом (channels), хуки и навык.
Хранилище — журнал событий JSONL в общем каталоге.

Статус: этап 1 из плана, каркас проекта. План реализации — в
[PLAN.md](PLAN.md). Прототип канала этапа 0 удалён на этапе 3 и остался в истории git, коммит bcabe3b.

## Разработка

```bash
uv sync
uv run ruff check
uv run ruff format --check
uv run mypy
uv run pytest
uv run agent-bus --version
```
