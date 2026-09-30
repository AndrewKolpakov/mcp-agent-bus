# Этап 0: прототип канала

Одноразовый прототип. Отвечает на вопросы Q1–Q6 из [PLAN.md](../PLAN.md)
и удаляется после этапа 0.

| Файл | Назначение |
| :- | :- |
| `channel_spike.py` | Stdio MCP-сервер с `claude/channel`. Каждая новая строка `inbox.txt` становится событием канала. Весь JSON-RPC пишется в `spike.log` |
| `drive.py` | Проверка без Claude Code: рукопожатие, инструменты, событие канала |
| `v2probe.py` | Какую ревизию протокола согласует сервер на `mcp` 2.x |
| `live/` | Каталог для живого запуска: `.mcp.json` и хуки, пишущие в `live/hooks.log` |
| `.claude-plugin/` | Тот же сервер, обёрнутый в плагин, для проверки имён (Q3) |
| `report.py` | Превращает `spike.log` и `live/hooks.log` в ответы |

## Проверка без Claude Code

```bash
python3 spike/drive.py                                   # mcp 1.x
SPIKE_SCRIPT=v2probe.py python3 spike/drive.py 2025-06-18 # mcp 2.x, ждём таймаут на канале
```

## Живой запуск, часть A: Q1, Q4, Q5, Q6

Запускается вручную. Автоматический режим Claude Code не даёт агенту
запускать сессию с флагом каналов для разработки.

1. Запустите сессию из каталога `live`:

    ```bash
    cd spike/live
    claude --model haiku --allowedTools mcp__spike__reply \
      --dangerously-load-development-channels server:spike
    ```

2. Примите три диалога: предупреждение о каналах для разработки
   («I am using this for local development»), доверие к каталогу и
   «Use this MCP server».
3. Ничего не вводите. Во втором терминале из корня репозитория отправьте
   событие:

    ```bash
    echo "ping one" >> spike/inbox.txt
    ```

4. Подождите 10–15 секунд. Сессия должна сама вызвать `reply` с текстом
   `ack HELIOTROPE`. Это ответ на Q4 и Q5.
5. Введите в сессии любой промпт, например `hello`. Он нужен для сравнения
   с событием канала в логе хуков. Затем отправьте второе событие:

    ```bash
    echo "ping two" >> spike/inbox.txt
    ```

6. Выйдите из сессии и запустите разбор:

    ```bash
    python3 spike/report.py
    ```

## Живой запуск, часть B: Q3, имена в плагине

Меняет конфигурацию аккаунта. В конце есть команды для отката.

```bash
claude plugin marketplace add "$PWD/spike"
claude plugin install agent-bus-spike@agent-bus-spike-dev

cd spike
claude --model haiku \
  --dangerously-load-development-channels plugin:agent-bus-spike@agent-bus-spike-dev
```

1. Отправьте событие: `echo "ping three" >> spike/inbox.txt`.
2. В запросе разрешения на `reply` будет полное имя инструмента.
   Запишите его и разрешите вызов.
3. Попросите сессию: `Quote verbatim the opening <channel> tag of the last
   channel event`. Так видно значение атрибута `source`.
4. Выйдите и откатите установку:

    ```bash
    claude plugin uninstall agent-bus-spike@agent-bus-spike-dev
    claude plugin marketplace remove agent-bus-spike-dev
    ```

## Необязательно: фоновая сессия

Проверяет вторую половину Q4. Нужно узнать, как фоновая сессия переживает
полноэкранное предупреждение флага разработки и просыпается ли от события.

```bash
cd spike/live
claude --bg --model haiku --allowedTools mcp__spike__reply \
  --dangerously-load-development-channels server:spike
```
