# Этап 5: запуск на двух аккаунтах

Инструкция для первого живого запуска шины между двумя личными аккаунтами
на Max: `~/.claude-personal` (агент `personal`) и `~/.claude-personal-a.k`
(агент `ak`). Все команды выполняются из корня основного checkout,
`~/Projects/Personal/mcp-agent-bus`. Маркетплейс из локального каталога
загружает плагин на месте по абсолютному пути, поэтому ставить его нужно
именно оттуда, а не из worktree.

Агент эти шаги выполнить не может: автоматический режим Claude Code не даёт
ему запускать сессии с флагом каналов для разработки.

## 1. Подготовка

Нужны `uv` и Claude Code не старше 2.1.285.

```bash
cd ~/Projects/Personal/mcp-agent-bus
mkdir -p ~/.agent-bus && chmod 700 ~/.agent-bus
```

Каталог шины создаётся заранее: тип `directory` в настройках плагина может
не принять несуществующий путь.

## 2. Установка в каждый аккаунт

```bash
for dir in ~/.claude-personal ~/.claude-personal-a.k; do
    CLAUDE_CONFIG_DIR=$dir claude plugin marketplace add "$PWD"
    CLAUDE_CONFIG_DIR=$dir claude plugin install agent-bus@agent-bus-dev
done

echo '{"agent_name": "personal", "bus_dir": "'"$HOME"'/.agent-bus", "max_agent_turns": "6"}' \
    | CLAUDE_CONFIG_DIR=~/.claude-personal claude plugin configure agent-bus@agent-bus-dev --values-stdin
echo '{"agent_name": "ak", "bus_dir": "'"$HOME"'/.agent-bus", "max_agent_turns": "6"}' \
    | CLAUDE_CONFIG_DIR=~/.claude-personal-a.k claude plugin configure agent-bus@agent-bus-dev --values-stdin
```

Проверка: команда без `--values-stdin` показывает сохранённые значения и
незаполненные опции.

```bash
CLAUDE_CONFIG_DIR=~/.claude-personal claude plugin configure agent-bus@agent-bus-dev
```

## 3. Терминал наблюдения

Во втором терминале из корня репозитория:

```bash
scripts/run.sh tail -f
```

Первый запуск создаёт окружение в `.plugin-data/`, это займёт несколько
секунд. Дальше журнал шины печатается вживую:

| Строка | Что значит |
| :- | :- |
| `HH:MM:SS personal -> ak [task] <id>` | Сообщение записано в журнал |
| `HH:MM:SS delivered <id> to ak` | Сервер сессии `ak` протолкнул его событием канала |
| `HH:MM:SS ack <id> by ak` | Агент `ak` обработал сообщение |
| `HH:MM:SS human turn for personal` | Человек ввёл промпт в сессии `personal` |

## 4. Запуск сессий

Две сессии в двух терминалах:

```bash
AGENT_BUS_SESSION=1 CLAUDE_CONFIG_DIR=~/.claude-personal claude --model sonnet \
    --allowedTools mcp__plugin_agent-bus_bus \
    --dangerously-load-development-channels plugin:agent-bus@agent-bus-dev

AGENT_BUS_SESSION=1 CLAUDE_CONFIG_DIR=~/.claude-personal-a.k claude --model sonnet \
    --allowedTools mcp__plugin_agent-bus_bus \
    --dangerously-load-development-channels plugin:agent-bus@agent-bus-dev
```

- **Диалог о каналах для разработки.** Выберите «I am using this for local
  development».
- **Первый запуск в аккаунте — холодная установка окружения.** Сервер и хук
  `SessionStart` одновременно ставят зависимости в каталог данных плагина.
  Если `/mcp` показывает сервер `bus` в статусе `failed`, дождитесь
  окончания стартового хука, затем переподключите сервер в `/mcp` или
  перезапустите сессию один раз.
- **Модель не слабее Sonnet.** На этапе 0 Haiku не выполнял инструкцию на
  пробуждении, а Sonnet выполнял.

## 5. Что прочитать в `server.log`

```bash
grep -E ' start | owns_channel=| delivery starts| stop ' ~/.agent-bus/server.log | tail -12
```

- **`env_keys` содержит `AGENT_BUS_SESSION`.** Значит, блок `env` сервера в
  манифесте дополняет окружение оболочки, а не заменяет его. Если ключа нет,
  строка `start` покажет `on_bus=False`, и сессия не на шине. Попробуйте
  добавить к строке запуска `--settings '{"env": {"AGENT_BUS_SESSION": "1"}}'`.
  Этот обход не проверен, сообщите о результате.
- **`owns_channel=True` у каждого интерактивного агента.** `False` при
  `background=False` значит, что у агента уже есть другая живая сессия на
  шине, и события уходят туда. Фоновая сессия всегда `owns_channel=False`.
- **`delivery starts for session <id>` через 5 секунд после старта.** С
  этого момента сервер доставляет события. До него он ждёт, пока Claude Code
  закончит подключение и запросит список инструментов.
- **`pushed <id> ... awaiting transcript`, затем `confirmed <id> in
  transcript`.** Строка `delivered` в журнале шины появляется только после
  подтверждения, то есть когда событие записано в транскрипт сессии.
- **`session <id> is deaf: ... released the channel`.** Событие не попало в
  транскрипт за 120 секунд, например потому, что сессию перевели в фон.
  Сообщение остаётся недоставленным до следующей живой сессии агента.
- **`took the channel for <агент>`.** Сессия, ждавшая канал, подхватила его
  у закрытой или глухой и сразу досылает недоставленное.
- **`stop pid=... signal=SIGTERM` при закрытии сессии.** Сервер освободил
  канал.

В `/mcp` у сервера `bus` должно быть четыре инструмента: `send`, `inbox`,
`ack`, `peers`.

## 6. Проверки

1. **Сообщение и ответ.** В сессии `personal` попросите:
   «Попроси агента ak ответить, сколько файлов в его текущем каталоге».
   В терминале наблюдения ожидается:

    ```text
    personal -> ak [task] <id1>
    delivered <id1> to ak
    ak -> personal [result] <id2> re <id1>
    ack <id1> by ak
    delivered <id2> to personal
    ```

    Сессия `ak` просыпается сама, без вашего ввода, а `personal` получает
    ответ событием канала.

2. **Ход человека.** Любой промпт, введённый вами, даёт строку
   `human turn for <агент>`. Событие канала такой строки не даёт.
3. **Досылка после закрытия.** Закройте сессию `ak` командой `/exit`. В
   `personal` попросите отправить `ak` любое сообщение. В журнале появится
   сообщение без строки `delivered`. Запустите `ak` снова и пройдите диалог
   флага разработки. Примерно через 5 секунд после старта ожидается ровно
   одна строка `delivered`, и сессия `ak` сама, без вашего ввода, отвечает
   на сообщение.
4. **Лимит ходов, по желанию.** Задайте одному аккаунту
   `"max_agent_turns": "2"` через `configure --values-stdin`, перезапустите
   сессию и попросите агентов обменяться сообщениями несколько раз. Третье
   сообщение будет отклонено, агент отправит `kind="await_human"` и
   остановится.
5. **Фоновая сессия.** Фоновая сессия не получает событий канала, поэтому
   канал не захватывает. Запустите `ak` с `--bg`, пока интерактивная `ak`
   закрыта. В `server.log` у фонового сервера ожидается `background=True
   owns_channel=False`. Сообщение для `ak` остаётся без `delivered`, а
   фоновая сессия узнаёт о нём из хуков в конце хода. Затем откройте
   интерактивную `ak`: она получает канал и сама отвечает на сообщение.
6. **Перевод сессии в фон.** Когда обе сессии обменялись сообщениями,
   переведите `ak` в фон и отправьте ей сообщение из `personal`. Старый
   процесс `ak` остаётся жить и держит канал, но событие до разговора не
   доходит. Через 120 секунд в `server.log` ожидается `is deaf ... released
   the channel`, а сообщение остаётся без `delivered`. Откройте новую `ak` на
   переднем плане: она пишет `took the channel` и сама отвечает на
   сообщение.

## 7. Что прислать обратно

- **Строки `start` и `owns_channel=` из `server.log` для обоих агентов.**
- **Вывод терминала наблюдения за проверки 1 и 3.**
- **Статус сервера `bus` в `/mcp` и все предупреждения при старте.**
- **Итоги проверок 5 и 6, если вы их делали.**

## Откат

```bash
for dir in ~/.claude-personal ~/.claude-personal-a.k; do
    CLAUDE_CONFIG_DIR=$dir claude plugin uninstall agent-bus@agent-bus-dev
    CLAUDE_CONFIG_DIR=$dir claude plugin marketplace remove agent-bus-dev
done
rm -rf ~/.agent-bus   # журнал шины, по желанию
```

В основном checkout остались логи живых запусков этапа 0. Выводы из них
уже записаны в план:

```bash
rm -rf spike
```
