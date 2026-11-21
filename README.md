# MQTT и Home Assistant — модуль Dreame Local

Модуль для [Dreame Local Agent](https://github.com/Tobirg/dreame_local_agent) 0.10 и новее. Публикует пылесос в MQTT-брокер с автообнаружением Home Assistant и принимает команды из MQTT. Подойдёт брокер, который уже работает для Zigbee2MQTT, или дополнение Mosquitto в Home Assistant.

Модуль не разблокирует робот: он ставится на робот, где агент уже работает.

## Установка и настройка

1. Веб-интерфейс агента → «Модули» → выбрать `mqtt-<версия>.tar.gz` → «Проверить пакет» → «Установить». В проверке пакета видно, что модуль управляет роботом и работает в фоне.
2. В панели «MQTT и Home Assistant» указать адрес брокера, порт, пользователя и пароль, включить «Публиковать робот в MQTT» и нажать «Сохранить и проверить».
3. В Home Assistant с настроенной интеграцией MQTT появится устройство «Dreame r2505» (по модели робота).

Настройки хранятся на роботе в `/data/dreame-local/module-data/mqtt/config.json` (только root) и сохраняются при обновлении модуля. При удалении модуль очищает свои сообщения автообнаружения, и устройство пропадает из Home Assistant.

## Что появляется в Home Assistant

| Сущность | Что делает |
|---|---|
| Пылесос | старт, пауза, стоп, на базу, найти, мощность (`quiet`, `standard`, `strong`, `turbo`), `vacuum.send_command` |
| Батарея | заряд, % |
| Ошибка | название ошибки (`no_error`, `bin_full`, …) |
| Расходники | остаток ресурса в % для того, что отдаёт модель (щётки, фильтр, датчики, колёса) |
| Кнопки станции | выгрузка пыли, мойка салфеток, начать и остановить сушку — только если модель робота их поддерживает |

Уборка комнат из автоматизации:

```yaml
action: vacuum.send_command
target:
  entity_id: vacuum.dreame_r2505
data:
  command: clean_rooms
  params: {rooms: [1, 2], repeats: 1, suction: 1, water: 2}
```

`command` — любая команда агента (`POST /api/v1/command`), `params` — её параметры.

## Топики

База по умолчанию — `dreame_local/<id робота>`:

- `<база>/state` — JSON `{"state", "fan_speed", "battery", "error", "error_name", "station", "status", "consumables"}`, retained;
- `<база>/availability` — `online`/`offline`: робот на связи, агент отвечает, данные свежие. При потере связи с брокером `offline` публикует сам брокер (last will);
- `<база>/command` — `start`, `pause`, `stop`, `return_to_base`, `locate`, `empty`, `wash_mop`, `dry_start`, `dry_stop`;
- `<база>/fan_speed` — скорость из списка выше;
- `<база>/send_command` — `{"command": "...", "params": {...}}`.

Автообнаружение: `<префикс>/vacuum|sensor|button/dreame_<id>/.../config`, префикс по умолчанию `homeassistant`.

## Как устроено

Один бинарник `mqtt-bridge` (Go, без внешних зависимостей) служит и обработчиком действий (`status`, `configure`, `test`, `uninstall`), и фоновым процессом (`mqtt-bridge run`). Агент запускает этот процесс сам, перезапускает при сбое и останавливает при обновлении или удалении модуля. Состояние приходит из потока событий агента `GET /api/v1/events`, команды уходят в `POST /api/v1/command` с токеном модуля.

Ограничения: MQTT 3.1.1, QoS 0, без TLS — рассчитано на брокер в домашней сети.

## Сборка и тесты

Нужны Go 1.22+ и GNU tar (Linux, WSL или Git Bash):

```sh
./build.sh                  # dist/mqtt-<версия>.tar.gz и его SHA-256
cd src && go test ./...     # тесты с фейковым брокером и фейковым агентом
```

Лицензия MIT.
