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
| Карта | картинка сохранённой карты со станцией, а во время уборки — с роботом, путём, вымытым полом и убираемой зоной; обновляется каждые 15 с, если изменилась |
| Камера карты | та же картинка как камера — для карточки карты; атрибуты `calibration_points` и `rooms` (агент 0.22+) |
| Настройки | переключатели, списки и числа из раздела «Настройки» панели агента, только те, что есть у робота (агент 0.22+); обновляются раз в 2 минуты и через 3 с после изменения |
| Расходники | остаток ресурса в % для того, что отдаёт модель (щётки, фильтр, датчики, колёса), и кнопки «Reset …» для сброса после замены или чистки |
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

Зона, точка (робот убирает квадрат 1,5 × 1,5 м вокруг неё) и настройка — координаты в миллиметрах робота, их показывает панель агента:

```yaml
command: clean_zone
params: {zones: [[-5000, 1000, -3800, 2200]], repeats: 1, suction: 1, water: 2}
---
command: clean_spot
params: {points: [[-4400, 1600]], repeats: 1, suction: 1, water: 2}
---
command: set_setting
params: {setting: child_lock, to: true}
```

## Комнаты и пространства Home Assistant

Модуль отдаёт комнаты робота как сегменты пылесоса (`segments` в состоянии, `<база>/clean_segments` для команды). В Home Assistant с поддержкой уборки по пространствам: «Настройки → Устройства и службы → Объекты» → пылесос → шестерёнка → «Сопоставить сегменты с пространствами». Дальше `vacuum.clean_area` с пространствами дома убирает соответствующие комнаты (1 проход, текущая мощность, средняя влажность).

## Карта с выбором зон, точек и комнат

Карточка [xiaomi-vacuum-map-card](https://github.com/PiotrMachowski/lovelace-xiaomi-vacuum-map-card) (ставится через HACS) берёт карту и калибровку у «Камеры карты» и отправляет команды через `vacuum.send_command`. Номера и центры комнат — в атрибуте `rooms` камеры (Инструменты разработчика → Состояния).

Проще всего — платформа **send_command** в редакторе карточки: модуль понимает её стандартные команды (`app_zoned_clean` — уборка зоны, `app_segment_clean` — уборка комнат, `app_goto_target` — уборка квадрата 1,5 × 1,5 м вокруг точки: отдельного «ехать в точку» здесь нет). Мощность — текущая у робота, влажность — средняя. Карта и калибровка:

```yaml
type: custom:xiaomi-vacuum-map-card
entity: vacuum.dreame_r2505
vacuum_platform: send_command
map_source:
  camera: camera.dreame_r2505_map_camera
calibration_source:
  camera: true
map_modes:
  - template: vacuum_clean_zone
  - template: vacuum_goto
  - template: vacuum_clean_segment
    predefined_selections:   # id и центр из атрибута rooms
      - id: 4
        label: {text: Коридор, x: -4482, y: 1035}
```

Свои режимы со своими мощностью и влажностью:

```yaml
type: custom:xiaomi-vacuum-map-card
entity: vacuum.dreame_r2505
map_source:
  camera: camera.dreame_r2505_map_camera
calibration_source:
  entity: camera.dreame_r2505_map_camera
  attribute: calibration_points
map_modes:
  - name: Зона
    icon: mdi:select-drag
    selection_type: MANUAL_RECTANGLE
    max_selections: 5
    repeats_type: EXTERNAL
    max_repeats: 3
    service_call_schema:
      service: vacuum.send_command
      service_data:
        entity_id: "[[entity_id]]"
        command: clean_zone
        params: {zones: "[[selection]]", repeats: "[[repeats]]", suction: 1, water: 2}
  - name: Точка
    icon: mdi:map-marker
    selection_type: MANUAL_POINT
    repeats_type: EXTERNAL
    max_repeats: 3
    service_call_schema:
      service: vacuum.send_command
      service_data:
        entity_id: "[[entity_id]]"
        command: clean_spot
        params: {points: [["[[point_x]]", "[[point_y]]"]], repeats: "[[repeats]]", suction: 1, water: 2}
  - name: Комнаты
    icon: mdi:floor-plan
    selection_type: ROOM
    max_selections: 10
    repeats_type: EXTERNAL
    max_repeats: 3
    service_call_schema:
      service: vacuum.send_command
      service_data:
        entity_id: "[[entity_id]]"
        command: clean_rooms
        params: {rooms: "[[selection]]", repeats: "[[repeats]]", suction: 1, water: 2}
    predefined_selections:   # по одной на комнату: id и центр из атрибута rooms
      - id: 4
        label: {text: Коридор, x: -4482, y: 1035}
```

Мощность (`suction` 0–3) и влажность (`water` 1–3) в примере постоянные; их можно вынести в `variables` карточки.

## Топики

База по умолчанию — `dreame_local/<id робота>`:

- `<база>/state` — JSON `{"state", "fan_speed", "battery", "error", "error_name", "station", "status", "consumables"}`, retained;
- `<база>/availability` — `online`/`offline`: робот на связи, агент отвечает, данные свежие. При потере связи с брокером `offline` публикует сам брокер (last will);
- `<база>/command` — `start`, `pause`, `stop`, `return_to_base`, `locate`, `empty`, `wash_mop`, `dry_start`, `dry_stop`;
- `<база>/fan_speed` — скорость из списка выше;
- `<база>/send_command` — `{"command": "...", "params": {...}}`;
- `<база>/settings` — JSON настроек (`ON`/`OFF`, подпись варианта или число), retained;
- `<база>/setting/<id>/set` — новое значение настройки в том же виде;
- `<база>/map`, `<база>/map_attributes` — картинка карты и её калибровка с комнатами, retained.

Автообнаружение: `<префикс>/vacuum|sensor|button|image|camera|switch|select|number/dreame_<id>/.../config`, префикс по умолчанию `homeassistant`.

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
