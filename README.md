# Коннектор Инфопанели: телефония Билайн

Выгружает данные Облачной АТС Билайн (https://cloudpbx.beeline.ru).
Построен на SDK [infopanel-connect](https://gitlab.itgrn.ru/infopanel/connector-sdk).

| Сущность | Метод API | Связь |
|---|---|---|
| `calls` — звонки | `GET /napis/v3/statistics/recordsChart` | `*_user_id` → `operators.user_id` |
| `operators` — сотрудники | `GET /apis/portal/abonents` | `user_id` |

Логические признаки отдаются числами 1/0 (логического типа у колонок SDK нет).
Период датасета передаётся в API (`from`/`to`); без периода выгружаются последние 30 дней.
Если за период больше 100 000 звонков, Билайн отдаёт только агрегаты — коннектор
попросит уменьшить период.

```
beeline_connector.py   коннектор (класс BeelineConnector)
tests/                 тесты (API Билайна подменяется ответами по схемам документации)
requirements.txt       зависимости: infopanel-connect, tzdata
requirements-dev.txt   + pytest
Dockerfile             образ сервиса
.github/workflows/     CI: тесты на Python 3.10–3.12 и сборка образа
```

## Колонки

### `calls` — Звонки

| Колонка | Тип | Описание |
|---|---|---|
| `id` | string | ID звонка |
| `date` | datetime | Дата и время начала |
| `direction` | string | Направление (INB/OUT) |
| `direction_name` | string | Направление |
| `status` | string | Статус (код) |
| `status_name` | string | Статус |
| `duration` | number | Длительность разговора, сек |
| `inbound_phone` | string | Многоканальный номер |
| `has_recording` | number | Есть запись разговора |
| `is_internal` | number | Внутренний звонок |
| `user_timezone` | string | Часовой пояс |
| `external_tracking_id` | string | Внешний ID звонка |
| `from_phone` | string | Сторона А: номер |
| `from_user_id` | string | Сторона А: логин сотрудника |
| `from_name` | string | Сторона А: сотрудник |
| `from_department` | string | Сторона А: отдел |
| `from_extension` | string | Сторона А: короткий номер |
| `from_service_name` | string | Сторона А: сервис |
| `from_service_type` | string | Сторона А: тип сервиса |
| `from_service_extension` | string | Сторона А: короткий номер сервиса |
| `to_phone` | string | Сторона Б: номер |
| `to_user_id` | string | Сторона Б: логин сотрудника |
| `to_name` | string | Сторона Б: сотрудник |
| `to_department` | string | Сторона Б: отдел |
| `to_extension` | string | Сторона Б: короткий номер |
| `to_service_name` | string | Сторона Б: сервис |
| `to_service_type` | string | Сторона Б: тип сервиса |
| `to_service_extension` | string | Сторона Б: короткий номер сервиса |
| `received_user_id` | string | Принял: логин |
| `received_name` | string | Принял: ФИО |
| `received_department` | string | Принял: отдел |
| `received_extension` | string | Принял: короткий номер |
| `employee_user_id` | string | Сотрудник: логин — *вычисляется* |
| `employee_name` | string | Сотрудник: ФИО — *вычисляется* |
| `employee_department` | string | Сотрудник: отдел — *вычисляется* |
| `employee_extension` | string | Сотрудник: короткий номер — *вычисляется* |

| `client_phone` | string | Номер клиента — *вычисляется* |
| `is_inbound` | number | Входящий (1/0) — *вычисляется* |
| `is_answered` | number | Разговор состоялся (1/0) — *вычисляется* |
| `is_missed` | number | Пропущенный входящий (1/0) — *вычисляется* |
| `minutes` | number | Длительность разговора, мин — *вычисляется* |
| `hour` | string | Час начала по времени АТС, «09:00» — *вычисляется* |
| `weekday` | string | День недели по времени АТС, «1 Пн» … «7 Вс» — *вычисляется* |

`employee_*` — сотрудник АТС, к которому относится звонок: у исходящего — сторона А,
у входящего — принявший вызов (`received_*`), а если никто не принял — сторона Б.
Для отчётов «звонки по сотрудникам» группируйте по этим колонкам.

Остальные вычисляемые колонки нужны потому, что в шагах датасета нет условий «если…»:

- `client_phone` — номер собеседника вне АТС: у входящего сторона А, у исходящего
  сторона Б; у внутренних звонков пусто.
- `is_answered` — статусы «Принят сотрудником», «Принят сервисом», «Дозвонился»;
  `is_missed` — «Пропущен сотрудником», «Пропущен». Сумма флага — количество,
  среднее — доля (доля пропущенных, доля дозвона).
- `hour` и `weekday` считаются в часовом поясе АТС (`userTimezone` звонка, если его
  нет — московский), а `date` остаётся в UTC. Звонок в 22:30 UTC воскресенья — это
  «01:00», «1 Пн» по Москве. Номер в начале дня недели нужен для сортировки.

### `operators` — Сотрудники

| Колонка | Тип | Описание |
|---|---|---|
| `user_id` | string | Логин |
| `phone` | string | Номер |
| `name` | string | ФИО |
| `email` | string | Электронная почта |
| `contact_email` | string | Почта для уведомлений |
| `department` | string | Отдел |
| `extension` | string | Добавочный номер |

## Запуск без Docker

```bash
pip install -r requirements.txt   --index-url https://gitlab.itgrn.ru/api/v4/projects/infopanel%2Fconnector-sdk/packages/pypi/simple   --extra-index-url https://pypi.org/simple
infopanel-connect check beeline_connector:BeelineConnector --secrets secrets.json  # {"api_token": "<токен>"}
INFOPANEL_SIGNING_SECRET=dev infopanel-connect serve beeline_connector:BeelineConnector --port 8090
```

Тесты: `pip install -r requirements-dev.txt` (с тем же `--index-url`), затем `pytest -q`.

Для правок SDK одновременно с коннектором: `pip install -e ../connector-sdk`.

## Docker

```bash
docker build -t call-reports-api-beeline .
docker run --rm -p 8090:8090 -e INFOPANEL_SIGNING_SECRET=dev call-reports-api-beeline
curl http://localhost:8090/health
```

## Подключение источника

Кнопка «Проверить» обращается к `GET /apis/portal/numbers`.

- **Токен API Облачной АТС** — из личного кабинета Облачной АТС Билайн (настройки API).
