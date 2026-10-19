"""Коннектор Инфопанели для телефонии Билайн (Облачная АТС).

Данные берутся из API Облачной АТС (https://cloudpbx.beeline.ru):

- звонки — ``GET /napis/v3/statistics/recordsChart``;
- сотрудники — ``GET /apis/portal/abonents``;
- кнопка «Проверить» — ``GET /apis/portal/numbers`` (самый лёгкий запрос).

Авторизация — заголовок ``X-MPBX-API-AUTH-TOKEN`` с токеном API из личного
кабинета Облачной АТС.

Запуск из корня репозитория (PowerShell):

    $env:INFOPANEL_SIGNING_SECRET = "dev"
    infopanel-connect serve beeline_connector:BeelineConnector

Проверка без платформы (secrets.json — {"api_token": "<токен>"}):

    infopanel-connect check beeline_connector:BeelineConnector --secrets secrets.json

Протокол и шаблоны — docs/protocol.md и docs/templates.md в репозитории
connector-sdk (https://gitlab.itgrn.ru/infopanel/connector-sdk).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from infopanel_connect import (
    Column,
    Condition,
    ConfigError,
    Connector,
    Context,
    Entity,
    FetchResult,
    Field,
    UpstreamError,
)
from infopanel_connect import egress

# --------------------------------------------------------------------------- #
# Сущности (таблицы), которые коннектор отдаёт платформе.
# id уникален в пределах источника; подпись видит пользователь.
# --------------------------------------------------------------------------- #

CALLS = Entity("calls", "Звонки")
OPERATORS = Entity("operators", "Сотрудники")

# --------------------------------------------------------------------------- #
# API Облачной АТС Билайн: один адрес для всех клиентов, авторизация
# заголовком X-MPBX-API-AUTH-TOKEN.
# --------------------------------------------------------------------------- #

BASE_URL = "https://cloudpbx.beeline.ru"
STATISTICS_PATH = "/napis/v3/statistics/recordsChart"
ABONENTS_PATH = "/apis/portal/abonents"
NUMBERS_PATH = "/apis/portal/numbers"
DATE_COLUMN = "date"  # колонка, по которой приходит период датасета
DEFAULT_PERIOD = timedelta(days=30)  # from у Билайна обязателен — если периода нет
PAGE_LIMIT = 500  # звонков за запрос; максимум в документации не указан
MAX_PAGES = 1000  # предохранитель от бесконечного обхода страниц

DIRECTIONS: dict[str, str] = {
    "OUT": "Исходящие",
    "INB": "Входящие",
}

STATUSES: dict[str, str] = {
    "RECEIVED_BY_ABONENT": "Принят сотрудником",
    "INBMISSED_BY_ABONENT": "Пропущен сотрудником",
    "RECEIVED": "Принят сервисом",
    "INBMISSED": "Пропущен",
    "OUTMISSED": "Не дозвонился",
    "PLACED": "Дозвонился",
}
ANSWERED = frozenset({"RECEIVED_BY_ABONENT", "RECEIVED", "PLACED"})  # разговор состоялся
MISSED = frozenset({"INBMISSED_BY_ABONENT", "INBMISSED"})  # входящий без ответа

# Час и день недели считаем по часам АТС: date — в UTC, а «звонки с 9 до 10»
# и «в понедельник» клиент понимает по своему времени. Пояс — из userTimezone
# звонка; если его нет или он незнакомый — московский.
DEFAULT_TIMEZONE = "Europe/Moscow"
WEEKDAYS = ("1 Пн", "2 Вт", "3 Ср", "4 Чт", "5 Пт", "6 Сб", "7 Вс")  # номер — чтобы сортировались по порядку

# --------------------------------------------------------------------------- #
# Колонки сущностей: имена — для платформы, подписи — для людей.
# Ключи строк из fetch обязаны совпадать с этими именами. Логического типа у
# колонок нет — флаги отдаём числами 1/0 (сумма по колонке — количество «да»).
# --------------------------------------------------------------------------- #

def _party_columns(prefix: str, side: str) -> list[Column]:
    """Сторона звонка (from/to): номер, абонент АТС или сервис АТС."""
    return [
        Column(f"{prefix}_phone", f"{side}: номер", "string"),
        Column(f"{prefix}_user_id", f"{side}: логин сотрудника", "string"),
        Column(f"{prefix}_name", f"{side}: сотрудник", "string"),
        Column(f"{prefix}_department", f"{side}: отдел", "string"),
        Column(f"{prefix}_extension", f"{side}: короткий номер", "string"),
        Column(f"{prefix}_service_name", f"{side}: сервис", "string"),
        Column(f"{prefix}_service_type", f"{side}: тип сервиса", "string"),
        Column(f"{prefix}_service_extension", f"{side}: короткий номер сервиса", "string"),
    ]


def _abonent_columns(prefix: str, who: str) -> list[Column]:
    return [
        Column(f"{prefix}_user_id", f"{who}: логин", "string"),
        Column(f"{prefix}_name", f"{who}: ФИО", "string"),
        Column(f"{prefix}_department", f"{who}: отдел", "string"),
        Column(f"{prefix}_extension", f"{who}: короткий номер", "string"),
    ]


COLUMNS: dict[str, list[Column]] = {
    # По ответу /napis/v3/statistics/recordsChart (payload.calls[]); объекты
    # from/to/receivedByAbonents развёрнуты в колонки с префиксами.
    "calls": [
        Column("id", "ID звонка", "string"),
        Column("date", "Дата и время начала", "datetime"),
        Column("direction", "Направление (INB/OUT)", "string"),
        Column("direction_name", "Направление", "string"),
        Column("status", "Статус (код)", "string"),
        Column("status_name", "Статус", "string"),
        Column("duration", "Длительность разговора, сек", "number"),
        Column("inbound_phone", "Многоканальный номер", "string"),
        Column("has_recording", "Есть запись разговора", "number"),
        Column("is_internal", "Внутренний звонок", "number"),
        Column("user_timezone", "Часовой пояс", "string"),
        Column("external_tracking_id", "Внешний ID звонка", "string"),
        *_party_columns("from", "Сторона А"),
        *_party_columns("to", "Сторона Б"),
        *_abonent_columns("received", "Принял"),
        # Вычисляемые: сотрудник АТС, к которому относится звонок (см. _employee).
        *_abonent_columns("employee", "Сотрудник"),
        # Вычисляемые для отчётов (см. _call_row): шаги датасета не умеют «если».
        Column("client_phone", "Номер клиента", "string"),
        Column("is_inbound", "Входящий", "number"),
        Column("is_answered", "Разговор состоялся", "number"),
        Column("is_missed", "Пропущенный входящий", "number"),
        Column("minutes", "Длительность разговора, мин", "number"),
        Column("hour", "Час начала (по времени АТС)", "string"),
        Column("weekday", "День недели (по времени АТС)", "string"),
    ],
    # По ответу /apis/portal/abonents; user_id совпадает с calls.*_user_id.
    "operators": [
        Column("user_id", "Логин", "string"),
        Column("phone", "Номер", "string"),
        Column("name", "ФИО", "string"),
        Column("email", "Электронная почта", "string"),
        Column("contact_email", "Почта для уведомлений", "string"),
        Column("department", "Отдел", "string"),
        Column("extension", "Добавочный номер", "string"),
    ],
}


# --------------------------------------------------------------------------- #
# Помощники: даты периода, разбор ответа.
# --------------------------------------------------------------------------- #

def _beeline_dt(value) -> str | None:
    """«2026-10-01T13:00:00+03:00» → «2026-10-01T10:00:00.000Z» (UTC, формат Билайна)."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _format_utc(moment)


def _format_utc(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _period(conditions: tuple[Condition, ...]):
    """Период датасета (gte начала, lt конца по колонке date) → from/to."""
    start = end = None
    applied: list[Condition] = []
    for cond in conditions:
        if cond.field != DATE_COLUMN or cond.op not in ("gte", "lt"):
            continue
        value = _beeline_dt(cond.value)
        if not value:
            continue
        if cond.op == "gte" and start is None:
            start = value
            applied.append(cond)
        elif cond.op == "lt" and end is None:
            end = value
            applied.append(cond)
    return start, end, tuple(applied)


def _moment(value) -> datetime | None:
    """startTime (мс от 1970 года, в UTC) → datetime в UTC. Секунды тоже понимаем."""
    number = _number(value)
    if number is None:
        return None
    seconds = number / 1000 if abs(number) >= 1e11 else number
    try:
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _zone(name) -> ZoneInfo:
    """Часовой пояс АТС из userTimezone; нет или незнакомый — московский."""
    try:
        return ZoneInfo(str(name).strip())
    except (ZoneInfoNotFoundError, ValueError, OSError):
        return ZoneInfo(DEFAULT_TIMEZONE)


def _client_phone(item: dict, direction: str | None) -> str | None:
    """Номер собеседника вне АТС: у входящего — сторона А, у исходящего — Б."""
    if _flag(item.get("isInternalCall")):
        return None
    side = {"INB": "from", "OUT": "to"}.get(direction or "")
    party = item.get(side) if side else None
    return _text(party.get("phone")) if isinstance(party, dict) else None


def _received(value) -> dict:
    """receivedByAbonents: массив (первый — последний принявший) или объект."""
    if isinstance(value, list):
        value = next((v for v in value if isinstance(v, dict)), None)
    return value if isinstance(value, dict) else {}


def _abonent(value) -> dict:
    return value if isinstance(value, dict) else {}


def _abonent_values(prefix: str, abonent: dict) -> dict:
    return {
        f"{prefix}_user_id": _text(abonent.get("userId")),
        f"{prefix}_name": _text(abonent.get("name")),
        f"{prefix}_department": _text(abonent.get("department")),
        f"{prefix}_extension": _text(abonent.get("extension")),
    }


def _party_values(prefix: str, party) -> dict:
    party = party if isinstance(party, dict) else {}
    service = _abonent(party.get("clientService"))
    return {
        f"{prefix}_phone": _text(party.get("phone")),
        **_abonent_values(prefix, _abonent(party.get("abonent"))),
        f"{prefix}_service_name": _text(service.get("name")),
        f"{prefix}_service_type": _text(service.get("dtype")),
        f"{prefix}_service_extension": _text(service.get("extension")),
    }


def _employee(item: dict) -> dict:
    """Сотрудник АТС, к которому относится звонок.

    Исходящий — инициатор (сторона А); входящий — принявший вызов, а если
    никто не принял — адресат (сторона Б). Направление неизвестно — первый
    найденный из принявшего, стороны А и стороны Б.
    """
    def side(name: str) -> dict:
        party = item.get(name)
        return _abonent(party.get("abonent")) if isinstance(party, dict) else {}

    received = _received(item.get("receivedByAbonents"))
    direction = item.get("direction")
    if direction == "OUT":
        candidates = (side("from"),)
    elif direction == "INB":
        candidates = (received, side("to"))
    else:
        candidates = (received, side("from"), side("to"))
    return next((c for c in candidates if c.get("userId")), {})


def _call_row(item: dict) -> dict:
    """Звонок из payload.calls[] → строка с ключами из COLUMNS["calls"]."""
    direction = _text(item.get("direction"))
    status = _text(item.get("status"))
    duration_ms = _number(item.get("duration"))
    seconds = round(duration_ms / 1000) if duration_ms is not None else None
    moment = _moment(item.get("startTime"))
    local = moment.astimezone(_zone(item.get("userTimezone"))) if moment else None
    return {
        "id": _text(item.get("keyId")),
        "date": moment.strftime("%Y-%m-%dT%H:%M:%SZ") if moment else None,
        "direction": direction,
        "direction_name": DIRECTIONS.get(direction or "", direction),
        "status": status,
        "status_name": STATUSES.get(status or "", status),
        "duration": seconds,
        "inbound_phone": _text(item.get("inboundPhone")),
        "has_recording": _flag(item.get("hasCallRecording")),
        "is_internal": _flag(item.get("isInternalCall")),
        "user_timezone": _text(item.get("userTimezone")),
        "external_tracking_id": _text(item.get("externalTrackingId")),
        **_party_values("from", item.get("from")),
        **_party_values("to", item.get("to")),
        **_abonent_values("received", _received(item.get("receivedByAbonents"))),
        **_abonent_values("employee", _employee(item)),
        "client_phone": _client_phone(item, direction),
        "is_inbound": {"INB": 1, "OUT": 0}.get(direction or ""),
        "is_answered": (1 if status in ANSWERED else 0) if status else None,
        "is_missed": (1 if status in MISSED else 0) if status else None,
        "minutes": round(seconds / 60, 2) if seconds is not None else None,
        "hour": f"{local.hour:02d}:00" if local else None,
        "weekday": WEEKDAYS[local.weekday()] if local else None,
    }


def _operator_row(item: dict) -> dict:
    """Абонент из /abonents → строка с ключами из COLUMNS["operators"]."""
    return {
        # «9000000000@pbx.example…» → «9000000000», как userId в звонках
        "user_id": _text(str(item.get("userId") or "").split("@", 1)[0]),
        "phone": _text(item.get("phone")),
        # Билайн кладёт ФИО целиком в firstName, lastName обычно не присылает
        "name": _text(" ".join(str(item.get(k) or "") for k in ("firstName", "lastName"))),
        "email": _text(item.get("email")),
        "contact_email": _text(item.get("contactEmail")),
        "department": _text(item.get("department")),
        "extension": _text(item.get("extension")),
    }


def _text(value):
    """Строка без пробелов по краям или None; пустые — тоже None. 101.0 → «101»."""
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = value if isinstance(value, str) else str(value)
    return text.strip() or None


def _number(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        number = float(str(value).strip())
        return int(number) if number.is_integer() else number
    except (TypeError, ValueError):
        return None


def _flag(value):
    """true/false из ответа → 1/0 (у колонок нет логического типа)."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip().lower() in ("1", "true", "yes", "on")
    return 1 if value else 0


class BeelineConnector(Connector):
    """Телефония Билайн: звонки и сотрудники Облачной АТС."""

    # --- Карточка в маркетплейсе (операция manifest) ----------------------- #

    name = "beeline-telephony"  # латиница/цифры/«-»/«_»; между версиями не менять
    label = "Телефония Билайн"
    description = "Звонки и сотрудники Облачной АТС Билайн."
    version = "0.2.0"
    publisher = "ITGRN"
    docs_url = ""  # ссылка на документацию, если появится

    # --- Форма подключения источника: что вводит пользователь -------------- #
    # secret-поля хранятся на платформе зашифрованными и приходят в ctx.secrets.

    fields = (
        Field(
            "api_token",
            "Токен API Облачной АТС",
            type="password",
            secret=True,
            help="Личный кабинет Облачной АТС Билайн → настройки API",
        ),
    )

    # Период датасета («этот месяц», «последние 30 дней») приходит двумя
    # условиями gte/lt по колонке date — передаём их в API как from/to.
    # Это подсказка конструктору датасетов; фактически применённые условия
    # возвращаются из fetch в FetchResult.applied.
    pushdown_operators = frozenset({"gte", "lt"})
    pushdown_fields = frozenset({DATE_COLUMN})

    # --- Наборы данных известны заранее (операция entities) ---------------- #
    # entities() не переопределяем: базовый отдаст entity_catalog как есть.

    entity_catalog = (CALLS, OPERATORS)

    # --- Данные (операция fetch) ------------------------------------------- #

    async def fetch(
        self, ctx: Context, entity: str, conditions: tuple[Condition, ...]
    ) -> FetchResult:
        """Строки сущности из API Облачной АТС.

        Звонки обходим по страницам сами (offset/limit), справочники приходят
        целиком; fetch_page не переопределяем: SDK сам нарежет строки на страницы.
        """
        self.entity(entity)

        if entity == OPERATORS.id:
            items = await self._list(ctx, ABONENTS_PATH)
            return FetchResult([_operator_row(i) for i in items])

        start, end, applied = _period(conditions)
        if start is None:
            start = _format_utc(datetime.now(timezone.utc) - DEFAULT_PERIOD)
        params: dict = {"from": start, "limit": PAGE_LIMIT, "sortDirection": "ASC"}
        if end:
            params["to"] = end
        calls = await self._calls(ctx, params)
        return FetchResult([_call_row(c) for c in calls], applied=applied)

    # --- Колонки без выгрузки строк (операция describe) -------------------- #

    async def describe(
        self, ctx: Context, entity_ids: list[str]
    ) -> dict[str, list[Column]]:
        return {i: COLUMNS[i] for i in entity_ids if i in COLUMNS}

    # --- Кнопка «Проверить» (операция test) -------------------------------- #

    async def test(self, ctx: Context) -> str:
        """Лёгкая проверка токена — список входящих номеров."""
        items = await self._list(ctx, NUMBERS_PATH)
        return f"Подключение к Облачной АТС Билайн работает; входящих номеров: {len(items)}"

    # --- Утилиты ------------------------------------------------------------ #

    def entity(self, entity_id: str) -> Entity:
        """Сущность по id; пригодится в fetch."""
        for item in self.entity_catalog:
            if item.id == entity_id:
                return item
        raise ConfigError(f"Нет сущности «{entity_id}»")

    async def _get(self, ctx: Context, path: str, params: dict | None = None):
        """GET к API Билайна → разобранный JSON; ошибки — понятными исключениями SDK."""
        token = ctx.require("api_token")
        # egress.fetch проверяет адрес, ходит через корпоративный прокси, если
        # он задан, и сам оборачивает таймауты и обрывы связи в UpstreamError.
        response = await egress.fetch(
            "GET",
            f"{BASE_URL}{path}",
            params=params or {},
            headers={"X-MPBX-API-AUTH-TOKEN": token},
            timeout=60.0,
        )
        if response.status_code in (401, 403):
            raise ConfigError("Билайн отверг токен — проверьте «Токен API Облачной АТС»")
        if response.status_code >= 400:
            raise UpstreamError(f"Облачная АТС Билайн ответила ошибкой (HTTP {response.status_code})")
        try:
            return response.json()
        except ValueError:
            raise UpstreamError("Облачная АТС Билайн ответила не в формате JSON") from None

    async def _list(self, ctx: Context, path: str) -> list[dict]:
        """Справочник целиком: ответ — простой список."""
        data = await self._get(ctx, path)
        if not isinstance(data, list):
            raise UpstreamError("Облачная АТС Билайн ответила в неожиданном формате")
        return [i for i in data if isinstance(i, dict)]

    async def _calls(self, ctx: Context, params: dict) -> list[dict]:
        """Все звонки за период: страницы offset/limit, пока не набрали totalCount."""
        calls: list[dict] = []
        query = dict(params)
        for _ in range(MAX_PAGES):
            query["offset"] = len(calls)
            data = await self._get(ctx, STATISTICS_PATH, query)
            if not isinstance(data, dict):
                raise UpstreamError("Облачная АТС Билайн ответила в неожиданном формате")
            errors = data.get("errors")
            if errors:
                messages = "; ".join(
                    str(e.get("message") or e.get("exception") or e) if isinstance(e, dict) else str(e)
                    for e in (errors if isinstance(errors, list) else [errors])
                )
                raise UpstreamError(f"Облачная АТС Билайн вернула ошибку: {messages}")
            payload = data.get("payload")
            if not isinstance(payload, dict):
                raise UpstreamError("Облачная АТС Билайн ответила в неожиданном формате")
            if str(payload.get("type") or "").lower() == "charts" or (payload.get("calls") is None and payload.get("chart")):
                # Свыше 100 000 звонков Билайн отдаёт только количество по интервалам.
                raise ConfigError("За период больше 100 000 звонков — уменьшите период датасета")
            page = [c for c in payload.get("calls") or [] if isinstance(c, dict)]
            calls.extend(page)
            total = _number(payload.get("totalCount"))
            if not page or total is None or len(calls) >= total:
                return calls
        raise UpstreamError("Облачная АТС Билайн отдаёт слишком много страниц")
