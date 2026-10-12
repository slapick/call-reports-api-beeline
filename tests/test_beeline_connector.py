"""Тесты коннектора Билайн: API подменяется ответами в формате живого API
(null у abonent/clientService, receivedByAbonents — массив, userId абонента с @домен)."""
from __future__ import annotations

import httpx
import pytest

import beeline_connector
from infopanel_connect import Condition, ConfigError, Context, UpstreamError
from infopanel_connect.testing import check_connector
from beeline_connector import BeelineConnector

# 2026-10-01T10:00:00Z в миллисекундах
START_MS = 1790848800000

CALL_IN = {
    "keyId": "k1", "startTime": START_MS, "direction": "INB", "status": "RECEIVED_BY_ABONENT",
    "from": {"phone": "79000000001", "abonent": None, "clientService": None},
    "to": {"phone": "74950000021", "abonent": None,
           "clientService": {"name": "Группа продаж", "dtype": "GROUP", "extension": 700}},
    "inboundPhone": "74950000021", "duration": 23400, "hasCallRecording": True,
    "userTimezone": "Europe/Moscow", "isInternalCall": False, "externalTrackingId": "ext-1",
    "receivedByAbonents": [
        {"userId": "ivanov", "name": "Иванов Иван ", "department": "Продажи", "extension": 101},
        {"userId": "petrov", "name": "Петров Пётр", "department": "Продажи", "extension": 102},
    ],
}

CALL_OUT = {
    "keyId": "k2", "startTime": START_MS + 60_000, "direction": "OUT", "status": "OUTMISSED",
    "from": {"phone": "74950000021",
             "abonent": {"userId": "petrov", "name": "Петров Пётр", "department": "Продажи", "extension": 102}},
    "to": {"phone": "79000000001"},
    "inboundPhone": "", "duration": 0, "hasCallRecording": False,
    "userTimezone": "Europe/Moscow", "isInternalCall": False, "externalTrackingId": "",
}

CALL_MISSED = {
    "keyId": "k3", "startTime": START_MS + 120_000, "direction": "INB", "status": "INBMISSED_BY_ABONENT",
    "from": {"phone": "79990000001"},
    "to": {"phone": "74950000021",
           "abonent": {"userId": "ivanov", "name": "Иванов Иван", "department": "Продажи", "extension": 101}},
    "inboundPhone": "74950000021", "duration": 0, "hasCallRecording": False,
    "isInternalCall": False,
}

STATISTICS = {"ts": START_MS, "type": "success",
              "payload": {"totalCount": 3, "type": "LOG", "calls": [CALL_IN, CALL_OUT, CALL_MISSED]}}

ABONENTS = [
    {"userId": "ivanov@pbx.example.org", "phone": "74950000001", "firstName": "Иван", "lastName": "Иванов",
     "email": "ivanov@x.ru", "contactEmail": "", "department": "Продажи", "extension": "101"},
    {"userId": "petrov", "phone": "74950000002", "firstName": "Пётр", "lastName": "Петров"},
]

NUMBERS = [{"numberId": "n1", "phone": "74950000021"}]


@pytest.fixture
def api(monkeypatch):
    """Подменяет egress.fetch; возвращает список запросов (path, params, headers)."""
    calls: list[tuple] = []
    answers = {
        beeline_connector.STATISTICS_PATH: STATISTICS,
        beeline_connector.ABONENTS_PATH: ABONENTS,
        beeline_connector.NUMBERS_PATH: NUMBERS,
    }

    async def fake_fetch(method, url, *, params=None, headers=None, **_):
        path = httpx.URL(url).path
        calls.append((path, dict(params or {}), dict(headers or {})))
        return httpx.Response(200, json=answers[path])

    monkeypatch.setattr(beeline_connector.egress, "fetch", fake_fetch)
    return calls


def ctx() -> Context:
    return Context(BeelineConnector(), settings={}, secrets={"api_token": "t"})


async def test_check_passes(api):
    report = await check_connector(BeelineConnector(), secrets={"api_token": "t"})
    assert report.ok, report.text()


async def test_calls_and_period(api):
    conditions = (
        Condition("date", "gte", "2026-10-01T13:00:00+03:00"),
        Condition("date", "lt", "2026-10-02T23:00:00+03:00"),
    )
    result = await BeelineConnector().fetch(ctx(), "calls", conditions)
    path, params, headers = api[0]
    assert params["from"] == "2026-10-01T10:00:00.000Z" and params["to"] == "2026-10-02T20:00:00.000Z"
    assert params["offset"] == 0 and params["limit"] == beeline_connector.PAGE_LIMIT
    assert headers["X-MPBX-API-AUTH-TOKEN"] == "t"
    assert result.applied == conditions

    incoming, outgoing, missed = result.rows
    assert incoming["date"] == "2026-10-01T10:00:00Z"
    assert incoming["duration"] == 23  # 23 400 мс
    assert (incoming["direction_name"], incoming["status_name"]) == ("Входящие", "Принят сотрудником")
    assert (incoming["has_recording"], incoming["is_internal"]) == (1, 0)
    assert incoming["to_service_name"] == "Группа продаж" and incoming["to_service_extension"] == "700"
    # receivedByAbonents — первый в массиве (последний принявший)
    assert incoming["received_user_id"] == "ivanov" and incoming["received_extension"] == "101"
    assert incoming["employee_user_id"] == "ivanov" and incoming["employee_name"] == "Иванов Иван"
    assert incoming["from_user_id"] is None and incoming["from_service_name"] is None

    assert outgoing["employee_name"] == "Петров Пётр"  # исходящий — сторона А
    assert outgoing["inbound_phone"] is None and outgoing["received_user_id"] is None
    assert missed["employee_user_id"] == "ivanov"  # никто не принял — сторона Б


async def test_default_period(api):
    await BeelineConnector().fetch(ctx(), "calls", ())
    params = api[0][1]
    assert params["from"].endswith(".000Z") and "to" not in params


async def test_operators(api):
    operators = (await BeelineConnector().fetch(ctx(), "operators", ())).rows
    assert operators[0]["user_id"] == "ivanov" and operators[0]["name"] == "Иван Иванов" and operators[0]["contact_email"] is None
    assert operators[1]["department"] is None


async def test_calls_are_paged(monkeypatch):
    pages = {0: [CALL_IN, CALL_OUT], 2: [CALL_MISSED]}
    seen = []

    async def fake_fetch(method, url, *, params=None, **_):
        seen.append(params["offset"])
        return httpx.Response(200, json={"payload": {"totalCount": 3, "type": "log", "calls": pages[params["offset"]]}})

    monkeypatch.setattr(beeline_connector.egress, "fetch", fake_fetch)
    rows = (await BeelineConnector().fetch(ctx(), "calls", ())).rows
    assert [r["id"] for r in rows] == ["k1", "k2", "k3"]
    assert seen == [0, 2]


@pytest.mark.parametrize(
    "answer, error",
    [
        ({"payload": {"totalCount": 150000, "type": "CHARTS", "chart": {"items": []}}}, ConfigError),
        ({"errors": [{"exception": "ValidationException", "message": "from is required"}]}, UpstreamError),
    ],
)
async def test_statistics_problems(monkeypatch, answer, error):
    async def fake_fetch(*_, **__):
        return httpx.Response(200, json=answer)

    monkeypatch.setattr(beeline_connector.egress, "fetch", fake_fetch)
    with pytest.raises(error):
        await BeelineConnector().fetch(ctx(), "calls", ())


async def test_bad_token(monkeypatch):
    async def fake_fetch(*_, **__):
        return httpx.Response(401)

    monkeypatch.setattr(beeline_connector.egress, "fetch", fake_fetch)
    with pytest.raises(ConfigError):
        await BeelineConnector().test(ctx())
