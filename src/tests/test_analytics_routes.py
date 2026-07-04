from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.dependencies import get_current_user
from app.config import Settings
from app.main import create_app
from app.models.analytics import AcquisitionReportResponse, CampaignVisitRequest
from app.routers.analytics import get_analytics_service, maybe_get_analytics_service
from app.services.analytics_service import AnalyticsService


class MemoryEventsCollection:
    def __init__(self):
        self.docs: list[dict] = []

    async def insert_one(self, document: dict):
        self.docs.append(dict(document))

    async def find_one(self, query: dict):
        for doc in self.docs:
            if all(doc.get(key) == value for key, value in query.items()):
                return doc
        return None


class FakeAggregateCursor:
    def __init__(self, rows: list[dict]):
        self.rows = rows

    async def to_list(self, length=None):  # noqa: ANN001
        return list(self.rows)


class FakeAggregateCollection:
    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.pipelines: list[list[dict]] = []

    def aggregate(self, pipeline: list[dict]):
        self.pipelines.append(pipeline)
        return FakeAggregateCursor(self.rows)


class FrozenAnalyticsService(AnalyticsService):
    now = datetime(2026, 6, 21, tzinfo=UTC)

    @classmethod
    def utcnow(cls) -> datetime:
        return cls.now


@pytest.mark.asyncio
async def test_analytics_service_records_minimal_campaign_visit_and_resolves_snapshot() -> None:
    events = MemoryEventsCollection()
    service = AnalyticsService(events)

    event = await service.record_campaign_visit(
        CampaignVisitRequest(
            landing_path="/lobby?utm_source=reddit",
            referrer_host="reddit.com",
            utm={"source": "reddit", "medium": "post", "campaign": "ruleset-default", "content": ""},
        )
    )
    snapshot = await service.attribution_snapshot_for_id(event["attribution_id"])

    assert snapshot is not None
    assert snapshot["attribution_id"] == event["attribution_id"]
    assert snapshot["utm"] == {"source": "reddit", "medium": "post", "campaign": "ruleset-default"}
    assert snapshot["landing_path"] == "/lobby?utm_source=reddit"
    assert snapshot["referrer_host"] == "reddit.com"
    assert "ip" not in events.docs[0]
    assert "user_agent" not in events.docs[0]


@pytest.mark.asyncio
async def test_attribution_snapshot_returns_none_for_invalid_or_missing_event() -> None:
    events = MemoryEventsCollection()
    service = AnalyticsService(events)

    assert await service.attribution_snapshot_for_id("not-an-object-id") is None
    assert await service.attribution_snapshot_for_id("507f1f77bcf86cd799439099") is None


@pytest.mark.asyncio
async def test_acquisition_report_merges_all_sources_and_orders_rows() -> None:
    reddit_key = {"source": "reddit", "medium": "post", "campaign": "ruleset-default"}
    organic_key = {"source": None, "medium": "search", "campaign": None}
    db = SimpleNamespace(
        analytics_events=FakeAggregateCollection([{"_id": reddit_key, "count": 3}]),
        sessions=FakeAggregateCollection([{"_id": reddit_key, "count": 2}]),
        users=FakeAggregateCollection([{"_id": reddit_key, "count": 1}]),
        games=FakeAggregateCollection([{"_id": organic_key, "count": 0}]),
        game_archives=FakeAggregateCollection([{"_id": organic_key, "count": 5}]),
    )
    service = FrozenAnalyticsService(MemoryEventsCollection())

    report = await service.acquisition_report(db, days=7)

    assert report.days == 7
    assert report.generated_at == FrozenAnalyticsService.now
    assert [(row.source, row.medium, row.campaign) for row in report.rows] == [
        ("reddit", "post", "ruleset-default"),
        (None, "search", None),
    ]
    assert report.rows[0].visits == 3
    assert report.rows[0].sessions == 2
    assert report.rows[0].acquired_users == 1
    assert report.rows[1].games_created == 0
    assert report.rows[1].games_completed == 5
    assert db.analytics_events.pipelines[0][0]["$match"] == {
        "event_type": "campaign_visit",
        "occurred_at": {"$gte": FrozenAnalyticsService.now - timedelta(days=7)},
    }
    assert db.sessions.pipelines[0][0]["$match"]["attribution"] == {"$exists": True}
    assert db.game_archives.pipelines[0][0]["$match"]["attribution"] == {"$exists": True}


def test_acquisition_report_field_helpers_cover_all_collection_shapes() -> None:
    assert AnalyticsService._row_key("reddit", "post", "launch") == ("reddit", "post", "launch")
    assert AnalyticsService._field_path("analytics_events", "source") == "$utm.source"
    assert AnalyticsService._field_path("games", "source") == "$attribution.utm.source"


def test_analytics_dependency_helpers_prefer_app_state_and_fallback_to_db(monkeypatch: pytest.MonkeyPatch) -> None:
    app_state_service = AnalyticsService(MemoryEventsCollection())
    request_with_service = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(analytics_service=app_state_service)))
    request_without_service = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(analytics_service=None)))
    fake_db = SimpleNamespace(analytics_events=MemoryEventsCollection())

    from app.routers import analytics as analytics_router_module

    monkeypatch.setattr(analytics_router_module, "require_db", lambda: fake_db)

    assert get_analytics_service(request_with_service) is app_state_service
    assert maybe_get_analytics_service(request_with_service) is app_state_service
    assert isinstance(get_analytics_service(request_without_service), AnalyticsService)
    assert maybe_get_analytics_service(request_without_service) is None


def test_campaign_visit_endpoint_sets_one_year_opaque_cookie() -> None:
    app = create_app(Settings(ENVIRONMENT="production"))
    recorded_payloads = []

    class FakeAnalyticsService:
        async def record_campaign_visit(self, payload):
            recorded_payloads.append(payload)
            return {
                "attribution_id": "507f1f77bcf86cd799439099",
                "occurred_at": datetime.now(UTC),
                "utm": {"source": "reddit"},
            }

    app.dependency_overrides[get_analytics_service] = lambda: FakeAnalyticsService()

    with TestClient(app) as client:
        response = client.post(
            "/api/analytics/visit",
            json={
                "landing_path": "https://evil.example/not-relative",
                "referrer_host": "reddit.com",
                "utm": {"source": "reddit", "medium": "post", "campaign": "ruleset-default"},
            },
        )

    assert response.status_code == 201
    assert response.json() == {
        "attribution_id": "507f1f77bcf86cd799439099",
        "cookie_max_age_seconds": AnalyticsService.ATTRIBUTION_COOKIE_MAX_AGE_SECONDS,
    }
    assert recorded_payloads[0].landing_path == "/"
    cookie = response.headers["set-cookie"]
    assert "ks_attribution_id=507f1f77bcf86cd799439099" in cookie
    assert f"Max-Age={60 * 60 * 24 * 365}" in cookie
    assert "Domain=.kriegspiel.org" in cookie
    assert "HttpOnly" in cookie
    assert "Secure" in cookie
    assert "SameSite=lax" in cookie


def test_campaign_visit_request_rejects_protocol_relative_landing_path() -> None:
    payload = CampaignVisitRequest(landing_path="//evil.example/path", utm={"source": "reddit"})

    assert payload.landing_path == "/"


def test_acquisition_report_endpoint_returns_aggregate_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    app = create_app(Settings(ENVIRONMENT="testing", TECH_REPORT_USERNAMES="playerone"))

    from app.routers import analytics as analytics_router_module

    monkeypatch.setattr(analytics_router_module, "require_db", lambda: SimpleNamespace())

    class FakeAnalyticsService:
        async def acquisition_report(self, db, *, days):  # noqa: ANN001
            return AcquisitionReportResponse(
                days=days,
                generated_at=datetime(2026, 6, 21, tzinfo=UTC),
                rows=[{"source": "reddit", "medium": "post", "campaign": "ruleset-default", "visits": 1}],
            )

    app.dependency_overrides[get_analytics_service] = lambda: FakeAnalyticsService()
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(username="playerone", role="user")

    with TestClient(app) as client:
        response = client.get("/api/tech/acquisition-report?days=7")

    assert response.status_code == 200
    assert response.json()["rows"] == [
        {
            "source": "reddit",
            "medium": "post",
            "campaign": "ruleset-default",
            "visits": 1,
            "sessions": 0,
            "acquired_users": 0,
            "games_created": 0,
            "games_completed": 0,
        }
    ]


def test_acquisition_report_endpoint_rejects_non_operators() -> None:
    app = create_app(Settings(ENVIRONMENT="testing", TECH_REPORT_USERNAMES="fil"))
    app.dependency_overrides[get_analytics_service] = lambda: SimpleNamespace()
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(username="outsider", role="user")

    with TestClient(app) as client:
        response = client.get("/api/tech/acquisition-report")

    assert response.status_code == 403
    assert response.json()["detail"] == "Tech reports are private"


def test_acquisition_report_user_pipeline_reads_user_acquisition() -> None:
    pipeline = AnalyticsService._group_pipeline(
        collection_name="users",
        since=datetime(2026, 6, 1, tzinfo=UTC),
    )

    assert pipeline[0]["$match"]["acquisition"] == {"$exists": True}
    assert pipeline[1]["$group"]["_id"] == {
        "source": "$acquisition.utm.source",
        "medium": "$acquisition.utm.medium",
        "campaign": "$acquisition.utm.campaign",
    }
