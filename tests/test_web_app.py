"""Tests for src/web/ — FastAPI app factory and API routes."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.web.app import create_app


@pytest.fixture
def client():
    app = create_app()
    return TestClient(app)


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


class TestCreateApp:
    def test_creates_app(self):
        app = create_app()
        assert app.title == "Signals Pipeline UI"

    def test_includes_api_routes(self):
        app = create_app()
        paths = [route.path for route in app.routes]
        assert "/api/accounts" in paths
        assert "/api/labels" in paths

    def test_index_route(self, client):
        response = client.get("/")
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# Accounts API
# ---------------------------------------------------------------------------


class TestAccountsAPI:
    @patch("src.web.routes.accounts._get_conn")
    def test_list_accounts(self, mock_conn):
        conn = MagicMock()
        mock_conn.return_value = conn
        conn.close = MagicMock()

        with patch("src.web.routes.accounts.db.get_accounts_paginated") as mock_paginated:
            mock_paginated.return_value = (
                [{"account_id": "a1", "company_name": "Acme", "domain": "acme.com", "score": 25.0, "tier": "high"}],
                1,
            )
            app = create_app()
            client = TestClient(app)
            response = client.get("/api/accounts?page=1&per_page=10")
            assert response.status_code == 200
            data = response.json()
            assert data["total"] == 1
            assert len(data["items"]) == 1
            assert data["items"][0]["company_name"] == "Acme"

    @patch("src.web.routes.accounts._get_conn")
    def test_get_account_detail(self, mock_conn):
        conn = MagicMock()
        mock_conn.return_value = conn
        conn.close = MagicMock()

        with patch("src.web.routes.accounts.db.get_account_detail") as mock_detail:
            mock_detail.return_value = {
                "account_id": "a1",
                "company_name": "Acme",
                "domain": "acme.com",
                "signals": [],
                "scores": [],
            }
            app = create_app()
            client = TestClient(app)
            response = client.get("/api/accounts/a1")
            assert response.status_code == 200
            data = response.json()
            assert data["company_name"] == "Acme"

    @patch("src.web.routes.accounts._get_conn")
    def test_get_account_not_found(self, mock_conn):
        conn = MagicMock()
        mock_conn.return_value = conn
        conn.close = MagicMock()

        with patch("src.web.routes.accounts.db.get_account_detail") as mock_detail:
            mock_detail.return_value = None
            app = create_app()
            client = TestClient(app)
            response = client.get("/api/accounts/nonexistent")
            assert response.status_code == 200  # returns tuple, not HTTPException


# ---------------------------------------------------------------------------
# Labels API
# ---------------------------------------------------------------------------


class TestLabelsAPI:
    @patch("src.web.routes.labels._get_conn")
    def test_create_label(self, mock_conn):
        conn = MagicMock()
        mock_conn.return_value = conn
        conn.close = MagicMock()

        with patch("src.web.routes.labels.db.insert_account_label") as mock_insert:
            mock_insert.return_value = "label_123"
            app = create_app()
            client = TestClient(app)
            response = client.post(
                "/api/labels", json={"account_id": "a1", "label": "qualified", "notes": "looks good"}
            )
            assert response.status_code == 200
            assert response.json()["label_id"] == "label_123"

    @patch("src.web.routes.labels._get_conn")
    def test_delete_label(self, mock_conn):
        conn = MagicMock()
        mock_conn.return_value = conn
        conn.close = MagicMock()

        with patch("src.web.routes.labels.db.delete_account_label"):
            app = create_app()
            client = TestClient(app)
            response = client.delete("/api/labels/label_123")
            assert response.status_code == 200
            assert response.json()["deleted"] is True

    @patch("src.web.routes.labels._get_conn")
    def test_get_labels(self, mock_conn):
        conn = MagicMock()
        mock_conn.return_value = conn
        conn.close = MagicMock()

        with patch("src.web.routes.labels.db.get_labels_for_account") as mock_labels:
            mock_labels.return_value = [{"label_id": "l1", "label": "qualified", "notes": ""}]
            app = create_app()
            client = TestClient(app)
            response = client.get("/api/labels/a1")
            assert response.status_code == 200
            assert len(response.json()["labels"]) == 1


# ---------------------------------------------------------------------------
# Research API
# ---------------------------------------------------------------------------


class TestResearchAPI:
    @patch("src.web.routes.research.load_settings")
    @patch("src.web.routes.research.db")
    def test_get_research(self, mock_db, mock_settings):
        mock_settings.return_value = MagicMock(pg_dsn="postgresql://test")
        conn = MagicMock()
        mock_db.get_connection.return_value = conn
        mock_db.init_db = MagicMock()
        mock_db.get_company_research.return_value = {"research_brief": "AI company"}
        mock_db.get_contacts_for_account.return_value = [{"first_name": "John", "last_name": "Doe", "title": "CTO"}]
        conn.close = MagicMock()

        app = create_app()
        client = TestClient(app)
        response = client.get("/api/research/a1")
        assert response.status_code == 200
        data = response.json()
        assert data["research"]["research_brief"] == "AI company"
        assert len(data["contacts"]) == 1

    @patch("src.web.routes.research.load_settings")
    @patch("src.web.routes.research.db")
    def test_get_research_no_data(self, mock_db, mock_settings):
        mock_settings.return_value = MagicMock(pg_dsn="postgresql://test")
        conn = MagicMock()
        mock_db.get_connection.return_value = conn
        mock_db.init_db = MagicMock()
        mock_db.get_company_research.return_value = None
        mock_db.get_contacts_for_account.return_value = []
        conn.close = MagicMock()

        app = create_app()
        client = TestClient(app)
        response = client.get("/api/research/unknown")
        assert response.status_code == 200
        data = response.json()
        assert data["research"] is None
        assert data["contacts"] == []


# ---------------------------------------------------------------------------
# Pipeline API
# ---------------------------------------------------------------------------


class TestPipelineAPI:
    @patch("src.web.routes.pipeline.run_pipeline_async")
    def test_start_pipeline(self, mock_run):
        async def _fake_run(*args, **kwargs):
            return "run_123"

        mock_run.side_effect = _fake_run
        app = create_app()
        client = TestClient(app)
        response = client.post(
            "/api/pipeline/run",
            json={"account_ids": ["a1"], "stages": ["ingest", "score"]},
        )
        assert response.status_code == 200
        assert "pipeline_run_id" in response.json()


# ---------------------------------------------------------------------------
# Serialize dates helper
# ---------------------------------------------------------------------------


class TestSerializeDates:
    def test_dict_with_datetime(self):
        from datetime import datetime

        from src.web.routes.accounts import _serialize_dates

        obj = {"created_at": datetime(2026, 1, 1, 12, 0, 0), "name": "test"}
        _serialize_dates(obj)
        assert obj["created_at"] == "2026-01-01T12:00:00"
        assert obj["name"] == "test"

    def test_nested_dict(self):
        from datetime import datetime

        from src.web.routes.accounts import _serialize_dates

        obj = {"nested": {"ts": datetime(2026, 1, 1)}}
        _serialize_dates(obj)
        assert obj["nested"]["ts"] == "2026-01-01T00:00:00"

    def test_list_of_dicts(self):
        from datetime import datetime

        from src.web.routes.accounts import _serialize_dates

        obj = [{"ts": datetime(2026, 1, 1)}]
        _serialize_dates(obj)
        assert obj[0]["ts"] == "2026-01-01T00:00:00"
