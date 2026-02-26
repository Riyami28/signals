"""Tests for batch pipeline trigger API and batch processing (Issue #38)."""

from __future__ import annotations

from src import db
from src.settings import load_settings

# ---------------------------------------------------------------------------
# DB-level batch CRUD tests
# ---------------------------------------------------------------------------


class TestBatchDBCrud:
    """Test upload_batches and batch_companies CRUD operations."""

    def _conn(self):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        return conn

    def test_create_and_get_upload_batch(self):
        conn = self._conn()
        try:
            batch_id = db.create_upload_batch(conn, "batch_test1", "companies.csv", 5, {"source": "web"})
            assert batch_id == "batch_test1"

            batch = db.get_upload_batch(conn, batch_id)
            assert batch is not None
            assert batch["filename"] == "companies.csv"
            assert batch["row_count"] == 5
            assert batch["status"] == "pending"
        finally:
            conn.close()

    def test_update_batch_status(self):
        conn = self._conn()
        try:
            db.create_upload_batch(conn, "batch_status", "test.csv", 3)
            db.update_batch_status(conn, "batch_status", "processing")

            batch = db.get_upload_batch(conn, "batch_status")
            assert batch["status"] == "processing"

            db.update_batch_status(conn, "batch_status", "scored")
            batch = db.get_upload_batch(conn, "batch_status")
            assert batch["status"] == "scored"
        finally:
            conn.close()

    def test_get_nonexistent_batch_returns_none(self):
        conn = self._conn()
        try:
            batch = db.get_upload_batch(conn, "nonexistent_batch")
            assert batch is None
        finally:
            conn.close()

    def test_insert_and_get_batch_companies(self):
        conn = self._conn()
        try:
            db.create_upload_batch(conn, "batch_co", "test.csv", 2)

            row_id_1 = db.insert_batch_company(conn, "batch_co", "Acme Inc", "acme.com", industry="SaaS")
            row_id_2 = db.insert_batch_company(conn, "batch_co", "Beta Corp", "beta.io", employee_count=250)
            conn.commit()

            assert row_id_1 > 0
            assert row_id_2 > row_id_1

            companies = db.get_batch_companies(conn, "batch_co")
            assert len(companies) == 2
            assert companies[0]["company_name"] == "Acme Inc"
            assert companies[0]["domain"] == "acme.com"
            assert companies[1]["company_name"] == "Beta Corp"
            assert companies[1]["employee_count"] == 250
        finally:
            conn.close()

    def test_link_batch_company_account(self):
        conn = self._conn()
        try:
            db.create_upload_batch(conn, "batch_link", "test.csv", 1)
            row_id = db.insert_batch_company(conn, "batch_link", "Acme", "acme.com")
            conn.commit()

            account_id = db.upsert_account(conn, "Acme", "acme.com", source_type="discovered")
            db.link_batch_company_account(conn, row_id, account_id)
            conn.commit()

            companies = db.get_batch_companies(conn, "batch_link")
            assert companies[0]["account_id"] == account_id
        finally:
            conn.close()

    def test_get_batch_results_empty(self):
        conn = self._conn()
        try:
            db.create_upload_batch(conn, "batch_empty", "test.csv", 0)
            results = db.get_batch_results(conn, "batch_empty")
            assert results == []
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# API-level tests (using FastAPI TestClient)
# ---------------------------------------------------------------------------


class TestBatchAPI:
    """Test batch API endpoints."""

    def _setup_batch(self, conn, batch_id="batch_api_test"):
        """Create a batch with companies for testing."""
        db.create_upload_batch(conn, batch_id, "test.csv", 2)
        id1 = db.insert_batch_company(conn, batch_id, "Acme", "acme.example")
        id2 = db.insert_batch_company(conn, batch_id, "Beta", "beta.example")
        conn.commit()

        acct1 = db.upsert_account(conn, "Acme", "acme.example", source_type="discovered")
        acct2 = db.upsert_account(conn, "Beta", "beta.example", source_type="discovered")
        db.link_batch_company_account(conn, id1, acct1)
        db.link_batch_company_account(conn, id2, acct2)
        conn.commit()
        return batch_id, [acct1, acct2]

    def test_get_batch_status(self):
        from fastapi.testclient import TestClient

        from src.web.app import create_app

        app = create_app()
        client = TestClient(app)

        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            self._setup_batch(conn, "batch_status_api")
        finally:
            conn.close()

        response = client.get("/api/batches/batch_status_api")
        assert response.status_code == 200
        data = response.json()
        assert data["batch_id"] == "batch_status_api"
        assert data["status"] == "pending"
        assert data["row_count"] == 2

    def test_get_batch_not_found(self):
        from fastapi.testclient import TestClient

        from src.web.app import create_app

        app = create_app()
        client = TestClient(app)

        response = client.get("/api/batches/nonexistent")
        assert response.status_code == 404

    def test_get_batch_results_json(self):
        from fastapi.testclient import TestClient

        from src.web.app import create_app

        app = create_app()
        client = TestClient(app)

        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            self._setup_batch(conn, "batch_results_json")
        finally:
            conn.close()

        response = client.get("/api/batches/batch_results_json/results")
        assert response.status_code == 200
        data = response.json()
        assert data["batch_id"] == "batch_results_json"
        assert "results" in data
        assert "count" in data

    def test_get_batch_results_csv(self):
        from fastapi.testclient import TestClient

        from src.web.app import create_app

        app = create_app()
        client = TestClient(app)

        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            self._setup_batch(conn, "batch_results_csv")
        finally:
            conn.close()

        response = client.get("/api/batches/batch_results_csv/results?format=csv")
        # May be 200 with CSV or 404 if no scores yet
        assert response.status_code in (200, 404)
        if response.status_code == 200:
            assert "text/csv" in response.headers.get("content-type", "")

    def test_pipeline_run_with_batch_id_not_found(self):
        from fastapi.testclient import TestClient

        from src.web.app import create_app

        app = create_app()
        client = TestClient(app)

        response = client.post(
            "/api/pipeline/run",
            json={
                "batch_id": "nonexistent_batch",
                "stages": ["score"],
            },
        )
        assert response.status_code == 404

    def test_pipeline_run_with_batch_id_returns_run_id(self):
        from fastapi.testclient import TestClient

        from src.web.app import create_app

        app = create_app()
        client = TestClient(app)

        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            self._setup_batch(conn, "batch_pipeline_test")
        finally:
            conn.close()

        response = client.post(
            "/api/pipeline/run",
            json={
                "batch_id": "batch_pipeline_test",
                "stages": ["score"],
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "pipeline_run_id" in data
        assert data["pipeline_run_id"].startswith("prun_")
        assert data["batch_id"] == "batch_pipeline_test"

    def test_pipeline_run_backward_compat_without_batch(self):
        """Existing behavior should work unchanged when batch_id is not provided."""
        from fastapi.testclient import TestClient

        from src.web.app import create_app

        app = create_app()
        client = TestClient(app)

        response = client.post(
            "/api/pipeline/run",
            json={
                "stages": ["score"],
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "pipeline_run_id" in data
        assert data["batch_id"] is None
