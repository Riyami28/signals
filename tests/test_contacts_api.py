"""Tests for contact discovery, enrichment, warm paths, and DB functions."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from src import db
from src.settings import load_settings

# Re-use conftest helpers
from tests.conftest import make_account


# ---------------------------------------------------------------------------
# DB function tests
# ---------------------------------------------------------------------------


class TestUpsertSingleContact:
    def test_insert_new_contact(self):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            account_id = make_account(conn, domain="test.com", company_name="Test Inc")
            contact = {
                "account_id": account_id,
                "first_name": "Jane",
                "last_name": "Doe",
                "title": "VP Engineering",
                "email": "jane@test.com",
                "linkedin_url": "https://linkedin.com/in/janedoe",
                "management_level": "VP",
                "contact_status": "discovered",
                "enrichment_source": "apollo",
            }
            contact_id = db.upsert_single_contact(conn, contact)
            assert contact_id.startswith("contact_")

            # Verify in DB
            fetched = db.get_contact_by_id(conn, contact_id)
            assert fetched is not None
            assert fetched["first_name"] == "Jane"
            assert fetched["last_name"] == "Doe"
            assert fetched["management_level"] == "VP"
            assert fetched["contact_status"] == "discovered"
        finally:
            conn.close()

    def test_upsert_preserves_enrichment(self):
        """Re-upserting a contact should not overwrite enriched fields with empty values."""
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            account_id = make_account(conn, domain="test2.com", company_name="Test2 Inc")
            contact = {
                "account_id": account_id,
                "first_name": "John",
                "last_name": "Smith",
                "title": "CTO",
                "email": "john@test2.com",
                "linkedin_url": "https://linkedin.com/in/johnsmith",
                "management_level": "C-Level",
                "contact_status": "ranked",
                "semantic_role": "Technical Champion",
                "authority_score": 0.9,
                "enrichment_source": "apollo",
            }
            contact_id = db.upsert_single_contact(conn, contact)

            # Re-upsert with empty semantic_role (simulating re-discovery)
            contact2 = {
                "account_id": account_id,
                "first_name": "John",
                "last_name": "Smith",
                "title": "CTO",
                "email": "",
                "linkedin_url": "https://linkedin.com/in/johnsmith",
                "management_level": "C-Level",
                "contact_status": "discovered",
                "semantic_role": "",
                "authority_score": 0.0,
            }
            contact_id2 = db.upsert_single_contact(conn, contact2)
            assert contact_id == contact_id2

            fetched = db.get_contact_by_id(conn, contact_id)
            # Ranked status should not be downgraded to discovered
            assert fetched["contact_status"] == "ranked"
            # Semantic role should be preserved
            assert fetched["semantic_role"] == "Technical Champion"
            # Authority score should be preserved
            assert fetched["authority_score"] == 0.9
            # Email should be preserved (non-empty wins)
            assert fetched["email"] == "john@test2.com"
        finally:
            conn.close()


class TestUpdateContactEnrichment:
    def test_partial_update(self):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            account_id = make_account(conn, domain="enrich.com", company_name="Enrich Inc")
            contact = {
                "account_id": account_id,
                "first_name": "Alice",
                "last_name": "Wonder",
                "title": "Director",
                "management_level": "Director",
                "contact_status": "discovered",
            }
            contact_id = db.upsert_single_contact(conn, contact)

            # Update just email and status
            result = db.update_contact_enrichment(conn, contact_id, {
                "email": "alice@enrich.com",
                "email_verified": True,
                "verification_status": "valid",
                "contact_status": "verified",
            })
            assert result is True

            fetched = db.get_contact_by_id(conn, contact_id)
            assert fetched["email"] == "alice@enrich.com"
            assert fetched["email_verified"] is True
            assert fetched["verification_status"] == "valid"
            assert fetched["contact_status"] == "verified"
            # Original fields preserved
            assert fetched["first_name"] == "Alice"
            assert fetched["management_level"] == "Director"
        finally:
            conn.close()

    def test_ignores_disallowed_fields(self):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            account_id = make_account(conn, domain="safe.com", company_name="Safe Inc")
            contact = {
                "account_id": account_id,
                "first_name": "Bob",
                "last_name": "Safe",
                "management_level": "IC",
                "contact_status": "discovered",
            }
            contact_id = db.upsert_single_contact(conn, contact)

            # Try updating disallowed field
            result = db.update_contact_enrichment(conn, contact_id, {
                "account_id": "hacked",
                "contact_id": "hacked",
            })
            assert result is False
        finally:
            conn.close()


class TestGetContactById:
    def test_returns_none_for_missing(self):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            assert db.get_contact_by_id(conn, "nonexistent") is None
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Internal network tests
# ---------------------------------------------------------------------------


class TestLoadInternalNetwork:
    def test_load_csv(self, tmp_path):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            csv_file = tmp_path / "network.csv"
            csv_file.write_text(
                "team_member,connection_name,connection_linkedin_url,connection_title,"
                "connection_company,past_companies,relationship_type\n"
                "Rajesh Kumar,Anjali Singh,https://linkedin.com/in/anjali,VP Eng,"
                "Tata Digital,Infosys;Wipro,connection\n"
            )
            count = db.load_internal_network(conn, str(csv_file))
            assert count == 1

            # Verify data
            matches = db.find_network_matches(
                conn, "Anjali Singh", "https://linkedin.com/in/anjali"
            )
            assert len(matches) >= 1
            assert matches[0]["match_type"] == "linkedin"
            assert matches[0]["team_member"] == "Rajesh Kumar"
        finally:
            conn.close()

    def test_load_empty_csv(self, tmp_path):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            csv_file = tmp_path / "empty.csv"
            csv_file.write_text(
                "team_member,connection_name,connection_linkedin_url,connection_title,"
                "connection_company,past_companies,relationship_type\n"
            )
            count = db.load_internal_network(conn, str(csv_file))
            assert count == 0
        finally:
            conn.close()


class TestFindNetworkMatches:
    def test_linkedin_match(self):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            # Insert a network entry directly
            from src.utils import stable_hash

            network_id = stable_hash(
                {"team_member": "VP Sales", "connection_name": "Target DM", "linkedin": "https://linkedin.com/in/target"},
                prefix="net",
                length=16,
            )
            conn.execute(
                """
                INSERT INTO internal_network
                    (network_id, team_member, connection_name, connection_linkedin_url,
                     connection_title, connection_company, past_companies, relationship_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (network_id, "VP Sales", "Target DM", "https://linkedin.com/in/target",
                 "CTO", "BigCorp", "acme;google", "connection"),
            )
            conn.commit()

            matches = db.find_network_matches(conn, "Target DM", "https://linkedin.com/in/target")
            assert len(matches) == 1
            assert matches[0]["match_type"] == "linkedin"
        finally:
            conn.close()

    def test_name_match_fallback(self):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            from src.utils import stable_hash

            network_id = stable_hash(
                {"team_member": "SDR Lead", "connection_name": "Name Only", "linkedin": ""},
                prefix="net",
                length=16,
            )
            conn.execute(
                """
                INSERT INTO internal_network
                    (network_id, team_member, connection_name, connection_linkedin_url,
                     connection_title, connection_company, past_companies, relationship_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (network_id, "SDR Lead", "Name Only", "",
                 "VP Eng", "SomeCo", "", "connection"),
            )
            conn.commit()

            # No linkedin_url, match by name
            matches = db.find_network_matches(conn, "Name Only", "")
            assert len(matches) == 1
            assert matches[0]["match_type"] == "name"
        finally:
            conn.close()

    def test_no_match(self):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            matches = db.find_network_matches(conn, "Nobody Here", "https://linkedin.com/in/nobody")
            assert matches == []
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Warm path scoring tests
# ---------------------------------------------------------------------------


class TestWarmPathScoring:
    def test_linkedin_match_scores(self):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            from src.utils import stable_hash
            from src.warm_path import compute_warm_paths

            # Insert network entry
            network_id = stable_hash(
                {"team_member": "Taran", "connection_name": "DM Person", "linkedin": "https://linkedin.com/in/dm"},
                prefix="net",
                length=16,
            )
            conn.execute(
                """
                INSERT INTO internal_network
                    (network_id, team_member, connection_name, connection_linkedin_url,
                     connection_title, connection_company, past_companies, relationship_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (network_id, "Taran", "DM Person", "https://linkedin.com/in/dm",
                 "CTO", "Target Co", "tata;google", "connection"),
            )
            conn.commit()

            contacts = [
                {
                    "first_name": "DM",
                    "last_name": "Person",
                    "linkedin_url": "https://linkedin.com/in/dm",
                    "title": "CTO",
                },
            ]
            result = compute_warm_paths(conn, contacts, "example.com")
            assert result[0]["warmth_score"] == pytest.approx(0.6)
            assert "Taran" in result[0]["warm_path_reason"]
        finally:
            conn.close()

    def test_company_overlap_adds_score(self):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            from src.utils import stable_hash
            from src.warm_path import compute_warm_paths

            network_id = stable_hash(
                {"team_member": "Sales VP", "connection_name": "Overlap DM",
                 "linkedin": "https://linkedin.com/in/overlap"},
                prefix="net",
                length=16,
            )
            conn.execute(
                """
                INSERT INTO internal_network
                    (network_id, team_member, connection_name, connection_linkedin_url,
                     connection_title, connection_company, past_companies, relationship_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (network_id, "Sales VP", "Overlap DM", "https://linkedin.com/in/overlap",
                 "VP Eng", "Current Co", "tatadigital;infosys", "connection"),
            )
            conn.commit()

            contacts = [
                {
                    "first_name": "Overlap",
                    "last_name": "DM",
                    "linkedin_url": "https://linkedin.com/in/overlap",
                    "title": "VP Eng",
                },
            ]
            # Domain "tatadigital.com" → first part "tatadigital" matches past_companies
            result = compute_warm_paths(conn, contacts, "tatadigital.com")
            # 0.6 (linkedin) + 0.2 (company overlap) = 0.8
            assert result[0]["warmth_score"] == pytest.approx(0.8)
        finally:
            conn.close()

    def test_no_match_zero_warmth(self):
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        try:
            from src.warm_path import compute_warm_paths

            contacts = [
                {
                    "first_name": "Unknown",
                    "last_name": "Person",
                    "linkedin_url": "",
                    "title": "IC",
                },
            ]
            result = compute_warm_paths(conn, contacts, "random.com")
            assert result[0]["warmth_score"] == 0.0
            assert result[0]["warm_path_reason"] == ""
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# API route tests (with mocked external services)
# ---------------------------------------------------------------------------


class TestDiscoverContactsAPI:
    def test_discover_no_apollo_key(self):
        """Without Apollo key, should return existing contacts or empty."""
        from fastapi.testclient import TestClient
        from src.web.app import create_app

        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        account_id = make_account(conn, domain="noapollo.com", company_name="No Apollo")
        conn.close()

        app = create_app()
        client = TestClient(app)

        with patch("src.web.routes.contacts.load_settings") as mock_settings:
            s = MagicMock()
            s.pg_dsn = settings.pg_dsn
            s.apollo_api_key = ""
            s.hunter_api_key = ""
            s.claude_api_key = ""
            s.minimax_api_key = ""
            s.project_root = settings.project_root
            s.apollo_rate_limit = 50
            mock_settings.return_value = s

            resp = client.post(f"/api/contacts/{account_id}/discover")
            assert resp.status_code == 200
            data = resp.json()
            assert data["total_discovered"] == 0

    @patch("src.web.routes.contacts.search_contacts_for_account")
    def test_discover_with_apollo(self, mock_search):
        """With Apollo, should store and return discovered contacts."""
        from fastapi.testclient import TestClient
        from src.web.app import create_app

        mock_search.return_value = [
            {
                "first_name": "Test",
                "last_name": "User",
                "title": "CTO",
                "email": "test@company.com",
                "linkedin_url": "https://linkedin.com/in/test",
                "management_level": "C-Level",
                "year_joined": None,
            },
        ]

        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        account_id = make_account(conn, domain="apollo.com", company_name="Apollo Co")
        conn.close()

        app = create_app()
        client = TestClient(app)

        with patch("src.web.routes.contacts.load_settings") as mock_settings:
            s = MagicMock()
            s.pg_dsn = settings.pg_dsn
            s.apollo_api_key = "test-key"
            s.hunter_api_key = ""
            s.claude_api_key = ""
            s.minimax_api_key = ""
            s.project_root = settings.project_root
            s.apollo_rate_limit = 50
            mock_settings.return_value = s

            resp = client.post(f"/api/contacts/{account_id}/discover")
            assert resp.status_code == 200
            data = resp.json()
            assert data["total_discovered"] == 1
            assert len(data["contacts"]) == 1
            assert data["contacts"][0]["first_name"] == "Test"
            assert data["contacts"][0]["contact_status"] == "discovered"


class TestEnrichContactAPI:
    @patch("src.web.routes.contacts.EmailVerifier")
    @patch("src.web.routes.contacts.ApolloClient")
    def test_enrich_with_email_verification(self, mock_apollo_cls, mock_verifier_cls):
        """Enrichment should verify email and update contact."""
        from fastapi.testclient import TestClient
        from src.web.app import create_app

        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        account_id = make_account(conn, domain="verify.com", company_name="Verify Inc")

        contact = {
            "account_id": account_id,
            "first_name": "Verify",
            "last_name": "Me",
            "title": "VP Eng",
            "email": "verify@verify.com",
            "management_level": "VP",
            "contact_status": "discovered",
            "enrichment_source": "apollo",
        }
        contact_id = db.upsert_single_contact(conn, contact)
        conn.close()

        # Mock Apollo enrich
        mock_apollo = MagicMock()
        mock_apollo.enrich_person.return_value = None
        mock_apollo_cls.return_value = mock_apollo

        # Mock email verification
        mock_verifier = MagicMock()
        mock_verifier.is_configured = True
        mock_verify_result = MagicMock()
        mock_verify_result.email_verified = True
        mock_verify_result.status.value = "valid"
        mock_verify_result.should_store = True
        mock_verifier.verify_with_retry.return_value = mock_verify_result
        mock_verifier_cls.return_value = mock_verifier

        app = create_app()
        client = TestClient(app)

        with patch("src.web.routes.contacts.load_settings") as mock_settings:
            s = MagicMock()
            s.pg_dsn = settings.pg_dsn
            s.apollo_api_key = "test-key"
            s.hunter_api_key = ""
            s.apollo_rate_limit = 50
            mock_settings.return_value = s

            resp = client.post(f"/api/contacts/{contact_id}/enrich")
            assert resp.status_code == 200
            data = resp.json()
            assert data["contact"]["email_verified"] is True
            assert data["contact"]["contact_status"] == "verified"
            assert data["contact"]["verification_status"] == "valid"

    def test_enrich_nonexistent_contact(self):
        """Should return 404 for missing contact."""
        from fastapi.testclient import TestClient
        from src.web.app import create_app

        app = create_app()
        client = TestClient(app)
        resp = client.post("/api/contacts/nonexistent_id/enrich")
        assert resp.status_code == 404
