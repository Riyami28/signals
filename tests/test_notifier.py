"""Tests for src/notifier.py — multi-channel alerting, fallback behavior."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.notifier import _append_local_alert, _send_gchat, send_alert
from src.settings import Settings


@pytest.fixture
def settings(tmp_path):
    return Settings(
        project_root=tmp_path,
        gchat_webhook_url="",
        alert_email_to="",
        alert_email_from="",
        alert_smtp_host="",
    )


@pytest.fixture
def settings_with_gchat(tmp_path):
    return Settings(
        project_root=tmp_path,
        gchat_webhook_url="https://chat.googleapis.com/webhook/test",
        alert_email_to="",
        alert_email_from="",
        alert_smtp_host="",
    )


@pytest.fixture
def settings_with_email(tmp_path):
    return Settings(
        project_root=tmp_path,
        gchat_webhook_url="",
        alert_email_to="admin@example.com",
        alert_email_from="signals@example.com",
        alert_smtp_host="smtp.example.com",
        alert_smtp_port=587,
        alert_smtp_user="user",
        alert_smtp_password="pass",
    )


# ---------------------------------------------------------------------------
# Local alert fallback
# ---------------------------------------------------------------------------


class TestAppendLocalAlert:
    def test_creates_file_and_writes(self, tmp_path):
        alert_path = tmp_path / "out" / "alerts.log"
        _append_local_alert(alert_path, "error", "Test Title", "Test body")
        assert alert_path.exists()
        content = alert_path.read_text()
        assert "severity=error" in content
        assert "title=Test Title" in content
        assert "body=Test body" in content

    def test_appends_multiple_alerts(self, tmp_path):
        alert_path = tmp_path / "alerts.log"
        _append_local_alert(alert_path, "error", "First", "body1")
        _append_local_alert(alert_path, "warning", "Second", "body2")
        lines = alert_path.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_creates_parent_dirs(self, tmp_path):
        alert_path = tmp_path / "deep" / "nested" / "alerts.log"
        _append_local_alert(alert_path, "info", "Test", "body")
        assert alert_path.exists()


# ---------------------------------------------------------------------------
# send_alert
# ---------------------------------------------------------------------------


class TestSendAlert:
    def test_local_only_when_no_channels_configured(self, settings):
        result = send_alert(settings, "Test", "body")
        assert "local_log" in result["delivered_channels"]
        assert len(result["errors"]) == 0

    @patch("src.notifier._send_gchat")
    def test_gchat_success(self, mock_gchat, settings_with_gchat):
        result = send_alert(settings_with_gchat, "Test", "body")
        assert "gchat" in result["delivered_channels"]
        assert "local_log" in result["delivered_channels"]
        mock_gchat.assert_called_once()

    @patch("src.notifier._send_gchat", side_effect=Exception("Connection failed"))
    def test_gchat_failure_recorded(self, mock_gchat, settings_with_gchat):
        result = send_alert(settings_with_gchat, "Test", "body")
        assert "gchat" not in result["delivered_channels"]
        assert any("gchat:" in e for e in result["errors"])
        assert "local_log" in result["delivered_channels"]

    @patch("src.notifier._send_email")
    def test_email_success(self, mock_email, settings_with_email):
        result = send_alert(settings_with_email, "Test", "body")
        assert "email" in result["delivered_channels"]
        mock_email.assert_called_once()

    @patch("src.notifier._send_email", side_effect=Exception("SMTP error"))
    def test_email_failure_recorded(self, mock_email, settings_with_email):
        result = send_alert(settings_with_email, "Test", "body")
        assert "email" not in result["delivered_channels"]
        assert any("email:" in e for e in result["errors"])

    def test_default_severity_is_error(self, settings):
        send_alert(settings, "Title", "Body")
        alert_log = (settings.out_dir / "alerts.log").read_text()
        assert "severity=error" in alert_log

    def test_custom_severity(self, settings):
        send_alert(settings, "Title", "Body", severity="warning")
        alert_log = (settings.out_dir / "alerts.log").read_text()
        assert "severity=warning" in alert_log

    @patch("src.notifier._send_gchat")
    @patch("src.notifier._send_email")
    def test_all_channels_success(self, mock_email, mock_gchat, tmp_path):
        s = Settings(
            project_root=tmp_path,
            gchat_webhook_url="https://webhook.test",
            alert_email_to="to@test.com",
            alert_email_from="from@test.com",
            alert_smtp_host="smtp.test.com",
        )
        result = send_alert(s, "Multi", "body")
        assert "gchat" in result["delivered_channels"]
        assert "email" in result["delivered_channels"]
        assert "local_log" in result["delivered_channels"]
        assert len(result["errors"]) == 0


# ---------------------------------------------------------------------------
# _send_gchat
# ---------------------------------------------------------------------------


class TestSendGchat:
    @patch("src.notifier.requests.post")
    def test_sends_correct_payload(self, mock_post):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        _send_gchat("https://webhook.test", "error", "Alert Title", "Alert body")
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert "[signals:error]" in payload["text"]
        assert "Alert Title" in payload["text"]
        assert "Alert body" in payload["text"]
