from __future__ import annotations

import smtplib
import tempfile
import unittest
from html.parser import HTMLParser
from pathlib import Path
from unittest.mock import patch

from flaskr import create_app
from flaskr.auth import admin_session_version
from flaskr.db import get_db


class EmailFormParser(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.elements = []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        self.elements.append((tag, dict(attrs)))


class TournamentEmailTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        (root / "rating").mkdir()
        for name in ("anonymous.txt", "not_anonymous.txt"):
            (root / "rating" / name).write_text("")
        self.app = create_app({
            "TESTING": True,
            "SECRET_KEY": "test-email",
            "INSTANCE_PATH": root,
            "MAIL_ENABLED": True,
            "MAIL_HOST": "smtp.example.com",
            "MAIL_FROM_EMAIL": "club@example.com",
            "MAIL_SUPPRESS_SEND": True,
        })
        self.client = self.app.test_client()
        with self.app.app_context(), self.client.session_transaction() as session:
            session["is_admin"] = True
            session["admin_password_version"] = admin_session_version()
        self.tournament_id = self.tournament()

    def tearDown(self):
        self.tempdir.cleanup()

    def tournament(self, slug="cup", *, team=False):
        with self.app.app_context():
            db = get_db()
            cursor = db.execute(
                """
                INSERT INTO tournament (name, slug, event_date, rounds_planned, is_team, excludes_rating)
                VALUES (?, ?, '2026-10-15', 1, ?, ?)
                """,
                (slug.title(), slug, int(team), int(team)),
            )
            db.commit()
            return cursor.lastrowid

    def entry(self, name, email=None, *, waiting=False, active=False, tournament_id=None, player_email=None):
        with self.app.app_context():
            db = get_db()
            player_id = None
            if player_email:
                player_id = db.execute(
                    "INSERT INTO player (name, normalized_name, email) VALUES (?, ?, ?)",
                    (name, name.lower(), player_email),
                ).lastrowid
            cursor = db.execute(
                """
                INSERT INTO tournament_entry (
                    tournament_id, player_id, imported_name, imported_email,
                    seed_rating, member_status, is_active, waitlist_position
                ) VALUES (?, ?, ?, ?, 1500, 'unknown', ?, ?)
                """,
                (tournament_id or self.tournament_id, player_id, name, email, int(active), 1 if waiting else None),
            )
            db.commit()
            return cursor.lastrowid

    def member(self, name, email, *, entry_id=None, waiting=False, tournament_id=None):
        with self.app.app_context():
            db = get_db()
            db.execute(
                """
                INSERT INTO team_member (tournament_id, entry_id, name, email, waitlist_position)
                VALUES (?, ?, ?, ?, ?)
                """,
                (tournament_id or self.tournament_id, entry_id, name, email, 1 if waiting else None),
            )
            db.commit()

    def send(self, audience="participants", **overrides):
        return self.client.post("/admin/t/cup/email", data={
            "audience": audience,
            "subject": "Venue update — Schach",
            "message": "Hello everyone,\n\nWe meet in CAB.\nSee you soon!",
            **overrides,
        }, follow_redirects=True)

    def outbox(self):
        return self.app.extensions.get("mail_outbox", [])

    def test_individual_audiences_include_unchecked_registrations_and_use_player_email_fallback(self):
        self.entry("Present", "present@example.com", active=True)
        self.entry("Not checked in", "absent@example.com")
        self.entry("Player email only", " ", player_email="fallback@example.com")
        self.entry("Registration email wins", "registration@example.com", player_email="old@example.com")
        self.entry("Waiting", "waiting@example.com", waiting=True)
        self.entry("No contact")
        other = self.tournament("other")
        self.entry("Other tournament", "other@example.com", tournament_id=other)
        participants = {"present@example.com", "absent@example.com", "fallback@example.com", "registration@example.com"}
        expected = {
            "participants": participants,
            "waitlist": {"waiting@example.com"},
            "both": participants | {"waiting@example.com"},
        }
        for audience, recipients in expected.items():
            with self.subTest(audience=audience):
                self.app.extensions["mail_outbox"] = []
                response = self.send(audience)
                self.assertEqual(response.status_code, 200)
                self.assertEqual({mail["to"] for mail in self.outbox()}, recipients)
                self.assertEqual(len(self.outbox()), len(recipients))
                for mail in self.outbox():
                    self.assertEqual(mail["subject"], "Venue update — Schach")
                    self.assertEqual(mail["body"], "Hello everyone,\n\nWe meet in CAB.\nSee you soon!\n")

    def test_addresses_are_trimmed_and_deduplicated_across_both_groups(self):
        self.entry("First", " Alex@example.com ")
        self.entry("Duplicate", "alex@example.com")
        self.entry("Waiting duplicate", "ALEX@example.com", waiting=True)
        response = self.send("both")
        self.assertEqual(response.status_code, 200)
        self.assertEqual([mail["to"] for mail in self.outbox()], ["Alex@example.com"])
        self.assertIn(b"Email sent to 1 recipient.", response.data)

    def test_team_audiences_include_every_member_and_unassigned_solos(self):
        with self.app.app_context():
            get_db().execute("UPDATE tournament SET is_team = 1, excludes_rating = 1 WHERE id = ?", (self.tournament_id,))
            get_db().commit()
        confirmed = self.entry("Rooks", "alex@example.com")
        waiting = self.entry("Bishops", "pat@example.com", waiting=True)
        self.member("Alex", "alex@example.com", entry_id=confirmed)
        self.member("Sam", "sam@example.com", entry_id=confirmed)
        self.member("Pat", "pat@example.com", entry_id=waiting)
        self.member("Lee", "lee@example.com", entry_id=waiting)
        self.member("Confirmed solo", "solo@example.com")
        self.member("Waiting solo", "waiting-solo@example.com", waiting=True)
        self.member("No contact", None)
        other = self.tournament("other", team=True)
        self.member("Other tournament", "other@example.com", tournament_id=other)
        participants = {"alex@example.com", "sam@example.com", "solo@example.com"}
        waitlist = {"pat@example.com", "lee@example.com", "waiting-solo@example.com"}
        for audience, recipients in {"participants": participants, "waitlist": waitlist, "both": participants | waitlist}.items():
            with self.subTest(audience=audience):
                self.app.extensions["mail_outbox"] = []
                response = self.send(audience)
                self.assertEqual(response.status_code, 200)
                self.assertEqual({mail["to"] for mail in self.outbox()}, recipients)
                self.assertEqual(len(self.outbox()), len(recipients))

    def test_invalid_input_never_sends_and_preserves_escaped_draft(self):
        self.entry("Player", "player@example.com")
        cases = (
            {"audience": ""}, {"audience": "everyone"}, {"subject": " "},
            {"message": "\n "}, {"subject": "x" * 201},
            {"subject": "Hello\r\nBcc: unexpected@example.com"},
        )
        for fields in cases:
            with self.subTest(fields=fields):
                response = self.send(**fields)
                self.assertEqual(response.status_code, 400)
                self.assertEqual(self.outbox(), [])
                self.assertIn(b"<details open>", response.data)
        response = self.send(subject="", message="</textarea><script>bad()</script>")
        self.assertIn(b"&lt;/textarea&gt;&lt;script&gt;bad()&lt;/script&gt;", response.data)
        self.assertNotIn(b"<script>bad()</script>", response.data)

    def test_empty_audience_and_unconfigured_mail_do_not_send(self):
        self.entry("No email")
        response = self.send()
        self.assertEqual(response.status_code, 400)
        self.assertIn(b"No email addresses", response.data)
        self.entry("Player", "player@example.com")
        for setting, value in (("MAIL_ENABLED", False), ("MAIL_HOST", None), ("MAIL_FROM_EMAIL", None)):
            with self.subTest(setting=setting), patch.dict(self.app.config, {setting: value}):
                response = self.send()
                self.assertEqual(response.status_code, 400)
                self.assertIn(b"Email delivery is not configured", response.data)
                self.assertEqual(self.outbox(), [])

    def test_partial_delivery_reports_failures_and_continues_with_private_emails(self):
        self.entry("First", "first@example.com")
        self.entry("Second", "second@example.com")
        self.app.config["MAIL_SUPPRESS_SEND"] = False
        with patch("flaskr.mailer.smtplib.SMTP") as smtp, self.assertLogs(self.app.logger, level="ERROR"):
            smtp.return_value.__enter__.return_value.send_message.side_effect = [
                smtplib.SMTPException("Temporary delivery failure"), None,
            ]
            response = self.send()
        self.assertEqual(response.status_code, 502)
        self.assertIn(b"Sent 1 of 2 emails. 1 could not be sent.", response.data)
        self.assertIn(b"already received the message", response.data)
        self.assertIn(b"Hello everyone,", response.data)
        messages = smtp.return_value.__enter__.return_value.send_message.call_args_list
        self.assertEqual([call.args[0]["To"] for call in messages], ["first@example.com", "second@example.com"])
        for call in messages:
            self.assertIsNone(call.args[0]["Cc"])
            self.assertIsNone(call.args[0]["Bcc"])

    def test_invalid_stored_email_headers_do_not_abort_the_remaining_deliveries(self):
        self.entry("Invalid", "invalid@example.com\nBcc: unexpected@example.com")
        self.entry("Valid", "valid@example.com")
        with self.assertLogs(self.app.logger, level="WARNING"):
            response = self.send()
        self.assertEqual(response.status_code, 502)
        self.assertIn(b"Sent 1 of 2 emails. 1 could not be sent.", response.data)
        self.assertEqual([mail["to"] for mail in self.outbox()], ["valid@example.com"])

    def test_admin_login_and_csrf_are_required(self):
        self.entry("Player", "player@example.com")
        with self.client.session_transaction() as session:
            session.clear()
        response = self.send()
        self.assertIn(b"Sign in to access the admin area.", response.data)
        self.assertEqual(self.outbox(), [])
        with self.app.app_context(), self.client.session_transaction() as session:
            session["is_admin"] = True
            session["admin_password_version"] = admin_session_version()
        self.app.config["TESTING"] = False
        self.assertEqual(self.send().status_code, 400)
        self.assertEqual(self.outbox(), [])
        with self.client.session_transaction() as session:
            token = session["_csrf_token"]
        self.assertEqual(self.send(csrf_token=token).status_code, 200)
        self.assertEqual(len(self.outbox()), 1)

    def test_form_is_separate_collapsed_and_above_advanced_actions(self):
        response = self.client.get("/admin/t/cup")
        self.assertEqual(response.status_code, 200)
        elements = EmailFormParser(response.get_data(as_text=True)).elements
        forms = [attrs for tag, attrs in elements if tag == "form" and "data-tournament-email" in attrs]
        self.assertEqual(len(forms), 1)
        self.assertEqual(forms[0]["method"], "post")
        self.assertEqual(forms[0]["action"], "/admin/t/cup/email")
        section = next(i for i, (tag, attrs) in enumerate(elements) if tag == "section" and "admin-email-panel" in attrs.get("class", ""))
        self.assertEqual(elements[section + 1], ("details", {}))
        self.assertLess(response.data.index(b"Email participants"), response.data.index(b"Advanced actions"))
        inputs = {attrs["name"]: attrs for tag, attrs in elements if tag in {"input", "textarea", "select"} and "name" in attrs}
        self.assertIn("required", inputs["subject"])
        self.assertIn("required", inputs["message"])
        self.assertIn("csrf_token", inputs)

    def test_missing_tournament_and_get_requests_do_not_send(self):
        self.assertEqual(self.client.get("/admin/t/cup/email").status_code, 405)
        self.assertEqual(self.client.post("/admin/t/missing/email").status_code, 404)
        self.assertEqual(self.outbox(), [])


if __name__ == "__main__":
    unittest.main()
