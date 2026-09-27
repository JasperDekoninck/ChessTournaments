from __future__ import annotations

import os
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from flaskr import create_app
from flaskr.core import parse_datetime_local, registration_local_time, registration_open_for_tournament
from flaskr.db import get_db


class RegistrationTimeTestCase(unittest.TestCase):
    def tournament(self, opens_at):
        return {"is_historical": 0, "status": "draft", "registration_enabled": 1, "registration_opens_at": opens_at}

    def test_zurich_opening_boundaries_and_legacy_schedules(self):
        cases = [
            ("2026-09-26T18:00", "2026-09-26T16:00+00:00"),
            ("2026-01-15T18:00", "2026-01-15T17:00+00:00"),
            ("2026-03-29T03:00", "2026-03-29T01:00+00:00"),
            ("2026-10-25T02:30", "2026-10-25T00:30+00:00"),
            ("2026-10-25T03:00", "2026-10-25T02:00+00:00"),
            ("2026-10-25T02:30+01:00", "2026-10-25T01:30+00:00"),
        ]
        for local_time, utc_time in cases:
            opening = datetime.fromisoformat(utc_time)
            for stored in (local_time, parse_datetime_local(local_time)):
                with self.subTest(stored=stored):
                    tournament = self.tournament(stored)
                    self.assertFalse(registration_open_for_tournament(tournament, opening - timedelta(microseconds=1)))
                    self.assertTrue(registration_open_for_tournament(tournament, opening))
                    self.assertTrue(registration_open_for_tournament(tournament, opening + timedelta(seconds=1)))

    def test_saved_offsets_and_form_values(self):
        self.assertEqual(parse_datetime_local("2026-09-26T18:00"), "2026-09-26T18:00+02:00")
        self.assertEqual(parse_datetime_local("2026-01-15T18:00"), "2026-01-15T18:00+01:00")
        self.assertEqual(parse_datetime_local("2026-09-26T16:00Z"), "2026-09-26T18:00+02:00")
        self.assertEqual(registration_local_time("2026-09-26T16:00+00:00"), "2026-09-26T18:00")
        self.assertEqual(registration_local_time("2026-09-26T18:00"), "2026-09-26T18:00")
        self.assertEqual(registration_local_time(None), "")
        self.assertIsNone(parse_datetime_local(""))

    def test_invalid_schedules_do_not_open(self):
        for invalid in ("2026-03-29T02:30", "not a date"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    parse_datetime_local(invalid)
                self.assertFalse(registration_open_for_tournament(self.tournament(invalid)))
        # Old invalid wall times remain editable in the admin form.
        self.assertEqual(registration_local_time("2026-03-29T02:30"), "2026-03-29T02:30")

    def test_server_timezone_does_not_change_opening(self):
        instant = datetime(2026, 9, 26, 16, 0, tzinfo=timezone.utc)

        class FrozenDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return instant.astimezone(tz) if tz else instant.astimezone().replace(tzinfo=None)

        try:
            for server_zone in ("UTC", "Europe/Zurich", "America/New_York"):
                with self.subTest(server_zone=server_zone), patch.dict(os.environ, {"TZ": server_zone}):
                    time.tzset()
                    with patch("flaskr.core.datetime", FrozenDateTime):
                        self.assertTrue(registration_open_for_tournament(self.tournament("2026-09-26T18:00")))
                        self.assertFalse(registration_open_for_tournament(self.tournament("2026-09-26T18:01")))
        finally:
            time.tzset()


class TournamentSchedulingTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.app = create_app({
            "TESTING": True, "SECRET_KEY": "test-scheduling", "INSTANCE_PATH": Path(self.tempdir.name),
            "MAIL_ENABLED": False,
        })
        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session["is_admin"] = True

    def tearDown(self):
        self.tempdir.cleanup()

    def rows(self, sql, args=()):
        with self.app.app_context():
            return [dict(row) for row in get_db().execute(sql, args)]

    def create(self, team=False, name="Scheduling Cup"):
        response = self.client.post("/admin/tournaments", data={
            "name": name, "event_date": "2026-09-27", "rounds_planned": "3",
            "is_team": "1" if team else "", "team_size": "2", "excludes_rating": "1",
        })
        self.assertEqual(response.status_code, 302)
        slug = response.location.rsplit("/", 1)[1]
        for index in range(4):
            data = {"name": f"Player {name} {index}", "declared_rating": str(1500 + index * 100)}
            if team:
                data = {"registration_mode": "team", "team_name": f"Team {index}",
                        "member_emails": f"a{index}@example.com,b{index}@example.com", "average_elo": str(1500 + index * 100)}
            self.client.post(f"/admin/t/{slug}/entries", data=data)
        return slug

    def settings(self, slug, rounds, team=False):
        return self.client.post(f"/admin/t/{slug}/settings", data={
            "rounds_planned": str(rounds), "is_team": "1" if team else "", "excludes_rating": "1", "team_size": "2",
        }, follow_redirects=True)

    def test_round_count_adds_and_removes_availability_for_players_and_teams(self):
        for team in (False, True):
            with self.subTest(team=team):
                slug = self.create(team=team, name=f"Cup {team}")
                tournament_id = self.rows("SELECT id FROM tournament WHERE slug = ?", (slug,))[0]["id"]
                entry_ids = [row["id"] for row in self.rows("SELECT id FROM tournament_entry WHERE tournament_id = ?", (tournament_id,))]
                with self.app.app_context():
                    db = get_db()
                    db.execute("UPDATE entry_round_status SET is_available = 0 WHERE entry_id = ? AND round_no = 2", (entry_ids[0],))
                    db.commit()
                response = self.settings(slug, 5, team=team)
                self.assertIn(b"Tournament settings updated", response.data)
                for entry_id in entry_ids:
                    rows = self.rows("SELECT * FROM entry_round_status WHERE entry_id = ? ORDER BY round_no", (entry_id,))
                    self.assertEqual([row["round_no"] for row in rows], [1, 2, 3, 4, 5])
                    self.assertTrue(all(row["is_available"] for row in rows[3:]))
                self.assertEqual(self.rows("SELECT is_available FROM entry_round_status WHERE entry_id = ? AND round_no = 2", (entry_ids[0],))[0]["is_available"], 0)
                self.settings(slug, 2, team=team)
                for entry_id in entry_ids:
                    self.assertEqual(len(self.rows("SELECT * FROM entry_round_status WHERE entry_id = ?", (entry_id,))), 2)
                self.assertEqual(self.rows("SELECT rounds_planned FROM tournament WHERE slug = ?", (slug,))[0]["rounds_planned"], 2)
                self.assertIn(b'Number of rounds', response.data)

    def test_running_tournament_keeps_pairings_and_can_continue_after_extension(self):
        slug = self.create(team=True)
        self.settings(slug, 1, team=True)
        self.client.post(f"/admin/t/{slug}/round/1/generate")
        pairings = self.rows("SELECT * FROM pairing ORDER BY board_no")
        self.assertEqual(len(pairings), 2)
        data = {"board_count": str(len(pairings))}
        for pairing in pairings:
            board = pairing["board_no"]
            data.update({f"white_{board}": pairing["white_entry_id"], f"black_{board}": pairing["black_entry_id"], f"result_{board}": "1-0"})
        self.client.post(f"/admin/t/{slug}/round/1/save", data=data)
        results = self.rows("SELECT * FROM pairing ORDER BY board_no")
        self.settings(slug, 3, team=True)
        self.assertEqual(self.rows("SELECT * FROM pairing ORDER BY board_no"), results)
        self.client.post(f"/admin/t/{slug}/round/2/generate")
        before = self.rows("SELECT * FROM pairing ORDER BY round_no, board_no")
        self.assertEqual(len(before), 4)
        response = self.settings(slug, 1, team=True)
        self.assertIn(b"round 2 already has pairings", response.data)
        self.assertEqual(self.rows("SELECT rounds_planned FROM tournament")[0]["rounds_planned"], 3)
        self.assertEqual(self.rows("SELECT * FROM pairing ORDER BY round_no, board_no"), before)
        self.settings(slug, 2, team=True)
        self.assertEqual(self.rows("SELECT rounds_planned FROM tournament")[0]["rounds_planned"], 2)
        self.assertEqual(self.rows("SELECT * FROM pairing ORDER BY round_no, board_no"), before)

    def test_invalid_round_counts_reject_creation_and_updates(self):
        slug = self.create()
        for value in ("", "0", "-1", "16", "2.5", "nan", "inf", "abc"):
            with self.subTest(value=value):
                response = self.settings(slug, value)
                self.assertIn(b"whole number of rounds between 1 and 15", response.data)
                self.assertEqual(self.rows("SELECT rounds_planned FROM tournament")[0]["rounds_planned"], 3)
                self.client.post("/admin/tournaments", data={"name": "Invalid", "event_date": "2026-09-27", "rounds_planned": value})
                self.assertEqual(len(self.rows("SELECT id FROM tournament")), 1)

    def test_completed_historical_and_unauthenticated_round_edits_are_rejected(self):
        slug = self.create()
        with self.app.app_context():
            db = get_db()
            db.execute("UPDATE tournament SET status = 'completed'")
            db.commit()
        self.assertIn(b"cannot change after the tournament is finished", self.settings(slug, 4).data)
        with self.app.app_context():
            db = get_db()
            db.execute("UPDATE tournament SET status = 'draft', is_historical = 1")
            db.commit()
        self.assertIn(b"historical tournaments are read-only", self.settings(slug, 4).data)
        with self.client.session_transaction() as session:
            session.clear()
        self.assertIn(b"Sign in", self.settings(slug, 4).data)
        self.assertEqual(self.rows("SELECT rounds_planned FROM tournament")[0]["rounds_planned"], 3)

    def test_registration_get_and_post_use_the_same_utc_boundary(self):
        slug = self.create(team=True)
        self.client.post(f"/admin/t/{slug}/registration", data={"registration_enabled": "1", "registration_opens_at": "2026-09-26T18:00"})
        self.assertEqual(self.rows("SELECT registration_opens_at FROM tournament")[0]["registration_opens_at"], "2026-09-26T18:00+02:00")
        admin_page = self.client.get(f"/admin/t/{slug}").data
        self.assertIn(b'value="2026-09-26T18:00"', admin_page)
        self.assertIn(b"Registration opens at (Europe/Zurich)", admin_page)
        with self.client.session_transaction() as session:
            session.clear()
        for instant, opened in (("2026-09-26T15:59:59+00:00", False), ("2026-09-26T16:00:00+00:00", True)):
            with patch("flaskr.core.datetime", wraps=datetime) as clock:
                clock.now.return_value = datetime.fromisoformat(instant)
                page = self.client.get("/register")
                self.assertEqual(b"Scheduling Cup" in page.data, opened)
                response = self.client.post(f"/register/{slug}", data={"registration_mode": "solo", "name": "Solo Example", "email": "solo@example.com"}, follow_redirects=True)
                expected = b"Your registration is confirmed" if opened else b"Registration is not open"
                self.assertIn(expected, response.data)
        self.assertEqual(len(self.rows("SELECT id FROM team_member WHERE entry_id IS NULL")), 1)

    def test_invalid_dst_opening_does_not_replace_saved_schedule(self):
        slug = self.create()
        self.client.post(f"/admin/t/{slug}/registration", data={"registration_enabled": "1", "registration_opens_at": "2026-03-29T01:30"})
        response = self.client.post(f"/admin/t/{slug}/registration", data={"registration_enabled": "1", "registration_opens_at": "2026-03-29T02:30"}, follow_redirects=True)
        self.assertIn(b"times skipped by daylight saving are invalid", response.data)
        self.assertEqual(self.rows("SELECT registration_opens_at FROM tournament")[0]["registration_opens_at"], "2026-03-29T01:30+01:00")


if __name__ == "__main__":
    unittest.main()
