import tempfile
import unittest
from unittest.mock import patch

from flaskr import create_app
from flaskr.auth import admin_session_version
from flaskr.db import get_db


class NavigationTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.app = create_app({"INSTANCE_PATH": self.tempdir.name, "TESTING": True,
                               "SECRET_KEY": "navigation-tests", "MAIL_ENABLED": False})
        self.client = self.app.test_client()
        with self.app.app_context(), self.client.session_transaction() as session:
            session["is_admin"] = True
            session["admin_password_version"] = admin_session_version()
            db = get_db()
            for slug, team in (("cup", 0), ("team", 1)):
                db.execute("""INSERT INTO tournament
                    (name, slug, event_date, rounds_planned, excludes_rating, is_team)
                    VALUES (?, ?, '2026-10-22', 9, 1, ?)""", (slug, slug, team))
            db.commit()

    def add(self, data, slug="cup"):
        return self.client.post(f"/admin/t/{slug}/entries", data=data,
                                headers={"X-Requested-With": "XMLHttpRequest"})

    def test_add_player_returns_updated_tables_and_rejects_invalid_input(self):
        response = self.add({"name": "New Player", "declared_rating": "1750"})
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertIn('data-admin-refresh="players"', payload["html"])
        self.assertIn("New Player", payload["html"])
        self.assertIn('data-pairing-options', payload["html"])
        with self.app.app_context():
            db = get_db()
            self.assertEqual(db.execute("SELECT seed_rating FROM tournament_entry").fetchone()[0], 1750)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM entry_round_status").fetchone()[0], 9)
        response = self.add({"name": ""})
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.get_json()["ok"])

    def test_team_and_solo_additions_return_refreshed_members_and_validation_errors(self):
        response = self.add({"registration_mode": "team", "team_name": "New Team", "average_elo": "1600"}, "team")
        self.assertEqual(response.status_code, 200)
        self.assertIn('data-admin-refresh="team-members"', response.get_json()["html"])
        self.assertIn("New Team", response.get_json()["html"])
        response = self.add({"registration_mode": "solo", "name": "Solo Player", "approximate_elo": "1500"}, "team")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Solo Player", response.get_json()["html"])
        self.assertIn('data-admin-refresh="awaiting-team"', response.get_json()["html"])
        response = self.add({"registration_mode": "team", "team_name": "Bad Team", "average_elo": "wrong"}, "team")
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.get_json()["ok"])

    def test_round_fragments_report_navigation_state_without_exposing_private_results(self):
        self.add({"name": "Alice"})
        self.add({"name": "Bob"})
        with self.app.app_context():
            db = get_db()
            alice, bob = [row[0] for row in db.execute("SELECT id FROM tournament_entry ORDER BY id")]
            db.execute("""INSERT INTO pairing
                (tournament_id, round_no, board_no, white_entry_id, black_entry_id, result_code)
                VALUES (1, 1, 1, ?, ?, '1-0')""", (alice, bob))
            db.commit()
        guest = self.app.test_client()
        payload = guest.get("/t/cup/live?view=standings&round_no=1").get_json()
        self.assertEqual((payload["selected_round"], payload["latest_round"], payload["view"]), (1, 1, "boards"))
        self.assertNotIn('public-standings-table', payload["html"])
        self.assertNotIn('>1-0</span>', payload["html"])
        payload = self.client.get("/t/cup/live?view=standings&round_no=1").get_json()
        self.assertEqual(payload["view"], "standings")
        self.assertIn('public-standings-table', payload["html"])

    def test_other_admin_tabs_do_not_build_the_member_directory(self):
        with patch("flaskr.web.list_member_directory", side_effect=AssertionError("unnecessary directory scan")):
            for tab in ("create", "tournaments", "rating", "password"):
                with self.subTest(tab=tab):
                    self.assertEqual(self.client.get(f"/admin?tab={tab}").status_code, 200)

    def test_rating_panel_is_quiet_when_up_to_date_but_shows_failures(self):
        with self.app.app_context():
            db = get_db()
            db.execute("INSERT INTO rating_job (id, status, message) VALUES (1, 'completed', 'All up to date.')")
            db.commit()
        response = self.client.get("/admin?tab=rating")
        self.assertNotIn(b'data-rating-job', response.data)
        self.assertNotIn(b'All up to date.', response.data)
        with self.app.app_context():
            db = get_db()
            db.execute("UPDATE rating_job SET status = 'failed', message = 'Update interrupted.'")
            db.commit()
        response = self.client.get("/admin?tab=rating")
        self.assertIn(b'data-rating-job', response.data)
        self.assertIn(b'Update interrupted.', response.data)
        self.assertIn(b'Retry rating rebuild', response.data)

    def test_large_admin_page_has_a_bounded_payload_and_keeps_selected_pairings(self):
        with self.app.app_context():
            db = get_db()
            ids = [db.execute("""INSERT INTO tournament_entry
                (tournament_id, imported_name, seed_rating, member_status)
                VALUES (1, ?, 1500, 'unknown')""", (f"Player {i}",)).lastrowid for i in range(40)]
            db.execute("""INSERT INTO pairing
                (tournament_id, round_no, board_no, white_entry_id, black_entry_id, result_code)
                VALUES (1, 1, 1, ?, ?, '1-0')""", ids[:2])
            db.commit()
        response = self.client.get("/admin/t/cup?open_round=1")
        self.assertEqual(response.status_code, 200)
        self.assertLess(len(response.data), 1_000_000)
        self.assertLess(response.data.count(b"<option"), 4000)
        self.assertIn(f'<option value="{ids[0]}" selected>Player 0</option>', response.get_data(as_text=True))
        self.assertIn(f'<option value="{ids[1]}" selected>Player 1</option>', response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
