import tempfile
import unittest

from flaskr import create_app
from flaskr.auth import admin_session_version
from flaskr.db import get_db


class PublicResultsTestCase(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.app = create_app({"INSTANCE_PATH": root.name, "SECRET_KEY": "results-test", "MAIL_ENABLED": False})
        self.public = self.app.test_client()
        self.admin = self.app.test_client()
        with self.app.app_context(), self.admin.session_transaction() as session:
            session["is_admin"] = True
            session["admin_password_version"] = admin_session_version()
            db = get_db()
            for slug, team in (("cup", 0), ("team", 1)):
                tid = db.execute("""INSERT INTO tournament
                    (name, slug, event_date, rounds_planned, status, is_team, is_active_public)
                    VALUES (?, ?, '2026-10-21', 1, 'running', ?, ?)""", (slug, slug, team, 1-team)).lastrowid
                entries = [db.execute("""INSERT INTO tournament_entry
                    (tournament_id, imported_name, seed_rating, member_status)
                    VALUES (?, ?, 1500, 'unknown')""", (tid, name)).lastrowid for name in ("Alice", "Bob")]
                db.execute("""INSERT INTO pairing
                    (tournament_id, round_no, board_no, white_entry_id, black_entry_id, result_code)
                    VALUES (?, 1, 1, ?, ?, '1-0')""", (tid, *entries))
            db.commit()

    def set_visibility(self, slug, value, *, client=None):
        client = client or self.admin
        client.get("/admin")
        with client.session_transaction() as session:
            token = session["_csrf_token"]
        return client.post(f"/admin/t/{slug}/results-visibility", data={"csrf_token": token, "results_public": value})

    def assert_hidden(self, html):
        self.assertNotIn('class="public-standings-table"', html)
        self.assertNotIn('>1-0</span>', html)
        self.assertNotIn('data-label="Score"', html)
        self.assertNotIn('Latest standings', html)
        self.assertIn('Hidden</span>', html)

    def test_private_results_are_not_exposed_by_pages_live_queries_or_team_history(self):
        self.assert_hidden(self.public.get("/").get_data(as_text=True))
        for slug in ("cup", "team"):
            for path in (f"/t/{slug}", f"/t/{slug}?view=standings", f"/t/{slug}/round/1?view=standings"):
                with self.subTest(path=path):
                    response = self.public.get(path)
                    self.assertEqual(response.status_code, 200)
                    self.assert_hidden(response.get_data(as_text=True))
            for query in ("view=standings", "view=standings&round_no=1", "view=boards"):
                self.assert_hidden(self.public.get(f"/t/{slug}/live?{query}").get_json()["html"])
        history = self.public.get("/t/team/player/3").get_data(as_text=True)
        self.assertIn("Results are not public yet.", history)
        self.assertNotIn('public-history-table', history)

    def test_admin_can_review_publish_and_hide_results(self):
        for slug in ("cup", "team"):
            page = self.admin.get(f"/t/{slug}?view=standings").get_data(as_text=True)
            self.assertIn("Latest standings", page)
            self.assertIn('class="public-standings-table"', page)
            self.assertIn("Publish standings and results", page)
            self.assertEqual(self.set_visibility(slug, "1").status_code, 302)
            page = self.public.get(f"/t/{slug}?view=standings").get_data(as_text=True)
            self.assertIn('class="public-standings-table"', page)
            live = self.public.get(f"/t/{slug}/live?view=boards").get_json()["html"]
            self.assertIn('>1-0</span>', live)
            self.assertIn('data-label="Score"', live)
            self.assertEqual(self.set_visibility(slug, "0").status_code, 302)
            self.assert_hidden(self.public.get(f"/t/{slug}?view=standings").get_data(as_text=True))

    def test_publishing_requires_admin_and_csrf(self):
        response = self.set_visibility("cup", "1", client=self.public)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/admin")
        self.assertEqual(self.admin.post("/admin/t/cup/results-visibility", data={"results_public": "1"}).status_code, 400)
        self.assert_hidden(self.public.get("/t/cup?view=standings").get_data(as_text=True))

    def test_finishing_reveals_results_without_manual_publication(self):
        with self.app.app_context():
            db = get_db()
            db.execute("UPDATE tournament SET status = 'completed'")
            db.commit()
        for slug in ("cup", "team"):
            page = self.public.get(f"/t/{slug}?view=standings").get_data(as_text=True)
            self.assertIn('class="public-standings-table"', page)
            live = self.public.get(f"/t/{slug}/live?view=boards").get_json()["html"]
            self.assertIn('>1-0</span>', live)
