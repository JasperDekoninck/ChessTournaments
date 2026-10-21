from __future__ import annotations

import csv
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from flaskr import create_app
from flaskr.db import get_db
from rating import DetailedLeaderboard, GameDatabase, Player, PlayerDatabase


class SecurityTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.config = {
            "INSTANCE_PATH": self.tempdir.name, "SECRET_KEY": "security-tests",
            "TESTING": False, "TRUST_PROXY_HEADERS": False,
            "MAIL_ENABLED": True, "MAIL_HOST": "example.invalid",
            "MAIL_FROM_EMAIL": "club@example.com", "MAIL_SUPPRESS_SEND": True,
        }
        self.app = create_app(self.config)
        self.client = self.app.test_client()
        with self.app.app_context():
            db = get_db()
            db.execute("""INSERT INTO tournament
                (name, slug, event_date, rounds_planned, registration_enabled, registration_opens_at)
                VALUES ('Security test', 'cup', '2026-10-21', 1, 1, '2020-01-01T12:00')""")
            db.commit()

    def post(self, path, data, *, client=None, ip="192.0.2.1"):
        client = client or self.client
        client.get("/admin")
        with client.session_transaction() as session:
            token = session["_csrf_token"]
        return client.post(path, data={**data, "csrf_token": token}, environ_overrides={"REMOTE_ADDR": ip})

    def login(self, client=None, password="admin"):
        self.assertEqual(self.post("/admin", {"password": password}, client=client).status_code, 302)

    def test_password_change_revokes_all_sessions_and_accepts_new_password(self):
        other = self.app.test_client()
        self.login()
        self.login(other)
        old_cookie = other.get_cookie("session").value
        response = self.post("/admin/password", {
            "current_password": "admin", "new_password": "replacement-password",
            "confirm_password": "replacement-password",
        })
        self.assertEqual(response.status_code, 302)
        replay = self.app.test_client()
        replay.set_cookie("session", old_cookie)
        for client in (self.client, other, replay):
            response = client.get("/admin/ratings/status")
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.location, "/admin")
        self.assertEqual(self.post("/admin", {"password": "admin"}).status_code, 200)
        self.login(password="replacement-password")
        self.assertEqual(self.client.get("/admin/ratings/status").status_code, 200)

    def test_cli_password_change_revokes_sessions_in_other_app_instances(self):
        self.login()
        other_app = create_app(self.config)
        result = other_app.test_cli_runner().invoke(args=["set-admin-password", "replacement-password"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(self.client.get("/admin/ratings/status").status_code, 302)

    def test_old_unversioned_admin_cookies_require_login(self):
        with self.client.session_transaction() as session:
            session["is_admin"] = True
        self.assertEqual(self.client.get("/admin/ratings/status").status_code, 302)

    def test_login_limit_is_shared_across_sessions_and_app_instances_and_expires(self):
        with patch("flaskr.auth.time", return_value=1000):
            self.assertEqual(self.client.post("/admin", data={"password": "wrong"}).status_code, 400)
            for _ in range(5):
                self.assertEqual(self.post("/admin", {"password": "wrong"}).status_code, 200)
            other = create_app(self.config).test_client()
            response = self.post("/admin", {"password": "admin"}, client=other)
            self.assertEqual(response.status_code, 429)
            self.assertEqual(response.headers["Retry-After"], "60")
            self.assertEqual(self.post("/admin", {"password": "wrong"}, ip="192.0.2.2").status_code, 200)
            self.assertEqual(self.client.get("/admin").status_code, 200)
        with patch("flaskr.auth.time", return_value=1060):
            self.login()

    def test_registration_limit_blocks_database_changes_and_email(self):
        with patch("flaskr.auth.time", return_value=1000):
            for index in range(30):
                response = self.post("/register/cup", {
                    "name": f"Player {index}", "email": f"player{index}@example.com",
                })
                self.assertEqual(response.status_code, 302)
            other = self.app.test_client()
            response = self.post("/register/cup", {"name": "Blocked", "email": "blocked@example.com"}, client=other)
            self.assertEqual(response.status_code, 429)
            self.assertEqual(len(self.app.extensions["mail_outbox"]), 30)
            with self.app.app_context():
                self.assertEqual(get_db().execute("SELECT COUNT(*) FROM tournament_entry").fetchone()[0], 30)
        with patch("flaskr.auth.time", return_value=1060):
            response = self.post("/register/cup", {"name": "Allowed", "email": "allowed@example.com"})
            self.assertEqual(response.status_code, 302)
            self.assertEqual(len(self.app.extensions["mail_outbox"]), 31)

    def test_admin_csv_neutralizes_formulas_without_changing_saved_names(self):
        names = ["Alice Example", "=1+1", "+SUM(1,1)", "-1+2", "@SUM(1,1)", "  =1+1", "\tName", "\r=1+1", "\n=1+1", 'A, "quoted" name']
        with self.app.app_context():
            db = get_db()
            db.executemany("""INSERT INTO tournament_entry
                (tournament_id, imported_name, seed_rating, member_status)
                VALUES (1, ?, 1500, 'unknown')""", [(name,) for name in names])
            db.execute("UPDATE tournament SET primary_tiebreak_label = '=1+1'")
            db.commit()
        self.login()
        for team in (0, 1):
            with self.subTest(team=team):
                with self.app.app_context():
                    db = get_db()
                    db.execute("UPDATE tournament SET is_team = ?", (team,))
                    db.commit()
                response = self.client.get("/admin/t/cup/export.csv")
                self.assertEqual(response.status_code, 200)
                rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
                expected = {names[0], names[-1]} | {"'" + name for name in names[1:-1]}
                self.assertEqual({row[1] for row in rows[1:]}, expected)
                self.assertIn("'=1+1", rows[0])
        with self.app.app_context():
            self.assertEqual([row[0] for row in get_db().execute("SELECT imported_name FROM tournament_entry ORDER BY id")], names)

    def test_rating_csv_is_safe_without_changing_returned_data_or_numeric_ratings(self):
        players = PlayerDatabase(strict=False)
        players.add(Player("=1+1"))
        frame = DetailedLeaderboard.compute(
            player_database=players, game_database=GameDatabase(),
            save_folder=self.tempdir.name, file_name="ratings.csv",
        )
        self.assertEqual(frame.iloc[0]["Name"], "=1+1")
        with (Path(self.tempdir.name) / "ratings.csv").open(newline="") as file:
            row = next(csv.DictReader(file))
        self.assertEqual(row["Name"], "'=1+1")
        self.assertGreater(float(row["Rating"]), 0)
