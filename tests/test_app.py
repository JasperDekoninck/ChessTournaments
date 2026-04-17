from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
from io import BytesIO
from pathlib import Path

from flaskr import create_app
from flaskr.auth import hash_password
from flaskr.core import _pair_group, fetch_pairings
from flaskr.db import get_db, init_db
from flaskr.rating_integration import get_player_history, get_player_profile, import_rating_history, sync_member_statuses
from rating import Manager, PlayerDatabase


class TournamentAppTestCase(unittest.TestCase):
    @staticmethod
    def _sample_registration_csv_bytes(player_count: int = 45) -> bytes:
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "Submission Time",
                "Select",
                "Full Name",
                "Email Address",
                "Rating (Fide, Lichess, chess.com, ...)",
            ],
        )
        writer.writeheader()
        for index in range(1, player_count + 1):
            writer.writerow(
                {
                    "Submission Time": f"Apr {index:02d}, 2026 @ 06:00 PM",
                    "Select": "Registered",
                    "Full Name": f"Imported Player {index:02d}",
                    "Email Address": f"imported{index:02d}@example.com",
                    "Rating (Fide, Lichess, chess.com, ...)": str(1800 - index),
                }
            )
        return output.getvalue().encode("utf-8")

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.database = root / "test.db"
        self.rating_dir = root / "rating"
        self.export_dir = root / "exports"
        self.rating_dir.mkdir(parents=True, exist_ok=True)
        self.export_dir.mkdir(parents=True, exist_ok=True)

        manager = Manager(player_database=PlayerDatabase(strict=False))
        manager.save(str(self.rating_dir / "baseline.json"))
        manager.save(str(self.rating_dir / "current.json"))
        (self.rating_dir / "anonymous.txt").write_text("", encoding="utf-8")
        (self.rating_dir / "not_anonymous.txt").write_text("", encoding="utf-8")
        (self.rating_dir / "members.csv").write_text("Member Name\n", encoding="utf-8")

        self.app = create_app(
            {
                "TESTING": True,
                "SECRET_KEY": "test-secret",
                "DATABASE": str(self.database),
                "RATING_DATA_DIR": str(self.rating_dir),
                "EXPORT_DIR": str(self.export_dir),
            }
        )
        with self.app.app_context():
            init_db()
        self.client = self.app.test_client()
        self.csv_bytes = self._sample_registration_csv_bytes()

    def tearDown(self):
        self.tempdir.cleanup()

    def _login(self):
        with self.client.session_transaction() as session:
            session["is_admin"] = True

    def _create_tournament(self, name="Integration Test Tournament") -> str:
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={
                "name": name,
                "event_date": "2026-04-16",
                "rounds_planned": "7",
                "registrations": (BytesIO(self.csv_bytes), "registrations.csv"),
            },
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT slug FROM tournament WHERE source_type = 'local' LIMIT 1").fetchone()
        return tournament["slug"]

    def _publish_tournament(self, slug: str):
        self._login()
        response = self.client.post(f"/admin/t/{slug}/activate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

    def _set_all_entries_active(self, slug: str):
        with self.app.app_context():
            db = get_db()
            db.execute(
                """
                UPDATE tournament_entry
                SET is_active = 1
                WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?)
                """,
                (slug,),
            )
            db.commit()

    def test_public_home_uses_only_published_tournament(self):
        slug = self._create_tournament()
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"No public tournament is active yet", response.data)
        self.assertNotIn(b"<h2>Leaderboard</h2>", response.data)

        self._publish_tournament(slug)
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(slug.encode("utf-8"), response.data)

    def test_public_boards_show_result_as_middle_column(self):
        slug = self._create_tournament(name="Public Boards Tournament")
        self._set_all_entries_active(slug)
        self._login()
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self._publish_tournament(slug)

        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        white_index = response.data.index(b'<th class="public-player-col public-player-col-white">White</th>')
        result_index = response.data.index(b'<th class="align-center public-result-col">Result</th>')
        black_index = response.data.index(b'<th class="public-player-col public-player-col-black">Black</th>')
        self.assertLess(white_index, result_index)
        self.assertLess(result_index, black_index)

    def test_admin_password_hash_is_stored_in_database(self):
        with self.app.app_context():
            db = get_db()
            row = db.execute("SELECT value FROM app_config WHERE key = 'admin_password_hash'").fetchone()
        self.assertIsNotNone(row)
        self.assertTrue(row["value"].startswith("scrypt:"))

    def test_legacy_admin_password_file_is_migrated_into_database(self):
        legacy_root = Path(self.tempdir.name) / "legacy-instance"
        legacy_root.mkdir(parents=True, exist_ok=True)
        legacy_file = legacy_root / ".admin_password_hash"
        legacy_hash = hash_password("migrated-secret")
        legacy_file.write_text(legacy_hash, encoding="utf-8")

        app = create_app(
            {
                "TESTING": True,
                "SECRET_KEY": "legacy-secret",
                "INSTANCE_PATH": str(legacy_root),
                "DATABASE": str(legacy_root / "test.db"),
                "RATING_DATA_DIR": str(self.rating_dir),
                "EXPORT_DIR": str(self.export_dir),
            }
        )

        with app.app_context():
            db = get_db()
            row = db.execute("SELECT value FROM app_config WHERE key = 'admin_password_hash'").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["value"], legacy_hash)
        self.assertFalse(legacy_file.exists())

    def test_public_registration_respects_opening_and_waitlist(self):
        slug = self._create_tournament(name="Registration Tournament")

        response = self.client.get("/register")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"Registration Tournament", response.data)

        self._login()
        response = self.client.post(
            f"/admin/t/{slug}/registration",
            data={
                "registration_enabled": "1",
                "registration_opens_at": "2026-04-15T18:00",
                "event_time": "18:30",
                "venue": "CAB H52",
                "max_registrations": "46",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        response = self.client.get("/register")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Registration Tournament", response.data)
        self.assertIn(b"18:30", response.data)
        self.assertIn(b"CAB H52", response.data)

        response = self.client.post(
            f"/register/{slug}",
            data={"name": "Public Player One", "email": "one@example.com", "declared_rating": "1800"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"spot is confirmed", response.data)

        response = self.client.post(
            f"/register/{slug}",
            data={"name": "Public Player Two", "email": "two@example.com", "declared_rating": "1700"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"waiting list in position 1", response.data)

        with self.app.app_context():
            db = get_db()
            rows = db.execute(
                """
                SELECT imported_name, is_active, waitlist_position, registration_source
                FROM tournament_entry
                WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?)
                AND imported_name LIKE 'Public Player%'
                ORDER BY imported_name ASC
                """,
                (slug,),
            ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["registration_source"], "public")
        self.assertEqual(rows[0]["is_active"], 0)
        self.assertIsNone(rows[0]["waitlist_position"])
        self.assertEqual(rows[1]["registration_source"], "public")
        self.assertEqual(rows[1]["is_active"], 0)
        self.assertEqual(rows[1]["waitlist_position"], 1)

    def test_public_registration_custom_fields_are_rendered_and_stored(self):
        slug = self._create_tournament(name="Custom Fields Tournament")

        self._login()
        response = self.client.post(
            f"/admin/t/{slug}/registration",
            data={
                "registration_enabled": "1",
                "registration_opens_at": "2026-04-15T18:00",
                "registration_field_type": ["text", "dropdown"],
                "registration_field_label": ["Department", "Prize group"],
                "registration_field_options": ["", "Open\nU1800"],
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        response = self.client.get("/register")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Department", response.data)
        self.assertIn(b"Prize group", response.data)
        self.assertIn(b"U1800", response.data)

        response = self.client.post(
            f"/register/{slug}",
            data={
                "name": "Public Player Fields",
                "email": "fields@example.com",
                "declared_rating": "1800",
                "registration_field_0": "CS",
                "registration_field_1": "Open",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"spot is confirmed", response.data)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute(
                "SELECT registration_form_json FROM tournament WHERE slug = ?",
                (slug,),
            ).fetchone()
            entry = db.execute(
                """
                SELECT registration_answers_json
                FROM tournament_entry
                WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?)
                  AND imported_name = 'Public Player Fields'
                """,
                (slug,),
            ).fetchone()
        self.assertIsNotNone(tournament)
        self.assertIsNotNone(entry)
        self.assertEqual(
            json.loads(tournament["registration_form_json"]),
            [
                {"type": "text", "label": "Department", "options": []},
                {"type": "dropdown", "label": "Prize group", "options": ["Open", "U1800"]},
            ],
        )
        self.assertEqual(
            json.loads(entry["registration_answers_json"]),
            [
                {"label": "Department", "type": "text", "value": "CS"},
                {"label": "Prize group", "type": "dropdown", "value": "Open"},
            ],
        )

    def test_registration_full_warning_does_not_say_inactive(self):
        slug = self._create_tournament(name="Full Registration Warning Tournament")
        self._login()
        response = self.client.post(
            f"/admin/t/{slug}/registration",
            data={
                "registration_enabled": "1",
                "registration_opens_at": "2026-04-15T18:00",
                "max_registrations": "45",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        response = self.client.get("/register")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"currently full", response.data)
        self.assertNotIn(b"start as inactive", response.data)

    def test_public_registration_rejects_duplicate_name(self):
        slug = self._create_tournament(name="Duplicate Registration Tournament")
        self._login()
        response = self.client.post(
            f"/admin/t/{slug}/registration",
            data={"registration_enabled": "1", "registration_opens_at": "2026-04-15T18:00"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        response = self.client.post(
            f"/register/{slug}",
            data={"name": "Public Player One", "email": "one@example.com", "declared_rating": "1800"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"spot is confirmed", response.data)

        response = self.client.post(
            f"/register/{slug}",
            data={"name": "Public Player One", "email": "duplicate@example.com", "declared_rating": "1800"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"already registered for this tournament", response.data)

        with self.app.app_context():
            db = get_db()
            count = db.execute(
                """
                SELECT COUNT(*) AS c
                FROM tournament_entry
                WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?)
                AND imported_name = 'Public Player One'
                """,
                (slug,),
            ).fetchone()["c"]
        self.assertEqual(count, 1)

    def test_registration_page_shows_privacy_notice(self):
        slug = self._create_tournament(name="Privacy Notice Tournament")
        self._login()
        response = self.client.post(
            f"/admin/t/{slug}/registration",
            data={"registration_enabled": "1", "registration_opens_at": "2026-04-15T18:00"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        response = self.client.get("/register")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"We store your data only for the purpose of registering for this tournament", response.data)

    def test_create_tournament_and_generate_pairings(self):
        slug = self._create_tournament()
        self._set_all_entries_active(slug)
        self._login()
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            entry_count = db.execute(
                "SELECT COUNT(*) AS c FROM tournament_entry WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?)",
                (slug,),
            ).fetchone()["c"]
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            round_one_pairings = fetch_pairings(db, tournament["id"], 1)

        self.assertEqual(entry_count, 45)
        self.assertEqual(len(round_one_pairings), 23)

    def test_toggling_entry_active_exposes_unfinished_round_availability(self):
        slug = self._create_tournament(name="Availability Toggle Tournament")
        self._login()
        with self.app.app_context():
            db = get_db()
            entry_id = db.execute(
                "SELECT id FROM tournament_entry WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?) ORDER BY id ASC LIMIT 1",
                (slug,),
            ).fetchone()["id"]

        response = self.client.post(
            f"/admin/t/{slug}/entries/{entry_id}/toggle",
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["entry"]["is_active"])
        self.assertTrue(payload["entry"]["round_cells"][0]["can_toggle"])
        self.assertEqual(payload["entry"]["round_cells"][0]["cell"]["label"], "in")

    def test_leaderboard_page_reads_cached_export_csv(self):
        (self.export_dir / "anonymous_leaderboard.csv").write_text(
            "\n".join(
                [
                    "Rank,Name,Rating,Wins,Losses,Draws",
                    "1,Cached Leader,2100,12,3,1",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        response = self.client.get("/leaderboard")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Cached Leader", response.data)
        self.assertIn(b"2100", response.data)

    def test_complete_tournament_writes_rating_exports(self):
        slug = self._create_tournament(name="Completed Tournament")
        self._set_all_entries_active(slug)
        self._login()
        self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            pairings = fetch_pairings(db, tournament["id"], 1)

        form = {"board_count": str(len(pairings))}
        for pairing in pairings:
            form[f"white_{pairing['board_no']}"] = str(pairing["white_entry_id"])
            if pairing["black_entry_id"] is not None:
                form[f"black_{pairing['board_no']}"] = str(pairing["black_entry_id"])
                form[f"result_{pairing['board_no']}"] = "1-0"
            else:
                form[f"result_{pairing['board_no']}"] = "BYE"

        response = self.client.post(f"/admin/t/{slug}/round/1/save", data=form, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response = self.client.post(f"/admin/t/{slug}/complete", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        self.assertTrue((self.export_dir / "anonymous_leaderboard.csv").exists())
        self.assertTrue((self.export_dir / "tournaments" / slug / "leaderboard.csv").exists())

    def test_complete_tournament_stores_final_snapshot_in_database(self):
        slug = self._create_tournament(name="Stored Snapshot Tournament")
        self._set_all_entries_active(slug)
        self._login()
        self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            pairings = fetch_pairings(db, tournament["id"], 1)

        form = {"board_count": str(len(pairings))}
        for pairing in pairings:
            form[f"white_{pairing['board_no']}"] = str(pairing["white_entry_id"])
            if pairing["black_entry_id"] is not None:
                form[f"black_{pairing['board_no']}"] = str(pairing["black_entry_id"])
                form[f"result_{pairing['board_no']}"] = "1-0"
            else:
                form[f"result_{pairing['board_no']}"] = "BYE"

        response = self.client.post(f"/admin/t/{slug}/round/1/save", data=form, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response = self.client.post(f"/admin/t/{slug}/complete", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute(
                "SELECT id, public_insights_json FROM tournament WHERE slug = ?",
                (slug,),
            ).fetchone()
            self.assertIsNotNone(tournament["public_insights_json"])
            snapshot_rows = db.execute(
                """
                SELECT COUNT(*) AS c
                FROM tournament_entry
                WHERE tournament_id = ?
                  AND final_rank IS NOT NULL
                  AND final_score IS NOT NULL
                  AND final_primary_tiebreak IS NOT NULL
                  AND final_secondary_tiebreak IS NOT NULL
                """,
                (tournament["id"],),
            ).fetchone()
            entry_count = db.execute(
                "SELECT COUNT(*) AS c FROM tournament_entry WHERE tournament_id = ?",
                (tournament["id"],),
            ).fetchone()
        self.assertEqual(snapshot_rows["c"], entry_count["c"])

    def test_completed_public_tournament_shows_final_highlights(self):
        slug = self._create_tournament(name="Highlights Tournament")
        self._set_all_entries_active(slug)
        self._login()
        self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            pairings = fetch_pairings(db, tournament["id"], 1)

        form = {"board_count": str(len(pairings))}
        for pairing in pairings:
            form[f"white_{pairing['board_no']}"] = str(pairing["white_entry_id"])
            if pairing["black_entry_id"] is not None:
                form[f"black_{pairing['board_no']}"] = str(pairing["black_entry_id"])
                form[f"result_{pairing['board_no']}"] = "1-0"
            else:
                form[f"result_{pairing['board_no']}"] = "BYE"

        response = self.client.post(f"/admin/t/{slug}/round/1/save", data=form, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response = self.client.post(f"/admin/t/{slug}/complete", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self._publish_tournament(slug)

        response = self.client.get(f"/t/{slug}?view=standings")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Final Highlights", response.data)
        self.assertIn(b"Played Above Level", response.data)
        self.assertIn(b"Most Unlikely Win", response.data)
        self.assertIn(b"1st", response.data)

    def test_add_player_manually(self):
        slug = self._create_tournament(name="Manual Entry Tournament")
        self._login()
        response = self.client.post(
            f"/admin/t/{slug}/entries",
            data={"name": "Manual Player", "email": "manual@example.com", "declared_rating": "1750"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            row = db.execute(
                """
                SELECT e.seed_rating, e.imported_name
                FROM tournament_entry e
                JOIN tournament t ON t.id = e.tournament_id
                WHERE t.slug = ? AND e.imported_name = 'Manual Player'
                """,
                (slug,),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["seed_rating"], 1750)

    def test_late_added_player_is_out_for_existing_rounds(self):
        slug = self._create_tournament(name="Late Entry Tournament")
        self._set_all_entries_active(slug)
        self._login()
        self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        response = self.client.post(
            f"/admin/t/{slug}/entries",
            data={"name": "Late Player", "declared_rating": "1700"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            entry = db.execute(
                """
                SELECT e.id
                FROM tournament_entry e
                JOIN tournament t ON t.id = e.tournament_id
                WHERE t.slug = ? AND e.imported_name = 'Late Player'
                """,
                (slug,),
            ).fetchone()
            round_one = db.execute(
                "SELECT is_available FROM entry_round_status WHERE entry_id = ? AND round_no = 1",
                (entry["id"],),
            ).fetchone()
            round_two = db.execute(
                "SELECT is_available FROM entry_round_status WHERE entry_id = ? AND round_no = 2",
                (entry["id"],),
            ).fetchone()
        self.assertEqual(round_one["is_available"], 0)
        self.assertEqual(round_two["is_available"], 1)

    def test_player_profile_uses_manager_stats_instead_of_stale_columns(self):
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Profile Freshness Tournament", "event_date": "2026-04-16", "rounds_planned": "1"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            slug = db.execute(
                "SELECT slug FROM tournament WHERE name = 'Profile Freshness Tournament'"
            ).fetchone()["slug"]

        for index, name in enumerate(("Alpha Example", "Beta Example"), start=1):
            response = self.client.post(
                f"/admin/t/{slug}/entries",
                data={"name": name, "declared_rating": str(1600 - index * 10)},
                follow_redirects=True,
            )
            self.assertEqual(response.status_code, 200)

        self._set_all_entries_active(slug)
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            pairing = fetch_pairings(db, tournament["id"], 1)[0]

        response = self.client.post(
            f"/admin/t/{slug}/round/1/save",
            data={
                "board_count": "1",
                "white_1": str(pairing["white_entry_id"]),
                "black_1": str(pairing["black_entry_id"]),
                "result_1": "1-0",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        response = self.client.post(f"/admin/t/{slug}/complete", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            db.execute(
                """
                UPDATE player
                SET historical_wins = 999, historical_losses = 999, historical_draws = 999
                WHERE name = 'Alpha Example'
                """
            )
            db.commit()
            profile = get_player_profile("Alpha Example")

        self.assertIsNotNone(profile)
        self.assertEqual(profile["wins"], 1)
        self.assertEqual(profile["losses"], 0)
        self.assertEqual(profile["draws"], 0)
        self.assertEqual(profile["games"], 1)

    def test_player_profile_and_history_match_completed_results(self):
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Profile Consistency Tournament", "event_date": "2026-04-16", "rounds_planned": "2"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            slug = db.execute(
                "SELECT slug FROM tournament WHERE name = 'Profile Consistency Tournament'"
            ).fetchone()["slug"]

        for index, name in enumerate(("Alpha Example", "Beta Example", "Gamma Example", "Delta Example"), start=1):
            response = self.client.post(
                f"/admin/t/{slug}/entries",
                data={"name": name, "declared_rating": str(1700 - index * 10)},
                follow_redirects=True,
            )
            self.assertEqual(response.status_code, 200)

        self._set_all_entries_active(slug)
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            round_one_pairings = fetch_pairings(db, tournament["id"], 1)

        round_one_form = {"board_count": str(len(round_one_pairings))}
        for pairing in round_one_pairings:
            round_one_form[f"white_{pairing['board_no']}"] = str(pairing["white_entry_id"])
            round_one_form[f"black_{pairing['board_no']}"] = str(pairing["black_entry_id"])
            if "Alpha Example" in {pairing["white_name"], pairing["black_name"]}:
                round_one_form[f"result_{pairing['board_no']}"] = (
                    "1-0" if pairing["white_name"] == "Alpha Example" else "0-1"
                )
            else:
                round_one_form[f"result_{pairing['board_no']}"] = "1/2-1/2"

        response = self.client.post(f"/admin/t/{slug}/round/1/save", data=round_one_form, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response = self.client.post(f"/admin/t/{slug}/round/2/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            round_two_pairings = fetch_pairings(db, tournament["id"], 2)

        round_two_form = {"board_count": str(len(round_two_pairings))}
        for pairing in round_two_pairings:
            round_two_form[f"white_{pairing['board_no']}"] = str(pairing["white_entry_id"])
            round_two_form[f"black_{pairing['board_no']}"] = str(pairing["black_entry_id"])
            if "Alpha Example" in {pairing["white_name"], pairing["black_name"]}:
                round_two_form[f"result_{pairing['board_no']}"] = (
                    "0-1" if pairing["white_name"] == "Alpha Example" else "1-0"
                )
            else:
                round_two_form[f"result_{pairing['board_no']}"] = "1/2-1/2"

        response = self.client.post(f"/admin/t/{slug}/round/2/save", data=round_two_form, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response = self.client.post(f"/admin/t/{slug}/complete", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            profile = get_player_profile("Alpha Example")
            history = get_player_history("Alpha Example")

        self.assertIsNotNone(profile)
        self.assertEqual(profile["wins"], 1)
        self.assertEqual(profile["losses"], 1)
        self.assertEqual(profile["draws"], 0)
        self.assertEqual(profile["games"], 2)
        self.assertEqual(len(history), 2)

    def test_player_history_omits_byes(self):
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Bye Omission Tournament", "event_date": "2026-04-16", "rounds_planned": "1"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            slug = db.execute(
                "SELECT slug FROM tournament WHERE name = 'Bye Omission Tournament'"
            ).fetchone()["slug"]

        for index, name in enumerate(("Alpha Example", "Beta Example", "Gamma Example"), start=1):
            response = self.client.post(
                f"/admin/t/{slug}/entries",
                data={"name": name, "declared_rating": str(1650 - index * 10)},
                follow_redirects=True,
            )
            self.assertEqual(response.status_code, 200)

        self._set_all_entries_active(slug)
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            pairings = fetch_pairings(db, tournament["id"], 1)

        form = {"board_count": str(len(pairings))}
        bye_player = None
        for pairing in pairings:
            form[f"white_{pairing['board_no']}"] = str(pairing["white_entry_id"])
            if pairing["black_entry_id"] is None:
                form[f"result_{pairing['board_no']}"] = "BYE"
                bye_player = pairing["white_name"]
            else:
                form[f"black_{pairing['board_no']}"] = str(pairing["black_entry_id"])
                form[f"result_{pairing['board_no']}"] = "1-0"

        self.assertIsNotNone(bye_player)
        response = self.client.post(f"/admin/t/{slug}/round/1/save", data=form, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response = self.client.post(f"/admin/t/{slug}/complete", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            profile = get_player_profile(bye_player)
            history = get_player_history(bye_player)

        self.assertIsNotNone(profile)
        self.assertEqual(profile["games"], 0)
        self.assertEqual(profile["wins"], 0)
        self.assertEqual(profile["losses"], 0)
        self.assertEqual(profile["draws"], 0)
        self.assertEqual(history, [])

    def test_tournament_player_history_uses_manager_stats_instead_of_stale_columns(self):
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Tournament Profile Freshness", "event_date": "2026-04-16", "rounds_planned": "1"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            slug = db.execute(
                "SELECT slug FROM tournament WHERE name = 'Tournament Profile Freshness'"
            ).fetchone()["slug"]

        for index, name in enumerate(("Alpha Example", "Beta Example"), start=1):
            response = self.client.post(
                f"/admin/t/{slug}/entries",
                data={"name": name, "declared_rating": str(1600 - index * 10)},
                follow_redirects=True,
            )
            self.assertEqual(response.status_code, 200)

        self._set_all_entries_active(slug)
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            pairing = fetch_pairings(db, tournament["id"], 1)[0]

        response = self.client.post(
            f"/admin/t/{slug}/round/1/save",
            data={
                "board_count": "1",
                "white_1": str(pairing["white_entry_id"]),
                "black_1": str(pairing["black_entry_id"]),
                "result_1": "1-0",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        response = self.client.post(f"/admin/t/{slug}/complete", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            db.execute(
                """
                UPDATE player
                SET historical_wins = 999, historical_losses = 999, historical_draws = 999
                WHERE name = 'Alpha Example'
                """
            )
            db.commit()
            entry_id = db.execute(
                """
                SELECT e.id
                FROM tournament_entry e
                JOIN tournament t ON t.id = e.tournament_id
                WHERE t.slug = ? AND e.imported_name = 'Alpha Example'
                """,
                (slug,),
            ).fetchone()["id"]

        response = self.client.get(f"/t/{slug}/player/{entry_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Wins:</strong> 1", response.data)
        self.assertIn(b"Losses:</strong> 0", response.data)
        self.assertIn(b"Draws:</strong> 0", response.data)
        self.assertIn(b"Games:</strong> 1", response.data)

    def test_current_round_pairings_can_include_late_player_after_marking_them_in(self):
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Current Round Edit Tournament", "event_date": "2026-04-16", "rounds_planned": "3"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            slug = db.execute(
                "SELECT slug FROM tournament WHERE name = 'Current Round Edit Tournament'"
            ).fetchone()["slug"]

        for index, name in enumerate(("Alice Example", "Bob Example", "Cara Example", "Dan Example"), start=1):
            response = self.client.post(
                f"/admin/t/{slug}/entries",
                data={"name": name, "declared_rating": str(1600 - index * 10)},
                follow_redirects=True,
            )
            self.assertEqual(response.status_code, 200)

        self._set_all_entries_active(slug)
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        response = self.client.post(
            f"/admin/t/{slug}/entries",
            data={"name": "Ana Example", "declared_rating": "1550"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            ana_entry = db.execute(
                """
                SELECT e.id
                FROM tournament_entry e
                WHERE e.tournament_id = ? AND e.imported_name = 'Ana Example'
                """,
                (tournament["id"],),
            ).fetchone()
            initial_pairings = fetch_pairings(db, tournament["id"], 1)

        response = self.client.post(
            f"/admin/t/{slug}/entries/{ana_entry['id']}/toggle",
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        form = {"board_count": "3"}
        for pairing in initial_pairings:
            form[f"white_{pairing['board_no']}"] = str(pairing["white_entry_id"])
            if pairing["black_entry_id"] is not None:
                form[f"black_{pairing['board_no']}"] = str(pairing["black_entry_id"])
            form[f"result_{pairing['board_no']}"] = pairing["result_code"] or ""
        form["white_3"] = str(ana_entry["id"])
        form["black_3"] = ""
        form["result_3"] = ""

        response = self.client.post(f"/admin/t/{slug}/round/1/save", data=form, follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            pairings = fetch_pairings(db, tournament["id"], 1)

        self.assertEqual(len(pairings), 3)
        ana_pairing = next(pairing for pairing in pairings if pairing["white_name"] == "Ana Example")
        self.assertIsNone(ana_pairing["black_entry_id"])

    def test_manual_round_save_rejects_bye_for_paired_board(self):
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Bye Validation Tournament", "event_date": "2026-04-16", "rounds_planned": "3"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            slug = db.execute(
                "SELECT slug FROM tournament WHERE name = 'Bye Validation Tournament'"
            ).fetchone()["slug"]

        for index, name in enumerate(("Alice Example", "Bob Example"), start=1):
            response = self.client.post(
                f"/admin/t/{slug}/entries",
                data={"name": name, "declared_rating": str(1600 - index * 10)},
                follow_redirects=True,
            )
            self.assertEqual(response.status_code, 200)

        self._set_all_entries_active(slug)
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            pairing = fetch_pairings(db, tournament["id"], 1)[0]

        response = self.client.post(
            f"/admin/t/{slug}/round/1/save",
            data={
                "board_count": "1",
                "white_1": str(pairing["white_entry_id"]),
                "black_1": str(pairing["black_entry_id"]),
                "result_1": "BYE",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Use BYE only for boards without an opponent.", response.data)

    def test_manual_add_defaults_seed_rating_to_1500_when_missing(self):
        slug = self._create_tournament(name="Manual Default Rating Tournament")
        self._login()
        response = self.client.post(
            f"/admin/t/{slug}/entries",
            data={"name": "Manual Default Rating"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            row = db.execute(
                """
                SELECT declared_rating, seed_rating
                FROM tournament_entry
                WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?)
                  AND imported_name = 'Manual Default Rating'
                """,
                (slug,),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row["declared_rating"])
        self.assertEqual(row["seed_rating"], 1500)

    def test_public_registration_defaults_seed_rating_to_1500_when_missing(self):
        slug = self._create_tournament(name="Public Default Rating Tournament")
        self._login()
        response = self.client.post(
            f"/admin/t/{slug}/registration",
            data={"registration_enabled": "1", "registration_opens_at": "2026-04-15T18:00"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        response = self.client.post(
            f"/register/{slug}",
            data={"name": "Public Default Rating", "email": "default@example.com"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            row = db.execute(
                """
                SELECT declared_rating, seed_rating
                FROM tournament_entry
                WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?)
                  AND imported_name = 'Public Default Rating'
                """,
                (slug,),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row["declared_rating"])
        self.assertEqual(row["seed_rating"], 1500)

    def test_member_cutoff_uses_played_rounds_only(self):
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Member Logic Tournament", "event_date": "2026-04-16", "rounds_planned": "3"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        with self.app.app_context():
            db = get_db()
            slug = db.execute(
                "SELECT slug FROM tournament WHERE name = 'Member Logic Tournament'"
            ).fetchone()["slug"]
        for name in ("Alice Example", "Bob Example", "Cara Example"):
            response = self.client.post(
                f"/admin/t/{slug}/entries",
                data={"name": name, "declared_rating": "1600"},
                follow_redirects=True,
            )
            self.assertEqual(response.status_code, 200)

        self._set_all_entries_active(slug)
        self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            pairings = fetch_pairings(db, tournament["id"], 1)
            sync_member_statuses(db)
            statuses = {
                row["imported_name"]: row["member_status"]
                for row in db.execute(
                    """
                    SELECT e.imported_name, e.member_status
                    FROM tournament_entry e
                    WHERE e.tournament_id = ?
                    """,
                    (tournament["id"],),
                ).fetchall()
            }

        bye_player = next(pairing["white_name"] for pairing in pairings if pairing["black_entry_id"] is None)
        paired_players = [
            pairing["white_name"]
            for pairing in pairings
            if pairing["black_entry_id"] is not None
        ] + [
            pairing["black_name"]
            for pairing in pairings
            if pairing["black_entry_id"] is not None
        ]
        for player_name in paired_players:
            self.assertEqual(statuses[player_name], "member")
        self.assertEqual(statuses[bye_player], "non-member")

        response = self.client.post(
            "/admin/members/cutoff",
            data={"member_since_date": "2026-12-01"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            statuses = {
                row["imported_name"]: row["member_status"]
                for row in db.execute(
                    """
                    SELECT e.imported_name, e.member_status
                    FROM tournament_entry e
                    JOIN tournament t ON t.id = e.tournament_id
                    WHERE t.slug = ?
                    """,
                    (slug,),
                ).fetchall()
            }
        self.assertTrue(all(status == "non-member" for status in statuses.values()))

    def test_member_directory_filters_by_name(self):
        source_root = Path(self.tempdir.name) / "member-search-source"
        (source_root / "data").mkdir(parents=True, exist_ok=True)
        (source_root / "scripts").mkdir(parents=True, exist_ok=True)
        (source_root / "data" / "tournaments" / "2024_01_10_Member Search Event").mkdir(parents=True, exist_ok=True)

        manager = Manager(player_database=PlayerDatabase(strict=False))
        manager.save(str(source_root / "data" / "databases.json"))
        (source_root / "members.csv").write_text("Member Name\nAlice Example\n", encoding="utf-8")
        (source_root / "data" / "anonymous.txt").write_text("", encoding="utf-8")
        (source_root / "data" / "not_anonymous.txt").write_text("", encoding="utf-8")
        (source_root / "scripts" / "main.sh").write_text(
            'python scripts/manual_tournament.py --tournament_path data/member_search.csv --tournament_name "Member Search Event" --tournament_date "2024-01-10"\n',
            encoding="utf-8",
        )
        (source_root / "data" / "member_search.csv").write_text(
            "\n".join(
                [
                    "Round;Board;White;Black;Result",
                    "1;1;Alice Example;Bob Example;1-0",
                    "1;2;Cara Example;;Bye",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (source_root / "data" / "tournaments" / "2024_01_10_Member Search Event" / "leaderboard.csv").write_text(
            "\n".join(
                [
                    "Rank,Name,Rating,Score,BH,BH-C1,Performance",
                    "1,Alice Example,1600,1.0,0.0,0.0,1600",
                    "2,Bob Example,1500,0.0,0.0,0.0,1500",
                    "3,Cara Example,1400,1.0,0.0,0.0,1400",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        with self.app.app_context():
            imported = import_rating_history(str(source_root))
            self.assertEqual(imported, 1)

        self._login()
        response = self.client.get("/admin?tab=members&member_q=ali")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Alice Example", response.data)
        self.assertNotIn(b"Bob Example", response.data)
        self.assertNotIn(b"Cara Example", response.data)

    def test_pair_group_avoids_repeat_when_non_repeat_matching_exists(self):
        group = [
            {
                "entry_id": 1,
                "name": "Alpha",
                "score": 2.0,
                "seed_rating": 2100,
                "opponent_ids": {4},
                "white_games": 1,
                "black_games": 1,
                "colors": ["W", "B"],
            },
            {
                "entry_id": 2,
                "name": "Bravo",
                "score": 2.0,
                "seed_rating": 2090,
                "opponent_ids": {3},
                "white_games": 1,
                "black_games": 1,
                "colors": ["B", "W"],
            },
            {
                "entry_id": 3,
                "name": "Charlie",
                "score": 2.0,
                "seed_rating": 1900,
                "opponent_ids": {2, 4},
                "white_games": 1,
                "black_games": 1,
                "colors": ["W", "B"],
            },
            {
                "entry_id": 4,
                "name": "Delta",
                "score": 2.0,
                "seed_rating": 1890,
                "opponent_ids": {1, 3},
                "white_games": 1,
                "black_games": 1,
                "colors": ["B", "W"],
            },
        ]

        pairs = _pair_group(group)
        paired_ids = {frozenset((white["entry_id"], black["entry_id"])) for white, black in pairs}
        self.assertEqual(paired_ids, {frozenset((1, 3)), frozenset((2, 4))})

    def test_toggle_public_homepage_redirects_back_to_tournament_page(self):
        slug = self._create_tournament(name="Homepage Toggle Tournament")
        self._login()
        response = self.client.post(f"/admin/t/{slug}/activate", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith(f"/admin/t/{slug}"))

    def test_toggle_public_homepage_preserves_open_round(self):
        slug = self._create_tournament(name="Homepage Toggle Open Round Tournament")
        self._login()
        response = self.client.post(
            f"/admin/t/{slug}/activate",
            data={"open_round": "3"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn(f"/admin/t/{slug}?open_round=3#round-3", response.headers["Location"])

    def test_player_history_page_shows_profile_summary(self):
        slug = self._create_tournament(name="Profile Summary Tournament")
        self._publish_tournament(slug)
        with self.client.session_transaction() as session:
            session.clear()
        with self.app.app_context():
            db = get_db()
            entry_id = db.execute(
                "SELECT id FROM tournament_entry WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?) ORDER BY id ASC LIMIT 1",
                (slug,),
            ).fetchone()["id"]

        response = self.client.get(f"/t/{slug}/player/{entry_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Rating:", response.data)
        self.assertIn(b"Games:", response.data)
        self.assertIn(b"Wins:", response.data)
        self.assertIn(b"Losses:", response.data)
        self.assertIn(b"Draws:", response.data)
        self.assertNotIn(b"Email:", response.data)

    def test_player_history_page_shows_email_only_to_admin(self):
        slug = self._create_tournament(name="Admin Email Visibility Tournament")
        self._publish_tournament(slug)
        with self.client.session_transaction() as session:
            session.clear()
        with self.app.app_context():
            db = get_db()
            row = db.execute(
                """
                SELECT id, imported_email
                FROM tournament_entry
                WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?)
                ORDER BY id ASC
                LIMIT 1
                """,
                (slug,),
            ).fetchone()
        response = self.client.get(f"/t/{slug}/player/{row['id']}")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"Email:", response.data)
        self.assertNotIn(row["imported_email"].encode("utf-8"), response.data)

        self._login()
        response = self.client.get(f"/t/{slug}/player/{row['id']}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Email:", response.data)
        self.assertIn(row["imported_email"].encode("utf-8"), response.data)

    def test_import_rating_history_syncs_rounds_from_source_csv(self):
        source_root = Path(self.tempdir.name) / "source"
        (source_root / "data").mkdir(parents=True, exist_ok=True)
        (source_root / "scripts").mkdir(parents=True, exist_ok=True)
        (source_root / "data" / "tournaments" / "2024_01_10_Test Source Event").mkdir(parents=True, exist_ok=True)

        manager = Manager(player_database=PlayerDatabase(strict=False))
        manager.save(str(source_root / "data" / "databases.json"))
        (source_root / "members.csv").write_text("Member Name\nAlice Example\n", encoding="utf-8")
        (source_root / "data" / "anonymous.txt").write_text("", encoding="utf-8")
        (source_root / "data" / "not_anonymous.txt").write_text("", encoding="utf-8")
        (source_root / "scripts" / "main.sh").write_text(
            'python scripts/manual_tournament.py --tournament_path data/test_event.csv --tournament_name "Test Source Event" --tournament_date "2024-01-10"\n',
            encoding="utf-8",
        )
        (source_root / "data" / "test_event.csv").write_text(
            "\n".join(
                [
                    "Round;Board;White;Black;Result",
                    "1;1;Alice Example;Bob Example;1-0",
                    "1;2;Cara Example;;Bye",
                    "2;1;Alice Example;Cara Example;0.5-0.5",
                    "2;2;Bob Example;;Bye",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (source_root / "data" / "tournaments" / "2024_01_10_Test Source Event" / "leaderboard.csv").write_text(
            "\n".join(
                [
                    "Rank,Name,Rating,Score,BH,BH-C1,Performance",
                    "1,Alice Example,1600,1.5,1.5,1.0,1600",
                    "2,Cara Example,1500,1.5,1.5,1.0,1500",
                    "3,Bob Example,1400,1.0,1.0,0.0,1400",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        with self.app.app_context():
            imported = import_rating_history(str(source_root))
            self.assertEqual(imported, 1)
            db = get_db()
            tournament = db.execute(
                "SELECT id, slug, primary_tiebreak_label, secondary_tiebreak_label FROM tournament WHERE source_type = 'history'"
            ).fetchone()
            self.assertIsNotNone(tournament)
            self.assertEqual(tournament["primary_tiebreak_label"], "BH")
            self.assertEqual(tournament["secondary_tiebreak_label"], "BH-C1")
            pairings_round_1 = fetch_pairings(db, tournament["id"], 1)
            pairings_round_2 = fetch_pairings(db, tournament["id"], 2)

        self.assertEqual(len(pairings_round_1), 2)
        self.assertEqual(len(pairings_round_2), 2)
        self.assertEqual(pairings_round_1[1]["result_code"], "BYE")


if __name__ == "__main__":
    unittest.main()
