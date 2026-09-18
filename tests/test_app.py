from __future__ import annotations

import csv
import io
import json
import random
import tempfile
import unittest
from io import BytesIO
from html.parser import HTMLParser
from pathlib import Path

from flaskr import create_app
from flaskr.auth import hash_password
from flaskr.core import (
    _pair_group,
    compute_standings,
    ensure_round_status_rows,
    fetch_availability,
    fetch_pairings,
    generate_swiss_pairings,
    replace_round_pairings,
)
from flaskr.db import _add_column_if_missing, _table_columns, get_db, init_db
from flaskr.mailer import registration_email_body, waitlist_confirmation_email_body
from flaskr.rating_integration import get_player_history, get_player_profile, import_rating_history, sync_member_statuses
from rating import Manager, PlayerDatabase


class ElementCollector(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.elements = []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        self.elements.append((tag, dict(attrs)))


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
        self.assertIn(b"No tournament in progress", response.data)
        self.assertNotIn(b"<h2>Leaderboard</h2>", response.data)

        self._publish_tournament(slug)
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(slug.encode("utf-8"), response.data)

    def test_public_boards_show_scores_next_to_players(self):
        slug = self._create_tournament(name="Public Boards Tournament")
        self._set_all_entries_active(slug)
        self._login()
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self._publish_tournament(slug)

        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        white_index = response.data.index(b'<th class="public-player-col public-player-col-white">White</th>')
        white_score_index = response.data.index(b'<th class="align-center public-score-col">Score</th>')
        result_index = response.data.index(b'<th class="align-center public-result-col">Result</th>')
        black_score_index = response.data.index(
            b'<th class="align-center public-score-col">Score</th>',
            white_score_index + 1,
        )
        black_index = response.data.index(b'<th class="public-player-col public-player-col-black">Black</th>')
        self.assertLess(white_index, result_index)
        self.assertLess(white_index, white_score_index)
        self.assertLess(white_score_index, result_index)
        self.assertLess(result_index, black_index)
        self.assertLess(result_index, black_score_index)
        self.assertLess(black_score_index, black_index)
        self.assertIn(b"0.0", response.data)

    def test_public_live_endpoint_reflects_scores_and_new_rounds(self):
        slug = self._create_tournament(name="Public Live Tournament")
        self._set_all_entries_active(slug)
        self._login()
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self._publish_tournament(slug)

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

        response = self.client.get(f"/t/{slug}/live?round_no=1&view=boards")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("version", payload)
        self.assertIn("1-0", payload["html"])
        self.assertIn("1.0", payload["html"])

        response = self.client.post(f"/admin/t/{slug}/round/2/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response = self.client.get(f"/t/{slug}/live?view=boards")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("Round 2", payload["html"])

    def test_admin_round_cards_render_score_buttons_for_results(self):
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Admin Result Buttons Tournament", "event_date": "2026-04-16", "rounds_planned": "1"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            slug = db.execute(
                "SELECT slug FROM tournament WHERE name = 'Admin Result Buttons Tournament'"
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

        response = self.client.get(f"/admin/t/{slug}?open_round=1")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'data-result-button-group', response.data)
        self.assertIn(b'data-result-choice="1-0"', response.data)
        self.assertIn(b'>1/2</button>', response.data)
        self.assertIn(b'data-result-choice="0-1"', response.data)
        self.assertIn(b'<select name="result_1">', response.data)

    def test_admin_password_hash_is_stored_in_database(self):
        with self.app.app_context():
            db = get_db()
            row = db.execute("SELECT value FROM app_config WHERE key = 'admin_password_hash'").fetchone()
        self.assertIsNotNone(row)
        self.assertTrue(row["value"].startswith("scrypt:"))

    def test_migration_helpers_reject_unsafe_sql_identifiers(self):
        with self.app.app_context():
            db = get_db()
            with self.assertRaises(ValueError):
                _table_columns(db, 'tournament; DROP TABLE tournament; --')
            with self.assertRaises(ValueError):
                _add_column_if_missing(db, "tournament", 'note"; DROP TABLE tournament; --', "TEXT")
            with self.assertRaises(ValueError):
                _add_column_if_missing(db, "tournament", "safe_note", "TEXT; DROP TABLE tournament; --")

    def test_migration_helpers_allow_safe_identifier_updates(self):
        with self.app.app_context():
            db = get_db()
            _add_column_if_missing(db, "tournament", "security_audit_note", "TEXT")
            self.assertIn("security_audit_note", _table_columns(db, "tournament"))

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
        self.assertIn(b"successfully registered", response.data)

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
        self.assertIn(b"successfully registered", response.data)

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

    def test_admin_add_player_uses_registration_fields_and_information_table(self):
        slug = self._create_tournament(name="Admin Custom Fields Tournament")

        self._login()
        response = self.client.post(
            f"/admin/t/{slug}/registration",
            data={
                "registration_enabled": "1",
                "registration_opens_at": "2026-04-15T18:00",
                "registration_field_type": ["dropdown", "text"],
                "registration_field_label": ["Gender", "Department"],
                "registration_field_options": ["Female\nMale\nOther", ""],
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Player information", response.data)
        self.assertIn(b'name="registration_field_0"', response.data)
        self.assertIn(b"Gender", response.data)
        self.assertIn(b"Department", response.data)

        response = self.client.post(
            f"/admin/t/{slug}/entries",
            data={
                "name": "Admin Field Player",
                "email": "admin-fields@example.com",
                "declared_rating": "1650",
                "registration_field_0": "Female",
                "registration_field_1": "D-MATH",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Admin Field Player", response.data)
        self.assertIn(b"admin-fields@example.com", response.data)
        self.assertIn(b"Female", response.data)
        self.assertIn(b"D-MATH", response.data)

        with self.app.app_context():
            db = get_db()
            entry = db.execute(
                """
                SELECT registration_answers_json
                FROM tournament_entry
                WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?)
                  AND imported_name = 'Admin Field Player'
                """,
                (slug,),
            ).fetchone()
        self.assertIsNotNone(entry)
        self.assertEqual(
            json.loads(entry["registration_answers_json"]),
            [
                {"label": "Gender", "type": "dropdown", "value": "Female"},
                {"label": "Department", "type": "text", "value": "D-MATH"},
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
        self.assertIn(b"successfully registered", response.data)

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

    def test_registration_notices_say_membership_is_free(self):
        tournament = {"name": "Free Tournament", "event_date": "2026-10-01", "event_time": None, "venue": None}
        response = self.client.get("/register")
        self.assertEqual(response.status_code, 200)
        notices = {
            "registration page": response.get_data(as_text=True),
            "confirmed registration": registration_email_body(tournament, "Player", None),
            "waitlisted registration": registration_email_body(tournament, "Player", 1),
            "waitlist confirmation": waitlist_confirmation_email_body(tournament, "Player"),
        }
        for name, notice in notices.items():
            with self.subTest(notice=name):
                self.assertIn("Membership is free this year, you will not have to pay to participate in the tournament.", notice)
                for old_wording in ("CHF 5", "5 CHF", "TWINT", "bank transfers", "payable at the start"):
                    self.assertNotIn(old_wording, notice)

    def test_shared_navigation_identifies_current_page_and_content_landmark(self):
        for path in ("/", "/register", "/ratings"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"<strong>Schwarzer K&ouml;nig</strong>", response.data)
                elements = ElementCollector(response.get_data(as_text=True)).elements
                current_links = [attrs for tag, attrs in elements if tag == "a" and attrs.get("aria-current") == "page"]
                self.assertEqual([attrs["href"] for attrs in current_links], [path])
                self.assertEqual([attrs.get("id") for tag, attrs in elements if tag == "main"], ["main-content"])
                self.assertTrue(any(tag == "a" and attrs.get("href") == "#main-content" for tag, attrs in elements))
        self._login()
        response = self.client.get("/admin")
        self.assertEqual(response.status_code, 200)
        elements = ElementCollector(response.get_data(as_text=True)).elements
        self.assertTrue(any(tag == "a" and attrs.get("href") == "/admin" and attrs.get("aria-current") == "page" for tag, attrs in elements))

    def test_interface_assets_are_local_and_headings_are_not_forced_uppercase(self):
        response = self.client.get("/static/style.css")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("text-transform: uppercase", response.get_data(as_text=True))
        response.close()
        for asset in ("fonts/manrope.ttf", "icons/trophy.svg", "icons/user-plus.svg", "icons/x.svg", "fonts/OFL.txt", "icons/LICENSE"):
            with self.subTest(asset=asset):
                response = self.client.get(f"/static/{asset}")
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.data)
                response.close()

    def test_icons_use_external_styles_compatible_with_content_security_policy(self):
        stylesheet = Path(self.app.static_folder, "style.css").read_text(encoding="utf-8")
        icon_template = self.app.jinja_env.get_template("_ui.html")
        for asset in sorted(Path(self.app.static_folder, "icons").glob("*.svg")):
            with self.subTest(icon=asset.stem):
                elements = ElementCollector(str(icon_template.module.icon(asset.stem))).elements
                self.assertEqual(len(elements), 1)
                tag, attrs = elements[0]
                self.assertEqual(tag, "span")
                self.assertNotIn("style", attrs)
                self.assertEqual(attrs["aria-hidden"], "true")
                self.assertIn(f"ui-icon-{asset.stem}", attrs["class"].split())
                self.assertIn(
                    f'.ui-icon-{asset.stem} {{\n  --icon: url("icons/{asset.name}");\n}}',
                    stylesheet,
                )

        slug = self._create_tournament(name="Icon Rendering Tournament")
        for path in (f"/t/{slug}", "/register", "/ratings", "/admin", f"/admin/t/{slug}"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 200)
                self.assertIn("style-src 'self';", response.headers["Content-Security-Policy"])
                icons = [
                    attrs for _, attrs in ElementCollector(response.get_data(as_text=True)).elements
                    if "ui-icon" in attrs.get("class", "").split()
                ]
                self.assertTrue(icons)
                for attrs in icons:
                    self.assertNotIn("style", attrs)
                    icon_classes = [name for name in attrs["class"].split() if name.startswith("ui-icon-")]
                    self.assertEqual(len(icon_classes), 1)
                    self.assertIn(f".{icon_classes[0]} {{", stylesheet)

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

    def test_reset_tournament_registrations_clears_entries_pairings_and_keeps_settings(self):
        slug = self._create_tournament(name="Reset Registrations Tournament")
        self._set_all_entries_active(slug)
        self._login()
        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            db.execute(
                """
                UPDATE tournament
                SET registration_enabled = 1,
                    registration_opens_at = '2026-04-15T18:00',
                    registration_form_json = ?,
                    event_time = '19:00',
                    venue = 'CAB H52',
                    max_registrations = 32,
                    status = 'running',
                    is_public = 1,
                    is_active_public = 1,
                    public_insights_json = '{}'
                WHERE id = ?
                """,
                (
                    json.dumps([{"type": "text", "label": "Gender", "options": []}]),
                    tournament["id"],
                ),
            )
            db.commit()

        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response = self.client.post(f"/admin/t/{slug}/reset-registrations", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Registrations, pairings, and results were reset.", response.data)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute(
                """
                SELECT id, status, registration_enabled, registration_opens_at, registration_form_json,
                       event_time, venue, max_registrations, is_public, is_active_public, public_insights_json
                FROM tournament
                WHERE slug = ?
                """,
                (slug,),
            ).fetchone()
            self.assertIsNotNone(tournament)
            entry_count = db.execute(
                "SELECT COUNT(*) AS count FROM tournament_entry WHERE tournament_id = ?",
                (tournament["id"],),
            ).fetchone()["count"]
            pairing_count = db.execute(
                "SELECT COUNT(*) AS count FROM pairing WHERE tournament_id = ?",
                (tournament["id"],),
            ).fetchone()["count"]
            availability_count = db.execute(
                """
                SELECT COUNT(*) AS count
                FROM entry_round_status ers
                JOIN tournament_entry e ON e.id = ers.entry_id
                WHERE e.tournament_id = ?
                """,
                (tournament["id"],),
            ).fetchone()["count"]

        self.assertEqual(entry_count, 0)
        self.assertEqual(pairing_count, 0)
        self.assertEqual(availability_count, 0)
        self.assertEqual(tournament["status"], "draft")
        self.assertEqual(tournament["registration_enabled"], 1)
        self.assertEqual(tournament["registration_opens_at"], "2026-04-15T18:00")
        self.assertEqual(json.loads(tournament["registration_form_json"])[0]["label"], "Gender")
        self.assertEqual(tournament["event_time"], "19:00")
        self.assertEqual(tournament["venue"], "CAB H52")
        self.assertEqual(tournament["max_registrations"], 32)
        self.assertEqual(tournament["is_public"], 0)
        self.assertEqual(tournament["is_active_public"], 0)
        self.assertIsNone(tournament["public_insights_json"])

    def test_delete_tournament_removes_tournament_owned_rows(self):
        slug = self._create_tournament(name="Delete Tournament")
        self._set_all_entries_active(slug)
        self._login()
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        with self.app.app_context():
            db = get_db()
            tournament_id = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()["id"]

        response = self.client.post(f"/admin/t/{slug}/delete", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Deleted Delete Tournament.", response.data)

        with self.app.app_context():
            db = get_db()
            tournament_count = db.execute(
                "SELECT COUNT(*) AS count FROM tournament WHERE id = ?",
                (tournament_id,),
            ).fetchone()["count"]
            entry_count = db.execute(
                "SELECT COUNT(*) AS count FROM tournament_entry WHERE tournament_id = ?",
                (tournament_id,),
            ).fetchone()["count"]
            pairing_count = db.execute(
                "SELECT COUNT(*) AS count FROM pairing WHERE tournament_id = ?",
                (tournament_id,),
            ).fetchone()["count"]

        self.assertEqual(tournament_count, 0)
        self.assertEqual(entry_count, 0)
        self.assertEqual(pairing_count, 0)

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

    def test_ratings_page_uses_manager_rows_without_csv_export(self):
        (self.export_dir / "anonymous_leaderboard.csv").write_text(
            "\n".join(
                [
                    "Rank,Name,Rating,Wins,Losses,Draws",
                    "1,Stale Cached Leader,2100,12,3,1",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Ratings Page Tournament", "event_date": "2026-04-16", "rounds_planned": "1"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        with self.app.app_context():
            db = get_db()
            slug = db.execute("SELECT slug FROM tournament WHERE name = 'Ratings Page Tournament'").fetchone()["slug"]

        for index, name in enumerate(("Alpha Example", "Beta Example"), start=1):
            response = self.client.post(
                f"/admin/t/{slug}/entries",
                data={"name": name, "declared_rating": str(1600 - index * 100)},
                follow_redirects=True,
            )
            self.assertEqual(response.status_code, 200)

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

        response = self.client.get("/ratings")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Alpha Example", response.data)
        self.assertNotIn(b"Stale Cached Leader", response.data)
        self.assertNotIn(b"Current club ranking", response.data)
        self.assertNotIn(b'<p class="kicker">', response.data)
        self.assertNotIn(b"CSV", response.data)
        elements = ElementCollector(response.get_data(as_text=True)).elements
        self.assertTrue(any(tag == "table" and "compact-table" in attrs.get("class", "").split() for tag, attrs in elements))

        response = self.client.get("/leaderboard.csv")
        self.assertEqual(response.status_code, 404)

    def test_complete_tournament_writes_tournament_rating_export(self):
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

        self.assertFalse((self.export_dir / "anonymous_leaderboard.csv").exists())
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

    def test_complete_tournament_rejects_unfinished_round(self):
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Unfinished Completion Tournament", "event_date": "2026-04-16", "rounds_planned": "1"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        with self.app.app_context():
            db = get_db()
            slug = db.execute(
                "SELECT slug FROM tournament WHERE name = 'Unfinished Completion Tournament'"
            ).fetchone()["slug"]
        for name in ("Alice Example", "Bob Example"):
            response = self.client.post(
                f"/admin/t/{slug}/entries",
                data={"name": name, "declared_rating": "1600"},
                follow_redirects=True,
            )
            self.assertEqual(response.status_code, 200)
        self._set_all_entries_active(slug)
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        response = self.client.post(f"/admin/t/{slug}/complete", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Enter all results for round 1 before finishing", response.data)

        with self.app.app_context():
            db = get_db()
            status = db.execute("SELECT status FROM tournament WHERE slug = ?", (slug,)).fetchone()["status"]
        self.assertEqual(status, "running")

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
        self.assertIn(b"Final highlights", response.data)
        self.assertIn(b"Played above level", response.data)
        self.assertIn(b"Most unlikely win", response.data)
        self.assertIn(b"1st", response.data)
        rating_index = response.data.index(b"<th>Rating</th>")
        performance_index = response.data.index(b"<th>Performance</th>")
        score_index = response.data.index(b"<th>Score</th>")
        self.assertLess(rating_index, performance_index)
        self.assertLess(performance_index, score_index)

        response = self.client.get(f"/admin/t/{slug}/export.csv")
        self.assertEqual(response.status_code, 200)
        rows = list(csv.reader(io.StringIO(response.data.decode("utf-8"))))
        self.assertEqual(rows[0][:5], ["Rank", "Name", "Rating", "Performance", "Score"])
        self.assertTrue(any(row[3] for row in rows[1:]))

    def test_performance_column_is_hidden_until_computed(self):
        slug = self._create_tournament(name="No Performance Tournament")
        with self.app.app_context():
            db = get_db()
            db.execute(
                """
                UPDATE tournament
                SET status = 'completed', public_insights_json = NULL
                WHERE slug = ?
                """,
                (slug,),
            )
            db.commit()
        self._publish_tournament(slug)

        response = self.client.get(f"/t/{slug}?view=standings")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"<th>Performance</th>", response.data)

        response = self.client.get(f"/admin/t/{slug}/export.csv")
        self.assertEqual(response.status_code, 200)
        rows = list(csv.reader(io.StringIO(response.data.decode("utf-8"))))
        self.assertEqual(rows[0][:4], ["Rank", "Name", "Rating", "Score"])
        self.assertNotIn("Performance", rows[0])

    def test_completed_tournament_views_use_post_tournament_rating_when_available(self):
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Post Rating Display Tournament", "event_date": "2026-04-16", "rounds_planned": "1"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            slug = db.execute(
                "SELECT slug FROM tournament WHERE name = 'Post Rating Display Tournament'"
            ).fetchone()["slug"]

        for name, rating in (("Alpha Example", 1600), ("Beta Example", 1500)):
            response = self.client.post(
                f"/admin/t/{slug}/entries",
                data={"name": name, "declared_rating": str(rating)},
                follow_redirects=True,
            )
            self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            db.execute(
                "UPDATE tournament SET registration_form_json = ? WHERE slug = ?",
                (json.dumps([{"type": "text", "label": "Club", "options": []}]), slug),
            )
            db.commit()

        self._set_all_entries_active(slug)
        response = self.client.post(f"/admin/t/{slug}/round/1/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            db = get_db()
            tournament = db.execute("SELECT id FROM tournament WHERE slug = ?", (slug,)).fetchone()
            pairing = fetch_pairings(db, tournament["id"], 1)[0]
            seed_ratings = {
                row["imported_name"]: row["seed_rating"]
                for row in db.execute(
                    """
                    SELECT imported_name, seed_rating
                    FROM tournament_entry
                    WHERE tournament_id = ?
                    """,
                    (tournament["id"],),
                ).fetchall()
            }

        result = "1-0" if pairing["white_name"] == "Beta Example" else "0-1"
        response = self.client.post(
            f"/admin/t/{slug}/round/1/save",
            data={
                "board_count": "1",
                "white_1": str(pairing["white_entry_id"]),
                "black_1": str(pairing["black_entry_id"]),
                "result_1": result,
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        response = self.client.post(f"/admin/t/{slug}/complete", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self._publish_tournament(slug)

        with self.app.app_context():
            profile = get_player_profile("Beta Example")

        self.assertIsNotNone(profile)
        post_rating = profile["rating"]
        seed_rating = seed_ratings["Beta Example"]
        self.assertNotEqual(post_rating, seed_rating)

        response = self.client.get(f"/t/{slug}?view=standings")
        self.assertEqual(response.status_code, 200)
        html = response.data.decode("utf-8")
        standings_start = html.index('class="public-standings-table"')
        standings_table = html[standings_start : html.index("</table>", standings_start)]
        self.assertIn(f"<td>{post_rating}</td>", standings_table)
        self.assertNotIn(f"<td>{seed_rating}</td>", standings_table)

        response = self.client.get(f"/admin/t/{slug}")
        self.assertEqual(response.status_code, 200)
        html = response.data.decode("utf-8")
        player_info_start = html.index("<h3>Player information</h3>")
        player_info_table = html[player_info_start : html.index("admin-danger-zone", player_info_start)]
        self.assertIn(f"<td>{post_rating}</td>", player_info_table)
        self.assertNotIn(f"<td>{seed_rating}</td>", player_info_table)

        response = self.client.get(f"/admin/t/{slug}/export.csv")
        self.assertEqual(response.status_code, 200)
        rows = list(csv.reader(io.StringIO(response.data.decode("utf-8"))))
        rating_index = rows[0].index("Rating")
        beta_row = next(row for row in rows[1:] if row[1] == "Beta Example")
        self.assertEqual(beta_row[rating_index], str(post_rating))
        self.assertNotEqual(beta_row[rating_index], str(seed_rating))

    def test_admin_only_shows_top_10_secondary_prize_rankings(self):
        slug = self._create_tournament(name="Secondary Prize Tournament")
        insights = {
            "above_level": {
                "name": "Performance Player 01",
                "start_rating": 1500,
                "performance_rating": 2100,
                "probability": 0.01,
                "normalized_boost": 2.3,
            },
            "biggest_upset": {
                "winner": "Game Winner 01",
                "winner_rating": 1450,
                "loser": "Game Loser 01",
                "loser_rating": 2200,
                "result": "1-0",
                "win_probability": 0.02,
            },
            "surprising_performance_ranking": [
                {
                    "name": f"Performance Player {index:02d}",
                    "start_rating": 1500 + index,
                    "performance_rating": 2100 - index,
                    "probability": index / 100,
                    "normalized_boost": 3.0 - index / 10,
                }
                for index in range(1, 13)
            ],
            "surprising_game_ranking": [
                {
                    "winner": f"Game Winner {index:02d}",
                    "winner_rating": 1450 + index,
                    "loser": f"Game Loser {index:02d}",
                    "loser_rating": 2200 - index,
                    "result": "1-0" if index % 2 else "0-1",
                    "win_probability": index / 100,
                }
                for index in range(1, 13)
            ],
        }
        with self.app.app_context():
            db = get_db()
            db.execute(
                """
                UPDATE tournament
                SET status = 'completed', public_insights_json = ?
                WHERE slug = ?
                """,
                (json.dumps(insights), slug),
            )
            db.commit()

        response = self.client.get(f"/admin/t/{slug}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Secondary prizes", response.data)
        self.assertIn(b"Most surprising performance", response.data)
        self.assertIn(b"Most surprising game", response.data)
        self.assertIn(b"Performance Player 10", response.data)
        self.assertNotIn(b"Performance Player 11", response.data)
        self.assertIn(b"Game Winner 10", response.data)
        self.assertNotIn(b"Game Winner 11", response.data)

        self._publish_tournament(slug)
        response = self.client.get(f"/t/{slug}?view=standings")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Final highlights", response.data)
        self.assertIn(b"Performance Player 01", response.data)
        self.assertIn(b"Game Winner 01", response.data)
        self.assertNotIn(b"Secondary prizes", response.data)
        self.assertNotIn(b"Performance Player 02", response.data)
        self.assertNotIn(b"Game Winner 02", response.data)

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
                SELECT e.seed_rating, e.imported_name, e.is_active
                FROM tournament_entry e
                JOIN tournament t ON t.id = e.tournament_id
                WHERE t.slug = ? AND e.imported_name = 'Manual Player'
                """,
                (slug,),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["seed_rating"], 1750)
        self.assertEqual(row["is_active"], 1)

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

    def test_current_round_pairings_can_include_late_active_player_after_marking_them_in(self):
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
                SELECT e.id, e.is_active
                FROM tournament_entry e
                WHERE e.tournament_id = ? AND e.imported_name = 'Ana Example'
                """,
                (tournament["id"],),
            ).fetchone()
            ana_round_one = db.execute(
                "SELECT is_available FROM entry_round_status WHERE entry_id = ? AND round_no = 1",
                (ana_entry["id"],),
            ).fetchone()
            initial_pairings = fetch_pairings(db, tournament["id"], 1)

        self.assertEqual(ana_entry["is_active"], 1)
        self.assertEqual(ana_round_one["is_available"], 0)

        response = self.client.post(
            f"/admin/t/{slug}/entries/{ana_entry['id']}/availability",
            data={"round_no": "1"},
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

    def test_manual_round_save_allows_repeat_pairing_override(self):
        self._login()
        response = self.client.post(
            "/admin/tournaments",
            data={"name": "Repeat Pairing Validation Tournament", "event_date": "2026-04-16", "rounds_planned": "2"},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        with self.app.app_context():
            db = get_db()
            slug = db.execute(
                "SELECT slug FROM tournament WHERE name = 'Repeat Pairing Validation Tournament'"
            ).fetchone()["slug"]

        for index, name in enumerate(("Alice Example", "Bob Example", "Cara Example", "Dan Example"), start=1):
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
            round_one = fetch_pairings(db, tournament["id"], 1)

        form = {"board_count": str(len(round_one))}
        for pairing in round_one:
            form[f"white_{pairing['board_no']}"] = str(pairing["white_entry_id"])
            form[f"black_{pairing['board_no']}"] = str(pairing["black_entry_id"])
            form[f"result_{pairing['board_no']}"] = "1-0"
        response = self.client.post(f"/admin/t/{slug}/round/1/save", data=form, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response = self.client.post(f"/admin/t/{slug}/round/2/generate", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

        repeated = round_one[0]
        remaining_ids = {
            pairing["white_entry_id"]
            for pairing in round_one
        } | {
            pairing["black_entry_id"]
            for pairing in round_one
            if pairing["black_entry_id"] is not None
        }
        remaining_ids -= {repeated["white_entry_id"], repeated["black_entry_id"]}
        other_white, other_black = sorted(remaining_ids)
        response = self.client.post(
            f"/admin/t/{slug}/round/2/save",
            data={
                "board_count": "2",
                "white_1": str(repeated["white_entry_id"]),
                "black_1": str(repeated["black_entry_id"]),
                "result_1": "1-0",
                "white_2": str(other_white),
                "black_2": str(other_black),
                "result_2": "1/2-1/2",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"This manual override repeats an earlier pairing.", response.data)
        with self.app.app_context():
            db = get_db()
            repeated_round_two = db.execute(
                """
                SELECT COUNT(*) AS count
                FROM pairing
                WHERE tournament_id = (SELECT id FROM tournament WHERE slug = ?)
                  AND round_no = 2
                  AND white_entry_id = ?
                  AND black_entry_id = ?
                  AND manual_override = 1
                """,
                (slug, repeated["white_entry_id"], repeated["black_entry_id"]),
            ).fetchone()["count"]
        self.assertEqual(repeated_round_two, 1)

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
        self.client.post("/admin/members/cutoff", data={"member_since_date": "2025-09-01"})
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

    def test_pair_group_avoids_same_absolute_colour_preferences(self):
        group = [
            {
                "entry_id": 1,
                "name": "Alpha",
                "score": 1.0,
                "seed_rating": 2100,
                "opponent_ids": set(),
                "white_games": 2,
                "black_games": 0,
                "colors": ["W", "W"],
                "color_rounds": [(1, "W"), (2, "W")],
            },
            {
                "entry_id": 2,
                "name": "Bravo",
                "score": 1.0,
                "seed_rating": 2090,
                "opponent_ids": set(),
                "white_games": 0,
                "black_games": 2,
                "colors": ["B", "B"],
                "color_rounds": [(1, "B"), (2, "B")],
            },
            {
                "entry_id": 3,
                "name": "Charlie",
                "score": 1.0,
                "seed_rating": 1900,
                "opponent_ids": set(),
                "white_games": 2,
                "black_games": 0,
                "colors": ["W", "W"],
                "color_rounds": [(1, "W"), (2, "W")],
            },
            {
                "entry_id": 4,
                "name": "Delta",
                "score": 1.0,
                "seed_rating": 1890,
                "opponent_ids": set(),
                "white_games": 0,
                "black_games": 2,
                "colors": ["B", "B"],
                "color_rounds": [(1, "B"), (2, "B")],
            },
        ]

        pairs = _pair_group(group)
        paired_ids = {frozenset((white["entry_id"], black["entry_id"])) for white, black in pairs}
        self.assertEqual(paired_ids, {frozenset((1, 4)), frozenset((2, 3))})

    def test_pair_group_corrects_absolute_colour_streaks(self):
        group = [
            {
                "entry_id": 1,
                "name": "Alpha",
                "score": 1.0,
                "seed_rating": 2100,
                "opponent_ids": set(),
                "white_games": 2,
                "black_games": 0,
                "colors": ["W", "W"],
                "color_rounds": [(1, "W"), (2, "W")],
            },
            {
                "entry_id": 2,
                "name": "Bravo",
                "score": 1.0,
                "seed_rating": 2000,
                "opponent_ids": set(),
                "white_games": 0,
                "black_games": 2,
                "colors": ["B", "B"],
                "color_rounds": [(1, "B"), (2, "B")],
            },
        ]

        pairs = _pair_group(group)
        self.assertEqual(len(pairs), 1)
        white, black = pairs[0]
        self.assertEqual(white["entry_id"], 2)
        self.assertEqual(black["entry_id"], 1)

    def test_generate_swiss_pairings_does_not_repeat_pairing_allocated_bye(self):
        with self.app.app_context():
            db = get_db()
            cursor = db.execute(
                """
                INSERT INTO tournament (
                  name, slug, event_date, rounds_planned, status, source_type,
                  primary_tiebreak_label, secondary_tiebreak_label
                ) VALUES ('Bye Test', 'bye-test', '2026-04-16', 3, 'running', 'local', 'BH', 'BH-C1')
                """
            )
            tournament_id = cursor.lastrowid
            entry_ids = []
            for index, name in enumerate(["Alpha", "Bravo", "Charlie", "Delta", "Echo"], start=1):
                player_cursor = db.execute(
                    """
                    INSERT INTO player (
                      name, normalized_name, email, canonical_rating_name, member_status,
                      historical_rating, rating_deviation, historical_wins, historical_losses, historical_draws
                    ) VALUES (?, ?, NULL, ?, 'non-member', NULL, NULL, 0, 0, 0)
                    """,
                    (name, name.lower(), name),
                )
                entry_cursor = db.execute(
                    """
                    INSERT INTO tournament_entry (
                      tournament_id, player_id, imported_name, seed_rating, member_status, is_active
                    ) VALUES (?, ?, ?, ?, 'non-member', 1)
                    """,
                    (tournament_id, player_cursor.lastrowid, name, 2200 - index * 10),
                )
                entry_ids.append(entry_cursor.lastrowid)
            db.commit()
            ensure_round_status_rows(db, tournament_id, 3)
            replace_round_pairings(
                db,
                tournament_id,
                1,
                [
                    {"board_no": 1, "white_entry_id": entry_ids[0], "black_entry_id": entry_ids[3], "result_code": "1/2-1/2"},
                    {"board_no": 2, "white_entry_id": entry_ids[1], "black_entry_id": entry_ids[2], "result_code": "1/2-1/2"},
                    {"board_no": 3, "white_entry_id": entry_ids[4], "black_entry_id": None, "result_code": "BYE"},
                ],
            )

            round_two = generate_swiss_pairings(db, tournament_id, 2)

        bye_rows = [pairing for pairing in round_two if pairing["black_entry_id"] is None]
        self.assertEqual(len(bye_rows), 1)
        self.assertNotEqual(bye_rows[0]["white_entry_id"], entry_ids[4])

    def test_buchholz_handles_withdrawals_and_pairing_allocated_byes(self):
        with self.app.app_context():
            db = get_db()
            cursor = db.execute(
                """
                INSERT INTO tournament (
                  name, slug, event_date, rounds_planned, status, source_type,
                  primary_tiebreak_label, secondary_tiebreak_label
                ) VALUES ('Tie Break Test', 'tie-break-test', '2026-04-16', 3, 'completed', 'local', 'BH', 'BH-C1')
                """
            )
            tournament_id = cursor.lastrowid
            entry_ids = {}
            for index, name in enumerate(["Alpha", "Bravo", "Charlie", "Delta"], start=1):
                player_cursor = db.execute(
                    """
                    INSERT INTO player (
                      name, normalized_name, email, canonical_rating_name, member_status,
                      historical_rating, rating_deviation, historical_wins, historical_losses, historical_draws
                    ) VALUES (?, ?, NULL, ?, 'non-member', NULL, NULL, 0, 0, 0)
                    """,
                    (name, name.lower(), name),
                )
                entry_cursor = db.execute(
                    """
                    INSERT INTO tournament_entry (
                      tournament_id, player_id, imported_name, seed_rating, member_status, is_active
                    ) VALUES (?, ?, ?, ?, 'non-member', ?)
                    """,
                    (tournament_id, player_cursor.lastrowid, name, 2200 - index * 10, 0 if name == "Delta" else 1),
                )
                entry_ids[name] = entry_cursor.lastrowid
            db.commit()
            ensure_round_status_rows(db, tournament_id, 3)
            db.execute(
                """
                UPDATE entry_round_status
                SET is_available = 0
                WHERE entry_id = ? AND round_no = 3
                """,
                (entry_ids["Delta"],),
            )
            replace_round_pairings(
                db,
                tournament_id,
                1,
                [
                    {"board_no": 1, "white_entry_id": entry_ids["Alpha"], "black_entry_id": entry_ids["Delta"], "result_code": "1-0"},
                    {"board_no": 2, "white_entry_id": entry_ids["Bravo"], "black_entry_id": entry_ids["Charlie"], "result_code": "1-0"},
                ],
            )
            replace_round_pairings(
                db,
                tournament_id,
                2,
                [
                    {"board_no": 1, "white_entry_id": entry_ids["Alpha"], "black_entry_id": entry_ids["Charlie"], "result_code": "1-0"},
                    {"board_no": 2, "white_entry_id": entry_ids["Bravo"], "black_entry_id": entry_ids["Delta"], "result_code": "1-0"},
                ],
            )
            replace_round_pairings(
                db,
                tournament_id,
                3,
                [
                    {"board_no": 1, "white_entry_id": entry_ids["Alpha"], "black_entry_id": entry_ids["Bravo"], "result_code": "1-0"},
                    {"board_no": 2, "white_entry_id": entry_ids["Charlie"], "black_entry_id": None, "result_code": "BYE"},
                ],
            )

            rows = {row["name"]: row for row in compute_standings(db, tournament_id, prefer_stored=False)}

        self.assertEqual(rows["Alpha"]["score"], 3.0)
        self.assertEqual(rows["Bravo"]["score"], 2.0)
        self.assertEqual(rows["Charlie"]["score"], 1.0)
        self.assertEqual(rows["Delta"]["score"], 0.0)
        self.assertEqual(rows["Alpha"]["bh"], 3.5)
        self.assertEqual(rows["Bravo"]["bh"], 4.5)
        self.assertEqual(rows["Charlie"]["bh"], 6.0)
        self.assertEqual(rows["Charlie"]["bh_c1"], 5.0)
        self.assertEqual(rows["Delta"]["bh"], 5.0)
        self.assertEqual(rows["Delta"]["bh_c1"], 5.0)

    def test_random_tournaments_with_absences_and_withdrawals_keep_pairing_invariants(self):
        def create_random_tournament(db, seed: int) -> tuple[int, list[int]]:
            cursor = db.execute(
                """
                INSERT INTO tournament (
                  name, slug, event_date, rounds_planned, status, source_type,
                  primary_tiebreak_label, secondary_tiebreak_label
                ) VALUES (?, ?, '2026-04-16', 7, 'running', 'local', 'BH', 'BH-C1')
                """,
                (f"Random Tournament {seed}", f"random-tournament-{seed}"),
            )
            tournament_id = cursor.lastrowid
            entry_ids = []
            for index in range(1, 49):
                name = f"Random {seed}-{index:02d}"
                player_cursor = db.execute(
                    """
                    INSERT INTO player (
                      name, normalized_name, email, canonical_rating_name, member_status,
                      historical_rating, rating_deviation, historical_wins, historical_losses, historical_draws
                    ) VALUES (?, ?, NULL, ?, 'non-member', NULL, NULL, 0, 0, 0)
                    """,
                    (name, name.lower(), name),
                )
                entry_cursor = db.execute(
                    """
                    INSERT INTO tournament_entry (
                      tournament_id, player_id, imported_name, seed_rating, member_status, is_active
                    ) VALUES (?, ?, ?, ?, 'non-member', 1)
                    """,
                    (tournament_id, player_cursor.lastrowid, name, 2200 - index),
                )
                entry_ids.append(entry_cursor.lastrowid)
            db.commit()
            ensure_round_status_rows(db, tournament_id, 7)
            return tournament_id, entry_ids

        def validate_boards(db, tournament_id: int, round_no: int, boards: list[dict], previous_pairs: set[frozenset[int]]):
            availability = fetch_availability(db, tournament_id)
            seen = set()
            byes = []
            for board in boards:
                white_id = board["white_entry_id"]
                black_id = board["black_entry_id"]
                self.assertNotIn(white_id, seen)
                seen.add(white_id)
                self.assertTrue(availability.get(white_id, {}).get(round_no, True))
                if black_id is None:
                    byes.append(white_id)
                    continue
                self.assertNotIn(black_id, seen)
                seen.add(black_id)
                self.assertTrue(availability.get(black_id, {}).get(round_no, True))
                pair_key = frozenset((white_id, black_id))
                self.assertNotIn(pair_key, previous_pairs)
                previous_pairs.add(pair_key)
            self.assertLessEqual(len(byes), 1)

        with self.app.app_context():
            db = get_db()
            for seed in range(20):
                rng = random.Random(seed)
                tournament_id, entry_ids = create_random_tournament(db, seed)
                previous_pairs: set[frozenset[int]] = set()
                bye_counts = {entry_id: 0 for entry_id in entry_ids}
                withdrawn = set()
                for round_no in range(1, 8):
                    for entry_id in entry_ids:
                        if entry_id in withdrawn:
                            db.execute(
                                "UPDATE entry_round_status SET is_available = 0 WHERE entry_id = ? AND round_no = ?",
                                (entry_id, round_no),
                            )
                            continue
                        if round_no >= 3 and rng.random() < 0.025:
                            withdrawn.add(entry_id)
                            db.execute("UPDATE tournament_entry SET is_active = 0 WHERE id = ?", (entry_id,))
                            db.execute(
                                "UPDATE entry_round_status SET is_available = 0 WHERE entry_id = ? AND round_no >= ?",
                                (entry_id, round_no),
                            )
                            continue
                        if rng.random() < 0.08:
                            db.execute(
                                "UPDATE entry_round_status SET is_available = 0 WHERE entry_id = ? AND round_no = ?",
                                (entry_id, round_no),
                            )
                    db.commit()

                    available_count = db.execute(
                        """
                        SELECT COUNT(*) AS c
                        FROM tournament_entry e
                        JOIN entry_round_status ers ON ers.entry_id = e.id AND ers.round_no = ?
                        WHERE e.tournament_id = ? AND e.is_active = 1 AND ers.is_available = 1
                        """,
                        (round_no, tournament_id),
                    ).fetchone()["c"]
                    boards = generate_swiss_pairings(db, tournament_id, round_no)
                    if available_count >= 2:
                        self.assertTrue(boards, f"seed={seed} round={round_no} available={available_count}")
                    validate_boards(db, tournament_id, round_no, boards, previous_pairs)

                    for board in boards:
                        if board["black_entry_id"] is None:
                            bye_counts[board["white_entry_id"]] += 1
                            board["result_code"] = "BYE"
                            continue
                        board["result_code"] = rng.choice(["1-0", "0-1", "1/2-1/2"])
                    replace_round_pairings(db, tournament_id, round_no, boards)

                rows = compute_standings(db, tournament_id, prefer_stored=False)
                self.assertEqual(len(rows), 48)
                self.assertEqual([row["rank"] for row in rows], list(range(1, 49)))
                for row in rows:
                    self.assertGreaterEqual(row["score"], 0.0)
                    self.assertLessEqual(row["score"], 7.0)
                    self.assertGreaterEqual(row["bh"], row["bh_c1"])
                    self.assertGreaterEqual(row["bh_c1"], 0.0)
                self.assertLessEqual(max(bye_counts.values()), 1)

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
