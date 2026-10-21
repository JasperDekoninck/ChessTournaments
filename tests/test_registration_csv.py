from __future__ import annotations

import csv
import io
import tempfile
import unittest
from pathlib import Path

from flaskr import create_app
from flaskr.db import get_db
from flaskr.rating_integration import _clear_runtime_caches
from rating import Manager, Player, PlayerDatabase, Rating


RATING_HEADER = "Rating (Fide, Lichess, chess.com, ...)"
HEADERS = ["Submission Time", "Select", "Full Name", "Email Address", RATING_HEADER]


class RegistrationCsvTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.app = create_app({
            "TESTING": True, "SECRET_KEY": "csv-tests", "INSTANCE_PATH": self.root,
            "MAIL_ENABLED": False,
        })
        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session["is_admin"] = True

    def tearDown(self):
        _clear_runtime_caches()
        self.tempdir.cleanup()

    @staticmethod
    def csv_bytes(rows, headers=HEADERS, *, bom=False, delimiter=","):
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=headers, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)
        return output.getvalue().encode("utf-8-sig" if bom else "utf-8")

    def upload(self, content, *, filename="Forminator export.csv", **extra):
        return self.client.post("/admin/tournaments", data={
            "name": "CSV Trial", "event_date": "2026-10-21", "rounds_planned": "3",
            "registrations": (io.BytesIO(content), filename), **extra,
        }, content_type="multipart/form-data", follow_redirects=True)

    def rows(self, sql, args=()):
        with self.app.app_context():
            return [dict(row) for row in get_db().execute(sql, args)]

    def test_complete_forminator_export_handles_bom_quotes_unicode_and_status_filter(self):
        content = self.csv_bytes([
            {"Submission Time": "Oct 20, 2026 @ 06:30 PM", "Select": "Registered", "Full Name": ' Zoë Müller, "Z" ', "Email Address": " zoe@example.com ", RATING_HEADER: "1825"},
            {"Submission Time": "Oct 20, 2026 @ 18:45", "Select": " rEgIsTeReD ", "Full Name": "陈 明", "Email Address": "chen@example.com", RATING_HEADER: "1700.0"},
            {"Select": "", "Full Name": "Lin Zhao"},
            {"Select": "Waiting list", "Full Name": "Waiting Player"},
            {"Select": "Cancelled", "Full Name": "Cancelled Player"},
            {"Select": "Registered", "Full Name": " "},
        ], bom=True)
        response = self.upload(content, filename="my arbitrary filename.txt")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Tournament created", response.data)
        entries = self.rows("SELECT * FROM tournament_entry ORDER BY id")
        self.assertEqual([row["imported_name"] for row in entries], ['Zoë Müller, "Z"', "陈 明", "Lin Zhao"])
        self.assertEqual([row["imported_email"] for row in entries], ["zoe@example.com", "chen@example.com", None])
        self.assertEqual([row["seed_rating"] for row in entries], [1825, 1700, 1500])
        self.assertEqual([row["submitted_at"] for row in entries[:2]], ["2026-10-20T18:30:00", "2026-10-20T18:45:00"])
        self.assertTrue(all(row["waitlist_position"] is None and not row["is_active"] for row in entries))
        self.assertEqual(len(self.rows("SELECT * FROM entry_round_status")), 9)
        self.assertEqual(self.rows("SELECT registration_csv_name FROM tournament")[0]["registration_csv_name"], "my arbitrary filename.txt")

    def test_name_only_import_defaults_optional_fields(self):
        response = self.upload(self.csv_bytes([{"Full Name": "Alice Example"}, {"Full Name": "Bob Sample"}], ["Full Name"]))
        self.assertEqual(response.status_code, 200)
        entries = self.rows("SELECT * FROM tournament_entry ORDER BY id")
        self.assertEqual(len(entries), 2)
        self.assertTrue(all(row["declared_rating"] is None and row["seed_rating"] == 1500 for row in entries))
        self.assertTrue(all(row["imported_email"] is None and row["submitted_at"] is None for row in entries))

    def test_saved_rating_is_used_in_preference_to_declared_rating(self):
        manager = Manager(player_database=PlayerDatabase(strict=False))
        manager.player_database.add(Player("Maya Schmid", rating=Rating(1876.4)))
        rating_dir = self.root / "rating"
        rating_dir.mkdir(exist_ok=True)
        manager.save(str(rating_dir / "current.json"))
        response = self.upload(self.csv_bytes([{"Full Name": "Maya Schmid", RATING_HEADER: "1200"}]))
        self.assertEqual(response.status_code, 200)
        entry = self.rows("SELECT seed_rating, declared_rating FROM tournament_entry")[0]
        self.assertEqual(entry, {"seed_rating": 1876, "declared_rating": 1200})

    def test_duplicate_registration_does_not_create_duplicate_players_or_entries(self):
        response = self.upload(self.csv_bytes([
            {"Full Name": "Alice Example", "Email Address": "alice@example.com", RATING_HEADER: "1600"},
            {"Full Name": "Alice Example", "Email Address": "alice@example.com", RATING_HEADER: "1700"},
        ]))
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Imported 1 registration.", response.data)
        self.assertEqual(len(self.rows("SELECT * FROM player")), 1)
        entries = self.rows("SELECT * FROM tournament_entry")
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["declared_rating"], 1700)
        self.assertEqual(len(self.rows("SELECT * FROM entry_round_status")), 3)

    def test_invalid_optional_ratings_do_not_crash_import(self):
        response = self.upload(self.csv_bytes([
            {"Full Name": "Alice Example", RATING_HEADER: "inf"},
            {"Full Name": "Bob Sample", RATING_HEADER: "nan"},
            {"Full Name": "Cara New", RATING_HEADER: "not known"},
        ]))
        self.assertEqual(response.status_code, 200)
        entries = self.rows("SELECT * FROM tournament_entry")
        self.assertEqual(len(entries), 3)
        self.assertTrue(all(row["declared_rating"] is None and row["seed_rating"] == 1500 for row in entries))

    def test_invalid_uploads_show_errors_without_creating_a_tournament(self):
        cases = {
            "missing name header": b"Participant,Email\nAlice,alice@example.com\n",
            "empty file": b"",
            "headers only": b"Full Name,Email Address\n",
            "no registered names": b"Full Name,Select\nAlice,Cancelled\n",
            "bad encoding": b"Full Name\nAndr\xe9\n",
            "unclosed quote": b'Full Name\n"Alice\n',
            "extra values": b"Full Name,Email Address\nAlice,alice@example.com,unexpected\n",
            "duplicate headers": b"Name,Name\nAlice,Bob\n",
        }
        for label, content in cases.items():
            with self.subTest(label=label):
                response = self.upload(content)
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"flash-error", response.data)
                self.assertEqual(self.rows("SELECT * FROM tournament"), [])
                self.assertEqual(self.rows("SELECT * FROM player"), [])

    def test_forgiving_full_name_column_headers(self):
        for header in ("Name", " FULL NAME ", "full_name", "FullName", "playerName", "Participant name", "Please enter your name", "Full name (required)", "First and last name"):
            with self.subTest(header=header):
                response = self.upload(self.csv_bytes([{header: "Ada Lovelace"}], [header]))
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"Imported 1 registration.", response.data)
                self.assertEqual(self.rows("SELECT imported_name FROM tournament_entry ORDER BY id DESC LIMIT 1")[0]["imported_name"], "Ada Lovelace")

    def test_separate_first_and_last_names(self):
        for first, last in (("First Name", "Last Name"), ("first_name", "last_name"), ("firstName", "lastName"), ("Given name", "Family name"), ("First", "Surname"), ("Name (First)", "Name (Last)")):
            with self.subTest(headers=(first, last)):
                response = self.upload(self.csv_bytes([{first: " Ada ", last: " Lovelace "}], [first, last]))
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"Imported 1 registration.", response.data)
                self.assertEqual(self.rows("SELECT imported_name FROM tournament_entry ORDER BY id DESC LIMIT 1")[0]["imported_name"], "Ada Lovelace")

    def test_explicit_full_name_is_preferred_and_blank_full_name_falls_back_to_parts(self):
        headers = ["Name", "Full name", "First name", "Last name"]
        response = self.upload(self.csv_bytes([
            {"Name": "Generic name", "Full name": "Ada Lovelace", "First name": "Other", "Last name": "Person"},
            {"First name": "Bob", "Last name": "Sample"},
        ], headers))
        self.assertEqual(response.status_code, 200)
        self.assertEqual([row["imported_name"] for row in self.rows("SELECT imported_name FROM tournament_entry ORDER BY id")], ["Ada Lovelace", "Bob Sample"])

    def test_common_email_elo_status_and_time_headers(self):
        headers = ["Participant name", "Contact E-mail", "Approximate Elo", "Registration status", "Submitted_at"]
        response = self.upload(self.csv_bytes([
            {"Participant name": "Ada Lovelace", "Contact E-mail": "ada@example.com", "Approximate Elo": "1800", "Registration status": "Registered", "Submitted_at": "Oct 20, 2026 @ 18:30"},
            {"Participant name": "Cancelled Example", "Registration status": "Cancelled"},
        ], headers))
        self.assertEqual(response.status_code, 200)
        rows = self.rows("SELECT imported_name, imported_email, seed_rating, submitted_at FROM tournament_entry")
        self.assertEqual(rows, [{"imported_name": "Ada Lovelace", "imported_email": "ada@example.com", "seed_rating": 1800, "submitted_at": "2026-10-20T18:30:00"}])

    def test_comma_semicolon_and_tab_separated_files(self):
        for delimiter in (",", ";", "\t"):
            with self.subTest(delimiter=delimiter):
                response = self.upload(self.csv_bytes([
                    {"Full Name": "Ada Lovelace", "Email Address": "ada@example.com", RATING_HEADER: "1800"},
                ], delimiter=delimiter))
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"Imported 1 registration.", response.data)
                self.assertEqual(self.rows("SELECT seed_rating FROM tournament_entry ORDER BY id DESC LIMIT 1")[0]["seed_rating"], 1800)

    def test_imported_players_can_check_in_and_be_paired(self):
        response = self.upload(self.csv_bytes([
            {"Full Name": "Alice Example", "Email Address": "alice@example.com", RATING_HEADER: "1700"},
            {"Full Name": "Bob Sample", "Email Address": "bob@example.com", RATING_HEADER: "1500"},
        ]))
        self.assertEqual(response.status_code, 200)
        event = self.rows("SELECT * FROM tournament")[0]
        for entry in self.rows("SELECT id FROM tournament_entry"):
            self.assertEqual(self.client.post(f"/admin/t/{event['slug']}/entries/{entry['id']}/toggle").status_code, 302)
        self.assertEqual(self.client.post(f"/admin/t/{event['slug']}/round/1/generate").status_code, 302)
        pairings = self.rows("SELECT * FROM pairing")
        self.assertEqual(len(pairings), 1)
        self.assertIsNotNone(pairings[0]["white_entry_id"])
        self.assertIsNotNone(pairings[0]["black_entry_id"])

    def test_imported_registrations_count_towards_capacity(self):
        self.upload(self.csv_bytes([{"Full Name": "Alice Example"}, {"Full Name": "Bob Sample"}], ["Full Name"]))
        slug = self.rows("SELECT slug FROM tournament")[0]["slug"]
        self.client.post(f"/admin/t/{slug}/registration", data={
            "registration_enabled": "1", "registration_opens_at": "2020-01-01T12:00", "max_registrations": "2",
        })
        response = self.client.post(f"/register/{slug}", data={"name": "Cara New", "email": "cara@example.com"}, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"waiting list in position 1", response.data)
        self.assertEqual(self.rows("SELECT waitlist_position FROM tournament_entry WHERE imported_name = 'Cara New'")[0]["waitlist_position"], 1)


if __name__ == "__main__":
    unittest.main()
