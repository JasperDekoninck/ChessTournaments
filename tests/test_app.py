from __future__ import annotations

import tempfile
import unittest
from io import BytesIO
from pathlib import Path

from flaskr import create_app
from flaskr.core import fetch_pairings
from flaskr.db import get_db, init_db
from flaskr.rating_integration import import_rating_history
from rating import Manager, PlayerDatabase


class TournamentAppTestCase(unittest.TestCase):
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
        self.csv_bytes = Path("forminator-easter-tournament-2026-260416193422.csv").read_bytes()

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

    def test_public_home_uses_active_tournament(self):
        slug = self._create_tournament()
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(slug.encode("utf-8"), response.data)

    def test_create_tournament_and_generate_pairings(self):
        slug = self._create_tournament()
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

    def test_complete_tournament_writes_rating_exports(self):
        slug = self._create_tournament(name="Completed Tournament")
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
