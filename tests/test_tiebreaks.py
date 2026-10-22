from __future__ import annotations

import csv
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from flaskr import create_app, rating_integration as ratings
from flaskr.auth import admin_session_version
from flaskr.core import compute_standings, persist_final_standings
from flaskr.db import get_db, migrate_db


class TiebreaksTestCase(unittest.TestCase):
    PLAYERS = (("Alice", 1500), ("Bob", 1800), ("Charlie", 1700), ("Diana", 1600))
    GAMES = (
        (1, "Alice", "Bob", "1-0"), (1, "Charlie", "Diana", "1-0"),
        (2, "Alice", "Charlie", "0-1"), (2, "Bob", "Diana", "1-0"),
        (3, "Alice", "Diana", "1/2-1/2"), (3, "Bob", "Charlie", "1/2-1/2"),
    )

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        rating_dir = self.root / "rating"
        rating_dir.mkdir()
        for name in ("anonymous.txt", "not_anonymous.txt"):
            (rating_dir / name).write_text("")
        self.app = create_app({
            "TESTING": True, "SECRET_KEY": "tiebreak-tests", "INSTANCE_PATH": self.root,
            "MAIL_ENABLED": False,
        })
        self.context = self.app.app_context()
        self.context.push()
        self.db = get_db()

    def tearDown(self):
        self.context.pop()
        ratings._clear_runtime_caches()
        self.tempdir.cleanup()

    def event(self, *, version=2, players=None, games=None, name="Cup", rated=True, team=False):
        players = self.PLAYERS if players is None else players
        games = self.GAMES if games is None else games
        tournament_id = self.db.execute(
            """INSERT INTO tournament
            (name, slug, event_date, rounds_planned, status, tiebreak_version, excludes_rating, is_team)
            VALUES (?, ?, '2026-10-22', ?, 'completed', ?, ?, ?)""",
            (name, name.lower(), max(game[0] for game in games), version, int(not rated), int(team)),
        ).lastrowid
        entries = {}
        for player_name, seed in players:
            player = self.db.execute("SELECT id FROM player WHERE name = ?", (player_name,)).fetchone()
            player_id = player[0] if player else self.db.execute(
                "INSERT INTO player (name, normalized_name, canonical_rating_name) VALUES (?, ?, ?)",
                (player_name, player_name.lower(), player_name),
            ).lastrowid
            entries[player_name] = self.db.execute(
                """INSERT INTO tournament_entry
                (tournament_id, player_id, imported_name, seed_rating, member_status)
                VALUES (?, ?, ?, ?, 'unknown')""",
                (tournament_id, player_id, player_name, seed),
            ).lastrowid
        boards = {}
        for round_no, white, black, result in games:
            boards[round_no] = boards.get(round_no, 0) + 1
            self.db.execute(
                """INSERT INTO pairing
                (tournament_id, round_no, board_no, white_entry_id, black_entry_id, result_code)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (tournament_id, round_no, boards[round_no], entries[white], entries[black], result),
            )
        self.db.commit()
        return tournament_id

    def performance(self, tournament_id, **values):
        self.db.execute("UPDATE tournament SET public_insights_json = ? WHERE id = ?", (
            json.dumps({"surprising_performance_ranking": [
                {"name": name, "performance_rating": value} for name, value in values.items()
            ]}), tournament_id,
        ))
        self.db.commit()

    def names(self, tournament_id, **kwargs):
        return [row["name"] for row in compute_standings(self.db, tournament_id, **kwargs)]

    def test_new_tournaments_use_duel_after_buchholz_and_before_seed(self):
        tournament_id = self.event(rated=False)
        standings = compute_standings(self.db, tournament_id)
        self.assertEqual([row["name"] for row in standings], ["Charlie", "Alice", "Bob", "Diana"])
        alice, bob = standings[1:3]
        self.assertEqual((alice["score"], alice["bh"], alice["bh_c1"]), (1.5, 4.5, 4.0))
        self.assertEqual((alice["score"], alice["bh"], alice["bh_c1"]),
                         (bob["score"], bob["bh"], bob["bh_c1"]))

    def test_rated_performance_precedes_duel_and_equal_performance_uses_duel(self):
        tournament_id = self.event()
        self.performance(tournament_id, Alice=1800, Bob=2200, Charlie=500, Diana=3000)
        self.assertEqual(self.names(tournament_id), ["Charlie", "Bob", "Alice", "Diana"])
        self.performance(tournament_id, Alice=2000, Bob=2000)
        self.assertEqual(self.names(tournament_id), ["Charlie", "Alice", "Bob", "Diana"])
        self.db.execute("UPDATE player SET canonical_rating_name = 'Canonical Alice' WHERE name = 'Alice'")
        self.performance(tournament_id, **{"Canonical Alice": 2300, "Bob": 2200})
        self.assertEqual(self.names(tournament_id), ["Charlie", "Alice", "Bob", "Diana"])

    def test_unrated_and_team_events_ignore_performance(self):
        for kwargs in ({"rated": False}, {"team": True}):
            with self.subTest(**kwargs):
                tournament_id = self.event(name=str(kwargs), **kwargs)
                self.performance(tournament_id, Alice=1800, Bob=2200)
                self.assertEqual(self.names(tournament_id), ["Charlie", "Alice", "Bob", "Diana"])

    def test_draw_uses_seed_then_name_and_earlier_round_ignores_final_performance(self):
        tournament_id = self.event(players=self.PLAYERS[:2], games=(
            (1, "Alice", "Bob", "1/2-1/2"), (2, "Alice", "Bob", "1/2-1/2"),
        ))
        self.assertEqual(self.names(tournament_id), ["Bob", "Alice"])
        self.db.execute("UPDATE tournament_entry SET seed_rating = 1500 WHERE tournament_id = ?", (tournament_id,))
        self.assertEqual(self.names(tournament_id), ["Alice", "Bob"])
        self.db.execute("UPDATE tournament_entry SET seed_rating = 1800 WHERE imported_name = 'Bob'")
        self.performance(tournament_id, Alice=2200, Bob=1800)
        self.assertEqual(self.names(tournament_id), ["Alice", "Bob"])
        self.assertEqual(self.names(tournament_id, through_round=1), ["Bob", "Alice"])

    def test_incomplete_tied_group_skips_duels(self):
        tournament_id = self.event(rated=False, games=(
            (1, "Alice", "Charlie", "1-0"), (1, "Bob", "Diana", "1-0"),
            (2, "Alice", "Diana", "0-1"), (2, "Bob", "Charlie", "0-1"),
        ))
        standings = compute_standings(self.db, tournament_id)
        self.assertEqual({(row["score"], row["bh"], row["bh_c1"]) for row in standings}, {(1, 2, 1)})
        self.assertEqual(self.names(tournament_id), ["Bob", "Charlie", "Diana", "Alice"])

    def test_complete_three_way_tie_uses_their_results_against_each_other(self):
        players = (("A", 1500), ("B", 1800), ("C", 1900), ("D", 1600), ("E", 1700), ("F", 1400))
        results = {
            "AB": 1, "AC": 1, "AD": 0, "AE": 0, "AF": .5,
            "BC": 1, "BD": 1, "BE": .5, "BF": 0,
            "CD": 1, "CE": 1, "CF": .5, "DE": 1, "DF": 1, "EF": 0,
        }
        # Full round robin: each player plays exactly once in each of five rounds.
        rotation = [name for name, _seed in players]
        games = []
        for round_no in range(1, 6):
            for white, black in zip(rotation[:3], reversed(rotation[3:])):
                key = "".join(sorted((white, black)))
                white_score = results[key] if white == key[0] else 1 - results[key]
                games.append((round_no, white, black, {1: "1-0", 0: "0-1", .5: "1/2-1/2"}[white_score]))
            rotation = [rotation[0], rotation[-1], *rotation[1:-1]]
        tournament_id = self.event(players=players, games=games, rated=False)
        standings = compute_standings(self.db, tournament_id)
        tied = [row for row in standings if row["name"] in "ABC"]
        self.assertEqual({(row["score"], row["bh"], row["bh_c1"]) for row in tied}, {(2.5, 12.5, 11)})
        self.assertEqual([row["name"] for row in tied], ["A", "B", "C"])

    def test_creation_enables_new_rules_and_migration_preserves_existing_rules_and_ranks(self):
        client = self.app.test_client()
        with client.session_transaction() as session:
            session["is_admin"] = True
            session["admin_password_version"] = admin_session_version()
        response = client.post("/admin/tournaments", data={
            "name": "Future", "event_date": "2026-11-01", "rounds_planned": "3",
        })
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.db.execute("SELECT tiebreak_version FROM tournament WHERE name = 'Future'").fetchone()[0], 2)
        with sqlite3.connect(":memory:") as legacy:
            legacy.row_factory = sqlite3.Row
            schema = Path(__file__).parents[1] / "src/flaskr/schema.sql"
            legacy.executescript(schema.read_text().replace("  tiebreak_version INTEGER NOT NULL DEFAULT 1,\n", ""))
            legacy.execute("INSERT INTO tournament (name, slug, event_date, rounds_planned) VALUES ('Old', 'old', '2026-10-01', 3)")
            legacy.execute("""INSERT INTO tournament_entry
                (tournament_id, imported_name, seed_rating, member_status, final_rank, final_score)
                VALUES (1, 'Old player', 1500, 'unknown', 2, 1.5)""")
            before = tuple(legacy.execute("SELECT * FROM tournament_entry").fetchone())
            migrate_db(legacy)
            migrate_db(legacy)
            self.assertEqual(legacy.execute("SELECT tiebreak_version FROM tournament").fetchone()[0], 1)
            self.assertEqual(tuple(legacy.execute("SELECT * FROM tournament_entry").fetchone()), before)

    def test_rebuild_publishes_performance_ranks_and_preserves_legacy_positions(self):
        old_id = self.event(version=1, name="Old")
        self.assertEqual(self.names(old_id), ["Charlie", "Bob", "Alice", "Diana"])
        persist_final_standings(self.db, old_id)
        self.db.execute("UPDATE tournament_entry SET final_rank = 5 - final_rank WHERE tournament_id = ?", (old_id,))
        self.db.commit()
        old_rows = [tuple(row) for row in self.db.execute(
            "SELECT final_rank, final_score, final_primary_tiebreak, final_secondary_tiebreak FROM tournament_entry WHERE tournament_id = ? ORDER BY id",
            (old_id,),
        )]
        new_id = self.event(name="New")
        persist_final_standings(self.db, new_id)
        self.assertEqual(self.names(new_id), ["Charlie", "Alice", "Bob", "Diana"])
        original_insights = ratings._compute_tournament_insights_from_manager

        def known_performances(manager, tournament):
            insights = original_insights(manager, tournament)
            for row in insights["surprising_performance_ranking"]:
                row["performance_rating"] = {"Alice": 1800, "Bob": 2200, "Charlie": 2500, "Diana": 1000}[row["name"]]
            return insights

        snapshots = []
        with patch.object(ratings, "_compute_tournament_insights_from_manager", side_effect=known_performances):
            for _ in range(2):
                ratings.rebuild_current_manager(self.db, full_recompute=True)
                self.assertEqual(self.names(new_id), ["Charlie", "Bob", "Alice", "Diana"])
                self.assertEqual([tuple(row) for row in self.db.execute(
                    "SELECT final_rank, final_score, final_primary_tiebreak, final_secondary_tiebreak FROM tournament_entry WHERE tournament_id = ? ORDER BY id",
                    (old_id,),
                )], old_rows)
                snapshots.append({player.name: (player.id, player.get_rating().rating)
                                  for player in ratings.current_manager().player_database})
        self.assertEqual(snapshots[0], snapshots[1])
        client = self.app.test_client()
        with client.session_transaction() as session:
            session["is_admin"] = True
            session["admin_password_version"] = admin_session_version()
        response = client.get("/admin/t/new/export.csv")
        self.assertEqual(response.status_code, 200)
        export_rows = list(csv.DictReader(io.StringIO(response.get_data(as_text=True))))
        self.assertEqual([row["Name"] for row in export_rows], self.names(new_id))
        with (self.root / "exports/tournaments/new/leaderboard.csv").open() as file:
            internal_rows = list(csv.DictReader(file))
        self.assertEqual([row["Name"] for row in internal_rows], self.names(new_id))
        self.assertEqual([row["Rank"] for row in internal_rows], ["1", "2", "3", "4"])
        self.assertEqual(float(next(row for row in internal_rows if row["Name"] == "Alice")["BH"]), 4.5)


if __name__ == "__main__":
    unittest.main()
