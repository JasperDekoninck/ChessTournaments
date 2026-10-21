from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from flaskr import create_app, rating_integration as ratings, rating_jobs as jobs
from flaskr.auth import admin_session_version
from flaskr.db import get_db
from rating import Game, Manager, Player, PlayerDatabase, Tournament
from rating.rating.polyrating_base import Polyrating


class RatingJobsTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.rating_dir = self.root / "rating"
        self.rating_dir.mkdir()
        for name in ("anonymous.txt", "not_anonymous.txt"):
            (self.rating_dir / name).write_text("")
        self.manager = Manager(player_database=PlayerDatabase(strict=False))
        self.alice = Player("Alice Example")
        self.bob = Player("Bob Sample")
        history = Tournament("History", datetime(2026, 1, 1), rounds=1)
        self.manager.add_tournament(history, players=[self.alice, self.bob], games=[
            Game(self.alice.id, self.bob.id, "1-0", history.get_date(), tournament_id=history.id),
        ])
        self.manager.update_rating()
        for kind in ("baseline", "current"):
            self.manager.save(str(self.rating_dir / f"{kind}.json"))
        self.app = create_app({
            "TESTING": True, "SECRET_KEY": "rating-jobs", "INSTANCE_PATH": self.root,
            "MAIL_ENABLED": False,
        })
        self.client = self.app.test_client()
        with self.app.app_context(), self.client.session_transaction() as session:
            session["is_admin"] = True
            session["admin_password_version"] = admin_session_version()
        self.history_id = self.event("History", "2026-01-01", historical=True)
        self.event_id = self.event("Cup", "2026-02-01", status="running")

    def tearDown(self):
        ratings._clear_runtime_caches()
        self.tempdir.cleanup()

    def event(self, name, date, *, status="completed", historical=False, excluded=False, players=("Alice Example", "Bob Sample")):
        with self.app.app_context():
            db = get_db()
            tournament_id = db.execute(
                """
                INSERT INTO tournament (name, slug, event_date, rounds_planned, status, is_historical, excludes_rating, is_public)
                VALUES (?, ?, ?, 1, ?, ?, ?, 1)
                """,
                (name, name.lower(), date, status, int(historical), int(excluded)),
            ).lastrowid
            entries = []
            for name in players:
                player = db.execute("SELECT id FROM player WHERE name = ?", (name,)).fetchone()
                player_id = player[0] if player else db.execute(
                    "INSERT INTO player (name, normalized_name, canonical_rating_name) VALUES (?, ?, ?)",
                    (name, name.lower(), name),
                ).lastrowid
                entries.append(db.execute(
                    """INSERT INTO tournament_entry (tournament_id, player_id, imported_name, seed_rating, member_status)
                    VALUES (?, ?, ?, 1500, 'unknown')""",
                    (tournament_id, player_id, name),
                ).lastrowid)
            db.execute(
                "INSERT INTO pairing (tournament_id, round_no, board_no, white_entry_id, black_entry_id, result_code) VALUES (?, 1, 1, ?, ?, '1-0')",
                (tournament_id, *entries),
            )
            db.commit()
            return tournament_id

    def finish(self):
        return self.client.post("/admin/t/cup/complete", follow_redirects=True)

    def queue(self, *, full=False):
        with self.app.app_context(), patch.object(jobs, "_start_worker") as start:
            jobs.request_rating_update(full_recompute=full)
            return start.call_count

    def run_worker(self):
        with self.app.app_context():
            jobs.run_pending_rating_updates()
            return jobs.rating_job_status()

    def snapshot(self):
        with self.app.app_context():
            manager = ratings.current_manager()
            return {
                "ratings": {p.name: p.get_rating().rating for p in manager.player_database},
                "histories": {p.name: [(r.rating, date.isoformat()) for r, date in p.get_rating_history()] for p in manager.player_database},
                "games": len(manager.game_database),
                "events": len(manager.tournament_database),
                "dates": [date.isoformat() for date in manager.rating_period],
            }

    def test_finishing_returns_without_calculating_and_does_not_queue_twice(self):
        original = (self.rating_dir / "current.json").read_bytes()
        with patch.object(jobs, "_start_worker") as start, patch.object(ratings, "rebuild_current_manager") as rebuild:
            response = self.finish()
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"updating in the background", response.data)
            self.assertEqual(self.finish().status_code, 200)
            start.assert_called_once()
            rebuild.assert_not_called()
        self.assertEqual((self.rating_dir / "current.json").read_bytes(), original)
        with self.app.app_context():
            self.assertEqual(get_db().execute("SELECT status FROM tournament WHERE id = ?", (self.event_id,)).fetchone()[0], "completed")
            self.assertEqual(jobs.rating_job_status()["requested_version"], 1)

    def test_multiple_requests_share_one_worker_and_consume_all_pending_changes(self):
        self.assertEqual(self.queue(), 1)
        self.assertEqual(self.queue(), 0)
        calls = []

        def rebuild(db, *, progress, full_recompute):
            calls.append(full_recompute)
            if len(calls) == 1:
                with patch.object(jobs, "_start_worker") as start:
                    jobs.request_rating_update(full_recompute=True)
                    start.assert_not_called()
            progress("Testing progress")

        with patch.object(ratings, "rebuild_current_manager", side_effect=rebuild):
            result = self.run_worker()
        self.assertEqual(calls, [False, True])
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["requested_version"], result["completed_version"])

    def test_repeated_full_rebuild_clicks_do_not_schedule_duplicate_replays(self):
        self.assertEqual(self.queue(full=True), 1)
        self.assertEqual(self.queue(full=True), 0)
        with self.app.app_context():
            self.assertEqual(jobs.rating_job_status()["requested_version"], 1)
        with patch.object(ratings, "rebuild_current_manager") as rebuild:
            self.assertEqual(self.run_worker()["status"], "completed")
            rebuild.assert_called_once()

    def test_rebuild_replays_history_once_in_order_and_repeated_rebuilds_are_identical(self):
        self.finish_with_queued_worker()
        self.event("Later", "2026-03-01")
        self.event("Unrated", "2026-04-01", excluded=True)
        # A separate reconstruction from the baseline's games is the reference.
        reference = self.manager.clone()
        for name, date in (("Cup", "2026-02-01"), ("Later", "2026-03-01")):
            tournament = Tournament(name, datetime.fromisoformat(date), rounds=1)
            reference.add_tournament(tournament, games=[Game(
                self.alice.id, self.bob.id, "1-0", tournament.get_date(), tournament_id=tournament.id,
                add_home_advantage=reference.add_home_advantage,
            )])
        reference.reset_and_recompute()
        # Simulate a bad current snapshot; recovery must use the saved inputs.
        corrupt = self.manager.clone()
        for player in corrupt.player_database:
            player.get_rating().reset()
        corrupt.save(str(self.rating_dir / "current.json"))
        self.queue(full=True)
        self.assertEqual(self.run_worker()["status"], "completed")
        first = self.snapshot()
        self.assertEqual(first["games"], 3)
        self.assertEqual(first["events"], 3)
        self.assertEqual(first["dates"], sorted(first["dates"]))
        for player in reference.player_database:
            self.assertAlmostEqual(first["ratings"][player.name], player.get_rating().rating, places=7)
        self.assertGreater(first["ratings"]["Alice Example"], 1500)
        self.assertLess(first["ratings"]["Bob Sample"], 1500)
        self.queue(full=True)
        with patch.object(ratings, "_compute_tournament_insights_from_manager", wraps=ratings._compute_tournament_insights_from_manager) as insights:
            self.assertEqual(self.run_worker()["status"], "completed")
        self.assertEqual(self.snapshot(), first)
        self.assertEqual(insights.call_count, 2, "unchanged historical statistics should be reused")

    def finish_with_queued_worker(self):
        with patch.object(jobs, "_start_worker"):
            self.assertEqual(self.finish().status_code, 200)

    def test_public_and_admin_views_never_run_the_performance_solver(self):
        self.finish_with_queued_worker()
        with self.app.app_context():
            manager = ratings.current_manager().clone()
            for event in manager.tournament_database:
                event.results = {}
            manager.save(str(self.rating_dir / "current.json"))
            ratings._clear_runtime_caches()
        with patch.object(Tournament, "compute_tournament_results", side_effect=AssertionError("page view ran calculations")):
            for url in ("/admin/t/history", "/t/history?view=standings", "/admin/t/cup", "/t/cup?view=standings", "/ratings"):
                with self.subTest(url=url):
                    self.assertEqual(self.client.get(url).status_code, 200)

    def test_rebuilds_with_new_players_are_identical_and_do_not_duplicate_games(self):
        self.finish_with_queued_worker()
        self.event("Newcomer", "2026-03-01", players=("Bob Sample", "Cara New"))
        self.queue(full=True)
        self.assertEqual(self.run_worker()["status"], "completed")
        first = self.snapshot()
        self.assertEqual(first["games"], 3)
        self.assertNotEqual(first["ratings"]["Cara New"], 1500)
        self.queue(full=True)
        self.assertEqual(self.run_worker()["status"], "completed")
        self.assertEqual(self.snapshot(), first)

    def test_history_missing_from_baseline_is_reconstructed_once_from_saved_results(self):
        Manager(player_database=PlayerDatabase(strict=False)).save(str(self.rating_dir / "baseline.json"))
        self.finish_with_queued_worker()
        self.queue(full=True)
        self.assertEqual(self.run_worker()["status"], "completed")
        snapshot = self.snapshot()
        self.assertEqual(snapshot["games"], 2)
        self.assertEqual(snapshot["events"], 2)
        self.assertGreater(snapshot["ratings"]["Alice Example"], 1500)

    def test_failure_keeps_old_ratings_and_can_be_retried(self):
        original = (self.rating_dir / "current.json").read_bytes()
        self.queue(full=True)
        with patch.object(ratings, "_compute_tournament_insights_from_manager", side_effect=RuntimeError("test error")), self.assertLogs(self.app.logger, level="ERROR"):
            result = self.run_worker()
        self.assertEqual(result["status"], "failed")
        self.assertEqual((self.rating_dir / "current.json").read_bytes(), original)
        self.assertIn(b"Retry rating rebuild", self.client.get("/admin?tab=rating").data)
        self.assertEqual(self.queue(full=True), 1)
        self.assertEqual(self.run_worker()["status"], "completed")

    def test_interrupted_worker_is_reported_and_a_queued_process_has_time_to_start(self):
        self.queue()
        with self.app.app_context():
            self.assertEqual(jobs.rating_job_status()["status"], "queued")
            db = get_db()
            db.execute("UPDATE rating_job SET status = 'running'")
            db.commit()
            self.assertEqual(jobs.rating_job_status()["status"], "failed")
        self.assertEqual(self.queue(), 1)

    def test_rebuild_lock_prevents_overlapping_calculations(self):
        with self.app.app_context(), jobs.rating_update_lock(), patch.object(Manager, "update_rating") as update:
            with self.assertRaises(jobs.RatingUpdateBusy):
                ratings.rebuild_current_manager(get_db())
            update.assert_not_called()

    def test_failed_atomic_save_keeps_the_previous_file(self):
        path = self.rating_dir / "current.json"
        original = path.read_bytes()

        def interrupted(path):
            Path(path).write_text('{"incomplete":')
            raise OSError("disk write interrupted")

        with patch.object(self.manager, "save", side_effect=interrupted):
            with self.assertRaises(OSError):
                ratings._save_manager_atomically(self.manager, path)
        self.assertEqual(path.read_bytes(), original)

    def test_atomic_publication_preserves_the_existing_file_permissions(self):
        path = self.rating_dir / "current.json"
        path.chmod(0o640)
        before = path.stat()
        ratings._save_manager_atomically(self.manager, path)
        after = path.stat()
        self.assertEqual(after.st_mode & 0o777, 0o640)
        self.assertEqual((after.st_uid, after.st_gid), (before.st_uid, before.st_gid))

    def test_performance_failure_restores_the_players_rating_and_history(self):
        before = self.alice.generate_settings()
        with patch.object(Polyrating, "compute_period_update", side_effect=RuntimeError("failed estimate")):
            with self.assertRaises(RuntimeError):
                self.manager.rating_system.compute_tournament_performance(
                    self.alice, next(iter(self.manager.tournament_database)).id, datetime(2026, 1, 1),
                    self.manager.game_database, self.manager.player_database,
                )
        self.assertEqual(self.alice.generate_settings(), before)

    def test_backdated_tournament_requires_recompute_even_after_a_newer_tournament_is_added(self):
        self.manager.add_tournament(Tournament("Backdated", datetime(2025, 12, 1), rounds=1))
        self.assertTrue(self.manager.recompute)
        self.manager.add_tournament(Tournament("New", datetime(2026, 3, 1), rounds=1))
        self.assertTrue(self.manager.recompute)

    def test_rating_tab_and_rebuild_endpoint_require_admin_and_csrf(self):
        page = self.client.get("/admin?tab=rating")
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"Rebuild all ratings", page.data)
        self.assertIn(b"Awaiting completion", page.data)
        with patch.object(jobs, "_start_worker"):
            self.assertEqual(self.client.post("/admin/ratings/rebuild").status_code, 302)
        with self.app.app_context():
            self.assertEqual(jobs.rating_job_status()["full_recompute_version"], 1)
        self.app.config["TESTING"] = False
        self.assertEqual(self.client.post("/admin/ratings/rebuild").status_code, 400)
        self.app.config["TESTING"] = True
        with self.client.session_transaction() as session:
            session.clear()
        self.assertEqual(self.client.get("/admin/ratings/status").status_code, 302)
        self.assertEqual(self.client.post("/admin/ratings/rebuild").status_code, 302)

    def test_real_subprocess_uses_the_same_database_and_publishes_results(self):
        self.finish_with_queued_worker()
        with self.app.app_context():
            process = jobs._start_worker()
            self.assertNotEqual(process.pid, os.getpid())
        try:
            self.assertEqual(self.client.get("/ratings").status_code, 200)
            self.assertEqual(process.wait(timeout=30), 0)
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
        with self.app.app_context():
            status = jobs.rating_job_status()
        self.assertEqual(status["status"], "completed", (self.root / "rating-worker.log").read_text())
        self.assertEqual(self.snapshot()["games"], 2)


if __name__ == "__main__":
    unittest.main()
