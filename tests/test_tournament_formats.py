from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from flaskr import create_app
from flaskr.core import compute_standings, fetch_entries, fetch_pairings, fetch_tournament_by_slug, registration_counts
from flaskr.db import get_db, migrate_db
from flaskr.rating_integration import current_manager, get_player_history, get_player_profile, rebuild_current_manager, tournament_insights


class TournamentFormatsTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        rating_dir = self.root / "rating"
        rating_dir.mkdir()
        for name in ("anonymous.txt", "not_anonymous.txt"):
            (rating_dir / name).write_text("")
        self.app = create_app({
            "TESTING": True, "SECRET_KEY": "test-formats", "INSTANCE_PATH": self.root,
            "MAIL_ENABLED": True, "MAIL_HOST": "smtp.example.com", "MAIL_FROM_EMAIL": "club@example.com",
            "MAIL_SUPPRESS_SEND": True,
        })
        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session["is_admin"] = True

    def tearDown(self):
        self.tempdir.cleanup()

    def create_tournament(self, name="Team Cup", *, team=True, excluded=False, capacity="", team_size="2"):
        response = self.client.post("/admin/tournaments", data={
            "name": name, "event_date": "2026-09-18", "rounds_planned": "1",
            "is_team": "1" if team else "", "excludes_rating": "1" if excluded else "",
            "team_size": team_size,
        })
        self.assertEqual(response.status_code, 302)
        slug = response.location.rsplit("/", 1)[1]
        response = self.client.post(f"/admin/t/{slug}/registration", data={
            "registration_enabled": "1", "registration_opens_at": "2020-01-01T12:00", "max_registrations": capacity,
        }, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        return slug

    def register_team(self, slug, name="Rooks", emails="alex@example.com, sam@example.com", **extra):
        return self.client.post(f"/register/{slug}", data={
            "registration_mode": "team", "team_name": name, "member_emails": emails, "average_elo": "1500", **extra,
        }, follow_redirects=True)

    def register_solo(self, slug, name="Taylor", email="taylor@example.com", **extra):
        return self.client.post(f"/register/{slug}", data={
            "registration_mode": "solo", "name": name, "email": email, **extra,
        }, follow_redirects=True)

    def rows(self, sql, args=()):
        with self.app.app_context():
            return [dict(row) for row in get_db().execute(sql, args).fetchall()]

    def counts(self, slug):
        with self.app.app_context():
            db = get_db()
            return registration_counts(db, fetch_tournament_by_slug(db, slug)["id"])

    def finish_round(self, slug):
        with self.app.app_context():
            db = get_db()
            tournament = fetch_tournament_by_slug(db, slug)
            db.execute("UPDATE tournament_entry SET is_active = 1 WHERE tournament_id = ?", (tournament["id"],))
            db.commit()
        self.client.post(f"/admin/t/{slug}/round/1/generate")
        with self.app.app_context():
            pairings = fetch_pairings(get_db(), tournament["id"], 1)
        self.assertEqual(len(pairings), 1)
        self.assertIsNotNone(pairings[0]["black_entry_id"])
        response = self.client.post(f"/admin/t/{slug}/round/1/save", data={
            "board_count": "1", "white_1": pairings[0]["white_entry_id"],
            "black_1": pairings[0]["black_entry_id"], "result_1": "1-0",
        })
        self.assertEqual(response.status_code, 302)
        response = self.client.post(f"/admin/t/{slug}/complete", follow_redirects=True)
        self.assertEqual(response.status_code, 200)

    def test_team_registration_is_separate_from_players_and_emails_are_private(self):
        slug = self.create_tournament(team_size="3")
        response = self.register_team(slug, emails="ALEX@example.com; sam@example.com\nlee@example.com")
        self.assertIn(b"Rooks is registered", response.data)
        self.assertEqual(self.rows("SELECT * FROM player"), [])
        entries = self.rows("SELECT * FROM tournament_entry")
        self.assertEqual(len(entries), 1)
        self.assertIsNone(entries[0]["player_id"])
        self.assertEqual(len(self.rows("SELECT * FROM team_member")), 3)
        self.assertEqual(len(self.app.extensions["mail_outbox"]), 3)
        self.assertEqual({row["to"] for row in self.app.extensions["mail_outbox"]}, {"alex@example.com", "sam@example.com", "lee@example.com"})
        response = self.client.get(f"/admin/t/{slug}")
        self.assertIn(b"sam@example.com", response.data)
        self.client.post(f"/admin/t/{slug}/activate")
        with self.client.session_transaction() as session:
            session.clear()
        for path in (f"/t/{slug}", f"/t/{slug}/player/{entries[0]['id']}", f"/t/{slug}/live"):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Rooks", response.data)
            self.assertNotIn(b"@example.com", response.data)
            self.assertNotIn(b"<th>Rating</th>", response.data)
        self.assertEqual(self.client.post(f"/admin/t/{slug}/teams/assign").status_code, 302)

    def test_duplicate_emails_names_and_invalid_registrations_are_atomic(self):
        slug = self.create_tournament()
        self.register_team(slug)
        attempts = [
            ("Other", "ALEX@example.com, new@example.com"),
            ("rooks", "new@example.com, another@example.com"),
            ("Other", "new@example.com, NEW@example.com"),
            ("Other", "new@example.com, invalid"),
            ("Other", "new@example.com"),
            ("Other", "new@example.com, another@example.com, third@example.com"),
            ("", "new@example.com, another@example.com"),
        ]
        for name, emails in attempts:
            with self.subTest(name=name, emails=emails):
                response = self.register_team(slug, name, emails)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(len(self.rows("SELECT * FROM tournament_entry")), 1)
                self.assertEqual(len(self.rows("SELECT * FROM team_member")), 2)
        self.register_solo(slug, email="SAM@example.com")
        self.assertEqual(len(self.rows("SELECT * FROM team_member")), 2)

    def test_solo_assignment_preserves_answers_and_enters_team_in_pairings(self):
        slug = self.create_tournament()
        self.client.post(f"/admin/t/{slug}/registration", data={
            "registration_enabled": "1", "registration_opens_at": "2020-01-01T12:00",
            "registration_field_type": "text", "registration_field_label": "Department", "registration_field_options": "",
        })
        response = self.register_solo(slug, registration_field_0="Physics")
        self.assertIn(b"Your registration is confirmed", response.data)
        self.register_solo(slug, "Morgan", "morgan@example.com", registration_field_0="Math")
        self.assertEqual(self.rows("SELECT * FROM tournament_entry"), [])
        response = self.client.get(f"/admin/t/{slug}")
        self.assertIn(b"Physics", response.data)
        members = self.rows("SELECT * FROM team_member")
        response = self.client.post(f"/admin/t/{slug}/teams/assign", data={
            "team_name": "The Knights", "member_id": [str(row["id"]) for row in members],
        }, follow_redirects=True)
        self.assertIn(b"Team assignment saved", response.data)
        self.assertIn(b"0 solo registrations", response.data)
        entry = self.rows("SELECT * FROM tournament_entry")[0]
        self.assertIsNone(entry["player_id"])
        self.assertIn("Taylor: Physics", entry["registration_answers_json"])
        self.assertEqual({row["entry_id"] for row in self.rows("SELECT * FROM team_member")}, {entry["id"]})
        self.client.post(f"/admin/t/{slug}/teams/assign", data={"team_name": "Duplicate", "member_id": [str(row["id"]) for row in members]})
        self.assertEqual(len(self.rows("SELECT * FROM tournament_entry")), 1)
        self.register_team(slug, registration_field_0="CS")
        with patch("flaskr.web.rebuild_current_manager") as rebuild:
            self.finish_round(slug)
            rebuild.assert_not_called()
        with self.app.app_context():
            db = get_db()
            tournament = fetch_tournament_by_slug(db, slug)
            self.assertEqual(len(fetch_entries(db, tournament["id"])), 2)
            self.assertEqual([row["score"] for row in compute_standings(db, tournament["id"])], [1, 0])
            rebuild_current_manager(db)
            self.assertEqual(len(current_manager().game_database), 0)
            self.assertEqual(len(current_manager().player_database), 0)
            self.assertIsNone(tournament_insights(tournament))
        response = self.client.get(f"/admin/t/{slug}/export.csv")
        self.assertIn(b"Rank,Team,Score", response.data)
        self.assertNotIn(b"Rating", response.data)
        self.assertNotIn(b"@example.com", response.data)

    def test_team_capacity_and_waitlist_confirmation_reach_every_member(self):
        slug = self.create_tournament(capacity="1")
        self.register_team(slug)
        response = self.register_team(slug, "Bishops", "pat@example.com, robin@example.com")
        self.assertIn(b"waiting list in position 1", response.data)
        entries = self.rows("SELECT * FROM tournament_entry ORDER BY id")
        self.assertIsNone(entries[0]["waitlist_position"])
        self.assertEqual(entries[1]["waitlist_position"], 1)
        self.app.extensions["mail_outbox"].clear()
        self.client.post(f"/admin/t/{slug}/entries/{entries[1]['id']}/confirm")
        self.assertEqual({mail["to"] for mail in self.app.extensions["mail_outbox"]}, {"pat@example.com", "robin@example.com"})

    def test_solo_is_confirmed_with_pairing_email_and_counts_as_half_a_team(self):
        slug = self.create_tournament(capacity="2")
        response = self.register_solo(slug)
        self.assertIn(b"Your registration is confirmed", response.data)
        self.assertIn(b"You will be paired with another player at the tournament.", response.data)
        self.assertIn(b"0.5/2 teams confirmed", response.data)
        self.assertIn(b'value="0.5" max="2"', response.data)
        self.assertEqual(self.counts(slug), {"confirmed_count": 0.5, "waitlist_count": 0})
        self.assertEqual(self.rows("SELECT * FROM tournament_entry"), [])
        self.assertIsNone(self.rows("SELECT entry_id FROM team_member")[0]["entry_id"])
        messages = self.app.extensions["mail_outbox"]
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["to"], "taylor@example.com")
        self.assertEqual(messages[0]["subject"], "Registration Confirmed for Team Cup")
        for text in (
            "Hello Taylor,", "Your registration is confirmed.",
            "You will be paired with another player at the tournament.",
            "Tournament: Team Cup", "Date: 2026-09-18", "Membership is free this year",
        ):
            self.assertIn(text, messages[0]["body"])
        self.assertNotIn("awaiting", messages[0]["body"])
        self.assertIn(b"0.5 teams confirmed", self.client.get(f"/admin/t/{slug}").data)

    def test_assigning_confirmed_solos_at_capacity_preserves_count_and_status(self):
        slug = self.create_tournament(capacity="2")
        self.register_team(slug)
        self.register_solo(slug)
        self.assertEqual(self.counts(slug)["confirmed_count"], 1.5)
        self.register_solo(slug, "Morgan", "morgan@example.com")
        self.assertEqual(self.counts(slug)["confirmed_count"], 2)
        members = self.rows("SELECT id FROM team_member WHERE entry_id IS NULL")
        response = self.client.post(f"/admin/t/{slug}/teams/assign", data={
            "team_name": "New Pair", "member_id": [str(row["id"]) for row in members],
        }, follow_redirects=True)
        self.assertIn(b"Team assignment saved", response.data)
        self.assertEqual(self.counts(slug), {"confirmed_count": 2, "waitlist_count": 0})
        self.assertIsNone(self.rows("SELECT waitlist_position FROM tournament_entry WHERE imported_name = 'New Pair'")[0]["waitlist_position"])
        response = self.register_team(slug, "Bishops", "pat@example.com, robin@example.com")
        self.assertIn(b"waiting list in position 1", response.data)
        self.assertEqual(self.counts(slug), {"confirmed_count": 2, "waitlist_count": 1})

    def test_a_new_full_team_cannot_take_a_confirmed_solos_half_place(self):
        slug = self.create_tournament(capacity="1")
        self.register_solo(slug)
        response = self.register_team(slug)
        self.assertIn(b"waiting list in position 1", response.data)
        self.assertEqual(self.counts(slug), {"confirmed_count": 0.5, "waitlist_count": 1})
        self.register_solo(slug, "Morgan", "morgan@example.com")
        members = self.rows("SELECT id FROM team_member WHERE entry_id IS NULL")
        self.client.post(f"/admin/t/{slug}/teams/assign", data={
            "team_name": "Confirmed Pair", "member_id": [str(row["id"]) for row in members],
        })
        self.assertEqual(self.counts(slug), {"confirmed_count": 1, "waitlist_count": 1})
        self.assertIsNone(self.rows("SELECT waitlist_position FROM tournament_entry WHERE imported_name = 'Confirmed Pair'")[0]["waitlist_position"])

    def test_solo_counts_respect_configured_team_size_and_tournament_scope(self):
        slug = self.create_tournament(team_size="3")
        other = self.create_tournament("Other Cup")
        self.register_solo(other)
        for index in range(1, 4):
            response = self.register_solo(slug, f"Player {index}", f"player{index}@example.com")
            self.assertAlmostEqual(self.counts(slug)["confirmed_count"], index / 3)
            self.assertEqual(self.counts(other)["confirmed_count"], 0.5)
            if index == 1:
                self.assertIn(b"0.33 teams confirmed", response.data)
        self.assertIn(b"1 teams confirmed", response.data)
        self.assertIn("You will be paired with other players at the tournament.", self.app.extensions["mail_outbox"][-1]["body"])
        self.client.post(f"/admin/t/{slug}/reset-registrations")
        self.assertEqual(self.counts(slug), {"confirmed_count": 0, "waitlist_count": 0})
        self.assertEqual(self.counts(other)["confirmed_count"], 0.5)

    def test_solos_stay_confirmed_when_team_capacity_is_full(self):
        slug = self.create_tournament(capacity="1")
        self.register_team(slug)
        response = self.register_solo(slug)
        self.assertIn(b"Your registration is confirmed", response.data)
        self.assertEqual(self.counts(slug), {"confirmed_count": 1.5, "waitlist_count": 0})
        self.assertEqual(self.app.extensions["mail_outbox"][-1]["subject"], "Registration Confirmed for Team Cup")

    def test_assignment_rejects_other_tournaments_and_playing_teams(self):
        slug = self.create_tournament()
        self.register_solo(slug)
        member = self.rows("SELECT * FROM team_member")[0]
        self.register_team(slug)
        entry = self.rows("SELECT * FROM tournament_entry")[0]
        other = self.create_tournament("Other Cup")
        response = self.client.post(f"/admin/t/{other}/teams/assign", data={"member_id": str(member["id"]), "existing_entry_id": str(entry["id"])}, follow_redirects=True)
        self.assertIn(b"Select unassigned members", response.data)
        self.client.post(f"/admin/t/{slug}/entries/{entry['id']}/toggle")
        self.client.post(f"/admin/t/{slug}/round/1/generate")
        response = self.client.post(f"/admin/t/{slug}/teams/assign", data={"member_id": str(member["id"]), "existing_entry_id": str(entry["id"])}, follow_redirects=True)
        self.assertIn(b"already has pairings", response.data)
        self.assertIsNone(self.rows("SELECT * FROM team_member WHERE id = ?", (member["id"],))[0]["entry_id"])

    def test_existing_team_assignment_rename_and_format_lock(self):
        slug = self.create_tournament()
        self.register_team(slug)
        self.register_solo(slug)
        member = self.rows("SELECT * FROM team_member WHERE entry_id IS NULL")[0]
        entry = self.rows("SELECT * FROM tournament_entry")[0]
        response = self.client.post(f"/admin/t/{slug}/teams/assign", data={"member_id": str(member["id"]), "existing_entry_id": str(entry["id"])}, follow_redirects=True)
        self.assertIn(b"exactly 2 members", response.data)
        self.assertEqual(len(self.rows("SELECT * FROM team_member WHERE entry_id = ?", (entry["id"],))), 2)
        self.assertIsNone(self.rows("SELECT entry_id FROM team_member WHERE id = ?", (member["id"],))[0]["entry_id"])
        self.client.post(f"/admin/t/{slug}/teams/{entry['id']}/name", data={"team_name": "New Name"})
        self.assertEqual(self.rows("SELECT imported_name FROM tournament_entry")[0]["imported_name"], "New Name")
        response = self.client.post(f"/admin/t/{slug}/settings", data={}, follow_redirects=True)
        self.assertIn(b"format can only be changed", response.data)
        self.assertEqual(self.rows("SELECT is_team, excludes_rating FROM tournament")[0], {"is_team": 1, "excludes_rating": 1})

    def test_team_size_defaults_to_two_and_can_be_configured(self):
        self.client.post("/admin/tournaments", data={
            "name": "Default Cup", "event_date": "2026-09-18", "is_team": "1",
        })
        self.assertEqual(self.rows("SELECT team_size FROM tournament")[0]["team_size"], 2)
        slug = self.create_tournament(team_size="3")
        response = self.client.get("/register")
        self.assertIn(b"Member email addresses (3 members)", response.data)
        self.assertIn(b"Average Elo", response.data)
        self.assertIn(b"Approximate Elo (optional)", response.data)
        response = self.register_team(slug)
        self.assertIn(b"exactly 3 member email addresses", response.data)
        self.assertEqual(self.rows("SELECT * FROM tournament_entry"), [])
        response = self.register_team(slug, emails="one@example.com,two@example.com,three@example.com")
        self.assertIn(b"Rooks is registered", response.data)
        self.assertEqual(len(self.rows("SELECT * FROM team_member")), 3)

    def test_invalid_team_sizes_reject_creation_and_updates(self):
        for size in ("", "1", "-2", "2.5", "abc", "101"):
            with self.subTest(size=size):
                response = self.client.post("/admin/tournaments", data={
                    "name": "Invalid", "event_date": "2026-09-18", "is_team": "1", "team_size": size,
                }, follow_redirects=True)
                self.assertIn(b"whole number of team members", response.data)
                self.assertEqual(self.rows("SELECT * FROM tournament"), [])
        slug = self.create_tournament()
        for size in ("", "1", "2.5", "abc", "101"):
            response = self.client.post(f"/admin/t/{slug}/settings", data={"is_team": "1", "team_size": size}, follow_redirects=True)
            self.assertIn(b"whole number of team members", response.data)
            self.assertEqual(self.rows("SELECT team_size FROM tournament")[0]["team_size"], 2)

    def test_size_changes_allow_waiting_solos_but_preserve_existing_teams(self):
        slug = self.create_tournament()
        self.register_solo(slug)
        response = self.client.post(f"/admin/t/{slug}/settings", data={"is_team": "1", "team_size": "3"}, follow_redirects=True)
        self.assertIn(b"Tournament settings updated", response.data)
        self.assertEqual(self.rows("SELECT team_size FROM tournament")[0]["team_size"], 3)
        self.register_team(slug, emails="one@example.com,two@example.com,three@example.com")
        response = self.client.post(f"/admin/t/{slug}/settings", data={"is_team": "1", "team_size": "2"}, follow_redirects=True)
        self.assertIn(b"Team size cannot change", response.data)
        self.assertEqual(self.rows("SELECT team_size FROM tournament")[0]["team_size"], 3)

    def test_team_average_elo_is_required_validated_and_used_for_seeding(self):
        slug = self.create_tournament()
        for value in ("", "abc", "1450.5", "-1", "4001"):
            with self.subTest(elo=value):
                self.register_team(slug, average_elo=value)
                self.assertEqual(self.rows("SELECT * FROM tournament_entry"), [])
                self.assertEqual(self.rows("SELECT * FROM team_member"), [])
        response = self.client.post(f"/register/{slug}", data={
            "team_name": "Rooks", "member_emails": "alex@example.com,sam@example.com",
        }, follow_redirects=True)
        self.assertIn(b"Average Elo is required", response.data)
        self.register_team(slug, average_elo="1875")
        entry = self.rows("SELECT * FROM tournament_entry")[0]
        self.assertEqual((entry["declared_rating"], entry["seed_rating"]), (1875, 1875))
        response = self.client.get(f"/admin/t/{slug}")
        self.assertIn(b"<th>Average Elo</th>", response.data)
        self.assertIn(b"<td>1875</td>", response.data)
        self.assertEqual(self.rows("SELECT * FROM player"), [])

    def test_solo_elo_is_optional_and_invalid_values_are_rejected(self):
        slug = self.create_tournament()
        for value in ("abc", "1450.5", "-1", "4001"):
            self.register_solo(slug, approximate_elo=value)
            self.assertEqual(self.rows("SELECT * FROM team_member"), [])
        self.register_solo(slug)
        self.register_solo(slug, "Morgan", "morgan@example.com", approximate_elo="1800")
        members = self.rows("SELECT * FROM team_member ORDER BY id")
        self.assertEqual([row["declared_rating"] for row in members], [None, 1800])
        response = self.client.get(f"/admin/t/{slug}")
        self.assertIn(b"<th>Approximate Elo</th>", response.data)
        self.assertIn(b"<td>1800</td>", response.data)
        self.client.post(f"/admin/t/{slug}/teams/assign", data={
            "team_name": "Mixed", "member_id": [str(row["id"]) for row in members],
        })
        entry = self.rows("SELECT * FROM tournament_entry")[0]
        self.assertIsNone(entry["declared_rating"])
        self.assertEqual(entry["seed_rating"], 1500)

    def test_admin_assignment_requires_configured_size_and_averages_elo(self):
        slug = self.create_tournament(team_size="3")
        for index, rating in enumerate((1200, 1500, 1800, 2100)):
            self.register_solo(slug, f"Player {index}", f"player{index}@example.com", approximate_elo=str(rating))
        members = self.rows("SELECT * FROM team_member ORDER BY id")
        for size in (2, 4):
            response = self.client.post(f"/admin/t/{slug}/teams/assign", data={
                "team_name": "New team", "member_id": [str(row["id"]) for row in members[:size]],
            }, follow_redirects=True)
            self.assertIn(b"Select exactly 3 members", response.data)
            self.assertEqual(self.rows("SELECT * FROM tournament_entry"), [])
            self.assertTrue(all(row["entry_id"] is None for row in self.rows("SELECT * FROM team_member")))
        response = self.client.post(f"/admin/t/{slug}/teams/assign", data={
            "team_name": "New team", "member_id": [str(row["id"]) for row in members[:3]],
        }, follow_redirects=True)
        self.assertIn(b"Team assignment saved", response.data)
        entry = self.rows("SELECT * FROM tournament_entry")[0]
        self.assertEqual((entry["declared_rating"], entry["seed_rating"]), (1500, 1500))
        self.assertEqual(len(self.rows("SELECT * FROM team_member WHERE entry_id IS NULL")), 1)

    def test_incomplete_existing_team_can_be_filled_with_weighted_average(self):
        slug = self.create_tournament(team_size="3")
        self.register_team(slug, emails="one@example.com,two@example.com,three@example.com", average_elo="1800")
        # Simulate a legacy team with fewer members than the configured team size.
        with self.app.app_context():
            db = get_db()
            db.execute("DELETE FROM team_member WHERE email = 'three@example.com'")
            db.commit()
        self.register_solo(slug, approximate_elo="1200")
        member = self.rows("SELECT * FROM team_member WHERE entry_id IS NULL")[0]
        entry = self.rows("SELECT * FROM tournament_entry")[0]
        response = self.client.post(f"/admin/t/{slug}/teams/assign", data={
            "member_id": str(member["id"]), "existing_entry_id": str(entry["id"]),
        }, follow_redirects=True)
        self.assertIn(b"Team assignment saved", response.data)
        entry = self.rows("SELECT * FROM tournament_entry")[0]
        self.assertEqual((entry["declared_rating"], entry["seed_rating"]), (1600, 1600))
        self.assertEqual(len(self.rows("SELECT * FROM team_member WHERE entry_id = ?", (entry["id"],))), 3)

    def test_admin_registration_enforces_size_and_preserves_zero_elo(self):
        slug = self.create_tournament()
        data = {"registration_mode": "team", "team_name": "Zero", "member_emails": "one@example.com", "average_elo": "0"}
        response = self.client.post(f"/admin/t/{slug}/entries", data=data, follow_redirects=True)
        self.assertIn(b"exactly 2 member email addresses", response.data)
        self.assertEqual(self.rows("SELECT * FROM tournament_entry"), [])
        data["member_emails"] = "one@example.com,two@example.com"
        response = self.client.post(f"/admin/t/{slug}/entries", data=data, follow_redirects=True)
        self.assertIn(b"Zero is registered", response.data)
        entry = self.rows("SELECT * FROM tournament_entry")[0]
        self.assertEqual((entry["declared_rating"], entry["seed_rating"]), (0, 0))
        for name in ("One", "Two"):
            self.client.post(f"/admin/t/{slug}/entries", data={
                "registration_mode": "solo", "name": name, "email": f"{name.lower()}solo@example.com", "approximate_elo": "0",
            })
        members = self.rows("SELECT * FROM team_member WHERE entry_id IS NULL")
        self.assertEqual([row["declared_rating"] for row in members], [0, 0])
        self.client.post(f"/admin/t/{slug}/teams/assign", data={
            "team_name": "More Zero", "member_id": [str(row["id"]) for row in members],
        })
        self.assertEqual(self.rows("SELECT seed_rating FROM tournament_entry WHERE imported_name = 'More Zero'")[0]["seed_rating"], 0)

    def test_solo_registration_locks_format_and_reset_clears_all_team_data(self):
        slug = self.create_tournament()
        self.register_solo(slug)
        response = self.client.post(f"/admin/t/{slug}/settings", follow_redirects=True)
        self.assertIn(b"format can only be changed", response.data)
        self.register_team(slug)
        self.client.post(f"/admin/t/{slug}/reset-registrations")
        self.assertEqual(self.rows("SELECT * FROM team_member"), [])
        self.assertEqual(self.rows("SELECT * FROM tournament_entry"), [])
        self.assertEqual(self.rows("SELECT is_team FROM tournament")[0]["is_team"], 1)
        self.client.post(f"/admin/t/{slug}/settings", data={"excludes_rating": "1"})
        self.assertEqual(self.rows("SELECT is_team FROM tournament")[0]["is_team"], 0)

    def test_unrated_completed_tournament_can_be_included_and_excluded_again(self):
        slug = self.create_tournament("Unrated Cup", team=False, excluded=True)
        for name in ("Alice Example", "Bob Sample"):
            self.client.post(f"/admin/t/{slug}/entries", data={"name": name, "declared_rating": "1500"})
        with patch("flaskr.web.rebuild_current_manager") as rebuild:
            self.finish_round(slug)
            rebuild.assert_not_called()
        with self.app.app_context():
            rebuild_current_manager(get_db())
            self.assertEqual(len(current_manager().game_database), 0)
        self.client.post(f"/admin/t/{slug}/settings", data={})
        export_path = self.root / "exports" / "tournaments" / slug / "leaderboard.csv"
        self.assertTrue(export_path.exists())
        with self.app.app_context():
            self.assertEqual(len(current_manager().game_database), 1)
            self.assertEqual(len(get_player_history("Alice Example")), 1)
        self.client.post(f"/admin/t/{slug}/settings", data={"excludes_rating": "1"})
        self.assertFalse(export_path.exists())
        with self.app.app_context():
            self.assertEqual(len(current_manager().game_database), 0)
            self.assertEqual(get_player_history("Alice Example"), [])
            self.assertEqual(get_player_profile("Alice Example")["games"], 0)
            self.assertIsNone(get_player_profile("Alice Example")["rating"])
            self.assertIsNone(tournament_insights(fetch_tournament_by_slug(get_db(), slug)))
        response = self.client.get(f"/admin/t/{slug}")
        self.assertNotIn(b"Secondary prizes", response.data)
        response = self.client.post(f"/register/{slug}", data={"name": "Late", "email": "late@example.com"}, follow_redirects=True)
        self.assertIn(b"Registration is not open", response.data)

    def test_unrated_history_fallback_and_stored_insights_are_excluded(self):
        slug = self.create_tournament("Unrated Cup", team=False, excluded=True)
        for name in ("Alice Example", "Bob Sample"):
            self.client.post(f"/admin/t/{slug}/entries", data={"name": name})
        self.finish_round(slug)
        with self.app.app_context():
            db = get_db()
            db.execute("UPDATE tournament SET public_insights_json = ?", (json.dumps({"surprising_performance_ranking": [{"name": "Alice"}], "surprising_game_ranking": []}),))
            db.commit()
            self.assertEqual(get_player_history("Alice Example"), [])
            self.assertIsNone(tournament_insights(fetch_tournament_by_slug(db, slug)))


class TournamentMigrationTestCase(unittest.TestCase):
    def test_legacy_entries_pairings_and_availability_survive_migration(self):
        schema = (Path(__file__).parents[1] / "src/flaskr/schema.sql").read_text()
        legacy_schema = schema.replace("player_id INTEGER REFERENCES player", "player_id INTEGER NOT NULL REFERENCES player")
        legacy_schema = legacy_schema.replace("  excludes_rating INTEGER NOT NULL DEFAULT 0,\n", "").replace("  is_team INTEGER NOT NULL DEFAULT 0,\n", "")
        legacy_schema = legacy_schema.replace("  team_size INTEGER NOT NULL DEFAULT 2,\n", "")
        legacy_schema = legacy_schema.replace("  email TEXT NOT NULL COLLATE NOCASE,\n  declared_rating INTEGER,", "  email TEXT NOT NULL COLLATE NOCASE,")
        db = sqlite3.connect(":memory:")
        self.addCleanup(db.close)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        db.executescript(legacy_schema)
        db.execute("INSERT INTO tournament (id, name, slug, event_date, rounds_planned) VALUES (1, 'Old Cup', 'old', '2026-01-01', 1)")
        db.execute("INSERT INTO player (id, name, normalized_name) VALUES (1, 'Alice', 'alice')")
        db.execute("INSERT INTO tournament_entry (id, tournament_id, player_id, imported_name, seed_rating, member_status) VALUES (5, 1, 1, 'Alice', 1600, 'member')")
        db.execute("INSERT INTO player (id, name, normalized_name) VALUES (2, 'Deleted', 'deleted')")
        db.execute("INSERT INTO tournament_entry (id, tournament_id, player_id, imported_name, seed_rating, member_status) VALUES (100, 1, 2, 'Deleted', 1500, 'unknown')")
        db.execute("DELETE FROM tournament_entry WHERE id = 100")
        db.execute("INSERT INTO pairing (tournament_id, round_no, board_no, white_entry_id, result_code) VALUES (1, 1, 1, 5, 'BYE')")
        db.execute("INSERT INTO entry_round_status (entry_id, round_no) VALUES (5, 1)")
        db.execute("INSERT INTO team_member (tournament_id, name, email) VALUES (1, 'Solo', 'solo@example.com')")
        db.commit()
        migrate_db(db)
        migrate_db(db)
        self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])
        self.assertEqual(db.execute("PRAGMA foreign_keys").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT white_entry_id FROM pairing").fetchone()[0], 5)
        self.assertEqual(db.execute("SELECT entry_id FROM entry_round_status").fetchone()[0], 5)
        self.assertEqual(db.execute("SELECT imported_name FROM tournament_entry").fetchone()[0], "Alice")
        self.assertEqual(tuple(db.execute("SELECT excludes_rating, is_team FROM tournament").fetchone()), (0, 0))
        self.assertEqual(db.execute("SELECT team_size FROM tournament").fetchone()[0], 2)
        self.assertEqual(tuple(db.execute("SELECT name, declared_rating FROM team_member").fetchone()), ("Solo", None))
        cursor = db.execute("INSERT INTO tournament_entry (tournament_id, imported_name, seed_rating, member_status) VALUES (1, 'Team', 1500, 'unknown')")
        self.assertEqual(cursor.lastrowid, 101)
        db.execute("DELETE FROM tournament_entry WHERE id = 5")
        self.assertIsNone(db.execute("SELECT white_entry_id FROM pairing").fetchone()[0])
        self.assertEqual(db.execute("SELECT * FROM entry_round_status").fetchall(), [])


if __name__ == "__main__":
    unittest.main()
