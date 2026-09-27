"""Regressions for FIDE C.04.3 / C.04.6 (effective 1 February 2026).

Exercise the application's adapter with complete histories, rather than testing
private matching helpers. Pair numbers below are ordered by starting rating.
"""
import json
import tempfile
import unittest
from pathlib import Path

from flaskr import create_app
from flaskr.core import compute_standings, fetch_pairings, pairings_complete
from flaskr.db import get_db
from flaskr.swiss_pairing import pair_swiss


def entrants(count):
    return [dict(id=i, seed_rating=2500 - i * 10, imported_name=f"Entrant {i}") for i in range(1, count + 1)]


def history(*rounds):
    return [dict(round_no=rnd, board_no=board, white_entry_id=w, black_entry_id=b, result_code=result)
            for rnd, games in enumerate(rounds, 1) for board, (w, b, result) in enumerate(games, 1)]


def pairs(boards):
    return [(b["white_entry_id"], b["black_entry_id"]) for b in boards]


class PairingRulesTestCase(unittest.TestCase):
    def test_independent_dutch_reference_with_floats_forfeits_and_board_order(self):
        fixture = json.loads((Path(__file__).parent / "fixtures/dutch_reference.json").read_text())
        past = [dict(zip(("round_no", "board_no", "white_entry_id", "black_entry_id", "result_code"), row))
                for row in fixture["history"]]
        result = pair_swiss(entrants(fixture["players"]), past, set(range(1, fixture["players"] + 1)),
                            fixture["round_no"], fixture["rounds_planned"])
        self.assertEqual([[b["white_entry_id"], b["black_entry_id"] or 0] for b in result], fixture["expected"])

    def test_dutch_exchanges_improve_the_whole_bracket(self):
        # Audit counterexample: a greedy upper/lower-half matching missed two
        # strong preferences. C12/C13 require an exchange and zero misses.
        past = history([(1, 7, "1/2-1/2"), (8, 2, "1/2-1/2"), (3, 9, "0-1"),
                        (10, 4, "1/2-1/2"), (5, 11, "1-0"), (12, 6, "1/2-1/2")])
        result = pairs(pair_swiss(entrants(12), past, set(range(1, 13)), 2, 7))
        self.assertEqual(result, [(9, 5), (6, 1), (2, 10), (4, 8), (7, 12), (11, 3)])

    def test_higher_score_gets_equal_colour_preference_before_start_number(self):
        past = history([(1, 3, "1-0"), (2, 7, "1-0")],
                       [(4, 1, "1-0"), (8, 2, "0-1")],
                       [(1, 5, "1-0"), (2, 9, "1-0")],
                       [(6, 1, "1-0"), (10, 2, "1-0")])
        # Both WBWB, but #2 has three points against #1's two (art. 5.2.4).
        self.assertEqual(pairs(pair_swiss(entrants(10), past, {1, 2}, 5, 7)), [(2, 1)])

    def test_colour_sequences_align_from_last_played_game_ignoring_absences(self):
        past = history([(2, 3, "1-0")], [(1, 4, "1-0"), (5, 2, "0-1")],
                       [(2, 6, "0-1")], [(7, 1, "0-1"), (8, 2, "1-0")])
        # #1: -W-B, #2: WBWB. Played sequences end in WB for both, so E4
        # (higher ranked) decides; comparing common calendar rounds is wrong.
        self.assertEqual(pairs(pair_swiss(entrants(8), past, {1, 2}, 5, 7)), [(1, 2)])

    def test_absolute_colour_preferences_and_repeats_block_individual_pairings(self):
        past = history([(1, 3, "1-0"), (2, 4, "1-0")],
                       [(1, 5, "1-0"), (2, 6, "1-0")])
        self.assertEqual(pair_swiss(entrants(6), past, {1, 2}, 3, 5), [])
        past = history([(1, 2, "1/2-1/2")])
        self.assertEqual(pair_swiss(entrants(2), past, {1, 2}, 2, 5), [])

    def test_opposite_absolute_preferences_are_granted(self):
        past = history([(1, 3, "1/2-1/2"), (4, 2, "1/2-1/2")],
                       [(1, 5, "1/2-1/2"), (6, 2, "1/2-1/2")])
        self.assertEqual(pairs(pair_swiss(entrants(6), past, {1, 2}, 3, 5)), [(2, 1)])

    def test_final_round_topscorers_may_meet_with_equal_absolute_preferences(self):
        past = history([(1, 3, "1-0"), (2, 4, "1-0")], [(1, 5, "1-0"), (2, 6, "1-0")])
        self.assertEqual(len(pair_swiss(entrants(6), past, {1, 2}, 3, 3)), 1)

    def test_team_colour_preferences_never_prohibit_a_match(self):
        past = history([(1, 2, "1-0"), (3, 4, "1-0"), (5, 6, "1-0")],
                       [(1, 4, "1-0"), (3, 6, "1-0"), (5, 2, "1-0")])
        result = pairs(pair_swiss(entrants(6), past, {1, 3}, 3, 5, is_team=True))
        self.assertEqual(result, [(3, 1)])

    def test_team_upfloater_history_and_last_two_round_exception(self):
        # Leader #1 needs an upfloater. #2 and #3 have equal scores, but #2
        # floated last round. Team C7 prefers #3 except in the last two rounds.
        past = history([(i, i + 6, "1-0") for i in range(1, 7)],
                       [(1, 8, "1-0"), (2, 9, "1/2-1/2"), (3, 4, "1/2-1/2"),
                        (5, 6, "1/2-1/2"), (10, 11, "1/2-1/2"), (12, 7, "1/2-1/2")])
        for planned, opponent in ((6, 3), (4, 2)):
            with self.subTest(planned=planned):
                result = pairs(pair_swiss(entrants(12), past, set(range(1, 13)), 3, planned, is_team=True))
                self.assertIn(frozenset((1, opponent)), {frozenset(p) for p in result})

    def test_forfeit_can_be_repaired_but_a_played_game_cannot(self):
        for team in (False, True):
            for code in ("1F-0F", "0F-1F", "0F-0F"):
                with self.subTest(team=team, code=code):
                    past = history([(1, 2, code)])
                    self.assertEqual(len(pair_swiss(entrants(2), past, {1, 2}, 2, 5, is_team=team)), 1)

    def test_full_point_forfeit_winner_and_previous_bye_are_ineligible_for_bye(self):
        past = history([(1, 2, "1F-0F"), (3, 4, "1-0"), (5, None, "BYE")])
        for team in (False, True):
            result = pairs(pair_swiss(entrants(5), past, {1, 3, 5}, 2, 5, is_team=team))
            self.assertIn((3, None), result)
            self.assertEqual(pair_swiss(entrants(5), past, {1}, 2, 5, is_team=team), [])
            self.assertEqual(pair_swiss(entrants(5), past, {5}, 2, 5, is_team=team), [])

    def test_database_ids_with_gaps_and_withdrawn_entrants_are_not_reassigned(self):
        entries = entrants(4)
        for entry in entries:
            entry["id"] *= 101
        result = pairs(pair_swiss(entries, [], {101, 202, 303, 404}, 1, 5))
        self.assertEqual(result, [(101, 303), (404, 202)])
        past = history([(101, 303, "1-0"), (404, 202, "0-1")])
        self.assertEqual(pairs(pair_swiss(entries, past, {101, 202}, 2, 5)), [(202, 101)])


class RoundResultsTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.app = create_app({"TESTING": True, "SECRET_KEY": "pairing-tests",
                               "INSTANCE_PATH": Path(self.tempdir.name), "MAIL_ENABLED": False})
        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session["is_admin"] = True
        with self.app.app_context():
            db = get_db()
            self.tid = db.execute("""INSERT INTO tournament
                (name,slug,event_date,rounds_planned,status,source_type,excludes_rating)
                VALUES ('Pairing regression','pairing-regression','2026-09-27',3,'draft','local',1)""").lastrowid
            self.ids = []
            for i in range(4):
                pid = db.execute("INSERT INTO player (name,normalized_name) VALUES (?,?)", (f"P{i}", f"p{i}")).lastrowid
                self.ids.append(db.execute("""INSERT INTO tournament_entry
                    (tournament_id,player_id,imported_name,seed_rating,member_status,is_active)
                    VALUES (?, ?, ?, ?, 'non-member', 1)""", (self.tid, pid, f"P{i}", 2000-i*100)).lastrowid)
            db.commit()
        self.form = {"board_count": "2", "white_1": self.ids[0], "black_1": self.ids[1], "result_1": "1-0",
                     "white_2": self.ids[2], "black_2": self.ids[3], "result_2": "1/2-1/2"}

    def tearDown(self):
        self.tempdir.cleanup()

    def post(self, rnd, action="save", data=None):
        return self.client.post(f"/admin/t/pairing-regression/round/{rnd}/{action}",
                                data=self.form if data is None else data,
                                headers={"X-Requested-With": "XMLHttpRequest"})

    def stored(self):
        with self.app.app_context():
            return [dict(r) for r in fetch_pairings(get_db(), self.tid)]

    def test_skipped_out_of_range_and_incomplete_rounds_reject_save_and_generation(self):
        for rnd in (0, 2, 99):
            for action in ("save", "generate"):
                with self.subTest(rnd=rnd, action=action):
                    self.assertEqual(self.post(rnd, action).status_code, 400)
                    self.assertEqual(self.stored(), [])
        self.assertEqual(self.post(1, data={**self.form, "result_1": ""}).status_code, 200)
        previous = self.stored()
        for action in ("save", "generate"):
            self.assertEqual(self.post(2, action).status_code, 400)
            self.assertEqual(self.stored(), previous)

    def test_results_can_be_corrected_until_next_round_is_paired(self):
        self.assertEqual(self.post(1).status_code, 200)
        self.assertEqual(self.post(1, data={**self.form, "result_1": "0-1"}).status_code, 200)
        self.assertEqual(self.post(2, "generate").status_code, 302)
        previous = self.stored()
        self.assertEqual(self.post(1).status_code, 400)
        self.assertEqual(self.post(2, "generate").status_code, 400)
        self.assertEqual(self.stored(), previous)

    def test_completed_and_historical_tournaments_reject_round_changes(self):
        self.post(1)
        for update in ("status = 'completed'", "status = 'running', is_historical = 1"):
            with self.app.app_context():
                db = get_db(); db.execute(f"UPDATE tournament SET {update} WHERE id = ?", (self.tid,)); db.commit()
            previous = self.stored()
            for action in ("save", "generate"):
                self.assertEqual(self.post(1, action).status_code, 400)
                self.assertEqual(self.post(2, action).status_code, 400)
            self.assertEqual(self.stored(), previous)

    def test_invalid_results_and_malformed_forms_do_not_replace_existing_results(self):
        self.post(1)
        previous = self.stored()
        for changes in ({"result_1": "banana"}, {"result_1": "2-0"}, {"result_1": "nan-inf"},
                        {"board_count": "1.5"}, {"board_count": "99"}, {"white_1": ""},
                        {"result_1": "BYE"}, {"black_1": "", "result_1": "1F-0F"}):
            with self.subTest(changes=changes):
                self.assertEqual(self.post(1, data={**self.form, **changes}).status_code, 400)
                self.assertEqual(self.stored(), previous)

    def test_legacy_invalid_result_does_not_complete_round(self):
        self.post(1)
        with self.app.app_context():
            db = get_db()
            db.execute("UPDATE pairing SET result_code = 'BANANA' WHERE tournament_id = ? AND board_no = 1", (self.tid,))
            db.commit()
            self.assertFalse(pairings_complete(db, self.tid, 1))
        self.assertEqual(self.post(2, "generate").status_code, 400)

    def test_forfeit_results_award_points_without_played_colours_or_opponents(self):
        for result, points in (("1F-0F", (1, 0)), ("0F-1F", (0, 1)), ("0F-0F", (0, 0))):
            with self.subTest(result=result):
                response = self.post(1, data={**self.form, "result_1": result})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json["next_round"], 2)
                with self.app.app_context():
                    db = get_db()
                    self.assertTrue(pairings_complete(db, self.tid, 1))
                    rows = {r["entry_id"]: r for r in compute_standings(db, self.tid)}
                for entry_id, score in zip(self.ids[:2], points):
                    self.assertEqual(rows[entry_id]["score"], score)
                    self.assertEqual(rows[entry_id]["colors"], [])
                    self.assertEqual(rows[entry_id]["opponent_ids"], set())
        response = self.post(1, data={**self.form, "result_1": ""})
        self.assertIsNone(response.json["next_round"])

    def test_ui_and_autosave_payload_reflect_round_locks_and_forfeit_options(self):
        response = self.post(1, data={**self.form, "result_1": "1F-0F"})
        self.assertIsNone(response.json["round_locks"]["1"])
        self.assertIsNone(response.json["round_locks"]["2"])
        self.assertIsNotNone(response.json["round_locks"]["3"])
        html = self.client.get("/admin/t/pairing-regression?open_round=1").data
        for text in (b"White wins</button>", b"Draw</button>", b"Black wins</button>",
                     b"White wins by forfeit", b"Black wins by forfeit", b"Both forfeit", b"Clear result",
                     b'value="1F-0F" selected', b'data-round-editor disabled'):
            self.assertIn(text, html)
        response = self.post(1, data={**self.form, "result_1": ""})
        self.assertIsNotNone(response.json["round_locks"]["2"])


if __name__ == "__main__":
    unittest.main()
