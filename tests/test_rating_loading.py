import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from rating import BaseClass, Game, Manager, Player, PlayerDatabase, Rating, Tournament


class RatingLoadingTestCase(unittest.TestCase):
    def test_repeated_loads_preserve_saved_ratings_history_games_and_settings(self):
        manager = Manager(player_database=PlayerDatabase(strict=False))
        alice = Player("Alice", id=7001, rating=Rating(rating=1734.25, deviation=71.5),
                       rating_history=[(Rating(rating=1690), datetime(2026, 1, 1))])
        bob = Player("Bob", id=7002, rating=Rating(rating=1550, deviation=95))
        tournament = Tournament("Cup", datetime(2026, 10, 1), rounds=2)
        manager.add_tournament(tournament, players=[alice, bob], games=[
            Game(alice.id, bob.id, "1-0", tournament.get_date(), tournament_id=tournament.id),
            Game(bob.id, alice.id, "1/2-1/2", tournament.get_date(), tournament_id=tournament.id),
        ])
        expected = manager.generate_settings()
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "ratings.json")
            manager.save(path)
            for _ in range(3):
                loaded = Manager.load(path)
                self.assertEqual(loaded.generate_settings(), expected)
                self.assertEqual(loaded.player_database[alice.id].get_rating().rating, 1734.25)
                self.assertEqual(len(list(loaded.game_database.get_games_per_player(alice.id))), 2)

    def test_newly_registered_classes_are_discovered_after_an_earlier_miss(self):
        settings = {"class": "LaterRegisteredRatingRecord", "payload": {"values": [1, 2]}}
        self.assertEqual(BaseClass.load_from_dict(settings), settings)
        record_type = type("LaterRegisteredRatingRecord", (BaseClass,), {})
        loaded = BaseClass.load_from_dict(settings)
        self.assertIsInstance(loaded, record_type)
        self.assertEqual(loaded.payload, {"values": [1, 2]})

    def test_plain_nested_dictionaries_and_unknown_class_names_stay_unchanged(self):
        settings = {"class": "NotARatingObject", "items": [{"label": "plain"}, [1, {"value": 2}]]}
        for _ in range(3):
            self.assertEqual(BaseClass.load_from_dict(settings), settings)


if __name__ == "__main__":
    unittest.main()
