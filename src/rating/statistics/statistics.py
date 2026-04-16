from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ..databases.game_database import GameDatabase
from ..databases.player_database import PlayerDatabase
from ..databases.tournament_database import TournamentDatabase
from ..objects.tournament import Tournament
from ..rating.rating_system import RatingSystem


class Statistic:
    @staticmethod
    def compute(
        player_database: PlayerDatabase = None,
        game_database: GameDatabase = None,
        tournament_database: TournamentDatabase = None,
        rating_system: RatingSystem = None,
        save_folder: str = None,
        file_name: str = None,
        **kwargs,
    ) -> Any:
        raise NotImplementedError


class TournamentStatistic(Statistic):
    @staticmethod
    def compute(
        player_database: PlayerDatabase = None,
        game_database: GameDatabase = None,
        tournament: Tournament = None,
        rating_system: RatingSystem = None,
        save_folder: str = None,
        file_name: str = None,
        **kwargs,
    ) -> Any:
        raise NotImplementedError


def _write_if_requested(frame: pd.DataFrame, save_folder: str | None, file_name: str | None) -> pd.DataFrame:
    if save_folder and file_name:
        path = Path(save_folder)
        path.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path / file_name, index=False)
    return frame


class DetailedLeaderboard(Statistic):
    @staticmethod
    def compute_leaderboard(player_database: PlayerDatabase, game_database: GameDatabase) -> pd.DataFrame:
        leaderboard = []
        advantage_names = set()
        for player in player_database:
            advantage_names = advantage_names.union(set(player.get_rating().get_advantage_names()))
        advantage_names = list(advantage_names)
        columns = ["Name", "Rating", "Deviation", "Wins", "Losses", "Draws"]
        for name in advantage_names:
            columns.append(f"{name} Rating")
            columns.append(f"{name} Deviation")
        for player in player_database:
            wins = player.get_number_of_wins(game_database.get_games_per_player(player.id))
            losses = player.get_number_of_losses(game_database.get_games_per_player(player.id))
            draws = player.get_number_of_draws(game_database.get_games_per_player(player.id))
            rating = player.get_rating()
            row = [player.name, rating.rating, rating.deviation, wins, losses, draws]
            for name in advantage_names:
                advantage_rating = rating.get_advantage(name)
                if advantage_rating is None:
                    row.extend([None, None])
                else:
                    row.extend([advantage_rating.rating, advantage_rating.deviation])
            leaderboard.append(row)
        leaderboard.sort(key=lambda x: x[1], reverse=True)
        return pd.DataFrame(leaderboard, columns=columns)

    @staticmethod
    def compute(
        player_database: PlayerDatabase = None,
        game_database: GameDatabase = None,
        tournament_database: TournamentDatabase = None,
        rating_system: RatingSystem = None,
        save_folder: str = None,
        file_name: str = "detailed_leaderboard.csv",
    ) -> pd.DataFrame:
        leaderboard = DetailedLeaderboard.compute_leaderboard(player_database, game_database)
        return _write_if_requested(leaderboard, save_folder, file_name)


class Leaderboard(Statistic):
    @staticmethod
    def compute_leaderboard(
        player_database: PlayerDatabase = None,
        game_database: GameDatabase = None,
        tournament_database: TournamentDatabase = None,
        restricted: bool = False,
        recent_days: int = 365,
    ) -> pd.DataFrame:
        leaderboard = []
        for player in player_database:
            wins = player.get_number_of_wins(game_database.get_games_per_player(player.id))
            losses = player.get_number_of_losses(game_database.get_games_per_player(player.id))
            draws = player.get_number_of_draws(game_database.get_games_per_player(player.id))
            rating = player.get_rating()
            last_game_dates = [
                game.get_date() for game in game_database.get_games_per_player(player.id, allow_forfeit=True)
            ]
            if not last_game_dates:
                continue
            last_game_date = max(last_game_dates)
            condition_met = wins + losses + draws >= 12 and last_game_date >= datetime.now() - timedelta(days=recent_days)
            if not restricted or condition_met:
                leaderboard.append((player.name, int(rating.rating), wins, losses, draws))
        leaderboard.sort(key=lambda x: x[1], reverse=True)
        return pd.DataFrame(leaderboard, columns=["Name", "Rating", "Wins", "Losses", "Draws"])

    @staticmethod
    def compute(
        player_database: PlayerDatabase = None,
        game_database: GameDatabase = None,
        tournament_database: TournamentDatabase = None,
        rating_system: RatingSystem = None,
        save_folder: str = None,
        file_name: str = "leaderboard.csv",
    ) -> pd.DataFrame:
        leaderboard = Leaderboard.compute_leaderboard(
            player_database,
            game_database,
            tournament_database,
            restricted=True,
        )
        return _write_if_requested(leaderboard, save_folder, file_name)


class AnonymousLeaderboard(Statistic):
    @staticmethod
    def compute_leaderboard(
        player_database: PlayerDatabase,
        game_database: GameDatabase,
        tournament_database: TournamentDatabase = None,
        anonymous_date: datetime = datetime(2024, 4, 28),
        anon_file: str = "anonymous.txt",
        not_anom_file: str = "not_anonymous.txt",
        data_dir: str = "data",
        recent_days: int = 300,
    ) -> pd.DataFrame:
        anon_names = []
        with open(Path(data_dir) / anon_file, "r", encoding="utf-8") as f:
            for line in f:
                anon_names.append(line.strip())
        not_anon_names = []
        with open(Path(data_dir) / not_anom_file, "r", encoding="utf-8") as f:
            for line in f:
                not_anon_names.append(line.strip())

        leaderboard = []
        for player in player_database:
            wins = player.get_number_of_wins(game_database.get_games_per_player(player.id))
            losses = player.get_number_of_losses(game_database.get_games_per_player(player.id))
            draws = player.get_number_of_draws(game_database.get_games_per_player(player.id))
            rating = player.get_rating()
            last_game_dates = [
                game.get_date() for game in game_database.get_games_per_player(player.id, allow_forfeit=True)
            ]
            if not last_game_dates:
                continue
            last_game_date = max(last_game_dates)
            condition_met = last_game_date >= datetime.now() - timedelta(days=recent_days)
            is_anon = (
                last_game_date < anonymous_date and player.name not in not_anon_names
            ) or player.name in anon_names
            n_games = wins + losses + draws
            question_mark = " (?)" if n_games < 12 else ""
            if condition_met:
                if not is_anon:
                    leaderboard.append((player.name, str(int(rating.rating)) + question_mark, wins, losses, draws))
                else:
                    leaderboard.append(("Anonymous", str(int(rating.rating)), "", "", ""))
        leaderboard.sort(key=lambda x: int(x[1].split(" ")[0]), reverse=True)
        frame = pd.DataFrame(leaderboard, columns=["Name", "Rating", "Wins", "Losses", "Draws"])
        frame["Rank"] = [i + 1 for i in range(len(frame))]
        return frame[["Rank"] + [col for col in frame.columns if col != "Rank"]]

    @staticmethod
    def compute(
        player_database: PlayerDatabase = None,
        game_database: GameDatabase = None,
        tournament_database: TournamentDatabase = None,
        rating_system: RatingSystem = None,
        save_folder: str = None,
        file_name: str = "anonymized_leaderboard.csv",
        anonymous_date: datetime = datetime(2024, 4, 28),
        anon_file: str = "anonymous.txt",
        not_anom_file: str = "not_anonymous.txt",
        data_dir: str = "data",
    ) -> pd.DataFrame:
        leaderboard = AnonymousLeaderboard.compute_leaderboard(
            player_database,
            game_database,
            tournament_database,
            anonymous_date=anonymous_date,
            anon_file=anon_file,
            not_anom_file=not_anom_file,
            data_dir=data_dir,
        )
        return _write_if_requested(leaderboard, save_folder, file_name)


class TournamentRanking(TournamentStatistic):
    @staticmethod
    def compute(
        player_database: PlayerDatabase = None,
        game_database: GameDatabase = None,
        tournament: Tournament = None,
        rating_system: RatingSystem = None,
        save_folder: str = None,
        file_name: str = "leaderboard.csv",
    ) -> pd.DataFrame:
        ranking = []
        for player_stats in tournament.get_results():
            player = player_database[player_stats["player"]]
            player_rating = player.get_rating_at_date(tournament.get_date(), next=True)
            ranking_info = [player.name, int(player_rating.rating), player_stats["score"]]
            for tie_break_name in tournament.tie_break_names:
                ranking_info.append(player_stats[tie_break_name])
            ranking_info.append(int(player_stats["rating_performance"].rating))
            ranking.append(ranking_info)
        ranking.sort(key=lambda x: tuple(x[2:]), reverse=True)
        frame = pd.DataFrame(
            ranking,
            columns=["Name", "Rating", "Score"] + tournament.tie_break_names + ["Performance"],
        )
        frame["Rank"] = np.arange(1, len(frame) + 1)
        frame = frame[["Rank"] + [col for col in frame.columns if col != "Rank"]]
        return _write_if_requested(frame, save_folder, file_name)
