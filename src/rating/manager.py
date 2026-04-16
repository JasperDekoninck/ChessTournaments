from __future__ import annotations

from datetime import datetime, timedelta

from loguru import logger

from .base import BaseClass
from .databases import GameDatabase, PlayerDatabase, TournamentDatabase
from .objects import Game, Player, RatingPeriod, RatingPeriodEnum, Tournament
from .rating import PolyratingCrossEntropy, RatingSystem
from .statistics import StatManager


class Manager(BaseClass):
    def __init__(
        self,
        rating_system: RatingSystem = None,
        player_database: PlayerDatabase = None,
        game_database: GameDatabase = None,
        tournament_database: TournamentDatabase = None,
        stat_manager: StatManager = None,
        rating_period: RatingPeriod = None,
        rating_period_type: int = RatingPeriodEnum.TOURNAMENT,
        custom_timedelta: timedelta = timedelta(days=7),
        last_date_update: datetime = None,
        do_recompute: bool = True,
        recompute: bool = False,
        add_home_advantage: bool = True,
        forfeit_keep_points: bool = True,
    ) -> "Manager":
        if rating_system is None:
            rating_system = PolyratingCrossEntropy(linearized=10, epsilon=1e-2)
        if player_database is None:
            player_database = PlayerDatabase()
        if game_database is None:
            game_database = GameDatabase()
        if tournament_database is None:
            tournament_database = TournamentDatabase()
        if stat_manager is None:
            stat_manager = StatManager()
        if rating_period is None:
            rating_period = RatingPeriod()

        super().__init__(
            rating_system=rating_system,
            player_database=player_database,
            game_database=game_database,
            tournament_database=tournament_database,
            stat_manager=stat_manager,
            rating_period=rating_period,
            rating_period_type=rating_period_type,
            custom_timedelta=custom_timedelta,
            last_date_update=last_date_update,
            recompute=recompute,
            do_recompute=do_recompute,
            add_home_advantage=add_home_advantage,
            forfeit_keep_points=forfeit_keep_points,
        )

    def generate_settings(self) -> dict:
        settings = super().generate_settings()
        settings["custom_timedelta"] = self.custom_timedelta.total_seconds()
        settings["last_date_update"] = None
        if self.last_date_update is not None:
            settings["last_date_update"] = self.last_date_update.strftime("%Y-%m-%d - %H:%M:%S")
        return settings

    def clone(self) -> "Manager":
        return Manager.load_from_settings(self.generate_settings())

    def reset_and_recompute(
        self,
        rating_system: RatingSystem = None,
        rating_period_type: int = None,
        custom_timedelta: timedelta = None,
    ):
        if rating_system is not None:
            self.rating_system = rating_system
        if custom_timedelta is not None:
            self.custom_timedelta = custom_timedelta
        if rating_period_type is not None:
            self.rating_period_type = rating_period_type
            if self.rating_period_type == RatingPeriodEnum.TOURNAMENT:
                self.rating_period = RatingPeriod()
                for tournament in self.tournament_database:
                    self.rating_period.trigger_new_period(tournament.get_date())
            elif self.rating_period_type == RatingPeriodEnum.TIMEDELTA:
                self.rating_period = RatingPeriod()
                self.rating_period.trigger_new_period(self.game_database.get_earliest_date())

        self.recompute = True
        self.update_rating()

    @classmethod
    def load_from_settings(cls, settings: dict) -> "Manager":
        kwargs = super().get_input_parameters(settings)
        kwargs["custom_timedelta"] = timedelta(seconds=kwargs["custom_timedelta"])
        if kwargs["last_date_update"] is not None:
            kwargs["last_date_update"] = datetime.strptime(kwargs["last_date_update"], "%Y-%m-%d - %H:%M:%S")
        return cls(**kwargs)

    def trigger_new_period(self, tournament: Tournament = None):
        if self.rating_period_type == RatingPeriodEnum.TOURNAMENT and tournament is not None:
            self.rating_period.trigger_new_period(tournament.get_date())
        elif self.rating_period_type == RatingPeriodEnum.TIMEDELTA:
            if self.last_date_update is None or len(self.rating_period) == 0:
                self.rating_period.trigger_new_period(self.game_database.get_earliest_date() + self.custom_timedelta)

            while self.rating_period.get_last_period() < self.game_database.get_latest_date():
                self.rating_period.trigger_new_period(self.rating_period.get_last_period() + self.custom_timedelta)
        else:
            self.rating_period.trigger_new_period(datetime.now())

    def update_rating(self):
        logger.info("Updating ratings...")

        if len(self.rating_period) == 0:
            logger.info("No rating period set. Automatically triggering a new period.")
            self.trigger_new_period()
        if self.recompute:
            for player in self.player_database:
                player.clear_rating_history()
                player.get_rating().reset()
                self.last_date_update = None
            self.recompute = False
        for period_dates in self.rating_period.iterate_periods(self.last_date_update):
            logger.info(f"Updating ratings for period {period_dates[-1]}...")
            if len(period_dates) == 1 and self.game_database.get_n_games_between_dates(period_dates[0]) == 0:
                continue
            if len(period_dates) > 1 and self.game_database.get_n_games_between_dates(period_dates[-1], period_dates[-2]) == 0:
                continue
            self.rating_system.period_update(self.player_database, self.game_database, period_dates)
            for player in self.player_database:
                player.store_rating(period_dates[-1])
        if len(self.rating_period) > 0:
            self.last_date_update = self.rating_period[-1]

    def add_tournament(
        self,
        tournament: Tournament,
        games: list[Game] | None = None,
        players: list[Player] | None = None,
        force: bool = False,
    ) -> Tournament:
        if tournament is None:
            raise ValueError("A Tournament object is required.")
        if self.tournament_database.check_duplicate(tournament) and not force:
            raise ValueError(
                f"Tournament {tournament.name} already exists in the database. Use force=True to add it anyway."
            )

        self.tournament_database.add(tournament)
        for player in players or []:
            if self.player_database.get_player_by_name(player.name) is None:
                self.player_database.add(player)
        for game in games or []:
            self.game_database.add(game)

        self.player_database.clear_empty(self.game_database)
        self.trigger_new_period(tournament)
        was_false = not self.recompute
        self.recompute = (
            self.last_date_update is not None
            and tournament.get_date() <= self.last_date_update
            and self.do_recompute
        )
        if self.recompute and was_false:
            logger.warning(
                "A tournament earlier than the last rating period was inserted. The next update will trigger a full recompute."
            )
        return tournament

    def add_game(self, game: Game):
        self.game_database.add(game)

    def remove_tournament(self, tournament_name: str = None, tournament: Tournament = None):
        if tournament is None:
            tournament = self.tournament_database.get_tournament_by_name(tournament_name)
        if tournament is None:
            raise ValueError(f"Tournament {tournament_name} not found in the database.")
        for game in self.game_database.get_games_per_tournament(tournament.id):
            if self.last_date_update is not None and game.get_date() < self.last_date_update and self.do_recompute:
                self.recompute = True
                logger.warning(
                    f"You removed a game from {game.get_date()} which is earlier than the last rating period. "
                    "Next recomputation will need a full recompute."
                )
            self.game_database.remove(game)
        self.tournament_database.remove(tournament)
        self.player_database.clear_empty(self.game_database)

    def remove_player(self, player_name: str = None, player: Player = None):
        if player is None:
            player = self.player_database.get_player_by_name(player_name)
        if player is None:
            raise ValueError(f"Player {player_name} not found in the database.")
        for game in self.game_database.get_games_per_player(player.id):
            if self.last_date_update is not None and game.get_date() < self.last_date_update and self.do_recompute:
                self.recompute = True
                logger.warning(
                    f"You removed a game from {game.get_date()} which is earlier than the last rating period. "
                    "Next recomputation will need a full recompute."
                )
            self.game_database.remove(self.game_database[game.id])
        self.player_database.remove(player)

    def add_player(self, player_name: str = None, player: Player = None) -> Player:
        if player is None:
            player = self.player_database.get_player_by_name(player_name)
            if player is None:
                player = Player(player_name)
                self.player_database.add(player)
        else:
            self.player_database.add(player)
        return player
