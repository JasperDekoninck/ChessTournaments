import os
import re
import sys
from datetime import datetime
from xml.etree import ElementTree as ET

from loguru import logger

from .databases import GameDatabase, PlayerDatabase
from .objects import Game, Player, Tournament


def set_logging_level(level: str):
    logger.remove()
    logger.add(sys.stdout, level=level)


def extract_tournament_trfx(folder: str) -> Tournament:
    main_file_pattern = re.compile(r".*\.trfx")
    main_file = None
    for file in os.scandir(folder):
        if main_file_pattern.match(file.name):
            main_file = file
            break
    if main_file is None:
        logger.warning(f"No main file found in {folder}. Resorting to defaults.")
        return Tournament(name="Unknown", date=datetime.now(), rounds=7, time_control="5+3")
    with open(main_file.path, "r", encoding="latin1") as file_handle:
        name = "Unknown"
        date = datetime.now().strftime("%d/%m/%Y")
        for index in range(0, 20):
            line = file_handle.readline()
            if index == 0:
                name = line[4:].strip()
            if index == 3:
                date = line[4:].strip()
    return Tournament(name, datetime.strptime(date, "%d/%m/%Y"), 7, "5+3")


def extract_tournament(folder: str) -> Tournament:
    main_file_pattern = re.compile(r".*\.vegx")
    main_file = None
    for file in os.scandir(folder):
        if main_file_pattern.match(file.name):
            main_file = file
            break

    if main_file is None:
        logger.warning(
            f"No main VEGX file found in {folder}. Resorting to TRFX fallback."
        )
        return extract_tournament_trfx(folder)

    with open(main_file.path, "r", encoding="utf-8", errors="ignore") as file_handle:
        data = file_handle.read()
    root = ET.fromstring(data)
    tournament_name = root.find("Name").text if root.find("Name") is not None else "Unknown"
    date_element = root.find("Date")
    begin_date = date_element.get("Begin") if date_element is not None else datetime.now().strftime("%d/%m/%Y")
    rounds_number = int(root.find(".//RoundsNumber").text if root.find(".//RoundsNumber") is not None else 7)
    rate_move = root.find(".//RateMove").text if root.find(".//RateMove") is not None else "Unknown"
    return Tournament(tournament_name, datetime.strptime(begin_date, "%d/%m/%Y"), rounds_number, rate_move)


def extract_players(folder: str) -> tuple[list[Player], dict, list[str]]:
    file = os.path.join(folder, "standings.qtf")
    if not os.path.isfile(file):
        file = os.path.join(folder, "standing.qtf")
    with open(file, "r", encoding="utf-8", errors="ignore") as file_handle:
        data = file_handle.read()
    lines = data.split("\n")
    players = []
    tie_breaks = dict()
    tie_break_names = []
    doing_tie_break_names = False
    done_first_player = False
    current_player = None
    for index, line in enumerate(lines):
        if index == 14:
            name = line.replace(":: [s0;>*2", "").strip().replace("]", "").strip()
            tie_break_names.append(name)
            doing_tie_break_names = True
        elif doing_tie_break_names and not done_first_player:
            name = line.replace(":: [s0;>*2", "").replace(":: [s0;>2", "").strip().replace("]", "").strip()
            if name in {"1", ""}:
                doing_tie_break_names = False
            else:
                tie_break_names.append(name)
        if "s0;*" in line and "[s0;*2 NAME]" not in line:
            done_first_player = True
            name = line.split("s0;*2 ")[1][:-1]
            if "(" in name:
                name = name.split("(")[0].strip()
            player = Player(name)
            players.append(player)
            current_player = player.id
        if ":: [s0;>2" in line and done_first_player:
            value = float(line.split(":: [s0;>2")[1].split("]")[0].strip())
            if current_player in tie_breaks and len(tie_breaks[current_player]) < len(tie_break_names):
                tie_breaks[current_player][tie_break_names[len(tie_breaks[current_player])]] = value
            elif current_player not in tie_breaks and tie_break_names:
                tie_breaks[current_player] = {tie_break_names[0]: value}
    return players, tie_breaks, tie_break_names


def extract_games(
    folder: str,
    tournament: Tournament,
    game_database: GameDatabase,
    player_database: PlayerDatabase,
    add_home_advantage: bool = True,
    forfeit_keep_points: bool = True,
):
    for round_no in range(1, tournament.rounds + 10):
        file = os.path.join(folder, f"pairings{round_no}.qtf")
        if not os.path.isfile(file):
            file = os.path.join(folder, f"pairs-bis{round_no}.qtf")
        if not os.path.isfile(file):
            tournament.rounds = round_no - 1
            break
        with open(file, "r", encoding="utf-8", errors="ignore") as file_handle:
            data = file_handle.read()
        local = data.split("\n")[0].split("::")
        for index in range(20, len(local) - 9, 10):
            if "(not paired)" not in local[index + 8]:
                white_player = re.sub(r"[\[\]=@123456789]", "", local[index + 2]).strip()
                white_player = player_database.get_player_by_name(white_player)
                result = local[index + 5][7:-1].replace(" ", "").replace("\u00bd", "1/2")
                black_player = re.sub(r"[\[\]=@123456789]", "", local[index + 8]).strip()

                if black_player == "( bye )":
                    logger.debug(f"Bye found for {white_player.name}")
                    tournament.add_bye(white_player.id, round_no)
                    continue

                black_player = player_database.get_player_by_name(black_player)
                if white_player is not None and black_player is not None:
                    game = Game(
                        white_player.id,
                        black_player.id,
                        result,
                        date=tournament.get_date(),
                        tournament_id=tournament.id,
                        add_home_advantage=add_home_advantage,
                        forfeit_keep_points=forfeit_keep_points,
                    )
                    game_database.add(game)
