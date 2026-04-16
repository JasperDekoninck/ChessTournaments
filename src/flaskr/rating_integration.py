from __future__ import annotations

import csv
import re
import shlex
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import click
from flask import current_app

from rating import (
    AnonymousLeaderboard,
    Game,
    Manager,
    Player,
    PlayerDatabase,
    Tournament,
    TournamentRanking,
)
from rating.utils import extract_players, extract_tournament

from .core import (
    MatchResult,
    attach_entries_to_tournament,
    compute_standings,
    ensure_round_status_rows,
    fetch_entries,
    fetch_pairings,
    fuzzy_best_match,
    normalize_name,
    normalize_result_code,
    replace_round_pairings,
    slugify,
    unique_slug,
)
from .db import get_db


@dataclass
class HistoricalSourceSpec:
    kind: str
    path: Path
    tournament_name: str
    tournament_date: str | None = None

    @property
    def source_ref(self) -> str:
        if self.kind == "manual":
            return f"manual:{self.path}:{self.tournament_date}"
        return f"vega:{self.path}"


def _rating_dir() -> Path:
    return Path(current_app.config["RATING_DATA_DIR"])


def _manager_path(kind: str) -> Path:
    return _rating_dir() / f"{kind}.json"


def _members_path() -> Path:
    return _rating_dir() / "members.csv"


def _anon_path(name: str) -> Path:
    return _rating_dir() / name


def _ensure_rating_dir():
    _rating_dir().mkdir(parents=True, exist_ok=True)
    Path(current_app.config["EXPORT_DIR"]).mkdir(parents=True, exist_ok=True)


def import_rating_history(source_repo: str):
    _ensure_rating_dir()
    source = Path(source_repo)
    shutil.copy2(source / "data" / "databases.json", _manager_path("baseline"))
    shutil.copy2(source / "data" / "databases.json", _manager_path("current"))
    shutil.copy2(source / "members.csv", _members_path())
    shutil.copy2(source / "data" / "anonymous.txt", _anon_path("anonymous.txt"))
    shutil.copy2(source / "data" / "not_anonymous.txt", _anon_path("not_anonymous.txt"))
    db = get_db()
    db.execute(
        """
        INSERT INTO app_config (key, value) VALUES ('rating_history_source', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (str(source),),
    )
    db.execute(
        """
        INSERT INTO app_config (key, value) VALUES ('rating_history_imported_at', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (datetime.utcnow().isoformat(),),
    )
    db.commit()
    return sync_historical_tournaments(str(source))


def _load_manager(kind: str) -> Manager:
    path = _manager_path(kind)
    if path.exists():
        return Manager.load(str(path))
    return Manager(player_database=PlayerDatabase(strict=False))


def current_manager() -> Manager:
    return _load_manager("current")


def baseline_manager() -> Manager:
    return _load_manager("baseline")


def load_member_names() -> list[str]:
    if not _members_path().exists():
        return []
    return [
        line.strip()
        for line in _members_path().read_text(encoding="utf-8-sig").splitlines()[1:]
        if line.strip()
    ]


def build_matcher():
    manager = baseline_manager()
    player_rows = []
    for player in manager.player_database:
        rating = player.get_rating()
        wins = player.get_number_of_wins(manager.game_database.get_games_per_player(player.id))
        losses = player.get_number_of_losses(manager.game_database.get_games_per_player(player.id))
        draws = player.get_number_of_draws(manager.game_database.get_games_per_player(player.id))
        player_rows.append(
            {
                "name": player.name,
                "rating": rating.rating,
                "deviation": rating.deviation,
                "wins": wins,
                "losses": losses,
                "draws": draws,
            }
        )
    historical_names = [row["name"] for row in player_rows]
    member_names = load_member_names()
    member_name_set = {normalize_name(name) for name in member_names}

    def matcher(name: str, declared_rating: int | None) -> MatchResult:
        exact_normalized = normalize_name(name)
        historical = None
        exact_player = next((row for row in player_rows if normalize_name(row["name"]) == exact_normalized), None)
        if exact_player is not None:
            historical = exact_player
        else:
            best_name, score = fuzzy_best_match(name, historical_names)
            if best_name and score >= 0.86:
                historical = next(row for row in player_rows if row["name"] == best_name)

        member_status = "member" if exact_normalized in member_name_set else "non-member"
        if member_status == "non-member":
            best_member, score = fuzzy_best_match(name, member_names)
            if best_member and score >= 0.92:
                member_status = "member"

        if historical is None:
            return MatchResult(
                canonical_name=name,
                historical_rating=float(declared_rating) if declared_rating is not None else None,
                rating_deviation=None,
                wins=0,
                losses=0,
                draws=0,
                member_status=member_status,
            )
        return MatchResult(
            canonical_name=historical["name"],
            historical_rating=historical["rating"],
            rating_deviation=historical["deviation"],
            wins=historical["wins"],
            losses=historical["losses"],
            draws=historical["draws"],
            member_status=member_status,
        )

    return matcher


def rating_status(db) -> dict:
    source = db.execute("SELECT value FROM app_config WHERE key = 'rating_history_source'").fetchone()
    imported_at = db.execute("SELECT value FROM app_config WHERE key = 'rating_history_imported_at'").fetchone()
    historical_count = db.execute(
        "SELECT COUNT(*) AS c FROM tournament WHERE source_type = 'history'"
    ).fetchone()["c"]
    return {
        "source": source["value"] if source else None,
        "imported_at": imported_at["value"] if imported_at else None,
        "ready": _manager_path("baseline").exists(),
        "historical_count": historical_count,
    }


def _parse_main_script(source_repo: Path) -> list[HistoricalSourceSpec]:
    script_path = source_repo / "scripts" / "main.sh"
    if not script_path.exists():
        return []
    specs: list[HistoricalSourceSpec] = []
    for raw_line in script_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line.startswith("python scripts/"):
            continue
        parts = shlex.split(line)
        script = parts[1]
        if script not in {"scripts/tournament.py", "scripts/manual_tournament.py"}:
            continue
        args = {}
        index = 2
        while index < len(parts):
            part = parts[index]
            if part.startswith("--"):
                if index + 1 < len(parts) and not parts[index + 1].startswith("--"):
                    args[part] = parts[index + 1]
                    index += 2
                else:
                    args[part] = True
                    index += 1
            else:
                index += 1
        source_path = Path(args["--tournament_path"])
        if not source_path.is_absolute():
            source_path = source_repo / source_path
        specs.append(
            HistoricalSourceSpec(
                kind="manual" if script.endswith("manual_tournament.py") else "vega",
                path=source_path,
                tournament_name=args.get("--tournament_name") or source_path.name,
                tournament_date=args.get("--tournament_date"),
            )
        )
    return specs


def _parse_pairing_name(value: str) -> str:
    cleaned = re.sub(r"[\[\]=@0-9]", "", value).strip()
    return " ".join(cleaned.split())


def _pairings_file(folder: Path, round_no: int) -> Path | None:
    candidates = [folder / f"pairings{round_no}.qtf", folder / f"pairs-bis{round_no}.qtf"]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _parse_vega_rounds(folder: Path, rounds_hint: int) -> dict[int, list[dict]]:
    rounds: dict[int, list[dict]] = defaultdict(list)
    for round_no in range(1, rounds_hint + 10):
        file_path = _pairings_file(folder, round_no)
        if file_path is None:
            break
        data = file_path.read_text(encoding="utf-8", errors="ignore")
        local = data.split("\n")[0].split("::")
        board_no = 1
        for index in range(20, len(local) - 9, 10):
            if "( not paired )" in local[index + 8] or "(not paired)" in local[index + 8]:
                continue
            white_name = _parse_pairing_name(local[index + 2])
            if not white_name:
                continue
            black_name = _parse_pairing_name(local[index + 8])
            result_code = normalize_result_code(local[index + 5][7:-1].replace(" ", "").replace("\u00bd", "1/2"))
            if black_name == "( bye )":
                black_name = None
                result_code = "BYE"
            rounds[round_no].append(
                {
                    "board_no": board_no,
                    "white_name": white_name,
                    "black_name": black_name,
                    "result_code": result_code,
                }
            )
            board_no += 1
    return rounds


def _parse_manual_rounds(csv_path: Path) -> tuple[dict[int, list[dict]], list[str], int]:
    rounds: dict[int, list[dict]] = defaultdict(list)
    players: set[str] = set()
    max_round = 0
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.DictReader(file_handle, delimiter=";")
        for row in reader:
            round_no = int(row["Round"])
            board_no = int(row["Board"])
            white_name = (row.get("White") or "").strip()
            black_name = (row.get("Black") or "").strip() or None
            result_code = normalize_result_code(row.get("Result"))
            if black_name is None or (result_code and result_code.lower() == "bye"):
                black_name = None
                result_code = "BYE"
            rounds[round_no].append(
                {
                    "board_no": board_no,
                    "white_name": white_name,
                    "black_name": black_name,
                    "result_code": result_code,
                }
            )
            players.add(white_name)
            if black_name:
                players.add(black_name)
            max_round = max(max_round, round_no)
    for boards in rounds.values():
        boards.sort(key=lambda row: row["board_no"])
    return rounds, sorted(players), max_round


def _load_source_tournament(spec: HistoricalSourceSpec) -> dict:
    if spec.kind == "manual":
        rounds, player_names, rounds_planned = _parse_manual_rounds(spec.path)
        return {
            "name": spec.tournament_name,
            "event_date": spec.tournament_date,
            "rounds_planned": rounds_planned,
            "player_names": player_names,
            "rounds": rounds,
        }

    tournament = extract_tournament(str(spec.path))
    tournament.name = spec.tournament_name
    players, _, _ = extract_players(str(spec.path))
    rounds = _parse_vega_rounds(spec.path, tournament.rounds)
    player_names = {player.name for player in players}
    for boards in rounds.values():
        for board in boards:
            player_names.add(board["white_name"])
            if board["black_name"]:
                player_names.add(board["black_name"])
    return {
        "name": tournament.name,
        "event_date": tournament.date.strftime("%Y-%m-%d"),
        "rounds_planned": max(rounds.keys(), default=tournament.rounds),
        "player_names": sorted(name for name in player_names if name),
        "rounds": rounds,
    }


def _find_leaderboard_csv(source_repo: Path, event_date: str, tournament_name: str) -> Path | None:
    tournaments_dir = source_repo / "data" / "tournaments"
    if not tournaments_dir.exists():
        return None
    prefix = datetime.fromisoformat(event_date).strftime("%Y_%m_%d")
    normalized_target = normalize_name(tournament_name)
    prefix_matches = []
    for folder in tournaments_dir.iterdir():
        if not folder.is_dir() or not folder.name.startswith(prefix):
            continue
        suffix = folder.name[len(prefix) + 1 :]
        leaderboard_path = folder / "leaderboard.csv"
        if not leaderboard_path.exists():
            continue
        if normalize_name(suffix) == normalized_target:
            return leaderboard_path
        prefix_matches.append((suffix, leaderboard_path))
    if not prefix_matches:
        return None
    best_name, score = fuzzy_best_match(tournament_name, [name for name, _ in prefix_matches])
    if not best_name or score < 0.8:
        return None
    return next(path for name, path in prefix_matches if name == best_name)


def _load_leaderboard_snapshot(source_repo: Path, event_date: str, tournament_name: str) -> tuple[dict, tuple[str, str]]:
    leaderboard_path = _find_leaderboard_csv(source_repo, event_date, tournament_name)
    if leaderboard_path is None:
        return {}, ("BH", "BH-C1")
    with leaderboard_path.open("r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        fieldnames = reader.fieldnames or []
        tie_break_fields = [
            field
            for field in fieldnames
            if field not in {"Rank", "Name", "Rating", "Score", "Performance"}
        ]
        primary_label = tie_break_fields[0] if len(tie_break_fields) >= 1 else "BH"
        secondary_label = tie_break_fields[1] if len(tie_break_fields) >= 2 else "BH-C1"
        rows = {}
        for row in reader:
            rows[row["Name"]] = {
                "rank": int(float(row["Rank"])),
                "score": float(row["Score"]),
                "primary": float(row[primary_label]) if row.get(primary_label) not in {None, ""} else 0.0,
                "secondary": float(row[secondary_label]) if row.get(secondary_label) not in {None, ""} else 0.0,
            }
    return rows, (primary_label, secondary_label)


def _match_entry(entry_name: str, entries: list[dict]) -> dict | None:
    exact_normalized = normalize_name(entry_name)
    exact = next((entry for entry in entries if normalize_name(entry["imported_name"]) == exact_normalized), None)
    if exact is not None:
        return exact
    best_name, score = fuzzy_best_match(entry_name, [entry["imported_name"] for entry in entries])
    if best_name and score >= 0.75:
        return next(entry for entry in entries if entry["imported_name"] == best_name)
    return None


def sync_historical_tournaments(source_repo: str) -> int:
    source_root = Path(source_repo)
    db = get_db()
    specs = _parse_main_script(source_root)
    if not specs:
        return 0

    db.execute("DELETE FROM tournament WHERE source_type = 'history'")
    db.commit()

    matcher = build_matcher()
    imported_count = 0

    for spec in specs:
        if not spec.path.exists():
            continue
        tournament_data = _load_source_tournament(spec)
        duplicate_local = db.execute(
            """
            SELECT 1
            FROM tournament
            WHERE source_type != 'history' AND event_date = ? AND lower(name) = lower(?)
            LIMIT 1
            """,
            (tournament_data["event_date"], tournament_data["name"]),
        ).fetchone()
        if duplicate_local is not None:
            continue
        leaderboard_snapshot, tie_break_labels = _load_leaderboard_snapshot(
            source_root,
            tournament_data["event_date"],
            tournament_data["name"],
        )
        slug = unique_slug(db, slugify(tournament_data["name"]))
        cursor = db.execute(
            """
            INSERT INTO tournament (
              name, slug, event_date, rounds_planned, status, source_type, source_ref,
              primary_tiebreak_label, secondary_tiebreak_label, is_historical, is_public, is_active_public
            ) VALUES (?, ?, ?, ?, 'completed', 'history', ?, ?, ?, 1, 1, 0)
            """,
            (
                tournament_data["name"],
                slug,
                tournament_data["event_date"],
                tournament_data["rounds_planned"],
                spec.source_ref,
                tie_break_labels[0],
                tie_break_labels[1],
            ),
        )
        tournament_id = cursor.lastrowid
        attach_entries_to_tournament(
            db,
            tournament_id,
            [
                {"name": name, "email": None, "submitted_at": None, "declared_rating": None}
                for name in tournament_data["player_names"]
            ],
            matcher,
        )
        ensure_round_status_rows(db, tournament_id, tournament_data["rounds_planned"])
        entries = list(fetch_entries(db, tournament_id))

        leaderboard_names = list(leaderboard_snapshot.keys())
        for entry in entries:
            snapshot = leaderboard_snapshot.get(entry["imported_name"])
            if snapshot is None and leaderboard_names:
                best_name, score = fuzzy_best_match(entry["imported_name"], leaderboard_names)
                if best_name and score >= 0.75:
                    snapshot = leaderboard_snapshot.get(best_name)
            if snapshot is None:
                continue
            db.execute(
                """
                UPDATE tournament_entry
                SET final_rank = ?, final_score = ?, final_primary_tiebreak = ?, final_secondary_tiebreak = ?
                WHERE id = ?
                """,
                (
                    snapshot["rank"],
                    snapshot["score"],
                    snapshot["primary"],
                    snapshot["secondary"],
                    entry["id"],
                ),
            )

        for round_no, boards in sorted(tournament_data["rounds"].items()):
            round_boards = []
            for board in boards:
                white_entry = _match_entry(board["white_name"], entries)
                black_entry = _match_entry(board["black_name"], entries) if board["black_name"] else None
                if white_entry is None:
                    continue
                round_boards.append(
                    {
                        "board_no": board["board_no"],
                        "white_entry_id": white_entry["id"],
                        "black_entry_id": black_entry["id"] if black_entry else None,
                        "result_code": board["result_code"],
                    }
                )
            replace_round_pairings(db, tournament_id, round_no, round_boards)
        imported_count += 1

    db.commit()
    return imported_count


def sync_historical_tournaments_from_saved_source() -> int:
    db = get_db()
    source = db.execute("SELECT value FROM app_config WHERE key = 'rating_history_source'").fetchone()
    if source is None:
        return 0
    return sync_historical_tournaments(source["value"])


def rebuild_current_manager(db):
    _ensure_rating_dir()
    manager = baseline_manager().clone()
    tournaments = db.execute(
        """
        SELECT id, name, slug, event_date, rounds_planned
        FROM tournament
        WHERE status = 'completed' AND is_historical = 0
        ORDER BY event_date ASC, id ASC
        """
    ).fetchall()

    tournament_export_root = Path(current_app.config["EXPORT_DIR"]) / "tournaments"
    tournament_export_root.mkdir(parents=True, exist_ok=True)

    for tournament_row in tournaments:
        existing_baseline = next(
            (
                tournament
                for tournament in manager.tournament_database
                if normalize_name(tournament.name) == normalize_name(tournament_row["name"])
                and tournament.get_date().strftime("%Y-%m-%d") == tournament_row["event_date"]
            ),
            None,
        )
        if existing_baseline is not None:
            continue
        standings = compute_standings(db, tournament_row["id"])
        tie_breaks = {
            row["entry_id"]: {"BH": row["bh"], "BH-C1": row["bh_c1"]}
            for row in standings
        }
        tournament = Tournament(
            tournament_row["name"],
            datetime.fromisoformat(tournament_row["event_date"]),
            rounds=tournament_row["rounds_planned"],
            tie_breaks=tie_breaks,
            tie_break_names=["BH", "BH-C1"],
        )
        entries = fetch_entries(db, tournament_row["id"])
        entry_to_player_id = {}
        players = []
        for entry in entries:
            canonical_name = entry["canonical_rating_name"] or entry["imported_name"]
            existing = manager.player_database.get_player_by_name(canonical_name)
            if existing is None:
                created = Player(canonical_name)
                players.append(created)
                entry_to_player_id[entry["id"]] = created.id
            else:
                entry_to_player_id[entry["id"]] = existing.id
        bye_rows = []
        games = []
        for pairing in fetch_pairings(db, tournament_row["id"]):
            if pairing["white_entry_id"] is None:
                continue
            if pairing["black_entry_id"] is None:
                bye_rows.append((entry_to_player_id[pairing["white_entry_id"]], pairing["round_no"]))
                continue
            if normalize_result_code(pairing["result_code"]) not in {"1-0", "0-1", "1/2-1/2"}:
                continue
            games.append(
                Game(
                    entry_to_player_id[pairing["white_entry_id"]],
                    entry_to_player_id[pairing["black_entry_id"]],
                    normalize_result_code(pairing["result_code"]),
                    datetime.fromisoformat(tournament_row["event_date"]),
                    tournament_id=tournament.id,
                    add_home_advantage=manager.add_home_advantage,
                    forfeit_keep_points=manager.forfeit_keep_points,
                )
            )
        tournament.byes = bye_rows
        manager.add_tournament(tournament, games=games, players=players)

    manager.update_rating()
    manager.save(str(_manager_path("current")))

    anonymous = AnonymousLeaderboard.compute(
        manager.player_database,
        manager.game_database,
        manager.tournament_database,
        save_folder=current_app.config["EXPORT_DIR"],
        file_name="anonymous_leaderboard.csv",
        data_dir=current_app.config["RATING_DATA_DIR"],
    )

    for tournament_row in tournaments:
        tournament = manager.tournament_database.get_tournament_by_name(tournament_row["name"])
        tournament.compute_tournament_results(manager.game_database, manager.player_database, manager.rating_system)
        folder = tournament_export_root / tournament_row["slug"]
        TournamentRanking.compute(
            manager.player_database,
            manager.game_database,
            tournament,
            manager.rating_system,
            save_folder=str(folder),
            file_name="leaderboard.csv",
        )
    return anonymous


def get_player_history(canonical_name: str):
    manager = current_manager()
    player = manager.player_database.get_player_by_name(canonical_name)
    if player is None:
        return []
    tournaments_by_id = {tournament.id: tournament for tournament in manager.tournament_database}
    history = []
    for game in manager.game_database.get_games_per_player(player.id, allow_forfeit=True):
        tournament = tournaments_by_id.get(game.tournament_id)
        history.append(
            {
                "date": game.get_date().strftime("%Y-%m-%d"),
                "tournament_name": tournament.name if tournament else "External game",
                "white": manager.player_database[game.home].name,
                "black": manager.player_database[game.out].name,
                "result": game.result,
            }
        )
    history.sort(key=lambda row: row["date"], reverse=True)
    return history


def anonymous_leaderboard_rows():
    if not _manager_path("current").exists():
        return []
    frame = AnonymousLeaderboard.compute(
        current_manager().player_database,
        current_manager().game_database,
        current_manager().tournament_database,
        data_dir=current_app.config["RATING_DATA_DIR"],
    )
    return frame.to_dict("records")


@click.command("import-rating-history")
@click.option("--source", required=True, type=click.Path(exists=True, file_okay=False, path_type=Path))
def import_rating_history_command(source: Path):
    imported = import_rating_history(str(source))
    click.echo(f"Imported rating history from {source}")
    click.echo(f"Synchronized {imported if isinstance(imported, int) else 'historical'} historical tournaments.")


@click.command("sync-historical-tournaments")
def sync_historical_tournaments_command():
    count = sync_historical_tournaments_from_saved_source()
    click.echo(f"Synchronized {count} historical tournaments.")


@click.command("rebuild-ratings")
def rebuild_ratings_command():
    db = get_db()
    rebuild_current_manager(db)
    click.echo("Rebuilt local rating state.")


def init_app(app):
    app.cli.add_command(import_rating_history_command)
    app.cli.add_command(sync_historical_tournaments_command)
    app.cli.add_command(rebuild_ratings_command)
