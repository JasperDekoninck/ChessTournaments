from __future__ import annotations

import csv
import json
import re
import shlex
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import NormalDist

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
    persist_final_standings,
    round_rating_value,
    replace_round_pairings,
    slugify,
    unique_slug,
)
from .db import get_db


MEMBER_SINCE_KEY = "member_since_date"
_MANAGER_CACHE: dict[str, tuple[int | None, Manager]] = {}
_CSV_CACHE: dict[str, tuple[int | None, list[dict]]] = {}
_TOURNAMENT_INSIGHTS_CACHE: dict[tuple[int | None, str, str], dict | None] = {}


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
    stamp = path.stat().st_mtime_ns if path.exists() else None
    cached = _MANAGER_CACHE.get(kind)
    if cached is not None and cached[0] == stamp:
        return cached[1]
    if path.exists():
        manager = Manager.load(str(path))
    else:
        manager = Manager(player_database=PlayerDatabase(strict=False))
    _MANAGER_CACHE[kind] = (stamp, manager)
    return manager


def _manager_stamp(kind: str) -> int | None:
    path = _manager_path(kind)
    return path.stat().st_mtime_ns if path.exists() else None


def current_manager() -> Manager:
    return _load_manager("current")


def baseline_manager() -> Manager:
    return _load_manager("baseline")


def _preferred_manager() -> tuple[Manager | None, int | None]:
    current_path = _manager_path("current")
    if current_path.exists():
        return current_manager(), _manager_stamp("current")
    baseline_path = _manager_path("baseline")
    if baseline_path.exists():
        return baseline_manager(), _manager_stamp("baseline")
    return None, None


def _cached_csv_rows(path: Path) -> list[dict]:
    stamp = path.stat().st_mtime_ns if path.exists() else None
    cached = _CSV_CACHE.get(str(path))
    if cached is not None and cached[0] == stamp:
        return cached[1]
    if not path.exists():
        _CSV_CACHE[str(path)] = (None, [])
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as file_handle:
        rows = list(csv.DictReader(file_handle))
    _CSV_CACHE[str(path)] = (stamp, rows)
    return rows


def default_member_since_date(reference: date | None = None) -> str:
    today = reference or date.today()
    year = today.year if today.month >= 9 else today.year - 1
    return f"{year}-09-01"


def get_member_since_date(db) -> str:
    row = db.execute("SELECT value FROM app_config WHERE key = ?", (MEMBER_SINCE_KEY,)).fetchone()
    if row is not None and row["value"]:
        return row["value"]
    default_value = default_member_since_date()
    db.execute(
        """
        INSERT INTO app_config (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (MEMBER_SINCE_KEY, default_value),
    )
    db.commit()
    return default_value


def _played_game_rows(db):
    return db.execute(
        """
        WITH played AS (
          SELECT we.player_id AS player_id, t.event_date AS event_date
          FROM pairing p
          JOIN tournament t ON t.id = p.tournament_id
          JOIN tournament_entry we ON we.id = p.white_entry_id
          WHERE p.white_entry_id IS NOT NULL AND p.black_entry_id IS NOT NULL
          UNION ALL
          SELECT be.player_id AS player_id, t.event_date AS event_date
          FROM pairing p
          JOIN tournament t ON t.id = p.tournament_id
          JOIN tournament_entry be ON be.id = p.black_entry_id
          WHERE p.black_entry_id IS NOT NULL
        )
        SELECT player_id, event_date FROM played
        """
    ).fetchall()


def _base_member_player_ids(db, cutoff_date: str) -> set[int]:
    return {
        row["player_id"]
        for row in db.execute(
            """
            WITH played AS (
              SELECT we.player_id AS player_id, t.event_date AS event_date
              FROM pairing p
              JOIN tournament t ON t.id = p.tournament_id
              JOIN tournament_entry we ON we.id = p.white_entry_id
              WHERE p.white_entry_id IS NOT NULL AND p.black_entry_id IS NOT NULL
              UNION
              SELECT be.player_id AS player_id, t.event_date AS event_date
              FROM pairing p
              JOIN tournament t ON t.id = p.tournament_id
              JOIN tournament_entry be ON be.id = p.black_entry_id
              WHERE p.black_entry_id IS NOT NULL
            )
            SELECT DISTINCT player_id
            FROM played
            WHERE event_date >= ?
            """,
            (cutoff_date,),
        ).fetchall()
    }


def sync_member_statuses(db) -> str:
    cutoff_date = get_member_since_date(db)
    base_members = _base_member_player_ids(db, cutoff_date)
    overrides = {
        row["player_id"]: bool(row["is_member"])
        for row in db.execute("SELECT player_id, is_member FROM member_override").fetchall()
    }
    players = db.execute("SELECT id FROM player").fetchall()
    updates = []
    for player in players:
        is_member = overrides.get(player["id"], player["id"] in base_members)
        updates.append(("member" if is_member else "non-member", player["id"]))
    if updates:
        db.executemany("UPDATE player SET member_status = ? WHERE id = ?", updates)
        db.execute(
            """
            UPDATE tournament_entry
            SET member_status = COALESCE(
              (SELECT member_status FROM player WHERE player.id = tournament_entry.player_id),
              member_status
            )
            """
        )
        db.commit()
    return cutoff_date


def set_member_since_date(db, value: str) -> str:
    parsed = datetime.fromisoformat(value).date()
    normalized = parsed.isoformat()
    db.execute(
        """
        INSERT INTO app_config (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (MEMBER_SINCE_KEY, normalized),
    )
    db.commit()
    sync_member_statuses(db)
    return normalized


def set_member_override(db, player_id: int, is_member: bool):
    cutoff_date = get_member_since_date(db)
    base_members = _base_member_player_ids(db, cutoff_date)
    if bool(is_member) == (player_id in base_members):
        db.execute("DELETE FROM member_override WHERE player_id = ?", (player_id,))
    else:
        db.execute(
            """
            INSERT INTO member_override (player_id, is_member, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(player_id) DO UPDATE SET
              is_member = excluded.is_member,
              updated_at = CURRENT_TIMESTAMP
            """,
            (player_id, 1 if is_member else 0),
        )
    db.commit()
    sync_member_statuses(db)


def list_member_directory(db) -> list[dict]:
    cutoff_date = get_member_since_date(db)
    rows = db.execute(
        """
        WITH played AS (
          SELECT we.player_id AS player_id, t.event_date AS event_date
          FROM pairing p
          JOIN tournament t ON t.id = p.tournament_id
          JOIN tournament_entry we ON we.id = p.white_entry_id
          WHERE p.white_entry_id IS NOT NULL AND p.black_entry_id IS NOT NULL
          UNION ALL
          SELECT be.player_id AS player_id, t.event_date AS event_date
          FROM pairing p
          JOIN tournament t ON t.id = p.tournament_id
          JOIN tournament_entry be ON be.id = p.black_entry_id
          WHERE p.black_entry_id IS NOT NULL
        )
        SELECT
          p.id,
          p.name,
          p.historical_rating,
          p.member_status,
          MIN(played.event_date) AS first_played,
          MAX(played.event_date) AS last_played,
          COUNT(*) AS games_played,
          SUM(CASE WHEN played.event_date >= ? THEN 1 ELSE 0 END) AS games_since_cutoff,
          mo.is_member AS override_member
        FROM played
        JOIN player p ON p.id = played.player_id
        LEFT JOIN member_override mo ON mo.player_id = p.id
        GROUP BY p.id, p.name, p.historical_rating, p.member_status, mo.is_member
        ORDER BY
          CASE WHEN p.member_status = 'member' THEN 0 ELSE 1 END,
          last_played DESC,
          p.name COLLATE NOCASE ASC
        """,
        (cutoff_date,),
    ).fetchall()
    members = [
        {
            "id": row["id"],
            "name": row["name"],
            "rating": round_rating_value(row["historical_rating"]),
            "member": row["member_status"] == "member",
            "first_played": row["first_played"],
            "last_played": row["last_played"],
            "games_played": row["games_played"],
            "base_member": bool(row["games_since_cutoff"]),
            "has_override": row["override_member"] is not None,
        }
        for row in rows
    ]
    unique_members = {}
    for member in members:
        key = normalize_name(member["name"])
        existing = unique_members.get(key)
        if existing is None or (
            (member["member"], member["games_played"], member["last_played"] or "", member["id"])
            > (existing["member"], existing["games_played"], existing["last_played"] or "", existing["id"])
        ):
            unique_members[key] = member
    return list(unique_members.values())


def player_suggestions(db, query: str, limit: int = 8) -> list[dict]:
    term = (query or "").strip()
    if not term:
        return []
    normalized_term = normalize_name(term)
    like = f"%{term}%"
    normalized_like = f"%{normalized_term.replace(' ', '%')}%" if normalized_term else like
    rows = db.execute(
        """
        SELECT
          p.id,
          p.name,
          p.email,
          p.historical_rating,
          p.member_status,
          MAX(t.event_date) AS last_played
        FROM player p
        LEFT JOIN tournament_entry e ON e.player_id = p.id
        LEFT JOIN tournament t ON t.id = e.tournament_id
        WHERE p.name LIKE ? COLLATE NOCASE
           OR p.normalized_name LIKE ? COLLATE NOCASE
           OR COALESCE(p.canonical_rating_name, '') LIKE ? COLLATE NOCASE
        GROUP BY p.id, p.name, p.email, p.historical_rating, p.member_status
        ORDER BY
          CASE WHEN p.member_status = 'member' THEN 0 ELSE 1 END,
          last_played DESC,
          p.name COLLATE NOCASE ASC
        LIMIT ?
        """,
        (like, normalized_like, like, limit),
    ).fetchall()
    if not rows:
        candidates = db.execute("SELECT id, name, email, historical_rating, member_status FROM player").fetchall()
        names = [row["name"] for row in candidates]
        best_name, score = fuzzy_best_match(term, names)
        if best_name and score >= 0.75:
            rows = [row for row in candidates if row["name"] == best_name][:limit]
    suggestions = [
        {
            "id": row["id"],
            "name": row["name"],
            "email": row["email"],
            "member": row["member_status"] == "member",
            "rating": round_rating_value(row["historical_rating"]),
            "last_played": row["last_played"] if "last_played" in row.keys() else None,
        }
        for row in rows
    ]
    unique_suggestions = {}
    for item in suggestions:
        key = normalize_name(item["name"])
        existing = unique_suggestions.get(key)
        if existing is None or (
            (item["member"], item["last_played"] or "", item["rating"] or 0, -item["id"])
            > (existing["member"], existing["last_played"] or "", existing["rating"] or 0, -existing["id"])
        ):
            unique_suggestions[key] = item
    return list(unique_suggestions.values())[:limit]


def _player_rows_from_manager(manager: Manager) -> list[dict]:
    player_rows = []
    for player in manager.player_database:
        rating = player.get_rating()
        games = manager.game_database.get_games_per_player(player.id)
        player_rows.append(
            {
                "name": player.name,
                "normalized_name": normalize_name(player.name),
                "rating": rating.rating,
                "deviation": rating.deviation,
                "wins": player.get_number_of_wins(games),
                "losses": player.get_number_of_losses(games),
                "draws": player.get_number_of_draws(games),
            }
        )
    return player_rows


def sync_player_profiles_from_manager(db, manager: Manager | None = None):
    manager = manager or (current_manager() if _manager_path("current").exists() else baseline_manager())
    rows_by_name = {row["normalized_name"]: row for row in _player_rows_from_manager(manager)}
    players = db.execute("SELECT id, name, canonical_rating_name FROM player").fetchall()
    updates = []
    for player in players:
        key = normalize_name(player["canonical_rating_name"] or player["name"])
        row = rows_by_name.get(key)
        if row is None:
            continue
        updates.append(
            (
                row["rating"],
                row["deviation"],
                row["wins"],
                row["losses"],
                row["draws"],
                player["id"],
            )
        )
    if updates:
        db.executemany(
            """
            UPDATE player
            SET historical_rating = ?, rating_deviation = ?, historical_wins = ?,
                historical_losses = ?, historical_draws = ?
            WHERE id = ?
            """,
            updates,
        )
        db.commit()


def build_matcher():
    db = get_db()
    sync_member_statuses(db)
    manager = current_manager() if _manager_path("current").exists() else baseline_manager()
    player_rows = _player_rows_from_manager(manager)
    historical_names = [row["name"] for row in player_rows]
    member_rows = db.execute("SELECT name FROM player WHERE member_status = 'member'").fetchall()
    member_names = [row["name"] for row in member_rows]
    member_name_set = {normalize_name(name) for name in member_names}

    def matcher(name: str, declared_rating: int | None) -> MatchResult:
        exact_normalized = normalize_name(name)
        historical = None
        exact_player = next((row for row in player_rows if row["normalized_name"] == exact_normalized), None)
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
            default_active=True,
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
    sync_member_statuses(db)
    sync_player_profiles_from_manager(db, current_manager() if _manager_path("current").exists() else baseline_manager())
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
    local_tournaments = db.execute(
        """
        SELECT id, name, slug, event_date, rounds_planned
        FROM tournament
        WHERE status = 'completed' AND is_historical = 0
        ORDER BY event_date ASC, id ASC
        """
    ).fetchall()

    tournament_export_root = Path(current_app.config["EXPORT_DIR"]) / "tournaments"
    tournament_export_root.mkdir(parents=True, exist_ok=True)

    for tournament_row in local_tournaments:
        persist_final_standings(db, tournament_row["id"])
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
    _MANAGER_CACHE.pop("current", None)
    sync_player_profiles_from_manager(db, manager)
    sync_member_statuses(db)

    anonymous = AnonymousLeaderboard.compute(
        manager.player_database,
        manager.game_database,
        manager.tournament_database,
        save_folder=current_app.config["EXPORT_DIR"],
        file_name="anonymous_leaderboard.csv",
        data_dir=current_app.config["RATING_DATA_DIR"],
    )

    completed_tournaments = db.execute(
        """
        SELECT id, name, slug, event_date, is_historical
        FROM tournament
        WHERE status = 'completed'
        ORDER BY event_date ASC, id ASC
        """
    ).fetchall()

    for tournament_row in completed_tournaments:
        tournament = _resolve_manager_tournament(manager, tournament_row["name"], tournament_row["event_date"])
        if tournament is None:
            continue
        insights = _compute_tournament_insights_from_manager(manager, tournament)
        db.execute(
            "UPDATE tournament SET public_insights_json = ? WHERE id = ?",
            (json.dumps(insights, ensure_ascii=True) if insights else None, tournament_row["id"]),
        )
        if tournament_row["is_historical"]:
            continue
        folder = tournament_export_root / tournament_row["slug"]
        TournamentRanking.compute(
            manager.player_database,
            manager.game_database,
            tournament,
            manager.rating_system,
            save_folder=str(folder),
            file_name="leaderboard.csv",
        )
    db.commit()
    return anonymous


def _resolve_manager_player(manager: Manager, player_name: str, db_row=None):
    candidate_names = []
    if db_row is not None:
        candidate_names.extend(
            [
                db_row["canonical_rating_name"] or "",
                db_row["name"] or "",
            ]
        )
    candidate_names.append(player_name)
    normalized_targets = {normalize_name(name) for name in candidate_names if name}
    players = list(manager.player_database)
    for player in players:
        if normalize_name(player.name) in normalized_targets:
            return player
    player_names = [player.name for player in players]
    best_name = None
    best_score = 0.0
    for candidate in candidate_names:
        if not candidate:
            continue
        match_name, score = fuzzy_best_match(candidate, player_names)
        if match_name and score > best_score:
            best_name = match_name
            best_score = score
    if best_name and best_score >= 0.82:
        return next(player for player in players if player.name == best_name)
    return None


def _profile_from_manager_player(manager: Manager, player) -> dict:
    games = list(manager.game_database.get_games_per_player(player.id))
    wins = int(player.get_number_of_wins(games))
    losses = int(player.get_number_of_losses(games))
    draws = int(player.get_number_of_draws(games))
    return {
        "name": player.name,
        "rating": round_rating_value(player.get_rating().rating),
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "games": wins + losses + draws,
    }


def _history_from_manager_player(manager: Manager, player) -> list[dict]:
    history = []
    games = list(manager.game_database.get_games_per_player(player.id))
    tournaments = manager.tournament_database
    players = manager.player_database
    for game in games:
        tournament = tournaments[game.tournament_id] if game.tournament_id is not None else None
        history.append(
            {
                "date": game.get_date().strftime("%Y-%m-%d"),
                "tournament_name": tournament.name if tournament is not None else "Standalone Game",
                "round_no": None,
                "board_no": None,
                "white": players[game.home].name,
                "black": players[game.out].name,
                "result": game.result,
                "sort_key": (game.get_date(), game.id or 0),
            }
        )
    history.sort(key=lambda row: row["sort_key"], reverse=True)
    for row in history:
        row.pop("sort_key", None)
    return history


def _resolve_manager_tournament(manager: Manager, tournament_name: str, event_date: str):
    same_date = [
        tournament
        for tournament in manager.tournament_database
        if tournament.get_date().strftime("%Y-%m-%d") == event_date
    ]
    normalized_target = normalize_name(tournament_name)
    for tournament in same_date:
        if normalize_name(tournament.name) == normalized_target:
            return tournament
    if same_date:
        best_name, score = fuzzy_best_match(tournament_name, [tournament.name for tournament in same_date])
        if best_name and score >= 0.82:
            return next(tournament for tournament in same_date if tournament.name == best_name)
    return None


def _compute_tournament_insights_from_manager(manager: Manager, manager_tournament) -> dict | None:
    manager_tournament.compute_tournament_results(
        manager.game_database,
        manager.player_database,
        manager.rating_system,
    )

    normal_dist = NormalDist()
    performance_rows = []
    for player in manager_tournament.get_players(
        manager.player_database,
        manager.game_database,
        manager.rating_system,
    ):
        rating_before = player.get_rating_at_date(manager_tournament.get_date(), next=False)
        tournament_performance = manager_tournament.get_player_performance(
            player.id,
            manager.game_database,
            manager.player_database,
            manager.rating_system,
        )
        if tournament_performance is None:
            continue
        performance_rating = tournament_performance["rating_performance"]
        deviation = (rating_before.deviation ** 2 + performance_rating.deviation ** 2) ** 0.5
        normalized_boost = (
            (performance_rating.rating - rating_before.rating) / deviation if deviation else 0.0
        )
        performance_rows.append(
            {
                "name": player.name,
                "start_rating": round_rating_value(rating_before.rating),
                "performance_rating": round_rating_value(performance_rating.rating),
                "probability": max(0.0, min(1.0, 1 - normal_dist.cdf(normalized_boost))),
                "normalized_boost": normalized_boost,
            }
        )
    performance_rows.sort(key=lambda row: row["normalized_boost"], reverse=True)
    above_level = performance_rows[0] if performance_rows else None

    upset_rows = []
    for game in manager.game_database.get_games_per_tournament(manager_tournament.id):
        result = game.get_result()
        if result not in {0, 1} or game.is_forfeit:
            continue
        home_player = manager.player_database[game.home]
        out_player = manager.player_database[game.out]
        game_date = game.get_date()
        home_rating = home_player.get_rating_at_date(game_date, next=True)
        out_rating = out_player.get_rating_at_date(game_date, next=True)
        expected_home = manager.rating_system.compute_expected_score(
            home_player,
            [game],
            manager.player_database,
            game_date,
            next=True,
        )
        if result == 1:
            upset_rows.append(
                {
                    "winner": home_player.name,
                    "winner_rating": round_rating_value(home_rating.rating),
                    "loser": out_player.name,
                    "loser_rating": round_rating_value(out_rating.rating),
                    "result": game.result,
                    "win_probability": max(0.0, min(1.0, expected_home)),
                }
            )
        else:
            upset_rows.append(
                {
                    "winner": out_player.name,
                    "winner_rating": round_rating_value(out_rating.rating),
                    "loser": home_player.name,
                    "loser_rating": round_rating_value(home_rating.rating),
                    "result": game.result,
                    "win_probability": max(0.0, min(1.0, 1 - expected_home)),
                }
            )
    upset_rows.sort(key=lambda row: row["win_probability"])
    biggest_upset = upset_rows[0] if upset_rows else None

    return {
        "above_level": above_level,
        "biggest_upset": biggest_upset,
    }


def _parse_stored_insights(value: str | None) -> dict | None:
    if not value:
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def tournament_insights(tournament) -> dict | None:
    if tournament is None or tournament["status"] != "completed":
        return None
    stored = _parse_stored_insights(tournament["public_insights_json"] if "public_insights_json" in tournament.keys() else None)
    if stored is not None:
        return stored
    manager, stamp = _preferred_manager()
    if manager is None:
        return None
    cache_key = (stamp, tournament["event_date"], normalize_name(tournament["name"]))
    if cache_key in _TOURNAMENT_INSIGHTS_CACHE:
        return _TOURNAMENT_INSIGHTS_CACHE[cache_key]

    manager_tournament = _resolve_manager_tournament(manager, tournament["name"], tournament["event_date"])
    if manager_tournament is None:
        _TOURNAMENT_INSIGHTS_CACHE[cache_key] = None
        return None

    insight = _compute_tournament_insights_from_manager(manager, manager_tournament)
    _TOURNAMENT_INSIGHTS_CACHE[cache_key] = insight
    if insight is not None:
        db = get_db()
        db.execute(
            "UPDATE tournament SET public_insights_json = ? WHERE id = ?",
            (json.dumps(insight, ensure_ascii=True), tournament["id"]),
        )
        db.commit()
    return insight


def _resolve_player_row(db, player_name: str):
    rows = db.execute(
        """
        SELECT
          id,
          name,
          canonical_rating_name,
          historical_rating,
          historical_wins,
          historical_losses,
          historical_draws
        FROM player
        """
    ).fetchall()
    if not rows:
        return None
    target = normalize_name(player_name)
    for row in rows:
        if normalize_name(row["canonical_rating_name"] or row["name"]) == target:
            return row
        if normalize_name(row["name"]) == target:
            return row
    candidate_names = [row["canonical_rating_name"] or row["name"] for row in rows]
    best_name, score = fuzzy_best_match(player_name, candidate_names)
    if not best_name or score < 0.82:
        return None
    return next(row for row in rows if (row["canonical_rating_name"] or row["name"]) == best_name)


def get_player_profile(player_name: str):
    db = get_db()
    row = _resolve_player_row(db, player_name)
    manager, _ = _preferred_manager()
    if manager is not None:
        player = _resolve_manager_player(manager, player_name, db_row=row)
        if player is not None:
            return _profile_from_manager_player(manager, player)
    if row is None:
        return None
    wins = int(row["historical_wins"] or 0)
    losses = int(row["historical_losses"] or 0)
    draws = int(row["historical_draws"] or 0)
    return {
        "name": row["canonical_rating_name"] or row["name"],
        "rating": round_rating_value(row["historical_rating"]),
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "games": wins + losses + draws,
    }


def get_player_history(player_name: str):
    db = get_db()
    row = _resolve_player_row(db, player_name)
    manager, _ = _preferred_manager()
    if manager is not None:
        player = _resolve_manager_player(manager, player_name, db_row=row)
        if player is not None:
            return _history_from_manager_player(manager, player)
    if row is None:
        return []
    history = db.execute(
        """
        SELECT
          t.event_date AS date,
          t.name AS tournament_name,
          p.round_no,
          p.board_no,
          we.imported_name AS white,
          COALESCE(be.imported_name, 'Bye') AS black,
          COALESCE(p.result_code, 'Pending') AS result
        FROM pairing p
        JOIN tournament t ON t.id = p.tournament_id
        JOIN tournament_entry we ON we.id = p.white_entry_id
        LEFT JOIN tournament_entry be ON be.id = p.black_entry_id
        WHERE (we.player_id = ? OR be.player_id = ?)
          AND t.status = 'completed'
          AND p.black_entry_id IS NOT NULL
          AND p.result_code IS NOT NULL
        ORDER BY t.event_date DESC, p.round_no DESC, p.board_no ASC
        """,
        (row["id"], row["id"]),
    ).fetchall()
    return [dict(game) for game in history]


def anonymous_leaderboard_rows():
    manager_path = _manager_path("current")
    export_path = Path(current_app.config["EXPORT_DIR"]) / "anonymous_leaderboard.csv"
    if export_path.exists() and (
        not manager_path.exists() or export_path.stat().st_mtime_ns >= manager_path.stat().st_mtime_ns
    ):
        return _cached_csv_rows(export_path)
    if not manager_path.exists():
        return []
    frame = AnonymousLeaderboard.compute(
        current_manager().player_database,
        current_manager().game_database,
        current_manager().tournament_database,
        save_folder=current_app.config["EXPORT_DIR"],
        file_name="anonymous_leaderboard.csv",
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
