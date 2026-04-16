from __future__ import annotations

import csv
import io
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from math import ceil


VALID_RESULTS = {"1-0", "0-1", "1/2-1/2", "BYE"}


def normalize_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name or "")
    ascii_only = "".join(char for char in normalized if not unicodedata.combining(char))
    tokens = re.findall(r"[a-z0-9]+", ascii_only.lower())
    return " ".join(sorted(tokens))


def slugify(value: str) -> str:
    base = normalize_name(value).replace(" ", "-")
    return base or "tournament"


def parse_submitted_time(value: str | None) -> str | None:
    if not value:
        return None
    for fmt in ("%b %d, %Y @ %I:%M %p", "%b %d, %Y @ %H:%M"):
        try:
            return datetime.strptime(value, fmt).isoformat()
        except ValueError:
            continue
    return value


def parse_int(value: str | None, default: int | None = None) -> int | None:
    if value is None:
        return default
    cleaned = str(value).strip()
    if not cleaned:
        return default
    try:
        return int(float(cleaned))
    except ValueError:
        return default


def fuzzy_best_match(name: str, candidates: list[str]) -> tuple[str | None, float]:
    target = normalize_name(name)
    if not target:
        return None, 0.0
    best_name = None
    best_score = 0.0
    for candidate in candidates:
        score = SequenceMatcher(None, target, normalize_name(candidate)).ratio()
        if score > best_score:
            best_score = score
            best_name = candidate
    return best_name, best_score


def parse_registration_csv(file_storage) -> list[dict]:
    if file_storage is None or not file_storage.filename:
        return []
    content = file_storage.read()
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for row in reader:
        select_value = (row.get("Select") or "").strip()
        if select_value and select_value.lower() != "registered":
            continue
        rows.append(
            {
                "submitted_at": parse_submitted_time(row.get("Submission Time")),
                "name": (row.get("Full Name") or "").strip(),
                "email": (row.get("Email Address") or "").strip() or None,
                "declared_rating": parse_int(row.get("Rating (Fide, Lichess, chess.com, ...)"), default=None),
            }
        )
    return [row for row in rows if row["name"]]


def fetch_tournaments(db):
    return db.execute(
        """
        SELECT
          id, name, slug, event_date, rounds_planned, status, registration_csv_name,
          source_type, source_ref, primary_tiebreak_label, secondary_tiebreak_label,
          is_historical, is_public, is_active_public
        FROM tournament
        ORDER BY is_active_public DESC, event_date DESC, id DESC
        """
    ).fetchall()


def fetch_public_tournaments(db):
    return db.execute(
        """
        SELECT
          id, name, slug, event_date, rounds_planned, status, registration_csv_name,
          source_type, source_ref, primary_tiebreak_label, secondary_tiebreak_label,
          is_historical, is_public, is_active_public
        FROM tournament
        WHERE is_public = 1
        ORDER BY is_active_public DESC, event_date DESC, id DESC
        """
    ).fetchall()


def fetch_active_tournament(db):
    return db.execute(
        """
        SELECT
          id, name, slug, event_date, rounds_planned, status, registration_csv_name,
          source_type, source_ref, primary_tiebreak_label, secondary_tiebreak_label,
          is_historical, is_public, is_active_public
        FROM tournament
        WHERE is_active_public = 1
        ORDER BY event_date DESC, id DESC
        LIMIT 1
        """
    ).fetchone()


def fetch_tournament_by_slug(db, slug: str):
    return db.execute(
        """
        SELECT
          id, name, slug, event_date, rounds_planned, status, registration_csv_name,
          source_type, source_ref, primary_tiebreak_label, secondary_tiebreak_label,
          is_historical, is_public, is_active_public
        FROM tournament
        WHERE slug = ?
        """,
        (slug,),
    ).fetchone()


def unique_slug(db, proposed: str) -> str:
    candidate = proposed
    index = 2
    while db.execute("SELECT 1 FROM tournament WHERE slug = ?", (candidate,)).fetchone():
        candidate = f"{proposed}-{index}"
        index += 1
    return candidate


def ensure_round_status_rows(db, tournament_id: int, rounds_planned: int):
    entries = db.execute(
        "SELECT id FROM tournament_entry WHERE tournament_id = ?",
        (tournament_id,),
    ).fetchall()
    existing = {
        (row["entry_id"], row["round_no"])
        for row in db.execute(
            """
            SELECT entry_id, round_no
            FROM entry_round_status
            WHERE entry_id IN (
                SELECT id FROM tournament_entry WHERE tournament_id = ?
            )
            """,
            (tournament_id,),
        ).fetchall()
    }
    inserts = []
    for entry in entries:
        for round_no in range(1, rounds_planned + 1):
            key = (entry["id"], round_no)
            if key not in existing:
                inserts.append((entry["id"], round_no, 1))
    if inserts:
        db.executemany(
            "INSERT INTO entry_round_status (entry_id, round_no, is_available) VALUES (?, ?, ?)",
            inserts,
        )
        db.commit()


def fetch_entries(db, tournament_id: int):
    return db.execute(
        """
        SELECT
          e.id,
          e.tournament_id,
          e.player_id,
          e.imported_name,
          e.imported_email,
          e.submitted_at,
          e.declared_rating,
          e.seed_rating,
          e.member_status,
          e.is_active,
          e.final_rank,
          e.final_score,
          e.final_primary_tiebreak,
          e.final_secondary_tiebreak,
          p.name AS player_name,
          p.email AS player_email,
          p.canonical_rating_name,
          p.historical_rating,
          p.rating_deviation,
          p.historical_wins,
          p.historical_losses,
          p.historical_draws
        FROM tournament_entry e
        JOIN player p ON p.id = e.player_id
        WHERE e.tournament_id = ?
        ORDER BY COALESCE(e.final_rank, 999999), e.seed_rating DESC, e.imported_name COLLATE NOCASE ASC
        """,
        (tournament_id,),
    ).fetchall()


def fetch_availability(db, tournament_id: int) -> dict[int, dict[int, bool]]:
    rows = db.execute(
        """
        SELECT ers.entry_id, ers.round_no, ers.is_available
        FROM entry_round_status ers
        JOIN tournament_entry e ON e.id = ers.entry_id
        WHERE e.tournament_id = ?
        """,
        (tournament_id,),
    ).fetchall()
    matrix: dict[int, dict[int, bool]] = defaultdict(dict)
    for row in rows:
        matrix[row["entry_id"]][row["round_no"]] = bool(row["is_available"])
    return matrix


def fetch_pairings(db, tournament_id: int, round_no: int | None = None):
    if round_no is None:
        return db.execute(
            """
            SELECT
              p.*,
              we.imported_name AS white_name,
              be.imported_name AS black_name
            FROM pairing p
            LEFT JOIN tournament_entry we ON we.id = p.white_entry_id
            LEFT JOIN tournament_entry be ON be.id = p.black_entry_id
            WHERE p.tournament_id = ?
            ORDER BY p.round_no ASC, p.board_no ASC
            """,
            (tournament_id,),
        ).fetchall()
    return db.execute(
        """
        SELECT
          p.*,
          we.imported_name AS white_name,
          be.imported_name AS black_name
        FROM pairing p
        LEFT JOIN tournament_entry we ON we.id = p.white_entry_id
        LEFT JOIN tournament_entry be ON be.id = p.black_entry_id
        WHERE p.tournament_id = ? AND p.round_no = ?
        ORDER BY p.board_no ASC
        """,
        (tournament_id, round_no),
    ).fetchall()


def had_bye_before(pairings, entry_id: int, round_no: int) -> bool:
    for pairing in pairings:
        if pairing["round_no"] >= round_no:
            continue
        if pairing["black_entry_id"] is None and pairing["white_entry_id"] == entry_id:
            return True
    return False


def normalize_result_code(result_code: str | None) -> str | None:
    if result_code is None:
        return None
    cleaned = str(result_code).strip()
    if not cleaned:
        return None
    compact = cleaned.replace(" ", "").replace(",", ".")
    compact = compact.replace("0.5-0.5", "1/2-1/2").replace("0.5", "1/2")
    if compact.lower() == "bye":
        return "BYE"
    return compact.upper()


def _result_tuple(result_code: str | None) -> tuple[float, float] | None:
    normalized = normalize_result_code(result_code)
    if normalized is None:
        return None
    if normalized == "BYE":
        return 1.0, 0.0
    if "-" not in normalized:
        return None
    left, right = normalized.split("-", 1)

    def parse_part(value: str) -> float | None:
        value = value.replace("F", "")
        if value == "1/2":
            return 0.5
        try:
            return float(value)
        except ValueError:
            return None

    left_score = parse_part(left)
    right_score = parse_part(right)
    if left_score is None or right_score is None:
        return None
    return left_score, right_score


def _result_points(result_code: str, side: str) -> float:
    result = _result_tuple(result_code)
    if result is None:
        return 0.0
    white_points, black_points = result
    return white_points if side == "white" else black_points


def latest_paired_round(db, tournament_id: int) -> int | None:
    row = db.execute(
        "SELECT MAX(round_no) AS max_round FROM pairing WHERE tournament_id = ?",
        (tournament_id,),
    ).fetchone()
    return row["max_round"] if row and row["max_round"] is not None else None


def public_rounds(db, tournament_id: int) -> list[int]:
    return [
        row["round_no"]
        for row in db.execute(
            "SELECT DISTINCT round_no FROM pairing WHERE tournament_id = ? ORDER BY round_no ASC",
            (tournament_id,),
        ).fetchall()
    ]


def set_active_tournament(db, tournament_id: int):
    db.execute("UPDATE tournament SET is_active_public = 0")
    db.execute("UPDATE tournament SET is_active_public = 1 WHERE id = ?", (tournament_id,))
    db.commit()


def compute_standings(db, tournament_id: int, through_round: int | None = None) -> list[dict]:
    tournament = db.execute(
        """
        SELECT is_historical, primary_tiebreak_label, secondary_tiebreak_label
        FROM tournament
        WHERE id = ?
        """,
        (tournament_id,),
    ).fetchone()
    entries = fetch_entries(db, tournament_id)
    pairings = fetch_pairings(db, tournament_id)

    if through_round is None and tournament and tournament["is_historical"]:
        historical_rows = [
            entry
            for entry in entries
            if entry["final_rank"] is not None and entry["final_score"] is not None
        ]
        if historical_rows:
            ordered = []
            for entry in sorted(historical_rows, key=lambda row: (row["final_rank"], row["imported_name"].lower())):
                ordered.append(
                    {
                        "entry_id": entry["id"],
                        "name": entry["imported_name"],
                        "seed_rating": entry["seed_rating"],
                        "member_status": entry["member_status"],
                        "is_active": bool(entry["is_active"]),
                        "score": float(entry["final_score"]),
                        "wins": 0,
                        "losses": 0,
                        "draws": 0,
                        "white_games": 0,
                        "black_games": 0,
                        "colors": [],
                        "opponents": [],
                        "opponent_ids": set(),
                        "had_bye": False,
                        "bh": float(entry["final_primary_tiebreak"] or 0.0),
                        "bh_c1": float(entry["final_secondary_tiebreak"] or 0.0),
                        "rank": int(entry["final_rank"]),
                    }
                )
            return ordered

    rows = {}
    for entry in entries:
        rows[entry["id"]] = {
            "entry_id": entry["id"],
            "name": entry["imported_name"],
            "seed_rating": entry["seed_rating"],
            "member_status": entry["member_status"],
            "is_active": bool(entry["is_active"]),
            "score": 0.0,
            "wins": 0,
            "losses": 0,
            "draws": 0,
            "white_games": 0,
            "black_games": 0,
            "colors": [],
            "opponents": [],
            "opponent_ids": set(),
            "had_bye": False,
        }

    for pairing in pairings:
        if through_round is not None and pairing["round_no"] > through_round:
            continue
        result_code = pairing["result_code"]
        white_id = pairing["white_entry_id"]
        black_id = pairing["black_entry_id"]
        if white_id is None:
            continue
        if black_id is None:
            rows[white_id]["had_bye"] = True
            white_points, _ = _result_tuple(result_code) or (1.0, 0.0)
            rows[white_id]["score"] += white_points
            continue
        rows[white_id]["white_games"] += 1
        rows[black_id]["black_games"] += 1
        rows[white_id]["colors"].append("W")
        rows[black_id]["colors"].append("B")
        rows[white_id]["opponents"].append(black_id)
        rows[black_id]["opponents"].append(white_id)
        rows[white_id]["opponent_ids"].add(black_id)
        rows[black_id]["opponent_ids"].add(white_id)
        result = _result_tuple(result_code)
        if result is None:
            continue
        white_points, black_points = result
        rows[white_id]["score"] += white_points
        rows[black_id]["score"] += black_points
        if white_points > black_points:
            rows[white_id]["wins"] += 1
            rows[black_id]["losses"] += 1
        elif black_points > white_points:
            rows[black_id]["wins"] += 1
            rows[white_id]["losses"] += 1
        elif white_points > 0:
            rows[white_id]["draws"] += 1
            rows[black_id]["draws"] += 1

    for row in rows.values():
        opponent_scores = [rows[opponent_id]["score"] for opponent_id in row["opponents"]]
        row["bh"] = round(sum(opponent_scores), 2)
        row["bh_c1"] = round(row["bh"] - min(opponent_scores), 2) if opponent_scores else 0.0

    ordered = sorted(
        rows.values(),
        key=lambda row: (-row["score"], -row["bh"], -row["bh_c1"], -row["seed_rating"], row["name"].lower()),
    )
    for index, row in enumerate(ordered, start=1):
        row["rank"] = index
    return ordered


def _desired_color(row: dict) -> str | None:
    balance = row["white_games"] - row["black_games"]
    if balance > 0:
        return "black"
    if balance < 0:
        return "white"
    if row["colors"]:
        return "black" if row["colors"][-1] == "W" else "white"
    return None


def _assignment_penalty(row: dict, color: str) -> float:
    next_balance = row["white_games"] - row["black_games"] + (1 if color == "white" else -1)
    penalty = abs(next_balance)
    desired = _desired_color(row)
    if desired and desired != color:
        penalty += 2
    marker = "W" if color == "white" else "B"
    if len(row["colors"]) >= 2 and row["colors"][-1] == marker and row["colors"][-2] == marker:
        penalty += 8
    elif row["colors"] and row["colors"][-1] == marker:
        penalty += 1
    return penalty


def _pair_penalty(a: dict, b: dict) -> float:
    repeat_penalty = 100 if b["entry_id"] in a["opponent_ids"] else 0
    score_penalty = abs(a["score"] - b["score"]) * 25
    seed_penalty = abs(a["seed_rating"] - b["seed_rating"]) / 100
    return repeat_penalty + score_penalty + seed_penalty


def _choose_colors(a: dict, b: dict) -> tuple[dict, dict]:
    white_first = _assignment_penalty(a, "white") + _assignment_penalty(b, "black")
    black_first = _assignment_penalty(a, "black") + _assignment_penalty(b, "white")
    if white_first <= black_first:
        return a, b
    return b, a


def _pair_group(group: list[dict]) -> list[tuple[dict, dict]]:
    remaining = list(group)
    pairs = []
    while remaining:
        current = remaining.pop(0)
        opponent = min(remaining, key=lambda candidate: _pair_penalty(current, candidate))
        remaining.remove(opponent)
        white, black = _choose_colors(current, opponent)
        pairs.append((white, black))
    return pairs


def generate_swiss_pairings(db, tournament_id: int, round_no: int) -> list[dict]:
    tournament = db.execute(
        "SELECT id, rounds_planned FROM tournament WHERE id = ?",
        (tournament_id,),
    ).fetchone()
    ensure_round_status_rows(db, tournament_id, tournament["rounds_planned"])
    standings = compute_standings(db, tournament_id, through_round=round_no - 1)
    availability = fetch_availability(db, tournament_id)
    active = [
        row
        for row in standings
        if row["is_active"] and availability.get(row["entry_id"], {}).get(round_no, True)
    ]
    if len(active) < 2:
        return []

    historical_pairings = fetch_pairings(db, tournament_id)
    bye_entry = None
    if len(active) % 2 == 1:
        bye_entry = sorted(
            active,
            key=lambda row: (
                had_bye_before(historical_pairings, row["entry_id"], round_no),
                row["score"],
                row["seed_rating"],
                row["name"].lower(),
            ),
        )[0]
        active = [row for row in active if row["entry_id"] != bye_entry["entry_id"]]

    boards: list[dict] = []
    if round_no == 1:
        active.sort(key=lambda row: (-row["seed_rating"], row["name"].lower()))
        midpoint = len(active) // 2
        top = active[:midpoint]
        bottom = active[midpoint:]
        for index, (a, b) in enumerate(zip(top, bottom), start=1):
            white, black = (a, b) if index % 2 else (b, a)
            boards.append({"board_no": index, "white_entry_id": white["entry_id"], "black_entry_id": black["entry_id"]})
    else:
        groups: dict[float, list[dict]] = defaultdict(list)
        for row in active:
            groups[row["score"]].append(row)
        carry = None
        board_no = 1
        for score in sorted(groups.keys(), reverse=True):
            group = sorted(groups[score], key=lambda row: (-row["seed_rating"], row["name"].lower()))
            if carry is not None:
                group.insert(0, carry)
                carry = None
            if len(group) % 2 == 1:
                carry = sorted(group, key=lambda row: (row["seed_rating"], row["name"].lower()))[0]
                group = [row for row in group if row["entry_id"] != carry["entry_id"]]
            for white, black in _pair_group(group):
                boards.append(
                    {
                        "board_no": board_no,
                        "white_entry_id": white["entry_id"],
                        "black_entry_id": black["entry_id"],
                    }
                )
                board_no += 1
        if carry is not None:
            bye_entry = carry

    if bye_entry is not None:
        boards.append({"board_no": len(boards) + 1, "white_entry_id": bye_entry["entry_id"], "black_entry_id": None})
    return boards


def replace_round_pairings(db, tournament_id: int, round_no: int, boards: list[dict], manual_override: bool = False):
    db.execute(
        "DELETE FROM pairing WHERE tournament_id = ? AND round_no = ?",
        (tournament_id, round_no),
    )
    for board in boards:
        result_code = board.get("result_code")
        if board.get("black_entry_id") is None:
            result_code = result_code or "BYE"
        result_code = normalize_result_code(result_code)
        db.execute(
            """
            INSERT INTO pairing (
              tournament_id, round_no, board_no, white_entry_id, black_entry_id, result_code, manual_override
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tournament_id,
                round_no,
                board["board_no"],
                board.get("white_entry_id"),
                board.get("black_entry_id"),
                result_code,
                1 if manual_override else 0,
            ),
        )
    db.commit()


def parse_manual_pairing_form(form, active_entry_ids: set[int], board_count: int) -> list[dict]:
    seen = set()
    boards = []
    for board_no in range(1, board_count + 1):
        white_id = parse_int(form.get(f"white_{board_no}"))
        black_id = parse_int(form.get(f"black_{board_no}"))
        result_code = normalize_result_code((form.get(f"result_{board_no}") or "").strip() or None)
        if white_id is None:
            continue
        if white_id not in active_entry_ids:
            raise ValueError("Manual pairing references a player who is not available this round.")
        if white_id in seen:
            raise ValueError("A player was assigned to multiple boards.")
        seen.add(white_id)
        if black_id is not None:
            if black_id not in active_entry_ids:
                raise ValueError("Manual pairing references a player who is not available this round.")
            if black_id in seen:
                raise ValueError("A player was assigned to multiple boards.")
            seen.add(black_id)
        boards.append(
            {
                "board_no": board_no,
                "white_entry_id": white_id,
                "black_entry_id": black_id,
                "result_code": result_code,
            }
        )
    return boards


@dataclass
class MatchResult:
    canonical_name: str
    historical_rating: float | None
    rating_deviation: float | None
    wins: int
    losses: int
    draws: int
    member_status: str


def upsert_player(db, row: dict, match: MatchResult):
    normalized = normalize_name(row["name"])
    player = None
    if row["email"]:
        player = db.execute("SELECT * FROM player WHERE email = ?", (row["email"],)).fetchone()
    if player is None:
        player = db.execute(
            "SELECT * FROM player WHERE normalized_name = ? ORDER BY id ASC LIMIT 1",
            (normalized,),
        ).fetchone()
    if player is None:
        cursor = db.execute(
            """
            INSERT INTO player (
              name, normalized_name, email, canonical_rating_name, member_status,
              historical_rating, rating_deviation, historical_wins, historical_losses, historical_draws
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["name"],
                normalized,
                row["email"],
                match.canonical_name,
                match.member_status,
                match.historical_rating,
                match.rating_deviation,
                match.wins,
                match.losses,
                match.draws,
            ),
        )
        return cursor.lastrowid
    db.execute(
        """
        UPDATE player
        SET name = ?, normalized_name = ?, email = COALESCE(?, email),
            canonical_rating_name = ?, member_status = ?, historical_rating = ?,
            rating_deviation = ?, historical_wins = ?, historical_losses = ?, historical_draws = ?
        WHERE id = ?
        """,
        (
            row["name"],
            normalized,
            row["email"],
            match.canonical_name,
            match.member_status,
            match.historical_rating,
            match.rating_deviation,
            match.wins,
            match.losses,
            match.draws,
            player["id"],
        ),
    )
    return player["id"]


def attach_entries_to_tournament(db, tournament_id: int, imported_rows: list[dict], matcher):
    tournament = db.execute(
        "SELECT rounds_planned FROM tournament WHERE id = ?",
        (tournament_id,),
    ).fetchone()
    for row in imported_rows:
        match = matcher(row["name"], row.get("declared_rating"))
        player_id = upsert_player(db, row, match)
        seed_rating = int(round(match.historical_rating or row.get("declared_rating") or 1500))
        db.execute(
            """
            INSERT INTO tournament_entry (
              tournament_id, player_id, imported_name, imported_email, submitted_at,
              declared_rating, seed_rating, member_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tournament_id, player_id) DO UPDATE SET
              imported_name = excluded.imported_name,
              imported_email = excluded.imported_email,
              submitted_at = excluded.submitted_at,
              declared_rating = excluded.declared_rating,
              seed_rating = excluded.seed_rating,
              member_status = excluded.member_status,
              is_active = 1
            """,
            (
                tournament_id,
                player_id,
                row["name"],
                row["email"],
                row["submitted_at"],
                row["declared_rating"],
                seed_rating,
                match.member_status,
            ),
        )
    db.commit()
    ensure_round_status_rows(db, tournament_id, tournament["rounds_planned"])


def pairings_complete(db, tournament_id: int, round_no: int) -> bool:
    rows = fetch_pairings(db, tournament_id, round_no)
    return bool(rows) and all(normalize_result_code(row["result_code"]) is not None for row in rows)


def next_round_to_pair(db, tournament_id: int, rounds_planned: int) -> int:
    for round_no in range(1, rounds_planned + 1):
        if not fetch_pairings(db, tournament_id, round_no):
            return round_no
    return rounds_planned


def serialize_csv(frame) -> str:
    buffer = io.StringIO()
    frame.to_csv(buffer, index=False)
    return buffer.getvalue()
