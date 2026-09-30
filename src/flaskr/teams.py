from __future__ import annotations

import json
import re

from .core import next_waitlist_position, normalize_name, registration_counts, round_rating_value


def parse_team_size(value: str) -> int:
    try:
        size = int(value)
    except (ValueError, TypeError):
        raise ValueError("Choose a whole number of team members between 2 and 100.") from None
    if not 2 <= size <= 100:
        raise ValueError("Choose a whole number of team members between 2 and 100.")
    return size


def parse_elo(value: str | None, *, required: bool = False) -> int | None:
    if not value or not value.strip():
        if required:
            raise ValueError("Average Elo is required for team registration.")
        return None
    try:
        rating = int(value)
    except ValueError:
        raise ValueError("Enter Elo as a whole number between 0 and 4000.") from None
    if not 0 <= rating <= 4000:
        raise ValueError("Enter Elo as a whole number between 0 and 4000.")
    return rating


def parse_member_emails(value: str) -> list[str]:
    emails = [email.strip().lower() for email in re.split(r"[,;\s]+", value.strip()) if email.strip()]
    if not emails or any(not re.fullmatch(r"[^@\s<>]+@[^@\s<>]+\.[^@\s<>]+", email) for email in emails):
        raise ValueError("Enter a valid email address for every member.")
    if len(set(emails)) != len(emails):
        raise ValueError("Each team member needs a different email address.")
    return emails


def team_members(db, tournament_id: int):
    return db.execute(
        "SELECT * FROM team_member WHERE tournament_id = ? ORDER BY id",
        (tournament_id,),
    ).fetchall()


def validate_team_name(db, tournament_id: int, name: str, entry_id: int | None = None):
    if not name.strip():
        raise ValueError("Team name is required.")
    entries = db.execute(
        "SELECT id, imported_name FROM tournament_entry WHERE tournament_id = ?",
        (tournament_id,),
    ).fetchall()
    if any(row["id"] != entry_id and normalize_name(row["imported_name"]) == normalize_name(name) for row in entries):
        raise ValueError("A team with this name is already registered.")


def registration_waitlist_position(db, tournament, *, solo: bool = False):
    counts = registration_counts(db, tournament["id"])
    capacity = tournament["max_registrations"]
    # Convert team equivalents back to whole people to avoid fractional rounding
    # errors for team sizes such as three. Existing waiters are never bypassed.
    confirmed_places = round(counts["confirmed_count"] * tournament["team_size"])
    requested_places = 1 if solo else tournament["team_size"]
    if counts["waitlist_count"] or (
        capacity is not None and confirmed_places + requested_places > capacity * tournament["team_size"]
    ):
        return next_waitlist_position(db, tournament["id"])
    return None


def create_team_entry(db, tournament, name: str, emails: list[str | None], answers: str | None, source: str, declared_rating: int | None, *, already_confirmed: bool = False):
    validate_team_name(db, tournament["id"], name)
    waitlist_position = None if already_confirmed else registration_waitlist_position(db, tournament)
    order = db.execute(
        "SELECT COALESCE(MAX(registration_order), 0) + 1 FROM tournament_entry WHERE tournament_id = ?",
        (tournament["id"],),
    ).fetchone()[0]
    cursor = db.execute(
        """
        INSERT INTO tournament_entry (
          tournament_id, imported_name, imported_email, declared_rating, seed_rating, member_status,
          is_active, registration_source, registration_order, registration_answers_json, waitlist_position
        ) VALUES (?, ?, ?, ?, ?, 'unknown', ?, ?, ?, ?, ?)
        """,
        (tournament["id"], name, next((email for email in emails if email), None), declared_rating, declared_rating if declared_rating is not None else 1500,
         int(source == "admin" and waitlist_position is None), source, order, answers, waitlist_position),
    )
    return cursor.lastrowid, waitlist_position


def register_team_members(db, tournament, *, name: str, emails: list[str], solo: bool, answers: str | None, source: str, declared_rating: int | None):
    if not name:
        raise ValueError("Your name is required." if solo else "Team name is required.")
    if source == "admin":
        required_members = 1 if solo else tournament["team_size"]
        if len(emails) > required_members:
            raise ValueError(f"Enter at most {required_members} member email addresses.")
        # Keep a record for every member even when the admin has no contact details.
        emails = [*emails, *([None] * (required_members - len(emails)))]
    if solo and len(emails) != 1:
        raise ValueError("Solo registration needs exactly one email address.")
    if not solo and len(emails) != tournament["team_size"]:
        raise ValueError(f"A team needs exactly {tournament['team_size']} member email addresses, or you can register alone.")
    existing_emails = {row["email"].lower() for row in team_members(db, tournament["id"]) if row["email"]}
    if existing_emails.intersection(emails):
        raise ValueError("A member with one of these email addresses is already registered for this tournament.")
    entry_id, waitlist_position = (None, registration_waitlist_position(db, tournament, solo=True)) if solo else create_team_entry(db, tournament, name, emails, answers, source, declared_rating)
    db.executemany(
        """
        INSERT INTO team_member (tournament_id, entry_id, name, email, registration_answers_json, declared_rating, waitlist_position)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [(tournament["id"], entry_id, name if solo else None, email, answers, declared_rating if solo else None, waitlist_position if solo else None) for email in emails],
    )
    return entry_id, waitlist_position


def assign_team_members(db, tournament, member_ids: list[int], name: str, existing_entry_id: int | None):
    all_members = team_members(db, tournament["id"])
    members = [row for row in all_members if row["id"] in member_ids]
    if not member_ids or len(members) != len(set(member_ids)) or any(row["entry_id"] is not None for row in members):
        raise ValueError("Select unassigned members from this tournament.")
    if any(row["waitlist_position"] is not None for row in members):
        raise ValueError("Confirm waiting-list players before assigning them to a team.")
    if existing_entry_id is not None:
        entry = db.execute(
            "SELECT * FROM tournament_entry WHERE id = ? AND tournament_id = ? AND player_id IS NULL",
            (existing_entry_id, tournament["id"]),
        ).fetchone()
        if entry is None:
            raise ValueError("Choose a team from this tournament.")
        if entry["waitlist_position"] is not None:
            raise ValueError("Confirm the waiting-list team before assigning members to it.")
        if db.execute(
            "SELECT 1 FROM pairing WHERE white_entry_id = ? OR black_entry_id = ?",
            (entry["id"], entry["id"]),
        ).fetchone():
            raise ValueError("Members cannot be added to a team that already has pairings.")
        existing_members = [row for row in all_members if row["entry_id"] == entry["id"]]
        if len(existing_members) + len(members) != tournament["team_size"]:
            raise ValueError(f"A team must have exactly {tournament['team_size']} members after assignment.")
        entry_id = entry["id"]
        ratings = [row["declared_rating"] for row in existing_members + members]
        if existing_members and entry["declared_rating"] is not None:
            ratings = [entry["declared_rating"]] * len(existing_members) + [row["declared_rating"] for row in members]
        average = round_rating_value(sum(ratings) / len(ratings)) if all(rating is not None for rating in ratings) else None
        db.execute(
            "UPDATE tournament_entry SET declared_rating = ?, seed_rating = ? WHERE id = ?",
            (average, average if average is not None else 1500, entry_id),
        )
    else:
        if len(members) != tournament["team_size"]:
            raise ValueError(f"Select exactly {tournament['team_size']} members for a new team.")
        # Keep each member's answers on their registration, and summarize them on the team.
        combined = {}
        for member in members:
            for answer in json.loads(member["registration_answers_json"] or "[]"):
                combined.setdefault(answer["label"], []).append(f"{member['name'] or member['email']}: {answer['value']}")
        answers = json.dumps([{"label": label, "value": "; ".join(values)} for label, values in combined.items()])
        ratings = [member["declared_rating"] for member in members]
        average = round_rating_value(sum(ratings) / len(ratings)) if all(rating is not None for rating in ratings) else None
        # These members already have confirmed places; assigning a team adds no new registrations.
        entry_id, _ = create_team_entry(
            db, tournament, name, [row["email"] for row in members], answers, "admin", average,
            already_confirmed=True,
        )
    db.executemany("UPDATE team_member SET entry_id = ? WHERE id = ?", [(entry_id, row["id"]) for row in members])
    return entry_id


def team_created_from_solos(entry, members) -> bool:
    # Solo registrations retain their individual names after assignment; members
    # registered together as a team have no individual name in this data model.
    return (
        entry["player_id"] is None
        and entry["registration_source"] == "admin"
        and bool(members)
        and all(member["name"] for member in members)
    )


def unpair_team_members(db, tournament, entry_id: int):
    if not tournament["is_team"] or tournament["is_historical"] or tournament["status"] == "completed":
        raise ValueError("Teams can only be unpaired in an unfinished team tournament.")
    entry = db.execute(
        "SELECT * FROM tournament_entry WHERE id = ? AND tournament_id = ?",
        (entry_id, tournament["id"]),
    ).fetchone()
    if entry is None:
        raise ValueError("Choose a team from this tournament.")
    members = db.execute(
        "SELECT * FROM team_member WHERE entry_id = ? AND tournament_id = ?",
        (entry_id, tournament["id"]),
    ).fetchall()
    if not team_created_from_solos(entry, members):
        raise ValueError("Only teams created from individual registrations can be unpaired.")
    if entry["waitlist_position"] is not None or any(member["waitlist_position"] is not None for member in members):
        raise ValueError("Confirm waiting-list registrations before unpairing their team.")
    if len(members) != tournament["team_size"]:
        raise ValueError("The team must have its full set of members before it can be unpaired.")
    if db.execute(
        "SELECT 1 FROM pairing WHERE white_entry_id = ? OR black_entry_id = ?",
        (entry_id, entry_id),
    ).fetchone():
        raise ValueError("This team already has game pairings and cannot be unpaired.")
    # Detach first: deleting an entry otherwise cascades to its registrations.
    # The caller holds a write transaction so both steps succeed together.
    db.execute(
        "UPDATE team_member SET entry_id = NULL WHERE entry_id = ? AND tournament_id = ?",
        (entry_id, tournament["id"]),
    )
    db.execute("DELETE FROM tournament_entry WHERE id = ? AND tournament_id = ?", (entry_id, tournament["id"]))
    return entry["imported_name"]
