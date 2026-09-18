from __future__ import annotations

import json
import re

from .core import next_waitlist_position, normalize_name, registration_counts


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


def create_team_entry(db, tournament, name: str, emails: list[str], answers: str | None, source: str):
    validate_team_name(db, tournament["id"], name)
    counts = registration_counts(db, tournament["id"])
    capacity = tournament["max_registrations"]
    waitlist_position = (
        next_waitlist_position(db, tournament["id"])
        if capacity and counts["confirmed_count"] >= capacity
        else None
    )
    order = db.execute(
        "SELECT COALESCE(MAX(registration_order), 0) + 1 FROM tournament_entry WHERE tournament_id = ?",
        (tournament["id"],),
    ).fetchone()[0]
    cursor = db.execute(
        """
        INSERT INTO tournament_entry (
          tournament_id, imported_name, imported_email, seed_rating, member_status,
          is_active, registration_source, registration_order, registration_answers_json, waitlist_position
        ) VALUES (?, ?, ?, 1500, 'unknown', ?, ?, ?, ?, ?)
        """,
        (tournament["id"], name, emails[0], int(source == "admin" and waitlist_position is None), source, order, answers, waitlist_position),
    )
    return cursor.lastrowid, waitlist_position


def register_team_members(db, tournament, *, name: str, emails: list[str], solo: bool, answers: str | None, source: str):
    if not name:
        raise ValueError("Your name is required." if solo else "Team name is required.")
    if solo and len(emails) != 1:
        raise ValueError("Solo registration needs exactly one email address.")
    if not solo and len(emails) < 2:
        raise ValueError("A team needs at least two members, or you can register alone.")
    existing_emails = {row["email"].lower() for row in team_members(db, tournament["id"])}
    if existing_emails.intersection(emails):
        raise ValueError("A member with one of these email addresses is already registered for this tournament.")
    entry_id, waitlist_position = (None, None) if solo else create_team_entry(db, tournament, name, emails, answers, source)
    db.executemany(
        """
        INSERT INTO team_member (tournament_id, entry_id, name, email, registration_answers_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        [(tournament["id"], entry_id, name if solo else None, email, answers) for email in emails],
    )
    return entry_id, waitlist_position


def assign_team_members(db, tournament, member_ids: list[int], name: str, existing_entry_id: int | None):
    members = [row for row in team_members(db, tournament["id"]) if row["id"] in member_ids]
    if not member_ids or len(members) != len(set(member_ids)) or any(row["entry_id"] is not None for row in members):
        raise ValueError("Select unassigned members from this tournament.")
    if existing_entry_id is not None:
        entry = db.execute(
            "SELECT * FROM tournament_entry WHERE id = ? AND tournament_id = ? AND player_id IS NULL",
            (existing_entry_id, tournament["id"]),
        ).fetchone()
        if entry is None:
            raise ValueError("Choose a team from this tournament.")
        if db.execute(
            "SELECT 1 FROM pairing WHERE white_entry_id = ? OR black_entry_id = ?",
            (entry["id"], entry["id"]),
        ).fetchone():
            raise ValueError("Members cannot be added to a team that already has pairings.")
        entry_id = entry["id"]
    else:
        if len(members) < 2:
            raise ValueError("Select at least two members for a new team.")
        # Keep each member's answers on their registration, and summarize them on the team.
        combined = {}
        for member in members:
            for answer in json.loads(member["registration_answers_json"] or "[]"):
                combined.setdefault(answer["label"], []).append(f"{member['name'] or member['email']}: {answer['value']}")
        answers = json.dumps([{"label": label, "value": "; ".join(values)} for label, values in combined.items()])
        entry_id, _ = create_team_entry(db, tournament, name, [row["email"] for row in members], answers, "admin")
    db.executemany("UPDATE team_member SET entry_id = ? WHERE id = ?", [(entry_id, row["id"]) for row in members])
    return entry_id
