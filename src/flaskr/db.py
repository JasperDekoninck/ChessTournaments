from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import click
from flask import current_app, g

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FORBIDDEN_DEFINITION_TOKENS = (";", "--", "/*", "*/", "\x00")


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        db_path = Path(current_app.config["DATABASE"])
        db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        g.db = connection
    return g.db


def close_db(error=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    migrate_db(db)
    schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    db.executescript(schema)
    db.commit()


def ensure_db():
    db_path = Path(current_app.config["DATABASE"])
    db = get_db()
    if not db_path.exists():
        init_db()
        return
    migrate_db(db)
    schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    db.executescript(schema)
    db.commit()


def _quote_identifier(value: str, kind: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"Unsafe {kind}: {value!r}")
    return f'"{value}"'


def _validate_column_definition(definition: str) -> str:
    cleaned = definition.strip()
    if not cleaned:
        raise ValueError("Column definition must not be empty.")
    if any(token in cleaned for token in _FORBIDDEN_DEFINITION_TOKENS):
        raise ValueError(f"Unsafe column definition: {definition!r}")
    return cleaned


def _table_columns(db, table_name: str) -> set[str]:
    table_ref = _quote_identifier(table_name, "table name")
    return {row["name"] for row in db.execute(f"PRAGMA table_info({table_ref})").fetchall()}


def _add_column_if_missing(db, table_name: str, column_name: str, definition: str):
    if column_name in _table_columns(db, table_name):
        return
    table_ref = _quote_identifier(table_name, "table name")
    column_ref = _quote_identifier(column_name, "column name")
    column_definition = _validate_column_definition(definition)
    try:
        db.execute(f"ALTER TABLE {table_ref} ADD COLUMN {column_ref} {column_definition}")
    except sqlite3.OperationalError as exc:
        if "duplicate column name" not in str(exc).lower():
            raise


def migrate_db(db):
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS member_override (
          player_id INTEGER PRIMARY KEY REFERENCES player(id) ON DELETE CASCADE,
          is_member INTEGER NOT NULL,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_member_override_status ON member_override(is_member)")

    tournament_columns = _table_columns(db, "tournament")
    if tournament_columns:
        _add_column_if_missing(db, "tournament", "registration_enabled", "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(db, "tournament", "registration_opens_at", "TEXT")
        _add_column_if_missing(db, "tournament", "registration_form_json", "TEXT")
        _add_column_if_missing(db, "tournament", "event_time", "TEXT")
        _add_column_if_missing(db, "tournament", "venue", "TEXT")
        _add_column_if_missing(db, "tournament", "max_registrations", "INTEGER")
        _add_column_if_missing(db, "tournament", "source_type", "TEXT NOT NULL DEFAULT 'local'")
        _add_column_if_missing(db, "tournament", "source_ref", "TEXT")
        _add_column_if_missing(db, "tournament", "primary_tiebreak_label", "TEXT NOT NULL DEFAULT 'BH'")
        _add_column_if_missing(db, "tournament", "secondary_tiebreak_label", "TEXT NOT NULL DEFAULT 'BH-C1'")
        _add_column_if_missing(db, "tournament", "public_insights_json", "TEXT")
        _add_column_if_missing(db, "tournament", "is_historical", "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(db, "tournament", "is_public", "INTEGER NOT NULL DEFAULT 1")
        _add_column_if_missing(db, "tournament", "is_active_public", "INTEGER NOT NULL DEFAULT 0")
        db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tournament_source
            ON tournament(source_type, source_ref)
            WHERE source_ref IS NOT NULL
            """
        )
        db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_active_public_tournament
            ON tournament(is_active_public)
            WHERE is_active_public = 1
            """
        )

    entry_columns = _table_columns(db, "tournament_entry")
    if entry_columns:
        _add_column_if_missing(db, "tournament_entry", "registration_source", "TEXT")
        _add_column_if_missing(db, "tournament_entry", "registration_order", "INTEGER")
        _add_column_if_missing(db, "tournament_entry", "registration_answers_json", "TEXT")
        _add_column_if_missing(db, "tournament_entry", "waitlist_position", "INTEGER")
        _add_column_if_missing(db, "tournament_entry", "final_rank", "INTEGER")
        _add_column_if_missing(db, "tournament_entry", "final_score", "REAL")
        _add_column_if_missing(db, "tournament_entry", "final_primary_tiebreak", "REAL")
        _add_column_if_missing(db, "tournament_entry", "final_secondary_tiebreak", "REAL")
        db.execute("CREATE INDEX IF NOT EXISTS idx_tournament_entry_player ON tournament_entry(player_id)")

    pairing_columns = _table_columns(db, "pairing")
    if pairing_columns:
        db.execute("CREATE INDEX IF NOT EXISTS idx_pairing_white_entry ON pairing(white_entry_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pairing_black_entry ON pairing(black_entry_id)")


@click.command("init-db")
def init_db_command():
    init_db()
    click.echo("Initialized the database.")


def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
