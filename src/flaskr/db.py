from __future__ import annotations

import sqlite3
from pathlib import Path

import click
from flask import current_app, g


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


def _table_columns(db, table_name: str) -> set[str]:
    return {row["name"] for row in db.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _add_column_if_missing(db, table_name: str, column_name: str, definition: str):
    if column_name in _table_columns(db, table_name):
        return
    try:
        db.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
    except sqlite3.OperationalError as exc:
        if "duplicate column name" not in str(exc).lower():
            raise


def migrate_db(db):
    tournament_columns = _table_columns(db, "tournament")
    if tournament_columns:
        _add_column_if_missing(db, "tournament", "source_type", "TEXT NOT NULL DEFAULT 'local'")
        _add_column_if_missing(db, "tournament", "source_ref", "TEXT")
        _add_column_if_missing(db, "tournament", "primary_tiebreak_label", "TEXT NOT NULL DEFAULT 'BH'")
        _add_column_if_missing(db, "tournament", "secondary_tiebreak_label", "TEXT NOT NULL DEFAULT 'BH-C1'")
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
        _add_column_if_missing(db, "tournament_entry", "final_rank", "INTEGER")
        _add_column_if_missing(db, "tournament_entry", "final_score", "REAL")
        _add_column_if_missing(db, "tournament_entry", "final_primary_tiebreak", "REAL")
        _add_column_if_missing(db, "tournament_entry", "final_secondary_tiebreak", "REAL")


@click.command("init-db")
def init_db_command():
    init_db()
    click.echo("Initialized the database.")


def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
