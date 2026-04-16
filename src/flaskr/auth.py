from __future__ import annotations

import secrets
from datetime import timedelta
from functools import wraps
from pathlib import Path
import click
from flask import abort, current_app, flash, g, redirect, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from .db import get_db


ADMIN_HASH_KEY = "admin_password_hash"


def _legacy_admin_hash_path() -> Path:
    return Path(current_app.instance_path) / ".admin_password_hash"


def _read_legacy_admin_hash_file() -> str | None:
    path = _legacy_admin_hash_path()
    if not path.exists():
        return None
    value = path.read_text(encoding="utf-8").strip()
    return value or None


def _delete_legacy_admin_hash_file():
    path = _legacy_admin_hash_path()
    if path.exists():
        path.unlink()


def _read_admin_hash_db() -> str | None:
    row = get_db().execute("SELECT value FROM app_config WHERE key = ?", (ADMIN_HASH_KEY,)).fetchone()
    if row is None:
        return None
    return row["value"]


def _write_admin_hash_db(password_hash: str):
    db = get_db()
    db.execute(
        """
        INSERT INTO app_config(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (ADMIN_HASH_KEY, password_hash),
    )
    db.commit()


def _csrf_token() -> str:
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


def hash_password(password: str) -> str:
    return generate_password_hash(password, method="scrypt")


def verify_password(password: str) -> bool:
    stored_hash = _read_admin_hash_db()
    return bool(stored_hash) and check_password_hash(stored_hash, password)


def ensure_admin_password(default_password: str = "admin"):
    if _read_admin_hash_db() is not None:
        _delete_legacy_admin_hash_file()
        return
    legacy_hash = _read_legacy_admin_hash_file()
    if legacy_hash:
        _write_admin_hash_db(legacy_hash)
        _delete_legacy_admin_hash_file()
        return
    _write_admin_hash_db(hash_password(default_password))


def set_admin_password(password: str):
    _write_admin_hash_db(hash_password(password))
    _delete_legacy_admin_hash_file()


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if not session.get("is_admin"):
            flash("Sign in to access the admin area.")
            return redirect(url_for("web.admin"))
        return view(*args, **kwargs)

    return wrapped_view


def init_app(app):
    app.config.setdefault("SESSION_COOKIE_HTTPONLY", True)
    app.config.setdefault("SESSION_COOKIE_SAMESITE", "Lax")
    app.config.setdefault("SESSION_COOKIE_SECURE", False)
    app.config.setdefault("PERMANENT_SESSION_LIFETIME", timedelta(hours=12))

    @app.before_request
    def load_session_flags():
        g.is_admin = bool(session.get("is_admin"))
        _csrf_token()
        if request.method == "POST" and not current_app.config.get("TESTING"):
            token = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token")
            if token != session.get("_csrf_token"):
                abort(400, "Invalid CSRF token.")

    @app.context_processor
    def inject_auth_state():
        return {"csrf_token": _csrf_token, "is_admin": bool(session.get("is_admin"))}

    @app.after_request
    def set_security_headers(response):
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "same-origin")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; img-src 'self' data:; style-src 'self'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'",
        )
        return response

    @click.command("set-admin-password")
    @click.argument("password")
    def set_admin_password_command(password: str):
        set_admin_password(password)
        click.echo("Admin password updated.")

    app.cli.add_command(set_admin_password_command)
