from __future__ import annotations

import hashlib
import secrets
from datetime import timedelta
from functools import wraps
from pathlib import Path
from time import time
import click
from flask import Response, abort, current_app, flash, g, redirect, request, session, url_for
from flask.cli import with_appcontext
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


def admin_session_version(password_hash: str | None = None) -> str:
    stored_hash = _read_admin_hash_db() if password_hash is None else password_hash
    return hashlib.sha256((stored_hash or "").encode()).hexdigest()


def sign_in_admin(password: str) -> bool:
    stored_hash = _read_admin_hash_db()
    if not stored_hash or not check_password_hash(stored_hash, password):
        return False
    session.clear()
    session["is_admin"] = True
    session["admin_password_version"] = admin_session_version(stored_hash)
    session.permanent = True
    return True


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


def _throttle(scope: str, limit: int) -> Response | None:
    now = int(time())
    key = f"{scope}:{request.remote_addr or 'unknown'}"
    db = get_db()
    db.execute("DELETE FROM request_throttle WHERE expires_at <= ?", (now,))
    cursor = db.execute(
        """
        INSERT INTO request_throttle (key, attempts, expires_at) VALUES (?, 1, ?)
        ON CONFLICT(key) DO UPDATE SET attempts = attempts + 1
        WHERE attempts < ?
        """,
        (key, now + 60, limit),
    )
    allowed = cursor.rowcount == 1
    expires_at = db.execute("SELECT expires_at FROM request_throttle WHERE key = ?", (key,)).fetchone()[0]
    db.commit()
    if not allowed:
        return Response(
            "Too many attempts. Please try again in a minute.", status=429,
            headers={"Retry-After": str(max(1, expires_at - now))}, mimetype="text/plain",
        )
    return None


def init_app(app):
    app.config.setdefault("SESSION_COOKIE_HTTPONLY", True)
    app.config.setdefault("SESSION_COOKIE_SAMESITE", "Lax")
    app.config.setdefault("SESSION_COOKIE_SECURE", False)
    app.config.setdefault("PERMANENT_SESSION_LIFETIME", timedelta(hours=12))

    @app.before_request
    def load_session_flags():
        if session.get("is_admin") and session.get("admin_password_version") != admin_session_version():
            session.clear()
        g.is_admin = bool(session.get("is_admin"))
        _csrf_token()
        if request.method == "POST" and not current_app.config.get("TESTING"):
            token = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token")
            if token != session.get("_csrf_token"):
                abort(400, "Invalid CSRF token.")
        if request.method == "POST" and not g.is_admin:
            if request.endpoint == "web.admin":
                return _throttle("login", 5)
            if request.endpoint == "web.submit_registration":
                return _throttle("registration", 30)

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
    @with_appcontext
    def set_admin_password_command(password: str):
        set_admin_password(password)
        click.echo("Admin password updated.")

    app.cli.add_command(set_admin_password_command)
