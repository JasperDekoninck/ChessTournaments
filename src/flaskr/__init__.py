from __future__ import annotations

import os
import secrets
from pathlib import Path

from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix

from . import auth, db, rating_integration, web


def _load_secret_key(instance_path: str, explicit_key: str | None) -> str:
    if explicit_key:
        return explicit_key
    secret_path = Path(instance_path) / ".secret_key"
    if secret_path.exists():
        return secret_path.read_text(encoding="utf-8").strip()
    secret = secrets.token_hex(32)
    secret_path.write_text(secret, encoding="utf-8")
    try:
        os.chmod(secret_path, 0o600)
    except OSError:
        pass
    return secret


def create_app(test_config: dict | None = None) -> Flask:
    project_root = Path(__file__).resolve().parents[2]
    configured_instance_path = os.environ.get("CHESS_INSTANCE_PATH")
    if test_config is not None and test_config.get("INSTANCE_PATH"):
        configured_instance_path = str(Path(test_config["INSTANCE_PATH"]))
    if configured_instance_path:
        configured_instance_path = str(Path(configured_instance_path).expanduser().resolve())
    app = Flask(
        __name__,
        instance_path=configured_instance_path or str(project_root / "instance"),
        instance_relative_config=True,
    )
    instance_root = Path(app.instance_path)
    instance_root.mkdir(parents=True, exist_ok=True)
    app.config.from_mapping(
        SECRET_KEY=_load_secret_key(str(instance_root), os.environ.get("CHESS_SECRET_KEY")),
        DATABASE=str(instance_root / "chess.db"),
        RATING_DATA_DIR=str(instance_root / "rating"),
        EXPORT_DIR=str(instance_root / "exports"),
        PUBLIC_BASE_URL=os.environ.get("CHESS_PUBLIC_BASE_URL", "http://127.0.0.1:5000"),
        MAIL_ENABLED=os.environ.get("CHESS_MAIL_ENABLED", "").lower() in {"1", "true", "yes"},
        MAIL_HOST=os.environ.get("CHESS_MAIL_HOST"),
        MAIL_PORT=int(os.environ.get("CHESS_MAIL_PORT", "587")),
        MAIL_USERNAME=os.environ.get("CHESS_MAIL_USERNAME"),
        MAIL_PASSWORD=os.environ.get("CHESS_MAIL_PASSWORD"),
        MAIL_USE_TLS=os.environ.get("CHESS_MAIL_USE_TLS", "1").lower() in {"1", "true", "yes"},
        MAIL_USE_SSL=os.environ.get("CHESS_MAIL_USE_SSL", "").lower() in {"1", "true", "yes"},
        MAIL_FROM_EMAIL=os.environ.get("CHESS_MAIL_FROM_EMAIL"),
        MAIL_FROM_NAME=os.environ.get("CHESS_MAIL_FROM_NAME", "ETH Chess"),
        MAIL_REPLY_TO=os.environ.get("CHESS_MAIL_REPLY_TO"),
        MAIL_SUPPRESS_SEND=False,
        SESSION_COOKIE_SECURE=os.environ.get("CHESS_SECURE_COOKIES", "").lower() in {"1", "true", "yes"},
        TRUST_PROXY_HEADERS=os.environ.get("CHESS_TRUST_PROXY", "1").lower() in {"1", "true", "yes"},
    )

    if test_config is not None:
        app.config.update(test_config)

    if app.config.get("TRUST_PROXY_HEADERS"):
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

    db.init_app(app)
    auth.init_app(app)
    rating_integration.init_app(app)
    app.register_blueprint(web.bp)

    with app.app_context():
        db.ensure_db()
        auth.ensure_admin_password("admin")
        rating_integration.get_member_since_date(db.get_db())
        rating_integration.sync_member_statuses(db.get_db())

    return app
