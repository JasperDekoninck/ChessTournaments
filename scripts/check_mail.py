#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import smtplib
import socket
import sys
import traceback
from typing import Iterable

from flaskr import create_app
from flaskr.mailer import send_email


TRUE_VALUES = {"1", "true", "yes", "on"}


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in TRUE_VALUES


def redact_secret(value: str | None) -> str:
    if not value:
        return "(unset)"
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:2]}{'*' * max(4, len(value) - 4)}{value[-2:]}"


def print_config():
    host = os.environ.get("CHESS_MAIL_HOST")
    port = os.environ.get("CHESS_MAIL_PORT", "587")
    username = os.environ.get("CHESS_MAIL_USERNAME")
    from_email = os.environ.get("CHESS_MAIL_FROM_EMAIL")
    reply_to = os.environ.get("CHESS_MAIL_REPLY_TO")
    print("Mail configuration")
    print(f"  CHESS_MAIL_ENABLED={os.environ.get('CHESS_MAIL_ENABLED', '(unset)')}")
    print(f"  CHESS_MAIL_HOST={host or '(unset)'}")
    print(f"  CHESS_MAIL_PORT={port}")
    print(f"  CHESS_MAIL_USE_TLS={os.environ.get('CHESS_MAIL_USE_TLS', '(unset)')}")
    print(f"  CHESS_MAIL_USE_SSL={os.environ.get('CHESS_MAIL_USE_SSL', '(unset)')}")
    print(f"  CHESS_MAIL_USERNAME={username or '(unset)'}")
    print(f"  CHESS_MAIL_PASSWORD={redact_secret(os.environ.get('CHESS_MAIL_PASSWORD'))}")
    print(f"  CHESS_MAIL_FROM_EMAIL={from_email or '(unset)'}")
    print(f"  CHESS_MAIL_REPLY_TO={reply_to or '(unset)'}")
    print()


def resolve_host(host: str) -> list[str]:
    infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    addresses = []
    seen = set()
    for info in infos:
        address = info[4][0]
        if address not in seen:
            addresses.append(address)
            seen.add(address)
    return addresses


def tcp_probe(host: str, port: int):
    with socket.create_connection((host, port), timeout=10):
        return


def smtp_probe(host: str, port: int, username: str | None, password: str | None, use_ssl: bool, use_tls: bool):
    smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    with smtp_cls(host, port, timeout=20) as server:
        if not use_ssl:
            server.ehlo()
        if use_tls and not use_ssl:
            server.starttls()
            server.ehlo()
        if username:
            server.login(username, password or "")
        return server.esmtp_features


def format_features(features: dict) -> str:
    if not features:
        return "(none reported)"
    parts = []
    for key in sorted(features):
        value = features[key]
        parts.append(f"{key}={value.decode() if isinstance(value, bytes) else value}")
    return ", ".join(parts)


def run_app_send(recipient: str) -> tuple[bool, str | None]:
    app = create_app()
    with app.app_context():
        return send_email(
            recipient,
            "ETH Chess test email",
            "This is a test email from the ETH Chess tournament app diagnostics script.",
        )


def fail(step: str):
    print(f"[FAIL] {step}")
    traceback.print_exc()
    sys.exit(1)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Diagnose the current CHESS_MAIL_* configuration and optionally send a test email.",
    )
    parser.add_argument(
        "--recipient",
        help="Send a real app-level test email to this address after SMTP login succeeds.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    host = os.environ.get("CHESS_MAIL_HOST")
    port = int(os.environ.get("CHESS_MAIL_PORT", "587"))
    username = os.environ.get("CHESS_MAIL_USERNAME")
    password = os.environ.get("CHESS_MAIL_PASSWORD")
    use_ssl = env_bool("CHESS_MAIL_USE_SSL")
    use_tls = env_bool("CHESS_MAIL_USE_TLS")

    print_config()

    if not host:
        print("CHESS_MAIL_HOST is not set.")
        return 2

    try:
        addresses = resolve_host(host)
        print(f"[OK] DNS resolved {host} -> {', '.join(addresses)}")
    except Exception:
        fail(f"DNS resolution for {host}")

    try:
        tcp_probe(host, port)
        print(f"[OK] TCP connection to {host}:{port}")
    except Exception:
        fail(f"TCP connection to {host}:{port}")

    try:
        features = smtp_probe(host, port, username, password, use_ssl=use_ssl, use_tls=use_tls)
        print(f"[OK] SMTP handshake/login on {host}:{port}")
        print(f"  ESMTP features: {format_features(features)}")
    except Exception:
        fail(f"SMTP handshake/login on {host}:{port}")

    if args.recipient:
        try:
            sent, error = run_app_send(args.recipient)
            if not sent:
                print(f"[FAIL] App-level send_email returned error: {error}")
                return 1
            print(f"[OK] App-level test email sent to {args.recipient}")
        except Exception:
            fail(f"app-level test email to {args.recipient}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
