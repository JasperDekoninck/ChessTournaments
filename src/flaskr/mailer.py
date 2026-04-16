from __future__ import annotations

import smtplib
from email.message import EmailMessage
from email.utils import formataddr
from typing import Any

from flask import current_app, url_for


MEMBERSHIP_NOTE = (
    "ETH Chess membership is valid for one academic year and costs CHF 5, "
    "payable at the start of the tournament if you are not a member yet."
)


def _mail_enabled() -> bool:
    config = current_app.config
    return bool(
        config.get("MAIL_ENABLED")
        and config.get("MAIL_HOST")
        and config.get("MAIL_FROM_EMAIL")
    )


def _sender_header() -> str:
    from_name = current_app.config.get("MAIL_FROM_NAME") or "ETH Chess"
    from_email = current_app.config.get("MAIL_FROM_EMAIL") or "no-reply@example.invalid"
    return formataddr((from_name, from_email))


def _base_url() -> str:
    return (current_app.config.get("PUBLIC_BASE_URL") or "").rstrip("/")


def _tournament_lines(tournament) -> list[str]:
    lines = [
        f"Tournament: {tournament['name']}",
        f"Date: {tournament['event_date']}",
    ]
    if tournament["event_time"]:
        lines.append(f"Start time: {tournament['event_time']}")
    if tournament["venue"]:
        lines.append(f"Venue: {tournament['venue']}")
    public_url = f"{_base_url()}{url_for('web.public_tournament', slug=tournament['slug'])}" if _base_url() else ""
    if public_url:
        lines.append(f"Website: {public_url}")
    return lines


def _deliver_message(message: EmailMessage) -> tuple[bool, str | None]:
    if not _mail_enabled():
        return False, "Mail delivery is not configured."

    if current_app.config.get("MAIL_SUPPRESS_SEND"):
        current_app.extensions.setdefault("mail_outbox", []).append(
            {
                "to": message["To"],
                "subject": message["Subject"],
                "body": message.get_content(),
            }
        )
        return True, None

    host = current_app.config["MAIL_HOST"]
    port = int(current_app.config.get("MAIL_PORT") or 587)
    username = current_app.config.get("MAIL_USERNAME")
    password = current_app.config.get("MAIL_PASSWORD")
    use_ssl = bool(current_app.config.get("MAIL_USE_SSL"))
    use_tls = bool(current_app.config.get("MAIL_USE_TLS")) and not use_ssl

    try:
        smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
        with smtp_cls(host, port, timeout=20) as server:
            if use_tls:
                server.starttls()
            if username:
                server.login(username, password or "")
            server.send_message(message)
    except OSError as exc:
        return False, str(exc)
    return True, None


def send_email(recipient: str, subject: str, body: str) -> tuple[bool, str | None]:
    if not recipient:
        return False, "Missing recipient email."
    if not _mail_enabled():
        return False, "Mail delivery is not configured."

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = _sender_header()
    message["To"] = recipient
    reply_to = current_app.config.get("MAIL_REPLY_TO")
    if reply_to:
        message["Reply-To"] = reply_to
    message.set_content(body)
    return _deliver_message(message)


def registration_email_body(tournament, player_name: str, waitlist_position: int | None) -> str:
    status_line = (
        f"You are currently on the waiting list in position {waitlist_position}."
        if waitlist_position is not None
        else "Your registration is confirmed."
    )
    lines = [
        f"Hello {player_name},",
        "",
        f"Thank you for registering for {tournament['name']}.",
        status_line,
        "",
        *_tournament_lines(tournament),
        "",
        MEMBERSHIP_NOTE,
        "Please be on time so pairings can start as scheduled.",
        "",
        "This message was sent automatically by the ETH Chess tournament desk.",
    ]
    return "\n".join(lines)


def waitlist_confirmation_email_body(tournament, player_name: str) -> str:
    lines = [
        f"Hello {player_name},",
        "",
        f"You now have a confirmed spot in {tournament['name']}.",
        "",
        *_tournament_lines(tournament),
        "",
        MEMBERSHIP_NOTE,
        "Please be on time so pairings can start as scheduled.",
        "",
        "This message was sent automatically by the ETH Chess tournament desk.",
    ]
    return "\n".join(lines)


def send_registration_email(tournament, entry: dict[str, Any], waitlist_position: int | None) -> tuple[bool, str | None]:
    recipient = entry.get("email") or entry.get("imported_email")
    body = registration_email_body(tournament, entry["name"], waitlist_position)
    return send_email(recipient, f"Registration for {tournament['name']}", body)


def send_waitlist_confirmation_email(tournament, entry: dict[str, Any]) -> tuple[bool, str | None]:
    recipient = entry.get("email") or entry.get("imported_email")
    body = waitlist_confirmation_email_body(tournament, entry["name"])
    return send_email(recipient, f"Confirmed spot for {tournament['name']}", body)
