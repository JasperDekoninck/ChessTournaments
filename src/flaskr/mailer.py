from __future__ import annotations

import smtplib
from email.message import EmailMessage
from email.utils import formataddr
from typing import Any

from flask import current_app, url_for


MEMBERSHIP_NOTE = """
If you are not yet an ETH Chess member for the current academic year, you can become one at the start of the tournament for CHF 5.
We accept cash, TWINT, and bank transfers. Your membership is valid for the entire academic year.
"""

PUNCTUALITY_NOTE = "Please arrive on time so we can begin the tournament and publish pairings without delay. If you can't make it, please let us know so we can give your spot to the next person in the waiting list."


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


def _tournament_lines(tournament) -> list[str]:
    lines = [
        f"Tournament: {tournament['name']}",
        f"Date: {tournament['event_date']}",
    ]
    if tournament["event_time"]:
        lines.append(f"Start time: {tournament['event_time']}")
    if tournament["venue"]:
        lines.append(f"Venue: {tournament['venue']}")
    return lines


def _deliver_message(message: EmailMessage) -> tuple[bool, str | None]:
    if not _mail_enabled():
        current_app.logger.warning("Mail delivery skipped because SMTP is not fully configured.")
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
    except Exception as exc:
        current_app.logger.exception(
            "Failed to send email to %s via %s:%s",
            message.get("To"),
            host,
            port,
        )
        return False, str(exc)
    return True, None


def send_email(recipient: str, subject: str, body: str) -> tuple[bool, str | None]:
    if not recipient:
        current_app.logger.warning("Mail delivery skipped because no recipient email was provided.")
        return False, "Missing recipient email."
    if not _mail_enabled():
        current_app.logger.warning("Mail delivery skipped because SMTP is not fully configured.")
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
        PUNCTUALITY_NOTE,
        "",
        "Kind Regards,",
        "Schwarzer König",
    ]
    return "\n".join(lines)


def waitlist_confirmation_email_body(tournament, player_name: str) -> str:
    lines = [
        f"Hello {player_name},",
        "",
        f"Good news! A spot has opened up and you are now confirmed for {tournament['name']}.",
        "",
        *_tournament_lines(tournament),
        "",
        MEMBERSHIP_NOTE,
        PUNCTUALITY_NOTE,
        "",
        "Kind Regards,",
        "Schwarzer König",
    ]
    return "\n".join(lines)


def send_registration_email(tournament, entry: dict[str, Any], waitlist_position: int | None) -> tuple[bool, str | None]:
    recipient = entry.get("email") or entry.get("imported_email")
    body = registration_email_body(tournament, entry["name"], waitlist_position)
    subject = (
        f"Waiting List for {tournament['name']}"
        if waitlist_position is not None
        else f"Registration Confirmed for {tournament['name']}"
    )
    return send_email(recipient, subject, body)


def send_waitlist_confirmation_email(tournament, entry: dict[str, Any]) -> tuple[bool, str | None]:
    recipient = entry.get("email") or entry.get("imported_email")
    body = waitlist_confirmation_email_body(tournament, entry["name"])
    return send_email(recipient, f"Confirmed spot for {tournament['name']}", body)
