# ETH Chess Tournament Manager

Flask app for running ETH Chess tournaments with integrated pairings, standings, public pages, and the local rating engine.

## Quick start

```bash
uv sync
uv run flask --app flaskr init-db
uv run flask --app flaskr import-rating-history --source /path/to/ETH-Chess-Rating
uv run flask --app flaskr run --debug
```

Open `http://127.0.0.1:5000/` for the public site, `http://127.0.0.1:5000/register` for public tournament registration, and `http://127.0.0.1:5000/admin` for administration.

The initial admin password is `admin`. Change it immediately from the admin dashboard or with:

```bash
uv run flask --app flaskr set-admin-password "your-new-password"
```

## Local Test Copy

The repo also includes a full copied test instance in `local-test-instance/`. It is meant as a safe sandbox so you can test pairings, registrations, and admin changes without touching `instance/chess.db`.

Recommended workflow:

```bash
CHESS_INSTANCE_PATH=local-test-instance uv run flask --app flaskr run --debug
```

If you already created `.local-test-run` earlier, recopy it after pulling changes. It is a plain copy and does not update automatically when `local-test-instance/` changes.

If you want to run directly against the committed copy instead:

```bash
CHESS_INSTANCE_PATH=local-test-instance uv run flask --app flaskr run --debug
```

The copied test instance uses the admin password `admin`.

## Email Setup

Registration and waitlist confirmation emails are disabled until you configure SMTP.

Set these environment variables before starting the app:

```bash
export CHESS_PUBLIC_BASE_URL="https://your-domain.example"
export CHESS_MAIL_ENABLED=1
export CHESS_MAIL_HOST="smtp.your-provider.example"
export CHESS_MAIL_PORT=587
export CHESS_MAIL_USERNAME="smtp-user"
export CHESS_MAIL_PASSWORD="smtp-password"
export CHESS_MAIL_USE_TLS=1
export CHESS_MAIL_USE_SSL=0
export CHESS_MAIL_FROM_EMAIL="noreply@your-domain.example"
export CHESS_MAIL_FROM_NAME="ETH Chess"
export CHESS_MAIL_REPLY_TO="chess@your-domain.example"
```

Then run the app normally:

```bash
uv run flask --app flaskr run --debug
```

To keep those emails out of spam, your sending domain should have valid `SPF`, `DKIM`, and `DMARC` DNS records, and `CHESS_MAIL_FROM_EMAIL` should use that same domain.

## Included

- admin-only tournament creation and round management at `/admin`
- public homepage that shows only the active tournament
- public registration page at `/register`, with admin-controlled opening time and maximum capacity
- tournaments stay private until an admin makes one public
- Forminator registration CSV import with member/rating matching
- automatic waiting-list placement once the registration cap is reached
- manual player entry, round-by-round availability, manual pairing edits, byes, and result entry
- standings with Buchholz tie-breaks
- historical tournament import from the original Vega/manual source files
- public player history pages and leaderboard export
- local port of the ETH Chess rating engine and tournament export CSVs
