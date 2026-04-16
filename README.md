# ETH Chess Tournament Manager

Flask app for running ETH Chess tournaments with integrated pairings, standings, public pages, and the local rating engine.

## Quick start

```bash
uv sync
uv run flask --app flaskr init-db
uv run flask --app flaskr import-rating-history --source /path/to/ETH-Chess-Rating
uv run flask --app flaskr run --debug
```

Open `http://127.0.0.1:5000/` for the public site and `http://127.0.0.1:5000/admin` for administration.

The initial admin password is `admin`. Change it immediately from the admin dashboard or with:

```bash
uv run flask --app flaskr set-admin-password "your-new-password"
```

## Included

- admin-only tournament creation and round management at `/admin`
- public homepage that shows only the active tournament
- Forminator registration CSV import with member/rating matching
- round-by-round availability, manual pairing edits, byes, and result entry
- standings with Buchholz tie-breaks
- historical tournament import from the original Vega/manual source files
- public player history pages and anonymous leaderboard export
- local port of the ETH Chess rating engine and tournament export CSVs
