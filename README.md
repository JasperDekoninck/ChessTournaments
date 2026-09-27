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

## Registration scheduling and rounds

Registration opening times use **Europe/Zurich** (CET/CEST), independently of the server or browser timezone. New schedules include the UTC offset; existing timezone-free schedules are interpreted as Zurich time. Times skipped when clocks move forward are rejected. When clocks move back, a repeated local time refers to its first occurrence.

To change the number of rounds, open a tournament's **Tournament settings**. Choose 1–15 rounds; rounds with existing pairings cannot be removed, and finished tournaments keep their round count. Existing results and round availability are preserved.

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

## Tournament Formats

When creating a tournament, or under **Tournament settings**, enable **Exclude from ratings and rating-based prizes** for an unrated event. Standings and results remain available, but the event does not contribute to player ratings, rated game histories, performance ratings, or rating-based prizes. Changing this setting on a completed local tournament rebuilds the ratings.

**Team tournament (unrated)** lets entrants register a team name, member email addresses, and average Elo, or register alone with their name, email, and optional approximate Elo. Set the number of members per team in tournament settings (default: 2). Team registrations and admin assignments must match this size. Teams compete as single entries with the usual round, pairing, and result controls. Solo registrations are confirmed immediately and receive a confirmation email explaining that they will be paired at the tournament. The registered count includes each unassigned solo as 1 / team size (half a team by default). Assigning these members to a team preserves their confirmed places without counting them twice. The registration limit applies to new full teams; solo registrations remain open even when that limit is reached.

Admins select solo registrations under **Awaiting team** and assign them to a new named team or an incomplete existing team that has not been paired. Elo estimates are shown in the admin alongside registrations. A team's average Elo is used for seeding, without affecting player ratings or rating-based prizes. Teams formed from solo registrations get an average when every member provides an estimate; otherwise they use the default seed of 1500. Member emails and registration answers are visible only to admins. Team format can only be changed before registrations or pairings exist, and team size changes must remain consistent with registered teams. Existing databases are migrated automatically at startup.

## Pairing and result controls

Individual tournaments use the FIDE Dutch rules effective February 2026. Team tournaments use the separate FIDE Swiss Team system with Type A colour preferences. The pairing adapter uses pinned `gacrux==1.10.62`; run `uv sync` when updating this checkout. Pairings consider the complete prior history, ordered bracket criteria, floats, played colours and bye eligibility. Board order follows General Handling article 3.6.

The existing club conventions remain: rating/name seeding, White as the initial colour, and one result per team encounter scored 1 / 0.5 / 0 with a full-point bye. Individual board scores and secondary team scores are not collected. This is not a claim of FIDE software certification.

Admins choose **White wins**, **Draw**, or **Black wins**. **Forfeit / clear** offers a win by forfeit for either side, both forfeiting, or clearing the result. Forfeits award the indicated points but do not count as played colours, played opponents, or rated games. A full-point forfeit winner cannot receive a later pairing-allocated bye.

Both automatic and manual pairing enforce the planned round limit and require valid results in every preceding round. The latest round can be corrected until another round is paired; older rounds and finished tournaments are locked. Intentional manual opponent overrides remain available in an editable round. Autosaves run in order, and round generation and finishing wait for pending result saves.

Regression tests: `uv run python -m unittest discover -s tests`. The Dutch reference fixture was checked with [BBP Pairings 6.0.0](https://github.com/BieremaBoyzProgramming/bbpPairings/releases/tag/v6.0.0); the team and colour cases target the [2026 Dutch](https://handbook.fide.com/chapter/C0403202602), [Swiss Team](https://handbook.fide.com/chapter/SwissTeamPairingSystem202602), and [General Handling](https://handbook.fide.com/chapter/GeneralHandlingRulesForSwissTournaments202602) rules.
