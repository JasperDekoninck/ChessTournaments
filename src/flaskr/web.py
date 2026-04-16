from __future__ import annotations

import csv
import io
import json
from math import ceil
from pathlib import Path

from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

from .auth import login_required, set_admin_password, verify_password
from .mailer import send_registration_email, send_waitlist_confirmation_email
from .core import (
    VALID_RESULTS,
    attach_entries_to_tournament,
    compact_waitlist,
    compute_standings,
    ensure_round_status_rows,
    ensure_entry_round_status_rows,
    fetch_active_tournament,
    fetch_availability,
    fetch_entries,
    fetch_open_registration_tournaments,
    fetch_pairings,
    fetch_public_tournaments,
    fetch_tournament_by_slug,
    fetch_tournaments,
    generate_swiss_pairings,
    latest_paired_round,
    next_round_to_pair,
    pairings_complete,
    persist_final_standings,
    normalize_result_code,
    normalize_name,
    parse_manual_pairing_form,
    parse_datetime_local,
    parse_registration_form_fields,
    parse_registration_csv,
    parse_int,
    public_rounds,
    registration_counts,
    registration_open_for_tournament,
    replace_round_pairings,
    result_points_label,
    round_rating_value,
    serialize_registration_form_fields,
    set_active_tournament,
    slugify,
    unset_active_tournament,
    upsert_player,
    unique_slug,
    next_waitlist_position,
)
from .db import get_db
from .rating_integration import (
    anonymous_leaderboard_rows,
    build_matcher,
    get_player_history,
    get_player_profile,
    get_member_since_date,
    list_member_directory,
    player_suggestions,
    rebuild_current_manager,
    set_member_override,
    set_member_since_date,
    sync_member_statuses,
    tournament_insights,
)


bp = Blueprint("web", __name__)
ADMIN_TABS = {"create", "tournaments", "members", "password"}


def flash_success(message: str):
    flash(message, "success")


def flash_warning(message: str):
    flash(message, "warning")


def flash_error(message: str):
    flash(message, "error")


def flash_info(message: str):
    flash(message, "info")


def _tournament_or_404(slug: str):
    tournament = fetch_tournament_by_slug(get_db(), slug)
    if tournament is None:
        abort(404)
    return tournament


def _public_tournament_or_404(slug: str):
    tournament = _tournament_or_404(slug)
    if tournament["is_public"] or session.get("is_admin"):
        return tournament
    abort(404)


def _ensure_editable(tournament) -> bool:
    if tournament["is_historical"]:
        flash_warning("Imported historical tournaments are read-only.")
        return False
    return True


def _selected_public_view(selected_round: int | None) -> str:
    requested = (request.args.get("view") or "").strip().lower()
    if requested in {"boards", "standings"}:
        return requested
    return "boards" if selected_round is not None else "standings"


def _register_context():
    db = get_db()
    tournaments = fetch_open_registration_tournaments(db)
    return {
        "open_tournaments": tournaments,
        "registration_counts": {tournament["id"]: registration_counts(db, tournament["id"]) for tournament in tournaments},
        "registration_fields_by_tournament": {
            tournament["id"]: parse_registration_form_fields(tournament["registration_form_json"])
            for tournament in tournaments
        },
    }


def _round_view_context(tournament, selected_round: int | None = None, final_standings: bool = False):
    db = get_db()
    round_numbers = public_rounds(db, tournament["id"])
    latest_round = latest_paired_round(db, tournament["id"])
    if selected_round is None:
        selected_round = latest_round
    if selected_round is not None and selected_round not in round_numbers:
        abort(404)
    pairings = fetch_pairings(db, tournament["id"], selected_round) if selected_round is not None else []
    standings_round = None
    if not final_standings and not (tournament["status"] == "completed" and selected_round == latest_round):
        standings_round = selected_round
    standings = compute_standings(db, tournament["id"], through_round=standings_round)
    podium = standings[:3] if tournament["status"] == "completed" else []
    return {
        "round_numbers": round_numbers,
        "selected_round": selected_round,
        "latest_round": latest_round,
        "pairings": pairings,
        "standings": standings,
        "podium": podium,
        "tournament_insights": tournament_insights(tournament),
        "view_mode": _selected_public_view(selected_round),
    }


def _selected_admin_tab() -> str:
    requested = (request.args.get("tab") or "create").strip().lower()
    return requested if requested in ADMIN_TABS else "create"


def _wants_json() -> bool:
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"


def _admin_tournament_url(slug: str, round_no: int | None = None) -> str:
    if round_no is None:
        return url_for("web.admin_tournament_detail", slug=slug)
    return f"{url_for('web.admin_tournament_detail', slug=slug, open_round=round_no)}#round-{round_no}"


def _tournament_standings_csv(tournament) -> str:
    db = get_db()
    standings = compute_standings(db, tournament["id"])
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "Rank",
            "Name",
            "Rating",
            "Score",
            tournament["primary_tiebreak_label"],
            tournament["secondary_tiebreak_label"],
        ]
    )
    for row in standings:
        writer.writerow(
            [
                row["rank"],
                row["name"],
                row["seed_rating"],
                f"{float(row['score']):.1f}",
                f"{float(row['bh']):.1f}",
                f"{float(row['bh_c1']):.1f}",
            ]
        )
    return output.getvalue()


def _entry_state_payload(db, entry_id: int) -> dict | None:
    entry = db.execute(
        """
        SELECT id, imported_name, is_active, waitlist_position
        FROM tournament_entry
        WHERE id = ?
        """,
        (entry_id,),
    ).fetchone()
    if entry is None:
        return None
    if entry["waitlist_position"] is not None:
        label = f"waitlist #{entry['waitlist_position']}"
        state = "waitlist"
    else:
        label = "active" if entry["is_active"] else "inactive"
        state = "active" if entry["is_active"] else "inactive"
    return {
        "id": entry["id"],
        "name": entry["imported_name"],
        "is_active": bool(entry["is_active"]),
        "waitlist_position": entry["waitlist_position"],
        "label": label,
        "state": state,
    }


def _tournament_round_meta(db, tournament) -> tuple[int | None, int | None, int | None]:
    latest_round = latest_paired_round(db, tournament["id"])
    next_round = next_round_to_pair(db, tournament["id"], tournament["rounds_planned"])
    editable_round = next_round
    if latest_round is not None and not pairings_complete(db, tournament["id"], latest_round):
        editable_round = latest_round
    return latest_round, next_round, editable_round


def _round_is_locked(db, tournament, round_no: int) -> bool:
    return pairings_complete(db, tournament["id"], round_no)


def _entry_row_payload(db, tournament, entry_id: int) -> dict | None:
    entry_state = _entry_state_payload(db, entry_id)
    if entry_state is None:
        return None
    pairings = fetch_pairings(db, tournament["id"])
    entries = _order_admin_entries(fetch_entries(db, tournament["id"]), pairings)
    availability = fetch_availability(db, tournament["id"])
    round_cells = _entry_round_cells(entries, availability, pairings, tournament["rounds_planned"])
    latest_round, next_round, editable_round = _tournament_round_meta(db, tournament)
    entry = next((row for row in entries if row["id"] == entry_id), None)
    if entry is None:
        return None
    entry_state["round_cells"] = [
        {
            "round_no": round_no,
            "cell": round_cells[entry_id][round_no],
            "can_toggle": bool(
                entry["waitlist_position"] is None
                and not _round_is_locked(db, tournament, round_no)
                and round_cells[entry_id][round_no]["kind"] not in {"played", "bye"}
            ),
        }
        for round_no in range(1, tournament["rounds_planned"] + 1)
    ]
    entry_state["editable_round"] = editable_round
    entry_state["next_round"] = next_round
    entry_state["latest_round"] = latest_round
    return entry_state


def _pairing_ids_by_round(pairings):
    paired_ids = {}
    for pairing in pairings:
        bucket = paired_ids.setdefault(pairing["round_no"], set())
        if pairing["white_entry_id"] is not None:
            bucket.add(pairing["white_entry_id"])
        if pairing["black_entry_id"] is not None:
            bucket.add(pairing["black_entry_id"])
    return paired_ids


def _entry_round_cells(entries, availability, pairings, rounds_planned: int):
    latest_round = max((pairing["round_no"] for pairing in pairings), default=0)
    cells = {entry["id"]: {} for entry in entries}
    paired_ids = _pairing_ids_by_round(pairings)
    for pairing in pairings:
        round_no = pairing["round_no"]
        white_id = pairing["white_entry_id"]
        black_id = pairing["black_entry_id"]
        if white_id is None:
            continue
        if black_id is None:
            cells[white_id][round_no] = {"label": "1", "kind": "bye"}
            continue
        white_label = result_points_label(pairing["result_code"], "white")
        black_label = result_points_label(pairing["result_code"], "black")
        cells[white_id][round_no] = {"label": white_label or "in", "kind": "played" if white_label else "pending"}
        cells[black_id][round_no] = {"label": black_label or "in", "kind": "played" if black_label else "pending"}
    for entry in entries:
        entry_cells = cells[entry["id"]]
        for round_no in range(1, rounds_planned + 1):
            if round_no in entry_cells:
                continue
            is_available = bool(availability.get(entry["id"], {}).get(round_no, True))
            if entry["waitlist_position"] is not None:
                is_available = False
            if round_no <= latest_round and not is_available:
                entry_cells[round_no] = {"label": "out", "kind": "out"}
            elif round_no > latest_round and not is_available:
                entry_cells[round_no] = {"label": "out", "kind": "future-out"}
            elif round_no <= latest_round and entry["id"] not in paired_ids.get(round_no, set()):
                entry_cells[round_no] = {"label": "in", "kind": "idle"}
            else:
                entry_cells[round_no] = {"label": "in", "kind": "empty"}
    return cells


def _registration_sort_key(entry) -> tuple:
    timestamp = (entry["submitted_at"] or entry["created_at"] or "").replace(" ", "T")
    timestamp_key = timestamp if timestamp else "9999-12-31T23:59:59"
    order_key = entry["registration_order"] if entry["registration_order"] is not None else entry["id"]
    return (
        1 if entry["waitlist_position"] is not None else 0,
        entry["waitlist_position"] if entry["waitlist_position"] is not None else 0,
        timestamp_key,
        order_key,
        entry["imported_name"].lower(),
    )


def _order_admin_entries(entries, pairings):
    if any(normalize_result_code(pairing["result_code"]) is not None for pairing in pairings):
        return entries
    return sorted(entries, key=_registration_sort_key)


def _round_panels(tournament, entries, availability):
    db = get_db()
    pairings = fetch_pairings(db, tournament["id"])
    pairings_by_round = {}
    paired_ids_by_round = _pairing_ids_by_round(pairings)
    for pairing in pairings:
        pairings_by_round.setdefault(pairing["round_no"], []).append(pairing)

    panels = []
    for round_no in range(1, tournament["rounds_planned"] + 1):
        current_pairings = sorted(pairings_by_round.get(round_no, []), key=lambda row: row["board_no"])
        paired_ids = paired_ids_by_round.get(round_no, set())
        editable_pool = [
            entry
            for entry in entries
            if entry["waitlist_position"] is None or entry["id"] in paired_ids
        ]
        editable_pool.sort(key=lambda row: (-row["seed_rating"], row["imported_name"].lower()))
        board_count = len(current_pairings)
        if not tournament["is_historical"]:
            available_count = sum(
                1 for entry in entries if entry["is_active"] and availability.get(entry["id"], {}).get(round_no, True)
            )
            board_count = max(board_count, ceil(available_count / 2))
        pairing_by_board = {pairing["board_no"]: pairing for pairing in current_pairings}
        board_rows = [
            pairing_by_board.get(
                board_no,
                {"board_no": board_no, "white_entry_id": None, "black_entry_id": None, "result_code": None},
            )
            for board_no in range(1, board_count + 1)
        ]
        panels.append(
            {
                "round_no": round_no,
                "board_rows": board_rows,
                "board_count": board_count,
                "pairings": current_pairings,
                "entries": editable_pool,
                "has_pairings": bool(current_pairings),
            }
        )
    return panels


@bp.route("/")
def index():
    active = fetch_active_tournament(get_db())
    if active is None:
        return render_template(
            "public_empty.html",
            tournaments=fetch_public_tournaments(get_db()),
        )
    context = _round_view_context(active, final_standings=bool(active["is_historical"]))
    return render_template(
        "public_tournament.html",
        tournament=active,
        archive=fetch_public_tournaments(get_db()),
        active_tournament=active,
        show_as_home=True,
        **context,
    )


@bp.route("/archive")
def archive():
    return redirect(url_for("web.index"))


@bp.route("/t/<slug>")
def public_tournament(slug: str):
    tournament = _public_tournament_or_404(slug)
    context = _round_view_context(tournament, final_standings=bool(tournament["is_historical"]))
    return render_template(
        "public_tournament.html",
        tournament=tournament,
        archive=fetch_public_tournaments(get_db()),
        active_tournament=fetch_active_tournament(get_db()),
        **context,
    )


@bp.route("/t/<slug>/round/<int:round_no>")
def public_tournament_round(slug: str, round_no: int):
    tournament = _public_tournament_or_404(slug)
    context = _round_view_context(tournament, selected_round=round_no)
    return render_template(
        "public_tournament.html",
        tournament=tournament,
        archive=fetch_public_tournaments(get_db()),
        active_tournament=fetch_active_tournament(get_db()),
        **context,
    )


@bp.route("/leaderboard")
@bp.route("/ratings")
def leaderboard():
    return render_template("leaderboard.html", leaderboard=anonymous_leaderboard_rows())


@bp.route("/register")
def register():
    return render_template("register.html", **_register_context())


@bp.get("/register/lookup")
def register_lookup():
    if not fetch_open_registration_tournaments(get_db()):
        return jsonify({"items": []})
    query = (request.args.get("q") or "").strip()
    items = [
        {"name": item["name"], "member": item["member"], "rating": item["rating"]}
        for item in player_suggestions(get_db(), query)
    ]
    return jsonify({"items": items})


@bp.post("/register/<slug>")
def submit_registration(slug: str):
    db = get_db()
    tournament = _tournament_or_404(slug)
    if not registration_open_for_tournament(tournament):
        flash_warning("Registration is not open for this tournament.")
        return redirect(url_for("web.register"))

    name = (request.form.get("name") or "").strip()
    if not name:
        flash_error("Your name is required.")
        return redirect(url_for("web.register"))
    email = (request.form.get("email") or "").strip()
    if not email:
        flash_error("Your email is required.")
        return redirect(url_for("web.register"))

    normalized_name = normalize_name(name)
    existing_by_name = db.execute(
        """
        SELECT imported_name, waitlist_position
        FROM tournament_entry
        WHERE tournament_id = ?
        ORDER BY id ASC
        """,
        (tournament["id"],),
    ).fetchall()
    duplicate_entry = next(
        (
            entry
            for entry in existing_by_name
            if normalize_name(entry["imported_name"]) == normalized_name
        ),
        None,
    )
    if duplicate_entry is not None:
        if duplicate_entry["waitlist_position"] is not None:
            flash_warning(
                f"{duplicate_entry['imported_name']} is already registered and currently on the waiting list in position {duplicate_entry['waitlist_position']}."
            )
        else:
            flash_warning(f"{duplicate_entry['imported_name']} is already registered for this tournament.")
        return redirect(url_for("web.register"))

    registration_fields = parse_registration_form_fields(tournament["registration_form_json"])
    registration_answers = []
    for index, field in enumerate(registration_fields):
        value = (request.form.get(f"registration_field_{index}") or "").strip()
        if not value:
            flash_error(f"{field['label']} is required.")
            return redirect(url_for("web.register"))
        if field["type"] == "dropdown" and value not in field["options"]:
            flash_error(f"Choose a valid value for {field['label']}.")
            return redirect(url_for("web.register"))
        registration_answers.append(
            {
                "label": field["label"],
                "type": field["type"],
                "value": value,
            }
        )

    row = {
        "name": name,
        "email": email,
        "submitted_at": None,
        "declared_rating": parse_int(request.form.get("declared_rating"), default=None),
    }
    match = build_matcher()(row["name"], row["declared_rating"])
    player_id = upsert_player(db, row, match)
    existing = db.execute(
        """
        SELECT waitlist_position
        FROM tournament_entry
        WHERE tournament_id = ? AND player_id = ?
        """,
        (tournament["id"], player_id),
    ).fetchone()
    if existing is not None:
        if existing["waitlist_position"] is not None:
            flash_warning(
                f"Registration already exists. You are currently on the waiting list in position {existing['waitlist_position']}."
            )
        else:
            flash_warning("Registration already exists. You have a confirmed spot in this tournament.")
        return redirect(url_for("web.register"))

    counts = registration_counts(db, tournament["id"])
    max_registrations = tournament["max_registrations"]
    waitlist_position = None
    is_active = 0
    if max_registrations is not None and max_registrations > 0 and counts["confirmed_count"] >= max_registrations:
        waitlist_position = next_waitlist_position(db, tournament["id"])
        is_active = 0

    seed_rating = round_rating_value(match.historical_rating or row.get("declared_rating") or 1500, default=1500)
    registration_order = db.execute(
        "SELECT COALESCE(MAX(registration_order), 0) + 1 AS next_order FROM tournament_entry WHERE tournament_id = ?",
        (tournament["id"],),
    ).fetchone()["next_order"]
    cursor = db.execute(
        """
        INSERT INTO tournament_entry (
          tournament_id, player_id, imported_name, imported_email, submitted_at,
          declared_rating, seed_rating, member_status, is_active,
          registration_source, registration_order, registration_answers_json, waitlist_position
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tournament["id"],
            player_id,
            row["name"],
            row["email"],
            row["submitted_at"],
            row["declared_rating"],
            seed_rating,
            match.member_status,
            is_active,
            "public",
            registration_order,
            json.dumps(registration_answers, ensure_ascii=True) if registration_answers else None,
            waitlist_position,
        ),
    )
    db.commit()
    ensure_entry_round_status_rows(
        db,
        cursor.lastrowid,
        tournament["rounds_planned"],
        available_from_round=(latest_paired_round(db, tournament["id"]) or 0) + 1,
    )

    email_sent, _ = send_registration_email(tournament, row, waitlist_position)
    if waitlist_position is not None:
        message = f"Registration received for {tournament['name']}. The event is currently full, so you are on the waiting list in position {waitlist_position}."
        if email_sent:
            flash_warning(f"{message} A confirmation email has been sent.")
        else:
            flash_warning(message)
    else:
        message = f"Registration received for {tournament['name']}. Your spot is confirmed."
        if email_sent:
            flash_success(f"{message} A confirmation email has been sent.")
        else:
            flash_success(message)
    return redirect(url_for("web.register"))


@bp.route("/players/<path:player_name>")
def leaderboard_player(player_name: str):
    profile = get_player_profile(player_name)
    return render_template(
        "global_player_history.html",
        player_name=profile["name"] if profile else player_name,
        profile=profile,
        games=get_player_history(player_name),
    )


@bp.route("/t/<slug>/player/<int:entry_id>")
def player_history(slug: str, entry_id: int):
    db = get_db()
    tournament = _public_tournament_or_404(slug)
    entry = db.execute(
        """
        SELECT
          e.id,
          e.imported_name,
          p.canonical_rating_name
        FROM tournament_entry e
        JOIN player p ON p.id = e.player_id
        WHERE e.id = ? AND e.tournament_id = ?
        """,
        (entry_id, tournament["id"]),
    ).fetchone()
    if entry is None:
        abort(404)
    player_name = entry["canonical_rating_name"] or entry["imported_name"]
    profile = get_player_profile(player_name)
    return render_template(
        "player_history.html",
        tournament=tournament,
        entry=entry,
        profile=profile,
        games=get_player_history(player_name),
    )


@bp.route("/admin", methods=("GET", "POST"))
def admin():
    if request.method == "POST" and not session.get("is_admin"):
        password = request.form.get("password") or ""
        if verify_password(password):
            session.clear()
            session["is_admin"] = True
            session.permanent = True
            flash_success("Signed in.")
            return redirect(url_for("web.admin"))
        flash_error("Invalid password.")

    if not session.get("is_admin"):
        return render_template("admin_login.html")

    db = get_db()
    active_tab = _selected_admin_tab()
    members = list_member_directory(db)
    member_query = (request.args.get("member_q") or "").strip()
    if member_query:
        needle = member_query.casefold()
        members = [member for member in members if needle in (member["name"] or "").casefold()]
    members_page = max(1, parse_int(request.args.get("members_page"), default=1) or 1)
    members_total_pages = max(1, ceil(len(members) / 20))
    members_page = min(members_page, members_total_pages)
    members_start = (members_page - 1) * 20
    return render_template(
        "admin_dashboard.html",
        tournaments=fetch_tournaments(db),
        active_tournament=fetch_active_tournament(db),
        member_cutoff=get_member_since_date(db),
        members=members[members_start : members_start + 20],
        active_tab=active_tab,
        members_page=members_page,
        members_total_pages=members_total_pages,
        member_query=member_query,
    )


@bp.post("/admin/members/cutoff")
@login_required
def update_member_cutoff():
    members_page = max(1, parse_int(request.form.get("members_page"), default=1) or 1)
    member_query = (request.form.get("member_q") or "").strip()
    cutoff = (request.form.get("member_since_date") or "").strip()
    try:
        normalized = set_member_since_date(get_db(), cutoff)
    except ValueError:
        flash_error("Choose a valid membership date.")
    else:
        flash_success(f"Membership date updated to {normalized}.")
    return redirect(url_for("web.admin", tab="members", members_page=members_page, member_q=member_query))


@bp.post("/admin/members/<int:player_id>/status")
@login_required
def update_member_status(player_id: int):
    members_page = max(1, parse_int(request.form.get("members_page"), default=1) or 1)
    member_query = (request.form.get("member_q") or "").strip()
    enabled = (request.form.get("is_member") or "").strip() == "1"
    db = get_db()
    set_member_override(db, player_id, enabled)
    player = db.execute("SELECT member_status FROM player WHERE id = ?", (player_id,)).fetchone()
    is_member = bool(player and player["member_status"] == "member")
    if _wants_json():
        return jsonify(
            {
                "ok": True,
                "is_member": is_member,
                "label": "active" if is_member else "inactive",
            }
        )
    flash_success("Member database updated.")
    return redirect(url_for("web.admin", tab="members", members_page=members_page, member_q=member_query))


@bp.post("/admin/logout")
@login_required
def admin_logout():
    session.clear()
    flash_success("Signed out.")
    return redirect(url_for("web.admin"))


@bp.post("/admin/password")
@login_required
def admin_password():
    current_password = request.form.get("current_password") or ""
    new_password = request.form.get("new_password") or ""
    confirm_password = request.form.get("confirm_password") or ""
    if not verify_password(current_password):
        flash_error("Current password is incorrect.")
        return redirect(url_for("web.admin", tab="password"))
    if len(new_password) < 10:
        flash_error("Choose a password with at least 10 characters.")
        return redirect(url_for("web.admin", tab="password"))
    if new_password != confirm_password:
        flash_error("The new passwords do not match.")
        return redirect(url_for("web.admin", tab="password"))
    set_admin_password(new_password)
    flash_success("Admin password updated.")
    return redirect(url_for("web.admin", tab="password"))


@bp.post("/admin/tournaments")
@login_required
def create_tournament():
    db = get_db()
    sync_member_statuses(db)
    name = (request.form.get("name") or "").strip()
    event_date = (request.form.get("event_date") or "").strip()
    rounds_planned = parse_int(request.form.get("rounds_planned"), default=7)
    csv_file = request.files.get("registrations")
    if not name or not event_date:
        flash_error("Tournament name and date are required.")
        return redirect(url_for("web.admin"))
    slug = unique_slug(db, slugify(name))
    cursor = db.execute(
        """
        INSERT INTO tournament (
          name, slug, event_date, rounds_planned, registration_csv_name, status,
          source_type, primary_tiebreak_label, secondary_tiebreak_label, is_public, is_active_public
        ) VALUES (?, ?, ?, ?, ?, 'draft', 'local', 'BH', 'BH-C1', 0, 0)
        """,
        (name, slug, event_date, rounds_planned, csv_file.filename if csv_file else None),
    )
    db.commit()
    if csv_file and csv_file.filename:
        attach_entries_to_tournament(db, cursor.lastrowid, parse_registration_csv(csv_file), build_matcher())
    flash_success("Tournament created.")
    return redirect(url_for("web.admin_tournament_detail", slug=slug))


@bp.get("/admin/player-suggestions")
@login_required
def admin_player_suggestions():
    db = get_db()
    query = (request.args.get("q") or "").strip()
    return jsonify({"items": player_suggestions(db, query)})


@bp.post("/admin/t/<slug>/entries")
@login_required
def add_entry(slug: str):
    db = get_db()
    sync_member_statuses(db)
    tournament = _tournament_or_404(slug)
    if not _ensure_editable(tournament):
        return redirect(url_for("web.admin_tournament_detail", slug=slug))
    name = (request.form.get("name") or "").strip()
    if not name:
        flash_error("Player name is required.")
        return redirect(url_for("web.admin_tournament_detail", slug=slug))
    row = {
        "name": name,
        "email": (request.form.get("email") or "").strip() or None,
        "submitted_at": None,
        "declared_rating": parse_int(request.form.get("declared_rating"), default=None),
    }
    attach_entries_to_tournament(db, tournament["id"], [row], build_matcher())
    flash_success(f"Added {name}.")
    return redirect(url_for("web.admin_tournament_detail", slug=slug))


@bp.post("/admin/t/<slug>/registration")
@login_required
def update_tournament_registration(slug: str):
    db = get_db()
    tournament = _tournament_or_404(slug)
    opens_at_raw = request.form.get("registration_opens_at")
    try:
        opens_at = parse_datetime_local(opens_at_raw)
    except ValueError:
        flash_error("Choose a valid registration opening date and time.")
        return redirect(url_for("web.admin_tournament_detail", slug=slug))

    registration_enabled = (request.form.get("registration_enabled") or "").strip() == "1"
    max_registrations = parse_int(request.form.get("max_registrations"), default=None)
    if max_registrations is not None and max_registrations <= 0:
        max_registrations = None
    event_time = (request.form.get("event_time") or "").strip() or None
    venue = (request.form.get("venue") or "").strip() or None
    field_types = request.form.getlist("registration_field_type")
    field_labels = request.form.getlist("registration_field_label")
    field_options = request.form.getlist("registration_field_options")
    custom_fields = []
    for index, (field_type, label, options_blob) in enumerate(zip(field_types, field_labels, field_options), start=1):
        normalized_type = (field_type or "").strip().lower()
        if normalized_type not in {"text", "dropdown"}:
            flash_error("Choose a valid extra registration field type.")
            return redirect(url_for("web.admin_tournament_detail", slug=slug))
        cleaned_label = " ".join((label or "").split())
        if not cleaned_label:
            flash_error(f"Extra registration field {index} needs a label.")
            return redirect(url_for("web.admin_tournament_detail", slug=slug))
        parsed_options = [line.strip() for line in (options_blob or "").replace(",", "\n").splitlines() if line.strip()]
        if normalized_type == "dropdown" and len(parsed_options) < 2:
            flash_error(f"Dropdown field “{cleaned_label}” needs at least two options.")
            return redirect(url_for("web.admin_tournament_detail", slug=slug))
        custom_fields.append(
            {
                "type": normalized_type,
                "label": cleaned_label,
                "options": parsed_options,
            }
        )
    serialized_fields = serialize_registration_form_fields(custom_fields)

    db.execute(
        """
        UPDATE tournament
        SET registration_enabled = ?, registration_opens_at = ?, registration_form_json = ?, event_time = ?, venue = ?, max_registrations = ?
        WHERE id = ?
        """,
        (
            1 if registration_enabled else 0,
            opens_at,
            serialized_fields,
            event_time,
            venue,
            max_registrations,
            tournament["id"],
        ),
    )
    db.commit()
    flash_success("Registration settings updated.")
    return redirect(url_for("web.admin_tournament_detail", slug=slug))


@bp.route("/admin/t/<slug>")
@login_required
def admin_tournament_detail(slug: str):
    db = get_db()
    tournament = _tournament_or_404(slug)
    ensure_round_status_rows(db, tournament["id"], tournament["rounds_planned"])
    pairings = fetch_pairings(db, tournament["id"])
    entries = _order_admin_entries(fetch_entries(db, tournament["id"]), pairings)
    availability = fetch_availability(db, tournament["id"])
    standings = compute_standings(db, tournament["id"])
    standings_by_entry = {row["entry_id"]: row for row in standings}
    round_panels = _round_panels(tournament, entries, availability)
    open_round = parse_int(request.args.get("open_round"), default=None)
    latest_round, next_round, editable_round = _tournament_round_meta(db, tournament)
    locked_rounds = {
        round_no
        for round_no in range(1, tournament["rounds_planned"] + 1)
        if _round_is_locked(db, tournament, round_no)
    }
    return render_template(
        "admin_tournament.html",
        tournament=tournament,
        registration_fields=parse_registration_form_fields(tournament["registration_form_json"]),
        entries=entries,
        registration_summary=registration_counts(db, tournament["id"]),
        availability=availability,
        standings=standings,
        standings_by_entry=standings_by_entry,
        round_cells=_entry_round_cells(entries, availability, pairings, tournament["rounds_planned"]),
        round_panels=round_panels,
        next_round=next_round,
        latest_round=latest_round,
        editable_round=editable_round,
        locked_rounds=locked_rounds,
        open_round=open_round,
        valid_results=sorted(VALID_RESULTS),
    )


def _admin_round_updates_payload(db, tournament, round_no: int) -> dict:
    pairings = fetch_pairings(db, tournament["id"])
    entries = _order_admin_entries(fetch_entries(db, tournament["id"]), pairings)
    availability = fetch_availability(db, tournament["id"])
    standings_by_entry = {
        row["entry_id"]: row
        for row in compute_standings(db, tournament["id"])
    }
    round_cells = _entry_round_cells(entries, availability, pairings, tournament["rounds_planned"])
    return {
        "entry_updates": [
            {
                "id": entry["id"],
                "score": float((standings_by_entry.get(entry["id"]) or {}).get("score", 0.0)),
                "bh": float((standings_by_entry.get(entry["id"]) or {}).get("bh", 0.0)),
                "bh_c1": float((standings_by_entry.get(entry["id"]) or {}).get("bh_c1", 0.0)),
                "round_cell": round_cells[entry["id"]][round_no],
            }
            for entry in entries
        ],
        "next_round": next_round_to_pair(db, tournament["id"], tournament["rounds_planned"]),
    }


@bp.post("/admin/t/<slug>/activate")
@login_required
def admin_activate_tournament(slug: str):
    open_round = parse_int(request.form.get("open_round"), default=None)
    tournament = _tournament_or_404(slug)
    if tournament["is_active_public"]:
        unset_active_tournament(get_db(), tournament["id"])
        flash_success(f"{tournament['name']} was removed from the public homepage.")
    else:
        set_active_tournament(get_db(), tournament["id"])
        flash_success(f"{tournament['name']} is now the public tournament.")
    return redirect(_admin_tournament_url(slug, open_round))


@bp.post("/admin/t/<slug>/entries/<int:entry_id>/toggle")
@login_required
def toggle_entry(slug: str, entry_id: int):
    db = get_db()
    tournament = _tournament_or_404(slug)
    if not _ensure_editable(tournament):
        return redirect(url_for("web.admin_tournament_detail", slug=slug))
    entry = db.execute(
        """
        SELECT id, is_active, waitlist_position
        FROM tournament_entry
        WHERE id = ? AND tournament_id = ?
        """,
        (entry_id, tournament["id"]),
    ).fetchone()
    if entry is None:
        abort(404)

    if entry["waitlist_position"] is not None:
        db.execute(
            "UPDATE tournament_entry SET is_active = 0, waitlist_position = NULL WHERE id = ?",
            (entry_id,),
        )
        db.commit()
        compact_waitlist(db, tournament["id"])
    else:
        becoming_active = not bool(entry["is_active"])
        db.execute(
            """
            UPDATE tournament_entry
            SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END
            WHERE id = ? AND tournament_id = ?
            """,
            (entry_id, tournament["id"]),
        )
        if becoming_active:
            for round_no in range(1, tournament["rounds_planned"] + 1):
                if _round_is_locked(db, tournament, round_no):
                    continue
                db.execute(
                    """
                    UPDATE entry_round_status
                    SET is_available = 1
                    WHERE entry_id = ? AND round_no = ?
                    """,
                    (entry_id, round_no),
                )
        db.commit()
    if _wants_json():
        payload = {"ok": True, "entry": _entry_row_payload(db, tournament, entry_id)}
        if entry["waitlist_position"] is not None:
            payload["waitlist"] = [
                _entry_row_payload(db, tournament, row["id"])
                for row in db.execute(
                    """
                    SELECT id FROM tournament_entry
                    WHERE tournament_id = ? AND waitlist_position IS NOT NULL
                    ORDER BY waitlist_position ASC
                    """,
                    (tournament["id"],),
                ).fetchall()
            ]
        return jsonify(payload)
    return redirect(url_for("web.admin_tournament_detail", slug=slug))


@bp.post("/admin/t/<slug>/entries/<int:entry_id>/confirm")
@login_required
def confirm_waitlist_entry(slug: str, entry_id: int):
    db = get_db()
    tournament = _tournament_or_404(slug)
    entry = db.execute(
        """
        SELECT id, imported_name, imported_email, waitlist_position
        FROM tournament_entry
        WHERE id = ? AND tournament_id = ?
        """,
        (entry_id, tournament["id"]),
    ).fetchone()
    if entry is None:
        abort(404)
    if entry["waitlist_position"] is None:
        flash_warning("This player is already confirmed.")
        return redirect(url_for("web.admin_tournament_detail", slug=slug))

    db.execute(
        "UPDATE tournament_entry SET is_active = 0, waitlist_position = NULL WHERE id = ?",
        (entry_id,),
    )
    db.commit()
    compact_waitlist(db, tournament["id"])
    sent, error = send_waitlist_confirmation_email(
        tournament,
        {"name": entry["imported_name"], "email": entry["imported_email"]},
    )
    if sent:
        flash_success(f"{entry['imported_name']} was confirmed and emailed.")
    else:
        flash_warning(f"{entry['imported_name']} was confirmed, but the confirmation email was not sent.")
        if error:
            flash_info(error)
    return redirect(url_for("web.admin_tournament_detail", slug=slug))


@bp.post("/admin/t/<slug>/entries/<int:entry_id>/availability")
@login_required
def toggle_availability(slug: str, entry_id: int):
    db = get_db()
    tournament = _tournament_or_404(slug)
    if not _ensure_editable(tournament):
        return redirect(url_for("web.admin_tournament_detail", slug=slug))
    round_no = parse_int(request.form.get("round_no"))
    if round_no is None:
        abort(400)
    entry = db.execute(
        """
        SELECT waitlist_position
        FROM tournament_entry
        WHERE id = ? AND tournament_id = ?
        """,
        (entry_id, tournament["id"]),
    ).fetchone()
    if entry is None:
        abort(404)
    if entry["waitlist_position"] is not None:
        if _wants_json():
            return jsonify({"ok": False, "message": "Waiting-list players cannot be toggled."}), 400
        flash_warning("Waiting-list players cannot be toggled.")
        return redirect(url_for("web.admin_tournament_detail", slug=slug))
    if _round_is_locked(db, tournament, round_no):
        if _wants_json():
            return jsonify({"ok": False, "message": "Finished rounds can no longer be changed."}), 400
        flash_warning("Finished rounds can no longer be changed.")
        return redirect(url_for("web.admin_tournament_detail", slug=slug))
    db.execute(
        """
        UPDATE entry_round_status
        SET is_available = CASE WHEN is_available = 1 THEN 0 ELSE 1 END
        WHERE entry_id = ? AND round_no = ?
        """,
        (entry_id, round_no),
    )
    db.commit()
    if _wants_json():
        row = db.execute(
            "SELECT is_available FROM entry_round_status WHERE entry_id = ? AND round_no = ?",
            (entry_id, round_no),
        ).fetchone()
        return jsonify(
            {
                "ok": True,
                "is_available": bool(row["is_available"]),
                "label": "in" if row["is_available"] else "out",
                "entry": _entry_row_payload(db, tournament, entry_id),
            }
        )
    return redirect(url_for("web.admin_tournament_detail", slug=slug))


@bp.route("/admin/t/<slug>/round/<int:round_no>")
@login_required
def admin_round_detail(slug: str, round_no: int):
    _tournament_or_404(slug)
    return redirect(_admin_tournament_url(slug, round_no))


@bp.post("/admin/t/<slug>/round/<int:round_no>/generate")
@login_required
def generate_round(slug: str, round_no: int):
    db = get_db()
    tournament = _tournament_or_404(slug)
    if not _ensure_editable(tournament):
        return redirect(_admin_tournament_url(slug, round_no))
    next_round = next_round_to_pair(db, tournament["id"], tournament["rounds_planned"])
    if next_round is None or round_no != next_round:
        flash_warning("You can only generate pairings for the next new round after the previous round is complete.")
        return redirect(_admin_tournament_url(slug, round_no))
    boards = generate_swiss_pairings(db, tournament["id"], round_no)
    if not boards:
        flash_warning("Not enough available players to generate pairings.")
    else:
        replace_round_pairings(db, tournament["id"], round_no, boards)
        db.execute(
            "UPDATE tournament SET status = CASE WHEN status = 'draft' THEN 'running' ELSE status END WHERE id = ?",
            (tournament["id"],),
        )
        db.commit()
        sync_member_statuses(db)
        flash_success(f"Generated pairings for round {round_no}.")
    return redirect(_admin_tournament_url(slug, round_no))


@bp.post("/admin/t/<slug>/round/<int:round_no>/save")
@login_required
def save_round(slug: str, round_no: int):
    db = get_db()
    tournament = _tournament_or_404(slug)
    if not _ensure_editable(tournament):
        return redirect(_admin_tournament_url(slug, round_no))
    ensure_round_status_rows(db, tournament["id"], tournament["rounds_planned"])
    availability = fetch_availability(db, tournament["id"])
    entries = fetch_entries(db, tournament["id"])
    active_entry_ids = {
        entry["id"]
        for entry in entries
        if entry["is_active"] and availability.get(entry["id"], {}).get(round_no, True)
    }
    existing_pairing_ids = {
        entry_id
        for pairing in fetch_pairings(db, tournament["id"], round_no)
        for entry_id in (pairing["white_entry_id"], pairing["black_entry_id"])
        if entry_id is not None
    }
    allowed_entry_ids = active_entry_ids | existing_pairing_ids
    board_count = parse_int(request.form.get("board_count"), default=ceil(len(active_entry_ids) / 2))
    try:
        boards = parse_manual_pairing_form(request.form, allowed_entry_ids, board_count)
    except ValueError as exc:
        if _wants_json():
            return jsonify({"ok": False, "message": str(exc)}), 400
        flash_error(str(exc))
        return redirect(_admin_tournament_url(slug, round_no))
    replace_round_pairings(db, tournament["id"], round_no, boards, manual_override=True)
    db.execute(
        "UPDATE tournament SET status = CASE WHEN status = 'draft' THEN 'running' ELSE status END WHERE id = ?",
        (tournament["id"],),
    )
    db.commit()
    sync_member_statuses(db)
    if _wants_json():
        payload = {"ok": True, "message": f"Round {round_no} saved."}
        payload.update(_admin_round_updates_payload(db, tournament, round_no))
        return jsonify(payload)
    flash_success(f"Saved round {round_no}.")
    return redirect(_admin_tournament_url(slug, round_no))


@bp.post("/admin/t/<slug>/complete")
@login_required
def complete_tournament(slug: str):
    db = get_db()
    open_round = parse_int(request.form.get("open_round"), default=None)
    tournament = _tournament_or_404(slug)
    if not _ensure_editable(tournament):
        return redirect(_admin_tournament_url(slug, open_round))
    db.execute("UPDATE tournament SET status = 'completed' WHERE id = ?", (tournament["id"],))
    db.commit()
    persist_final_standings(db, tournament["id"])
    try:
        rebuild_current_manager(db)
        flash_success("Tournament finished and the leaderboard was updated.")
    except Exception as exc:
        flash_warning(f"Tournament was finished, but the leaderboard update failed: {exc}")
    return redirect(_admin_tournament_url(slug, open_round))


@bp.route("/admin/t/<slug>/export.csv")
@login_required
def export_tournament(slug: str):
    tournament = _tournament_or_404(slug)
    return Response(
        _tournament_standings_csv(tournament),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{slug}-leaderboard.csv"'},
    )


@bp.route("/leaderboard.csv")
@bp.route("/anonymous-leaderboard.csv")
def export_anonymous_leaderboard():
    export_path = Path(current_app.config["EXPORT_DIR"]) / "anonymous_leaderboard.csv"
    if not export_path.exists():
        rows = anonymous_leaderboard_rows()
        header = "Rank,Name,Rating,Wins,Losses,Draws\n"
        body = "\n".join(
            f"{row['Rank']},{row['Name']},{row['Rating']},{row['Wins']},{row['Losses']},{row['Draws']}" for row in rows
        )
        return Response(f"{header}{body}\n", mimetype="text/csv")
    return send_file(export_path, mimetype="text/csv", as_attachment=True, download_name="anonymous_leaderboard.csv")
