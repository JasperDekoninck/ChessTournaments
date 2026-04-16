from __future__ import annotations

from math import ceil
from pathlib import Path

from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

from .auth import login_required, set_admin_password, verify_password
from .core import (
    VALID_RESULTS,
    attach_entries_to_tournament,
    compute_standings,
    ensure_round_status_rows,
    fetch_active_tournament,
    fetch_availability,
    fetch_entries,
    fetch_pairings,
    fetch_public_tournaments,
    fetch_tournament_by_slug,
    fetch_tournaments,
    generate_swiss_pairings,
    latest_paired_round,
    next_round_to_pair,
    parse_manual_pairing_form,
    parse_registration_csv,
    parse_int,
    public_rounds,
    replace_round_pairings,
    set_active_tournament,
    slugify,
    unique_slug,
)
from .db import get_db
from .rating_integration import (
    anonymous_leaderboard_rows,
    build_matcher,
    get_player_history,
    rating_status,
    rebuild_current_manager,
    sync_historical_tournaments_from_saved_source,
)


bp = Blueprint("web", __name__)


def _tournament_or_404(slug: str):
    tournament = fetch_tournament_by_slug(get_db(), slug)
    if tournament is None:
        abort(404)
    return tournament


def _ensure_editable(tournament) -> bool:
    if tournament["is_historical"]:
        flash("Imported historical tournaments are read-only.")
        return False
    return True


def _round_view_context(tournament, selected_round: int | None = None, final_standings: bool = False):
    db = get_db()
    round_numbers = public_rounds(db, tournament["id"])
    latest_round = latest_paired_round(db, tournament["id"])
    if selected_round is None:
        selected_round = latest_round
    if selected_round is not None and selected_round not in round_numbers:
        abort(404)
    pairings = fetch_pairings(db, tournament["id"], selected_round) if selected_round is not None else []
    standings_round = None if final_standings else selected_round
    standings = compute_standings(db, tournament["id"], through_round=standings_round)
    return {
        "round_numbers": round_numbers,
        "selected_round": selected_round,
        "latest_round": latest_round,
        "pairings": pairings,
        "standings": standings,
    }


@bp.route("/")
def index():
    active = fetch_active_tournament(get_db())
    if active is None:
        return render_template(
            "public_empty.html",
            tournaments=fetch_public_tournaments(get_db()),
            leaderboard=anonymous_leaderboard_rows()[:20],
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
    tournaments = [
        tournament
        for tournament in fetch_public_tournaments(get_db())
        if not tournament["is_active_public"]
    ]
    return render_template("archive.html", tournaments=tournaments)


@bp.route("/t/<slug>")
def public_tournament(slug: str):
    tournament = _tournament_or_404(slug)
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
    tournament = _tournament_or_404(slug)
    context = _round_view_context(tournament, selected_round=round_no)
    return render_template(
        "public_tournament.html",
        tournament=tournament,
        archive=fetch_public_tournaments(get_db()),
        active_tournament=fetch_active_tournament(get_db()),
        **context,
    )


@bp.route("/ratings")
def ratings():
    return render_template("leaderboard.html", leaderboard=anonymous_leaderboard_rows())


@bp.route("/t/<slug>/player/<int:entry_id>")
def player_history(slug: str, entry_id: int):
    db = get_db()
    tournament = _tournament_or_404(slug)
    entry = db.execute(
        """
        SELECT e.id, e.imported_name, p.canonical_rating_name
        FROM tournament_entry e
        JOIN player p ON p.id = e.player_id
        WHERE e.id = ? AND e.tournament_id = ?
        """,
        (entry_id, tournament["id"]),
    ).fetchone()
    if entry is None:
        abort(404)
    return render_template(
        "player_history.html",
        tournament=tournament,
        entry=entry,
        games=get_player_history(entry["canonical_rating_name"] or entry["imported_name"]),
    )


@bp.route("/admin", methods=("GET", "POST"))
def admin():
    if request.method == "POST" and not session.get("is_admin"):
        password = request.form.get("password") or ""
        if verify_password(password):
            session.clear()
            session["is_admin"] = True
            session.permanent = True
            flash("Signed in.")
            return redirect(url_for("web.admin"))
        flash("Invalid password.")

    if not session.get("is_admin"):
        return render_template("admin_login.html")

    db = get_db()
    return render_template(
        "admin_dashboard.html",
        tournaments=fetch_tournaments(db),
        active_tournament=fetch_active_tournament(db),
        rating=rating_status(db),
        leaderboard=anonymous_leaderboard_rows()[:20],
    )


@bp.post("/admin/logout")
@login_required
def admin_logout():
    session.clear()
    flash("Signed out.")
    return redirect(url_for("web.admin"))


@bp.post("/admin/password")
@login_required
def admin_password():
    current_password = request.form.get("current_password") or ""
    new_password = request.form.get("new_password") or ""
    confirm_password = request.form.get("confirm_password") or ""
    if not verify_password(current_password):
        flash("Current password is incorrect.")
        return redirect(url_for("web.admin"))
    if len(new_password) < 10:
        flash("Choose a password with at least 10 characters.")
        return redirect(url_for("web.admin"))
    if new_password != confirm_password:
        flash("The new passwords do not match.")
        return redirect(url_for("web.admin"))
    set_admin_password(new_password)
    flash("Admin password updated.")
    return redirect(url_for("web.admin"))


@bp.post("/admin/tournaments")
@login_required
def create_tournament():
    db = get_db()
    name = (request.form.get("name") or "").strip()
    event_date = (request.form.get("event_date") or "").strip()
    rounds_planned = parse_int(request.form.get("rounds_planned"), default=7)
    csv_file = request.files.get("registrations")
    if not name or not event_date:
        flash("Tournament name and date are required.")
        return redirect(url_for("web.admin"))
    slug = unique_slug(db, slugify(name))
    cursor = db.execute(
        """
        INSERT INTO tournament (
          name, slug, event_date, rounds_planned, registration_csv_name, status,
          source_type, primary_tiebreak_label, secondary_tiebreak_label, is_public, is_active_public
        ) VALUES (?, ?, ?, ?, ?, 'draft', 'local', 'BH', 'BH-C1', 1, 0)
        """,
        (name, slug, event_date, rounds_planned, csv_file.filename if csv_file else None),
    )
    db.commit()
    if csv_file and csv_file.filename:
        attach_entries_to_tournament(db, cursor.lastrowid, parse_registration_csv(csv_file), build_matcher())
    if fetch_active_tournament(db) is None:
        set_active_tournament(db, cursor.lastrowid)
    flash("Tournament created.")
    return redirect(url_for("web.admin_tournament_detail", slug=slug))


@bp.route("/admin/t/<slug>")
@login_required
def admin_tournament_detail(slug: str):
    db = get_db()
    tournament = _tournament_or_404(slug)
    ensure_round_status_rows(db, tournament["id"], tournament["rounds_planned"])
    return render_template(
        "admin_tournament.html",
        tournament=tournament,
        entries=fetch_entries(db, tournament["id"]),
        availability=fetch_availability(db, tournament["id"]),
        standings=compute_standings(db, tournament["id"]),
        next_round=next_round_to_pair(db, tournament["id"], tournament["rounds_planned"]),
        latest_round=latest_paired_round(db, tournament["id"]),
    )


@bp.post("/admin/t/<slug>/activate")
@login_required
def admin_activate_tournament(slug: str):
    tournament = _tournament_or_404(slug)
    set_active_tournament(get_db(), tournament["id"])
    flash(f"{tournament['name']} is now the public tournament.")
    return redirect(url_for("web.admin"))


@bp.post("/admin/t/<slug>/import")
@login_required
def import_registrations(slug: str):
    db = get_db()
    tournament = _tournament_or_404(slug)
    if not _ensure_editable(tournament):
        return redirect(url_for("web.admin_tournament_detail", slug=slug))
    rows = parse_registration_csv(request.files.get("registrations"))
    attach_entries_to_tournament(db, tournament["id"], rows, build_matcher())
    flash(f"Imported {len(rows)} registrations.")
    return redirect(url_for("web.admin_tournament_detail", slug=slug))


@bp.post("/admin/t/<slug>/entries/<int:entry_id>/toggle")
@login_required
def toggle_entry(slug: str, entry_id: int):
    db = get_db()
    tournament = _tournament_or_404(slug)
    if not _ensure_editable(tournament):
        return redirect(url_for("web.admin_tournament_detail", slug=slug))
    db.execute(
        """
        UPDATE tournament_entry
        SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END
        WHERE id = ? AND tournament_id = ?
        """,
        (entry_id, tournament["id"]),
    )
    db.commit()
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
    db.execute(
        """
        UPDATE entry_round_status
        SET is_available = CASE WHEN is_available = 1 THEN 0 ELSE 1 END
        WHERE entry_id = ? AND round_no = ?
        """,
        (entry_id, round_no),
    )
    db.commit()
    return redirect(url_for("web.admin_tournament_detail", slug=slug))


@bp.route("/admin/t/<slug>/round/<int:round_no>")
@login_required
def admin_round_detail(slug: str, round_no: int):
    db = get_db()
    tournament = _tournament_or_404(slug)
    ensure_round_status_rows(db, tournament["id"], tournament["rounds_planned"])
    entries = fetch_entries(db, tournament["id"])
    availability = fetch_availability(db, tournament["id"])
    active_entries = [
        entry
        for entry in entries
        if entry["is_active"] and availability.get(entry["id"], {}).get(round_no, True)
    ]
    pairings = fetch_pairings(db, tournament["id"], round_no)
    board_count = max(len(pairings), ceil(len(active_entries) / 2))
    pairing_by_board = {pairing["board_no"]: pairing for pairing in pairings}
    board_rows = [
        pairing_by_board.get(
            board_no,
            {"board_no": board_no, "white_entry_id": None, "black_entry_id": None, "result_code": None},
        )
        for board_no in range(1, board_count + 1)
    ]
    return render_template(
        "admin_round.html",
        tournament=tournament,
        round_no=round_no,
        active_entries=active_entries,
        board_rows=board_rows,
        board_count=board_count,
        valid_results=sorted(VALID_RESULTS),
    )


@bp.post("/admin/t/<slug>/round/<int:round_no>/generate")
@login_required
def generate_round(slug: str, round_no: int):
    db = get_db()
    tournament = _tournament_or_404(slug)
    if not _ensure_editable(tournament):
        return redirect(url_for("web.admin_round_detail", slug=slug, round_no=round_no))
    boards = generate_swiss_pairings(db, tournament["id"], round_no)
    if not boards:
        flash("Not enough available players to generate pairings.")
    else:
        replace_round_pairings(db, tournament["id"], round_no, boards)
        db.execute(
            "UPDATE tournament SET status = CASE WHEN status = 'draft' THEN 'running' ELSE status END WHERE id = ?",
            (tournament["id"],),
        )
        db.commit()
        flash(f"Generated pairings for round {round_no}.")
    return redirect(url_for("web.admin_round_detail", slug=slug, round_no=round_no))


@bp.post("/admin/t/<slug>/round/<int:round_no>/save")
@login_required
def save_round(slug: str, round_no: int):
    db = get_db()
    tournament = _tournament_or_404(slug)
    if not _ensure_editable(tournament):
        return redirect(url_for("web.admin_round_detail", slug=slug, round_no=round_no))
    ensure_round_status_rows(db, tournament["id"], tournament["rounds_planned"])
    availability = fetch_availability(db, tournament["id"])
    entries = fetch_entries(db, tournament["id"])
    active_entry_ids = {
        entry["id"]
        for entry in entries
        if entry["is_active"] and availability.get(entry["id"], {}).get(round_no, True)
    }
    board_count = parse_int(request.form.get("board_count"), default=ceil(len(active_entry_ids) / 2))
    try:
        boards = parse_manual_pairing_form(request.form, active_entry_ids, board_count)
    except ValueError as exc:
        flash(str(exc))
        return redirect(url_for("web.admin_round_detail", slug=slug, round_no=round_no))
    replace_round_pairings(db, tournament["id"], round_no, boards, manual_override=True)
    db.execute(
        "UPDATE tournament SET status = CASE WHEN status = 'draft' THEN 'running' ELSE status END WHERE id = ?",
        (tournament["id"],),
    )
    db.commit()
    flash(f"Saved round {round_no}.")
    return redirect(url_for("web.admin_round_detail", slug=slug, round_no=round_no))


@bp.post("/admin/t/<slug>/complete")
@login_required
def complete_tournament(slug: str):
    db = get_db()
    tournament = _tournament_or_404(slug)
    if not _ensure_editable(tournament):
        return redirect(url_for("web.admin_tournament_detail", slug=slug))
    db.execute("UPDATE tournament SET status = 'completed' WHERE id = ?", (tournament["id"],))
    db.commit()
    try:
        rebuild_current_manager(db)
        flash("Tournament marked complete and ratings rebuilt.")
    except Exception as exc:
        flash(f"Tournament marked complete, but rating rebuild failed: {exc}")
    return redirect(url_for("web.admin_tournament_detail", slug=slug))


@bp.post("/admin/ratings/rebuild")
@login_required
def rebuild_ratings():
    db = get_db()
    rebuild_current_manager(db)
    flash("Rebuilt local rating state.")
    return redirect(url_for("web.admin"))


@bp.post("/admin/history/sync")
@login_required
def sync_history():
    count = sync_historical_tournaments_from_saved_source()
    if count:
        flash(f"Synchronized {count} historical tournaments from the original source files.")
    else:
        flash("No historical source is configured yet.")
    return redirect(url_for("web.admin"))


@bp.route("/admin/t/<slug>/export.csv")
@login_required
def export_tournament(slug: str):
    tournament = _tournament_or_404(slug)
    export_path = Path(current_app.config["EXPORT_DIR"]) / "tournaments" / slug / "leaderboard.csv"
    if not export_path.exists():
        flash("No export exists yet for this tournament. Complete the tournament first.")
        return redirect(url_for("web.admin_tournament_detail", slug=slug))
    return send_file(export_path, mimetype="text/csv", as_attachment=True, download_name=f"{slug}-leaderboard.csv")


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
