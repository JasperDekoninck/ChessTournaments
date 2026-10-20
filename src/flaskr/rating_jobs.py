"""Persistent, coalesced rating updates in a separate, low-priority process."""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path

from flask import current_app

from .db import get_db


class RatingUpdateBusy(RuntimeError):
    pass


def _lock_file(name):
    root = Path(current_app.config["RATING_DATA_DIR"])
    root.mkdir(parents=True, exist_ok=True)
    lock = (root / name).open("a")
    if os.geteuid() == 0:
        owner = root.stat()
        os.fchown(lock.fileno(), owner.st_uid, owner.st_gid)
    return lock


@contextmanager
def rating_update_lock():
    with _lock_file("rebuild.lock") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RatingUpdateBusy("A rating update is already running.") from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _worker_running():
    with _lock_file("worker.lock") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(lock, fcntl.LOCK_UN)
        return False


def rating_job_status():
    db = get_db()
    row = db.execute("SELECT * FROM rating_job WHERE id = 1").fetchone()
    if row is None:
        return None
    if row["status"] in {"queued", "running"} and not _worker_running():
        # A queued process needs time to start Python and import NumPy. Running
        # workers already own the lock, so its absence means they were stopped.
        db.execute(
            """
            UPDATE rating_job SET status = 'failed',
                message = 'The rating update was interrupted. Please retry.', updated_at = CURRENT_TIMESTAMP
            WHERE id = 1 AND requested_version = ? AND status = ?
              AND (status = 'running' OR updated_at < datetime('now', '-60 seconds'))
            """,
            (row["requested_version"], row["status"]),
        )
        db.commit()
        row = db.execute("SELECT * FROM rating_job WHERE id = 1").fetchone()
    return dict(row)


def _start_worker():
    root = Path(current_app.instance_path)
    config = {
        key: str(Path(current_app.config[key]).resolve())
        for key in ("DATABASE", "RATING_DATA_DIR", "EXPORT_DIR")
    }
    config.update(INSTANCE_PATH=str(root), MAIL_ENABLED=False)
    config_path = root / "rating-worker.json"
    with tempfile.NamedTemporaryFile(mode="w", dir=root, delete=False) as handle:
        temp_path = Path(handle.name)
        try:
            json.dump(config, handle)
            handle.close()
            os.replace(temp_path, config_path)
        finally:
            temp_path.unlink(missing_ok=True)
    env = os.environ.copy()
    # Avoid numerical libraries starting a thread for every CPU on a small VM.
    env.update(OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", MKL_NUM_THREADS="1")
    with (root / "rating-worker.log").open("a") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "flaskr.rating_worker", str(config_path)],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log,
            start_new_session=True, close_fds=True, env=env,
        )
    threading.Thread(target=process.wait, daemon=True).start()
    return process


def request_rating_update(*, full_recompute=False):
    db = get_db()
    rating_job_status()
    db.execute("BEGIN IMMEDIATE")
    row = db.execute("SELECT * FROM rating_job WHERE id = 1").fetchone()
    if full_recompute and row is not None and row["status"] in {"queued", "running"} and row["full_recompute_version"] > row["completed_version"]:
        db.commit()
        return
    start = row is None or row["status"] not in {"queued", "running"}
    db.execute(
        """
        INSERT INTO rating_job (id, requested_version, full_recompute_version, status, message)
        VALUES (1, 1, ?, 'queued', 'Waiting to update ratings…')
        ON CONFLICT(id) DO UPDATE SET
            requested_version = requested_version + 1,
            full_recompute_version = CASE WHEN ? THEN requested_version + 1 ELSE full_recompute_version END,
            status = CASE WHEN status IN ('queued', 'running') THEN status ELSE 'queued' END,
            message = CASE WHEN status IN ('queued', 'running') THEN message ELSE 'Waiting to update ratings…' END,
            updated_at = CURRENT_TIMESTAMP
        """,
        (int(full_recompute), int(full_recompute)),
    )
    db.commit()
    if current_app.config.get("RATING_JOBS_EAGER"):
        run_pending_rating_updates()
    elif start:
        try:
            _start_worker()
        except OSError:
            current_app.logger.exception("Could not start the rating worker")
            db.execute(
                "UPDATE rating_job SET status = 'failed', message = 'Could not start the rating update. Please retry.' WHERE id = 1 AND status = 'queued'"
            )
            db.commit()


def run_pending_rating_updates():
    from .rating_integration import rebuild_current_manager

    db = get_db()
    with _lock_file("worker.lock") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        try:
            while True:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT * FROM rating_job WHERE id = 1").fetchone()
                if row is None or row["requested_version"] <= row["completed_version"]:
                    # Release while holding the short DB transaction so a new
                    # request cannot launch a worker before this one lets go.
                    fcntl.flock(lock, fcntl.LOCK_UN)
                    db.commit()
                    return
                version = row["requested_version"]
                full_recompute = row["full_recompute_version"] > row["completed_version"]
                db.execute("UPDATE rating_job SET status = 'running', message = 'Updating ratings…', updated_at = CURRENT_TIMESTAMP WHERE id = 1")
                db.commit()

                def progress(message):
                    db.execute("UPDATE rating_job SET message = ?, updated_at = CURRENT_TIMESTAMP WHERE id = 1", (message,))
                    db.commit()

                rebuild_current_manager(db, progress=progress, full_recompute=full_recompute)
                db.execute("BEGIN IMMEDIATE")
                latest = db.execute("SELECT requested_version FROM rating_job WHERE id = 1").fetchone()[0]
                done = latest == version
                db.execute(
                    "UPDATE rating_job SET completed_version = ?, status = ?, message = ?, updated_at = CURRENT_TIMESTAMP WHERE id = 1",
                    (version, "completed" if done else "running", "Ratings and tournament statistics are up to date." if done else "Updating the latest tournament changes…"),
                )
                if done:
                    fcntl.flock(lock, fcntl.LOCK_UN)
                db.commit()
                if done:
                    return
        except Exception:
            db.rollback()
            current_app.logger.exception("Background rating update failed")
            db.execute(
                "UPDATE rating_job SET status = 'failed', message = 'The rating update failed. Please retry; details are in rating-worker.log.', updated_at = CURRENT_TIMESTAMP WHERE id = 1"
            )
            db.commit()
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


