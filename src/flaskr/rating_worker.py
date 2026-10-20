"""Entry point for a detached rating process; launched by rating_jobs."""
import json
import os
import sys
from pathlib import Path

from loguru import logger

from . import create_app
from .rating_jobs import run_pending_rating_updates


if __name__ == "__main__":
    os.nice(10)
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    config = json.loads(Path(sys.argv[1]).read_text())
    with create_app(config).app_context():
        run_pending_rating_updates()
