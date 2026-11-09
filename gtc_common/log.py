"""One logging setup for every GTC script.

Each run writes to ``<LOG_DIR>/<project>.log`` (rotating, all levels) and
prints INFO and above to the console. Pipelines that belong to a group
(e.g. the two sales pipelines) can also write a shared summary log.
"""
from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

from gtc_common import env

_FMT_FILE = "%(asctime)s - [%(project)s] - %(name)s - %(levelname)s - %(message)s"
_FMT_CONSOLE = "%(levelname)s: %(message)s"


class _ProjectFilter(logging.Filter):
    def __init__(self, project: str):
        super().__init__()
        self.project = project

    def filter(self, record: logging.LogRecord) -> bool:
        record.project = self.project
        return True


def log_dir() -> Path:
    directory = env.path("LOG_DIR", "logs")
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def setup_logging(project: str, summary_log: str | None = None) -> logging.Logger:
    """Configure the root logger for a script and return the project logger.

    Args:
        project: short name, used for the log file name (``<project>.log``).
        summary_log: optional extra log file name (INFO+) shared by several projects.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    root.setLevel(logging.DEBUG)
    project_filter = _ProjectFilter(project)

    def _file_handler(file_name: str, level: int) -> logging.Handler:
        handler = logging.handlers.RotatingFileHandler(
            log_dir() / file_name, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(_FMT_FILE))
        handler.addFilter(project_filter)
        return handler

    root.addHandler(_file_handler(f"{project}.log", logging.DEBUG))
    if summary_log:
        root.addHandler(_file_handler(summary_log, logging.INFO))

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_FMT_CONSOLE))
    root.addHandler(console)

    # Third-party libraries are noisy at DEBUG.
    for noisy in ("urllib3", "matplotlib", "PIL", "werkzeug"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger(project)
