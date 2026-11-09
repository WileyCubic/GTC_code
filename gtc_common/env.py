"""Single source of configuration: the ``.env`` file at the repo root.

Every project calls these helpers instead of ``os.getenv`` directly so that

* the root ``.env`` is always found, no matter which folder a script runs from;
* a missing setting fails with a clear message naming the variable,
  instead of a confusing ``Path(None)`` TypeError deep inside a module;
* path settings may be written relative to the repo root in ``.env``.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"

load_dotenv(ENV_FILE)


class MissingSetting(RuntimeError):
    """Raised when a required variable is not set in the root .env."""


def get(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def require(name: str) -> str:
    value = get(name)
    if value is None:
        raise MissingSetting(
            f"Setting '{name}' is missing. Add it to {ENV_FILE} "
            f"(see .env.example and docs/ENV.md for what it should contain)."
        )
    return value


def _to_path(value: str) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else REPO_ROOT / path


def path(name: str, default: str | None = None) -> Path:
    """Read a path setting. Relative paths are resolved against the repo root."""
    value = get(name, default)
    if value is None:
        require(name)  # raises with a helpful message
    return _to_path(value)


def optional_path(name: str) -> Path | None:
    value = get(name)
    return _to_path(value) if value else None


def path_list(name: str) -> list[Path]:
    """Read a ``|``-separated list of paths."""
    return [_to_path(p.strip()) for p in require(name).split("|") if p.strip()]


def str_list(name: str) -> list[str]:
    """Read a ``|``-separated list of strings (empty list when unset)."""
    value = get(name)
    if not value:
        return []
    return [v.strip() for v in value.split("|") if v.strip()]
