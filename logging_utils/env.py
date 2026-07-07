from pathlib import Path

from dotenv import load_dotenv


def load_root_env(override: bool = False) -> Path:
    """Load the single shared .env at the repo root, regardless of cwd."""
    root = Path(__file__).resolve().parent.parent
    load_dotenv(dotenv_path=root / ".env", override=override)
    return root
