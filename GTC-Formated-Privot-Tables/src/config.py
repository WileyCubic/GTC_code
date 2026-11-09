"""Regex patterns for the pivot tables (sizes, colors, organization names)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import re

from gtc_common.patterns import word_pattern

PATTERN_FILE = Path(__file__).parent / "assets" / "regex_patterns.json"


@dataclass(frozen=True)
class Patterns:
    size: re.Pattern
    color: re.Pattern
    organization: re.Pattern


@lru_cache(maxsize=1)
def patterns() -> Patterns:
    lists = json.loads(PATTERN_FILE.read_text(encoding="utf-8"))
    return Patterns(
        size=word_pattern(lists["sizes"]),
        color=word_pattern(lists["colors"]),
        organization=word_pattern(lists["organization_names"]),
    )
