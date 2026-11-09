"""Regex builders for pulling size / color / organization out of item names.

Used by the Sales ETL line-item analysis and the formatted pivot tables.
"""
from __future__ import annotations

import re


def _alternation(words: list[str]) -> str:
    # Longest first: Python's regex takes the first alternative that matches,
    # so "White" listed before "White/Blue" used to hide "White/Blue" forever.
    return "|".join(re.escape(w) for w in sorted(set(words), key=len, reverse=True))


def word_pattern(words: list[str]) -> re.Pattern:
    """Whole-word, case-insensitive match. Apostrophes count as part of a word,
    so the "s" in "Men's" is not read as size S (it was with a plain ``\\b``)."""
    return re.compile(rf"(?<![\w'])({_alternation(words)})(?![\w'])", re.IGNORECASE)


def partial_pattern(words: list[str]) -> re.Pattern:
    """Match anywhere, e.g. garment "hat" inside "truckerhat"."""
    return re.compile(rf"({_alternation(words)})", re.IGNORECASE)
