from dataclasses import dataclass, field
import json
from pathlib import Path
import re
import os

from dotenv import load_dotenv
load_dotenv()

#----------------------------------------#
# Classes for path configuration
#----------------------------------------#
    
#come back to this later    
@dataclass
class QueryPathsConfig:
    lineitem_analysis: Path = Path(os.getenv('lineitem_analysis_query'))

#----------------------------------------#    
# Classes for pipeline configuration
#----------------------------------------#
    
@dataclass
class LineitemAnalysisPipelineConfig:
    table_name: str = os.getenv('lineitem_analysis')

#----------------------------------------#
# Classes for regex patterns
#----------------------------------------#

PATTERN_FILE = Path(os.getenv('Sales_ETL_lineitem_patterns'))


def _load_pattern_lists() -> dict:
    with PATTERN_FILE.open() as pattern_file:
        return json.load(pattern_file)


def _compile_word_pattern(words: list[str]) -> re.Pattern:
    # Build a readable alternation and compile with word boundaries.
    choices = '|'.join(re.escape(word) for word in words)
    return re.compile(rf'\b({choices})\b', re.IGNORECASE)


def _compile_partial_pattern(words: list[str]) -> re.Pattern:
    # Allow garment matches to appear anywhere inside larger tokens (e.g., "hat" in "truckerhat").
    choices = '|'.join(re.escape(word) for word in words)
    return re.compile(rf'({choices})', re.IGNORECASE)


@dataclass
class LineitemPatternsConfig:
    _pattern_lists: dict = field(default_factory=_load_pattern_lists, repr=False)
    size_pattern: re.Pattern = field(init=False)
    color_pattern: re.Pattern = field(init=False)
    organization_pattern: re.Pattern = field(init=False)
    garment_patterns: re.Pattern = field(init=False)

    def __post_init__(self) -> None:
        lists = self._pattern_lists
        self.size_pattern = _compile_word_pattern(lists['sizes'])
        self.color_pattern = _compile_word_pattern(lists['colors'])
        self.organization_pattern = _compile_word_pattern(lists['organization_names'])
        self.garment_patterns = _compile_partial_pattern(lists['garments'])

#----------------------------------------#
# Functions to load configurations
#----------------------------------------#

def path_config() -> QueryPathsConfig:
    query_paths = QueryPathsConfig()
    return query_paths

def pipeline_config() -> LineitemAnalysisPipelineConfig:
    lineitem_pipeline = LineitemAnalysisPipelineConfig()
    return lineitem_pipeline

def lineitem_patterns_config() -> LineitemPatternsConfig:
    patterns = LineitemPatternsConfig()
    return patterns