from gtc_common.patterns import partial_pattern, word_pattern
from Sales_ETL.Pipelines.line_item_analysis.transform import patterns as lineitem_patterns


def test_apostrophe_s_is_not_a_size():
    sizes = word_pattern(["S", "M", "L", "XL"])
    assert sizes.search("Men's Hoodie") is None
    assert sizes.search("Men's Hoodie - S").group(1) == "S"


def test_longest_alternative_wins():
    colors = word_pattern(["White", "White/Blue"])
    assert colors.search("Tee - White/Blue").group(1) == "White/Blue"


def test_partial_pattern_matches_inside_words():
    assert partial_pattern(["hat"]).search("NCL truckerhat").group(1) == "hat"


def test_lineitem_pattern_file_loads():
    p = lineitem_patterns()
    assert set(p) == {"size", "color", "organization", "garment"}
