import pytest

from rag_youtube.text_cleaning import clean_tags, clean_text


@pytest.mark.parametrize("value", [None, "No description", "  No description  "])
def test_clean_text_returns_empty_for_missing_description(value):
    assert clean_text(value) == ""


def test_clean_text_uses_custom_placeholder():
    assert clean_text("n/a", placeholder="n/a") == ""
    assert clean_text("No description", placeholder="n/a") == "No description"


def test_clean_text_replaces_literal_newlines():
    assert clean_text(r"line one\nline two\rline three") == "line one line two line three"


def test_clean_text_removes_urls():
    text = "Watch https://youtu.be/abc and http://example.com/x?y=1 or www.site.org now"

    assert clean_text(text) == "Watch and or now"


def test_clean_text_removes_emoji_and_symbols():
    assert clean_text("Great video 🔥🔥 ★ ♥ end") == "Great video end"


def test_clean_text_keeps_allowed_punctuation():
    text = "Hi, it's \"me\" (really)! 50% & $5 #tag @user a/b +1 - ok? yes; no: done."

    assert clean_text(text) == text


def test_clean_text_collapses_whitespace_and_strips():
    assert clean_text("  many    spaces\t\tand\n\nlines  ") == "many spaces and lines"


@pytest.mark.parametrize("value", [None, "", "   ", "[none]", " [none] "])
def test_clean_tags_returns_empty_for_missing_tags(value):
    assert clean_tags(value) == ""


def test_clean_tags_uses_custom_empty_marker():
    assert clean_tags("-", empty_marker="-") == ""
    assert clean_tags("[none]", empty_marker="-") == "[none]"


def test_clean_tags_joins_quoted_tags():
    assert clean_tags('"tag a"|"tag b"') == "tag a, tag b"


def test_clean_tags_removes_duplicates_keeping_first_order():
    assert clean_tags('"b"|"a"|"b"|"c"|"a"') == "b, a, c"


def test_clean_tags_drops_empty_parts():
    assert clean_tags('"a"||" "|""|"b"') == "a, b"
