import re

_LITERAL_NEWLINE = re.compile(r"\\[nr]")
_URL = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)
_NON_TEXT = re.compile(r"[^\w\s.,!?;:'\"()&%$#@/+\-]")
_WHITESPACE = re.compile(r"\s+")


def clean_text(text: str | None, placeholder: str = "No description") -> str:
    """Remove YouTube description noise: literal newlines, URLs, symbols, extra spaces."""
    if text is None or text.strip() == placeholder:
        return ""

    text = _LITERAL_NEWLINE.sub(" ", text)
    text = _URL.sub(" ", text)
    text = _NON_TEXT.sub(" ", text)
    text = _WHITESPACE.sub(" ", text)
    return text.strip()


def clean_tags(tags: str | None, empty_marker: str = "[none]") -> str:
    """Turn '"tag a"|"tag b"' into 'tag a, tag b' without duplicates."""
    if tags is None or tags.strip() in ("", empty_marker):
        return ""

    parts = [part.strip().strip('"').strip() for part in tags.split("|")]
    unique_parts = list(dict.fromkeys(part for part in parts if part))
    return ", ".join(unique_parts)