import pytest

from rag_youtube.chunking import build_chunks, split_text

LONG_TEXT = " ".join(f"word{i}" for i in range(200))


@pytest.mark.parametrize("chunk_size", [0, -1])
def test_split_text_rejects_non_positive_chunk_size(chunk_size):
    with pytest.raises(ValueError):
        split_text("some text", chunk_size, 0)


@pytest.mark.parametrize("chunk_overlap", [-1, 10, 11])
def test_split_text_rejects_invalid_overlap(chunk_overlap):
    with pytest.raises(ValueError):
        split_text("some text", 10, chunk_overlap)


@pytest.mark.parametrize("text", ["", None, "   \n\t "])
def test_split_text_returns_empty_list_for_empty_text(text):
    assert split_text(text, 100, 10) == []


def test_split_text_returns_short_text_as_one_chunk():
    assert split_text("  short text  ", 100, 10) == ["short text"]


def test_split_text_chunks_do_not_exceed_chunk_size():
    chunks = split_text(LONG_TEXT, 100, 20)

    assert len(chunks) > 1
    assert all(len(chunk) <= 100 for chunk in chunks)


def test_split_text_cuts_on_word_boundaries():
    words = set(LONG_TEXT.split())

    for chunk in split_text(LONG_TEXT, 100, 20):
        assert all(word in words for word in chunk.split())


def test_split_text_keeps_all_words_in_order():
    words = LONG_TEXT.split()
    chunks = split_text(LONG_TEXT, 100, 20)

    covered = []
    for chunk in chunks:
        indices = [words.index(word) for word in chunk.split()]
        assert indices == list(range(indices[0], indices[-1] + 1))
        covered.extend(indices)

    assert sorted(set(covered)) == list(range(len(words)))
    assert [words.index(chunk.split()[0]) for chunk in chunks] == sorted(
        words.index(chunk.split()[0]) for chunk in chunks
    )


def test_split_text_consecutive_chunks_overlap():
    chunks = split_text(LONG_TEXT, 100, 20)

    for previous, current in zip(chunks, chunks[1:]):
        assert current.split()[0] in previous.split()


def test_split_text_without_overlap_has_no_repeated_words():
    chunks = split_text(LONG_TEXT, 100, 0)

    all_words = [word for chunk in chunks for word in chunk.split()]
    assert all_words == LONG_TEXT.split()


def test_split_text_handles_long_word_without_spaces():
    text = "x" * 250

    chunks = split_text(text, 100, 10)

    assert len(chunks) > 1
    assert all(len(chunk) <= 100 for chunk in chunks)
    assert chunks[-1].endswith("x")


def test_build_chunks_puts_header_on_every_chunk():
    header = "Title: Test\nChannel: Demo"

    chunks = build_chunks(header, LONG_TEXT, 100, 20)

    assert len(chunks) == len(split_text(LONG_TEXT, 100, 20))
    assert all(chunk.startswith(header + "\n") for chunk in chunks)


def test_build_chunks_with_empty_body_returns_only_header():
    assert build_chunks("Title: Test", "", 100, 20) == ["Title: Test"]


def test_build_chunks_with_empty_header_has_no_leading_newline():
    chunks = build_chunks("", "short body", 100, 20)

    assert chunks == ["short body"]
