def split_text(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    """Split text into chunks of about chunk_size characters with overlap, on word boundaries."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if not 0 <= chunk_overlap < chunk_size:
        raise ValueError("chunk_overlap must be >= 0 and smaller than chunk_size")

    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        if end < len(text):
            last_space = text.rfind(" ", start, end)
            if last_space > start:
                end = last_space

        chunks.append(text[start:end].strip())
        if end >= len(text):
            break

        next_start = end - chunk_overlap
        next_space = text.find(" ", next_start, end)
        next_start = next_space + 1 if next_space != -1 else next_start
        start = max(next_start, start + 1)

    return [chunk for chunk in chunks if chunk]


def build_chunks(header: str, body: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    """Split the body and put the same header on top of every chunk."""
    pieces = split_text(body, chunk_size, chunk_overlap) or [""]
    return [f"{header}\n{piece}".strip() for piece in pieces]