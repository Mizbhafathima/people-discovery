import re
from typing import List
from urllib.parse import urlparse


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    netloc = parsed.netloc
    if not netloc:
        netloc = urlparse(f"https://{url}").netloc
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def normalize_url(url: str, base_domain: str) -> str:
    if re.match(r"^https?://", url, flags=re.IGNORECASE):
        return url
    if url.startswith("/"):
        return f"https://{base_domain}{url}"
    return url


def is_valid_email(email: str) -> bool:
    if "@" not in email:
        return False
    local, _, domain = email.partition("@")
    return bool(local) and "." in domain


def chunk_text(text: str, max_chars: int = 3000) -> List[str]:
    if max_chars <= 0:
        return [text] if text else []

    chunks: List[str] = []
    remaining = text.strip()

    while remaining:
        if len(remaining) <= max_chars:
            chunks.append(remaining)
            break

        split_idx = remaining.rfind(" ", 0, max_chars + 1)
        if split_idx <= 0:
            split_idx = max_chars

        chunk = remaining[:split_idx].strip()
        if chunk:
            chunks.append(chunk)

        remaining = remaining[split_idx:].strip()

    return chunks


def deduplicate_people(people: List[dict]) -> List[dict]:
    deduped: dict[str, dict] = {}
    unnamed_counter = 0

    for person in people:
        email = person.get("email")
        name = person.get("name")

        if email:
            key = f"email:{str(email).strip().lower()}"
        elif name:
            key = f"name:{str(name).strip().lower()}"
        else:
            key = f"unnamed:{unnamed_counter}"
            unnamed_counter += 1

        if key not in deduped:
            deduped[key] = dict(person)
            continue

        merged = deduped[key]
        for field, value in person.items():
            if merged.get(field) is None and value is not None:
                merged[field] = value

    return list(deduped.values())


def calculate_confidence(person: dict) -> int:
    has_email = bool(person.get("email"))
    has_name = bool(person.get("name"))
    has_phone = bool(person.get("phone"))
    has_postcode = bool(person.get("postcode"))

    if has_email and has_name and (has_phone or has_postcode):
        return 3
    if has_email and has_name:
        return 2
    return 1


def sanitize_text(text: str) -> str:
    no_html = re.sub(r"<[^>]+>", " ", text)
    normalized = re.sub(r"\s+", " ", no_html).strip()
    return normalized[:5000]
