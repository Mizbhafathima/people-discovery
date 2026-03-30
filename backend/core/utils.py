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
    def normalize_name(name: str) -> str:
        if not name:
            return ""

        normalized = str(name).strip().lower()
        prefixes = ["mr.", "mrs.", "ms.", "dr.", "prof.", "mr ", "mrs ", "ms ", "dr "]
        changed = True
        while changed:
            changed = False
            for prefix in prefixes:
                if normalized.startswith(prefix):
                    normalized = normalized[len(prefix) :].strip()
                    changed = True
        return normalized

    deduped: dict[str, dict] = {}
    unnamed_counter = 0

    for person in people:
        email = person.get("email")
        name = person.get("name")

        if email:
            key = str(email).strip().lower()
        elif name:
            key = normalize_name(str(name))
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
    has_name = bool(person.get("name"))
    has_job_title = bool(person.get("job_title"))

    if has_name and has_job_title:
        return 2
    if has_name:
        return 1
    return 0


def looks_like_people_page(url: str) -> bool:
    """Heuristic: returns True if URL likely represents people/team content."""
    if not url:
        return False
    u = url.lower()
    signals = [
        "team",
        "leadership",
        "management",
        "executive",
        "board",
        "our-story",
        "about",
        "people",
        "staff",
        "directors",
    ]
    return any(s in u for s in signals)


def sanitize_text(text: str) -> str:
    no_html = re.sub(r"<[^>]+>", " ", text)
    normalized = re.sub(r"\s+", " ", no_html).strip()
    return normalized[:5000]


def estimate_tokens(text: str) -> int:
    """Estimates token count of text using character-based approximation."""
    return max(1, len(text) // 4)


def extract_people_sections(text: str) -> str:
    """Pre-filters page text to extract only sections likely to contain people data."""
    lines = text.splitlines()

    title_signals = [
        "ceo",
        "cto",
        "cfo",
        "coo",
        "founder",
        "co-founder",
        "cofounder",
        "director",
        "manager",
        "head of",
        "vp ",
        "vice president",
        "president",
        "engineer",
        "lead",
        "principal",
        "partner",
        "associate",
        "consultant",
        "chairman",
        "board",
        "executive",
        "officer",
        "chief",
    ]
    section_signals = [
        "team",
        "leadership",
        "people",
        "staff",
        "about us",
        "our team",
        "meet",
        "management",
        "executives",
        "board of",
        "who we are",
    ]

    anchor_indices = []
    for idx, line in enumerate(lines):
        lower_line = line.lower()
        is_anchor = (
            any(signal in lower_line for signal in title_signals)
            or any(signal in lower_line for signal in section_signals)
            or "@" in line
            or "linkedin.com" in lower_line
        )
        if is_anchor:
            anchor_indices.append(idx)

    collected_lines: List[str] = []
    seen = set()
    for anchor_idx in anchor_indices:
        start = max(0, anchor_idx - 3)
        end = min(len(lines), anchor_idx + 4)
        for line in lines[start:end]:
            if line not in seen:
                seen.add(line)
                collected_lines.append(line)

    result = "\n".join(collected_lines)
    if len(result) < 100:
        return text[:8000]

    people_name_pattern = re.compile(
        r"\b(?:Mr|Mrs|Ms|Dr|Prof)\.?\s*[A-Z][A-Za-z'’-]+|\b[A-Z][A-Za-z'’-]+\s+[A-Z][A-Za-z'’-]+\b"
    )
    source_name_count = len(people_name_pattern.findall(text))
    result_name_count = len(people_name_pattern.findall(result))

    # If the focused slice lost most likely names, keep a larger slice of the original page.
    if source_name_count >= 3 and result_name_count < max(2, source_name_count // 2):
        return text[:16000]

    # Keep more context for profile-heavy pages so later extractors can pair names with titles.
    return result[:12000]


def score_url_for_people(url: str) -> int:
    """Heuristic score for how likely a URL contains people/team data."""
    url_lower = url.lower()
    score = 0

    high_value_paths = [
        "/team",
        "/leadership",
        "/people",
        "/staff",
        "/about-us",
        "/about/team",
        "/our-team",
        "/meet-the-team",
        "/management",
        "/executives",
        "/board",
        "/directors",
        "/founders",
        "/partners",
    ]
    if any(path in url_lower for path in high_value_paths):
        return 10

    medium_value_paths = [
        "/about",
        "/company",
        "/who-we-are",
        "/our-story",
        "/contact",
    ]
    if any(path in url_lower for path in medium_value_paths):
        score = 5

    low_value_paths = [
        "/blog",
        "/news",
        "/press",
        "/product",
        "/pricing",
        "/careers",
        "/jobs",
        "/legal",
        "/privacy",
        "/terms",
        "/cookie",
        "/login",
        "/signup",
        "/register",
        "/api",
        "/docs",
        "/support",
        "/help",
        "/faq",
        "/sitemap",
        ".pdf",
        ".jpg",
        ".png",
        ".css",
        ".js",
    ]
    if any(path in url_lower for path in low_value_paths):
        return 0

    return score
