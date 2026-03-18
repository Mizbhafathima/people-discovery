import re

EMAIL_PATTERN = re.compile(
    r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b",
    re.IGNORECASE,
)

PHONE_PATTERNS = [
    re.compile(r"\+\d{1,3}[\s\-]?\(?\d[\d\s\-()]{6,}\d\b"),
    re.compile(r"\b(?:01\d{3}\s?\d{6}|02\d\s?\d{4}\s?\d{4})\b"),
    re.compile(r"\b07\d{3}\s?\d{6}\b"),
    re.compile(r"\b(?:\(\d{3}\)\s?\d{3}-\d{4}|\d{3}-\d{3}-\d{4})\b"),
]

POSTCODE_PATTERNS = {
    "UK": re.compile(r"\b(?:GIR\s?0AA|[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\b", re.IGNORECASE),
    "US": re.compile(r"\b\d{5}(?:-\d{4})?\b"),
    "IN": re.compile(r"\b\d{6}\b"),
    "GENERIC": re.compile(r"\b\d{4,8}\b"),
}

LINKEDIN_PATTERN = re.compile(
    r"https?://(?:[a-z]{2,3}\.)?(?:www\.)?linkedin\.com/in/[A-Za-z0-9\-_%]+(?:/[A-Za-z0-9\-_%]+)*/?",
    re.IGNORECASE,
)

SOCIAL_PATTERNS = {
    "twitter": re.compile(r"https?://(?:www\.)?(?:twitter\.com|x\.com)/[A-Za-z0-9_]{1,15}/?", re.IGNORECASE),
    "github": re.compile(r"https?://(?:www\.)?github\.com/[A-Za-z0-9-]+/?", re.IGNORECASE),
}


def clean_phone(phone_str: str) -> str:
    return re.sub(r"[^0-9+()\-\s]", "", phone_str).strip()
