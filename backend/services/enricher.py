from typing import List, Optional
import logging
import re

from rapidfuzz import fuzz

from backend.core.utils import calculate_confidence, looks_like_people_page


HONORIFIC_PREFIXES = [
    "mr.",
    "mrs.",
    "ms.",
    "dr.",
    "prof.",
    "mr ",
    "mrs ",
    "ms ",
    "dr ",
    "prof ",
]


def _normalize_honorific_spacing(value: Optional[str]) -> Optional[str]:
    """Normalizes honorifics like 'Mr.Smith' -> 'Mr. Smith'."""
    text = normalize_field(value)
    if not text:
        return text

    text = re.sub(r"\b(Mr|Mrs|Ms|Dr|Prof)\.\s*", r"\1. ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(Mr|Mrs|Ms|Dr|Prof)\s+(?=[A-Z])", r"\1. ", text, flags=re.IGNORECASE)
    return " ".join(text.split()).strip()


def _has_honorific_single_name_shape(name: str) -> bool:
    """Allows names like 'Dr. Muneef' or 'Mr. Sakthi' when sites omit surnames."""
    text = _normalize_honorific_spacing(name) or ""
    if not text:
        return False

    parts = text.split()
    if len(parts) != 2:
        return False

    prefix = parts[0].lower()
    token = parts[1]
    return prefix in {"mr.", "mrs.", "ms.", "dr.", "prof."} and bool(
        re.fullmatch(r"[A-Za-z][A-Za-z'’-]{1,40}", token)
    )


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = (_normalize_honorific_spacing(name) or "").lower()
    for prefix in HONORIFIC_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix):]

    # Detect and fix doubled names like "Marco Dorna Marco Dorna"
    words = name.split()
    half = len(words) // 2
    if half > 0 and len(words) % 2 == 0:
        first_half = " ".join(words[:half])
        second_half = " ".join(words[half:])
        if first_half == second_half:
            name = first_half

    return name.strip()


def normalize_email(email: str) -> str:
    if not email:
        return ""
    return " ".join(email.split()).lower().strip()


def normalize_field(value) -> Optional[str]:
    if not value or not isinstance(value, str):
        return None
    result = " ".join(value.split()).strip()
    return result if result else None


def _strip_name_prefix_from_title(name: Optional[str], job_title: Optional[str]) -> Optional[str]:
    """Removes a duplicated leading name from title text."""
    if not name or not job_title:
        return job_title

    n = " ".join(name.split()).strip()
    t = " ".join(job_title.split()).strip()
    if not n or not t:
        return job_title

    if t.lower().startswith(n.lower() + " "):
        trimmed = t[len(n):].strip(" ,-:")
        return trimmed or None
    return job_title


def _split_combined_name_title(value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Attempts to split strings like 'Jane Doe Chief Financial Officer'."""
    text = normalize_field(value)
    if not text:
        return None, None

    match = re.match(
        r"^([A-Z][A-Za-z'’.-]+(?:\s+[A-Z][A-Za-z'’.-]+){1,3})\s+(.+)$",
        text,
    )
    if not match:
        return None, None

    candidate_name = normalize_field(match.group(1))
    candidate_title = normalize_field(match.group(2))
    if not candidate_name or not candidate_title:
        return None, None

    role_tokens = [
        "chief", "officer", "director", "president", "vice president",
        "head", "manager", "lead", "partner", "counsel", "chair", "ceo",
        "cto", "cfo", "coo", "founder", "engineer", "scientist", "analyst",
    ]
    low_title = candidate_title.lower()
    has_role_signal = any(token in low_title for token in role_tokens)

    if is_real_person_name(candidate_name) and has_role_signal and is_plausible_job_title(candidate_title):
        return candidate_name, candidate_title

    return None, None


def is_valid_person_name(name: str) -> bool:
    """Returns True only if name looks like a real human name."""
    if not name:
        return False

    name = (_normalize_honorific_spacing(name) or "").strip()

    # Must be at least 2 chars
    if len(name) < 2:
        return False

    # Must not be too long (real names are under 60 chars)
    if len(name) > 60:
        return False

    # Must not start with a number
    if name[0].isdigit():
        return False

    # Must not contain emoji
    if re.search(r"[^\x00-\x7F]", name):
        # Allow non-ASCII letters (international names like Seb)
        # but reject emoji and symbols
        if re.search(r"[\U00010000-\U0010ffff]", name):
            return False

    # Must not be ALL CAPS (likely acronym or header)
    if name.isupper() and len(name) > 5:
        return False

    # Must not contain these characters (navigation/URL artifacts)
    bad_chars = ["|", "/", "\\", "@", "#", "&", ">", "<", "{", "}", "[", "]"]
    if any(c in name for c in bad_chars):
        return False

    if _has_honorific_single_name_shape(name):
        return True

    # Generic non-person words that appear as names
    generic_words = {
        "doctor",
        "nurse",
        "engineer",
        "developer",
        "designer",
        "manager",
        "director",
        "analyst",
        "consultant",
        "specialist",
        "officer",
        "assistant",
        "coordinator",
        "executive",
        "associate",
        "founder",
        "partner",
        "chairman",
        "president",
        "secretary",
        "medical services",
        "medical tourism",
        "legal policies",
        "emergency hotline",
        "healthcare specialists",
        "contact us",
        "about us",
        "our team",
        "our story",
        "privacy policy",
        "terms of service",
        "cookie policy",
        "data protection",
        "legal disclaimer",
        "all rights reserved",
        "get in touch",
        "follow us",
        "social media",
        "read more",
        "learn more",
        "find out more",
        "click here",
        "view all",
        "see all",
        "show more",
        "legal & policies",
        "legal and policies",
        "home",
        "services",
        "solutions",
        "products",
        "platform",
        "technology",
        "company",
        "business",
        "enterprise",
        "team",
        "staff",
        "people",
        "member",
        "user",
        "admin",
        "guest",
        "anonymous",
        "unknown",
    }
    if name.lower().strip() in generic_words:
        return False

    # Must have at least one letter
    if not re.search(r"[a-zA-Z]", name):
        return False

    # Words that indicate this is a company/product not a person
    company_indicators = [
        " ltd",
        " llc",
        " inc",
        " corp",
        " group",
        " services",
        " solutions",
        " platform",
        " database",
        " system",
        " software",
        " technologies",
        " tech",
        " ai ",
        " martech",
        " analytics",
        " consulting",
        " management",
        " international",
    ]
    name_lower = name.lower()
    if any(indicator in name_lower for indicator in company_indicators):
        return False

    return True


def _is_organization_name(name: str) -> bool:
    """Detects if name is an organization, company, or partnership rather than a person."""
    if not name:
        return False
    
    lower = name.lower().strip()
    
    # Direct company/org patterns
    org_patterns = [
        "lloyds", "scottish widows", "woodland trust", "partnership",
        "bank", "group", "company", "corporation", "fund",
        "holdings", "investments", "management", "consulting",
        "services", "solutions", "platform", "technologies",
        "inc.", "llc", "ltd", "s.a.", "plc", "nv", "ag",
    ]
    
    if any(pattern in lower for pattern in org_patterns):
        return True
    
    # Ends with org indicators
    org_endings = [" ltd", " llc", " inc", " corp", " group", " bank",
                   " fund", " company", " services", " solutions",
                   " plc", " partnership"]
    if any(lower.endswith(e) for e in org_endings):
        return True
    
    return False


def is_real_person_name(name: str) -> bool:
    """
    Returns True only if the name looks like a real human name.
    Rejects navigation items, page titles, marketing phrases.
    """
    if not name:
        return False

    name = (_normalize_honorific_spacing(name) or "").strip()

    # Must be between 2 and 60 characters
    if len(name) < 2 or len(name) > 60:
        return False

    # Reject organization names first
    if _is_organization_name(name):
        return False

    # Reject if contains sentence-like punctuation
    # Allowing '.' and ',' as they frequently appear in 'Dr.', 'Mr.', etc.
    if any(c in name for c in ["!", "?", ":", ";",
                                "(", ")", "[", "]", "/"]):
        return False

    # Reject obvious cookie/privacy banner fragments.
    lower_full = name.lower()
    cookie_noise = [
        "cookie",
        "cookies",
        "consent",
        "privacy settings",
        "accept all",
        "strictly necessary",
        "analytics",
        "targeting",
    ]
    if any(token in lower_full for token in cookie_noise):
        return False

    # Reject common editorial/navigation phrases seen on corporate pages.
    nav_phrase_tokens = [
        "news", "media", "investor", "relations", "sustainability",
        "approach", "transition", "countries", "territories", "facebook",
        "messenger", "downloads", "alerts", "shareholder", "calendar",
        "press", "search jobs", "most popular", "other brands", "financial wellbeing",
        "climate", "nature", "housing market", "supporting businesses",
    ]
    if any(token in lower_full for token in nav_phrase_tokens):
        return False

    # Reject if it is more than 5 words (names are short)
    words = name.split()
    if len(words) > 5:
        return False

    # Enforce real-person shape: at least first and last name.
    if len(words) < 2:
        return False

    if _has_honorific_single_name_shape(name):
        return True

    # Real names should usually have at least two capitalized tokens.
    capitalized_count = sum(1 for w in words if w and w[0].isupper())
    if capitalized_count < 2:
        return False

    # Reject topic phrases with connector words rarely used in names.
    connector_words = {"and", "of", "the", "for", "to", "our", "your", "in", "on"}
    lowered_words = [w.lower() for w in words]
    if any(w in connector_words for w in lowered_words):
        return False

    # Reject names that start with these navigation/page words
    name_lower = name.lower()
    bad_starts = [
        "why ", "our ", "the ", "how ", "what ", "when ", "where ",
        "who ", "we ", "data ", "merge ", "join ", "about ",
        "contact ", "meet ", "learn ", "find ", "get ", "see ",
        "view ", "read ", "click ", "download ", "sign ", "log ",
        "privacy ", "cookie ", "terms ", "legal ", "global ",
        "compensation ", "investing ", "w-9", "w9", "careers ", "gallagher ",
        "in order", "please "
    ]
    if any(name_lower.startswith(b) for b in bad_starts):
        return False

    # Reject if any word is a common non-name word
    non_name_words = {
        "privacy", "policy", "cookie", "careers", "purpose",
        "standards", "disclosure", "forms", "team", "executive",
        "committee", "board", "group", "company", "corporate",
        "services", "solutions", "partners", "associates",
        "decisions", "drives", "value", "values", "global",
        "compensation", "merger", "gallagher", "join", "why",
        "data", "video", "preferences", "consent", "order",
        "adjust", "view", "please", "featured", "report", "cookies", "consent",
        "salary", "salaries", "staff", "senior", "independent", "non-executive"
    }
    word_set = {w.lower() for w in words}
    if word_set & non_name_words:
        return False

    # Must have at least one word that looks like a proper noun
    # (starts with uppercase letter)
    has_proper_noun = any(
        w[0].isupper() for w in words if len(w) > 1
    )
    if not has_proper_noun:
        return False

    # Reject single-word names that are common English words
    if len(words) == 1:
        common_words = {
            "leadership", "management", "team", "people",
            "about", "contact", "news", "press", "blog",
            "home", "search", "menu", "close", "back",
            "next", "previous", "more", "less", "all"
        }
        if name_lower in common_words:
            return False

    return True


def _looks_like_person_name_not_job(text: str) -> bool:
    """
    Returns True if text LOOKS LIKE a person name (e.g. 'Pilar López')
    rather than a real job title.
    Person names: 2-3 capitalized words, NO job keywords.
    Job titles: contain keywords like Chief, Officer, Director, etc.
    """
    if not text:
        return False
    
    text = text.strip()
    words = text.split()
    
    # Person names are typically 2-3 words; titles can vary more
    if len(words) > 4:
        return False  # Too long to be just a person name
    
    # Check for key job-related keywords
    job_keywords = [
        "chief", "officer", "director", "president", "vice",
        "manager", "head", "lead", "partner", "counsel",
        "chair", "ceo", "cto", "cfo", "coo", "cio",
        "founder", "engineer", "architect", "scientist",
        "analyst", "consultant", "specialist", "advisor",
        "coordinator", "associate", "assistant", "intern",
        "supervisor", "vice president", "executive", "secretary",
        "chairman", "member", "trustee", "officer", "governor",
    ]
    
    text_lower = text.lower()
    has_job_keyword = any(kw in text_lower for kw in job_keywords)
    
    if not has_job_keyword:
        # No job keywords - could be a person name
        # Check if it looks like a person name (2+ capitalized words)
        capitalized_count = sum(1 for w in words if w and w[0].isupper())
        if capitalized_count >= 2 and len(text) < 50:
            return True  # Looks like a person name, not a job
    
    return False


def _normalize_appointment_title(job_title: Optional[str]) -> Optional[str]:
    """Normalizes timeline-style appointment text into a usable role when possible."""
    title = normalize_field(job_title)
    if not title:
        return None

    low = title.lower()
    if not low.startswith("appointment:"):
        return title

    # Pattern: "... as Non-Executive Director in ..."
    match_as = re.search(r"\bas\s+([^.,]+)", title, flags=re.IGNORECASE)
    if match_as:
        candidate = normalize_field(match_as.group(1))
        if candidate and is_plausible_job_title(candidate):
            return candidate

    # Pattern: "..., Non-Executive Director since ..."
    match_after_comma = re.search(
        r",\s*([^.,]*non-?executive director[^.,]*)",
        title,
        flags=re.IGNORECASE,
    )
    if match_after_comma:
        candidate = normalize_field(match_after_comma.group(1))
        if candidate and is_plausible_job_title(candidate):
            return candidate

    # Timeline-only metadata should be dropped.
    return None


def is_plausible_job_title(job_title: str) -> bool:
    """Return True if a title resembles a real role, not nav/category text."""
    if not job_title:
        return False

    title = " ".join(job_title.split()).strip()
    if not title:
        return False

    # Reject date-like values such as '29 January 2026'.
    if re.match(r"^\d{1,2}\s+[A-Za-z]+\s+\d{4}$", title):
        return False

    low = title.lower()
    if len(title) > 90:
        return False

    banned = {
        "news",
        "investors",
        "sustainability",
        "dividends",
        "halifax",
    }
    if low in banned:
        return False

    banned_tokens = [
        "latest news",
        "secondary navigation",
        "find the latest",
        "media releases",
        "reports, images",
        "see all the key dates",
        "download",
        "alerts",
    ]
    if any(tok in low for tok in banned_tokens):
        return False

    # **NEW**: Reject titles that look like person names (e.g. "Pilar López")
    if _looks_like_person_name_not_job(title):
        return False

    return True


class EnricherService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _log_discard(self, person: dict, reason: str) -> None:
        name = (person.get("name") or "").strip() or "<empty>"
        source = (person.get("source_url") or "").strip() or "<unknown source>"
        self.logger.warning(
            f"Discarded record | name='{name}' | reason='{reason}' | source='{source}'"
        )

    def _is_same_person(self, a: dict, b: dict) -> bool:
        """Determines if two records refer to the same person."""
        email_a = normalize_email(a.get("email") or "")
        email_b = normalize_email(b.get("email") or "")
        if email_a and email_b:
            return email_a == email_b
        if email_a or email_b:
            return False

        name_a = normalize_name(a.get("name") or "")
        name_b = normalize_name(b.get("name") or "")
        if not name_a or not name_b:
            return False
        if name_a == name_b:
            return True
        if fuzz.ratio(name_a, name_b) >= 90:
            return True

        words_a = name_a.split()
        words_b = name_b.split()
        if len(words_a) == 1 and len(words_b) > 1 and words_b[0] == words_a[0]:
            return True
        if len(words_b) == 1 and len(words_a) > 1 and words_a[0] == words_b[0]:
            return True
        return False

    def _merge_records(self, primary: dict, secondary: dict) -> dict:
        """Merges two records for the same person. Primary takes precedence."""
        merged = dict(primary)
        name_p = normalize_name(primary.get("name") or "")
        name_s = normalize_name(secondary.get("name") or "")
        if len(name_s.split()) > len(name_p.split()):
            merged["name"] = secondary.get("name")

        for field in [
            "email",
            "phone",
            "job_title",
            "linkedin_url",
            "instagram_url",
            "twitter_url",
        ]:
            if not merged.get(field) and secondary.get(field):
                merged[field] = secondary.get(field)
        return merged

    def deduplicate(self, people: List[dict]) -> List[dict]:
        """Single source of truth for deduplication using fuzzy name matching."""
        unique = []
        for person in people:
            merged = False
            for i, existing in enumerate(unique):
                if self._is_same_person(person, existing):
                    unique[i] = self._merge_records(existing, person)
                    merged = True
                    break
            if not merged:
                unique.append(dict(person))

        self.logger.info(
            f"Dedup: {len(people)} -> {len(unique)} "
            f"({len(people) - len(unique)} duplicates removed)"
        )
        return unique

    def normalize_all_fields(self, person: dict) -> dict:
        """Cleans all string fields. Removes newlines and collapses whitespace."""
        for field in [
            "name",
            "job_title",
            "phone",
            "linkedin_url",
            "instagram_url",
            "twitter_url",
        ]:
            person[field] = normalize_field(person.get(field))

        person["name"] = _normalize_honorific_spacing(person.get("name"))

        email = person.get("email")
        if email:
            person["email"] = normalize_email(email)
            if "example.com" in person["email"] or "@" not in person["email"]:
                person["email"] = None

        # Fix records where title contains the person's full name.
        person["job_title"] = _strip_name_prefix_from_title(
            person.get("name"), person.get("job_title")
        )

        # Recover missing names when combined values were put in job_title.
        if not person.get("name") and person.get("job_title"):
            parsed_name, parsed_title = _split_combined_name_title(person.get("job_title"))
            if parsed_name and parsed_title:
                person["name"] = parsed_name
                person["job_title"] = parsed_title

        return person

    def filter_garbage(self, people: List[dict]) -> List[dict]:
        """Removes records with no useful data and generic company emails."""
        job_title_words = [
            "general counsel", "chief", "officer", "director",
            "manager", "head of", "president", "vice president",
            "counsel", "secretary", "treasurer", "chairman",
            "partner", "associate", "consultant", "analyst",
            "coordinator", "administrator", "executive"
        ]
        result = []
        for person in people:
            name = person.get("name")
            email = person.get("email") or ""
            phone = person.get("phone") or ""
            job_title = person.get("job_title")
            if not name and not email and not phone:
                self._log_discard(person, "no_name_email_phone")
                continue

            # Reject records where name is not a real person name
            if name and not is_real_person_name(name):
                self._log_discard(person, "invalid_human_name")
                continue

            # Remove records where name is clearly a job title not a person
            if name:
                name_lower = name.lower().strip()
                if any(name_lower == jt or name_lower.startswith(jt)
                       for jt in job_title_words):
                    self._log_discard(person, "name_looks_like_job_title")
                    continue

            if name and job_title and not is_plausible_job_title(str(job_title)):
                self._log_discard(person, "invalid_or_navigation_job_title")
                continue

            result.append(person)
        return result

    def enrich(self, people: List[dict], domain: str, crawl_id: str) -> List[dict]:
        """
        Main enrichment entry point. Single source of truth.
        Runs: normalize -> deduplicate -> filter -> tag -> confidence score.
        This is the ONLY place deduplication happens in the entire pipeline.
        """
        people = [
            p for p in people if is_valid_person_name(p.get("name") or "")
            or (not p.get("name") and (p.get("email") or p.get("phone")))
        ]

        normalized = []
        for person in people:
            normalized.append(self.normalize_all_fields(dict(person)))

        deduped = self.deduplicate(normalized)
        filtered = self.filter_garbage(deduped)

        # Filter out garbage records
        noise_names = [
            "medical services",
            "medical tourism",
            "legal policies",
            "legal & policies",
            "legal and policies",
            "emergency hotline",
            "healthcare specialists",
            "contact us",
            "about us",
            "our team",
            "our story",
            "privacy policy",
            "terms of service",
            "cookie policy",
            "data protection",
            "legal disclaimer",
            "all rights reserved",
            "get in touch",
            "follow us",
            "social media",
            "read more",
            "learn more",
            "find out more",
            "click here",
            "view all",
            "see all",
            "show more",
            "doctor",
            "nurse",
            "patient",
            "specialist",
            "consultant",
            "manager",
            "director",
            "engineer",
            "developer",
            "designer",
            "analyst",
            "coordinator",
            "assistant",
            "officer",
            "executive",
            "associate",
            "intern",
            "staff",
            "team",
            "member",
            "user",
            "admin",
            "guest",
            "anonymous",
            "staff salaries",
            "salary",
            "salaries",
            "investing in our people",
            "news and media",
            "investor relations",
            "barclays' approach to the transition",
            "barclays approach to the transition",
        ]
        common_job_words = [
            "doctor", "nurse", "engineer", "developer", "designer",
            "manager", "director", "analyst", "consultant", "specialist",
            "officer", "assistant", "coordinator", "executive", "associate",
            "founder", "partner", "chairman", "president", "secretary"
        ]
        nav_symbols = {"🛡", "⚠", "⭐", "📞", "🌐", "🧬", "🔒", "📱", "💊"}
        strict_filtered = []
        for person in filtered:
            name = (person.get("name") or "").strip()
            job_title = _normalize_appointment_title(person.get("job_title"))
            person["job_title"] = job_title

            # Check 1 - Name too long
            if name and len(name) > 80:
                self._log_discard(person, "name_too_long")
                continue

            # Check 2 - Reject emoji/symbol-heavy names but allow international letters.
            if name and re.search(r"[^\x00-\x7F]", name):
                word_count = len(name.split())
                cleaned = re.sub(r"[\s\-\.'’]", "", name, flags=re.UNICODE)
                has_only_letters = cleaned.isalpha()
                if not (2 <= word_count <= 5 and has_only_letters):
                    self._log_discard(person, "name_contains_invalid_unicode_or_symbols")
                    continue

            # Check 3 - Job title is emoji/non-ASCII only
            if job_title and isinstance(job_title, str):
                if all((not ch.isascii()) or ch.isspace() for ch in job_title):
                    person["job_title"] = None

            # Check 3b - **Job title looks like a person name (e.g. "Pilar López")**
            if job_title and _looks_like_person_name_not_job(job_title):
                self._log_discard(person, "job_title_looks_like_person_name")
                continue

            # Check 4 - Name is common non-person phrase
            if name and name.lower().strip() in noise_names:
                self._log_discard(person, "name_in_noise_dictionary")
                continue

            # Check 5 - Name contains navigation symbols
            if name and any(ch in name for ch in nav_symbols):
                self._log_discard(person, "name_contains_navigation_symbol")
                continue

            # Check 6 - Name starts with number
            if name and name[0].isdigit():
                self._log_discard(person, "name_starts_with_digit")
                continue

            # Check 7 - Single generic word that is a job title not a name
            if name and name.lower().strip() in common_job_words:
                self._log_discard(person, "single_generic_job_word")
                continue

            strict_filtered.append(person)

        filtered = strict_filtered

        required_keys = [
            "name",
            "email",
            "phone",
            "job_title",
            "linkedin_url",
            "instagram_url",
            "twitter_url",
            "source_url",
            "confidence",
            "domain",
            "crawl_id",
        ]

        for person in filtered:
            person["domain"] = domain
            person["crawl_id"] = crawl_id

            for key in required_keys:
                if key not in person:
                    person[key] = None

            person["confidence"] = calculate_confidence(person)

            # **CRITICAL: Drop records with null job_title** - they are weak/hallucinated.
            # A real person on a leadership page must have a job title.
            if not person.get("job_title"):
                self._log_discard(person, "null_job_title_rejected")
                person["_discarded"] = True
                continue

            # Drop records where name is missing but job_title exists (role without person).
            if not person.get("name"):
                self._log_discard(person, "missing_name")
                person["_discarded"] = True
                continue

        filtered = [p for p in filtered if not p.get("_discarded")]

        # Final strict dedupe by name+domain (or email/phone fallbacks) keeping richer records.
        deduped_by_domain = {}
        for person in filtered:
            name_key = normalize_name(person.get("name") or "")
            domain_key = (person.get("domain") or "").strip().lower()
            email_key = normalize_email(person.get("email") or "")
            phone_key = (person.get("phone") or "").strip()

            if name_key:
                key = f"name|{name_key}|{domain_key}"
            elif email_key:
                key = f"email|{email_key}|{domain_key}"
            elif phone_key:
                key = f"phone|{phone_key}|{domain_key}"
            else:
                key = f"raw|{domain_key}|{id(person)}"

            if key not in deduped_by_domain:
                deduped_by_domain[key] = person
                continue

            existing = deduped_by_domain[key]
            existing_score = sum(1 for v in existing.values() if v not in (None, "", []))
            person_score = sum(1 for v in person.values() if v not in (None, "", []))
            if person_score > existing_score:
                deduped_by_domain[key] = person

        return list(deduped_by_domain.values())
