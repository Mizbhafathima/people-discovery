import re
import phonenumbers
from typing import List

from backend.core.regex_patterns import (
    EMAIL_PATTERN,
    PHONE_PATTERNS,
    POSTCODE_PATTERNS,
    LINKEDIN_PATTERN,
    INSTAGRAM_PATTERN,
    TWITTER_PATTERN,
    clean_phone,
)
from backend.core.utils import is_valid_email


class RegexExtractor:
    def extract_emails(self, text: str) -> List[str]:
        emails = []
        for email in EMAIL_PATTERN.findall(text):
            normalized = email.lower()
            if is_valid_email(normalized) and normalized not in emails:
                emails.append(normalized)
        return emails

    def extract_phones(self, text: str) -> List[str]:
        phones = []

        for match in phonenumbers.PhoneNumberMatcher(text, None):
            try:
                if phonenumbers.is_valid_number(match.number):
                    formatted = phonenumbers.format_number(
                        match.number,
                        phonenumbers.PhoneNumberFormat.E164,
                    )
                    if formatted not in phones:
                        phones.append(formatted)
            except Exception:
                continue

        for pattern in PHONE_PATTERNS:
            for match in pattern.finditer(text):
                raw_phone = clean_phone(match.group(0))
                parsed_number = None

                for region in (None, "US", "GB", "IN", "CA", "AU", "DE", "FR", "AE"):
                    try:
                        parsed_number = phonenumbers.parse(raw_phone, region)
                        if phonenumbers.is_valid_number(parsed_number):
                            break
                    except Exception:
                        parsed_number = None

                if parsed_number and phonenumbers.is_valid_number(parsed_number):
                    try:
                        formatted = phonenumbers.format_number(
                            parsed_number,
                            phonenumbers.PhoneNumberFormat.E164,
                        )
                    except Exception:
                        formatted = raw_phone
                    if formatted not in phones:
                        phones.append(formatted)

        return phones

    def extract_postcodes(self, text: str) -> List[str]:
        postcodes = []
        for country, pattern in POSTCODE_PATTERNS.items():
            for match in pattern.finditer(text):
                value = f"{match.group(0)} ({country})"
                if value not in postcodes:
                    postcodes.append(value)
        return postcodes

    def extract_linkedin_urls(self, text: str) -> List[str]:
        urls = []
        for match in LINKEDIN_PATTERN.finditer(text):
            url = match.group(0)
            # Normalize to ensure https://
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            # Filter out company, school, org pages
            if '/company/' not in url and '/school/' not in url and '/org/' not in url:
                if url not in urls:
                    urls.append(url)
        return urls

    def extract_instagram_urls(self, text: str) -> List[str]:
        urls = []
        for match in INSTAGRAM_PATTERN.finditer(text):
            url = match.group(0)
            # Normalize to ensure https://
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            # Filter out company/business pages
            if '/company/' not in url and '/business/' not in url:
                if url not in urls:
                    urls.append(url)
        return urls

    def extract_twitter_urls(self, text: str) -> List[str]:
        urls = []
        non_person_paths = ['/home', '/explore', '/notifications', '/messages', '/i/', '/intent/', '/share', '/hashtag']
        
        for match in TWITTER_PATTERN.finditer(text):
            url = match.group(0)
            # Normalize to ensure https://
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            # Filter out known non-person paths
            if not any(path in url for path in non_person_paths):
                if url not in urls:
                    urls.append(url)
        return urls

    def extract_all(self, text: str, source_url: str) -> dict:
        return {
            "emails": self.extract_emails(text),
            "phones": self.extract_phones(text),
            "postcodes": self.extract_postcodes(text),
            "linkedin_urls": self.extract_linkedin_urls(text),
            "instagram_urls": self.extract_instagram_urls(text),
            "twitter_urls": self.extract_twitter_urls(text),
            "source_url": source_url,
        }
