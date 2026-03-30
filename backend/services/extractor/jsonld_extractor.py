import json
import logging
from typing import List
from bs4 import BeautifulSoup


class JSONLDExtractor:
    """Extracts people from JSON-LD structured data embedded in HTML."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def extract(self, html: str, source_url: str) -> List[dict]:
        """Finds all JSON-LD Person blocks in HTML and returns people list."""
        if not html:
            return []
        people = []
        try:
            soup = BeautifulSoup(html, "lxml")
            scripts = soup.find_all("script", type="application/ld+json")
            self.logger.info(
                f"Found {len(scripts)} JSON-LD blocks in {source_url}"
            )
            for script in scripts:
                try:
                    if not script.string:
                        continue
                    data = json.loads(script.string)
                    extracted = self._parse_jsonld_block(data, source_url)
                    people.extend(extracted)
                except Exception:
                    continue
        except Exception as e:
            self.logger.warning(f"JSON-LD extraction failed: {e}")
        seen = set()
        unique = []
        for p in people:
            key = (p.get("name") or "").lower()
            if key and key not in seen:
                seen.add(key)
                unique.append(p)
        if unique:
            self.logger.info(
                f"JSON-LD extracted {len(unique)} people from {source_url}"
            )
        return unique

    def _parse_jsonld_block(self, data, source_url: str) -> List[dict]:
        """Recursively parses JSON-LD block for Person entities."""
        people = []
        if isinstance(data, list):
            for item in data:
                people.extend(self._parse_jsonld_block(item, source_url))
            return people
        if not isinstance(data, dict):
            return people
        if "@graph" in data:
            for item in data["@graph"]:
                people.extend(self._parse_jsonld_block(item, source_url))
            return people
        type_value = data.get("@type", "")
        if isinstance(type_value, list):
            is_person = any(
                t in ["Person", "ProfilePage"] for t in type_value
            )
        else:
            is_person = type_value in ["Person", "ProfilePage"]
        if is_person:
            person = self._extract_person_fields(data, source_url)
            if person.get("name"):
                people.append(person)
        if data.get("@type") == "ItemList":
            for item in data.get("itemListElement", []):
                if isinstance(item, dict):
                    people.extend(
                        self._parse_jsonld_block(item, source_url)
                    )
        return people

    def _extract_person_fields(self, data: dict, source_url: str) -> dict:
        """Maps JSON-LD Person fields to our data model."""
        name = data.get("name") or data.get("alternateName") or ""
        if isinstance(name, list):
            name = name[0] if name else ""
        job_title = data.get("jobTitle") or data.get("title") or ""
        if isinstance(job_title, list):
            job_title = job_title[0] if job_title else ""
        email = data.get("email", "")
        if email and email.startswith("mailto:"):
            email = email.replace("mailto:", "")
        phone = data.get("telephone") or data.get("phone") or ""
        if isinstance(phone, list):
            phone = phone[0] if phone else ""
        linkedin_url = None
        instagram_url = None
        twitter_url = None
        same_as = data.get("sameAs", [])
        if isinstance(same_as, str):
            same_as = [same_as]
        for url in same_as:
            if not isinstance(url, str):
                continue
            url_lower = url.lower()
            if "linkedin.com/in/" in url_lower:
                linkedin_url = url
            elif "instagram.com/" in url_lower:
                instagram_url = url
            elif "twitter.com/" in url_lower or "x.com/" in url_lower:
                twitter_url = url
        name_clean = name.strip() if name else None
        job_clean = job_title.strip() if job_title else None
        email_clean = email.strip() if email else None
        phone_clean = phone.strip() if phone else None
        if name_clean and email_clean:
            confidence = 3
        elif name_clean and job_clean:
            confidence = 2
        else:
            confidence = 1
        return {
            "name": name_clean,
            "job_title": job_clean,
            "email": email_clean,
            "phone": phone_clean,
            "linkedin_url": linkedin_url,
            "instagram_url": instagram_url,
            "twitter_url": twitter_url,
            "source_url": source_url,
            "confidence": confidence
        }