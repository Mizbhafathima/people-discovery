from typing import List

from backend.services.extractor.regex_extractor import RegexExtractor
from backend.services.extractor.llm_extractor import LLMExtractor
from backend.core.utils import sanitize_text, chunk_text, calculate_confidence

MAX_LLM_CHUNKS = 3


class ExtractionPipeline:
    def __init__(self):
        self.regex = RegexExtractor()
        self.llm = LLMExtractor()

    def run(self, raw_html: str, source_url: str) -> List[dict]:
        clean_text = sanitize_text(raw_html)
        regex_results = self.regex.extract_all(clean_text, source_url)
        chunks = chunk_text(clean_text, max_chars=3000)[:MAX_LLM_CHUNKS]

        llm_people = []
        for chunk in chunks:
            llm_people.extend(self.llm.extract(chunk))

        merged = []
        regex_emails = {email.lower() for email in regex_results.get("emails", [])}

        for person in llm_people:
            person_data = dict(person)
            email = person_data.get("email")
            if email:
                candidate = str(email).strip().lower()
                if candidate in regex_emails:
                    person_data["email"] = candidate
                elif "@" in candidate and "." in candidate:
                    person_data["email"] = candidate
                else:
                    person_data["email"] = None

            if not person_data.get("phone") and regex_results.get("phones"):
                person_data["phone"] = regex_results["phones"][0]
            if not person_data.get("postcode") and regex_results.get("postcodes"):
                person_data["postcode"] = regex_results["postcodes"][0]
            if not person_data.get("linkedin_url") and regex_results.get("linkedin_urls"):
                person_data["linkedin_url"] = regex_results["linkedin_urls"][0]

            person_data["source_url"] = source_url
            merged.append(person_data)

        merged_emails = {
            str(person.get("email")).strip().lower()
            for person in merged
            if person.get("email")
        }
        for email in regex_results.get("emails", []):
            normalized = str(email).strip().lower()
            if normalized not in merged_emails:
                merged.append({"email": normalized, "source_url": source_url})

        for person in merged:
            person["confidence"] = calculate_confidence(person)

        return merged
