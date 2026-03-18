from typing import List

from backend.core.utils import calculate_confidence, deduplicate_people


class EnricherService:
    def enrich(self, people: List[dict], domain: str, crawl_id: str) -> List[dict]:
        people = deduplicate_people(people)

        required_keys = [
            "name",
            "email",
            "phone",
            "postcode",
            "job_title",
            "linkedin_url",
            "source_url",
            "confidence",
            "domain",
            "crawl_id",
        ]

        cleaned = []
        for person in people:
            person["domain"] = domain
            person["crawl_id"] = crawl_id

            if person.get("email") is not None:
                person["email"] = str(person["email"]).lower()

            for key, value in list(person.items()):
                if isinstance(value, str):
                    person[key] = value.strip()

            person["confidence"] = calculate_confidence(person)

            for key in required_keys:
                if key not in person:
                    person[key] = None

            if person.get("name") is None and person.get("email") is None:
                continue

            cleaned.append(person)

        return cleaned
