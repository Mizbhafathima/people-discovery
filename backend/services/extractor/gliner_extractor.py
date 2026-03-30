import logging
from typing import List, Dict


class GLiNERExtractor:
    """Uses GLiNER model for local named entity recognition."""

    LABELS = ["person", "job_title", "email", "phone"]

    def __init__(self):
        """Loads GLiNER model. Fails gracefully if unavailable."""
        self.logger = logging.getLogger(__name__)
        self.model = None
        self._load_model()

    def _load_model(self):
        """Loads GLiNER model from local cache."""
        try:
            from gliner import GLiNER
            self.model = GLiNER.from_pretrained(
                "knowledgator/gliner-multitask-large-v0.5",
                local_files_only=True
            )
            self.logger.info("GLiNER model loaded successfully")
        except Exception as e:
            self.logger.warning(f"GLiNER not available: {e}")
            self.model = None

    def is_available(self) -> bool:
        """Returns True if model is loaded and ready."""
        return self.model is not None

    def extract(self, text: str) -> Dict[str, List[str]]:
        """Extracts named entities using GLiNER."""
        empty = {
            "persons": [],
            "job_titles": [],
            "emails": [],
            "phones": []
        }
        if not self.model or not text:
            return empty
        try:
            entities = self.model.predict_entities(
                text[:50000],
                self.LABELS,
                threshold=0.5
            )
            persons, job_titles, emails, phones = [], [], [], []
            seen_p, seen_t, seen_e, seen_ph = set(), set(), set(), set()

            for ent in entities:
                label = ent.get("label", "")
                val = ent.get("text", "").strip()
                if not val:
                    continue
                if label == "person":
                    if len(val) > 1 and val.lower() not in seen_p:
                        seen_p.add(val.lower())
                        persons.append(val)
                elif label == "job_title":
                    if val.lower() not in seen_t:
                        seen_t.add(val.lower())
                        job_titles.append(val)
                elif label == "email":
                    if "@" in val and val.lower() not in seen_e:
                        seen_e.add(val.lower())
                        emails.append(val)
                elif label == "phone":
                    if val.lower() not in seen_ph:
                        seen_ph.add(val.lower())
                        phones.append(val)

            self.logger.info(
                f"GLiNER found: {len(persons)} persons, "
                f"{len(job_titles)} titles, "
                f"{len(emails)} emails, "
                f"{len(phones)} phones"
            )
            return {
                "persons": persons,
                "job_titles": job_titles,
                "emails": emails,
                "phones": phones
            }
        except Exception as e:
            self.logger.warning(f"GLiNER extraction failed: {e}")
            return empty

    def build_hint_for_llm(self, ner_results: Dict[str, List[str]]) -> str:
        """Builds hint string from GLiNER results for LLM prompt."""
        persons = ner_results.get("persons", [])
        job_titles = ner_results.get("job_titles", [])
        if not persons and not job_titles:
            return ""
        parts = []
        if persons:
            names = "\n".join(f"  - {n}" for n in persons[:30])
            parts.append(f"CONFIRMED PEOPLE ON THIS PAGE:\n{names}")
        if job_titles:
            titles = "\n".join(f"  - {t}" for t in job_titles[:20])
            parts.append(f"JOB TITLES FOUND:\n{titles}")
        if parts:
            return (
                "\n".join(parts) +
                "\n\nIMPORTANT: Extract ALL confirmed people above. "
                "Match each person to their correct job title."
            )
        return ""

    def pair_persons_with_titles(
        self,
        text: str,
        ner_results: Dict[str, List[str]]
    ) -> List[dict]:
        """Pairs person names with nearest job titles in text."""
        persons = ner_results.get("persons", [])
        job_titles = ner_results.get("job_titles", [])
        if not persons:
            return []
        lines = text.split("\n")
        paired = []
        for person in persons:
            person_line = None
            for i, line in enumerate(lines):
                if person.lower() in line.lower():
                    person_line = i
                    break
            if person_line is None:
                paired.append({"name": person, "job_title": None})
                continue
            best_title = None
            min_distance = float("inf")
            for title in job_titles:
                for i, line in enumerate(lines):
                    if title.lower() in line.lower():
                        distance = abs(i - person_line)
                        if distance < min_distance and distance <= 5:
                            min_distance = distance
                            best_title = title
            paired.append({"name": person, "job_title": best_title})
        return paired