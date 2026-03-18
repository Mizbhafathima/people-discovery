import json
import logging
import warnings
from typing import List

with warnings.catch_warnings():
    warnings.simplefilter("ignore", FutureWarning)
    import google.generativeai as genai
import ollama

from backend.config import settings


class LLMExtractor:
    EXTRACTION_PROMPT = """You are a people data extraction engine.
Extract every person mentioned in the page text below.
Return ONLY a valid JSON object with a people array. No markdown. No explanation.
Each person object must have these exact keys, use null for missing values:
name, job_title, email, phone, postcode, linkedin_url, confidence, context
Associate each email and phone to the nearest person based on surrounding context.
If no people found return exactly: {{\"people\": []}}
Page text: {page_text}"""

    def __init__(self):
        genai.configure(api_key=settings.GOOGLE_API_KEY)
        self.google_model = genai.GenerativeModel(settings.GOOGLE_MODEL)
        self.logger = logging.getLogger(__name__)

    def _parse_llm_response(self, raw: str) -> List[dict]:
        cleaned = raw.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:].strip()
        elif cleaned.startswith("```"):
            cleaned = cleaned[3:].strip()
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3].strip()

        try:
            data = json.loads(cleaned)
            people = data.get("people", [])
            return people if isinstance(people, list) else []
        except Exception as exc:
            self.logger.warning("Failed to parse LLM response: %s", exc)
            return []

    def _extract_with_google(self, page_text: str) -> List[dict]:
        prompt = self.EXTRACTION_PROMPT.format(page_text=page_text)
        response = self.google_model.generate_content(
            prompt,
            request_options={"timeout": 15},
        )
        return self._parse_llm_response(response.text)

    def _extract_with_ollama(self, page_text: str) -> List[dict]:
        prompt = self.EXTRACTION_PROMPT.format(page_text=page_text)
        response = ollama.chat(
            model=settings.OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}],
        )
        return self._parse_llm_response(response["message"]["content"])

    def extract(self, page_text: str) -> List[dict]:
        try:
            return self._extract_with_google(page_text)
        except Exception as exc:
            self.logger.warning("Google extraction failed, falling back to Ollama: %s", exc)

        try:
            return self._extract_with_ollama(page_text)
        except Exception as exc:
            self.logger.error("Ollama extraction failed: %s", exc)
            return []
