import asyncio
import concurrent.futures
import json
import logging
import re
from typing import List
from google import genai
from backend.config import settings


class LLMExtractor:
    """Google Gemma-based people extraction and link scoring service."""

    EXTRACTION_PROMPT = """
{name_hint}

You are a people data extraction specialist.
Your ONLY job is to find REAL INDIVIDUAL HUMAN BEINGS mentioned on this page.

A real person has:
    - A human first name AND last name (e.g. "John Smith", "Dr. Sarah Jones")
    - OR a name with a title prefix (Mr., Mrs., Ms., Dr., Prof.)
    - A job title that describes their role at a company

Extract every real person you can find. For each person return:
    name: their full name exactly as written (first + last name)
    job_title: their exact job title or role at the company
    email: their email ONLY if explicitly written on the page
    phone: their phone ONLY if explicitly written on the page
    linkedin_url: their personal linkedin.com/in/ URL if present
    instagram_url: their personal instagram.com/ URL if present
    twitter_url: their personal twitter.com/ or x.com/ URL if present

STRICT RULES - READ CAREFULLY:
    - Extract each person from a single local profile/card/container context
    - Name and job_title must belong to the SAME person container; never mix across sections
    - Only extract REAL INDIVIDUAL HUMAN BEINGS
    - A name MUST be a human name with at least a first name AND a last name
    - NEVER extract company names as people
    - NEVER extract product names as people
    - NEVER extract department names as people
    - NEVER extract countries, locations, or cities (e.g., "Australia", "Abu Dhabi")
    - NEVER extract generic terms, categories, or services
    - NEVER invent or guess any field
    - NEVER use example.com emails
    - Only include contact details explicitly on the page
    - Do not include company social pages, only personal profiles
    - Set any missing field to null
    - Return ONLY valid JSON, nothing else
    - NEVER extract page titles, navigation items, menu text, or section headings as names
    - NEVER extract marketing phrases or company slogans as names
    - A real person name has a first name and last name (e.g. "John Smith")
    - If you are not certain something is a real human name DO NOT include it
    - Cookie notices, privacy policies, video placeholders are NOT people
    - Section headings like "Our Executive Team" or "Why Join Us" are NOT people
    - Only extract names where you can also identify a job title or role nearby
    - If only a name is visible and no nearby role exists, keep job_title as null

Return exactly this format:
{{
    "people": [
        {{
            "name": "John Smith",
            "job_title": "Chief Executive Officer",
            "email": null,
            "phone": null,
            "linkedin_url": null,
            "instagram_url": null,
            "twitter_url": null
        }}
    ]
}}

If no people found return: {{"people": []}}

PAGE CONTENT:
{page_text}
"""

    def __init__(self):
        """Initialize Google Gemma model."""
        self.client = genai.Client(api_key=settings.GOOGLE_API_KEY)
        self.logger = logging.getLogger(__name__)

    def _parse_response(self, raw: str) -> List[dict]:
        """Parses LLM response into list of people dicts. Never raises."""
        try:
            clean = re.sub(r'```(?:json)?|```', '', raw).strip()
            try:
                data = json.loads(clean)
            except json.JSONDecodeError:
                match = re.search(r'\{.*\}', clean, re.DOTALL)
                if not match:
                    return []
                data = json.loads(match.group())
            if isinstance(data, dict) and "people" in data:
                people = data["people"]
            elif isinstance(data, list):
                people = data
            elif isinstance(data, dict) and "name" in data:
                people = [data]
            else:
                return []
            valid = []
            for p in people:
                if not isinstance(p, dict):
                    continue
                
                name = p.get("name")
                if not name or not isinstance(name, str):
                    continue
                
                name = name.strip()
                # Exclude single-word names (usually countries or categories)
                if len(name.split()) < 2:
                    continue
                
                # Exclude common non-human phrases that keep slipping through
                lower_name = name.lower()
                invalid_keywords = [
                    'offerings', 'generation', 'policies', 'abu dhabi', 'legal', 
                    'featured', 'report', 'doctor', 'read more', 'view all',
                    'learn more', 'contact us', 'our story', 'sustainability report'
                ]
                if any(kw in lower_name for kw in invalid_keywords):
                    continue
                
                p["name"] = name
                email = p.get("email", "")
                if email and ("example.com" in email or "@" not in email):
                    p["email"] = None
                valid.append(p)
            return valid
        except Exception as e:
            self.logger.warning(f"Failed to parse LLM response: {e}")
            return []

    def _call_google_sync(self, prompt: str) -> str:
        """Synchronous Google API call using new google.genai SDK."""
        response = self.client.models.generate_content(
            model=settings.GOOGLE_MODEL,
            contents=prompt
        )
        return response.text or ""

    async def extract(self, page_text: str, name_hint: str = "") -> List[dict]:
        """
        Extract people from page text using Google Gemma.
        Handles rate limits by waiting the suggested retry delay and retrying.
        Returns empty list if all attempts fail.
        """
        if not page_text or len(page_text.strip()) < 50:
            return []

        prompt = self.EXTRACTION_PROMPT.format(
            name_hint=name_hint,
            page_text=page_text
        )

        for attempt in range(3):
            try:
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = loop.run_in_executor(
                        executor, self._call_google_sync, prompt
                    )
                    response_text = await asyncio.wait_for(
                        future, timeout=60.0
                    )
                people = self._parse_response(response_text)
                self.logger.info(
                    f"Google extracted {len(people)} people "
                    f"(attempt {attempt+1})"
                )
                return people

            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Google API timed out after 60s "
                    f"(attempt {attempt+1}/3)"
                )
                if attempt < 2:
                    await asyncio.sleep(5)
                continue

            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "quota" in error_str.lower():
                    retry_match = re.search(
                        r'retry[^\d]*(\d+(?:\.\d+)?)\s*s',
                        error_str,
                        re.IGNORECASE
                    )
                    wait_time = (
                        float(retry_match.group(1)) + 5
                        if retry_match else 65
                    )
                    self.logger.warning(
                        f"Rate limited. Waiting {wait_time:.0f}s "
                        f"(attempt {attempt+1}/3)"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Google extraction failed: {e}")
                    return []

        self.logger.error("All 3 attempts failed. Returning empty list.")
        return []

    async def score_links(self, links: List[str], domain: str) -> List[dict]:
        """Scores URLs for likelihood of containing people data."""
        if not links:
            return []

        links_text = "\n".join(links[:50])
        prompt = f"""You are analyzing URLs from the domain {domain}.
Score each URL from 0-10 based on how likely it contains employee, team, or leadership information.

Score 10: URLs clearly about team/leadership/people/staff/executives/board/founders
Score 5: URLs about company/about-us/contact that might have some people info
Score 0: URLs about products/blog/news/pricing/legal/careers/jobs/support

Return ONLY a valid JSON array, no explanation, no markdown:
[
  {{"url": "full_url_here", "score": 8}},
  {{"url": "full_url_here", "score": 3}}
]

URLs to score:
{links_text}
"""

        for attempt in range(3):
            try:
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = loop.run_in_executor(
                        executor, self._call_google_sync, prompt
                    )
                    response_text = await asyncio.wait_for(
                        future, timeout=45.0
                    )

                clean = re.sub(r'```(?:json)?|```', '', response_text).strip()
                scored = json.loads(clean)

                if isinstance(scored, list):
                    valid = [
                        item for item in scored
                        if isinstance(item, dict)
                        and "url" in item
                        and "score" in item
                    ]
                    return sorted(
                        valid,
                        key=lambda x: x.get("score", 0),
                        reverse=True
                    )
                return []

            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Google link scoring timed out (attempt {attempt+1}/3)"
                )
                if attempt < 2:
                    await asyncio.sleep(5)
                continue
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "quota" in error_str.lower():
                    retry_match = re.search(
                        r'retry[^\d]*(\d+(?:\.\d+)?)\s*s',
                        error_str,
                        re.IGNORECASE
                    )
                    wait_time = (
                        float(retry_match.group(1)) + 5
                        if retry_match else 65
                    )
                    self.logger.warning(
                        f"Link scoring rate limited. Waiting {wait_time:.0f}s "
                        f"(attempt {attempt+1}/3)"
                    )
                    await asyncio.sleep(wait_time)