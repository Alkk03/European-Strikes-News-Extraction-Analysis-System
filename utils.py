"""
Utility functions for the European Strikes News Extraction System.
Contains helper functions for URL canonicalization, content hashing, date handling, etc.
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from typing import Any, Dict, List, Union

from config import TRACKING_PREFIXES

# ===================== URL CANONICALIZATION =====================

def canonicalize_url(u: str) -> str:
    """Canonicalize URL by removing tracking parameters, normalizing scheme/host, etc."""
    if not u or not isinstance(u, str):
        return u
    
    try:
        s = urlsplit(u.strip())
        # lowercase scheme/host
        scheme = s.scheme.lower()
        netloc = s.netloc.lower()
        path = s.path or '/'
        
        # clean query from tracking params & sort
        q = [(k, v) for (k, v) in parse_qsl(s.query, keep_blank_values=True)
             if not k.lower().startswith(TRACKING_PREFIXES)]
        query = urlencode(sorted(q))
        
        # remove anchors and normalize path
        return urlunsplit((scheme, netloc, path.rstrip('/'), query, ''))
    except Exception:
        return u

# ===================== CONTENT HASHING =====================

def content_hash(title: str, body: str) -> str:
    """Generate content hash from title and body for duplicate detection."""
    try:
        raw = (title or '').strip() + '\n' + (body or '').strip()
        return hashlib.sha256(raw.encode('utf-8', errors='ignore')).hexdigest()
    except Exception:
        return ''

# ===================== DATE HANDLING =====================

# safe_date_conversion() function removed - replaced by normalize_article_dates_for_database()

def make_json_serializable_with_date_logging(data, context="", article_data=None):
    """
    Convert datetime objects to strings for JSON serialization with date logging.
    
    Args:
        data: The data to make JSON serializable
        context: Context information for logging
        article_data: Full article data to log if date conversion fails
        
    Returns:
        JSON serializable data
    """
    if isinstance(data, dict):
        serializable_data = {}
        for key, value in data.items():
            if isinstance(value, datetime):
                serializable_data[key] = value.isoformat()
            elif isinstance(value, dict):
                serializable_data[key] = make_json_serializable_with_date_logging(value, f"{context}.{key}", article_data)
            elif isinstance(value, list):
                serializable_data[key] = [make_json_serializable_with_date_logging(item, f"{context}.{key}[{i}]", article_data) if isinstance(item, dict) else item for i, item in enumerate(value)]
            else:
                # Date normalization is handled centrally by normalize_article_dates_for_database()
                serializable_data[key] = value
        return serializable_data
    return data

def normalize_article_dates_for_database(article):
    if not isinstance(article, dict):
        return article
    
    normalized_article = article.copy()
    
    # List of date fields that should be converted to datetime objects
    date_fields = [
        'publication_date', 'lastmod', 'created_at', 'updated_at', 'imported_at'
    ]
    
    for field in date_fields:
        if field in normalized_article and normalized_article[field] is not None:
            value = normalized_article[field]
            
            # If it's already a datetime object, keep it
            if isinstance(value, datetime):
                continue
                
            # If it's a string, try to convert it to datetime
            if isinstance(value, str) and value.strip():
                try:
                    # Try different parsing methods
                    if 'T' in value and ('Z' in value or '+' in value or '-' in value[-6:]):
                        # ISO format
                        normalized_article[field] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    else:
                        # Try dateutil parser
                        from dateutil.parser import parse
                        normalized_article[field] = parse(value)
                    
                    print(f"✅ Normalized {field} to datetime: {normalized_article[field]}")
                    
                except Exception as e:
                    print(f"⚠️ Could not normalize {field} '{value}': {e}")
                    # Keep original value if conversion fails
                    continue
    
    # Ensure imported_at is always a datetime object
    if 'imported_at' not in normalized_article:
        normalized_article['imported_at'] = datetime.now(timezone.utc)
    elif isinstance(normalized_article['imported_at'], str):
        try:
            from dateutil.parser import parse
            normalized_article['imported_at'] = parse(normalized_article['imported_at'])
        except:
            normalized_article['imported_at'] = datetime.now(timezone.utc)
    
    return normalized_article

# ===================== DATA SANITIZATION =====================

def sanitize_for_mongo(obj):
    """Sanitize data for MongoDB storage by converting numpy types to Python types."""
    import numpy as np
    
    if isinstance(obj, dict):
        return {k: sanitize_for_mongo(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_mongo(i) for i in obj]
    elif isinstance(obj, (np.floating, np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, (np.integer, np.int32, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.bool_,)):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj

# ===================== COUNTRY UTILITIES =====================

def is_eu_country(country):
    """Check if a country is in the European Union."""
    from config import EU_COUNTRIES
    return country in EU_COUNTRIES

# ===================== TEXT PROCESSING =====================

def _make_fallback_summary(text: str) -> str:
    """Simple, robust fallback summarizer when no model is available."""
    from nltk.tokenize import sent_tokenize
    
    text = (text or "").strip()
    if not text:
        return ""
    # 1st and 2nd sentence or first ~200-300 chars
    sents = sent_tokenize(text)
    if sents:
        return " ".join(sents[:2])[:300]
    return text[:300]

# ===================== PARTICIPANT COUNT EXTRACTION =====================
import re

def extract_participant_count(text: str) -> dict:
    """Extract participant counts from text using robust regex + number parsing."""
    if not text:
        return {"counts": [], "total_estimated": 0, "max_count": 0}

    # Words → numbers (approximate)
    number_words = {
        "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
        "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
        "couple": 2, "few": 3, "several": 5, "dozen": 12, "dozens": 24,  
        "scores": 40,  
        "hundred": 100, "hundreds": 200,  
        "thousand": 1_000, "thousands": 2000,
        "tens of thousands": 10_000,
        "hundreds of thousands": 100_000,
        "million": 1_000_000, "millions": 2_000_000,
        "billion": 1_000_000_000, "billions": 2_000_000_000,
        "handful": 5,
        "half a million": 500_000,
        "quarter of a million": 250_000,
    }

    # Common labels
    labels = r"(?:people|attendees|participants|protesters|supporters|workers|activists|citizens|demonstrators|strikers|employees|crowd|union members|marchers)"

    # Numeric token (with separators/decimals) + optional K/M/B suffix
    NUM_TOKEN = r"(?P<num>\d{1,3}(?:[,\s]\d{3})*|\d+(?:\.\d+)?)\s*(?P<sfx>[KkMmBb])?"
    NUM_TOKEN2 = r"(?P<num2>\d{1,3}(?:[,\s]\d{3})*|\d+(?:\.\d+)?)\s*(?P<sfx2>[KkMmBb])?"

    # Helper: parse number string + suffixes + phrases like “1 million”
    def parse_num_str(val: str, sfx: str | None) -> float:
        v = val.replace(",", "").replace(" ", "")
        try:
            x = float(v)
        except ValueError:
            return 0.0
        if sfx:
            sfx = sfx.lower()
            if sfx == "k": x *= 1_000
            elif sfx == "m": x *= 1_000_000
            elif sfx == "b": x *= 1_000_000_000
        return x

    def parse_number_phrase(phrase: str) -> int:
        p = phrase.lower().strip()
        if p in number_words:
            return int(number_words[p])
        # “<num> million/billion/thousand”
        m = re.match(rf"^\s*(\d+(?:\.\d+)?)\s+(million|billion|thousand)s?\s*$", p)
        if m:
            base = float(m.group(1))
            unit = m.group(2)
            mul = 1_000_000 if unit == "million" else 1_000_000_000 if unit == "billion" else 1_000
            return int(base * mul)
        # “half a million” / “quarter of a million”
        if "half a million" in p: return 500_000
        if "quarter of a million" in p: return 250_000
        return 0

    # Patterns (all with named groups)
    patterns = [
        # between A and B people
        re.compile(rf"\bbetween\s+{NUM_TOKEN}\s+and\s+{NUM_TOKEN2}\s+{labels}\b", re.IGNORECASE),

        # conflicting estimates: ... A ... (while|but|however) ... B ...
        re.compile(
            rf"\b{NUM_TOKEN}\s+{labels}.*?\b(?:while|whereas|but|however|meanwhile|on\s+the\s+other\s+hand|in\s+contrast|by\s+contrast)\b.*?{NUM_TOKEN2}\s+{labels}\b",
            re.IGNORECASE | re.DOTALL
        ),

        # single numeric with qualifiers
        re.compile(
            rf"\b(?:more than|over|at least|around|approximately|some|estimated|nearly|about|up to|as many as)?\s*{NUM_TOKEN}\s+{labels}\b",
            re.IGNORECASE
        ),

        # estimated number
        re.compile(rf"\b(?:an\s+)?estimated\s+(?:number of\s+)?{NUM_TOKEN}\s+{labels}\b", re.IGNORECASE),

        # word numbers (one|dozens|hundreds|…)
        re.compile(
            r"\b(?P<word>"
            r"one|two|three|four|five|six|seven|eight|nine|ten|"
            r"several|few|couple|dozen|dozens|scores|"
            r"hundred|hundreds|thousand|thousands|"
            r"tens of thousands|hundreds of thousands|"
            r"million|millions|billion|billions|"
            r"half a million|quarter of a million"
            r")\b\s+" + labels,
            re.IGNORECASE
        ),

        # handful of X
        re.compile(rf"\b(?:a|only a)?\s*handful\s+of\s+{labels}\b", re.IGNORECASE),

        # 1k / 2.5K style
        re.compile(rf"\b{NUM_TOKEN}\s*{labels}\b", re.IGNORECASE),
    ]

    # Iterate and collect matches (avoid duplicate spans)
    seen_spans = set()
    total_estimated = 0
    max_count = 0
    min_count = float('inf')  # Initialize with infinity

    for pat in patterns:
        for m in pat.finditer(text):
            span = m.span()
            if span in seen_spans:
                continue
            seen_spans.add(span)

            ctx = m.group(0)
            raw = 0.0

            # Case 1: between A and B
            if "between" in ctx.lower() and (" and " in ctx.lower()):
                n1 = parse_num_str(m.group("num") or "0", m.group("sfx"))
                n2 = parse_num_str(m.group("num2") or "0", m.group("sfx2"))
                lo, hi = sorted([n1, n2])
                if hi > 0:
                    total_estimated += hi
                    max_count = max(max_count, int(hi))
                    min_count = min(min_count, int(lo))
                continue
                
            # Case 2: conflicting estimates A ... (while|but) ... B => keep both as min/max
            if any(w in ctx.lower() for w in [" while ", " whereas ", " but ", " however ", " on the other hand ", " in contrast ", " by contrast "]):
                n1 = parse_num_str(m.group("num") or "0", m.group("sfx"))
                n2 = parse_num_str(m.group("num2") or "0", m.group("sfx2"))
                min_val, max_val = sorted([n1, n2])
                
                if max_val > 0:
                    total_estimated += max_val
                    max_count = max(max_count, int(max_val))
                    min_count = min(min_count, int(min_val))
                    continue

            # Case 3: word-based number
            elif "word" in m.groupdict() and m.group("word"):
                raw = float(parse_number_phrase(m.group("word")))

            # Case 4: single numeric token (with suffix)
            elif "num" in m.groupdict():
                raw = parse_num_str(m.group("num") or "0", m.group("sfx"))

            # Fallback
            if raw <= 0:
            continue

            count_int = int(round(raw))
            total_estimated += count_int
            max_count = max(max_count, count_int)
            min_count = min(min_count, count_int)

    # Handle case where no counts were found
    if min_count == float('inf'):
        min_count = 0

    return {
        "total_estimated": int(total_estimated),
        "max_count": int(max_count),
        "min_count": int(min_count),
    }
