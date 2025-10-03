"""
Unified Location Processor for the European Strikes News Extraction System.
Handles location extraction, geocoding, and country identification.
"""

import spacy
import logging
import re
import os
from datetime import datetime, timezone
import requests
import time
from urllib.parse import urlparse

# Disable HTTP request logging from requests library
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


nlp = spacy.load("en_core_web_sm")

class UnifiedLocationProcessor:
    def __init__(self, connection_string=None):
        """Initialize the location processor without database dependencies."""
        self.geocoding_cache = {}
        self.geonames_username = os.environ.get("GEONAMES_USERNAME")
        if not self.geonames_username:
            raise ValueError("GEONAMES_USERNAME environment variable is required. Please set it in your .env file or environment.")  

    def extract_locations_from_text(self, text):
        """Extract location entities from text using spaCy."""
        if not text:
            return []
        try:
            doc = nlp(text)
            return list(set([ent.text.strip() for ent in doc.ents if ent.label_ in ["GPE", "LOC"]]))
        except Exception as e:
            logger.error(f"Error extracting locations from text: {e}")
            return []

    def get_country_from_url(self, url):
        """Extract country from URL based on domain patterns."""
        if not url:
            return None
            
        # URL to country mapping based on corpus.py URLs
        url_country_mapping = {
            # Malta
            'timesofmalta.com': 'Malta',
            
            # Poland
            'rp.pl': 'Poland',
            
            # Finland
            'hs.fi': 'Finland',
            
            # Croatia
            '24sata.hr': 'Croatia',
            
            # Denmark
            'bt.dk': 'Denmark',
            
            # Estonia
            'delfi.ee': 'Estonia',
            'postimees.ee': 'Estonia',
            
            # Bulgaria
            'standartnews.com': 'Bulgaria',
            
            # Belgium
            'lalibre.be': 'Belgium',
            
            # Netherlands
            'volkskrant.nl': 'Netherlands',
            
            # Lithuania
            've.lt': 'Lithuania',
            
            # Czech Republic
            'blesk.cz': 'Czech Republic',
            
            # Romania
            'realitatea.net': 'Romania',
            
            # Luxembourg
            'lessentiel.lu': 'Luxembourg',
            
            # Germany
            'welt.de': 'Germany',
            
            # Austria
            'krone.at': 'Austria',
            
            # Greece
            'tanea.gr': 'Greece',
            
            # Italy
            'repubblica.it': 'Italy',
            
            # France
            'lemonde.fr': 'France',
            
            # Portugal
            'publico.pt': 'Portugal',
            
            # Spain
            'elpais.com': 'Spain',
            
            # Ireland
            'irishtimes.com': 'Ireland',
            
            # Hungary
            'nepszava.hu': 'Hungary',
            
            # Slovakia
            'sme.sk': 'Slovakia',
            
            # Sweden
            'gp.se': 'Sweden',
            
            # Cyprus
            'politis.com.cy': 'Cyprus',
            'philenews.com': 'Cyprus',
            
            # Latvia
            'diena.lv': 'Latvia'
        }
        
        try:
            # Extract domain from URL
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower()
            
            # Check for exact domain match
            if domain in url_country_mapping:
                country = url_country_mapping[domain]
                return country
            
            # Check for partial domain match (for subdomains)
            for url_pattern, country in url_country_mapping.items():
                if url_pattern in domain:
                    return country
            
            return None
            
        except Exception as e:
            return None

    def get_country_from_geonames(self, location, username=None, max_retries=2):
        """Get country name from location using GeoNames API with exponential backoff and caching."""
        if username is None:
            username = self.geonames_username
        url = f"http://api.geonames.org/searchJSON?q={location}&maxRows=1&username={username}"
        
        # Check memory cache first
        if location in self.geocoding_cache:
            return self.geocoding_cache[location]
        
        # Exponential backoff retry logic
        for attempt in range(max_retries):
            try:             
                response = requests.get(url, timeout=10) 
                if response.status_code == 200:
                    data = response.json()
                    if data['totalResultsCount'] > 0:
                        country_name = data['geonames'][0].get('countryName')
                        self.geocoding_cache[location] = country_name
                            
                        logger.info(f"Found country for {location} (GeoNames API): {country_name}")
                        return country_name
                elif response.status_code == 429:  # Rate limit exceeded
                    logger.warning(f"GeoNames API rate limit exceeded for {location}")
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return None
                else:
                    logger.warning(f"GeoNames API error for {location}: {response.status_code}")
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return None
                        
            except requests.exceptions.Timeout:
                logger.error(f"GeoNames API timeout for {location} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    continue
                else:
                    return None
            except requests.exceptions.RequestException as e:
                logger.error(f"GeoNames API request error for {location} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    continue
                else:
                    return None
            except Exception as e:
                logger.error(f"Unexpected error with GeoNames for {location} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    continue
                else:
                    return None
        
        self.geocoding_cache[location] = None
        
        logger.warning(f"All retries failed for {location}")
        geo_logger.warning(f"GeoNames failed for location: {location} (after {max_retries} attempts)")

        return None
    
    

    def find_country_near_keywords(self, text, keywords):
        """
        Find country based on proximity to protest keywords.
        Hierarchical approach: same sentence > adjacent sentences > top-K global locations
        """
        import nltk
        from nltk.tokenize import sent_tokenize
        
        sentences = sent_tokenize(text)
        
        # Find sentences with keywords (text is already lowercase)
        keyword_sentences = []
        for i, sentence in enumerate(sentences):
            for keyword in keywords:
                if keyword in sentence:
                    keyword_sentences.append((i, sentence))
                    break
        
        if not keyword_sentences:
            return None, []
        
        # Strategy 1: Check same sentence as keywords
        same_sentence_locations = []
        for sent_idx, sentence in keyword_sentences:
            sentence_locations = self.extract_locations_from_text(sentence)
            same_sentence_locations.extend(sentence_locations)
        
        if same_sentence_locations:
            logger.info(f"🔍 Checking {len(same_sentence_locations)} locations in same sentence as keywords")
            for loc in same_sentence_locations:
                if loc:
                    country = self.get_country_from_geonames(loc, self.geonames_username)
                    if country:
                        logger.info(f"✅ Found country '{country}' from location '{loc}' in same sentence as keyword")
                        return country, [loc]
        
        # Strategy 2: Check adjacent sentences (±1, ±2)
        adjacent_locations = []
        for sent_idx, sentence in keyword_sentences:
            for offset in [-1, 1]:
                check_idx = sent_idx + offset
                if 0 <= check_idx < len(sentences):
                    adj_sentence = sentences[check_idx]
                    adj_locations = self.extract_locations_from_text(adj_sentence)
                    adjacent_locations.extend(adj_locations)
        
        if adjacent_locations:
            logger.info(f"🔍 Checking {len(adjacent_locations)} locations in adjacent sentences to keywords")
            for loc in adjacent_locations:
                if loc:
                    country = self.get_country_from_geonames(loc, self.geonames_username)
                    if country:
                        logger.info(f"✅ Found country '{country}' from location '{loc}' in adjacent sentence to keyword")
                        return country, [loc]
        
        # Strategy 3: Fallback - check top-K global locations (most frequent)
        all_locations = self.extract_locations_from_text(text)
        if all_locations:
            # Count frequency and get top-K (limit to 5 to avoid too many API calls)
            from collections import Counter
            location_counts = Counter(all_locations)
            top_locations = [loc for loc, count in location_counts.most_common(5)]
            
            logger.info(f"🔍 Fallback: Checking top-{len(top_locations)} most frequent locations globally")
            for loc in top_locations:
                if loc:
                    country = self.get_country_from_geonames(loc, self.geonames_username)
                    if country:
                        logger.info(f"✅ Found country '{country}' from top-K location '{loc}' (fallback)")
                        return country, [loc]
        
        return None, []

    def process_text(self, text, url=None):
        """
        Process text to extract locations and find country using proximity to keywords.
        If no locations found, tries to extract country from URL
        Returns: (locations_list, country_name) or (None, None) if no locations found
        """
        try:
            if not text:
                return None, None
            
            # Define protest keywords (from corpus.py)
            protest_keywords = [
                'protest', 'demonstration', 'rally', 'boycott', 'strike', 'walkout', 
                'stoppage', 'workstop', 'gather', 'mobilize', 'march', 'picket',
                'occupation', 'riot', 'blockade', 'activism', 'activist'
            ]
            
            # Try to find country based on keyword proximity
            found_country, keyword_locations = self.find_country_near_keywords(text, protest_keywords)
            
            if found_country:
                logger.info(f"✅ Found country '{found_country}' using keyword proximity")
                return keyword_locations, found_country
            
            # Fallback: Extract all locations and try GeoNames
            all_locations = self.extract_locations_from_text(str(text))
            cleaned_locations = []
            
            for loc in all_locations:
                if loc and loc not in cleaned_locations:
                    cleaned_locations.append(loc)
                    country = self.get_country_from_geonames(loc, self.geonames_username)
                    if country:
                        logger.info(f"✅ Found country '{country}' from location '{loc}' (fallback)")
                        return cleaned_locations, country

            # Final fallback: URL-based country detection
            if url:
                logger.info(f"🔍 No country found from locations, trying URL fallback: {url}")
                url_country = self.get_country_from_url(url)
                if url_country:
                    logger.info(f"✅ Found country '{url_country}' from URL fallback")
                    return cleaned_locations if cleaned_locations else None, url_country
                else:
                    logger.info(f"⚠️ No country found from URL fallback either")

            return cleaned_locations if cleaned_locations else None, None
            
        except Exception as e:
            logger.error(f"Error processing text: {e}")
            return None, None