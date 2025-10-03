"""
Article processing module for the European Strikes News Extraction System.
Contains functions for article processing, location extraction, and analysis.
"""

import logging
from datetime import datetime, timezone

from config import CONNECTION_STRING, logger

# ===================== ARTICLE LOCATION PROCESSING =====================

def process_article_locations_and_countries(article, location_processor=None):
    """Process a single article to extract locations and determine country"""
    try:
        # Initialize location processor if not provided
        if location_processor is None:
            from location_extractor import UnifiedLocationProcessor
            location_processor = UnifiedLocationProcessor(CONNECTION_STRING)
            should_close_connection = True
        else:
            should_close_connection = False
        
        # Extract locations from compact_article field only
        all_locations = []
        
        # Use only compact_article field
        article_text = ""
        if 'compact_article' in article and article['compact_article']:
            article_text = str(article['compact_article'])
        
        if article_text.strip():
            # Extract locations using spaCy
            locations = location_processor.extract_locations_from_text(article_text)
            all_locations.extend(locations)
        
        # Clean locations
        cleaned_locations = []
        for location in all_locations:
            cleaned_location = location_processor.clean_location(location)
            if cleaned_location and cleaned_location not in cleaned_locations:
                cleaned_locations.append(cleaned_location)
        
        # Try to find country from locations
        found_country = None
        if cleaned_locations:
            for location in cleaned_locations:
                country = location_processor.get_country_from_location(location)
                if country:
                    found_country = country
                    print(f"Found country '{country}' from location '{location}'")
                    break
        
        # Add location and country fields to article
        article['locations'] = cleaned_locations  
        
        # Set country field - prioritize found_country from location extraction
        if found_country:
            article['country'] = found_country
            print(f" Set country to '{found_country}' from location extraction")
        else:
            # If no country found from locations, try to extract from countries_info (if exists)
            if 'countries_info' in article and article['countries_info']:
                countries_info = article['countries_info']
                if 'Countries mentioned:' in countries_info:
                    # Extract countries from the countries_info string
                    countries_part = countries_info.replace('Countries mentioned:', '').strip()
                    countries_list = [country.strip() for country in countries_part.split(',')]
                    if countries_list:
                        # Use the first country found
                        article['country'] = countries_list[0]
                        print(f" Set country to '{countries_list[0]}' from countries_info")
                    else:
                        print(f" No country found from countries_info")
                else:
                    print(f" No country found from countries_info")
            else:
                print(f" No country found - article will be processed without country information")
        
        # Extract participant counts if available
        if article_text.strip():
            try:
                from utils import extract_participant_count
                participant_counts = extract_participant_count(article_text)
                article['participant_counts'] = participant_counts
                print(f" Extracted participant counts: {participant_counts}")
            except Exception as e:
                print(f" Error extracting participant counts: {e}")
        
        # Close connection if we created it
        if should_close_connection:
            location_processor.close_connection()
        
        return article
        
    except Exception as e:
        print(f"Error processing locations for article: {e}")
        return article
