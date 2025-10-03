"""
Database operations module for the European Strikes News Extraction System.
Contains MongoDB operations, data transfer functions, and database utilities.
"""

import logging
from datetime import datetime, timedelta, timezone
from pymongo import UpdateOne, DESCENDING

from config import get_database_connections, logger
from utils import is_eu_country, normalize_article_dates_for_database

# ===================== DATABASE SETUP =====================

def setup_unique_indexes():
    """Setup unique indexes on MongoDB collections to prevent duplicates."""
    try:
        client, databases, collections = get_database_connections()
        
        # Main collection
        collections['main'].create_index('url', unique=True, sparse=True)
        collections['main'].create_index('content_hash', unique=True, sparse=True)
        
        # Analysis collection
        collections['analysis'].create_index('url', unique=True, sparse=True)
        collections['analysis'].create_index('content_hash', unique=True, sparse=True)
        
        # Final collection
        collections['final'].create_index('url', unique=True, sparse=True)
        collections['final'].create_index('content_hash', unique=True, sparse=True)
        
        logger.info("Unique indexes created successfully")
    except Exception as e:
        logger.warning(f"Error creating unique indexes: {e}")



# ===================== DATA TRANSFER FUNCTIONS =====================

def transfer_prediction_articles():
    """Transfer articles with prediction=1 from last 6 hours to analysis database"""
    print("\n" + "=" * 80)
    print("TRANSFERRING ARTICLES WITH prediction=1 FROM LAST 6 HOURS")
    print("=" * 80)

    client, databases, collections = get_database_connections()
    records_main = collections['main']
    records_analysis = collections['analysis']

    now = datetime.now(timezone.utc)
    six_hours_ago = now - timedelta(hours=6)
    print(f"Looking for articles from: {six_hours_ago} to {now}")

    # Define query and projection
    query = {
        'prediction': 1,
        'imported_at': {'$gte': six_hours_ago, '$lte': now}
    }
    projection = {
        '_id': 1,
        'url': 1,
        'prediction': 1,
        'keys': 1,
        'sorted_keys': 1,
        'translated_title': 1,
        'translated_summary': 1,
        'translated_content': 1,
        'translated_keywords': 1,
        'translated_article': 1,
        'status': 1,
        'language': 1,
        'publication_date': 1,
        'author': 1,
        'compact_article': 1,
        'countries_info': 1,
        'processed': 1,
        'imported_at': 1,
        'source': 1,
        'participant_counts': 1,
        'parent_id': 1,
        'is_parent': 1,
        'is_child': 1,
        'has_violent_events': 1,
        'has_events': 1,
        'event_status': 1,
        'occupation_info': 1,
        'story_thread_id': 1,
        'authors': 1,
        'locations': 1,
        'country': 1     
    }

    articles = list(records_main.find(query, projection))
    print(f"Found {len(articles)} articles with prediction=1")

    if not articles:
        return 0

    bulk_ops = []
    transferred_count = 0

    for article in articles:
        try:
            # Process article locations and countries
            from processor import process_article_locations_and_countries
            article = process_article_locations_and_countries(article)

            # Remove _id and unwanted fields
            article_copy = {k: v for k, v in article.items() if k not in {
                '_id', 'title', 'summary', 'content', 'label',
                'keywords', 'name', 'article', 'lastmod'
            }}

            article_copy = normalize_article_dates_for_database(article_copy)

            op = UpdateOne(
                {'url': article['url']},
                {'$set': article_copy},
                upsert=True
            )
            bulk_ops.append(op)
            transferred_count += 1

            title = article.get('translated_title', 'Unknown')[:50]
            locations_info = ""
            if 'locations' in article_copy:
                locs = article_copy['locations']
                if isinstance(locs, list) and locs:
                    locations_info += f" | Locations: {locs[:3]}"
            if 'country' in article_copy:
                locations_info += f" | Country: {article_copy['country']}"
            print(f"Prepared: {title}...{locations_info}")

        except Exception as e:
            print(f"Error processing article: {e}")

    if bulk_ops:
        result = records_analysis.bulk_write(bulk_ops)
        print(f"\nBulk write completed:")
        print(f"   - Upserted: {result.upserted_count}")
        print(f"   - Modified: {result.modified_count}")
        print(f"   - Matched: {result.matched_count}")

    print(f"Total articles transferred from last 6 hours: {transferred_count}")
    print("=" * 80)
    return transferred_count

def transfer_eu_articles_to_final():
    """Transfer EU articles from analysis to final database."""
    client, databases, collections = get_database_connections()
    records_analysis = collections['analysis']
    records_final = collections['final']

    six_hours_ago = datetime.now(timezone.utc) - timedelta(hours=6)
    current_time = datetime.now(timezone.utc)

    print(f"Looking for articles from: {six_hours_ago} to {current_time}")

    # Get recent articles from source collection
    analysis_articles = list(records_analysis.find({
        'imported_at': {'$gte': six_hours_ago, '$lte': current_time}
    }))

    print(f"Found {len(analysis_articles)} recent articles")

    transferred_count, skipped_count = 0, 0

    for article in analysis_articles:
        try:
            country = article.get('country')

            if not country:
                print(f" Skipping article (no country): {article.get('url', 'Unknown')[:50]}...")
                skipped_count += 1
                continue

            if is_eu_country(country):
                article_copy = article.copy()
                article_copy.pop('_id', None)
                article_copy = normalize_article_dates_for_database(article_copy)
                
                records_final.update_one(
                    {'url': article['url']},
                    {'$set': article_copy},
                    upsert=True
                )
                transferred_count += 1
                print(f" Transferred EU article: {country} | {article.get('url', 'Unknown')[:50]}...")
            else:
                print(f" Skipping non-EU article: {country} | {article.get('url', 'Unknown')[:50]}...")
                skipped_count += 1

        except Exception as e:
            print(f" Error processing article {article.get('url', 'Unknown')[:50]}: {e}")
            skipped_count += 1
            continue

    print("\n Transfer Summary:")
    print(f" EU articles transferred: {transferred_count}")
    print(f" Articles skipped: {skipped_count}")
    print(f" Total processed: {transferred_count + skipped_count}")
    print("="*80)

    return transferred_count

# ===================== EVENT PATTERN EXTRACTION =====================

def extract_event_patterns_from_final_strikes():
    """Extract event patterns and participant counts"""
    try:
        print("\n" + "="*80)
        print("EXTRACTING EVENT PATTERNS AND PARTICIPANT COUNTS FROM FINAL DATABASE")
        print("="*80)

        client, databases, collections = get_database_connections()
        records_final = collections['final']

        now = datetime.now(timezone.utc)
        six_hours_ago = now - timedelta(hours=6)
        print(f"Looking for articles from: {six_hours_ago} to {now}")

        articles_without_patterns = list(records_final.find({
            'imported_at': {'$gte': six_hours_ago, '$lte': now}
        }, {'_id': 1, 'url': 1, 'text': 1, 'article': 1}))
        
        print(f"Found {len(articles_without_patterns)} articles without event patterns or participant counts")
        
        if not articles_without_patterns:
            print(" All articles already have event patterns and participant counts")
            return 0
        
        articles = articles_without_patterns

        updated_count = 0
        bulk_ops = []

        # Precompile patterns
        import re
        
        # 1) Protest context (use for sentence-level filtering)
        PROTEST_CONTEXT = re.compile(
            r'\b('
            r'protest|protests|protester|protesters|'
            r'demonstration|demonstrations|demonstrator|demonstrators|'
            r'rally|rallies|march|marches|picket|pickets|'
            r'strike|strikes|striker|strikers|'
            r'walkout|blockade|sit[-\s]?in|occupation|'
            r'counter[-\s]?protest(?:ers)?'
            r')\b', re.IGNORECASE)

        # 2) Negations / peaceful (if found in same sentence, lower score or reject)
        NEGATION = re.compile(
            r'\b('
            r'peaceful|peacefully|non[-\s]?violent|without\s+incident|'
            r'no\s+violence|no\s+injur(?:y|ies)|no\s+arrests?'
            r')\b', re.IGNORECASE)

        # 3) Strong violence indicators
        VIOLENT_STRONG = [re.compile(p, re.IGNORECASE) for p in [
            r'\bshot\s+dead\b',
            r'\bkilled\b',
            r'\bfatal(?:ity|ities)?\b',
            r'\bopened\s+fire\b',
            r'\bfired\s+(?:live\s+)?rounds?\b',
            r'\blive\s+ammunition\b',
            r'\bgunfire\b',
            r'\b(arson|torched|set\s+(?:on\s+)?fire|set\s+ablaze)\b',
        ]]

        # 4) Medium violence indicators / conflicts / suppression / damage
        VIOLENT_MEDIUM = [re.compile(p, re.IGNORECASE) for p in [
            r'(?<!non[-\s]?)\b(violence|violent)\b',  # avoids "non-violent"
            r'\battack(?:s|ed|ing)?\b',
            r'\b(fight|fights|fighting)\b',
            r'\bclash(?:es|ed|ing)?\b',
            r'\bconfrontation(?:s)?\b',
            r'\briot(?:s|ed|ing)?\b',
            r'\bscuffle(?:s|d|ing)?\b',
            r'\bskirmish(?:es)?\b',
            r'\bbrawl(?:s|ed|ing)?\b',
            r'\bmelee\b',
            r'\b(injur(?:y|ies)|injured|wounded|hurt|casualt(?:y|ies)|hospitali[sz]ed)\b',
            r'\b(tear[-\s]?gas|pepper[-\s]?spray|rubber[-\s]?bullets?|water[-\s]?cannons?)\b',
            r'\b(stun\s+grenades?|flash[-\s]?bangs?)\b',
            r'\b(baton|batons|truncheon(?:s)?)\b',
            r'\b(looting|vandalism|property\s+damage)\b',
        ]]

        # 5) Arrests with proximity to protesters/strikers (both orders)
        ARREST_PROXIMITY = [
            re.compile(r'\b(arrest(?:s|ed|ing)?|detain(?:ed|ment|s)?)\b.{0,30}\b('
                       r'protester|protesters|demonstrator|demonstrators|striker|strikers|activist(?:s)?'
                       r')\b', re.IGNORECASE),
            re.compile(r'\b('
                       r'protester|protesters|demonstrator|demonstrators|striker|strikers|activist(?:s)?'
                       r')\b.{0,30}\b(arrest(?:s|ed|ing)?|detain(?:ed|ment|s)?)\b', re.IGNORECASE),
        ]

        # 6) Clashes with police/counter-protesters (both orders)
        CLASH_WITH_POLICE = [
            re.compile(r'\b(clash(?:es|ed|ing)?|confrontation(?:s)?)\b.{0,30}\b('
                       r'police|riot\s+police|security\s+forces|counter[-\s]?protesters'
                       r')\b', re.IGNORECASE),
            re.compile(r'\b('
                       r'police|riot\s+police|security\s+forces|counter[-\s]?protesters'
                       r')\b.{0,30}\b(clash(?:es|ed|ing)?|confrontation(?:s)?)\b', re.IGNORECASE),
        ]
        occupation_patterns = [
            re.compile(p, re.IGNORECASE) for p in [
                r'\b(occupation|occupying|sit-in|sit-ins)\b',
                r'\b(school\s+occupation|university\s+occupation)\b',
                r'\b(building\s+occupation|office\s+occupation)\b',
                r'\b(students?\s+occupying|workers?\s+occupying)\b',
                r'\b(protesters?\s+occupying|activists?\s+occupying)\b',
                r'\b(occupy\s+(?:the|a|an)\s+\w+)\b',
                r'\b(occupation\s+of\s+\w+)\b',
                r'\b(sit-in\s+protest|sit-in\s+demonstration)\b'
            ]
        ]

        for article in articles:
            try:
                article_text = str(article.get('text')).strip()
                if not article_text:
                    continue

               
                has_violent = False
                has_occupation = any(p.search(article_text) for p in occupation_patterns)
                
                # Check for protest context first
                if PROTEST_CONTEXT.search(article_text):
                    # Check for negation (peaceful indicators)
                    if NEGATION.search(article_text):
                        has_violent = False
                    else:
                        # Check for strong violent indicators
                        if any(p.search(article_text) for p in VIOLENT_STRONG):
                            has_violent = True
                        # Check for medium violent indicators
                        elif any(p.search(article_text) for p in VIOLENT_MEDIUM):
                            has_violent = True
                        # Check for arrest proximity
                        elif any(p.search(article_text) for p in ARREST_PROXIMITY):
                            has_violent = True
                        # Check for clashes with police
                        elif any(p.search(article_text) for p in CLASH_WITH_POLICE):
                            has_violent = True
                
                status = "violent" if has_violent else "peaceful"

                # Extract participant counts using the extract_participant_count function
                from utils import extract_participant_count
                participant_counts = extract_participant_count(article_text)

                update_data = {
                    'has_violent_events': has_violent,
                    'occupation_info': has_occupation,
                    'participant_counts': participant_counts
                }

                bulk_ops.append(
                    UpdateOne({'_id': article['_id']}, {'$set': update_data})
                )
                updated_count += 1

                # Show progress with participant count info
                if participant_counts['max_count'] > 0:
                    print(f" Found {participant_counts['max_count']} participants in article {updated_count}")

                if updated_count % 50 == 0:
                    print(f" Processed {updated_count} articles...")

            except Exception as e:
                print(f" Error processing article {article.get('url', 'Unknown')[:50]}: {e}")

        # Commit all updates
        if bulk_ops:
            result = records_final.bulk_write(bulk_ops)
            print(f"\n Bulk write completed: {result.modified_count} modified")

        # Stats
        violent = records_final.count_documents({'has_violent_events': True})
        occupation = records_final.count_documents({'occupation_info': True})
        
        # Participant count stats
        articles_with_participants = records_final.count_documents({
            'participant_counts.max_count': {'$gt': 0}
        })
        total_articles = records_final.count_documents({})

        print(f"\n Event Pattern Stats:")
        print(f" Violent events: {violent}")
        print(f"🏫 Occupation events: {occupation}")
        print(f"\n Participant Count Stats:")
        print(f" Articles with participant counts: {articles_with_participants}")
        print("="*80)

        return updated_count

    except Exception as e:
        logging.error(f" Error in event pattern extraction: {e}")
        return 0

# ===================== PARENT-CHILD RELATIONSHIPS =====================

def check_parent_child_relationships_final():
    """Check for parent-child relationships in final database"""
    print("\n" + "="*80)
    print("CHECKING PARENT-CHILD RELATIONSHIPS IN FINAL DATABASE")
    print("="*80)
    
    try:
        client, databases, collections = get_database_connections()
        records_final = collections['final']
        relationships_collection = collections['relationships']
       
        print(f"Checking relationships in final database...")
        print(f"Found {records_final.count_documents({})} articles in final database")
        from create_parent_child_relationships import ParentChildRelationships
        
        # Initialize relationship finder
        relationship_finder = ParentChildRelationships()
        
        # Find relationships
        created_count = relationship_finder.find_recent_child_relationships()
        
        if created_count == 0:
            print(" No relationships found for recent articles")
        else:
            print(" New relationships created")
            print(f" Total new relationships created from recent articles: {created_count}")
        
        # Show some example relationships
        sample_relationships = list(relationships_collection.find().limit(5))
        if sample_relationships:
            print("\nSample relationships:")
            for i, rel in enumerate(sample_relationships, 1):
                parent_url = rel.get('parent_url', 'Unknown')[:50]
                child_url = rel.get('child_url', 'Unknown')[:50]
                print(f"{i}. Parent: {parent_url}...")
                print(f"   Child:  {child_url}...")
        
        print(f"\n Parent-child relationships check completed successfully!")
        
        return True
        
    except Exception as e:
        print(f" Error in parent-child relationships check: {e}")
        logging.error(f"Error in parent-child relationships check: {e}")
        return False
