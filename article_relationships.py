import re
import logging
from collections import defaultdict
from typing import List, Dict
from pymongo import MongoClient, InsertOne
from datetime import datetime, timezone
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from bson import ObjectId

# MongoDB connection
from config import get_database_connections
client, databases, collections = get_database_connections()
collection = collections['final']
relationships_collection = collections['relationships']

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ParentChildRelationships:
    def __init__(self):
        # Similarity thresholds for different relationship types
        self.story_threading_threshold = 0.4  # Similarity >= 0.4: Story threading
        self.follow_up_threshold = 0.7        # Similarity >= 0.7: Follow-up
        self.duplicate_threshold = 0.9        # Similarity >= 0.9: Duplicate
        
        # Use the lowest threshold for general parent-child detection
        self.similarity_threshold = self.story_threading_threshold
        
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english', ngram_range=(1, 2))
        self.relationships_collection = relationships_collection


    def get_relationship_type(self, similarity_score: float) -> str:
        """Determine relationship type based on similarity score"""
        if similarity_score >= self.duplicate_threshold:
            return "duplicate"
        elif similarity_score >= self.follow_up_threshold:
            return "follow_up"
        elif similarity_score >= self.story_threading_threshold:
            return "story_thread"
        else:
            return ""

    def get_articles_by_country(self) -> Dict[str, List[Dict]]:
        articles_by_country = defaultdict(list)
        all_articles = list(collection.find({}))
        logger.info(f"Found {len(all_articles)} total articles")
        for article in all_articles:
            country = article.get('country')
            articles_by_country[country].append(article)
        logger.info(f"Articles grouped by {len(articles_by_country)} countries")
        return articles_by_country

    def convert_datetime_to_iso(self, data):

        if not isinstance(data, dict):
            return data
        
        # Fields that might contain datetime objects (excluding created_at)
        # Use publication_date if available, otherwise fallback to imported_at
        datetime_fields = ['publication_date', 'imported_at']
        
        converted_data = data.copy()
        
        for field in datetime_fields:
            if field in converted_data and converted_data[field] is not None:
                value = converted_data[field]
               
        # Keep created_at as datetime object for proper date comparisons
        if 'created_at' in converted_data and converted_data['created_at'] is not None:
            value = converted_data['created_at']
            if isinstance(value, str):
                # Convert string to datetime if needed
                try:
                    from dateutil.parser import parse
                    converted_data['created_at'] = parse(value)
                except:
                    pass 
        
        return converted_data

    def find_parent_child_relationships(self) -> Dict[str, List[Dict]]:
        articles_by_country = self.get_articles_by_country()
        all_relationships = {}

        for country, articles in articles_by_country.items():
            logger.info(f"Processing {len(articles)} articles for {country}")
            relationships = []

            texts = [a.get('text', '') for a in articles]
            tfidf_matrix = self.vectorizer.fit_transform(texts)
            similarity_matrix = cosine_similarity(tfidf_matrix)

            for i in range(len(articles)):
                for j in range(i + 1, len(articles)):
                    similarity = similarity_matrix[i, j]
                    if similarity < self.similarity_threshold:
                        continue

                    a1 = articles[i]
                    a2 = articles[j]
                    date1 = a1.get('publication_date') or a1.get('imported_at')
                    date2 = a2.get('publication_date') or a2.get('imported_at')

                    try:
                        if isinstance(date1, str):
                            normalized_date1 = date1.replace('Z', '+00:00')
                            if '.' in normalized_date1:
                                date1 = datetime.strptime(normalized_date1, "%Y-%m-%dT%H:%M:%S.%f%z")
                            else:
                                date1 = datetime.strptime(normalized_date1, "%Y-%m-%dT%H:%M:%S%z")
                        if isinstance(date2, str):
                            normalized_date2 = date2.replace('Z', '+00:00')
                            if '.' in normalized_date2:
                                date2 = datetime.strptime(normalized_date2, "%Y-%m-%dT%H:%M:%S.%f%z")
                            else:
                                date2 = datetime.strptime(normalized_date2, "%Y-%m-%dT%H:%M:%S%z")

                        parent, child = (a1, a2) if date1 <= date2 else (a2, a1)
                    except:
                        parent, child = a1, a2

                    # Determine relationship type
                    relationship_type = self.get_relationship_type(similarity)

                    relationship_data = {
                        'parent_id': parent['_id'],
                        'child_id': child['_id'],
                        'similarity_score': float(similarity),
                        'relationship_type': relationship_type,
                        'parent_url': parent.get('url', ''),
                        'child_url': child.get('url', ''),
                        'parent_title': parent.get('translated_title', ''),
                        'child_title': child.get('translated_title', ''),
                        'parent_date': parent.get('publication_date') or parent.get('imported_at'),
                        'child_date': child.get('publication_date') or child.get('imported_at'),
                        'country': country,
                        'created_at': datetime.now(timezone.utc)
                    }
                    
                    # Convert datetime objects to ISO format strings before MongoDB storage
                    relationship_data = self.convert_datetime_to_iso(relationship_data)
                    relationships.append(relationship_data)

            all_relationships[country] = relationships
            logger.info(f"Found {len(relationships)} relationships for {country}")

        return all_relationships

    def save_relationships_to_database(self, relationships: Dict[str, List[Dict]]) -> int:
        logger.info("Saving relationships to database...")
        ops = []
        seen_pairs = set()

        for country_rels in relationships.values():
            for rel in country_rels:
                key = (rel['parent_id'], rel['child_id'])
                if key not in seen_pairs:
                    seen_pairs.add(key)
                    ops.append(InsertOne(rel))

        if ops:
            result = relationships_collection.bulk_write(ops, ordered=False)
            logger.info(f"Saved {result.inserted_count} new relationships")
            return result.inserted_count
        return 0

    def update_articles_with_relationship_info(self, relationships: Dict[str, List[Dict]]) -> int:
        logger.info("Updating articles with relationship information...")
        updated_count = 0

        parent_ids = {rel['parent_id'] for rels in relationships.values() for rel in rels}
        child_ids = {rel['child_id'] for rels in relationships.values() for rel in rels}

        for pid in parent_ids:
            children_count = relationships_collection.count_documents({'parent_id': pid})
            update_data = {
                'is_parent': True,
                'children_count': children_count,
                'updated_at': datetime.now(timezone.utc)
            }
            # Convert datetime objects to ISO format strings before MongoDB storage
            update_data = self.convert_datetime_to_iso(update_data)
            collection.update_one({'_id': pid}, {'$set': update_data})
            updated_count += 1

        for cid in child_ids:
            rel = relationships_collection.find_one({'child_id': cid})
            if rel:
                update_data = {
                    'is_child': True,
                    'parent_id': rel['parent_id'],
                    'relationship_type': rel.get('relationship_type', 'story_thread'),
                    'updated_at': datetime.now(timezone.utc)
                }
                # Convert datetime objects to ISO format strings before MongoDB storage
                update_data = self.convert_datetime_to_iso(update_data)
                collection.update_one({'_id': cid}, {'$set': update_data})
                updated_count += 1

        logger.info(f"Updated {updated_count} articles with relationship information")
        return updated_count
    
    def find_recent_child_relationships(self) -> int:
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        six_hours_ago = now - timedelta(hours=6)
        
        # Find recent articles (last 6 hours)
        recent_articles = list(collection.find({
            'imported_at': {'$gte': six_hours_ago}
        }))

        recent_articles.sort(key=lambda a: a.get('imported_at'))

        logger.info(f"Checking {len(recent_articles)} recent articles for parent-child relationships")
        created = 0

        for article in recent_articles:
            country = article.get('country')
            if not country or not article.get('text'):
                continue

            # Get the date to use for comparison (publication_date or imported_at as fallback)
            article_date = article.get('publication_date') or article.get('imported_at')
            if not article_date:
                continue
            
            # Find older articles that can be parents
            parent_candidates = list(collection.find({
                '_id': {'$ne': article['_id']},
                'country': country,
                'text': {'$exists': True},
                '$or': [
                    {'publication_date': {'$lte': article_date}},
                    {'imported_at': {'$lte': article_date}}
                ]
            }))

            if not parent_candidates:
                continue

            all_articles = [article] + parent_candidates
            texts = [self.clean_text(a.get('text', '')) for a in all_articles]
            tfidf_matrix = self.vectorizer.fit_transform(texts)
            similarities = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:])[0]
            best_idx = similarities.argmax()
            best_score = similarities[best_idx]

            if best_score < self.similarity_threshold:
                continue

            parent = parent_candidates[best_idx]
            child = article

            # Determine relationship type
            relationship_type = self.get_relationship_type(best_score)

            # Check if relationship already exists for this parent-child pair
            existing_relation = relationships_collection.find_one({
                'parent_id': parent['_id'],
                'child_id': child['_id']
            })
            if existing_relation:
                logger.info(f"⏩ Skipping {child.get('url')} (relationship already exists)")
                continue
            
            # Check if child article already has a parent (only one parent per child)
            existing_child_parent = relationships_collection.find_one({
                'child_id': child['_id']
            })
            if existing_child_parent:
                # If existing parent has lower similarity, replace it
                if existing_child_parent['similarity_score'] < best_score:
                    logger.info(f"🔄 Replacing parent for {child.get('url')} (better similarity: {best_score:.3f} > {existing_child_parent['similarity_score']:.3f})")
                    relationships_collection.delete_one({'_id': existing_child_parent['_id']})
                else:
                    logger.info(f"⏩ Skipping {child.get('url')} (already has better parent)")
                    continue

            relationship_data = {
                'parent_id': parent['_id'],
                'child_id': child['_id'],
                'similarity_score': float(best_score),
                'relationship_type': relationship_type,
                'parent_url': parent.get('url', ''),
                'child_url': child.get('url', ''),
                'parent_title': parent.get('translated_title', ''),
                'child_title': child.get('translated_title', ''),
                'parent_date': parent.get('publication_date') or parent.get('imported_at'),
                'child_date': child.get('publication_date') or child.get('imported_at'),
                'country': country,
                'created_at': now
            }
            relationship_data = self.convert_datetime_to_iso(relationship_data)

            relationships_collection.insert_one(relationship_data)
            
            # Update child article with relationship type
            collection.update_one(
                {'_id': child['_id']},
                {'$set': {
                    'is_child': True,
                    'parent_id': parent['_id'],
                    'relationship_type': relationship_type,
                    'updated_at': now.isoformat()
                }}
            )
            
            children_count = relationships_collection.count_documents({'parent_id': parent['_id']})
            collection.update_one(
                {'_id': parent['_id']},
                {'$set': {
                    'is_parent': True,
                    'children_count': children_count,
                    'updated_at': now.isoformat()
                }}
            )
            
            # Log relationship type
            if relationship_type == "duplicate":
                logger.info(f"🔄 DUPLICATE found: {child.get('url')} ← {parent.get('url')} (score: {best_score:.3f})")
            elif relationship_type == "follow_up":
                logger.info(f" FOLLOW-UP found: {child.get('url')} ← {parent.get('url')} (score: {best_score:.3f})")
            else:
                logger.info(f"🧵 STORY THREAD found: {child.get('url')} ← {parent.get('url')} (score: {best_score:.3f})")
            
            created += 1

        logger.info(f" Total new relationships found and saved: {created}")
        return created

    def get_relationship_statistics(self) -> Dict[str, int]:
        """Get statistics about different relationship types"""
        stats = {
            'story_thread': relationships_collection.count_documents({'relationship_type': 'story_thread'}),
            'follow_up': relationships_collection.count_documents({'relationship_type': 'follow_up'}),
            'duplicate': relationships_collection.count_documents({'relationship_type': 'duplicate'}),
            'total': relationships_collection.count_documents({})
        }
        
        logger.info(f" Relationship Statistics:")
        logger.info(f"   • Story Threads: {stats['story_thread']}")
        logger.info(f"   • Follow-ups: {stats['follow_up']}")
        logger.info(f"   • Duplicates: {stats['duplicate']}")
        logger.info(f"   • Total: {stats['total']}")
        
        return stats
