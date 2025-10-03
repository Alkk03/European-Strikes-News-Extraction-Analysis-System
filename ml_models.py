"""
Machine Learning models module for the European Strikes News Extraction System.
Contains functions for summarization, classification, and participant count extraction.
"""

import pickle
import logging
from datetime import datetime, timezone, timedelta

import torch
import torch.nn.functional as F
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification

from config import MODEL_PATHS, logger
from utils import _make_fallback_summary

# ===================== MULTILABEL CLASSIFICATION =====================

def load_multilabel_model():
    """Load the multilabel RoBERTa model for story thread categorization"""
    try:
        model_path = MODEL_PATHS['multilabel_roberta']
        tokenizer = AutoTokenizer.from_pretrained(model_path)
        model = AutoModelForSequenceClassification.from_pretrained(model_path)
        model.eval()
        
        # Load label binarizer to get category names
        with open(f"{model_path}/label_binarizer.pkl", 'rb') as f:
            label_binarizer = pickle.load(f)
        
        return tokenizer, model, label_binarizer
    except Exception as e:
        print(f" Error loading multilabel model: {e}")
        return None, None, None

def categorize_story_thread(text, tokenizer, model, label_binarizer):
    """Categorize story thread text"""
    try:
        if not all([tokenizer, model, label_binarizer]):
            return {"category": "unknown", "confidence": 0.0, "all_predictions": {}}

        text = text.strip()
        if not text:
            return {"category": "unknown", "confidence": 0.0, "all_predictions": {}}

        # Truncate to 500 characters, but try to break at word boundary
        if len(text) > 500:
            truncated = text[:500]
            last_space = truncated.rfind(' ')
            if last_space > 400:
                text = truncated[:last_space]
            else:
                text = truncated

        inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
        with torch.no_grad():
            outputs = model(**inputs)
            probs = torch.sigmoid(outputs.logits)

        predictions = (probs > 0.5).squeeze().cpu().numpy()
        categories = getattr(label_binarizer, 'classes_', [f"Category_{i}" for i in range(len(predictions))])

        max_prob_idx = torch.argmax(probs).item()
        max_prob = probs[0][max_prob_idx].item()

        high_confidence_predictions = {
            category: float(prob)
            for category, prob in zip(categories, probs[0].cpu().numpy()) if prob > 0.5
        }

        predicted_category = categories[max_prob_idx] if max_prob > 0.5 else "unknown"

        return {
            "category": predicted_category,
            "confidence": float(max_prob),
            "all_predictions": high_confidence_predictions
        }

    except Exception as e:
        print(f" Error in story thread categorization: {e}")
        return {"category": "unknown", "confidence": 0.0, "all_predictions": {}}

# ===================== SUMMARIZATION =====================

def generate_summaries_and_labels():
    """Generate summary, multilabel classification, and participant count for recent articles"""
    try:
        print("\n" + "="*80)
        print("GENERATING SUMMARIES, LABELS, PARTICIPANT COUNTS FOR RECENT ARTICLES")
        print("="*80)

        from config import get_database_connections
        client, databases, collections = get_database_connections()
        records_final = collections['final']

        now = datetime.now(timezone.utc)
        six_hours_ago = now - timedelta(hours=6)
        print(f"Searching for articles imported after {six_hours_ago}")

        articles = list(records_final.find({"imported_at": {"$gte": six_hours_ago}}))
        print(f"Found {len(articles)} recent articles")

        if not articles:
            print(" No recent articles found, looking for articles without imported_at field...")
            articles = list(records_final.find({"imported_at": {"$exists": False}}))
            print(f"Found {len(articles)} articles without imported_at field")
            if not articles:
                print(" No articles without imported_at found, getting all articles...")
                articles = list(records_final.find({}))
                print(f"Found {len(articles)} total articles")

        if not articles:
            return 0

        summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

        tokenizer, model, label_binarizer = load_multilabel_model()

        updated_articles = []
        for idx, article in enumerate(articles):
            try:
                article_id = article['_id']                
                               
                article_text = article.get('text').strip()

                if not article_text or len(article_text.split()) < 10:
                    summary = article.get('text', '') or _make_fallback_summary(article_text)
                else:
                    if summarizer:
                        word_count = len(article_text.split())
                        max_len = 50 if word_count > 70 else 20
                        min_len = 25 if word_count > 70 else 10
                        try:
                            summary = summarizer(article_text, max_length=max_len, min_length=min_len, do_sample=False)[0]['summary_text']
                        except Exception:
                            summary = _make_fallback_summary(article_text)
                    else:
                        summary = _make_fallback_summary(article_text)

                category_result = categorize_story_thread(article_text, tokenizer, model, label_binarizer)
                category_result.setdefault("category", "unknown")
                category_result.setdefault("confidence", 0.0)
                category_result.setdefault("all_predictions", {})

                update_fields = {
                    'summary': summary,
                    'category': category_result.get('category', 'unknown'),
                    'confidence': category_result.get('confidence', 0.0),
                    'all_predictions': category_result.get('all_predictions', {}),
                }

                records_final.update_one({'_id': article_id}, {'$set': update_fields})
                updated_articles.append(article_id)

                if idx % 10 == 0:
                    print(f" Processed {idx + 1}/{len(articles)}")

            except Exception as e:
                print(f" Error processing article: {e}")
                logger.error(f"Error processing article {article.get('_id', '')}: {e}")
                continue

        print("\n" + "="*80)
        print(f" Finished! Total articles updated: {len(updated_articles)}")
        print("="*80)
        print(f" Skipped articles (already processed): {skipped_articles}")

        return len(updated_articles)

    except Exception as e:
        print(f" Error in processing: {e}")
        logger.error(f"Fatal error in summary/classification pipeline: {e}")
        return 0
