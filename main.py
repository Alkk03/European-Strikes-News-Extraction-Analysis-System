import logging

# Import all the modular components
from config import logger, setup_huggingface
from crawler import run_single_pass_all_countries
from database import (
    transfer_prediction_articles,
    transfer_eu_articles_to_final,
    extract_event_patterns_from_final_strikes,
    check_parent_child_relationships_final
)
from ml_models import generate_summaries_and_labels
from translate import check_translation_service, close_all_sessions

# ===================== MAIN EXECUTION PIPELINE =====================

def main():
    """
    Main execution pipeline for the European Strikes News Extraction System.
    Executes all steps in the correct order.
    """
    
    print("\n" + "="*80)
    print("EUROPEAN STRIKES NEWS EXTRACTION SYSTEM")
    print("="*80)
    
    # Check translation service first
    print("Checking translation service availability...")
    translation_available = check_translation_service()
    if translation_available:
        print("Translation service is ready")
    else:
        print("Translation service not available - will use fallbacks")
    
    # Step 1: Single pass crawling
    print("\n" + "="*80)
    print("STEP 1: ONE RSS FETCH PER COUNTRY")
    print("="*80)
    
    try:
        total_processed, total_visited = run_single_pass_all_countries()
        print(f"Single pass crawling completed: {total_processed} protests found, {total_visited} articles visited")
    except Exception as e:
        print(f"Single pass crawling failed: {e}")
        logging.error(f"Single pass crawling failed: {e}")
    
    # Step 3: Transfer prediction articles with location processing
    print("\n" + "="*80)
    print("STEP 2: TRANSFER PREDICTION ARTICLES WITH LOCATION PROCESSING")
    print("="*80)
    transfer_prediction_articles()
    
    # Step 4: Transfer EU articles to final database
    print("\n" + "="*80)
    print("STEP 3: TRANSFER EU ARTICLES TO FINAL DATABASE")
    print("="*80)
    transfer_eu_articles_to_final()
    
    # Step 5: Extract event patterns from final_strikes
    print("\n" + "="*80)
    print("STEP 4: EXTRACT EVENT PATTERNS FROM FINAL_STRIKES")
    print("="*80)
    
    try:
        updated_count = extract_event_patterns_from_final_strikes()
        if updated_count > 0:
            print(f"Event pattern extraction completed successfully!")
            print(f"Updated {updated_count} articles with event patterns")
        else:
            print("Event pattern extraction failed!")
    except Exception as e:
        print(f"Error in event pattern extraction: {e}")
        logging.error(f"Error in event pattern extraction: {e}")
    
    # Step 6: Check parent-child relationships in final database
    print("\n" + "="*80)
    print("STEP 5: CHECK PARENT-CHILD RELATIONSHIPS IN FINAL DATABASE")
    print("="*80)
    
    try:
        relationships_success = check_parent_child_relationships_final()
        if relationships_success:
            print("Parent-child relationships check completed successfully!")
        else:
            print("Parent-child relationships check failed!")
    except Exception as e:
        print(f"Error in parent-child relationships check: {e}")
        logging.error(f"Error in parent-child relationships check: {e}")
    
    # Step 7: Create labels and summaries for final database
    print("\n" + "="*80)
    print("STEP 6: CREATE LABELS AND SUMMARIES FOR FINAL DATABASE")
    print("="*80)
    
    try:
        story_threads_count = generate_summaries_and_labels()
        if story_threads_count > 0:
            print(f"Story threads creation completed successfully!")
            print(f"Created {story_threads_count} story threads")
        else:
            print("Story threads creation failed!")
    except Exception as e:
        print(f"Error in story threads creation: {e}")
        logging.error(f"Error in story threads creation: {e}")
    
    print("\n" + "="*80)
    print("ALL PROCESSES COMPLETED!")
    print("="*80)
    
    # Final cleanup
    try:
        print("Cleaning up sessions...")
        close_all_sessions()
        print("All sessions closed successfully")
    except Exception as e:
        print(f"Error during cleanup: {e}")
        logging.warning(f"Error during cleanup: {e}")

if __name__ == "__main__":
    main()
