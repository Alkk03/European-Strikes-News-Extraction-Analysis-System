"""
Configuration module for the European Strikes News Extraction System.
Contains all constants, database connections, and configuration settings.
"""

import os
import logging
from pymongo import MongoClient


# ===================== LOGGING CONFIGURATION =====================

def setup_logging():
    """Configure logging for the application."""
    # Quiet noisy libs
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    # Configure logging once at the beginning
    root = logging.getLogger()
    root.setLevel(logging.WARNING)  # Reduced logging for less noise
    root.handlers.clear()

    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)

    fh = logging.FileHandler('crawl_errors.log')
    fh.setFormatter(fmt)
    root.addHandler(fh)

    return logging.getLogger(__name__)

# ===================== DATABASE CONFIGURATION =====================

# MongoDB connection string
CONNECTION_STRING = os.environ.get("MONGODB_URI")
if not CONNECTION_STRING:
    raise ValueError("MONGODB_URI environment variable is required. Please set it in your .env file or environment.")

# Database and collection names
DATABASE_NAMES = {
    'main': 'european_strikes',
    'analysis': 'european_strikes_analysis', 
    'final': 'final_strikes',
}

COLLECTION_NAMES = {
    'main': 'dataset',
    'analysis': 'analyzed_strikes',
    'final': 'analyzed_strikes',
    'relationships': 'parent_child_relationships'
}

def get_database_connections():
    """Get MongoDB database connections."""
    client = MongoClient(CONNECTION_STRING)
    
    databases = {}
    for key, db_name in DATABASE_NAMES.items():
        databases[key] = client[db_name]
    
    collections = {}
    for key, coll_name in COLLECTION_NAMES.items():
        if key in databases:
            collections[key] = databases[key][coll_name]
    
    return client, databases, collections

# ===================== CRAWLING CONFIGURATION =====================

# RSS refresh intervals (seconds)
RSS_REFRESH_SEC = int(os.environ.get("RSS_REFRESH_SEC", "300"))  # default 5'
DEMO_MINUTES = int(os.environ.get("DEMO_MINUTES", "10"))  # for the demo loop

# Country-specific RSS refresh intervals
COUNTRY_RSS_REFRESH = {
    "estonia": 60,
    "denmark": 30,
    "_default": 1       # All others 1 second
}

# ===================== URL CANONICALIZATION =====================

TRACKING_PREFIXES = ('utm_', 'fbclid', 'gclid', 'mc_cid', 'mc_eid', 'ref', 'source')

# ===================== EU COUNTRIES =====================

EU_COUNTRIES = {
    'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic',
    'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary',
    'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta',
    'Netherlands', 'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia',
    'Spain', 'Sweden', 'Europe'
}

# ===================== HUGGING FACE CONFIGURATION =====================

def setup_huggingface():
    """Setup Hugging Face authentication."""
    try:
        from huggingface_hub import login as hf_login
        HF_TOKEN = os.environ.get("HF_TOKEN")
        if HF_TOKEN:
            try:
                hf_login(HF_TOKEN)
                logger = logging.getLogger(__name__)
                logger.info("Hugging Face login successful.")
                return True
            except Exception as e:
                logger = logging.getLogger(__name__)
                logger.warning(f"Hugging Face login failed: {e}")
                return False
        else:
            logger = logging.getLogger(__name__)
            logger.info("No HF_TOKEN provided. Proceeding without login.")
            return False
    except Exception:
        logger = logging.getLogger(__name__)
        logger.info("huggingface_hub not installed. Proceeding without login.")
        return False

# ===================== MODEL PATHS =====================

MODEL_PATHS = {
    'multilabel_roberta': './multilabel_roberta_model',
    'trained_roberta': './binary_classification/trained_roberta_model'
}


# ===================== COUNTRY PROCESSORS AND RSS URLS =====================

# Import country processors and RSS URLs from existing modules
try:
    from protest_keywords import (
        luxembourg_url_rss, greece_url_rss, cyprus_url_rss, portugal_url_rss,
        estonia_url_rss, bulgaria_url_rss, belgium_url_rss, czech_url_rss,
        lithuania_url_rss, sweden_url_rss, slovakia_url_rss, ireland_url_rss,
        denmark_url_rss, romania_url_rss, italy_url_rss, france_url_rss,
        germany_url_rss, austria_url_rss, malta_url_rss, poland_url_rss,
        finland_url_rss, croatia_url_rss, netherlands_url_rss, hungary_url_rss,
        spain_url_rss, latvia_url_rss,
        de_words, hu_protest, root_words_sk, pt_protest, et_protest,
        root_words_bg, fr_protest, root_words_extended_cs, de_en_protest,
        root_words_sv, da_protest, ro_protest, it_protest, en_protest,
        pl_words, fi_protest, hr_words, nl_protest, lt_protest, lv_protest
    )
    
    from country_crawlers import (
        process_luxembourg_soup, process_greece_soup, process_cyprus_soup,
        process_portugal_soup, process_estonia_soup, process_bulgaria_soup,
        process_belgium_soup, process_czech_soup, ireland_crawling, process_denmark_soup,
        process_romania_soup, process_italy_soup, process_france_soup, process_germany_soup,
        process_austria_soup, process_malta_soup, process_poland_soup,
        process_finland_soup, process_croatia_soup, process_netherlands_soup,
        process_lithuania_soup, sweden_crawling, slovakia_crawling, hungary_crawling,
        spain_crawling, latvia_crawling, slovenia_crawling, first_crawling, sec_crawling
    )
    
    # RSS URLs mapping
    RSS_URLS = {
        'luxembourg': luxembourg_url_rss,
        'greece': greece_url_rss,
        'cyprus': cyprus_url_rss,
        'portugal': portugal_url_rss,
        'estonia': estonia_url_rss,
        'bulgaria': bulgaria_url_rss,
        'belgium': belgium_url_rss,
        'czech': czech_url_rss,
        'lithuania': lithuania_url_rss,
        'sweden': sweden_url_rss,
        'slovakia': slovakia_url_rss,
        'ireland': ireland_url_rss,
        'denmark': denmark_url_rss,
        'romania': romania_url_rss,
        'italy': italy_url_rss,
        'france': france_url_rss,
        'germany': germany_url_rss,
        'austria': austria_url_rss,
        'malta': malta_url_rss,
        'poland': poland_url_rss,
        'finland': finland_url_rss,
        'croatia': croatia_url_rss,
        'netherlands': netherlands_url_rss,
        'hungary': hungary_url_rss,
        'spain': spain_url_rss,
        'latvia': latvia_url_rss,
    }
    
    # Country processing functions mapping
    COUNTRY_PROCESSORS = {
        'luxembourg': (process_luxembourg_soup, [de_words]),
        'greece': (process_greece_soup, []),
        'hungary': (hungary_crawling, [hu_protest]),
        'slovakia': (slovakia_crawling, [root_words_sk]),
        'cyprus': (process_cyprus_soup, []),
        'portugal': (process_portugal_soup, [pt_protest]),
        'estonia': (process_estonia_soup, [et_protest]),
        'bulgaria': (process_bulgaria_soup, [root_words_bg]),
        'belgium': (process_belgium_soup, [fr_protest]),
        'czech': (process_czech_soup, [root_words_extended_cs]),
        'austria': (process_austria_soup, [de_en_protest]),
        'sweden': (sweden_crawling, [root_words_sv]),
        'ireland': (ireland_crawling, []),
        'denmark': (process_denmark_soup, [da_protest]),
        'romania': (process_romania_soup, [ro_protest]),
        'italy': (process_italy_soup, [it_protest]),
        'france': (process_france_soup, [fr_protest]),
        'germany': (process_germany_soup, [de_words]),
        'malta': (process_malta_soup, [en_protest]),
        'poland': (process_poland_soup, [pl_words]),
        'finland': (process_finland_soup, [fi_protest]),
        'croatia': (process_croatia_soup, [hr_words]),
        'netherlands': (process_netherlands_soup, [nl_protest]),
        'lithuania': (process_lithuania_soup, [lt_protest]),
        'spain': (spain_crawling, [en_protest]),
        'latvia': (latvia_crawling, [lv_protest]),
        'slovenia': (slovenia_crawling, []),
    }
    
except ImportError as e:
    print(f"Warning: Could not import country processors: {e}")
    RSS_URLS = {}
    COUNTRY_PROCESSORS = {}

# ===================== INITIALIZATION =====================

# Initialize logging
logger = setup_logging()

# Setup Hugging Face
setup_huggingface()


