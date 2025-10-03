# European Strikes News Extraction System

A modular system for extracting and analyzing news about strikes and protests across European countries.

## Features

- **Multi-country RSS crawling** for 27 EU countries
- **Machine learning classification** using RoBERTa models
- **Automatic translation** support for multilingual content
- **Event pattern extraction** and relationship mapping
- **MongoDB integration** for data storage and analysis

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up environment variables:**
   ```bash
   # Copy the example environment file
   cp env.example .env
   
   # Edit .env with your actual credentials
   nano .env
   ```
   
   **Required environment variables:**
   - `MONGODB_URI` - Your MongoDB connection string
   - `HF_TOKEN` - Your Hugging Face authentication token
   - `GEONAMES_USERNAME` - Your GeoNames API username (free at geonames.org)

3. **Run the complete pipeline:**
   ```bash
   python main.py
   ```

## Project Structure

- `main.py` - Main execution pipeline
- `config.py` - Configuration and settings
- `cooperative_scheduler.py` - Country state management and scheduling
- `country_crawlers.py` - Country-specific RSS crawling functions
- `crawler.py` - Web crawling logic
- `database.py` - MongoDB operations
- `location_extractor.py` - Location extraction and geocoding
- `ml_models.py` - Machine learning models
- `processor.py` - Article processing
- `protest_keywords.py` - Protest keywords and RSS URLs
- `article_relationships.py` - Parent-child relationship detection
- `translate.py` - Translation services
- `utils.py` - Utility functions

## Configuration

The system uses environment variables for configuration:

- `MONGODB_URI` - MongoDB connection string
- `HF_TOKEN` - Hugging Face authentication token
- `GEONAMES_USERNAME` - GeoNames API username (free at geonames.org)
- `RSS_REFRESH_SEC` - RSS refresh interval (default: 300 seconds)

## Documentation

This is a modular system with the following main components:

- **Cooperative Scheduler**: Manages country states and crawling schedules
- **Country Crawlers**: RSS feed processing for 27 EU countries
- **Location Extractor**: Geographic location extraction and geocoding
- **ML Models**: RoBERTa-based protest classification
- **Database**: MongoDB operations for data storage
- **Article Relationships**: Parent-child relationship detection
- **Translation**: Multi-language support with fallbacks
- **Processing**: Article processing and analysis

## License

This project is for educational and research purposes.
