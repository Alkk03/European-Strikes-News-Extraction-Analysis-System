from datetime import datetime, timezone
import json
import logging
import re
from typing import Any
from function import check_word_starts_with
from check_protests import checkProtests
from bs4 import BeautifulSoup
import requests
from cleaning_data import Cleaner, rem_apostr
from langdetect import detect
from protest_keywords import PROTEST_RE, en_protest, root_words_sv
from date_converter import normalize_publication_date
from translate import throttled_get

import time

# Disable HTTP request logging from requests library
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


cleaner = Cleaner()

def first_crawling(country_url, country=None, session=None):
    candidate_articles = []
    
    try:
        page = throttled_get(country_url, country, session=session)
        print("Status code:", page.status_code)
        logging.info("Status code: %s", page.status_code)
        
        # Check if response is valid
        if not page or not page.content:
            print(f" Empty response from {country_url}")
            logging.warning(f"Empty response from {country_url}")
            return json.dumps(candidate_articles, ensure_ascii=False)
        
        soup = BeautifulSoup(page.content, 'lxml-xml')
        
        # Check if soup is valid
        if not soup:
            print(f" Could not parse content from {country_url}")
            logging.warning(f"Could not parse content from {country_url}")
            return json.dumps(candidate_articles, ensure_ascii=False)
        
        # Try to find items - support both 'url' and 'link' elements
        items = soup.find_all('url')
        if not items:
            items = soup.find_all('link')
            if not items:
                print(f" No 'url' or 'link' items found in {country_url}")
                logging.warning(f"No 'url' or 'link' items found in {country_url}")
                return json.dumps(candidate_articles, ensure_ascii=False)
            else:
                print(f"📝 Found {len(items)} 'link' items in {country_url}")
        else:
            print(f"📝 Found {len(items)} 'url' items in {country_url}")
        
        for item in items:
            if not item:
                continue
            
            # Handle both 'url' and 'link' formats
            if item.name == 'url':
                # Sitemap format with 'url' elements
                url = item.find('loc').text if item.find('loc') else 'no url'
                language = item.find('news:language').text if item.find('news:language') else 'en'
                
                if language == 'el' or language == 'bg':
                    name = item.find('news:name').text if item.find('news:name') else 'No name'
                    language = item.find('news:language').text if item.find('news:language') else 'en'
                    try:
                        date_text = item.find('news:publication_date').text
                        publication_date = normalize_pubdate(date_text)
                    except (AttributeError, ValueError):
                        publication_date = ''

                    title = item.find('news:title').text if item.find('news:title') else ''
                    lastmod = item.find('lastmod').text if item.find('lastmod') else ''
                    keywords = item.find('news:keywords').text if item.find('news:keywords') else ''
                else:
                    name = item.find('news:name').text if item.find('news:name') else 'No name'
                    language = cleaner.clean(item.find('news:language').text) if item.find('news:language') else 'en'
                    
                    try:
                        date_text = item.find('news:publication_date').text
                        # Try ISO format first (e.g., "2025-09-04T16:54:26+02:00")
                        publication_date = normalize_pubdate(date_text)
                    except (AttributeError, ValueError):
                        publication_date = ''
                    title = item.find('news:title').text if item.find('news:title') else ''
                    lastmod = item.find('lastmod').text if item.find('lastmod') else ''
                    keywords = cleaner.clean(item.find('news:keywords').text) if item.find('news:keywords') else ''
            
            elif item.name == 'link':
                # RSS format with 'link' elements
                url = item.text.strip() if item.text else 'no url'
                # For link elements, we need to get metadata from parent or other elements
                parent = item.parent
                if parent:
                    title = parent.find('title').text if parent.find('title') else ''
                    pub_date = parent.find('pubDate').text if parent.find('pubDate') else ''
                    try:
                        publication_date = normalize_pubdate(pub_date)
                    except (AttributeError, ValueError):
                        publication_date = ''
                    lastmod = ''
                    keywords = ''
                    name = 'Unknown Source'
                    language = 'en'
                else:
                    title = ''
                    publication_date = ''
                    lastmod = ''
                    keywords = ''
                    name = 'Unknown Source'
                    language = 'en'

            candidate_articles.append({
                "label": "",
                "url": url,
                "text": '',
                "keys": 'keys',
                'sorted_keys': '',
                "title": title,
                "summary": 'summary',
                "content": '',
                "keywords": keywords,
                "translated_title": 'translated_title',
                "translated_summary": 'translated_summary',
                "translated_content": 'translated_paragraphs',
                "translated_keywords": 'translated_keywords',
                "status": 'status',
                "name": name,
                "language": language,
                "publication_date": publication_date,
                "lastmod": lastmod,
                "author": 'author',
                "processed": False,
            })
            
    except Exception as e:
        print(f" Error in first_crawling for {country_url}: {e}")
        logging.error(f"Error in first_crawling for {country_url}: {e}")
        return json.dumps(candidate_articles, ensure_ascii=False)
        
    return json.dumps(candidate_articles, ensure_ascii=False)


def normalize_pubdate(raw_date):
    """
    CENTRALIZED date normalization function for all date parsing in crawling_countries.py
    Handles all common date formats and returns datetime objects
    """
    if not raw_date or not isinstance(raw_date, str):
        return ''
    
    # Clean the date string
    raw_date = raw_date.strip()
    if not raw_date:
        return ''
    
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",       # Mon, 04 Aug 2025 07:50:30 +0200
        "%a, %d %b %Y %H:%M:%S GMT",      # Mon, 04 Aug 2025 07:50:30 GMT
        "%a, %d %b %Y %H:%M:%S UTC",      # Mon, 04 Aug 2025 07:50:30 UTC
        "%Y-%m-%dT%H:%M:%S%z",            # ISO 8601 with timezone
        "%Y-%m-%dT%H:%M:%S",              # ISO 8601 without timezone
        "%Y-%m-%d %H:%M:%S",              # 2024-01-15 10:30:00
        "%d/%m/%Y %H:%M:%S",              # 15/01/2024 10:30:00
        "%d-%m-%Y %H:%M:%S",              # 15-01-2024 10:30:00
        "%Y-%m-%d",                       # 2024-01-15
        "%d/%m/%Y",                       # 15/01/2024
        "%d-%m-%Y",                       # 15-01-2024
    ]
    
    # Try standard formats first
    for fmt in formats:
        try:
            dt = datetime.strptime(raw_date, fmt)
            return dt  # Keep as datetime object
        except ValueError:
            continue
    
    # Try ISO format with Z replacement
    try:
        if 'T' in raw_date and ('Z' in raw_date or '+' in raw_date or '-' in raw_date[-6:]):
            # Handle ISO format with Z
            iso_date = raw_date.replace('Z', '+00:00')
            dt = datetime.fromisoformat(iso_date)
            return dt
    except ValueError:
        pass
    
    # Try dateutil parser as fallback
    try:
        from dateutil.parser import parse
        dt = parse(raw_date)
        return dt
    except Exception:
        pass
    
    # If all parsing fails, return empty string
    return '' 

def sec_crawling(url, country=None, session=None):
    candidate_articles = []
    
    try:
        page = throttled_get(url, country, session=session)
        logging.info("Status code: %s", page.status_code)
        
        # Check if response is valid
        if not page or not page.content:
            print(f" Empty response from {url}")
            logging.warning(f"Empty response from {url}")
            return json.dumps(candidate_articles, ensure_ascii=False)

        soup = BeautifulSoup(page.content, 'lxml-xml')
        
        # Check if soup is valid
        if not soup:
            print(f" Could not parse content from {url}")
            logging.warning(f"Could not parse content from {url}")
            return json.dumps(candidate_articles, ensure_ascii=False)
        
        source = soup.find('channel')
        if not source:
            print(f" No 'channel' found in {url}")
            logging.warning(f"No 'channel' found in {url}")
            return json.dumps(candidate_articles, ensure_ascii=False)
            
        name = source.find('title').text if source.find('title') else 'Unknown Source'
        language = source.find('language').text if source.find('language') else 'en'

        items = soup.find_all('item')
        
        if not items:
            print(f" No 'item' elements found in {url}")
            logging.warning(f"No 'item' elements found in {url}")
            return json.dumps(candidate_articles, ensure_ascii=False)

        for item in items:
            if not item:
                continue
                
            title = rem_apostr(item.find('title').text) if item.find('title') else ''
            raw_pub_date = item.find('pubDate').text if item.find('pubDate') else ''
            pubDate = normalize_pubdate(raw_pub_date) if raw_pub_date else ''
            url = item.find('link').text if item.find('link') else ''
            author = item.find('dc:creator').text if item.find('dc:creator') else 'Anonymous'
            summary = rem_apostr(item.find('description').text if item.find('description') else '')
            category = item.find_all('category')
            categories = ' '.join(c.text for c in category)

            candidate_articles.append({
                "label": "",
                "url": url,
                "text": '',
                "keys": 'keys',
                'sorted_keys': '',
                "title": title,
                "summary": summary,
                "content": '',
                "keywords": categories,
                "translated_title": 'translated_title',
                "translated_summary": 'translated_summary',
                "translated_content": 'translated_paragraphs',
                "translated_keywords": 'translated_keywords',
                'country': 'country',
                "status": 'status',
                "name": name,
                "language": language,
                "publication_date": pubDate,
                "lastmod": '',
                "author": author,
                "processed": False,
            })
            
    except Exception as e:
        print(f" Error in sec_crawling for {url}: {e}")
        logging.error(f"Error in sec_crawling for {url}: {e}")
        return json.dumps(candidate_articles, ensure_ascii=False)
        
    return json.dumps(candidate_articles, ensure_ascii=False)

def process_romania_soup(json_article_data, corpus):
    try:
        ProtestData = []
        try:
            # Ensure json_article_data is a string that can be parsed
            if isinstance(json_article_data, str):
                json_data = json.loads(json_article_data)
            else:
                json_data = json_article_data  # If it's already parsed
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON data: {str(e)}")
            return 0

        processed_count = 0
        for article_data in json_data:
            try:
                # Ensure article_data is a dictionary
                if not isinstance(article_data, dict):
                    print(f"Skipping invalid article data: {type(article_data)}")
                    continue

                url = article_data.get('url', '')
                if not url:
                    print("Skipping article: No URL found")
                    continue

                try:
                    article_page = throttled_get(url, "romania", timeout=10)
                    article_page.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching URL {url}: {str(e)}")
                    continue

                try:
                    article_soup = BeautifulSoup(article_page.content, 'html.parser')
                except Exception as e:
                    print(f"Error parsing HTML for URL {url}: {str(e)}")
                    continue

                try:
                    content_box = article_soup.find('div', class_="realitatea-article-content-box")
                    if content_box:
                        paragraphs = ''.join(cleaner.clean(p.text) for p in content_box.find_all('p'))
                    else:
                        paragraphs = ''

                    summ_div = article_soup.find('div', class_='article-box-description')
                    summary = cleaner.clean(summ_div.text) if summ_div and summ_div.text else ''
                    aut = article_soup.find("meta", property="article:author")
                    author = aut.get('content', '') if aut else ''
                    last = article_soup.find('meta', property='article:modified_time')
                    lastmod = last.get('content', '') if last else ''
                    print(f"📄 Processing: {url}")
                    print("=" * 80)
                    article_data.update({
                        'author': author,
                        'summary': summary,
                        'paragraphs': paragraphs,
                        'lastmod': lastmod,
                        'country': 'Romania'
                    })

                    title_words = article_data.get('title', '').lower().split()
                    summary_words = article_data.get('summary', '').lower().split()
                    paragraphs_words = article_data.get('paragraphs', '').lower().split()

                    if isinstance(corpus, re.Pattern):
                        var1 = bool(corpus.search(article_data.get('title', '').lower()))
                        var2 = bool(corpus.search(summary.lower()))
                        var3 = bool(corpus.search(paragraphs.lower()))
                    else:
                        corpus = set(w.lower() for w in corpus)
                        var1 = check_word_starts_with(title_words, corpus)
                        var2 = check_word_starts_with(summary_words, corpus)
                        var3 = check_word_starts_with(paragraphs_words, corpus)
                    found_words = []
                    if isinstance(corpus, set):
                        for word in title_words + summary_words + paragraphs_words:
                            if word in corpus:
                                found_words.append(word)

                    if found_words:
                        print(f"Found words in article: {', '.join(found_words)}")
                    print(article_data.get('title', ''))
                    if var1 or var2 or var3:
                        result = checkProtests(json.dumps(article_data))
                        if result:  # Only extend if checkProtests returned valid data
                            ProtestData.append(result)
                            processed_count += 1
                            print(f"Processed article: {article_data.get('title', '')}")
                    else:
                        print(f"✘ Skipped: {article_data['title']}")

                except Exception as e:
                    print(f"Error processing article content for URL {url}: {str(e)}")
                    continue

            except Exception as e:
                print(f"Error processing article: {str(e)}")
                continue

        print(f"Total processed articles: {processed_count}")
        return processed_count

    except Exception as e:
        print(f"Unexpected error in process_romania_soup: {str(e)}")
        return 0

def process_luxembourg_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    print(json_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        print(f"Fetching: {url}")

        try:
            article_page = throttled_get(url, "luxembourg", timeout=10)
            article_page.raise_for_status()

            article_soup = BeautifulSoup(article_page.content, 'html.parser')

            # Keyword
            tag_div = article_soup.find('div', class_='Article_elementTitle__9QPjy')
            p_tag = tag_div.find('p') if tag_div else None
            keyword = p_tag.text if p_tag else ''

            # Author
            author_div = article_soup.find('div', class_="sc-a6e8a2b9-5 lckFXZ")
            author = author_div.get_text(strip=True) if author_div else 'Anonymous'

            # Paragraphs
            paragraph_tags = article_soup.find_all('div', class_="Article_elementTextblockarray__WNyan")
            paragraphs = ' '.join(rem_apostr(p.text) for p in paragraph_tags)

            # Summary
            meta = article_soup.find('meta', attrs={'name': 'description'})
            summary = rem_apostr(meta.get('content', '')) if meta else ''
            time_tag = article_soup.find('time')

            if time_tag:
                date_published = time_tag['datetime']
                date_obj = normalize_pubdate(date_published)
                # Keep as datetime object for MongoDB
            else:
                date_published = ''

            article_data['author'] = author
            article_data['summary'] = summary
            article_data['paragraphs'] = paragraphs
            article_data['keywords'] = keyword
            article_data['country'] = 'Luxembourg'
            article_data['publication_date'] = date_published

            if isinstance(corpus, re.Pattern):
                var1 = bool(corpus.search(article_data.get('title', '').lower()))
                var2 = bool(corpus.search(summary.lower()))
                var3 = bool(corpus.search(paragraphs.lower()))
            else:
                var1 = check_word_starts_with(article_data['title'].lower().split(), corpus)
                var2 = check_word_starts_with(summary.lower().split(), corpus)
                var3 = check_word_starts_with(paragraphs.lower().split(), corpus)

            if var1 or var2 or var3:
                checkProtests(json.dumps(article_data))
                processed_count += 1
                print(f"✔ Processed: {article_data['title']}")
            else:
                print(f"✘ Skipped: {article_data['title']}")

        except requests.exceptions.RequestException as e:
            print(f" Error fetching {url}: {e}")
            continue

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_germany_soup(json_article_data, corpus, session=None):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "germany", timeout=10, session=session)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article = article_soup.find_all("div", class_="c-rich-text-renderer c-rich-text-renderer--article")
        paragraphs = ''.join(p.get_text() for p in article) if article else ''

        sum = article_soup.find('meta', attrs={'name': 'description'})
        summary = sum.get('content', '') if sum else ''
        tag = article_soup.find('meta', attr={'name': 'keywords'})
        keywords = tag.get('content', '') if tag else ''

        authors = article_soup.find('span', class_="ob-unit ob-rec-author")
        author = authors.text.strip() if authors else 'Anonymous'
        title = article_soup.find('title').get_text()

        print(f"📄 Processing: {url}")
        print("=" * 80)
        article_data['author'] = author
        article_data['summary'] = cleaner.clean(summary)
        article_data['paragraphs'] = cleaner.clean(paragraphs)
        article_data['keywords'] = keywords
        article_data['title'] = title
        article_data['country'] = 'Germany'
 
        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(summary.lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data.get('title', ''))
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data.get('title', '')}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_austria_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "austria", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article_ = article_soup.find('div', class_='box col-xs-12 c_content')
        article = article_.find_all('p') if article_ else ''
        paragraphs = ''.join(cleaner.clean(p.text if hasattr(p, 'text') else p) for p in article) if article else ''

        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = cleaner.clean(summ.get('content', '')) if summ else ''

        tag_div = article_soup.find('meta', attrs={'name': 'keywords'})
        tags = tag_div.get('content', '') if tag_div else ''

        au = article_soup.find('meta', attrs={'name': 'author'})
        author = au.get('content', '') if au else ''

        print(f"📄 Processing: {url}")
        print("=" * 80)

        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tags
        article_data['language'] = detect(article_data.get('title', ''))
        article_data['country'] = 'Austria'
 
        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(article_data.get('summary', '').lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus_words = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus_words)
            var2 = check_word_starts_with(summary_words, corpus_words)
            var3 = check_word_starts_with(paragraphs_words, corpus_words)

        print(article_data.get('url', ''))
        print(article_data.get('title', ''))
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data.get('title', '')}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_greece_soup(json_article_data):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get("url", "")
        try:
            page = throttled_get(url, "greece", timeout=10)
            page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        soup = BeautifulSoup(page.content, "html.parser")

        content = soup.find("div", class_="post-body main-content pos-rel article-wrapper")
        paragraphs = rem_apostr(content.get_text()) if content else ""

        summary_tag = soup.find("meta", attrs={'name': 'description'})
        summary = rem_apostr(summary_tag.get('content', '')) if summary_tag else ''

        au = soup.find('meta', attrs={'name': 'description'})
        author = au.get('content', '') if au else ''

        print(f"📄 Processing: {url}")
        print("=" * 80)

        article_data.update({
            "author": author,
            "summary": summary,
            "paragraphs": paragraphs,
            "language": "el", 
            "country": "Greece"
        })

        title_up = article_data.get("title", "").upper()
        summary_up = summary.upper()
        paras_up = paragraphs.upper()

        var1 = bool(PROTEST_RE.search(title_up))
        var2 = bool(PROTEST_RE.search(summary_up))
        var3 = bool(PROTEST_RE.search(paras_up))

        print(url, article_data.get("title", "No title"))
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data.get('title', 'No title')}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_cyprus_soup(json_article_data):
    print(json_article_data)
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get("url", "")
        try:
            page = throttled_get(url, "cyprus", timeout=10)
            page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        soup = BeautifulSoup(page.content, "html.parser")

        content = soup.find("div", class_="article-body")
        paragraphs = ' '.join(rem_apostr(p.text) for p in content.find_all('p')) if content else ""
        summ = soup.find('meta', attrs={'name': 'description'})
        summary = rem_apostr(summ.get('content', '')) if summ else ''
        au = soup.find('meta', attrs={'name': 'author'})
        author = au.get('content', '') if au else ''
        
        # Check if we have any content before proceeding
        if not paragraphs and not summary:
            print(f"No content found for {url}, skipping...")
            continue

        print(f"📄 Processing: {url}")
        print("=" * 80)

        article_data.update({
            "author": author,
            "summary": summary,
            "paragraphs": paragraphs,
            "language": "el", 
            "country": "Cyprus"
        })

        title_up = article_data.get("title", "").upper()
        summary_up = summary.upper()
        paras_up = paragraphs.upper()

        var1 = bool(PROTEST_RE.search(title_up))
        var2 = bool(PROTEST_RE.search(summary_up))
        var3 = bool(PROTEST_RE.search(paras_up))

        print(url, article_data.get("title", "No title"))
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data.get('title', 'No title')}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_italy_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "italy", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article = article_soup.find('div', class_='story__text')
        paragraphs = ' '.join(cleaner.clean(p.get_text(strip=True)) for p in article.find_all('p')) if article else ''

        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''

        tag_div = article_soup.find('meta', attrs={'name': 'tags'})
        tags = tag_div.get('content', '') if tag_div else ''

        author_ = article_soup.find('em', class_='story__author')
        author = author_.text.strip() if author_ else 'Anonymous'

        # Print article details

        print(f"📄 Processing: {url}")
        print("=" * 80)

        article_data['author'] = author
        article_data['language'] = 'it'
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tags
        article_data['country'] = 'Italy'

        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(summary.lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data.get('title', '').lower().split()
            summary_words = article_data.get('summary', '').lower().split()
            paragraphs_words = article_data.get('paragraphs', '').lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)
        print(article_data.get('title', ''))
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data.get('title', '')}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_france_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "france", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article = article_soup.find_all('p', class_='article__paragraph')
        paragraphs = ' '.join(cleaner.clean(paragraph.text) for paragraph in article) if article else ''

        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''

        tag_div = article_soup.find('meta', attrs={'name': 'ad:keywords'})
        tags = tag_div.get('content', '') if tag_div else ''

        au = article_soup.find('meta', attrs={'property': 'og:article:author'})
        author = au.get('content', '') if au else 'Anonymous'

        print(f"📄 Processing: {url}")
        print("=" * 80)

        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tags
        article_data['country'] = 'France'
        # print(url, '\n', author, '\n', summary, '\n', paragraphs, '\n', tags, '\n')

        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(summary.lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data.get('url', ''))
        print(article_data.get('title', ''))
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data.get('title', '')}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_portugal_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "portugal", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article = article_soup.find('div', class_='story__body')
        paragraphs = ' '.join(cleaner.clean(p.get_text(strip=True)) for p in article.find_all('p')) if article else ''

        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''

        tag_div = article_soup.find('meta', attrs={'name': 'keywords'})
        tags = tag_div.get('content', '') if tag_div else ''

        author_elem = article_soup.find('span', class_='byline__name')
        author = author_elem.text.strip() if author_elem else 'Anonymous'

        print(f"📄 Processing: {url}")
        print("=" * 80)

        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tags
        article_data['country'] = 'Portugal'
        # print(url, '\n', author, '\n', summary, '\n', paragraphs, '\n', tags, '\n')

        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(summary.lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['url'])
        print(article_data['title'])
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data['title']}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_malta_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "malta", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''
        au = article_soup.find('meta', attrs={'name': 'author'})
        author = au.get('content', '') if au else ''
        main = article_soup.find('div', class_='ar-Article_Main').find_all('p') if article_soup.find('div',
                                                                                                     class_='ar-Article_Main') else ''
        paragraphs = ''.join(cleaner.clean(p.text) for p in main)
        tags = [btn.get_text(strip=True) for btn in article_soup.select('.wi-WidgetKeywords-container button.light')]

        keywords = ', '.join(tags)

        print(f"📄 Processing: {url}")
        print("=" * 80)
        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = keywords
        article_data['country'] = 'Malta'
  
        print(url, '\n', author, '\n', summary, '\n', paragraphs, '\n')
        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(summary.lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['url'])
        print(article_data['title'])
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data['title']}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_poland_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "poland", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        script_tag = article_soup.find('script', {'id': 'authors-ld', 'type': 'application/ld+json'})
        if script_tag:
            data = json.loads(script_tag.string)
            author = data["@graph"][0]["name"]
        else:
            author = "Anonymous"

        article = article_soup.find_all('p', class_="articleBodyBlock article--paragraph")
        paragraphs = ' '.join(cleaner.clean(p.get_text()) for p in article) if article else ''
        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''
        tag = article_soup.find('meta', attrs={'property': 'mrf:tags'})
        tags = tag.get('content', '') if tag else ''

        print(f"📄 Processing: {url}")
        print("=" * 80)
        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tags
        article_data['country'] = 'Poland'
        # print(url, '\n', author, '\n', summary, '\n', paragraphs, '\n', tags, '\n')

        if isinstance(en_protest, re.Pattern):
            var1 = bool(en_protest.search(article_data.get('title', '').lower()))
            var2 = bool(en_protest.search(article_data.get('summary', '').lower()))
            var3 = bool(en_protest.search(paragraphs.lower()))
        else:
            title_words = article_data.get('title', '').lower().split()
            summary_words = article_data.get('summary', '').lower().split()
            paragraphs_words = paragraphs.lower().split()
            corpus = set(w.lower() for w in en_protest)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        # print(article_data.get('title', ''))

        print(article_data['url'])
        print(article_data['title'])
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data['title']}")
    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_finland_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "finland", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        text = article_soup.find(class_='article-body ab-test-article-body width-full')
        if not text:
            text = article_soup.find_all('p', class_='article-body ab-test-article-body width-full article-body--xl')

        paragraphs = ''
        if text:
            if isinstance(text, list):
                paragraphs = ''.join(cleaner.clean(p.text if hasattr(p, 'text') else '') for p in text)
            else:
                paragraphs = ''.join(cleaner.clean(p.text if hasattr(p, 'text') else '') for p in text.find_all('p'))

        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''

        authors = article_soup.find('div', itemprop='name')
        author = cleaner.clean(authors.text) if authors else 'Anonymous'
        date_tag = article_soup.find('meta', {'itemprop': 'datePublished'})
        if date_tag:
            date_published = date_tag['content']
        else:
            date_published = ''

        print(f"📄 Processing: {url}")
        print("=" * 80)
        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['country'] = 'Finland'
        article_data['publication_date'] = date_published

        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(summary.lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['url'])
        print(article_data['title'])
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data['title']}")
    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_croatia_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "croatia", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article = article_soup.find('div', 'article__content article_content_container') or article_soup.find('div',
                                                                                                              class_='article__lead_text')
        paragraphs = ' '.join(cleaner.clean(p.get_text()) for p in
                              article.find_all('p')) if article else ''
        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''

        tag = article_soup.find('meta', atts={'name': 'keywords'})
        tags = tag.get('content', '') if tag else ''
        author = cleaner.clean(article_soup.find('div', class_='article__authors').find('a').text) if hasattr(
            article_soup.find(
                'div', class_='article__authors'), 'a') else 'Anonymous'

        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tags
        article_data['country'] = 'Croatia'
      
        print(f"📄 Processing: {url}")
        print("=" * 80)
        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(summary.lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['url'])
        print(article_data['title'])
        if var1 or var2 or var3:
            data1, data2 = checkProtests(json.dumps(article_data))
            processed_count += len(data1)
            print(f"Processed article: {article_data['title']}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_denmark_soup(json_article_data, corpus, session=None):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "denmark", timeout=10, session=session)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article = article_soup.find('main', class_='article-content')
        paragraphs = ' '.join(cleaner.clean(p.get_text()) for p in
                              article.find_all('p')) if article else ''
        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''
        tags = cleaner.clean(article_soup.find('div', class_='article-meta').find(
            class_='article-category').text) if article_soup.find('div', class_='article-meta') else ''
        author = cleaner.clean(article_soup.find('div', class_='article__authors').find('a').text) if article_soup.find(
            'div', class_='article__authors') else 'Anonymous'
        time_tag = article_soup.find('time', itemprop='datePublished')

        try:
            publication_date = normalize_pubdate(time_tag.get("datetime"))
        except (AttributeError, ValueError):
            publication_date = ''
        
        article_data['publication_date'] = publication_date
        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tags
        article_data['country'] = 'Denmark'
      
        print(f"📄 Processing: {url}")
        print("=" * 80)
        if isinstance(en_protest, re.Pattern):
            var1 = bool(en_protest.search(article_data.get('title', '').lower()))
            var2 = bool(en_protest.search(article_data.get('summary', '').lower()))
            var3 = bool(en_protest.search(paragraphs.lower()))
        else:
            title_words = article_data.get('title', '').lower().split()
            summary_words = article_data.get('summary', '').lower().split()
            paragraphs_words = paragraphs.lower().split()
            corpus = set(w.lower() for w in en_protest)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)
        print(article_data['title'])
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data['title']}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_estonia_soup(json_article_data, corpus, session=None):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "estonia", timeout=10, session=session)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''
        art = article_soup.find('div', class_="fragment fragment-html fragment-html--paragraph")
        paragraphs = ' '.join(p.text for p in art) if art else ""
        authorr = article_soup.find('meta', attrs={'name': 'cXenseParse:author'})
        author = authorr.get('content', '') if authorr else ''
        tags = article_soup.find('meta', attrs={'name': 'keywords'})
        tag = tags.get('content', '') if tags else ''
        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tag
        article_data['country'] = 'Estonia'
     
        print(f"📄 Processing: {url}")
        print("=" * 80)
        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(article_data.get('summary', '').lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = paragraphs.lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)
        # print(article_data['title'])
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data['title']}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_bulgaria_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "bulgaria", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article = article_soup.find('div', class_='post-content')
        paragraphs = ''.join(p.get_text(strip=True) for p in article.find_all('p')) if hasattr(article, 'p') else ''

        tit = article_soup.find('meta', attrs={'name': 'title'})
        title = tit.get('content', '') if tit else ''
        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''
        auth = article_soup.find('meta', attrs={'name': 'author'})
        author = auth.get('content', '') if auth else ''

        article_data['author'] = author
        article_data['title'] = title
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['country'] = 'Bulgaria'
      
        print(f"📄 Processing: {url}")
        print("=" * 80)
        title_words = article_data['title'].lower().split()
        summary_words = article_data['summary'].lower().split()
        paragraphs_words = article_data['paragraphs'].lower().split()
        corpus = set(w.lower() for w in corpus)

        var1 = check_word_starts_with(title_words, corpus)
        var2 = check_word_starts_with(summary_words, corpus)
        var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['title'])
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data['title']}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_belgium_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "belgium", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''
        art = article_soup.find('div', class_="fragment fragment-html fragment-html--paragraph")
        paragraphs = ' '.join(p.text for p in art) if art else ""
        authorr = article_soup.find('meta', attrs={'name': 'author'})
        author = authorr.get('content', '') if authorr else ''

        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['language'] = 'fr'
        article_data['country'] = 'Belgium'
     
        print(url)
        print("=====================\n")

        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(article_data.get('summary', '').lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = paragraphs.lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)
        print(article_data['title'])
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))
            processed_count += 1
            print(f"Processed article: {article_data['title']}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_netherlands_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "netherlands", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article = article_soup.find_all('p', class_='z3lfzo5 z3lfzo0 _1iobnq20')
        paragraphs = ''.join(cleaner.clean(p.text) for p in article) if article else ''

        tag = article_soup.find_all('span', class_="_1o954t80 _13ybfml0 _13ybfml1")
        tags = ''.join(cleaner.clean(p.text) for p in tag) if tag else ''

        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''
        auth = article_soup.find('meta', attrs={'name': 'author'})
        author = auth.get('content', '') if auth else ''

        print(f"📄 Processing: {url}")
        print("=" * 80)
        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tags
        article_data['country'] = 'Netherlands'
  
        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(article_data.get('summary', '').lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['title'])
        if var1 or var2 or var3:
            data1, data2 = checkProtests(json.dumps(article_data))
            processed_count += len(data1)
            print(f"Processed article: {article_data['title']}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_czech_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "czech", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        text = article_soup.find('div', class_='content')
        paragraphs = ''.join(cleaner.clean(p.text) for p in text.find_all('p')) if text else ''

        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''
        auth = article_soup.find('meta', attrs={'name': 'author'})
        author = auth.get('content', '') if auth else ''
        

        print(f"📄 Processing: {url}")
        print("=" * 80)
        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['country'] = 'Czech Republic'
     
        title_words = article_data['title'].lower().split()
        summary_words = article_data['summary'].lower().split()
        paragraphs_words = article_data['paragraphs'].lower().split()
        corpus = set(w.lower() for w in corpus)

        var1 = check_word_starts_with(title_words, corpus)
        var2 = check_word_starts_with(summary_words, corpus)
        var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['title'])
        if var1 or var2 or var3:
            data1, data2 = checkProtests(json.dumps(article_data))
            processed_count += len(data1)
            print(f"Processed article: {article_data['title']}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_lithuania_soup(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    processed_count = 0

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "lithuania", timeout=10)
            article_page.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            continue

        article_soup = BeautifulSoup(article_page.content, 'html.parser')
        article_div = article_soup.find(
            'div',
            class_="clearfix text-formatted field field--name-field-text field--type-text-long field--label-hidden field__item"
        )
        if article_div:
            paras = article_div.find_all('p')
            paragraphs = ' '.join(p.get_text(strip=True) for p in paras)
        else:
            paragraphs = ''

        print(paragraphs)
        summ = article_soup.find('meta', attrs={'name': 'description'})
        summary = summ.get('content', '') if summ else ''
        tags = cleaner.clean(article_soup.find('div', class_='article-category').text) if article_soup.find('div',
                                                                                                            class_='article-category') else ''
        author = cleaner.clean(article_soup.find('div', class_='article-author').text) if article_soup.find('div',
                                                                                                            class_='article-author') else 'Anonymous'
        # print(url, '\n', author, '\n', summary, '\n', paragraphs, '\n', tags, '\n')
        article_data['author'] = author
        article_data['summary'] = summary
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tags
        article_data['country'] = 'Lithuania'

        pub_date = article_data.get('publication_date')
        print(pub_date)
        if not pub_date or not isinstance(pub_date, str) or pub_date.strip() == '':
            time_tag = article_soup.find('time', attrs={'datetime': True})
            if time_tag: 
                pub_date = time_tag['datetime'] 
                print('FROM TIME TAG',pub_date)

        if pub_date:
            try:
                dt = normalize_pubdate(pub_date)
                article_data['publication_date'] = dt
            except Exception as e:
                print(f"Invalid date for {url}: {pub_date} ({e})")
                article_data['publication_date'] = pub_date 
        else:
            article_data['publication_date'] = ''


        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(article_data.get('summary', '').lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['title'])
        if var1 or var2 or var3:
            data1, data2 = checkProtests(json.dumps(article_data))
            processed_count += len(data1)
            print(f"Processed article: {article_data['title']}")

    print(f"Total processed articles: {processed_count}")
    return processed_count

def process_country(country: str, process_func: callable, *corpus_args: Any) -> int:
    """Process articles for a single country"""
    try:
        start_time = time.time()
        print(f"\nProcessing articles from {country.title()}...")
        rss_url = globals().get(f"{country}_url_rss")
        if not rss_url:
            print(f"Warning: No RSS URL found for {country}")
            return 0

        articles = first_crawling(rss_url)
        if not articles:
            print(f"No articles found for {country}")
            return 0

        processed = process_func(articles, *corpus_args)
        print(f" Processed {processed} articles from {country.title()}")
        elapsed_time = time.time() - start_time
        logging.info(f"TiME {country.title()}: {elapsed_time}")
        return processed

    except Exception as e:
        print(f" Error processing {country.title()}: {str(e)}")
        return 0

def slovenia_crawling():
    slovenia_url = 'https://sloveniatimes.com/feed/'
    page = throttled_get(slovenia_url, "slovenia")
    soup = BeautifulSoup(page.content, 'lxml-xml')
    source = soup.find('channel')
    name = source.find('title').text
    language = 'en'
    items = soup.find_all('item')

    for item in items:
        url = item.find('link').text
        title = item.find('title').text
        pub_element = item.find('pubDate')
        publication_date = pub_element.text.strip() if pub_element else None
        print('PUBLICATION DATE: ',publication_date)
        if publication_date:
            try:
                publication_date = normalize_pubdate(publication_date)
                print('FORMATTED DATE: ',publication_date)
            except ValueError:
                print(f"Invalid date format: {publication_date}")
                publication_date = ''
        else:
            print('-------------------NO DATE-------------------')
            publication_date = ''
        if isinstance(publication_date, datetime):
            # Keep as datetime object
            print('ISO FORMAT: ',publication_date)
        category = item.find('category').text
        description = item.find('description').text

        page2 = throttled_get(url, "slovenia")
        soup2 = BeautifulSoup(page2.content, 'html.parser')
        art = soup2.find('div', class_='abody')
        article = ' '.join(p.text for p in art) if art else ''

        clean = re.compile('<.*?>')
        text = cleaner.clean(re.sub(clean, '', article))

        print('************' * 10)
        print(text)

        json_rss = {
            'label': 'something',
            'url': url,
            'keys': 'keys',
            'title': title,
            'translated_title': title,
            'article': text,
            'translated_article': text,
            'summary': description,
            'translated_summary': description,
            'keywords': category,
            'translated_keywords': category,
            'name': name,
            'country': 'Slovenia',
            'language': language,
            'publication_date': publication_date,
            'lastmod': '',
            'author': 'Anonymous'
        }

        if isinstance(en_protest, re.Pattern):
            var1 = bool(en_protest.search(title.lower()))
            var2 = bool(en_protest.search(description.lower()))
            var3 = bool(en_protest.search(article.lower()))
        else:
            title_words = title.lower().split()
            summary_words = description.lower().split()
            paragraphs_words = article.lower().split()
            corpus = set(w.lower() for w in en_protest)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(title)
        if var1 or var2 or var3:
            checkProtests(json.dumps(json_rss))

def ireland_crawling(json_article_data):
    json_data = json.loads(json_article_data)
    for article_data in json_data:
        url = article_data.get('url', '')
        print(url)
        article_page = throttled_get(url, "ireland")
        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article = article_soup.find(class_='b-it-article-body article-body-wrapper article-sub-wrapper')
        paragraphs = ''.join(
            p.text for p in article.find_all('p')) if article else ''
        tag = article_soup.find('div', class_='c-grid b-it-overline-block').text if article_soup.find('div',
                                                                                                      class_='c-grid b-it-overline-block') else ''
        publication_date = article_soup.select_one("time").get('datetime') if article_soup.select_one("time") else ''
        try:    
            publication_date = normalize_pubdate(publication_date)
            # Convert datetime to ISO format string for JSON serialization
            # Keep as datetime object
        except (AttributeError, ValueError):
            publication_date = ''

        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tag
        print(f"Processing: {url}")
        print("=" * 80)

        json_rss = {
            'label': 'something',
            'url': url,
            'keys': 'keys',
            'title': article_data.get('title', ''),
            'translated_title': article_data.get('title', ''),
            'article': paragraphs,
            'translated_article': paragraphs,
            'summary': article_data.get('summary', ''),
            'translated_summary': article_data.get('summary', ''),
            'keywords': tag,
            'translated_keywords': tag,
            'name': 'Irish Times Feeds',
            'country': 'Ireland',
            'language': 'en',
            'publication_date': publication_date,
            'lastmod': '',
            'author': 'Anonymous'
        }

        if isinstance(en_protest, re.Pattern):
            var1 = bool(en_protest.search(article_data.get('title', '').lower()))
            var2 = bool(en_protest.search(article_data.get('summary', '').lower()))
            var3 = bool(en_protest.search(paragraphs.lower()))
        else:
            title_words = article_data.get('title', '').lower().split()
            summary_words = article_data.get('summary', '').lower().split()
            paragraphs_words = paragraphs.lower().split()
            corpus = set(w.lower() for w in en_protest)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data.get('title', ''))
        if var1 or var2 or var3:
            checkProtests(json.dumps(json_rss))

def hungary_crawling(json_article_data, corpus):
    json_data = json.loads(json_article_data)

    for article_data in json_data:
        url = article_data.get('url', '')
        article_page = throttled_get(url, "hungary")
        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        paragraphs = ''.join(
            p.text for p in article_soup.find('div', itemprop="articleBody").find_all('p')
            if
            article_soup.find('div', itemprop="articleBody").find_all('p'))

        time_ = article_soup.find('p', itemprop='datePublished')
        publication_date = time_.text if time_ else ''
        if not publication_date:
            scripts = article_soup.find_all("script", type="application/ld+json")
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, list):
                        for item in data:
                            if item.get("@type") == "NewsArticle":
                                publication_date = item.get("datePublished", '')
                                break
                    elif data.get("@type") == "NewsArticle":
                        publication_date = data.get("datePublished", '')
                except (json.JSONDecodeError, TypeError):
                    continue
        try:
            publication_date = normalize_pubdate(publication_date)
            # Convert datetime to ISO format string for JSON serialization
            # Keep as datetime object
        except (AttributeError, ValueError):
            publication_date = ''

        article_data['paragraphs'] = paragraphs
        article_data['language'] = 'hu'
        article_data['publication_date'] = publication_date
        article_data['lastmod'] = publication_date
        article_data['country'] = 'Hungary'
        article_data['publication_date'] = publication_date
        print(f"📄 Processing: {url}")
        print("=" * 80)

        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(article_data.get('summary', '').lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['title'])
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))

def spain_crawling(json_article_data, corpus):
    json_data = json.loads(json_article_data)

    for article_data in json_data:
        url = article_data.get('url', '')
        try:
            article_page = throttled_get(url, "spain")
            article_soup = BeautifulSoup(article_page.content, 'html.parser')

            article = article_soup.find('div', class_='a_c clearfix')
            paragraphs = ''.join(
                x.text for x in article.find_all('p')) if article else ''

            # tags = article_soup.find('div', class_="cs_t").text if article_soup.find('div', class_="cs_t") else ''

            time_element = article_soup.select_one("time")
            publication_date = time_element.get('datetime') if time_element else article_data.get('publication_date',
                                                                                                  '')
            try:
                publication_date = normalize_pubdate(publication_date)
                # Convert datetime to ISO format string for JSON serialization
                # Keep as datetime object
            except (AttributeError, ValueError):
                publication_date = ''

            print(url)
            print("=====================\n")

            article_data['paragraphs'] = paragraphs
            article_data['lastmod'] = publication_date
            article_data['publication_date'] = publication_date
            article_data['country'] = 'Spain'

            if isinstance(corpus, re.Pattern):
                var1 = bool(corpus.search(article_data.get('title', '').lower()))
                var2 = bool(corpus.search(article_data.get('summary', '').lower()))
                var3 = bool(corpus.search(paragraphs.lower()))
            else:
                title_words = article_data['title'].lower().split()
                summary_words = article_data['summary'].lower().split()
                paragraphs_words = paragraphs.lower().split()
                corpus = set(w.lower() for w in corpus)
                var1 = check_word_starts_with(title_words, corpus)
                var2 = check_word_starts_with(summary_words, corpus)
                var3 = check_word_starts_with(paragraphs_words, corpus)

            print(article_data['title'])
            if var1 or var2 or var3:
                checkProtests(json.dumps(article_data))
        except Exception as e:
            print(f"Error processing article {url}: {str(e)}")
            continue

def sweden_crawling(json_article_data, corpus):
    json_data = json.loads(json_article_data)

    for article_data in json_data:
        url = article_data.get('url', '')
        article_page = throttled_get(url, "sweden")
        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article = article_soup.find('div', class_='sc-bf0483d0-1 kgpqAW')
        paragraphs = ''.join(article.text) if article else ''
        au = article_soup.find('meta', attrs={'property': 'article:author'})
        author = au.get('content', '') if au else 'Anonymous'

        article_data['paragraphs'] = paragraphs
        article_data['language'] = 'sv'
        article_data['author'] = author
        article_data['country'] = 'Sweden'

        print(url)
        print("=====================\n")

        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(article_data.get('summary', '').lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['title'])
        if var1 or var2 or var3:
            # Convert datetime to ISO string before JSON serialization
            article_data_for_json = article_data.copy()
            if isinstance(article_data_for_json.get('publication_date'), datetime):
                # Keep as datetime object - no conversion needed
            checkProtests(json.dumps(article_data_for_json))

def slovakia_crawling(json_article_data, corpus):
    json_data = json.loads(json_article_data)

    for article_data in json_data:
        url = article_data.get('url', '')
        article_page = throttled_get(url, "slovakia")
        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        article = article_soup.find('article',
                                    class_='js-remp-article-data cf js-font-resize js-article-stats-item')
        paragraphs = ''.join(p.text for p in article.find_all('p')) if article else ''
        au = article_soup.find('meta', attrs={'name': 'author'})
        author = au.get('content', '') if au else 'Anonymous'
        key = article_soup.find('meta', attrs={'name': 'keywords'})
        keywords = key.get('content', '') if key else ''
        print(url)
        print("=====================\n")
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = keywords
        article_data['author'] = author
        article_data['country'] = 'Slovakia'
        # Check if publication_date is already in ISO format
        if isinstance(article_data['publication_date'], str):
            if 'T' in article_data['publication_date']:
                # Already in ISO format
                try:
                    publication_date = normalize_pubdate(article_data['publication_date'])
                except ValueError:
                    publication_date = article_data['publication_date']
            else:
                # Try to parse as RSS format
                try:
                    publication_date = normalize_pubdate(article_data['publication_date'])
                except ValueError:
                    publication_date = article_data['publication_date']
        else:
            publication_date = article_data['publication_date']
        
        article_data['publication_date'] = publication_date


        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(article_data.get('summary', '').lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['title'])
        if var1 or var2 or var3:
            # Convert datetime to ISO string before JSON serialization
            article_data_for_json = article_data.copy()
            if isinstance(article_data_for_json.get('publication_date'), datetime):
                # Keep as datetime object - no conversion needed
            checkProtests(json.dumps(article_data_for_json))

def latvia_crawling(json_article_data, corpus):
    json_data = json.loads(json_article_data)
    for article_data in json_data:
        url = article_data.get('url', '')
        print(url)
        article_page = throttled_get(url, "latvia")
        article_soup = BeautifulSoup(article_page.content, 'html.parser')

        tags = article_soup.find('meta', attrs={'name': 'keywords'})
        tag = tags.get('content', '') if tags else ''
        paragraph = article_soup.find('section', class_="block article__body")
        paragraphs = ''.join(p.text for p in paragraph.find_all('p')) if paragraph else ''
        article_data['paragraphs'] = paragraphs
        article_data['keywords'] = tag
        # Handle publication_date - check if it's already in ISO format
        if isinstance(article_data['publication_date'], str):
            if 'T' in article_data['publication_date']:
                # Already in ISO format
                try:
                    dt = normalize_pubdate(article_data['publication_date'])
                    # Keep as datetime object
                except ValueError:
                    iso_format = article_data['publication_date']
            else:
                # Try to parse as RSS format
                try:
                    dt = normalize_pubdate(article_data['publication_date'])
                    # Keep as datetime object
                except ValueError:
                    iso_format = article_data['publication_date']
        else:
            iso_format = article_data['publication_date']
        
        article_data['publication_date'] = iso_format
        article_data['author'] = 'Anonymous'
        article_data['country'] = 'Latvia'

        print(url)
        print("=====================\n")

        if isinstance(corpus, re.Pattern):
            var1 = bool(corpus.search(article_data.get('title', '').lower()))
            var2 = bool(corpus.search(article_data.get('summary', '').lower()))
            var3 = bool(corpus.search(paragraphs.lower()))
        else:
            title_words = article_data['title'].lower().split()
            summary_words = article_data['summary'].lower().split()
            paragraphs_words = article_data['paragraphs'].lower().split()
            corpus = set(w.lower() for w in corpus)
            var1 = check_word_starts_with(title_words, corpus)
            var2 = check_word_starts_with(summary_words, corpus)
            var3 = check_word_starts_with(paragraphs_words, corpus)

        print(article_data['title'])
        if var1 or var2 or var3:
            checkProtests(json.dumps(article_data))

