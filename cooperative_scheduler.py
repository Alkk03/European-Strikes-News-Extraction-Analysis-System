"""
Cooperative Scheduler & Country State Management for the European Strikes News Extraction System.
Contains CountryState and CooperativeScheduler classes for single-threaded RSS crawling.
"""

import inspect
import json
import logging
import time
from typing import Any, Callable, Dict, List, Set
from time import monotonic

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import COUNTRY_RSS_REFRESH
from utils import canonicalize_url


# ===================== COUNTRY STATE =====================

def _build_retrying_session(pool_connections=20, pool_maxsize=100):
    """Build a requests session with retry logic."""
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=Retry(
            total=2,
            backoff_factor=0.2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=None, 
        ),
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

class CountryState:
    """Country state for cooperative, single-thread scheduler."""
    
    def __init__(self, country: str, process_func, corpus_args: list, rss_url: str):
        self.country = country
        self.process_func = process_func
        self.corpus_args = corpus_args or []
        self.rss_url = rss_url
        self.session = _build_retrying_session()
        self.jobs: list[Any] = []         
        self.processed_count = 0
        self.visited_count = 0
        self.fetched_count = 0

        self.refresh_sec = COUNTRY_RSS_REFRESH.get(country, COUNTRY_RSS_REFRESH["_default"])
        self.next_rss_ts = 0.0  # monotonic timestamp for next fetch
        
        # Process cooldown
        self.process_interval = COUNTRY_RSS_REFRESH.get(country, COUNTRY_RSS_REFRESH["_default"])
        self.next_process_ts = 0.0          # when the next job is "allowed"
        
        # Seen URLs tracking to avoid duplicate processing
        self.seen_urls_recent: set[str] = set()

        self.logger = logging.getLogger(f"Coop-{country}")
        self.logger.setLevel(logging.INFO)

    def _enqueue(self, articles_json):
        """Enqueue articles for processing."""
        if not articles_json:
            return
        try:
            items = articles_json
            if isinstance(articles_json, str):
                items = json.loads(articles_json)
            if isinstance(items, dict):
                items = [items]
            for it in items:
                url = canonicalize_url(it.get("url") or it.get("link") or "")
                if url and url not in self.seen_urls_recent:
                    self.jobs.append(it)
                    self.seen_urls_recent.add(url)
        except Exception:
            self.logger.warning(f"[{self.country}] Cannot parse articles_json; skipping.")

    def due_for_fetch(self, now_ts: float) -> bool:
        """Check if it's time to fetch RSS."""
        return now_ts >= self.next_rss_ts

    def fetch_rss_if_due(self, now_ts: float):
        """If it's time, do RSS fetch and fill queue with articles."""
        if not self.due_for_fetch(now_ts):
            return False

        try:
            self.logger.info(f"[{self.country}] Fetching RSS from {self.rss_url}")
            # Check if URL is a sitemap (contains 'sitemap' or ends with '/news.xml' for Google News sitemaps)
            is_sitemap = ("sitemap" in (self.rss_url or "").lower() or 
                         (self.rss_url or "").endswith('/news.xml'))
            
            from country_crawlers import first_crawling, sec_crawling
            if is_sitemap:
                articles_json = first_crawling(self.rss_url, self.country, session=self.session)
            else:
                articles_json = sec_crawling(self.rss_url, self.country, session=self.session)

            if articles_json:
                self._enqueue(articles_json)
                self.fetched_count += 1
                self.logger.info(f"[{self.country}] Enqueued articles (RSS fetch #{self.fetched_count})")
        except Exception as e:
            self.logger.error(f"[{self.country}] RSS fetch error: {e}")

        # Schedule the next fetch
        self.next_rss_ts = now_ts + self.refresh_sec
        return True

    def _call_processor_safely(self, json_blob):
        """
        Calls the processor with a flexible signature:
        Tries with (json, *corpus), then (json). Adds session=... if supported.
        """
        func = self.process_func
        last_err = None

        try:
            sig = inspect.signature(func)
            params = sig.parameters
        except (ValueError, TypeError):
            params = {}

        kwargs = {}
        if 'session' in params:
            kwargs['session'] = self.session

        attempts = [
            ( [json_blob, *self.corpus_args], kwargs ),  # json + corpus + session
            ( [json_blob],                     kwargs ),  # json + session
        ]

        for args, kw in attempts:
            try:
                return func(*args, **kw)
            except TypeError as e:
                last_err = e
                continue

        # If every attempt failed, re-raise the last TypeError so it logs cleanly
        raise last_err if last_err else RuntimeError("Unknown processor call failure")

    def process_one_job_if_any(self, now_ts: float) -> bool:
        """Process EXACTLY 1 job if:
           - has work AND
           - cooldown has passed (next_process_ts)."""
        if not self.jobs or now_ts < self.next_process_ts:
            return False

        job = self.jobs.pop(0)
        try:
            self.logger.info(f"[{self.country}] Processing article...")
            # Your processors expect JSON. Wrap the single article in a list.
            processed = self._call_processor_safely(json.dumps([job]))
            self.processed_count += int(processed or 0)
            self.visited_count += 1

            self.logger.info(
                f"[{self.country}] +{int(processed or 0)} protests "
                f"(visited total: {self.visited_count}, processed total: {self.processed_count})"
            )
        except Exception as e:
            self.logger.error(f"[{self.country}] Processing error: {e}")

        # set when the next job for this country is allowed to run
        self.next_process_ts = now_ts + self.process_interval
        return True

    def close(self):
        """Close the session."""
        try:
            self.session.close()
        except Exception:
            pass

# ===================== COOPERATIVE SCHEDULER =====================

class CooperativeScheduler:
    """Single-thread, cooperative round-robin scheduler."""
    
    def __init__(self):
        self.logger = logging.getLogger("CooperativeScheduler")
    
        from database import setup_unique_indexes
        setup_unique_indexes()
        
        # Check translation service (once)
        try:
            from translate import check_translation_service
            self.translation_service_available = check_translation_service()
        except Exception as e:
            self.translation_service_available = False
            self.logger.warning(f"Translation service check failed: {e}")

        # Create country states
        self.states: dict[str, CountryState] = {}
        
        # Import country processors and RSS URLs
        from config import COUNTRY_PROCESSORS, RSS_URLS
        
        for country, (process_func, corpus_args) in COUNTRY_PROCESSORS.items():
            rss_url = RSS_URLS.get(country)
            if not rss_url:
                self.logger.warning(f"No RSS URL found for {country}")
                continue
            self.states[country] = CountryState(country, process_func, corpus_args, rss_url)


    def run(self, minutes: int = None):
        """Main loop: fetches when time comes & processes other countries' queues in between."""
        from time import sleep
        
        start = monotonic()
        demo_seconds = None if minutes is None else int(minutes) * 60

        completion_threshold = 1
        while True:
            now_ts = monotonic()

            # 1) fetch where due (and haven't exceeded threshold)
            for st in self.states.values():
                if st.fetched_count < completion_threshold:
                    st.fetch_rss_if_due(now_ts)

            # 2) process 1 job with priority based on crawl delay (larger delay = higher priority)
            ready_states = [st for st in self.states.values() if st.jobs and now_ts >= st.next_process_ts]

            processed_any = False
            if ready_states:
                # Priority: 1) Larger process_interval (crawl delay), 2) Older next_process_ts, 3) Larger queue
                st = max(ready_states, key=lambda s: (s.process_interval, -s.next_process_ts, len(s.jobs)))
                processed_any = st.process_one_job_if_any(now_ts)

            # 3) Completion check: fetch must be done AND queues must be empty
            all_fetched_once = all(st.fetched_count >= completion_threshold for st in self.states.values())
            all_queues_empty = all(not st.jobs for st in self.states.values())
            if all_fetched_once and all_queues_empty:
                self.logger.info("🎉 All countries fetched once and all queues are empty.")
                break

            # 4) Calculate next due (either fetch or process) to sleep a bit
            next_fetch_due = min((st.next_rss_ts for st in self.states.values()
                                  if st.fetched_count < completion_threshold), default=now_ts + 1.0)
            next_proc_due = min((st.next_process_ts for st in self.states.values() if st.jobs),
                                default=now_ts + 1.0)
            next_due = min(next_fetch_due, next_proc_due)

            if not processed_any:
                wait = max(0.1, min(1.5, next_due - now_ts))
                sleep(wait)

            if demo_seconds is not None and (monotonic() - start) >= demo_seconds:
                self.logger.info("Demo window finished.")
                break

    def _log_progress(self, elapsed_s: int):
        """Log progress information."""
        print(f"\n  Progress after {elapsed_s} seconds")
        print("-" * 50)
        
        # Count completed countries
        completed_count = sum(1 for st in self.states.values() if st.fetched_count >= 1)
        total_countries = len(self.states)
        print(f" Completion: {completed_count}/{total_countries} countries completed (1+ RSS fetches)")
        
        # Show all countries with their status
        for country, st in sorted(self.states.items()):
            status = "" if st.fetched_count >= 1 else " "
            next_fetch = f"next: {st.next_rss_ts - monotonic():.0f}s" if st.next_rss_ts > monotonic() else "due"
            next_process = f"proc: {st.next_process_ts - monotonic():.0f}s" if st.next_process_ts > monotonic() else "ready"
            queue_size = len(st.jobs)
            print(f"  {status} {country:12} : {st.processed_count:3d} protests "
                  f"(visited: {st.visited_count:3d}, RSS: {st.fetched_count:2d}, queue: {queue_size}, {next_process})")

    def get_stats(self):
        """Get statistics for all countries."""
        return {
            "countries": {
                c: {
                    "processed_count": st.processed_count,
                    "visited_count": st.visited_count,
                    "fetched_count": st.fetched_count,
                } for c, st in self.states.items()
            }
        }

    def stop(self):
        """Stop the scheduler and close all sessions."""
        for st in self.states.values():
            st.close()
        try:
            from translate import close_all_sessions
            close_all_sessions()  # from translate module
        except Exception as e:
            self.logger.warning(f"Error closing pooled sessions: {e}")

