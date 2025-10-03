"""
Crawler module for the European Strikes News Extraction System.
Contains the cooperative scheduler and crawling logic.
"""

import logging
from time import monotonic

from cooperative_scheduler import CooperativeScheduler
from config import logger

# ===================== CRAWLING FUNCTIONS =====================

def run_single_pass_all_countries():
    scheduler = CooperativeScheduler()
    try:
        print("Loaded countries:", ", ".join(sorted(scheduler.states.keys())))

        final_stats = scheduler.get_stats()
        print("\n FINAL STATISTICS")
        print("="*50)
        total_processed = 0
        total_visited = 0
        for country, data in sorted(final_stats["countries"].items()):
            print(f"{country:12} : {data['processed_count']:3d} protests "
                  f"(visited: {data['visited_count']:3d}, RSS: {data['fetched_count']:2d})")
            total_processed += data["processed_count"]
            total_visited += data["visited_count"]

        print(f"\nTotal protests found: {total_processed}")
        print(f"Total articles visited: {total_visited}")
        print("="*80)
        return total_processed, total_visited

    finally:
        print(" Stopping cooperative scheduler...")
        scheduler.stop()
