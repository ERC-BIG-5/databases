"""
Simple test for audit functionality to verify anonymization process.

This test samples random posts from the anonymized database and attempts
to verify them against the original database using the private key.
"""

import sqlite3
import json
import random
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load the private key environment
load_dotenv('test_private_decr.env')

def sample_random_posts(db_path: str, sample_size: int = 20) -> List[Dict[str, Any]]:
    """Sample random posts from the database."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get total count
        cursor.execute("SELECT COUNT(*) as total FROM post")
        total = cursor.fetchone()['total']

        # Sample random posts
        cursor.execute("""
            SELECT id, platform_id, content, metadata_content, date_created, post_url
            FROM post
            ORDER BY RANDOM()
            LIMIT ?
        """, (sample_size,))

        posts = []
        for row in cursor.fetchall():
            post_data = dict(row)
            # Parse JSON fields
            if post_data['content']:
                try:
                    post_data['content'] = json.loads(post_data['content'])
                except json.JSONDecodeError:
                    pass
            if post_data['metadata_content']:
                try:
                    post_data['metadata_content'] = json.loads(post_data['metadata_content'])
                except json.JSONDecodeError:
                    pass
            posts.append(post_data)

    return posts

def find_matching_posts_in_original(original_db_path: str, anon_posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Find matching posts in the original database by timestamp and content patterns."""
    with sqlite3.connect(original_db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        original_posts = []
        for anon_post in anon_posts:
            # Try to match by date_created and other non-user-specific fields
            cursor.execute("""
                SELECT id, platform_id, content, metadata_content, date_created, post_url
                FROM post
                WHERE date_created = ?
            """, (anon_post['date_created'],))

            rows = cursor.fetchall()
            if rows:
                # If multiple posts with same timestamp, try to match by content length or other factors
                for row in rows:
                    orig_data = dict(row)
                    if orig_data['content']:
                        try:
                            orig_data['content'] = json.loads(orig_data['content'])
                        except json.JSONDecodeError:
                            pass
                    if orig_data['metadata_content']:
                        try:
                            orig_data['metadata_content'] = json.loads(orig_data['metadata_content'])
                        except json.JSONDecodeError:
                            pass

                    original_posts.append({
                        'anon_post': anon_post,
                        'original_post': orig_data
                    })
                    break  # Take first match for now

    return original_posts

def compare_posts(matched_posts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compare anonymized posts with original posts."""
    results = {
        'total_compared': len(matched_posts),
        'platform_id_differences': 0,
        'content_differences': 0,
        'metadata_differences': 0,
        'url_differences': 0,
        'details': []
    }

    for match in matched_posts:
        anon = match['anon_post']
        orig = match['original_post']

        comparison = {
            'anon_id': anon['id'],
            'orig_id': orig['id'],
            'timestamp': anon['date_created']
        }

        # Compare platform_id (should be anonymized)
        if anon['platform_id'] != orig['platform_id']:
            results['platform_id_differences'] += 1
            comparison['platform_id_diff'] = {
                'anon': anon['platform_id'],
                'orig': orig['platform_id']
            }

        # Compare content (might be anonymized)
        anon_content_str = json.dumps(anon['content'], sort_keys=True) if anon['content'] else ""
        orig_content_str = json.dumps(orig['content'], sort_keys=True) if orig['content'] else ""
        if anon_content_str != orig_content_str:
            results['content_differences'] += 1
            comparison['content_diff'] = True

        # Compare metadata
        anon_meta_str = json.dumps(anon['metadata_content'], sort_keys=True) if anon['metadata_content'] else ""
        orig_meta_str = json.dumps(orig['metadata_content'], sort_keys=True) if orig['metadata_content'] else ""
        if anon_meta_str != orig_meta_str:
            results['metadata_differences'] += 1
            comparison['metadata_diff'] = True

        # Compare URLs
        if anon['post_url'] != orig['post_url']:
            results['url_differences'] += 1
            comparison['url_diff'] = {
                'anon': anon['post_url'],
                'orig': orig['post_url']
            }

        results['details'].append(comparison)

    return results

def main():
    """Main test function."""
    print("🔍 Starting simple audit test...")

    # Paths
    anon_db = "ANON_TEST_twitter_phase1.sqlite"
    orig_db = "twitter_phase1.sqlite"

    if not Path(anon_db).exists():
        print(f"❌ Anonymized database not found: {anon_db}")
        return

    if not Path(orig_db).exists():
        print(f"❌ Original database not found: {orig_db}")
        return

    print(f"📊 Sampling 20 random posts from {anon_db}...")
    anon_posts = sample_random_posts(anon_db, 20)
    print(f"✅ Sampled {len(anon_posts)} posts")

    print(f"🔍 Finding matching posts in {orig_db}...")
    matched_posts = find_matching_posts_in_original(orig_db, anon_posts)
    print(f"✅ Found {len(matched_posts)} matching posts")

    print("🔄 Comparing posts...")
    results = compare_posts(matched_posts)

    print("\n📋 AUDIT RESULTS:")
    print(f"Total posts compared: {results['total_compared']}")
    print(f"Platform ID differences: {results['platform_id_differences']}")
    print(f"Content differences: {results['content_differences']}")
    print(f"Metadata differences: {results['metadata_differences']}")
    print(f"URL differences: {results['url_differences']}")

    if results['platform_id_differences'] > 0:
        print("\n🔐 Sample platform ID differences (anonymized vs original):")
        count = 0
        for detail in results['details']:
            if 'platform_id_diff' in detail and count < 3:
                print(f"  {detail['platform_id_diff']['anon']} → {detail['platform_id_diff']['orig']}")
                count += 1

    # Save detailed results
    with open('audit_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n💾 Detailed results saved to audit_results.json")

if __name__ == "__main__":
    main()