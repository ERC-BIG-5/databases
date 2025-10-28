"""
Advanced audit test that attempts to decrypt anonymized data.

This test looks for the anonymization mapping database and attempts to
decrypt anonymized data using the private key to compare with original data.
"""

import sqlite3
import json
import random
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Load the private key environment
load_dotenv('test_private_decr.env')

def find_anonymization_database(anon_db_path: Path) -> Optional[Path]:
    """
    Try to find the corresponding anonymization mapping database.

    Args:
        anon_db_path: Path to the anonymized database

    Returns:
        Path to the anonymization mapping database or None
    """
    # Try different naming patterns
    base_name = anon_db_path.stem
    parent_dir = anon_db_path.parent

    # Pattern 1: same name with .anon.sqlite extension
    pattern1 = parent_dir / f"{base_name}.anon.sqlite"
    if pattern1.exists():
        return pattern1

    # Pattern 2: remove ANON_TEST prefix and add .anon.sqlite
    if base_name.startswith('ANON_TEST_'):
        base_without_prefix = base_name.replace('ANON_TEST_', '')
        pattern2 = parent_dir / f"{base_without_prefix}.anon.sqlite"
        if pattern2.exists():
            return pattern2

    # Pattern 3: look in subdirectories
    for subdir in ['data', 'data/temp']:
        subdir_path = parent_dir / subdir
        if subdir_path.exists():
            pattern3 = subdir_path / f"{base_name}.anon.sqlite"
            if pattern3.exists():
                return pattern3

    return None

def load_decryption_modules():
    """Load the security modules for decryption."""
    try:
        from src.big5_databases.databases.security.secure_config import SecurityConfig
        from src.big5_databases.databases.security.decryption_manager import DecryptionManager
        from src.big5_databases.databases.security.db_operations import DatabaseOperations
        return SecurityConfig, DecryptionManager, DatabaseOperations
    except ImportError as e:
        print(f"❌ Failed to import security modules: {e}")
        return None, None, None

def get_sample_posts_with_protected_content(db_path: str, sample_size: int = 20) -> List[Dict[str, Any]]:
    """Get sample posts that have <PROTECTED> content."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Look for posts with <PROTECTED> in content
        cursor.execute("""
            SELECT id, platform_id, content, metadata_content, date_created, post_url
            FROM post
            WHERE content LIKE '%<PROTECTED>%'
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

def find_original_posts_by_timestamp(original_db_path: str, anon_posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Find original posts by matching timestamps."""
    with sqlite3.connect(original_db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        matched_posts = []
        for anon_post in anon_posts:
            cursor.execute("""
                SELECT id, platform_id, content, metadata_content, date_created, post_url
                FROM post
                WHERE date_created = ?
            """, (anon_post['date_created'],))

            rows = cursor.fetchall()
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

                matched_posts.append({
                    'anon_post': anon_post,
                    'original_post': orig_data
                })
                break  # Take first timestamp match

        return matched_posts

def extract_protected_fields(data, path=""):
    """Recursively extract fields that contain <PROTECTED>."""
    protected_fields = []

    if isinstance(data, dict):
        for key, value in data.items():
            current_path = f"{path}.{key}" if path else key
            if isinstance(value, str) and value == "<PROTECTED>":
                protected_fields.append(current_path)
            elif isinstance(value, (dict, list)):
                protected_fields.extend(extract_protected_fields(value, current_path))
    elif isinstance(data, list):
        for i, value in enumerate(data):
            current_path = f"{path}[{i}]"
            if isinstance(value, str) and value == "<PROTECTED>":
                protected_fields.append(current_path)
            elif isinstance(value, (dict, list)):
                protected_fields.extend(extract_protected_fields(value, current_path))

    return protected_fields

def analyze_anonymization_patterns(matched_posts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze what fields were anonymized."""
    analysis = {
        'total_posts': len(matched_posts),
        'protected_field_frequency': {},
        'examples': []
    }

    for match in matched_posts:
        anon_post = match['anon_post']
        orig_post = match['original_post']

        # Find protected fields in anonymized content
        protected_content_fields = extract_protected_fields(anon_post.get('content', {}))
        protected_metadata_fields = extract_protected_fields(anon_post.get('metadata_content', {}))

        all_protected = protected_content_fields + protected_metadata_fields

        # Count frequency
        for field in all_protected:
            analysis['protected_field_frequency'][field] = (
                analysis['protected_field_frequency'].get(field, 0) + 1
            )

        # Add example
        if len(analysis['examples']) < 3 and all_protected:
            example = {
                'post_id': anon_post['id'],
                'timestamp': anon_post['date_created'],
                'protected_fields': all_protected,
                'sample_original_values': {}
            }

            # Get original values for protected fields
            for field in all_protected[:3]:  # Limit to first 3 for brevity
                try:
                    # Navigate to the field in original data
                    field_path = field.split('.')
                    value = orig_post.get('content', {}) if not field.startswith('metadata') else orig_post.get('metadata_content', {})

                    for part in field_path:
                        if part.startswith('metadata_content'):
                            continue
                        elif '[' in part and ']' in part:
                            # Handle array indices
                            key = part.split('[')[0]
                            idx = int(part.split('[')[1].split(']')[0])
                            value = value[key][idx]
                        else:
                            value = value[part]

                    example['sample_original_values'][field] = str(value)[:100]  # Truncate for display
                except (KeyError, IndexError, TypeError):
                    example['sample_original_values'][field] = "Could not extract"

            analysis['examples'].append(example)

    return analysis

def main():
    """Main audit function."""
    print("🔍 Starting advanced audit with decryption analysis...")

    # Paths
    anon_db_path = Path("ANON_TEST_twitter_phase1.sqlite")
    orig_db_path = Path("twitter_phase1.sqlite")

    if not anon_db_path.exists():
        print(f"❌ Anonymized database not found: {anon_db_path}")
        return

    if not orig_db_path.exists():
        print(f"❌ Original database not found: {orig_db_path}")
        return

    print(f"📋 Looking for anonymization mapping database...")
    anon_mapping_db = find_anonymization_database(anon_db_path)
    if anon_mapping_db:
        print(f"✅ Found anonymization mapping: {anon_mapping_db}")
    else:
        print("⚠️  No anonymization mapping database found")
        print("   Audit will analyze patterns without decryption")

    print(f"📊 Sampling posts with <PROTECTED> content from {anon_db_path}...")
    anon_posts = get_sample_posts_with_protected_content(anon_db_path, 20)
    print(f"✅ Found {len(anon_posts)} posts with protected content")

    if not anon_posts:
        print("⚠️  No posts with <PROTECTED> content found")
        print("   This database may not be properly anonymized")
        return

    print(f"🔍 Finding matching posts in {orig_db_path}...")
    matched_posts = find_original_posts_by_timestamp(orig_db_path, anon_posts)
    print(f"✅ Matched {len(matched_posts)} posts")

    print("🔄 Analyzing anonymization patterns...")
    analysis = analyze_anonymization_patterns(matched_posts)

    print("\n📋 ANONYMIZATION ANALYSIS RESULTS:")
    print(f"Total posts analyzed: {analysis['total_posts']}")
    print(f"Protected field types found: {len(analysis['protected_field_frequency'])}")

    print("\n🔐 Most frequently protected fields:")
    sorted_fields = sorted(
        analysis['protected_field_frequency'].items(),
        key=lambda x: x[1],
        reverse=True
    )
    for field, count in sorted_fields[:10]:
        percentage = (count / analysis['total_posts']) * 100
        print(f"  {field}: {count}/{analysis['total_posts']} posts ({percentage:.1f}%)")

    print("\n📝 Sample anonymization examples:")
    for i, example in enumerate(analysis['examples'], 1):
        print(f"\nExample {i} (Post ID: {example['post_id']}):")
        print(f"  Timestamp: {example['timestamp']}")
        print(f"  Protected fields: {len(example['protected_fields'])}")
        for field, orig_value in example['sample_original_values'].items():
            print(f"    {field}: {orig_value}")

    # Save results
    results_file = 'advanced_audit_results.json'
    with open(results_file, 'w') as f:
        json.dump(analysis, f, indent=2)
    print(f"\n💾 Detailed results saved to {results_file}")

    if anon_mapping_db:
        print(f"\n🔑 Next step: Implement decryption using mapping database at {anon_mapping_db}")
        print("   This would require loading the private key and decrypting the protected values")
    else:
        print(f"\n⚠️  To perform full decryption audit, need to:")
        print("   1. Find or create the anonymization mapping database")
        print("   2. Load the private key from test_private_decr.env")
        print("   3. Decrypt the protected values using the security modules")

if __name__ == "__main__":
    main()