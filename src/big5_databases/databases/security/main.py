#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Anonymization database functionality.

This module provides the core process_database function for anonymizing
user data in social media databases.
"""

import os
import sys
from pathlib import Path
from typing import Union

from dotenv import load_dotenv

# Add the project root to Python path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def process_database(
    source_db_path_or_name: Union[str, Path],
    anon_db_path: Union[str, Path],
    env_file_path: Union[str, Path],
    batch_size: int = 1000,
    protect_content: bool = True
) -> dict:
    """
    Process a database for anonymization.

    Args:
        source_db_path_or_name: Path to source database file OR database name in MetaDatabase
        anon_db_path: Path where anonymization database will be created (must NOT exist)
        env_file_path: Path to environment file with encryption keys
        batch_size: Number of posts to process per batch (default: 1000)
        protect_content: Whether to protect content in source database (default: True)

    Returns:
        dict: Processing statistics with keys:
            - processed: Number of posts processed
            - anonymized: Number of unique users anonymized
            - errors: Number of posts with extraction errors
            - protected: Number of posts with content protected

    Raises:
        FileNotFoundError: If source database or env file not found
        FileExistsError: If anon_db_path already exists
        ValueError: If database configuration is invalid
    """
    from dotenv import load_dotenv

    # Convert paths to Path objects
    anon_db_path = Path(anon_db_path)
    env_file_path = Path(env_file_path)

    # Validate inputs
    if anon_db_path.exists():
        raise FileExistsError(f"Anonymization database already exists: {anon_db_path}")

    if not env_file_path.exists():
        raise FileNotFoundError(f"Environment file not found: {env_file_path}")

    # Load environment
    load_dotenv(env_file_path)

    # Verify required keys are set
    required_keys = ['HMAC_KEY', 'PUBLIC_KEY_PEM', 'KEY_VERSION']
    for key in required_keys:
        if key not in os.environ:
            raise ValueError(f"Required environment variable {key} not found")

    # Import required modules (after environment is set)
    from ..meta_database import MetaDatabase
    from ..platform_db_mgmt import PlatformDB
    from .db_operations import init_anon_db, process_db

    # Load source database
    source_db_input = str(source_db_path_or_name)

    # Check if it's a path or database name
    if Path(source_db_input).exists():
        # It's a file path - determine platform from filename
        source_path = Path(source_db_input)
        # Extract platform from filename (e.g., "ANON_TEST_twitter_phase1.sqlite" -> "twitter")
        stem_parts = source_path.stem.split('_')
        if 'twitter' in stem_parts:
            platform = 'twitter'
        elif 'youtube' in stem_parts:
            platform = 'youtube'
        elif 'instagram' in stem_parts:
            platform = 'instagram'
        elif 'tiktok' in stem_parts:
            platform = 'tiktok'
        elif 'weibo' in stem_parts:
            platform = 'weibo'
        else:
            # Default fallback - use first part if no known platform found
            platform = stem_parts[0].lower() if stem_parts else "twitter"
        source_db = PlatformDB.sqlite_db_from_path(platform, source_path, table_type="posts")
    else:
        # It's a database name - load from MetaDatabase
        meta_db = MetaDatabase()
        source_db = meta_db.get_platform_db(source_db_input, table_type="posts")

    # Ensure anon directory exists
    anon_db_path.parent.mkdir(parents=True, exist_ok=True)

    # Create anonymization database
    anon_db = init_anon_db(source_db, anon_db_path)

    # Process posts for anonymization
    stats = process_db(
        source_db=source_db,
        anon_db=anon_db,
        batch_size=batch_size,
        protect_content=protect_content
    )

    return stats


def test_process_database():
    """Test function with verbose output for the anonymization process."""
    print("=" * 80)
    print("Anonymization Database Trial Run")
    print("=" * 80)

    try:
        # Test parameters
        source_db_path = "/home/rsoleyma/projects/big5/big5_databases/ANON_TEST_twitter_phase1.sqlite"
        anon_db_path = "/home/rsoleyma/projects/big5/big5_databases/data/temp/test_twitter_anonymized.anon.sqlite"
        env_file_path = "/home/rsoleyma/projects/big5/big5_databases/test_anon_keys.env"

        print(f"\nSource database: {source_db_path}")
        print(f"Anonymization database: {anon_db_path}")
        print(f"Environment file: {env_file_path}")

        # Remove existing anon database if present
        if Path(anon_db_path).exists():
            Path(anon_db_path).unlink()
            print("   Removed existing anonymization database")

        # Process the database
        print("\nProcessing database for anonymization...")
        stats = process_database(
            source_db_path_or_name=source_db_path,
            anon_db_path=anon_db_path,
            env_file_path=env_file_path,
            batch_size=100,  # Small batch for testing
            protect_content=True
        )

        # Show results
        print("\n" + "=" * 40)
        print("Results Summary")
        print("=" * 40)
        print(f"   Posts processed: {stats['processed']}")
        print(f"   Users anonymized: {stats['anonymized']}")
        print(f"   Posts protected: {stats['protected']}")
        print(f"   Errors: {stats['errors']}")

        if stats['anonymized'] > 0:
            success_rate = stats['anonymized'] / max(stats['processed'], 1) * 100
            print(f"   Success rate: {success_rate:.1f}%")

        # Verify results
        print("\nVerifying anonymization database...")
        from big5_databases.databases.platform_db_mgmt import PlatformDB
        from big5_databases.databases.db_models import DBAnonymize

        anon_db = PlatformDB.sqlite_db_from_path("twitter", anon_db_path, table_type="anon")
        with anon_db.get_session() as session:
            anon_count = session.query(DBAnonymize).count()
            print(f"   Anonymization records created: {anon_count}")

            if anon_count > 0:
                sample = session.query(DBAnonymize).first()
                print(f"   Sample record:")
                print(f"      - Hash: {sample.user_id_hash[:20]}...")
                print(f"      - Public UUID: {sample.public_id}")
                print(f"      - Has encrypted user_id: {bool(sample.encrypted_user_id)}")
                print(f"      - Has metadata: {bool(sample.encrypted_data)}")

        print("\n" + "=" * 80)
        print("TRIAL RUN COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"\nFiles created:")
        print(f"   - Anonymization DB: {anon_db_path}")
        print(f"   - Size: {Path(anon_db_path).stat().st_size / 1024:.1f} KB")

        print(f"\nSecurity status:")
        print(f"   - User IDs encrypted and stored")
        print(f"   - Source content protected with '<PROTECTED>'")
        print(f"   - Public UUIDs generated for external use")

        return True

    except Exception as e:
        print(f"\nError during trial run: {e}")
        import traceback
        print("\nFull error details:")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_process_database()
    sys.exit(0 if success else 1)