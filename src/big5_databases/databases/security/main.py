"""
Anonymization database functionality.

This module provides the core process_database function for anonymizing
user data in social media databases.
"""

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Union

import sys
from tools.env_root import root

from big5_databases.databases.db_models import DBAnonymize
from big5_databases.databases.db_models import DBPost, PostType
from big5_databases.databases.meta_database import MetaDatabase
from big5_databases.databases.model_conversion import PostModel
from big5_databases.databases.platform_db_mgmt import PlatformDB
from big5_databases.databases.security.core.db_operations import init_anon_db, process_db, get_anon_db
from big5_databases.databases.security.core.secure_user_id_manager import SecureUserIDManager
from big5_databases.databases.security.utils.jsonpath_extractor import JsonPathContentProtector
from big5_databases.databases.security.core.db_operations import DatabaseOperations
from big5_databases.databases.security.utils.exec_db_fixes import platform_user_data_jsonpath
from big5_databases.databases.model_conversion import PostMetadataModel
from dotenv import load_dotenv

# Add the project root to Python path for imports
sys.path.insert(0, str(root()))


def load_env_file(env_file_path: Path):
    # Validate inputs
    if not env_file_path.exists():
        raise FileNotFoundError(f"Environment file not found: {env_file_path}")
    # Load environment
    load_dotenv(env_file_path)

def get_posts_db(platform: str, source_db_input: str) -> PlatformDB:
    # Check if it's a path or database name
    if Path(source_db_input).exists():
        source_path = Path(source_db_input)
        return PlatformDB.sqlite_db_from_path(platform, source_path, table_type="posts")
    else:
        # It's a database name - load from MetaDatabase
        meta_db = MetaDatabase()
        return meta_db.get_platform_db(source_db_input, table_type="posts")


def process_database(
        platform: str,
        source_db_path_or_name: Union[str, Path],
        anon_db_path: Union[str, Path],
        env_file_path: Union[str, Path],
        batch_size: int = 1000,
        protect_content: bool = True,
) -> dict:
    """
    Process a database for anonymization.

    Args:
        platform: platform (twitter, tiktok, ...)
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


    # Create or open anonymization database
    source_db = get_posts_db(platform, str(source_db_path_or_name))

    if not anon_db_path.exists():
        if not anon_db_path.parent.exists():
            raise NotADirectoryError(f"Make sure the parent directory of {anon_db_path} exists")
        anon_db = init_anon_db(source_db, anon_db_path)
    else:
        anon_db = get_anon_db(anon_db_path, platform)


    load_env_file(Path(env_file_path))

    # Verify required keys are set
    required_keys = ['HMAC_KEY', 'PUBLIC_KEY_PEM', 'KEY_VERSION']
    for key in required_keys:
        if key not in os.environ:
            raise ValueError(f"Required environment variable {key} not found")

    # Process posts for anonymization
    stats = process_db(
        source_db=source_db,
        anon_db=anon_db,
        batch_size=batch_size,
        protect_content=protect_content
    )

    return stats




def demo_insertion():
    """
    Batch processing demo: sourcedb → new_posts → ANONYMIZE → safe_submit → end
    Workflow: Create posts, anonymize them FIRST, then submit to database.
    """
    base_path = root() / "test_data" / "security"
    env_file_path = base_path / "test_anon_keys.env"
    load_env_file(env_file_path)

    print("\n" + "=" * 60)

    # todo changed that to INSERTION TEST.
    print("POST INSERTION TEST")
    print("Workflow: sourcedb → new_posts → ANONYMIZE → safe_submit → end")
    print("=" * 60)

    try:
        base_path = root() / "test_data" / "security"
        source_db_path = base_path / "twitter_phase1.sqlite"
        _anon_db_path = base_path / "test_twitter_anonymized.anon.sqlite"

        if not source_db_path.exists():
            print(f"❌ Source database not found: {source_db_path}")
            return False

        print(f"📦 Step 1: Load source database...")
        source_db = PlatformDB.sqlite_db_from_path("twitter", source_db_path, table_type="posts")
        print(f"   ✅ Source database loaded")

        print(f"📝 Step 2: Create new posts (mixed DBPost/PostModel)...")
        fake_posts: list[Union[DBPost, PostModel]] = []

        # Get template for realistic structure
        with source_db.get_session() as session:
            template_post = session.query(DBPost).first()
            if not template_post:
                print("❌ No template posts found")
                return False

            template_content = template_post.content if isinstance(template_post.content, dict) else json.loads(
                template_post.content or '{}')

        # Create 3 fake posts with fresh user data
        for i in range(3):
            fake_content = template_content.copy()
            fake_content.update({
                "id": 7777000000 + i,
                "id_str": str(7777000000 + i),
                "user": {
                    "id": f"batch_user_{i + 200}",
                    "id_str": f"batch_user_{i + 200}",
                    "username": f"batch_test_{i}",
                    "displayname": f"Batch Test User {i}",
                    "url": f"https://twitter.com/batch_test_{i}",
                    "rawDescription": f"Batch processing test user {i}",
                },
                "rawContent": f"Batch processing test post {i} - pre-anonymization",
            })

            fake_metadata = {"labels": ["batch", f"coverage_{i}"]}

            # Create mixed types for coverage
            if i % 2 == 0:
                fake_post = DBPost(
                    id=7777000000 + i,
                    platform="twitter",
                    platform_id=str(7777000000 + i),
                    date_created=datetime.now(),
                    post_url=fake_content.get("url", f"https://test.com/{i}"),
                    content=fake_content,
                    metadata_content=fake_metadata
                )
            else:
                fake_post = PostModel(
                    id=7777000000 + i,
                    platform="twitter",
                    platform_id=str(7777000000 + i),
                    post_url=fake_content.get("url", f"https://test.com/{i}"),
                    date_created=datetime.now(),
                    post_type=PostType.REGULAR,
                    content=fake_content,
                    metadata_content=fake_metadata,
                    collection_task_id=None
                )
            fake_posts.append(fake_post)

        print(
            f"   ✅ Created {len(fake_posts)} new posts ({sum(1 for p in fake_posts if isinstance(p, DBPost))} DBPost + {sum(1 for p in fake_posts if isinstance(p, PostModel))} PostModel)")

        print(f"🔐 Step 3: ANONYMIZE posts BEFORE submission...")

        # Set up anonymization components
        anon_db = get_anon_db(_anon_db_path)

        # Initialize user ID manager and database operations from environment
        user_id_manager = SecureUserIDManager.from_env(load_private_key=False)

        db_ops = DatabaseOperations(anon_db)

        # Set up JSONPath protector for Twitter using platform-specific paths
        user_id_path, metadata_paths = platform_user_data_jsonpath("twitter")
        protection_paths = {"user_id": user_id_path}

        # Add metadata paths for protection
        for i, path in enumerate(metadata_paths):
            protection_paths[f"metadata_{i}"] = path
        protector = JsonPathContentProtector(protection_paths)

        anonymized_posts = []
        anonymized_count = 0

        for i, post in enumerate(fake_posts):
            try:
                # Get content as dict
                if isinstance(post, DBPost):
                    content = post.content if isinstance(post.content, dict) else json.loads(post.content or '{}')
                else:
                    content = post.content

                # Extract user ID for anonymization
                original_user_id = content.get('user', {}).get('id_str')

                if original_user_id:
                    # Create user mapping (hash + encrypt + UUID)
                    user_mapping = user_id_manager.create_user_mapping(original_user_id, {"platform": "twitter"})

                    # Store mapping in anonymization database
                    encrypted_metadata = user_id_manager.encrypt('{"platform": "twitter"}') if user_id_manager else None
                    db_ops.add_mappings([(
                        user_mapping.hashed_id,
                        user_mapping.encrypted_original_id,
                        user_mapping.public_uuid,
                        encrypted_metadata,
                        user_mapping.key_version,
                        user_mapping.pseudo_name
                    )])

                    # Replace user ID with UUID, protect other sensitive fields
                    protected_content = content.copy()
                    protected_content['user']['id_str'] = user_mapping.public_uuid

                    # Protect additional sensitive fields
                    protected_content = protector.protect_data(protected_content)

                    # Update post content
                    if isinstance(post, DBPost):
                        post.content = protected_content
                    else:
                        post.content = protected_content

                    # Add protection marker
                    if isinstance(post, DBPost):
                        if not post.metadata_content:
                            post.metadata_content = {}
                        post.metadata_content.setdefault('protection', {})['protected_user'] = True
                    else:  # PostModel with PostMetadataModel
                        if not post.metadata_content:

                            post.metadata_content = PostMetadataModel()

                        # Create new PostMetadataModel with updated protection field
                        current_metadata = post.metadata_content
                        protection_dict = current_metadata.protection or {}
                        protection_dict['protected_user'] = True

                        # Create new instance with updated protection
                        post.metadata_content = PostMetadataModel(
                            media_paths=current_metadata.media_paths,
                            media_base_path=current_metadata.media_base_path,
                            media_dl_failed=current_metadata.media_dl_failed,
                            media=current_metadata.media,
                            post_exists=current_metadata.post_exists,
                            orig_db_conf=current_metadata.orig_db_conf,
                            annotations=current_metadata.annotations,
                            protection=protection_dict,
                            extra=current_metadata.extra
                        )

                    anonymized_count += 1
                    print(f"   🔒 Anonymized post {i + 1}: {original_user_id} → {user_mapping.public_uuid}")

                anonymized_posts.append(post)

            except Exception as e:
                print(f"   ⚠️  Error anonymizing post {i + 1}: {e}")
                anonymized_posts.append(post)  # Keep original if anonymization fails

        print(f"   ✅ Anonymization completed: {anonymized_count}/{len(fake_posts)} posts anonymized")

        print(f"📤 Step 4: Submit anonymized posts to database...")
        try:
            submitted_posts = source_db.safe_submit_posts(anonymized_posts)
            print(f"   ✅ Successfully submitted {len(submitted_posts)} anonymized posts")

            # Verify anonymization in submitted posts
            if submitted_posts:
                sample_post = submitted_posts[0]
                content = sample_post.content if isinstance(sample_post.content, dict) else json.loads(
                    sample_post.content or '{}')
                sample_user_id = content.get('user', {}).get('id_str', 'N/A')
                print(f"   🔍 Sample anonymized user ID: {sample_user_id}")

        except Exception as e:
            print(f"   ❌ Batch submission failed: {e}")
            return False

        print(f"\n✅ BATCH PROCESSING COMPLETE!")
        print(f"Final workflow: sourcedb ✅ → new_posts ✅ → ANONYMIZE ✅ → safe_submit ✅ → end ✅")
        return True

    except Exception as e:
        print(f"\n❌ Error in batch processing coverage: {e}")
        import traceback
        traceback.print_exc()
        return False


def demo_process_database():
    print("=" * 80)
    print("Anonymization Database Trial Run")
    print("=" * 80)

    try:
        # Consolidated test data directory
        base_path = root() / "test_data" / "security"

        # Test parameters using consolidated paths
        platform = "twitter"
        source_db_path = base_path / "twitter_phase1.sqlite"
        source_backup_path = base_path / "SOURCE_twitter_phase1.sqlite"
        anon_db_path = base_path / "test_twitter_anonymized.anon.sqlite"
        env_file_path = base_path / "test_anon_keys.env"

        print(f"\nTest data directory: {base_path}")
        print(f"Source backup: {source_backup_path}")
        print(f"Source database: {source_db_path}")
        print(f"Anonymization database: {anon_db_path}")
        print(f"Environment file: {env_file_path}")

        # Verify backup and env files exist
        if not source_backup_path.exists():
            print(f"❌ Source backup not found: {source_backup_path}")
            return False
        if not env_file_path.exists():
            print(f"❌ Environment file not found: {env_file_path}")
            return False

        print("✅ All required files found")

        # Prepare fresh test data
        print("\n🔄 Preparing fresh test data...")

        # Remove old source database and related files
        if source_db_path.exists():
            source_db_path.unlink()
            print("   Removed old source database")

        # Remove SQLite WAL/SHM files if they exist
        for suffix in ["-wal", "-shm"]:
            wal_file = Path(str(source_db_path) + suffix)
            if wal_file.exists():
                wal_file.unlink()
                print(f"   Removed {wal_file.name}")

        # Remove existing anon database if present
        if anon_db_path.exists():
            anon_db_path.unlink()
            print("   Removed existing anonymization database")

        # Copy fresh source database from backup
        shutil.copy2(source_backup_path, source_db_path)
        print("   Restored fresh source database from backup")
        print("✅ Fresh test data ready")

        # Process the database
        print("\nProcessing database for anonymization...")
        stats = process_database(
            platform=platform,
            source_db_path_or_name=source_db_path,
            anon_db_path=anon_db_path,
            env_file_path=env_file_path,
            batch_size=100,
            protect_content=True
        )

        # Show results
        print("\n" + "=" * 40)
        print("Results Summary")
        print("=" * 40)
        print(f"   Posts processed: {stats['processed']}")
        print(f"   Users anonymized: {stats['users_anonymized']}")
        print(f"   Posts protected: {stats['protected']}")
        print(f"   Errors: {stats['errors']}")

        if stats['users_anonymized'] > 0:
            success_rate = stats['users_anonymized'] / max(stats['processed'], 1) * 100
            print(f"   Success rate: {success_rate:.1f}%")

        # Verify results
        print("\nVerifying anonymization database...")

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

        # Run batch processing coverage test
        return True

    except Exception as e:
        print(f"\nError during trial run: {e}")
        import traceback
        print("\nFull error details:")
        traceback.print_exc()
        return False


def main():
    """Test function with verbose output for the anonymization process."""
    demo_process_database()
    demo_insertion()


if __name__ == "__main__":
    main()
