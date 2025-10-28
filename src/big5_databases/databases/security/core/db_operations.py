"""
Database Operations for Two-Layer Anonymization System

Separated from SecureUserIDManager to follow single responsibility principle.
Handles all database interactions for user ID mappings.
"""

import json
from pathlib import Path
from typing import Optional, Dict, Any, List

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.attributes import flag_modified

from ...db_models import DBAnonymize, DBPost
from ...platform_db_mgmt import PlatformDB

from ..utils.jsonpath_extractor import JsonPathFieldExtractor, JsonPathContentProtector
from ..utils.exec_db_fixes import platform_user_data_jsonpath
from .secure_user_id_manager import SecureUserIDManager

from tools.project_logging import get_logger


class DatabaseOperations:
    """
    Handles database operations for anonymization mappings.

    Separated from crypto operations for:
    - Better separation of concerns
    - Easier testing
    - Clearer code organization
    """

    def __init__(self, db_manager):
        """
        Initialize with database manager.

        Args:
            db_manager: Your DatabaseManager instance
        """
        self.db = db_manager
        self.DBAnonymize = DBAnonymize
    
    def add_mappings(
        self,
        mappings: list[tuple[str, str, str, Optional[str], str]]
    ) -> dict[str, str]:
        """
        Add user mappings to database with transaction safety.
        
        Args:
            mappings: List of (hashed_id, encrypted_user_id, public_uuid, 
                               encrypted_data, key_version) tuples
        
        Returns:
            Dict mapping hashed_id → public_uuid
            
        Raises:
            ValueError: If transaction fails after retries
        """
        if not mappings:
            return {}
        
        # Extract hashes for lookup
        hashes = [m[0] for m in mappings]
        hash_to_mapping = {m[0]: m for m in mappings}
        hash2public_id = {}
        
        with self.db.get_session() as session:
            try:
                # Query existing mappings with row locks
                # Note: with_for_update() may not work with all DB backends
                try:
                    existing = session.query(
                        self.DBAnonymize.user_id_hash,
                        self.DBAnonymize.public_id
                    ).filter(
                        self.DBAnonymize.user_id_hash.in_(hashes)
                    ).with_for_update().all()
                except (AttributeError, TypeError):
                    # Fallback for mocks or simple DBs
                    existing = session.query(
                        self.DBAnonymize
                    ).all()
                    existing = [(e.user_id_hash, e.public_id) for e in existing 
                               if e.user_id_hash in hashes]
                
                # Map existing entries
                for user_id_hash, public_id in existing:
                    hash2public_id[user_id_hash] = public_id
                
                # Determine which need insertion
                existing_hashes = set(hash2public_id.keys())
                hashes_to_add = set(hashes) - existing_hashes
                
                # Insert missing entries
                for hash_ in hashes_to_add:
                    (hashed_id, encrypted_user_id, public_uuid,
                     encrypted_data, key_version) = hash_to_mapping[hash_]

                    db_entry = self.DBAnonymize(
                        user_id_hash=hashed_id,
                        encrypted_user_id=encrypted_user_id,
                        public_id=public_uuid,
                        encrypted_data=encrypted_data,
                        key_version=key_version
                    )
                    
                    session.add(db_entry)
                    hash2public_id[hash_] = public_uuid
                
                # Commit transaction
                session.commit()
                
            except IntegrityError:
                # Handle race condition: another process inserted concurrently
                session.rollback()
                
                # Re-query to get values inserted by other process
                try:
                    existing = session.query(
                        self.DBAnonymize.user_id_hash,
                        self.DBAnonymize.public_id
                    ).filter(
                        self.DBAnonymize.user_id_hash.in_(hashes)
                    ).all()
                except (AttributeError, TypeError):
                    # Fallback for mocks
                    existing = session.query(self.DBAnonymize).all()
                    existing = [(e.user_id_hash, e.public_id) for e in existing 
                               if e.user_id_hash in hashes]
                
                for user_id_hash, public_id in existing:
                    hash2public_id[user_id_hash] = public_id
                
                # Verify all hashes now have mappings
                if len(hash2public_id) < len(hashes):
                    raise ValueError(
                        f"Transaction conflict: Unable to create all mappings. "
                        f"Expected {len(hashes)}, got {len(hash2public_id)}"
                    )
        
        return hash2public_id
    
    def get_mapping_by_uuid(self, public_uuid: str):
        """
        Retrieve a mapping by public UUID.
        
        Args:
            public_uuid: The public UUID to look up
            
        Returns:
            self.DBAnonymize entry or None if not found
        """
        with self.db.get_session() as session:
            try:
                entry = session.query(self.DBAnonymize).filter(
                    self.DBAnonymize.public_id == public_uuid
                ).first()
            except (AttributeError, TypeError):
                # Fallback for mocks
                all_entries = session.query(self.DBAnonymize).all()
                entry = next((e for e in all_entries if e.public_id == public_uuid), None)
            return entry
    
    def get_mapping_by_hash(self, hashed_id: str):
        """
        Retrieve a mapping by hashed ID.
        
        Args:
            hashed_id: The hashed user ID to look up
            
        Returns:
            self.DBAnonymize entry or None if not found
        """
        with self.db.get_session() as session:
            try:
                entry = session.query(self.DBAnonymize).filter(
                    self.DBAnonymize.user_id_hash == hashed_id
                ).first()
            except (AttributeError, TypeError):
                # Fallback for mocks
                all_entries = session.query(self.DBAnonymize).all()
                entry = next((e for e in all_entries if e.user_id_hash == hashed_id), None)
            return entry
    
    def get_mappings_by_hashes(self, hashed_ids: list[str]) -> dict[str, str]:
        """
        Batch lookup: get public UUIDs for multiple hashed IDs.
        
        Args:
            hashed_ids: List of hashed user IDs
            
        Returns:
            Dict mapping hashed_id → public_uuid
        """
        if not hashed_ids:
            return {}
        
        with self.db.get_session() as session:
            try:
                results = session.query(
                    self.DBAnonymize.user_id_hash,
                    self.DBAnonymize.public_id
                ).filter(
                    self.DBAnonymize.user_id_hash.in_(hashed_ids)
                ).all()
            except (AttributeError, TypeError):
                # Fallback for mocks
                all_entries = session.query(self.DBAnonymize).all()
                results = [(e.user_id_hash, e.public_id) for e in all_entries 
                          if e.user_id_hash in hashed_ids]
            
            return {hash_: uuid for hash_, uuid in results}
    
    def count_mappings(self, key_version: Optional[str] = None) -> int:
        """
        Count total mappings, optionally filtered by key version.
        
        Args:
            key_version: Optional key version to filter by
            
        Returns:
            Count of mappings
        """
        with self.db.get_session() as session:
            query = session.query(self.DBAnonymize)
            
            if key_version:
                query = query.filter(self.DBAnonymize.key_version == key_version)
            
            return query.count()


# Example usage
# if __name__ == "__main__":
#     print("=" * 70)
#     print("DATABASE OPERATIONS MODULE")
#     print("=" * 70)
#
#     print("\nThis module provides database operations separated from crypto logic.")
#     print("\nKey benefits:")
#     print("  ✅ Single responsibility principle")
#     print("  ✅ Easier testing")
#     print("  ✅ Clearer separation of concerns")
#     print("  ✅ Can swap database implementations easily")
#
#     print("\n" + "=" * 70)
#     print("See updated integration example for usage")
#     print("=" * 70)


# =============================================================================
# NEW ANONYMIZATION DATABASE FUNCTIONS
# =============================================================================

def init_anon_db(source_platform_db, anon_db_path):
    """
    Create anonymization database from existing platform database.

    This creates a new platform database with table_type="anon" that includes
    the anonymization table (anonymize) and database statistics table (database_stats).

    Args:
        source_platform_db: Existing posts-type platform database (PlatformDB instance)
        anon_db_path: Path where to create the new anonymization database (Path or str)

    Returns:
        PlatformDB: New anonymization database instance configured with anon tables

    Raises:
        ValueError: If source database doesn't exist or invalid platform
        OSError: If cannot create new database file

    Example:
        >>> from big5_databases.databases.platform_db_mgmt import PlatformDB
        >>> from pathlib import Path
        >>>
        >>> source_db = PlatformDB.sqlite_db_from_path("twitter", "twitter_posts.sqlite")
        >>> anon_db = init_anon_db(source_db, Path("twitter_anon.sqlite"))
        >>> print(f"Created anon database for {anon_db.platform}")
    """
    # Validate inputs
    if not hasattr(source_platform_db, 'platform'):
        raise ValueError("source_platform_db must be a PlatformDB instance")

    anon_db_path = Path(anon_db_path)

    # Create anonymization database with table_type="anon"
    anon_db = PlatformDB.sqlite_db_from_path(
        platform=source_platform_db.platform,
        path=anon_db_path,
        create=True,
        table_type="anon"
    )

    print(f"✅ Created anonymization database for {source_platform_db.platform}")
    print(f"   Path: {anon_db_path}")
    print(f"   Tables: anonymize, database_stats")

    return anon_db


def process_db(source_db, anon_db, batch_size=1000, protect_content=True):
    """
    Process posts from source database and anonymize them into anon database.

    This function:
    1. Extracts user_ids from posts using platform-specific JSONPath patterns
    2. Creates anonymization mappings (hashed_id -> public_uuid)
    3. Stores encrypted user data in anonymization database
    4. Optionally modifies source database to replace user_ids with UUIDs
    5. Protects sensitive content by replacing with "<PROTECTED>"

    Args:
        source_db: Source platform database (table_type="posts")
        anon_db: Target anonymization database (table_type="anon")
        batch_size: Number of posts to process per batch (default: 1000)
        protect_content: Whether to replace user_ids in source with "<PROTECTED>" (default: True)

    Returns:
        dict: Processing statistics with keys:
            - processed: Number of posts processed
            - anonymized: Number of unique users anonymized
            - errors: Number of posts with extraction errors
            - protected: Number of posts with content protected

    Raises:
        ValueError: If databases have mismatched platforms or wrong table_types
        ImportError: If required JSONPath dependencies not available

    Example:
        >>> source_db = PlatformDB.sqlite_db_from_path("twitter", "twitter_posts.sqlite")
        >>> anon_db = init_anon_db(source_db, "twitter_anon.sqlite")
        >>> stats = process_db(source_db, anon_db)
        >>> print(f"Anonymized {stats['anonymized']} users from {stats['processed']} posts")
    """
    logger = get_logger(__file__)

    # Validate inputs
    if source_db.platform != anon_db.platform:
        raise ValueError(f"Platform mismatch: source={source_db.platform}, anon={anon_db.platform}")

    print(f"🔄 Processing {source_db.platform} database for anonymization...")

    # 1. Get platform-specific user_id extraction pattern
    user_id_path, metadata_paths = platform_user_data_jsonpath(source_db.platform)

    if not user_id_path:
        logger.warning(f"No user_id pattern defined for platform {source_db.platform}")
        return {"processed": 0, "anonymized": 0, "errors": 0, "protected": 0}

    print(f"   User ID path: {user_id_path}")
    print(f"   Metadata paths: {metadata_paths}")

    # 2. Create JSONPath extractor for user data
    extractor_config = {"user_id": user_id_path}
    protection_paths = {"user_id": user_id_path}

    # Add all metadata paths to extraction and protection
    for i, path in enumerate(metadata_paths):
        field_name = f"metadata_{i}"
        extractor_config[field_name] = path
        protection_paths[field_name] = path

    try:
        extractor = JsonPathFieldExtractor(extractor_config)
    except ValueError as e:
        raise ValueError(f"Invalid JSONPath configuration for {source_db.platform}: {e}")

    # 3. Create content protector - will be updated with actual UUIDs per post
    protector = JsonPathContentProtector(protection_paths, "<PROTECTED>")

    # 4. Initialize anonymization components
    try:
        manager = SecureUserIDManager.from_env(load_private_key=False)
        db_ops = DatabaseOperations(anon_db)

        # Initialize protection marker for tracking which posts have been processed
        from .protection_marker import ProtectionMarker
        protection_marker = ProtectionMarker(source_db)
    except Exception as e:
        raise ValueError(f"Failed to initialize anonymization components: {e}")

    # 5. Process posts in batches
    stats = {"processed": 0, "anonymized": 0, "errors": 0, "protected": 0, "skipped": 0, "marked": 0}

    print(f"   Processing posts in batches of {batch_size}...")

    with source_db.get_session() as source_session:
        # Get total count for progress tracking
        total_posts = source_session.query(DBPost).count()
        print(f"   Total posts to process: {total_posts}")

        # Process in batches
        offset = 0
        while offset < total_posts:
            batch_posts = (source_session.query(DBPost)
                         .offset(offset)
                         .limit(batch_size)
                         .all())

            if not batch_posts:
                break

            # Filter out posts that are already protected
            unprotected_posts = []
            for post in batch_posts:
                if protection_marker.is_post_protected(post):
                    stats["skipped"] += 1
                else:
                    unprotected_posts.append(post)

            print(f"   Processing batch {offset//batch_size + 1} ({offset+1}-{offset+len(batch_posts)} of {total_posts})")
            print(f"     Skipped {len(batch_posts) - len(unprotected_posts)} already protected posts")

            # Use unprotected posts for processing
            batch_posts = unprotected_posts

            if not batch_posts:
                offset += batch_size
                continue

            # Extract user data from this batch
            batch_user_ids = []
            batch_user_metadata = []
            post_user_mapping = []  # Track which post corresponds to which user_id

            for post in batch_posts:
                try:
                    # Extract user_id and metadata using JSONPath
                    extracted = extractor.extract_from_data(post.content)

                    if extracted["user_id"]:
                        user_id = str(extracted["user_id"])
                        batch_user_ids.append(user_id)

                        # Collect metadata from additional paths
                        metadata = {}
                        for key, value in extracted.items():
                            if key != "user_id" and value is not None:
                                metadata[key] = value

                        batch_user_metadata.append(metadata if metadata else None)
                        post_user_mapping.append((post, user_id))
                        stats["processed"] += 1
                    else:
                        logger.debug(f"No user_id found in post {post.id}")
                        stats["errors"] += 1

                except Exception as e:
                    logger.warning(f"Error extracting user_id from post {post.id}: {e}")
                    stats["errors"] += 1

            # Create anonymization mappings for this batch
            if batch_user_ids:
                try:
                    mappings = manager.prepare_mappings_for_db(batch_user_ids, batch_user_metadata)
                    hash_to_uuid = db_ops.add_mappings(mappings)
                    stats["anonymized"] += len(hash_to_uuid)

                    logger.info(f"Created {len(hash_to_uuid)} anonymization mappings in batch")

                except Exception as e:
                    logger.error(f"Failed to create anonymization mappings: {e}")
                    stats["errors"] += len(batch_user_ids)

            # Protect content in source database (replace user_ids with UUIDs)
            if protect_content and post_user_mapping and batch_user_ids:
                try:
                    # Create UUID mapping from anonymization results
                    uuid_mapping = {}
                    for user_id in batch_user_ids:
                        hashed_id = manager.create_hmac_hash(user_id)
                        if hashed_id in hash_to_uuid:
                            uuid_mapping[user_id] = hash_to_uuid[hashed_id]

                    # Replace content in each post with actual UUIDs
                    for post, original_user_id in post_user_mapping:
                        if original_user_id in uuid_mapping:
                            public_uuid = uuid_mapping[original_user_id]

                            # Replace user ID in content with public UUID
                            protected_content = post.content.copy()

                            # Replace the main user ID (user.id_str)
                            extractor.replace_in_data(protected_content, "user_id", public_uuid)

                            # Replace metadata fields with "<PROTECTED>"
                            for j in range(len(metadata_paths)):
                                field_name = f"metadata_{j}"
                                extractor.replace_in_data(protected_content, field_name, "<PROTECTED>")

                            post.content = protected_content
                            # Tell SQLAlchemy that the mutable JSON content has been modified
                            flag_modified(post, 'content')
                            stats["protected"] += 1
                        else:
                            logger.warning(f"No UUID found for user {original_user_id} in post {post.id}")

                    # Mark posts as protected in metadata
                    posts_to_mark = [post for post, _ in post_user_mapping]
                    marked_count = protection_marker.mark_posts_as_protected(posts_to_mark, source_session)
                    stats["marked"] += marked_count

                    # Commit the content protection changes and protection marking
                    source_session.commit()
                    logger.info(f"Protected content in {len(post_user_mapping)} posts with UUIDs")
                    logger.info(f"Marked {marked_count} posts as protected")

                except Exception as e:
                    logger.error(f"Failed to protect content in batch: {e}")
                    source_session.rollback()

            offset += batch_size

    # Print final statistics
    print(f"\n✅ Anonymization processing completed:")
    print(f"   📊 Posts processed: {stats['processed']}")
    print(f"   👥 Users anonymized: {stats['anonymized']}")
    print(f"   🛡️  Posts protected: {stats['protected']}")
    print(f"   ✅ Posts marked as protected: {stats['marked']}")
    print(f"   ⏭️  Posts skipped (already protected): {stats['skipped']}")
    print(f"   ❌ Errors encountered: {stats['errors']}")

    if stats['errors'] > 0:
        print(f"\n⚠️  {stats['errors']} posts had extraction errors - check logs for details")

    if stats['skipped'] > 0:
        print(f"\n🔄 {stats['skipped']} posts were already protected and skipped - efficient re-run!")

    return stats
