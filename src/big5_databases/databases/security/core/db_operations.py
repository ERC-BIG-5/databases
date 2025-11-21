"""
Database Operations for Two-Layer Anonymization System

Separated from SecureUserIDManager to follow single responsibility principle.
Handles all database interactions for user ID mappings.
"""

import json
import typing
from pathlib import Path
from typing import Optional, Any, Union

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.attributes import flag_modified


from ...db_models import DBAnonymize, DBPost
from ...model_conversion import PostModel
from ...platform_db_mgmt import PlatformDB
from ...db_mgmt import DatabaseManager


from ..utils.jsonpath_extractor import JsonPathFieldExtractor, JsonPathContentProtector
from ..utils.exec_db_fixes import platform_user_data_jsonpath
from .secure_user_id_manager import SecureUserIDManager

from tools.project_logging import get_logger

if typing.TYPE_CHECKING:
    from big5_databases.databases.security import ProtectionMarker

class DatabaseOperations:
    """
    Handles database operations for anonymization mappings.

    Separated from crypto operations for:
    - Better separation of concerns
    - Easier testing
    - Clearer code organization
    """

    def __init__(self, db_manager: DatabaseManager) -> None:
        """
        Initialize with database manager.

        Args:
            db_manager: Your DatabaseManager instance
        """
        self.db = db_manager
        self.DBAnonymize = DBAnonymize
    
    def add_mappings(
        self,
        mappings: list[tuple[str, str, str, Optional[str], str, str]]
    ) -> dict[str, str]:
        """
        Add user mappings to database with transaction safety.

        Args:
            mappings: List of (hashed_id, encrypted_user_id, public_uuid,
                               encrypted_data, key_version, pseudo_name) tuples
        
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
                     encrypted_data, key_version, pseudo_name) = hash_to_mapping[hash_]

                    db_entry = self.DBAnonymize(
                        user_id_hash=hashed_id,
                        encrypted_user_id=encrypted_user_id,
                        public_id=public_uuid,
                        encrypted_data=encrypted_data,
                        key_version=key_version,
                        pseudo_name= pseudo_name
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
    
    def get_mapping_by_uuid(self, public_uuid: str) -> Optional[DBAnonymize]:
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
    
    def get_mapping_by_hash(self, hashed_id: str) -> Optional[DBAnonymize]:
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


def init_anon_db(source_platform_db: PlatformDB, anon_db_path: Union[Path, str]) -> PlatformDB:
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

def get_anon_db(anon_db_path: Path, platform: str = "?"):
    return PlatformDB.sqlite_db_from_path(
        platform=platform,
        path=anon_db_path,
        create=True,
        table_type="anon"
    )

def _get_existing_protected_users_ids(anon_db: PlatformDB, hash_ids: list[str]) -> dict[str, str]:
    """

    Parameters
    ----------
    anon_db
    hash_ids

    Returns
    -------
    map: hashes: existing random uuids form the
    """
    assert anon_db.table_type == "anon"
    with anon_db.get_session() as session:
        return dict(session.query(DBAnonymize.user_id_hash, DBAnonymize.public_id).filter(DBAnonymize.user_id_hash.in_(hash_ids)).all())


def protect_posts_batch(
    posts: list[Union[DBPost, PostModel]],
    platform: str,
    user_id_manager: SecureUserIDManager,
    anon_db: PlatformDB,
    protection_marker: Optional["ProtectionMarker"] = None,
    skip_already_protected: bool = True
) -> dict[str, int]:
    """
    Universal helper function to protect a batch of posts with user anonymization.

    This is the core function for post protection that:
    1. Extracts user IDs from posts using platform-specific JSONPath patterns
    2. Checks anon DB for existing mappings (hash -> UUID)
    3. Creates new mappings for users that don't exist yet
    4. Protects post content by replacing user IDs with UUIDs
    5. Marks posts as protected

    Can be used for:
    - Processing whole databases (called from process_db)
    - Inserting new posts (called from anywhere in databases package)

    Args:
        posts: List of DBPost or PostModel objects to protect
        platform: Platform name (twitter, tiktok, instagram, youtube)
        user_id_manager: SecureUserIDManager instance for crypto operations
        anon_db: Anonymization database (PlatformDB with table_type="anon")
        protection_marker: Optional ProtectionMarker for tracking protection status
        skip_already_protected: If True, skip posts already marked as protected

    Returns:
        dict: Processing statistics with keys:
            - processed: Number of posts processed
            - protected: Number of posts protected
            - skipped: Number of posts skipped (already protected)
            - errors: Number of errors encountered
            - users_anonymized: Number of new user mappings created

    Raises:
        ValueError: If platform not supported or invalid configuration

    Example:
        >>> from big5_databases.databases.security import protect_posts_batch
        >>> user_id_manager = SecureUserIDManager.from_env()
        >>> anon_db = get_anon_db("twitter_anon.sqlite", "twitter")
        >>> stats = protect_posts_batch(new_posts, "twitter", user_id_manager, anon_db)
        >>> print(f"Protected {stats['protected']} posts")
    """
    logger = get_logger(__file__)

    # Validate inputs
    if not posts:
        return {"processed": 0, "protected": 0, "skipped": 0, "errors": 0, "users_anonymized": 0}

    if anon_db.table_type != "anon":
        raise ValueError(f"anon_db must have table_type='anon', got '{anon_db.table_type}'")

    # Get platform-specific JSONPath patterns for user data extraction
    user_id_path, metadata_paths = platform_user_data_jsonpath(platform)

    if not user_id_path:
        raise ValueError(f"No user_id pattern defined for platform {platform}")

    # Set up extractor and protector
    extractor_config: dict[str, str] = {"user_id": user_id_path}
    protection_paths: dict[str, str] = {"user_id": user_id_path}

    # Add metadata paths
    for i, path in enumerate(metadata_paths):
        field_name = f"metadata_{i}"
        extractor_config[field_name] = path
        protection_paths[field_name] = path

    extractor = JsonPathFieldExtractor(extractor_config)
    protector = JsonPathContentProtector(protection_paths)

    # Initialize statistics
    stats = {"processed": 0, "protected": 0, "skipped": 0, "errors": 0, "users_anonymized": 0}

    # Map posts to user data for batch processing
    post_user_map: dict[str, list[Union[DBPost, PostModel]]] = {}  # hashed_id -> posts
    user_id_to_hash: dict[str, str] = {}  # original_user_id -> hashed_id
    user_metadata: dict[str, Optional[dict]] = {}  # hashed_id -> metadata

    # Phase 1: Extract user IDs and build mappings
    for post in posts:
        try:
            # Skip if already protected
            if skip_already_protected and protection_marker and protection_marker.is_post_protected(post):
                stats["skipped"] += 1
                continue

            # Get content as dict
            if isinstance(post, DBPost):
                content = post.content if isinstance(post.content, dict) else json.loads(post.content or '{}')
            else:
                content = post.content

            # Extract user data
            extracted = extractor.extract_from_data(content)
            user_id = extracted.get("user_id")

            if not user_id:
                logger.warning(f"No user_id found in post {getattr(post, 'id', 'unknown')}")
                stats["errors"] += 1
                continue

            # Create hash
            hashed_id = user_id_manager.create_hmac_hash(str(user_id))
            user_id_to_hash[str(user_id)] = hashed_id

            # Collect metadata
            metadata: dict[str, str] = {}
            for key, value in extracted.items():
                if key != "user_id" and value is not None:
                    metadata[key] = value

            if hashed_id not in user_metadata:
                user_metadata[hashed_id] = metadata if metadata else None

            # Map post to hashed user
            if hashed_id not in post_user_map:
                post_user_map[hashed_id] = []
            post_user_map[hashed_id].append(post)

            stats["processed"] += 1

        except Exception as e:
            logger.error(f"Error extracting user data from post: {e}")
            stats["errors"] += 1

    if not post_user_map:
        logger.info("No posts to protect after filtering")
        return stats

    # Phase 2: Get existing mappings and create new ones
    all_hashed_ids = list(post_user_map.keys())
    hash_to_uuid = _get_existing_protected_users_ids(anon_db, all_hashed_ids)

    # Determine which users need new mappings
    existing_hashes = set(hash_to_uuid.keys())
    new_hashes = set(all_hashed_ids) - existing_hashes

    logger.info(f"Found {len(existing_hashes)} existing mappings, creating {len(new_hashes)} new ones")

    # Create new mappings
    if new_hashes:
        # Reverse lookup: hash -> original user_id
        hash_to_user_id = {v: k for k, v in user_id_to_hash.items()}

        # Prepare mappings for new users
        new_user_ids = [hash_to_user_id[h] for h in new_hashes]
        new_metadata = [user_metadata.get(h) for h in new_hashes]

        # Create mappings
        mappings = user_id_manager.prepare_mappings_for_db(new_user_ids, new_metadata)

        # Convert AnonymizeModel to tuple format for add_mappings
        mapping_tuples = [
            (
                m.user_id_hash.get_secret_value(),
                m.encrypted_user_id.get_secret_value(),
                str(m.public_id),
                m.encrypted_data.get_secret_value() if m.encrypted_data else None,
                m.key_version,
                m.pseudo_name
            )
            for m in mappings
        ]

        # Add to database
        db_ops = DatabaseOperations(anon_db)
        new_hash_to_uuid = db_ops.add_mappings(mapping_tuples)

        # Update hash_to_uuid with new mappings
        hash_to_uuid.update(new_hash_to_uuid)
        stats["users_anonymized"] = len(new_hashes)

    # Phase 3: Protect post content
    for hashed_id, posts_for_user in post_user_map.items():
        if hashed_id not in hash_to_uuid:
            logger.error(f"No UUID mapping found for hash {hashed_id[:10]}...")
            stats["errors"] += len(posts_for_user)
            continue

        public_uuid = hash_to_uuid[hashed_id]

        for post in posts_for_user:
            try:
                # Get content
                if isinstance(post, DBPost):
                    content = post.content if isinstance(post.content, dict) else json.loads(post.content or '{}')
                else:
                    content = post.content

                # Create protected content
                protected_content = content.copy()

                # Replace user_id with UUID
                extractor.replace_in_data(protected_content, "user_id", public_uuid)

                # Protect other sensitive fields with "<PROTECTED>"
                for i in range(len(metadata_paths)):
                    field_name = f"metadata_{i}"
                    extractor.replace_in_data(protected_content, field_name, "<PROTECTED>")

                # Update post content
                if isinstance(post, DBPost):
                    post.content = protected_content
                    flag_modified(post, 'content')
                else:
                    post.content = protected_content

                # Mark as protected
                if protection_marker:
                    # For DBPost
                    if isinstance(post, DBPost):
                        if not post.metadata_content:
                            post.metadata_content = {}
                        post.metadata_content.setdefault('protection', {})['protected_user'] = True
                        flag_modified(post, 'metadata_content')
                    # For PostModel
                    else:
                        if not post.metadata_content:
                            post.metadata_content = {"protection": {"protected_user": True}}
                        elif isinstance(post.metadata_content, dict):
                            post.metadata_content.setdefault('protection', {})['protected_user'] = True

                stats["protected"] += 1

            except Exception as e:
                logger.error(f"Error protecting post content: {e}")
                stats["errors"] += 1

    logger.info(f"Batch protection complete: {stats}")
    return stats


def process_db(source_db: PlatformDB, anon_db: PlatformDB, batch_size: int = 1000, protect_content: bool = True) -> dict[str, int]:
    """
    Process posts from source database and anonymize them into anon database.

    This function now uses the universal `protect_posts_batch()` helper for clean,
    maintainable batch processing.

    Workflow:
    1. Fetches posts in batches from source database
    2. Calls protect_posts_batch() to handle protection
    3. Commits protected posts back to database

    Args:
        source_db: Source platform database (table_type="posts")
        anon_db: Target anonymization database (table_type="anon")
        batch_size: Number of posts to process per batch (default: 1000)
        protect_content: Whether to replace user_ids in source with "<PROTECTED>" (default: True)

    Returns:
        dict: Processing statistics with keys:
            - processed: Number of posts processed
            - users_anonymized: Number of unique users anonymized
            - errors: Number of posts with extraction errors
            - protected: Number of posts with content protected
            - skipped: Number of posts skipped (already protected)

    Raises:
        ValueError: If databases have mismatched platforms or wrong table_types
        ImportError: If required JSONPath dependencies not available

    Example:
        >>> source_db = PlatformDB.sqlite_db_from_path("twitter", "twitter_posts.sqlite")
        >>> anon_db = init_anon_db(source_db, "twitter_anon.sqlite")
        >>> stats = process_db(source_db, anon_db)
        >>> print(f"Anonymized {stats['users_anonymized']} users from {stats['processed']} posts")
    """
    from big5_databases.databases.security import ProtectionMarker
    logger = get_logger(__file__)

    # Validate inputs
    if source_db.platform != anon_db.platform:
        raise ValueError(f"Platform mismatch: source={source_db.platform}, anon={anon_db.platform}")

    if anon_db.table_type != "anon":
        raise ValueError(f"anon_db must have table_type='anon', got '{anon_db.table_type}'")

    logger.info(f"🔄 Processing {source_db.platform} database for anonymization...")

    # Initialize anonymization components
    try:
        user_id_manager = SecureUserIDManager.from_env(load_private_key=False)
        protection_marker = ProtectionMarker(source_db)
    except Exception as e:
        raise ValueError(f"Failed to initialize anonymization components: {e}")

    # Initialize statistics
    total_stats = {"processed": 0, "users_anonymized": 0, "errors": 0, "protected": 0, "skipped": 0}

    print(f"   Processing posts in batches of {batch_size}...")

    # Process posts in batches
    with source_db.get_session() as session:
        # Get total count for progress tracking
        total_posts = session.query(DBPost).count()
        logger.info(f"   Total posts in database: {total_posts}")

        # Query posts in batches
        offset = 0
        batch_num = 0

        while True:
            # Fetch batch
            posts_batch = session.query(DBPost).offset(offset).limit(batch_size).all()

            if not posts_batch:
                break

            batch_num += 1
            logger.info(f"   Processing batch {batch_num} ({offset}-{offset + len(posts_batch)})...")

            # Protect batch using universal helper
            if protect_content:
                batch_stats = protect_posts_batch(
                    posts=posts_batch,
                    platform=source_db.platform,
                    user_id_manager=user_id_manager,
                    anon_db=anon_db,
                    protection_marker=protection_marker,
                    skip_already_protected=True
                )

                # Accumulate statistics
                for key in total_stats:
                    total_stats[key] += batch_stats.get(key, 0)

                # Commit this batch
                try:
                    session.commit()
                    logger.info(f"   Batch {batch_num} committed: "
                              f"{batch_stats['protected']} protected, "
                              f"{batch_stats['skipped']} skipped, "
                              f"{batch_stats['errors']} errors")
                except Exception as e:
                    logger.error(f"   Failed to commit batch {batch_num}: {e}")
                    session.rollback()
                    total_stats["errors"] += len(posts_batch)
            else:
                # If protect_content is False, just skip the batch
                logger.info(f"   Batch {batch_num} skipped (protect_content=False)")

            offset += batch_size

    # Print final statistics
    print(f"\n✅ Anonymization processing completed:")
    print(f"   📊 Posts processed: {total_stats['processed']}")
    print(f"   👥 Users anonymized: {total_stats['users_anonymized']}")
    print(f"   🛡️  Posts protected: {total_stats['protected']}")
    print(f"   ⏭️  Posts skipped (already protected): {total_stats['skipped']}")
    print(f"   ❌ Errors encountered: {total_stats['errors']}")

    if total_stats['errors'] > 0:
        print(f"\n⚠️  {total_stats['errors']} posts had errors - check logs for details")

    if total_stats['skipped'] > 0:
        print(f"\n🔄 {total_stats['skipped']} posts were already protected and skipped - efficient re-run!")

    return total_stats
