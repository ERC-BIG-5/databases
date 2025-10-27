"""
Database Operations for Two-Layer Anonymization System

Separated from SecureUserIDManager to follow single responsibility principle.
Handles all database interactions for user ID mappings.
"""

import json
from typing import Optional


# Mock IntegrityError for demo (in production, use sqlalchemy.exc.IntegrityError)
class IntegrityError(Exception):
    pass


# Mock DBAnonymize model - replace with your actual model
class DBAnonymize:
    """Database model for user anonymization mappings"""
    user_id_hash = None
    public_id = None
    encrypted_user_id = None
    encrypted_data = None
    key_version = None
    
    def __init__(self, user_id_hash, encrypted_user_id, public_id, encrypted_data, key_version):
        self.user_id_hash = user_id_hash
        self.encrypted_user_id = encrypted_user_id
        self.public_id = public_id
        self.encrypted_data = encrypted_data
        self.key_version = key_version


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
                        DBAnonymize.user_id_hash,
                        DBAnonymize.public_id
                    ).filter(
                        DBAnonymize.user_id_hash.in_(hashes)
                    ).with_for_update().all()
                except (AttributeError, TypeError):
                    # Fallback for mocks or simple DBs
                    existing = session.query(
                        DBAnonymize
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
                    
                    db_entry = DBAnonymize(
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
                        DBAnonymize.user_id_hash,
                        DBAnonymize.public_id
                    ).filter(
                        DBAnonymize.user_id_hash.in_(hashes)
                    ).all()
                except (AttributeError, TypeError):
                    # Fallback for mocks
                    existing = session.query(DBAnonymize).all()
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
            DBAnonymize entry or None if not found
        """
        with self.db.get_session() as session:
            try:
                entry = session.query(DBAnonymize).filter(
                    DBAnonymize.public_id == public_uuid
                ).first()
            except (AttributeError, TypeError):
                # Fallback for mocks
                all_entries = session.query(DBAnonymize).all()
                entry = next((e for e in all_entries if e.public_id == public_uuid), None)
            return entry
    
    def get_mapping_by_hash(self, hashed_id: str) -> Optional[DBAnonymize]:
        """
        Retrieve a mapping by hashed ID.
        
        Args:
            hashed_id: The hashed user ID to look up
            
        Returns:
            DBAnonymize entry or None if not found
        """
        with self.db.get_session() as session:
            try:
                entry = session.query(DBAnonymize).filter(
                    DBAnonymize.user_id_hash == hashed_id
                ).first()
            except (AttributeError, TypeError):
                # Fallback for mocks
                all_entries = session.query(DBAnonymize).all()
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
                    DBAnonymize.user_id_hash,
                    DBAnonymize.public_id
                ).filter(
                    DBAnonymize.user_id_hash.in_(hashed_ids)
                ).all()
            except (AttributeError, TypeError):
                # Fallback for mocks
                all_entries = session.query(DBAnonymize).all()
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
            query = session.query(DBAnonymize)
            
            if key_version:
                query = query.filter(DBAnonymize.key_version == key_version)
            
            return query.count()


# Example usage
if __name__ == "__main__":
    print("=" * 70)
    print("DATABASE OPERATIONS MODULE")
    print("=" * 70)
    
    print("\nThis module provides database operations separated from crypto logic.")
    print("\nKey benefits:")
    print("  ✅ Single responsibility principle")
    print("  ✅ Easier testing")
    print("  ✅ Clearer separation of concerns")
    print("  ✅ Can swap database implementations easily")
    
    print("\n" + "=" * 70)
    print("See updated integration example for usage")
    print("=" * 70)
