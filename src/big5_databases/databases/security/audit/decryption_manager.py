"""
Decryption Manager for Audit/Investigation Operations

IMPORTANT: Use this ONLY for authorized audit operations.
This manager loads the private key and should be used sparingly.

SECURITY:
- Load private key only when needed
- Log all decryption operations
- Restrict access via RBAC
- Time-limit private key exposure
"""

from typing import Optional
import logging

from ..core.secure_config import SecurityConfig
from ..core.secure_user_id_manager import SecureUserIDManager
from ..core.db_operations import DatabaseOperations


# Configure audit logging
audit_logger = logging.getLogger("anonymization.audit")


class DecryptionManager:
    """
    Handles decryption operations for authorized audit/investigation.
    
    CRITICAL: This class loads the private key. Use only when necessary.
    
    Usage:
    - Load only during investigation/audit
    - Log all operations
    - Time-limit the session
    - Require authorization checks
    """
    
    def __init__(
        self,
        config: SecurityConfig,
        db_ops: DatabaseOperations,
        authorized_by: str
    ):
        """
        Initialize decryption manager.
        
        Args:
            config: SecurityConfig with private key
            db_ops: DatabaseOperations instance
            authorized_by: Username/ID of person authorizing this operation
        """
        # Load manager WITH private key
        self.manager = SecureUserIDManager(config, load_private_key=True)
        self.db_ops = db_ops
        self.authorized_by = authorized_by
        
        # Log the initialization
        audit_logger.warning(
            f"DecryptionManager initialized by {authorized_by}"
        )
    
    @classmethod
    def from_env(
        cls,
        db_ops: DatabaseOperations,
        authorized_by: str
    ) -> "DecryptionManager":
        """
        Create from environment variables.
        
        Args:
            db_ops: DatabaseOperations instance
            authorized_by: Username/ID of authorizing person
            
        Returns:
            DecryptionManager with private key loaded
        """
        config = SecurityConfig.from_env()
        return cls(config, db_ops, authorized_by)
    
    def get_original_id_by_uuid(
        self,
        public_uuid: str,
        reason: str
    ) -> Optional[str]:
        """
        Decrypt and retrieve original user ID from public UUID.
        
        ⚠️ SENSITIVE OPERATION - Always logged
        
        Args:
            public_uuid: The public UUID to investigate
            reason: Justification for decryption (required)
            
        Returns:
            Original user ID or None if not found
        """
        # Log the operation
        audit_logger.warning(
            f"Decryption requested | "
            f"UUID: {public_uuid} | "
            f"Authorized by: {self.authorized_by} | "
            f"Reason: {reason}"
        )
        
        # Lookup in database
        entry = self.db_ops.get_mapping_by_uuid(public_uuid)
        
        if not entry:
            audit_logger.info(f"UUID {public_uuid} not found")
            return None
        
        # Decrypt the original ID
        original_id = self.manager.decrypt(entry.encrypted_user_id)
        
        audit_logger.warning(
            f"Decryption completed | "
            f"UUID: {public_uuid} | "
            f"Original ID: {original_id} | "
            f"Key version: {entry.key_version}"
        )
        
        return original_id
    
    def get_original_id_by_hash(
        self,
        hashed_id: str,
        reason: str
    ) -> Optional[str]:
        """
        Decrypt and retrieve original user ID from hashed ID.
        
        ⚠️ SENSITIVE OPERATION - Always logged
        
        Args:
            hashed_id: The internal hashed ID to investigate
            reason: Justification for decryption (required)
            
        Returns:
            Original user ID or None if not found
        """
        # Log the operation
        audit_logger.warning(
            f"Decryption requested | "
            f"Hash: {hashed_id[:20]}... | "
            f"Authorized by: {self.authorized_by} | "
            f"Reason: {reason}"
        )
        
        # Lookup in database
        entry = self.db_ops.get_mapping_by_hash(hashed_id)
        
        if not entry:
            audit_logger.info(f"Hash {hashed_id[:20]}... not found")
            return None
        
        # Decrypt the original ID
        original_id = self.manager.decrypt(entry.encrypted_user_id)
        
        audit_logger.warning(
            f"Decryption completed | "
            f"Hash: {hashed_id[:20]}... | "
            f"Original ID: {original_id} | "
            f"Key version: {entry.key_version}"
        )
        
        return original_id
    
    def get_user_metadata(
        self,
        public_uuid: str,
        reason: str
    ) -> Optional[dict]:
        """
        Decrypt and retrieve user metadata from public UUID.
        
        ⚠️ SENSITIVE OPERATION - Always logged
        
        Args:
            public_uuid: The public UUID to investigate
            reason: Justification for decryption (required)
            
        Returns:
            Decrypted user metadata dict or None
        """
        audit_logger.warning(
            f"Metadata decryption requested | "
            f"UUID: {public_uuid} | "
            f"Authorized by: {self.authorized_by} | "
            f"Reason: {reason}"
        )
        
        # Lookup in database
        entry = self.db_ops.get_mapping_by_uuid(public_uuid)
        
        if not entry or not entry.encrypted_data:
            return None
        
        # Decrypt metadata
        import json
        metadata_json = self.manager.decrypt(entry.encrypted_data)
        metadata = json.loads(metadata_json)
        
        audit_logger.warning(
            f"Metadata decryption completed | "
            f"UUID: {public_uuid}"
        )
        
        return metadata

    def decrypt_user_id(self, encrypted_user_id: str) -> str:
        """
        Decrypt an encrypted user ID directly.

        This is a lower-level method that decrypts without database lookup.
        Used when you already have the encrypted value from the anon database.

        Parameters
        ----------
        encrypted_user_id : str
            The envelope-encrypted user ID (JSON string from anon database).

        Returns
        -------
        str
            The original plaintext user ID.

        Raises
        ------
        ValueError
            If decryption fails due to invalid key or corrupted data.

        Notes
        -----
        This operation is logged for audit purposes.
        """
        audit_logger.warning(
            f"Direct decryption requested | "
            f"Authorized by: {self.authorized_by}"
        )

        original_id = self.manager.decrypt(encrypted_user_id)

        audit_logger.warning(
            f"Direct decryption completed | "
            f"Original ID: {original_id}"
        )

        return original_id


