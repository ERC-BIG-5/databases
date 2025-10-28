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

from .secure_config import SecurityConfig
from .secure_user_id_manager import SecureUserIDManager
from .db_operations import DatabaseOperations


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


# Example usage
if __name__ == "__main__":
    import os
    import base64
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    
    # Set up logging to see audit trail
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 70)
    print("DECRYPTION MANAGER - AUDIT OPERATIONS")
    print("=" * 70)
    
    # Generate test keys
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ).decode()
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()
    
    # Set up environment
    os.environ["HMAC_KEY"] = base64.b64encode(os.urandom(32)).decode()
    os.environ["PUBLIC_KEY_PEM"] = public_pem
    os.environ["PRIVATE_KEY_PEM"] = private_pem
    os.environ["KEY_VERSION"] = "v1"
    
    print("\n⚠️  IMPORTANT: This manager should only be used for:")
    print("   • Authorized investigations")
    print("   • Legal compliance requests")
    print("   • Security incident response")
    print("   • Regulatory audits")
    
    print("\n✅ All operations are logged to audit trail")
    print("✅ Requires authorization justification")
    print("✅ Private key loaded only when needed")
    
    print("\n" + "=" * 70)
    print("See integration example for usage patterns")
    print("=" * 70)
