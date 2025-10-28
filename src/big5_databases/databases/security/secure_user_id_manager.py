"""
SecureUserIDManager - Crypto Operations Only

DESIGN:
- DB operations moved to db_operations.py
- Private key is now OPTIONAL (not needed for regular anonymization)
- Focused solely on cryptographic operations
- Cleaner separation of concerns

USAGE:
- Regular operations (anonymization): Only needs public key
- Audit operations (decryption): Needs private key (use DecryptionManager)
"""

import json
import hmac
import hashlib
import uuid
from typing import Optional
from dataclasses import dataclass

from cryptography.hazmat.primitives import serialization

from secure_config import SecurityConfig
from envelope_encryption import EnvelopeEncryption, EnvelopeData


@dataclass
class UserMapping:
    """Represents a complete user ID mapping"""
    hashed_id: str
    encrypted_original_id: str
    public_uuid: str
    key_version: str


class SecureUserIDManager:
    """
    Handles cryptographic operations for two-layer anonymization.
    
    SECURITY NOTE: Private key is OPTIONAL
    - Regular operations (encrypt, hash, anonymize) only need public key
    - Decryption operations need private key (use DecryptionManager instead)
    
    This separation means your daily batch job doesn't need the private key!
    """
    
    def __init__(
        self,
        config: SecurityConfig,
        load_private_key: bool = False
    ):
        """
        Initialize manager with cryptographic keys.
        
        Args:
            config: SecurityConfig loaded from environment
            load_private_key: If False (default), private key won't be loaded.
                            Set to True only for audit/investigation operations.
        """
        self.config = config
        self.key_version = config.key_version
        
        # Extract HMAC key (always needed)
        self.hmac_key = config.hmac_key.get_secret_value()
        
        # Load public key (always needed for encryption)
        self.public_key = serialization.load_pem_public_key(
            config.public_key_pem.get_secret_value().encode()
        )
        
        # Load private key ONLY if requested
        self.private_key = None
        if load_private_key:
            if not config.private_key_pem or not config.private_key_pem.get_secret_value():
                raise ValueError(
                    "Private key requested but not available in config"
                )
            self.private_key = serialization.load_pem_private_key(
                config.private_key_pem.get_secret_value().encode(),
                password=None
            )
        
        # Initialize envelope encryption
        self.envelope = EnvelopeEncryption(
            public_key=self.public_key,
            private_key=self.private_key,  # Will be None for regular operations
            key_version=self.key_version
        )
    
    @classmethod
    def from_env(cls, load_private_key: bool = False) -> "SecureUserIDManager":
        """
        Convenience constructor that loads config from environment.
        
        Args:
            load_private_key: If False (default), private key won't be loaded
            
        Returns:
            Initialized SecureUserIDManager
            
        Example:
            # Regular batch job (no private key)
            manager = SecureUserIDManager.from_env()
            
            # Audit operation (with private key)
            manager = SecureUserIDManager.from_env(load_private_key=True)
        """
        config = SecurityConfig.from_env()
        return cls(config, load_private_key=load_private_key)
    
    def create_hmac_hash(self, user_id: str) -> str:
        """
        Create a keyed HMAC hash of the user ID.
        
        This is Layer 1: the INTERNAL identifier.
        Include key_version to support rotation.
        
        Args:
            user_id: The user ID to hash
            
        Returns:
            64-character hex string (SHA256 hash)
        """
        message = f"{self.key_version}|{user_id}"
        return hmac.new(
            key=self.hmac_key.encode(),
            msg=message.encode(),
            digestmod=hashlib.sha256
        ).hexdigest()
    
    def generate_public_uuid(self) -> str:
        """
        Generate a random UUID for external use.
        
        This is Layer 2: the EXTERNAL identifier in handed-out datasets.
        
        Returns:
            Random UUID as string
        """
        return str(uuid.uuid4())
    
    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt a string using envelope encryption.
        
        Only requires public key (no private key needed).
        
        Args:
            plaintext: String to encrypt
            
        Returns:
            JSON string containing encrypted envelope
        """
        envelope = self.envelope.encrypt(plaintext.encode())
        return envelope.to_json()
    
    def decrypt(self, encrypted_json: str) -> str:
        """
        Decrypt an envelope-encrypted string.
        
        REQUIRES PRIVATE KEY - will raise error if not loaded.
        
        Args:
            encrypted_json: JSON string from envelope encryption
            
        Returns:
            Decrypted plaintext string
            
        Raises:
            ValueError: If private key not loaded
        """
        if not self.private_key:
            raise ValueError(
                "Private key required for decryption. "
                "Initialize with load_private_key=True"
            )
        
        envelope = EnvelopeData.from_json(encrypted_json)
        decrypted_bytes = self.envelope.decrypt(envelope)
        return decrypted_bytes.decode()
    
    def create_user_mapping(
        self,
        user_id: str,
        user_data: Optional[dict] = None
    ) -> UserMapping:
        """
        Create a complete user mapping with all identifiers.
        
        Does NOT save to database - use DatabaseOperations.add_mappings() for that.
        
        Args:
            user_id: The original user ID (e.g., "@username")
            user_data: Optional metadata to encrypt alongside
            
        Returns:
            UserMapping with hashed_id, encrypted_original_id, public_uuid, key_version
        """
        hashed_id = self.create_hmac_hash(user_id)
        encrypted_id = self.encrypt(user_id)
        public_uuid = self.generate_public_uuid()
        
        return UserMapping(
            hashed_id=hashed_id,
            encrypted_original_id=encrypted_id,
            public_uuid=public_uuid,
            key_version=self.key_version
        )
    
    def prepare_mappings_for_db(
        self,
        user_ids: list[str],
        user_data: list[Optional[dict]]
    ) -> list[tuple[str, str, str, Optional[str], str]]:
        """
        Prepare user mappings for database insertion.
        
        Returns tuples ready for DatabaseOperations.add_mappings().
        
        Args:
            user_ids: List of user IDs to process
            user_data: List of optional metadata dicts (same length)
            
        Returns:
            List of (hashed_id, encrypted_user_id, public_uuid, 
                     encrypted_data, key_version) tuples
                     
        Raises:
            ValueError: If lists have different lengths
        """
        if len(user_ids) != len(user_data):
            raise ValueError(
                f"Mismatched lengths: {len(user_ids)} user_ids, "
                f"{len(user_data)} user_data"
            )
        
        mappings = []
        for user_id, _user_data in zip(user_ids, user_data):
            hashed_id = self.create_hmac_hash(user_id)
            encrypted_user_id = self.encrypt(user_id)
            public_uuid = self.generate_public_uuid()
            
            # Encrypt user data if present
            encrypted_data = None
            if _user_data:
                encrypted_data = self.encrypt(json.dumps(_user_data))
            
            mappings.append((
                hashed_id,
                encrypted_user_id,
                public_uuid,
                encrypted_data,
                self.key_version
            ))
        
        return mappings


# Example usage
if __name__ == "__main__":
    import os
    import base64
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    
    print("=" * 70)
    print("SECURE USER ID MANAGER - CRYPTO ONLY")
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
    os.environ["KEY_VERSION"] = "v2"
    
    print("\n🔒 SECURITY FEATURE: Optional Private Key\n")
    
    # Regular operation - NO private key
    print("1. Regular batch job (NO private key loaded):")
    manager = SecureUserIDManager.from_env(load_private_key=False)
    print(f"   ✅ Manager initialized")
    print(f"   ✅ Can hash: {manager.create_hmac_hash('@alice')[:20]}...")
    print(f"   ✅ Can encrypt: {len(manager.encrypt('@alice'))} bytes")
    print(f"   ❌ Cannot decrypt (private key not loaded)")
    
    try:
        manager.decrypt("some_encrypted_data")
    except ValueError as e:
        print(f"   Expected error: {e}")
    
    # Audit operation - WITH private key
    print("\n2. Audit operation (WITH private key loaded):")
    audit_manager = SecureUserIDManager.from_env(load_private_key=True)
    encrypted = audit_manager.encrypt("@bob")
    decrypted = audit_manager.decrypt(encrypted)
    print(f"   ✅ Can decrypt: {decrypted}")
    
    print("\n" + "=" * 70)
    print("BENEFITS:")
    print("=" * 70)
    print("✅ Daily batch job doesn't need private key")
    print("✅ Reduced attack surface (private key not in memory)")
    print("✅ Separate credentials for regular vs. audit operations")
    print("✅ Clear separation: crypto logic here, DB logic elsewhere")
