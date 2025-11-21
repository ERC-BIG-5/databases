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
from pydantic import SecretStr
from cryptography.hazmat.primitives import serialization

from .secure_config import SecurityConfig
from .envelope_encryption import EnvelopeEncryption, EnvelopeData
from ..utils.named_uuids import generate
from ...model_conversion import AnonymizeModel


@dataclass
class UserMapping:
    """Represents a complete user ID mapping"""
    hashed_id: str
    encrypted_original_id: str
    public_uuid: str
    key_version: str
    pseudo_name: str


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
    
    def generate_public_uuid(self) -> uuid.UUID:
        """
        Generate a random UUID for external use.
        
        This is Layer 2: the EXTERNAL identifier in handed-out datasets.
        
        Returns:
            Random UUID as string
        """
        return uuid.uuid4()
    
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
            public_uuid=str(public_uuid),
            key_version=self.key_version,
            pseudo_name=generate(public_uuid)
        )
    
    def prepare_mappings_for_db(
        self,
        user_ids: list[str],
        user_data: list[Optional[dict]]
    ) -> list[AnonymizeModel]:
        """
        Prepare user mappings for database insertion.

        # todo check an evaluate
        Returns tuples ready for DatabaseOperations.add_mappings().
        
        Args:
            user_ids: List of user IDs to process
            user_data: List of optional metadata dicts (same length)
            
        Returns:
            List of AnonymizeModel
                     
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
            pseudo_name = generate(public_uuid)
            
            # Encrypt user data if present
            encrypted_data = None
            if _user_data:
                encrypted_data = self.encrypt(json.dumps(_user_data))

            mappings.append(AnonymizeModel(
                user_id_hash=SecretStr(hashed_id),
                encrypted_user_id=SecretStr(encrypted_user_id),
                public_id=public_uuid,
                encrypted_data=encrypted_data,
                key_version=self.key_version,
                pseudo_name=pseudo_name
            ))
        
        return mappings
