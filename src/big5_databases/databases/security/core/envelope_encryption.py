"""
Envelope Encryption Implementation for Two-Layer Anonymization System

WHAT IS ENVELOPE ENCRYPTION?
Instead of encrypting data directly with RSA (which has size limits and is slow),
we use a hybrid approach:
1. Generate a random AES key
2. Encrypt the data with AES-GCM (fast, supports any size, authenticated)
3. Encrypt (wrap) the AES key with RSA
4. Store: {encrypted_data, wrapped_key, nonce, tag}

BENEFITS:
- No size limits on encrypted data
- Much faster than pure RSA
- Authenticated encryption (prevents tampering)
- Supports key rotation via key_version
"""

import json
import os
import base64
from typing import Dict
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding


@dataclass
class EnvelopeData:
    """
    Container for envelope-encrypted data.
    
    Attributes:
        wrapped_key_b64: RSA-encrypted AES key (base64)
        nonce_b64: AES-GCM nonce (base64)
        ciphertext_b64: AES-GCM encrypted data (base64)
        key_version: Version of the RSA key used for wrapping
    """
    wrapped_key_b64: str
    nonce_b64: str
    ciphertext_b64: str
    key_version: str = "v1"
    
    def to_json(self) -> str:
        """Serialize to JSON for database storage"""
        return json.dumps({
            "wrapped_key": self.wrapped_key_b64,
            "nonce": self.nonce_b64,
            "ciphertext": self.ciphertext_b64,
            "key_version": self.key_version
        })
    
    @classmethod
    def from_json(cls, json_str: str) -> "EnvelopeData":
        """Deserialize from JSON"""
        data = json.loads(json_str)
        return cls(
            wrapped_key_b64=data["wrapped_key"],
            nonce_b64=data["nonce"],
            ciphertext_b64=data["ciphertext"],
            key_version=data.get("key_version", "v1")
        )


class EnvelopeEncryption:
    """
    Handles envelope encryption: AES-GCM for data, RSA for key wrapping.
    """
    
    def __init__(self, public_key, private_key=None, key_version: str = "v1"):
        """
        Initialize envelope encryption.
        
        Args:
            public_key: RSA public key for wrapping AES keys
            private_key: RSA private key for unwrapping (optional, only needed for decryption)
            key_version: Version identifier for key rotation
        """
        self.public_key = public_key
        self.private_key = private_key
        self.key_version = key_version
    
    def encrypt(self, plaintext: bytes) -> EnvelopeData:
        """
        Encrypt data using envelope encryption.
        
        Process:
        1. Generate random AES-256 key
        2. Encrypt plaintext with AES-GCM
        3. Wrap AES key with RSA public key
        4. Return all components
        
        Args:
            plaintext: Raw bytes to encrypt
            
        Returns:
            EnvelopeData containing all encrypted components
        """
        if not self.public_key:
            raise ValueError("Public key required for encryption")
        
        # Step 1: Generate random AES key
        aes_key = AESGCM.generate_key(bit_length=256)
        aesgcm = AESGCM(aes_key)
        
        # Step 2: Encrypt data with AES-GCM
        nonce = os.urandom(12)  # 96-bit nonce for GCM
        ciphertext = aesgcm.encrypt(nonce, plaintext, associated_data=None)
        
        # Step 3: Wrap AES key with RSA
        wrapped_key = self.public_key.encrypt(
            aes_key,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        
        # Step 4: Package everything
        return EnvelopeData(
            wrapped_key_b64=base64.b64encode(wrapped_key).decode(),
            nonce_b64=base64.b64encode(nonce).decode(),
            ciphertext_b64=base64.b64encode(ciphertext).decode(),
            key_version=self.key_version
        )
    
    def decrypt(self, envelope: EnvelopeData) -> bytes:
        """
        Decrypt data from envelope encryption.
        
        Process:
        1. Unwrap AES key using RSA private key
        2. Decrypt data using AES-GCM
        3. Return plaintext
        
        Args:
            envelope: EnvelopeData containing encrypted components
            
        Returns:
            Decrypted plaintext bytes
        """
        if not self.private_key:
            raise ValueError("Private key required for decryption")
        
        # Step 1: Unwrap AES key
        wrapped_key = base64.b64decode(envelope.wrapped_key_b64)
        aes_key = self.private_key.decrypt(
            wrapped_key,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        
        # Step 2: Decrypt data
        aesgcm = AESGCM(aes_key)
        nonce = base64.b64decode(envelope.nonce_b64)
        ciphertext = base64.b64decode(envelope.ciphertext_b64)
        
        return aesgcm.decrypt(nonce, ciphertext, associated_data=None)
