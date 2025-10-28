"""
Secure Configuration Management with Secret Protection

This module provides a dataclass-based configuration loader that:
- Reads from environment variables
- Protects sensitive values from accidental exposure in logs/prints
- Validates required configuration on startup
"""

import os
from dataclasses import dataclass, field
from typing import Optional


class SecretStr:
    """
    A string wrapper that hides its value in repr() and str() to prevent
    accidental exposure in logs, error messages, or debug output.
    
    Similar to Pydantic's SecretStr but without the dependency.
    """
    
    def __init__(self, value: str):
        self._value = value
    
    def get_secret_value(self) -> str:
        """Get the actual secret value"""
        return self._value
    
    def __repr__(self) -> str:
        return "SecretStr('**********')"
    
    def __str__(self) -> str:
        return "**********"
    
    def __eq__(self, other) -> bool:
        if isinstance(other, SecretStr):
            return self._value == other._value
        return False
    
    def __bool__(self) -> bool:
        """Allow truthiness checks"""
        return bool(self._value)


@dataclass
class SecurityConfig:
    """
    Configuration for the two-layer anonymization system.
    
    All sensitive values are wrapped in SecretStr to prevent accidental exposure.
    Load from environment variables with validation.
    """
    
    # HMAC key for creating hashed_id (Layer 1)
    hmac_key: SecretStr
    
    # RSA keys for encrypting original IDs
    public_key_pem: SecretStr
    private_key_pem: Optional[SecretStr] = None
    
    # Key version for rotation support
    key_version: str = "v1"
    
    # Database connection string (also sensitive)
    database_url: SecretStr = field(default_factory=lambda: SecretStr(""))
    
    @classmethod
    def from_env(cls) -> "SecurityConfig":
        """
        Load configuration from environment variables.
        
        Required environment variables:
        - HMAC_KEY: Secret key for HMAC hashing
        - PUBLIC_KEY_PEM: RSA public key in PEM format
        - PRIVATE_KEY_PEM: RSA private key in PEM format
        
        Optional:
        - KEY_VERSION: Version identifier for key rotation (default: "v1")
        - DATABASE_URL: Database connection string
        
        Raises:
            ValueError: If required environment variables are missing
        """
        # Required variables
        hmac_key = os.getenv("HMAC_KEY")
        public_key_pem = os.getenv("PUBLIC_KEY_PEM")
        private_key_pem = os.getenv("PRIVATE_KEY_PEM")
        
        # Validate required variables (private key is optional)
        missing = []
        if not hmac_key:
            missing.append("HMAC_KEY")
        if not public_key_pem:
            missing.append("PUBLIC_KEY_PEM")

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                "Please set these variables before starting the application."
            )
        
        # Optional variables with defaults
        key_version = os.getenv("KEY_VERSION", "v1")
        database_url = os.getenv("DATABASE_URL", "")
        
        return cls(
            hmac_key=SecretStr(hmac_key or ""),
            public_key_pem=SecretStr(public_key_pem or ""),
            private_key_pem=SecretStr(private_key_pem) if private_key_pem else None,
            key_version=key_version,
            database_url=SecretStr(database_url)
        )
    
    def __repr__(self) -> str:
        """Custom repr that hides all secret values"""
        return (
            f"SecurityConfig("
            f"hmac_key=SecretStr('**********'), "
            f"public_key_pem=SecretStr('**********'), "
            f"private_key_pem=SecretStr('**********'), "
            f"key_version='{self.key_version}', "
            f"database_url=SecretStr('**********')"
            f")"
        )


# Example usage and testing
if __name__ == "__main__":
    print("=" * 70)
    print("SECURE CONFIGURATION DEMO")
    print("=" * 70)
    
    # Simulate setting environment variables
    os.environ["HMAC_KEY"] = "super-secret-hmac-key-12345"
    os.environ["PUBLIC_KEY_PEM"] = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBg..."
    os.environ["PRIVATE_KEY_PEM"] = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADA..."
    os.environ["KEY_VERSION"] = "v2"
    os.environ["DATABASE_URL"] = "postgresql://user:pass@localhost/db"
    
    # Load config
    config = SecurityConfig.from_env()
    
    print("\n✅ Configuration loaded successfully!\n")
    
    # Demonstrate secret protection
    print("🔒 SAFETY DEMONSTRATION:\n")
    
    print(f"Config object: {config}")
    print(f"HMAC key field: {config.hmac_key}")
    print(f"Database URL: {config.database_url}")
    
    print("\n❌ These would leak secrets in normal strings:")
    print(f"   HMAC_KEY env var: {os.getenv('HMAC_KEY')}")
    
    print("\n✅ To access actual values (in controlled contexts only):")
    print(f"   Actual HMAC key: {config.hmac_key.get_secret_value()[:20]}...")
    print(f"   Key version (not secret): {config.key_version}")
    
    # Test missing required variable
    print("\n" + "=" * 70)
    print("VALIDATION TEST")
    print("=" * 70)
    
    del os.environ["HMAC_KEY"]
    try:
        SecurityConfig.from_env()
    except ValueError as e:
        print(f"\n✅ Properly caught missing config:\n   {e}")
    
    print("\n✓ Configuration system is production-ready!")
