# Security module for user anonymization with organized subpackages

# Core functionality - main anonymization features
from .core import (
    SecurityConfig,
    EnvelopeEncryption,
    EnvelopeData,
    SecureUserIDManager,
    UserMapping,
    DatabaseOperations,
    init_anon_db,
    process_db,
    protect_posts_batch,
    ProtectionMarker
)

# Audit functionality - verification and decryption
from .audit import (
    DecryptionManager,
    AnonymizationAuditor,
    AuditConfig,
    AuditResult,
    quick_audit,
    DecryptAuditor,
    DecryptAuditResult,
    decrypt_and_verify_uuids
)

# Utilities - platform support and extraction tools
from .utils import (
    JsonPathFieldExtractor,
    JsonPathContentProtector,
    platform_user_data_jsonpath
)

__all__ = [
    # Core functionality
    'SecurityConfig',
    'EnvelopeEncryption',
    'EnvelopeData',
    'SecureUserIDManager',
    'UserMapping',
    'DatabaseOperations',
    'init_anon_db',
    'process_db',
    'protect_posts_batch',
    'ProtectionMarker',
    # Audit functionality
    'DecryptionManager',
    'AnonymizationAuditor',
    'AuditConfig',
    'AuditResult',
    'quick_audit',
    'DecryptAuditor',
    'DecryptAuditResult',
    'decrypt_and_verify_uuids',

    # Utilities
    'JsonPathFieldExtractor',
    'JsonPathContentProtector',
    'platform_user_data_jsonpath'
]