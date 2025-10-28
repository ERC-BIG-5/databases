# User Anonymization Security Module

## Overview

Production-ready two-layer user anonymization system for social media data with audit capabilities. The module implements HMAC hashing combined with RSA envelope encryption to protect user identities while maintaining data utility for analysis. The package is organized into focused subpackages for maintainability and clear separation of concerns.

## Package Structure

```
security/
├── __init__.py                 # Main package exports
│
├── core/                       # Core anonymization functionality
│   ├── __init__.py
│   ├── secure_config.py        # Configuration & environment management
│   ├── envelope_encryption.py  # Hybrid RSA+AES encryption system
│   ├── secure_user_id_manager.py # User ID anonymization & mapping
│   ├── db_operations.py        # Database anonymization operations
│   ├── protection_marker.py    # Post protection status tracking
│   └── main.py                 # High-level processing workflows
│
├── audit/                      # Audit & verification tools
│   ├── __init__.py
│   ├── decryption_manager.py   # Investigation/audit decryption
│   ├── audit_manager.py        # Anonymization verification & analysis
│   └── decrypt_audit.py        # UUID-based audit & verification
│
├── utils/                      # Utilities & platform support
│   ├── __init__.py
│   ├── jsonpath_extractor.py   # JSONPath field extraction & protection
│   └── exec_db_fixes.py        # Platform-specific data patterns
│
└── examples/                   # Examples & documentation
    ├── __init__.py
    ├── demo_usage.py           # Usage demonstration scripts
    ├── example.py              # Example implementations
    ├── TODO.md                 # Development notes & roadmap
    └── enhancements.md         # Enhancement plans & ideas
```

## Quick Usage

### 1. Database Anonymization
```python
# Complete anonymization workflow - recommended approach
from big5_databases.databases.security import process_database

# Process entire database with anonymization
stats = process_database(
    source_db_path_or_name="twitter_posts.sqlite",
    anon_db_path="twitter_anonymized.anon.sqlite",
    env_file_path="anonymization_keys.env"
)

print(f"Processed: {stats['processed']}, Protected: {stats['protected']}")
```

### 2. Core Components (Advanced Usage)
```python
# Using individual components for custom workflows
from big5_databases.databases.security.core import (
    DatabaseOperations,
    SecureUserIDManager,
    ProtectionMarker,
    init_anon_db,
    process_db
)

# Initialize anonymization database
anon_db = init_anon_db("output.anon.sqlite", "source.sqlite")

# Process posts with custom configuration
stats = process_db(
    source_db_name="posts.sqlite",
    anon_db_path="output.anon.sqlite",
    env_file_path="keys.env",
    batch_size=500
)
```

### 3. Audit & Verification
```python
# Quick audit - compare anonymized vs original
from big5_databases.databases.security.audit import quick_audit

audit_result = quick_audit(
    anonymized_db="ANON_TEST_twitter_phase1.sqlite",
    original_db="twitter_phase1.sqlite"
)

print(f"Audit passed: {audit_result.passed}")
print(f"Issues found: {len(audit_result.issues)}")

# Detailed audit with custom configuration
from big5_databases.databases.security.audit import AnonymizationAuditor, AuditConfig

config = AuditConfig(check_user_anonymization=True, check_content_protection=True)
auditor = AnonymizationAuditor(config)
detailed_result = auditor.audit_database(
    anonymized_db="anonymized.sqlite",
    original_db="original.sqlite"
)
```

### 4. UUID-based Investigation (Authorized Personnel Only)
```python
# Decrypt specific users for investigation/audit purposes
from big5_databases.databases.security.audit import DecryptAuditor

# Initialize with private key environment file
auditor = DecryptAuditor("investigation_private_key.env")

# Decrypt and verify specific UUIDs
result = auditor.decrypt_and_verify(
    protected_db="anonymized_posts.sqlite",      # Database with <PROTECTED> markers
    anon_db="mappings.anon.sqlite",              # Encrypted UUID mappings
    original_db="original_posts.sqlite",        # Original data for verification
    uuids=["uuid-1234-5678", "uuid-abcd-efgh"]  # Specific UUIDs to investigate
)

# Review investigation results
for uuid, data in result.decrypted_data.items():
    print(f"UUID {uuid}: Original user_id = {data['original_user_id']}")
    if result.verification_results[uuid]['matches']:
        print(f"  ✅ Verification passed")
    else:
        print(f"  ❌ Verification failed")
```

## Key Features

### Security & Privacy
- **Two-layer anonymization**: HMAC hashing + RSA envelope encryption for robust user protection
- **Envelope encryption**: AES-GCM for data + RSA for key wrapping ensures maximum security
- **Content protection**: `<PROTECTED>` markers replace sensitive user data in anonymized datasets
- **Audit trail**: Comprehensive logging of all decryption and anonymization operations
- **Key versioning**: Support for cryptographic key rotation and version management

### Performance & Scalability
- **Batch processing**: Memory-efficient processing of large datasets with configurable batch sizes
- **Protection markers**: Skip already-processed posts to avoid duplicate work
- **Database optimization**: Efficient SQLite operations with transaction management
- **Progress tracking**: Real-time statistics on processing status and completion

### Audit & Verification
- **UUID-based investigation**: Decrypt and verify specific anonymized users for authorized investigations
- **Comprehensive auditing**: Compare anonymized vs original data to verify anonymization quality
- **Pattern analysis**: Detect anonymization patterns and ensure consistent protection across platforms
- **Verification workflows**: Automated verification of decryption results against original data

### Developer Experience
- **Clean package structure**: Organized subpackages for core, audit, utils, and examples
- **Type safety**: Full mypy validation with comprehensive type hints
- **Simple imports**: Clean relative imports throughout the package
- **Comprehensive documentation**: Examples and usage patterns for all functionality

## Technical Architecture

### Anonymization Process
1. **User ID Hashing**: HMAC-SHA256 with secret key creates consistent anonymous IDs
2. **Envelope Encryption**: User IDs encrypted with RSA public key, wrapped with AES-GCM
3. **Content Protection**: User-identifiable content replaced with `<PROTECTED>` markers
4. **Database Storage**: Encrypted mappings stored in `.anon.sqlite` databases
5. **Protection Tracking**: Post metadata tracks which posts have been processed

### Security Model
- **Public Key Operations**: Anonymization uses only public key (safe for production)
- **Private Key Operations**: Decryption requires private key (investigation/audit only)
- **Key Separation**: Anonymization and investigation keys can be managed separately
- **Audit Logging**: All private key operations logged for security compliance

## Usage Notes

### Environment & Setup
- **Project root execution**: Run all commands from `big5_databases` project root directory
- **Clean imports**: Use standard package imports (e.g., `from big5_databases.databases.security import ...`)
- **Environment files**: Crypto keys loaded from `.env` files for security
- **Database paths**: Support both absolute paths and database names from registry

### Best Practices
- **Batch processing**: Use appropriate batch sizes (100-1000) for memory efficiency
- **Protection checking**: Always check protection status to avoid duplicate processing
- **Audit verification**: Regularly audit anonymized data to ensure quality
- **Key management**: Rotate keys periodically and maintain version tracking

## Required Environment Variables

Create environment files (e.g., `anonymization_keys.env`) with:

```bash
# Required for all operations
HMAC_KEY="base64_encoded_secret_key_for_hashing"
PUBLIC_KEY_PEM="-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...
-----END PUBLIC KEY-----"

# Required for investigation/audit operations only
PRIVATE_KEY_PEM="-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC...
-----END PRIVATE KEY-----"

# Optional - defaults to "v1"
KEY_VERSION="v1"
```

### Key Generation
```bash
# Generate RSA key pair for envelope encryption
openssl genpkey -algorithm RSA -out private_key.pem -pkcs8 -aes256
openssl rsa -pubout -in private_key.pem -out public_key.pem

# Generate base64-encoded HMAC key
python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
```