# User Anonymization Security Module

## Overview

Production-ready two-layer user anonymization system for social media data with audit capabilities.

## Core Modules

- **secure_config.py** - Configuration management with secret protection
- **envelope_encryption.py** - Hybrid AES-GCM + RSA encryption
- **secure_user_id_manager.py** - User anonymization (HMAC hashing + encryption)
- **db_operations.py** - Database operations for anonymization mappings
- **decryption_manager.py** - Audit operations with private key access
- **audit_manager.py** - Anonymization verification and analysis
- **decrypt_audit.py** - UUID-based decryption and verification
- **main.py** - High-level database processing function

## Quick Usage

### Database Anonymization
```python
from main import process_database

stats = process_database(
    source_db_path_or_name="twitter_posts.sqlite",
    anon_db_path="twitter_anonymized.anon.sqlite",
    env_file_path="anonymization_keys.env"
)
```

### Audit Verification
```python
from audit_manager import quick_audit

result = quick_audit(
    anonymized_db="ANON_TEST_twitter_phase1.sqlite",
    original_db="twitter_phase1.sqlite"
)
```

### Decrypt Specific UUIDs
```python
from decrypt_audit import DecryptAuditor

auditor = DecryptAuditor("private_key.env")
result = auditor.decrypt_and_verify(
    protected_db="anonymized.sqlite",
    anon_db="mappings.anon.sqlite",
    original_db="original.sqlite",
    uuids=["uuid1", "uuid2"]
)
```

## Key Features

- **Two-layer security**: HMAC hashing + RSA encryption
- **Audit trail**: All decryption operations logged
- **Batch processing**: Memory-efficient for large datasets
- **UUID verification**: Decrypt and verify specific anonymized users
- **Content protection**: `<PROTECTED>` markers in anonymized data

## Usage Notes

- **Run from project root**: All modules use clean relative imports and should be run from the `big5_databases` project root directory
- **Import directly**: For standalone usage, import modules directly (e.g., `from src.big5_databases.databases.security.protection_marker import ProtectionMarker`)
- **Environment required**: Anonymization requires environment variables for crypto keys

## Required Environment Variables

```bash
HMAC_KEY="base64_encoded_key"
PUBLIC_KEY_PEM="-----BEGIN PUBLIC KEY-----..."
PRIVATE_KEY_PEM="-----BEGIN PRIVATE KEY-----..."  # Audit only
KEY_VERSION="v1"
```