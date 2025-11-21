# User Anonymization Security Module

Production-ready two-layer user anonymization system for social media data with HMAC hashing + RSA envelope encryption.

```mermaid
graph LR
    post --> identify(Identify user-id field and sensitive fields)
    identify --> user_id & user-data
    user_id --> hash & encrypt
    user-data --> encrypt
    subgraph keys [keys]
    hmac_key{{ROTATABLE KEY}}
    public_key{{PUBLIC KEY}}
    private_key{{PRIVATE KEY}}
    end
    hmac_key -.-> hash
    public_key -.-> encrypt
    hash --> in_db{In the db?}
    in_db -- no --> new_uuid
    in_db -- yes --> RETURN_UUID((END\nReturn UUID))
    hash ==> insert[(insert into db)]
    encrypt ==> insert
    new_uuid ==> insert
    hmac_key ==version==> insert
    insert --> RETURN_UUID
    private_key -.-> audit
    insert --> audit

```

**Data Flow:** Posts are processed to extract user IDs and metadata. User IDs are hashed (HMAC) and checked against the anonymization database. If not found, a new UUID is generated and all data (hash, encrypted user ID, encrypted metadata, UUID, key version) is stored. The UUID is returned to replace the original user ID in the dataset.

## Quick Usage

### Database Anonymization
```python
from big5_databases.databases.security import process_database

stats = process_database(
    source_db_path_or_name="twitter_posts.sqlite",
    anon_db_path="twitter_anonymized.anon.sqlite",
    env_file_path="anonymization_keys.env"
)
```

### Universal Post Protection Helper

The `protect_posts_batch()` function is the core helper for protecting posts. It can be called from anywhere in the databases package when inserting new posts or processing existing databases.

**Key Features:**
- Extracts user IDs using platform-specific JSONPath patterns
- Checks for existing mappings and creates new ones only if needed
- Protects content by replacing user IDs with UUIDs
- Marks posts as protected to avoid reprocessing
- Works with both DBPost and PostModel objects

```python
from big5_databases.databases.security import protect_posts_batch
from big5_databases.databases.security.core.secure_user_id_manager import SecureUserIDManager
from big5_databases.databases.security.core.db_operations import get_anon_db

# Setup (one-time initialization)
user_id_manager = SecureUserIDManager.from_env(load_private_key=False)
anon_db = get_anon_db("twitter_anon.sqlite", "twitter")

# Protect any batch of posts
posts = [...]  # List of DBPost or PostModel objects
stats = protect_posts_batch(
    posts=posts,
    platform="twitter",
    user_id_manager=user_id_manager,
    anon_db=anon_db
)

# Returns statistics
print(f"Processed: {stats['processed']}")
print(f"Protected: {stats['protected']}")
print(f"New users: {stats['users_anonymized']}")
print(f"Skipped: {stats['skipped']}")
```

### Batch Processing New Posts
```python
# Workflow: sourcedb → new_posts → ANONYMIZE → safe_submit → end
from big5_databases.databases.platform_db_mgmt import PlatformDB
from big5_databases.databases.security import protect_posts_batch
from big5_databases.databases.security.core.secure_user_id_manager import SecureUserIDManager
from big5_databases.databases.security.core.db_operations import get_anon_db

source_db = PlatformDB.sqlite_db_from_path("twitter", "posts.sqlite", table_type="posts")
new_posts = [...]  # List of DBPost and/or PostModel objects

# 1. Setup anonymization
anon_db = get_anon_db("twitter_anon.sqlite", "twitter")
user_id_manager = SecureUserIDManager.from_env(load_private_key=False)

# 2. ANONYMIZE posts FIRST using universal helper
stats = protect_posts_batch(
    posts=new_posts,
    platform="twitter",
    user_id_manager=user_id_manager,
    anon_db=anon_db
)
print(f"Protected {stats['protected']} posts, {stats['users_anonymized']} new users")

# 3. Submit already-anonymized posts
submitted_posts = source_db.safe_submit_posts(new_posts)
```

### Audit & Verification
```python
from big5_databases.databases.security.audit import quick_audit, DecryptAuditor

# Quick audit
result = quick_audit(anonymized_db="anon.sqlite", original_db="original.sqlite")

# UUID-based investigation (requires private key)
auditor = DecryptAuditor("investigation_keys.env")
decrypt_result = auditor.decrypt_and_verify(
    protected_db="protected.sqlite",
    anon_db="mappings.anon.sqlite",
    original_db="original.sqlite",
    uuids=["uuid-list"]
)
```

## Test & Demo

```bash
# Run complete test with batch processing coverage (main entry point)
python -m big5_databases.databases.security.core.main
# ✅ Processes 321 posts with full anonymization
# ✅ Tests mixed DBPost/PostModel batch processing
# ✅ Generates pseudo names for UUIDs
# ✅ Covers all security components

# Individual demonstrations (no main block - use functions directly)
# See examples/demo_usage.py for function examples
```

## JSONPath Patterns (Platform-Specific)

| Platform | Primary User ID | Additional Metadata |
|----------|----------------|-------------------|
| **Twitter** | `user.id_str` | `user.id`, `user.url`, `user.username`, `user.displayname` |
| **Instagram** | `post_owner.id` | `post_owner.type`, `post_owner.name`, `post_owner.username` |
| **YouTube** | `snippet.channelId` | `snippet.channelTitle` |
| **TikTok** | `username` | *(none)* |

*Update paths in: `src/big5_databases/databases/security/utils/exec_db_fixes.py`*

## Environment Setup

Create `keys.env` file:
```bash
# Required for anonymization
HMAC_KEY="base64_encoded_secret_key"
PUBLIC_KEY_PEM="-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...
-----END PUBLIC KEY-----"

# Required for audit/investigation only
PRIVATE_KEY_PEM="-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC...
-----END PRIVATE KEY-----"

KEY_VERSION="v1"  # Optional
```

### Key Generation
```bash
# RSA keys
openssl genpkey -algorithm RSA -out private_key.pem -pkcs8 -aes256
openssl rsa -pubout -in private_key.pem -out public_key.pem

# HMAC key
python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
```

## Architecture

### Security Layers
**Two-Layer Security:**
1. **HMAC Layer**: Fast, deterministic hashing for user ID mapping
2. **RSA Layer**: Envelope encryption (AES-GCM + RSA key wrapping) for metadata

### Package Structure
- `core/` - Anonymization, encryption, database operations
  - **`protect_posts_batch()`** - Universal helper for post protection
  - `process_db()` - Batch processor for whole databases
  - `SecureUserIDManager` - Crypto operations (hash, encrypt, UUID generation)
  - `DatabaseOperations` - Anon DB mapping management
  - `ProtectionMarker` - Track protected posts
- `audit/` - Verification, decryption, investigation tools
- `utils/` - JSONPath extraction, platform patterns
- `examples/` - Demonstrations and usage examples

### Data Flow

```
┌─────────────────────────────────────────────────────────┐
│              protect_posts_batch()                      │
│            [Universal Helper Function]                  │
│                                                         │
│  Phase 1: Extract & Map                                │
│    • Extract user_ids via JSONPath                     │
│    • Create HMAC hashes                                │
│    • Build post-to-user mappings                       │
│                                                         │
│  Phase 2: Get/Create Mappings                          │
│    • Query anon DB for existing UUIDs                  │
│    • Create new mappings for new users                 │
│    • Insert to anon DB (transaction-safe)              │
│                                                         │
│  Phase 3: Protect Content                              │
│    • Replace user_ids with UUIDs                       │
│    • Mark sensitive fields as <PROTECTED>              │
│    • Mark posts as protected                           │
│                                                         │
└─────────────────────────────────────────────────────────┘
         ▲                                   ▲
         │                                   │
    ┌────┴────────┐                 ┌────────┴──────────┐
    │ process_db()│                 │ New Post Insertion│
    │             │                 │   (anywhere in    │
    │ Fetches     │                 │  databases pkg)   │
    │ batches     │                 │                   │
    │ from DB     │                 │  Your custom      │
    └─────────────┘                 │  insertion code   │
                                    └───────────────────┘
```

### Key Features
- **Universal Helper**: Single `protect_posts_batch()` function for all protection needs
- **Reusable**: Callable from anywhere in the databases package
- **Efficient**: Batch processing with existing mapping lookups
- **Mixed data type support**: Works with both DBPost and PostModel
- **Protection marker system**: Avoids duplicate processing
- **UUID-based audit trail**: With human-readable pseudo names
- **Platform-specific patterns**: JSONPath patterns for each platform
- **Transaction-safe**: Rollback support for database operations