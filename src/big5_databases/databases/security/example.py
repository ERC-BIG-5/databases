"""
Complete Integration Example - Separated Architecture

ARCHITECTURE:
- SecureUserIDManager: Crypto operations only (no DB)
- DatabaseOperations: Database operations only (no crypto)
- DecryptionManager: Audit operations only (with private key)

BENEFITS:
- Private key NOT needed for regular batch jobs
- Clear separation of concerns
- Better security (reduced attack surface)
- Easier testing and maintenance
"""

import os
import base64
import logging
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

# Import our separated modules
from secure_config import SecurityConfig
from secure_user_id_manager import SecureUserIDManager
from db_operations import DatabaseOperations, DBAnonymize
from decryption_manager import DecryptionManager


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# Mock database (same as before)
class MockDatabase:
    """Simulated database for demonstration"""
    
    def __init__(self):
        self.data = {}
    
    class MockQuery:
        def __init__(self, session, model=None, columns=None):
            self.session = session
            self.columns = columns
            self._filter_hashes = []
            self._filter_uuid = None
        
        def filter(self, condition):
            # Store filter info for later use
            return self
        
        def with_for_update(self):
            return self
        
        def all(self):
            result = []
            for hash_, data in self.session.db.data.items():
                if self.columns:
                    result.append((data['user_id_hash'], data['public_id']))
                else:
                    result.append(DBAnonymize(**data))
            return result
        
        def first(self):
            for data in self.session.db.data.values():
                if self.columns:
                    return (data['user_id_hash'], data['public_id'])
                return DBAnonymize(**data)
            return None
    
    class Session:
        def __init__(self, db):
            self.db = db
        
        def __enter__(self):
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
        
        def query(self, *args):
            if len(args) > 1:
                return MockDatabase.MockQuery(self, columns=args)
            return MockDatabase.MockQuery(self, model=args[0])
        
        def add(self, entry):
            self.db.data[entry.user_id_hash] = {
                'user_id_hash': entry.user_id_hash,
                'encrypted_user_id': entry.encrypted_user_id,
                'public_id': entry.public_id,
                'encrypted_data': entry.encrypted_data,
                'key_version': entry.key_version
            }
        
        def commit(self):
            pass
        
        def rollback(self):
            pass
    
    def get_session(self):
        return self.Session(self)


def setup_test_environment():
    """Set up test environment with RSA keys"""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ).decode()
    
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()
    
    os.environ["HMAC_KEY"] = base64.b64encode(os.urandom(32)).decode()
    os.environ["PUBLIC_KEY_PEM"] = public_pem
    os.environ["PRIVATE_KEY_PEM"] = private_pem
    os.environ["KEY_VERSION"] = "v2"


def demonstrate_regular_batch_job():
    """
    SCENARIO 1: Regular daily batch job
    - Does NOT need private key
    - Only anonymizes users
    - This is your 2-hour daily job
    """
    print("=" * 70)
    print("SCENARIO 1: REGULAR BATCH JOB (No Private Key)")
    print("=" * 70)
    
    # Initialize WITHOUT private key
    print("\n📋 Step 1: Initialize (NO private key loaded)")
    manager = SecureUserIDManager.from_env(load_private_key=False)
    db = MockDatabase()
    db_ops = DatabaseOperations(db)
    print("   ✅ Manager ready (private key NOT loaded)")
    print("   ✅ Reduced attack surface!")
    
    # Process users
    print("\n👥 Step 2: Process Users")
    users = [
        ("@alice", {"role": "researcher", "department": "AI"}),
        ("@bob", {"role": "engineer", "department": "Backend"}),
        ("@charlie", {"role": "scientist", "department": "ML"}),
    ]
    
    user_ids = [u[0] for u in users]
    user_data = [u[1] for u in users]
    
    # Prepare mappings (crypto operations)
    mappings = manager.prepare_mappings_for_db(user_ids, user_data)
    print(f"   ✅ Prepared {len(mappings)} mappings")
    
    # Save to database (DB operations)
    hash_to_uuid = db_ops.add_mappings(mappings)
    print(f"   ✅ Saved to database")
    
    for user_id in user_ids:
        hashed = manager.create_hmac_hash(user_id)
        uuid = hash_to_uuid[hashed]
        print(f"      {user_id:15} → {uuid}")
    
    # Internal storage
    print("\n📊 Step 3: Internal Storage (Hashed IDs)")
    internal_posts = []
    for i, user_id in enumerate(["@alice", "@bob", "@alice"]):
        post = {
            "hashed_id": manager.create_hmac_hash(user_id),
            "content": f"Post {i+1} content",
            "likes": (i+1) * 10
        }
        internal_posts.append(post)
        print(f"   {post['hashed_id'][:20]}... | {post['likes']} likes")
    
    # Export for external use
    print("\n📤 Step 4: Export Dataset (Public UUIDs Only)")
    external_dataset = []
    for post in internal_posts:
        uuid = hash_to_uuid[post["hashed_id"]]
        external_dataset.append({
            "public_uuid": uuid,
            "content": post["content"],
            "likes": post["likes"]
        })
        print(f"   {uuid} | {post['likes']} likes")
    
    print("\n✅ Batch job complete - no private key was needed!")
    return db, db_ops, hash_to_uuid


def demonstrate_audit_operation(db, db_ops, hash_to_uuid):
    """
    SCENARIO 2: Authorized audit/investigation
    - DOES need private key
    - Separate process from batch job
    - Used sparingly, with authorization
    """
    print("\n" + "=" * 70)
    print("SCENARIO 2: AUDIT OPERATION (With Private Key)")
    print("=" * 70)
    
    print("\n⚠️  WARNING: Loading private key for authorized investigation")
    print("   Authorized by: security_team@company.com")
    print("   Reason: Compliance investigation #12345")
    
    # Initialize decryption manager (WITH private key)
    decrypt_manager = DecryptionManager.from_env(
        db_ops=db_ops,
        authorized_by="security_team@company.com"
    )
    
    # Get a UUID to investigate
    test_uuid = list(hash_to_uuid.values())[0]
    
    print(f"\n🔍 Investigating UUID: {test_uuid}")
    
    # Decrypt original ID
    original_id = decrypt_manager.get_original_id_by_uuid(
        public_uuid=test_uuid,
        reason="Compliance investigation #12345"
    )
    
    print(f"   ✅ Original ID: {original_id}")
    
    # Get metadata
    metadata = decrypt_manager.get_user_metadata(
        public_uuid=test_uuid,
        reason="Compliance investigation #12345"
    )
    
    if metadata:
        print(f"   ✅ Metadata: {metadata}")
    
    print("\n✅ Audit complete - all operations logged")


def demonstrate_security_benefits():
    """Show the security benefits of separated architecture"""
    print("\n" + "=" * 70)
    print("SECURITY BENEFITS")
    print("=" * 70)
    
    print("\n📊 Traditional Approach:")
    print("   ❌ Private key loaded in daily batch job")
    print("   ❌ 2 hours of exposure every day")
    print("   ❌ 730 hours/year of private key in memory")
    
    print("\n✅ New Separated Approach:")
    print("   ✅ Private key NOT loaded in daily batch job")
    print("   ✅ Only loaded during rare audit operations")
    print("   ✅ Maybe 10 hours/year of private key exposure")
    print("   ✅ 98.6% reduction in attack surface!")
    
    print("\n🔒 Attack Scenarios:")
    print("\n   Scenario: Attacker compromises batch job server")
    print("   Traditional: ❌ Gets private key, can decrypt everything")
    print("   New approach: ✅ No private key available!")
    
    print("\n   Scenario: Attacker gets HMAC key")
    print("   Both: ✅ External data still anonymous (two-layer design)")
    
    print("\n   Scenario: Need to investigate user")
    print("   Both: ✅ Use DecryptionManager (authorized, logged)")


def main():
    """Run complete demonstration"""
    setup_test_environment()
    
    print("=" * 70)
    print("SEPARATED ARCHITECTURE DEMONSTRATION")
    print("=" * 70)
    print("\nNEW MODULES:")
    print("  • SecureUserIDManager: Crypto only (no DB, no private key)")
    print("  • DatabaseOperations: DB only (no crypto)")
    print("  • DecryptionManager: Audit only (with private key, logged)")
    
    # Demonstrate regular batch job (no private key)
    db, db_ops, hash_to_uuid = demonstrate_regular_batch_job()
    
    # Demonstrate audit operation (with private key)
    demonstrate_audit_operation(db, db_ops, hash_to_uuid)
    
    # Show security benefits
    demonstrate_security_benefits()
    
    print("\n" + "=" * 70)
    print("✅ ARCHITECTURE COMPLETE")
    print("=" * 70)
    print("\nREADY FOR PRODUCTION:")
    print("  1. Regular batch job: No private key needed")
    print("  2. Audit operations: Separate, logged, authorized")
    print("  3. Clear separation of concerns")
    print("  4. 98%+ reduction in private key exposure")


if __name__ == "__main__":
    main()
