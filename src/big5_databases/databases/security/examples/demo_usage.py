"""
Demo script showing how to use the new anonymization database functions.

This script demonstrates:
1. Creating an anonymization database from an existing platform database
2. Processing posts to extract and anonymize user data
3. Protecting sensitive content in the source database
"""

from pathlib import Path

from tools.env_root import root
from big5_databases.databases.platform_db_mgmt import PlatformDB
from big5_databases.databases.security import (
    init_anon_db, process_db, platform_user_data_jsonpath,
    DecryptAuditor, AnonymizationAuditor, quick_audit,
    ProtectionMarker, EnvelopeEncryption, EnvelopeData
)
from big5_databases.databases.security.core.main import x_process_database


def demo_anonymization_workflow():
    """
    Demonstrate the complete anonymization workflow.
    """
    print("=" * 70)
    print("ANONYMIZATION WORKFLOW DEMONSTRATION")
    print("=" * 70)

    # Use actual test data paths
    base_path = root() / "test_data" / "security"
    source_db_path = base_path / "twitter_phase1.sqlite"  # Working source database
    anon_db_path = base_path / "demo_twitter_anon.anon.sqlite"  # New anonymization database

    try:
        # Check if source database exists
        if not source_db_path.exists():
            print(f"❌ Source database not found: {source_db_path}")
            print(f"   Run the main test first: python -m big5_databases.databases.security.core.main")
            return False

        # Remove old demo database if it exists
        if anon_db_path.exists():
            anon_db_path.unlink()
            print(f"   Removed old demo database: {anon_db_path}")

        # Step 1: Load existing platform database
        print("\n📂 Step 1: Loading existing platform database...")
        source_db = PlatformDB.sqlite_db_from_path(
            platform="twitter",
            path=source_db_path,
            table_type="posts"  # This is the default
        )
        print(f"   ✅ Loaded {source_db.platform} database from {source_db_path}")

        # Step 2: Create anonymization database
        print("\n🔧 Step 2: Creating anonymization database...")
        anon_db = init_anon_db(source_db, anon_db_path)
        print(f"   ✅ Created anonymization database at {anon_db_path}")

        # Step 3: Process posts and anonymize
        print("\n🔐 Step 3: Processing posts for anonymization...")
        stats = process_db(
            source_db=source_db,
            anon_db=anon_db,
            batch_size=500,  # Process 500 posts at a time
            protect_content=True  # Replace user_ids with "<PROTECTED>"
        )

        # Step 4: Show results
        print("\n📈 Step 4: Anonymization Results:")
        print(f"   Posts processed: {stats['processed']}")
        print(f"   Users anonymized: {stats['anonymized']}")
        print(f"   Posts protected: {stats['protected']}")
        print(f"   Errors: {stats['errors']}")

        if stats['anonymized'] > 0:
            print(f"\n✅ Successfully anonymized {stats['anonymized']} users!")
            print(f"   📊 Anonymization rate: {stats['anonymized']/max(stats['processed'],1)*100:.1f}%")

        if stats['errors'] > 0:
            print(f"\n⚠️  Encountered {stats['errors']} errors during processing")

        print("\n" + "=" * 70)
        print("WORKFLOW COMPLETED SUCCESSFULLY")
        print("=" * 70)

        return True

    except Exception as e:
        print(f"\n❌ Error in anonymization workflow: {e}")
        print("\nCommon issues:")
        print("  - Source database file doesn't exist")
        print("  - Insufficient permissions to create new database")
        print("  - Missing environment variables for encryption keys")
        print("  - Platform not supported (check exec_db_fixes.py)")
        return False


def demo_audit_functionality():
    """
    Demonstrate the audit and verification functionality.
    """
    print("\n" + "=" * 70)
    print("AUDIT FUNCTIONALITY DEMONSTRATION")
    print("=" * 70)

    try:
        # Use the same paths as main demo - files are actually in current directory when run
        base_path = root() / "test_data" / "security"
        source_db_path = base_path / "twitter_phase1.sqlite"
        anon_db_path = base_path / "demo_twitter_anon.anon.sqlite"
        env_file_path = base_path / "test_private_decr.env"  # Has private key for audit

        # Check if files are in current directory (when run from different location)
        if not source_db_path.exists():
            source_db_path = Path("twitter_phase1.sqlite")
            anon_db_path = Path("demo_twitter_anon.anon.sqlite")
            env_file_path = base_path / "test_private_decr.env"  # This should still be in test_data

        # Check if required files exist
        if not source_db_path.exists():
            print(f"❌ Source database not found: {source_db_path}")
            return False

        if not anon_db_path.exists():
            print(f"❌ Anonymization database not found: {anon_db_path}")
            print("   Run the anonymization workflow first")
            return False

        if not env_file_path.exists():
            print(f"❌ Private key environment file not found: {env_file_path}")
            return False

        print(f"\n🔍 Step 1: Basic Anonymization Audit...")

        # Load databases for audit
        source_db = PlatformDB.sqlite_db_from_path("twitter", source_db_path, table_type="posts")

        # Perform quick audit without decryption
        print(f"   Running quick audit on anonymization database...")
        try:
            audit_result = quick_audit(
                anonymized_db=str(source_db_path),  # Use protected database
                original_db=str(source_db_path)     # Same source for demo
            )

            print(f"   ✅ Quick audit completed:")
            print(f"      - Posts analyzed: {audit_result.posts_analyzed}")
            print(f"      - Anonymization records found: {audit_result.anonymization_records}")
            print(f"      - Issues detected: {audit_result.issues}")
        except Exception as e:
            print(f"   ⚠️  Quick audit encountered issue: {e}")
            print(f"   Continuing with other audit methods...")

        print(f"\n🔐 Step 2: Decrypt Audit with Sample UUIDs...")

        # Get some UUIDs from the anonymization database for testing
        anon_db = PlatformDB.sqlite_db_from_path("twitter", anon_db_path, table_type="anon")
        with anon_db.get_session() as session:
            from big5_databases.databases.db_models import DBAnonymize
            sample_records = session.query(DBAnonymize).limit(3).all()
            sample_uuids = [record.public_id for record in sample_records]

        if sample_uuids:
            print(f"   Testing with {len(sample_uuids)} sample UUIDs...")

            # Initialize decrypt auditor
            decrypt_auditor = DecryptAuditor(env_file_path)

            # Perform decryption audit
            decrypt_result = decrypt_auditor.decrypt_and_verify(
                protected_db=str(source_db_path),
                anon_db=str(anon_db_path),
                original_db=str(source_db_path),  # Same as protected in this demo
                uuids=sample_uuids[:2]  # Test with first 2 UUIDs
            )

            print(f"   ✅ Decrypt audit completed:")
            print(f"      - UUIDs requested: {decrypt_result.total_requested}")
            print(f"      - Successfully decrypted: {decrypt_result.successfully_decrypted}")
            print(f"      - Verification passed: {decrypt_result.verification_passed}")
            print(f"      - Errors: {len(decrypt_result.errors)}")

            if decrypt_result.decrypted_data:
                sample_data = decrypt_result.decrypted_data[0]
                print(f"      - Sample decrypted user: {sample_data.get('original_user_id', 'N/A')[:20]}...")
        else:
            print(f"   ❌ No UUIDs found in anonymization database")

        print(f"\n🔎 Step 3: Advanced Anonymization Analysis...")

        # Use AnonymizationAuditor for more detailed analysis
        auditor = AnonymizationAuditor()

        # Analyze anonymization quality
        analysis_result = auditor.audit_database(
            anonymized_db=str(source_db_path),  # The protected database
            original_db=str(source_db_path),    # Same source (demo limitation)
        )

        print(f"   ✅ Advanced analysis completed:")
        print(f"      - Posts analyzed: {analysis_result.total_posts_analyzed}")
        print(f"      - Anonymized posts found: {analysis_result.anonymized_posts_found}")
        print(f"      - Protected field frequency: {len(analysis_result.protected_field_frequency)}")
        print(f"      - Examples found: {len(analysis_result.examples)}")
        print(f"      - Decryption attempted: {analysis_result.decryption_attempted}")

        print(f"\n" + "=" * 70)
        print("AUDIT DEMONSTRATION COMPLETED SUCCESSFULLY!")
        print("=" * 70)

        return True

    except Exception as e:
        print(f"\n❌ Error in audit demonstration: {e}")
        print(f"\nCommon issues:")
        print(f"  - Private key environment file missing or incorrect")
        print(f"  - Anonymization database not created yet")
        print(f"  - Database file permissions issues")
        import traceback
        traceback.print_exc()
        return False


def demo_protection_and_encryption():
    """
    Demonstrate ProtectionMarker and EnvelopeEncryption functionality to improve coverage.
    """
    print("\n" + "=" * 70)
    print("PROTECTION MARKER & ENCRYPTION DEMONSTRATION")
    print("=" * 70)

    try:
        # Use the same paths as main demo
        base_path = root() / "test_data" / "security"
        source_db_path = base_path / "twitter_phase1.sqlite"

        # Check if files are in current directory (when run from different location)
        if not source_db_path.exists():
            source_db_path = Path("twitter_phase1.sqlite")

        # Check if required files exist
        if not source_db_path.exists():
            print(f"❌ Source database not found: {source_db_path}")
            return False

        print(f"\n🛡️  Step 1: ProtectionMarker Functionality Demo...")

        # Load database for protection marker demo
        source_db = PlatformDB.sqlite_db_from_path("twitter", source_db_path, table_type="posts")

        # Create protection marker
        marker = ProtectionMarker(source_db)

        # Get protection statistics
        print(f"   Getting protection statistics...")
        stats = marker.get_protection_statistics()
        print(f"   ✅ Protection Statistics:")
        print(f"      - Total posts: {stats['total_posts']}")
        print(f"      - Protected posts: {stats['protected_posts']}")
        print(f"      - Unprotected posts: {stats['unprotected_posts']}")
        print(f"      - Posts with protected content: {stats['posts_with_protected_content']}")

        # Get some unprotected posts to test with
        print(f"   Getting sample of unprotected posts...")
        with source_db.get_session() as session:
            unprotected_posts = marker.get_unprotected_posts(session, batch_size=5)
            print(f"   Found {len(unprotected_posts)} unprotected posts")

        # Test individual post protection checking
        if unprotected_posts:
            sample_post = unprotected_posts[0]
            is_protected_before = marker.is_post_protected(sample_post)
            print(f"   Sample post protection status (before): {is_protected_before}")

            # Mark the sample post as protected
            with source_db.get_session() as session:
                marker.mark_post_as_protected(sample_post, session)
                session.commit()

            is_protected_after = marker.is_post_protected(sample_post)
            print(f"   Sample post protection status (after): {is_protected_after}")

        print(f"\n🔐 Step 2: EnvelopeEncryption Functionality Demo...")

        # Generate test RSA keys for envelope encryption demo
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization

        print(f"   Generating test RSA keys...")
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()

        # Create envelope encryption instance
        envelope = EnvelopeEncryption(public_key, private_key, key_version="demo_v1")
        print(f"   ✅ EnvelopeEncryption instance created with key version: demo_v1")

        # Test encryption and decryption
        test_data = b"This is sensitive user data that needs encryption!"
        print(f"   Encrypting test data: {test_data.decode()[:30]}...")

        # Encrypt the data
        encrypted_envelope = envelope.encrypt(test_data)
        print(f"   ✅ Data encrypted successfully")
        print(f"      - Wrapped key length: {len(encrypted_envelope.wrapped_key_b64)} chars")
        print(f"      - Nonce length: {len(encrypted_envelope.nonce_b64)} chars")
        print(f"      - Ciphertext length: {len(encrypted_envelope.ciphertext_b64)} chars")
        print(f"      - Key version: {encrypted_envelope.key_version}")

        # Test JSON serialization
        json_data = encrypted_envelope.to_json()
        print(f"   JSON serialization: {len(json_data)} chars")

        # Test JSON deserialization
        deserialized_envelope = EnvelopeData.from_json(json_data)
        print(f"   ✅ JSON deserialization successful")

        # Decrypt the data
        decrypted_data = envelope.decrypt(deserialized_envelope)
        print(f"   ✅ Data decrypted successfully: {decrypted_data.decode()[:30]}...")

        # Verify data integrity
        if decrypted_data == test_data:
            print(f"   ✅ Data integrity verified - original and decrypted data match!")
        else:
            print(f"   ❌ Data integrity check failed!")

        print(f"\n🔎 Step 3: Advanced ProtectionMarker Features...")

        # Test database-wide protection marking (with small batch size for demo)
        print(f"   Running database protection marking analysis...")
        try:
            protection_stats = marker.process_database_protection_marking(batch_size=10)
            print(f"   ✅ Database protection analysis completed:")
            print(f"      - Posts processed: {protection_stats['processed']}")
            print(f"      - Posts marked: {protection_stats['marked']}")
            print(f"      - Already marked: {protection_stats['already_marked']}")
            print(f"      - Errors: {protection_stats['errors']}")
        except Exception as e:
            print(f"   ⚠️  Protection analysis had issues: {e}")

        print(f"\n" + "=" * 70)
        print("PROTECTION & ENCRYPTION DEMONSTRATION COMPLETED!")
        print("=" * 70)

        return True

    except Exception as e:
        print(f"\n❌ Error in protection/encryption demonstration: {e}")
        print(f"\nCommon issues:")
        print(f"  - Database file not accessible")
        print(f"  - Cryptography library issues")
        import traceback
        traceback.print_exc()
        return False


def show_platform_patterns():
    """
    Show the supported platforms and their JSONPath patterns.
    """
    print("\n" + "=" * 70)
    print("SUPPORTED PLATFORM PATTERNS")
    print("=" * 70)


    platforms = ["twitter", "instagram", "youtube", "tiktok"]

    for platform in platforms:
        try:
            user_id_path, metadata_paths = platform_user_data_jsonpath(platform)
            print(f"\n📱 {platform.upper()}:")
            print(f"   User ID: {user_id_path}")
            if metadata_paths:
                print(f"   Metadata: {metadata_paths}")
            else:
                print(f"   Metadata: None")
        except Exception as e:
            print(f"\n📱 {platform.upper()}: Error - {e}")


def show_database_structure():
    """
    Show the structure of anonymization databases.
    """
    print("\n" + "=" * 70)
    print("ANONYMIZATION DATABASE STRUCTURE")
    print("=" * 70)

    print("\n🗃️  Table: anonymize")
    print("   - user_id_hash: HMAC hash of original user_id (for lookups)")
    print("   - encrypted_user_id: RSA encrypted original user_id")
    print("   - public_id: Random UUID for external datasets")
    print("   - encrypted_data: RSA encrypted user metadata (JSON)")
    print("   - key_version: Version of encryption keys used")

    print("\n🗃️  Table: database_stats")
    print("   - task_counts: Statistics about collection tasks")
    print("   - post_count: Total number of posts processed")
    print("   - last_task_change: When tasks were last modified")
    print("   - last_calculated: When stats were last updated")

    print("\n🔐 Security Features:")
    print("   ✅ Two-layer anonymization (hash + UUID)")
    print("   ✅ Envelope encryption (AES-GCM + RSA)")
    print("   ✅ Key rotation support")
    print("   ✅ Audit trail logging")
    print("   ✅ Content protection in source database")


if __name__ == "__main__":
    print("🔐 Anonymization Database Demo")
    print("==============================")

    # Show supported patterns
    show_platform_patterns()

    # Show database structure
    show_database_structure()


    print("\n💡 To run the anonymization workflow:")
    print("   1. Make sure test data is available (run: python -m big5_databases.databases.security.core.main)")
    print("   2. Uncomment the demo_anonymization_workflow() call below")
    print("   3. Run this script")

    # Run the actual workflow demonstration
    demo_anonymization_workflow()

    # Run audit functionality demonstration
    demo_audit_functionality()

    # Run protection marker and encryption demonstration
    demo_protection_and_encryption()

    # Run the main test to ensure fresh data
    x_process_database()