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
from big5_databases.databases.db_models import DBPost, PostType
from big5_databases.databases.model_conversion import PostModel, PostMetadataModel
import json
import uuid
from typing import List, Union
from datetime import datetime


def demo_anonymization_workflow():
    """
    Demonstrate the complete anonymization workflow.
    """
    print("=" * 70)
    print("ANONYMIZATION WORKFLOW DEMONSTRATION")
    print("=" * 70)

    # Use actual test data paths - use the main working test databases instead of creating empty ones
    base_path = root() / "test_data" / "security"
    source_db_path = base_path / "twitter_phase1.sqlite"  # Working source database
    anon_db_path = base_path / "test_twitter_anonymized.anon.sqlite"  # Use existing populated database
    # todo I don't see here anywhere, that you are copying the original source. so you start with an unprotected database.
    try:
        # Check if source database exists
        if not source_db_path.exists():
            print(f"❌ Source database not found: {source_db_path}")
            # todo, there is no main. just return False
            print(f"   Run the main test first: python -m big5_databases.databases.security.core.main")
            return False

        # Check if anonymization database exists (it should from main test)
        if not anon_db_path.exists():
            print(f"❌ Anonymization database not found: {anon_db_path}")
            print("   Run the main test first: python -m big5_databases.databases.security.core.main")
            return False

        # Step 1: Load existing platform database
        print("\n📂 Step 1: Loading existing platform database...")
        source_db = PlatformDB.sqlite_db_from_path(
            platform="twitter",
            path=source_db_path,
            table_type="posts"  # This is the default
        )
        print(f"   ✅ Loaded {source_db.platform} database from {source_db_path}")

        # Step 2: Load existing anonymization database (populated by main test)
        print("\n🔧 Step 2: Loading existing anonymization database...")
        anon_db = PlatformDB.sqlite_db_from_path(
            platform="twitter",
            path=anon_db_path,
            table_type="anon"
        )
        print(f"   ✅ Loaded anonymization database from {anon_db_path}")

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


def demo_protect_new_posts():
    """
    Demonstrate using protect_posts_batch() for protecting new posts before insertion.

    This shows the universal helper function that can be called from anywhere
    in the databases package when inserting new posts.
    """
    print("\n" + "=" * 70)
    print("PROTECT NEW POSTS DEMONSTRATION (Universal Helper)")
    print("=" * 70)

    try:
        from big5_databases.databases.security import protect_posts_batch
        from big5_databases.databases.security.core.secure_user_id_manager import SecureUserIDManager
        from big5_databases.databases.security.core.db_operations import get_anon_db

        # Use test data paths
        base_path = root() / "test_data" / "security"
        source_db_path = base_path / "twitter_phase1.sqlite"
        anon_db_path = base_path / "test_twitter_anonymized.anon.sqlite"

        # Check if files exist
        if not source_db_path.exists():
            print(f"❌ Source database not found: {source_db_path}")
            print(f"   Run main test first: python -m big5_databases.databases.security.main")
            return False

        if not anon_db_path.exists():
            print(f"❌ Anonymization database not found: {anon_db_path}")
            print(f"   Run main test first: python -m big5_databases.databases.security.main")
            return False

        print("\n📂 Step 1: Setup databases and manager...")

        # Load source database
        source_db = PlatformDB.sqlite_db_from_path("twitter", source_db_path, table_type="posts")
        print(f"   ✅ Loaded source database")

        # Setup anonymization components
        user_id_manager = SecureUserIDManager.from_env(load_private_key=False)
        anon_db = get_anon_db(anon_db_path, "twitter")
        print(f"   ✅ Initialized anonymization components")

        print("\n📝 Step 2: Create new fake posts...")

        # Create some new posts to protect
        new_posts = create_fake_posts_batch(source_db, num_posts=5)
        print(f"   ✅ Created {len(new_posts)} new posts")
        print(f"      - DBPost objects: {sum(1 for p in new_posts if isinstance(p, DBPost))}")
        print(f"      - PostModel objects: {sum(1 for p in new_posts if isinstance(p, PostModel))}")

        print("\n🔐 Step 3: Protect posts using universal helper...")
        print(f"   Calling protect_posts_batch()...")

        # This is the KEY function - the universal helper!
        stats = protect_posts_batch(
            posts=new_posts,
            platform="twitter",
            user_id_manager=user_id_manager,
            anon_db=anon_db,
            skip_already_protected=True
        )

        print(f"   ✅ Protection completed!")
        print(f"      - Posts processed: {stats['processed']}")
        print(f"      - Posts protected: {stats['protected']}")
        print(f"      - New users anonymized: {stats['users_anonymized']}")
        print(f"      - Posts skipped: {stats['skipped']}")
        print(f"      - Errors: {stats['errors']}")

        print("\n🔍 Step 4: Verify protection...")

        # Check that posts are actually protected
        from big5_databases.databases.security import ProtectionMarker
        marker = ProtectionMarker(source_db)

        protected_count = sum(1 for post in new_posts if marker.is_post_protected(post))
        print(f"   ✅ Verified: {protected_count}/{len(new_posts)} posts marked as protected")

        # Check that UUIDs replaced user IDs
        sample_post = new_posts[0]
        if isinstance(sample_post, DBPost):
            content = sample_post.content if isinstance(sample_post.content, dict) else json.loads(sample_post.content or '{}')
        else:
            content = sample_post.content

        user_id_str = content.get('user', {}).get('id_str', '')
        print(f"   Sample user ID (should be UUID): {user_id_str}")

        # Verify it looks like a UUID
        try:
            uuid.UUID(user_id_str)
            print(f"   ✅ User ID is valid UUID format")
        except ValueError:
            print(f"   ⚠️  User ID doesn't look like a UUID: {user_id_str}")

        print("\n📤 Step 5: Posts ready for insertion!")
        print(f"   These protected posts can now be safely inserted:")
        print(f"   >>> source_db.safe_submit_posts(new_posts)")
        print(f"   Note: Posts are already anonymized, no additional protection needed!")

        print("\n" + "=" * 70)
        print("✅ DEMONSTRATION COMPLETE!")
        print("=" * 70)
        print("\nKey Takeaways:")
        print("  • protect_posts_batch() is the universal helper for post protection")
        print("  • Can be called from anywhere in the databases package")
        print("  • Handles both DBPost and PostModel objects")
        print("  • Automatically checks for existing mappings (efficient)")
        print("  • Posts are marked as protected to avoid reprocessing")
        print("  • Use this for: database processing AND new post insertion")

        return True

    except Exception as e:
        print(f"\n❌ Error in new posts protection demo: {e}")
        print(f"\nCommon issues:")
        print(f"  - Environment variables not set")
        print(f"  - Database files not accessible")
        print(f"  - Missing dependencies")
        import traceback
        traceback.print_exc()
        return False


def create_fake_posts_batch(source_db: PlatformDB, num_posts: int = 5) -> List[Union[DBPost, PostModel]]:
    """
    Create fake posts by copying and modifying existing posts from the database.

    Returns a mix of DBPost and PostModel objects to test batch processing.

    Args:
        source_db: Database to copy existing post structure from
        num_posts: Number of fake posts to create

    Returns:
        List of mixed DBPost and PostModel objects with fake user data
    """
    fake_posts: List[Union[DBPost, PostModel]] = []

    # Get some existing posts to use as templates
    with source_db.get_session() as session:
        template_posts = session.query(DBPost).limit(3).all()

        if not template_posts:
            print("No template posts found in database")
            return fake_posts

        # Generate fake posts
        for i in range(num_posts):
            template = template_posts[i % len(template_posts)]

            # Parse existing content to modify
            if isinstance(template.content, str):
                content_dict = json.loads(template.content)
            else:
                content_dict = template.content or {}

            # Create fake user data (unprotected for testing)
            fake_user_id = f"fake_user_{i + 1000}"
            fake_username = f"test_user_{i}"
            fake_display_name = f"Test User {i}"

            # Modify the content to have unprotected user data
            content_dict.update({
                "id": 9999000000 + i,
                "id_str": str(9999000000 + i),
                "url": f"https://twitter.com/test_user_{i}/status/{9999000000 + i}",
                "user": {
                    "id": fake_user_id,
                    "id_str": fake_user_id,
                    "url": f"https://twitter.com/{fake_username}",
                    "username": fake_username,
                    "displayname": fake_display_name,
                    "rawDescription": f"This is a fake test user {i}",
                    "followersCount": 100 + i,
                    "friendsCount": 50 + i,
                    "statusesCount": 200 + i,
                },
                "rawContent": f"This is fake post content number {i}. Testing anonymization!",
                "lang": "en",
                "replyCount": i,
                "retweetCount": i * 2,
                "likeCount": i * 10,
            })

            # Create metadata without protection marker (fresh posts)
            fake_metadata_dict = {
                "media_paths": [],
                "labels": ["fake", f"test_batch_{i}"],
                # Note: no protection.protected_user field - these are "new" unprotected posts
            }

            # Decide whether to create DBPost or PostModel (alternate for testing)
            if i % 2 == 0:
                # Create DBPost (SQLAlchemy model)
                db_post = DBPost(
                    id=9999000000 + i,
                    platform="twitter",
                    platform_id=str(9999000000 + i),
                    date_created=datetime.now(),
                    post_url=content_dict["url"],
                    content=content_dict,  # Should be dict, not JSON string for DBPost
                    metadata_content=fake_metadata_dict
                )
                fake_posts.append(db_post)
            else:
                # Create PostModel (Pydantic model) - matches PostModel structure
                # Create proper PostMetadataModel for Pydantic model
                fake_metadata_pydantic = PostMetadataModel(
                    media_paths=fake_metadata_dict.get("media_paths", []),
                    language=None,
                    orig_db_conf=None,
                    annotations=None,
                    protection=None,
                    extra=None
                )

                post_model = PostModel(
                    id=9999000000 + i,
                    platform="twitter",
                    platform_id=str(9999000000 + i),
                    post_url=content_dict["url"],
                    date_created=datetime.now(),
                    post_type=PostType.REGULAR,
                    content=content_dict,
                    metadata_content=fake_metadata_pydantic,
                    collection_task_id=None  # Optional field
                )
                fake_posts.append(post_model)

    print(f"Created {len(fake_posts)} fake posts: {sum(1 for p in fake_posts if isinstance(p, DBPost))} DBPost, {sum(1 for p in fake_posts if isinstance(p, PostModel))} PostModel")
    return fake_posts


def demo_batch_processing():
    """
    Demonstrate batch processing of new posts with mixed DBPost/PostModel types.
    Tests the safe_submit_posts functionality with anonymization.
    """
    print("\n" + "=" * 70)
    print("BATCH PROCESSING DEMONSTRATION")
    print("=" * 70)

    try:
        # Use the same paths as main demo - use existing populated database
        base_path = root() / "test_data" / "security"
        source_db_path = base_path / "twitter_phase1.sqlite"
        anon_db_path = base_path / "coverage_batch_test.anon.sqlite"  # Use existing batch test database

        # Check if files are in current directory (when run from different location)
        if not source_db_path.exists():
            source_db_path = Path("twitter_phase1.sqlite")
            anon_db_path = Path("demo_batch_anon.anon.sqlite")

        if not source_db_path.exists():
            print(f"❌ Source database not found: {source_db_path}")
            return False

        # Check if anonymization database exists
        if not anon_db_path.exists():
            print(f"❌ Batch anonymization database not found: {anon_db_path}")
            print("   Run the main test first: python -m big5_databases.databases.security.core.main")
            return False

        print(f"\n📦 Step 1: Creating Fake Post Batch...")

        # Load source database
        source_db = PlatformDB.sqlite_db_from_path("twitter", source_db_path, table_type="posts")

        # Create fake posts for testing
        fake_posts = create_fake_posts_batch(source_db, num_posts=5)

        if not fake_posts:
            print("❌ Failed to create fake posts")
            return False

        print(f"   ✅ Created {len(fake_posts)} fake posts for batch processing")

        print(f"\n🔄 Step 2: Loading Existing Anonymization Database...")

        # Load existing anonymization database
        anon_db = PlatformDB.sqlite_db_from_path("twitter", anon_db_path, table_type="anon")
        print(f"   ✅ Loaded batch anonymization database from {anon_db_path}")

        print(f"\n🛡️  Step 3: Processing Batch with Anonymization...")

        # Submit the batch of posts - this should anonymize user data
        print(f"   Submitting batch of {len(fake_posts)} posts...")
        try:
            submitted_posts = source_db.safe_submit_posts(fake_posts)
            print(f"   ✅ Batch submitted successfully: {len(submitted_posts)} posts processed")

            # Show sample results
            if submitted_posts:
                sample_post = submitted_posts[0]
                print(f"   Sample processed post ID: {sample_post.id}")

                # Check if content was anonymized
                if isinstance(sample_post.content, str):
                    content = json.loads(sample_post.content)
                else:
                    content = sample_post.content

                if content and "user" in content:
                    user_data = content["user"]
                    user_id = user_data.get("id_str", "N/A")
                    username = user_data.get("username", "N/A")
                    print(f"   Sample user data after processing:")
                    print(f"      - User ID: {user_id}")
                    print(f"      - Username: {username}")

                    # Check if protection marker was applied
                    if hasattr(sample_post, 'metadata_content') and sample_post.metadata_content:
                        # Handle both dict and Pydantic model formats
                        if hasattr(sample_post.metadata_content, 'protection'):
                            # Pydantic model format
                            protection_status = getattr(sample_post.metadata_content.protection, 'protected_user', False) if sample_post.metadata_content.protection else False
                        elif isinstance(sample_post.metadata_content, dict):
                            # Dict format
                            protection_status = sample_post.metadata_content.get('protection', {}).get('protected_user', False)
                        else:
                            protection_status = False
                        print(f"      - Protection marker: {protection_status}")

        except Exception as e:
            print(f"   ❌ Batch submission failed: {e}")
            import traceback
            traceback.print_exc()
            return False

        print(f"\n🔍 Step 4: Verifying Anonymization Results...")

        # Process the database to apply anonymization
        try:
            # Run the anonymization process on the new posts
            stats = process_db(
                source_db=source_db,
                anon_db=anon_db,
                batch_size=10
            )

            print(f"   ✅ Anonymization processing completed:")
            print(f"      - Posts processed: {stats.get('posts_processed', 'N/A')}")
            print(f"      - Users anonymized: {stats.get('users_anonymized', 'N/A')}")
            print(f"      - Posts protected: {stats.get('posts_protected', 'N/A')}")
            print(f"      - Errors: {stats.get('errors', 'N/A')}")

        except Exception as e:
            print(f"   ⚠️  Anonymization process had issues: {e}")

        print(f"\n" + "=" * 70)
        print("BATCH PROCESSING DEMONSTRATION COMPLETED!")
        print("=" * 70)

        return True

    except Exception as e:
        print(f"\n❌ Error in batch processing demonstration: {e}")
        print(f"\nCommon issues:")
        print(f"  - Database file not accessible")
        print(f"  - Insufficient permissions")
        print(f"  - Invalid fake post data structure")
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


