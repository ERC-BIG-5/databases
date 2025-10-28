"""
Test script for the protection marker functionality.

This demonstrates how posts are marked with protection.protected_user metadata
to track which posts have been processed for anonymization, avoiding re-processing.
"""

import sys
from tools.env_root import root

# Add project root to path
sys.path.insert(0, str(root()))

# Import using the new package structure
from src.big5_databases.databases.security.core import (
    ProtectionMarker,
    process_database_protection_marking
)


def test_protection_marker():
    """Test the protection marker functionality."""
    print("🛡️  PROTECTION MARKER TEST")
    print("=" * 50)

    # Test database path
    test_db = "ANON_TEST_twitter_phase1.sqlite"

    if not Path(test_db).exists():
        print(f"❌ Test database not found: {test_db}")
        print("   This test requires the anonymized database to be present")
        return

    try:
        # Load the database using the same approach as the security modules
        from src.big5_databases.databases.platform_db_mgmt import PlatformDB

        # Create database manager with absolute path
        db_path = Path(test_db).absolute()
        db_manager = PlatformDB.sqlite_db_from_path("twitter", db_path, table_type="posts")
        print(f"✅ Connected to database: {test_db}")

        # Initialize protection marker
        marker = ProtectionMarker(db_manager)
        print(f"✅ Protection marker initialized")

        # Get protection statistics
        print(f"\n📊 Getting protection statistics...")
        stats = marker.get_protection_statistics()

        print(f"   Total posts: {stats['total_posts']}")
        print(f"   Protected posts (marked): {stats['protected_posts']}")
        print(f"   Unprotected posts (unmarked): {stats['unprotected_posts']}")
        print(f"   Posts with <PROTECTED> content: {stats['posts_with_protected_content']}")
        print(f"   Posts marked but no protected content: {stats['posts_marked_but_no_content']}")

        # Test marking posts that have protected content
        print(f"\n🔄 Testing protection marking for posts with <PROTECTED> content...")

        with db_manager.get_session() as session:
            # Get a few unprotected posts that have <PROTECTED> content
            unprotected_posts = marker.get_unprotected_posts(session, batch_size=5)
            print(f"   Found {len(unprotected_posts)} unprotected posts")

            if unprotected_posts:
                # Check which have protected content
                posts_with_content = []
                for post in unprotected_posts:
                    if marker._post_has_protected_content(post):
                        posts_with_content.append(post)

                print(f"   {len(posts_with_content)} posts have <PROTECTED> content")

                if posts_with_content:
                    # Mark these posts
                    marked_count = marker.mark_posts_as_protected(posts_with_content, session)
                    session.commit()
                    print(f"   ✅ Marked {marked_count} posts as protected")

                    # Verify the marking worked
                    for post in posts_with_content:
                        session.refresh(post)  # Refresh from database
                        is_protected = marker.is_post_protected(post)
                        print(f"     Post {post.id}: protected = {is_protected}")

        # Get updated statistics
        print(f"\n📊 Updated protection statistics:")
        updated_stats = marker.get_protection_statistics()
        print(f"   Protected posts: {updated_stats['protected_posts']} (was {stats['protected_posts']})")
        print(f"   Unprotected posts: {updated_stats['unprotected_posts']} (was {stats['unprotected_posts']})")

        # Test the convenience function
        print(f"\n🔄 Testing batch protection marking...")
        try:
            batch_stats = process_database_protection_marking(db_manager, batch_size=100)
            print(f"   Batch processing results:")
            print(f"     Processed: {batch_stats['processed']}")
            print(f"     Marked: {batch_stats['marked']}")
            print(f"     Already marked: {batch_stats['already_marked']}")
            print(f"     Errors: {batch_stats['errors']}")
        except Exception as e:
            print(f"   ⚠️  Batch processing test failed: {e}")

        print(f"\n✅ Protection marker test completed successfully!")

        # Show example of how this integrates with anonymization process
        print(f"\n💡 INTEGRATION WITH ANONYMIZATION:")
        print(f"   When running anonymization process:")
        print(f"   1. Check if post.metadata_content.protection.protected_user = True")
        print(f"   2. If True: Skip processing (already anonymized)")
        print(f"   3. If False/missing: Process for anonymization")
        print(f"   4. After processing: Mark as protected_user = True")
        print(f"   5. Use flag_modified(post, 'metadata_content') to save changes")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_protection_marker()