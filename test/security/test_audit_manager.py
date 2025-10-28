"""
Test the abstract audit manager functionality.

This script demonstrates how to use the AnonymizationAuditor class
to audit anonymized databases.
"""

import sys
from tools.env_root import root

# Add project root to path
sys.path.insert(0, str(root()))

# Import using the new package structure
from src.big5_databases.databases.security.audit import (
    AnonymizationAuditor,
    AuditConfig,
    quick_audit,
    full_audit_with_decryption
)


def main():
    print("🔍 Testing Anonymization Audit Manager")
    print("=" * 50)

    # Test 1: Quick audit (no decryption)
    print("\n📋 Test 1: Quick Audit (Analysis Only)")
    try:
        result = quick_audit(
            anonymized_db="ANON_TEST_twitter_phase1.sqlite",
            original_db="twitter_phase1.sqlite",
            sample_size=10
        )

        print(f"✅ Quick audit completed successfully")
        print(f"   Posts analyzed: {result.total_posts_analyzed}")
        print(f"   Anonymized posts found: {result.anonymized_posts_found}")
        print(f"   Protected field types: {len(result.protected_field_frequency)}")

        # Show top protected fields
        sorted_fields = sorted(
            result.protected_field_frequency.items(),
            key=lambda x: x[1],
            reverse=True
        )
        print("   Top protected fields:")
        for field, count in sorted_fields[:3]:
            print(f"     {field}: {count} posts")

    except Exception as e:
        print(f"❌ Quick audit failed: {e}")

    # Test 2: Full audit manager usage
    print("\n📋 Test 2: Full Audit Manager Usage")
    try:
        auditor = AnonymizationAuditor(authorized_by="test_script")
        config = AuditConfig(
            sample_size=15,
            enable_decryption=False,  # Set to True when decryption is implemented
            max_examples=2
        )

        result = auditor.audit_database(
            anonymized_db="ANON_TEST_twitter_phase1.sqlite",
            original_db="twitter_phase1.sqlite",
            config=config
        )

        print(f"✅ Full audit completed successfully")

        # Generate and display report
        report = auditor.generate_audit_report(result)
        print("\n📄 AUDIT REPORT:")
        print(report)

        # Save report to file
        with open('audit_report.txt', 'w') as f:
            f.write(report)
        print("💾 Full report saved to audit_report.txt")

    except Exception as e:
        print(f"❌ Full audit failed: {e}")

    # Test 3: Decryption audit (placeholder)
    print("\n📋 Test 3: Decryption Audit (Placeholder)")
    try:
        result = full_audit_with_decryption(
            anonymized_db="ANON_TEST_twitter_phase1.sqlite",
            original_db="twitter_phase1.sqlite",
            private_key_env="test_private_decr.env",
            sample_size=5
        )

        print(f"✅ Decryption audit completed")
        print(f"   Decryption attempted: {result.decryption_attempted}")
        print(f"   Decryption successful: {result.decryption_successful}")

        if not result.decryption_successful:
            print("   ⚠️  Decryption not yet implemented - this is expected")

    except Exception as e:
        print(f"❌ Decryption audit failed: {e}")

    print("\n✅ All tests completed!")
    print("\nNEXT STEPS for full decryption capability:")
    print("1. Find or create the anonymization mapping database (.anon.sqlite)")
    print("2. Implement the _attempt_decryption method in audit_manager.py")
    print("3. Load private key and use security modules for actual decryption")
    print("4. Compare decrypted values with original database values")


if __name__ == "__main__":
    main()