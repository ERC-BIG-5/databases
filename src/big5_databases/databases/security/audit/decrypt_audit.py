"""
Decrypt and Audit Method for Anonymized Data Verification

This module provides functionality to decrypt specific posts from an anonymized database
using the anonymization mapping database and private key, then verify the decrypted
data against the original database.

IMPORTANT: Use this ONLY for authorized audit operations.
This module loads private keys and should be used with proper authorization.
"""

import sqlite3
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime
from dotenv import load_dotenv
import os
import importlib.util

# Configure logging
logger = logging.getLogger("decrypt_audit")


@dataclass
class DecryptAuditResult:
    """Results from a decrypt and audit operation."""
    total_requested: int
    successfully_decrypted: int
    verification_passed: int
    verification_failed: int
    errors: List[str]
    decrypted_data: List[Dict[str, Any]]
    verification_results: List[Dict[str, Any]]
    timestamp: datetime


class DecryptAuditor:
    """
    Handles decryption and verification of anonymized data.

    Usage:
        auditor = DecryptAuditor("test_private_decr.env")
        result = auditor.decrypt_and_verify(
            protected_db="ANON_TEST_twitter_phase1.sqlite",
            anon_db="mapping.anon.sqlite",
            original_db="twitter_phase1.sqlite",
            uuids=["uuid1", "uuid2", "uuid3"]
        )
    """

    def __init__(self, private_key_env_file: Union[str, Path]):
        """
        Initialize the decrypt auditor.

        Args:
            private_key_env_file: Path to environment file containing private key
        """
        self.private_key_env_file = Path(private_key_env_file)
        self.security_config = None
        self.decryption_manager = None

        # Load environment and initialize crypto
        self._load_security_modules()

    def _load_security_modules(self):
        """Load the security modules and private key."""
        try:
            # Load environment
            load_dotenv(self.private_key_env_file)

            # Verify required keys are present
            required_keys = ['HMAC_KEY', 'PUBLIC_KEY_PEM', 'PRIVATE_KEY_PEM', 'KEY_VERSION']
            for key in required_keys:
                if key not in os.environ:
                    raise ValueError(f"Required environment variable {key} not found")

            # Load SecurityConfig using clean imports
            from ..core.secure_config import SecurityConfig

            # Load components using clean imports
            from .decryption_manager import DecryptionManager
            from ..core.db_operations import DatabaseOperations

            # Initialize security components
            self.security_config = SecurityConfig()
            self.DatabaseOperations = DatabaseOperations
            self.DecryptionManager = DecryptionManager

            logger.info("Security modules loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load security modules: {e}")
            raise

    def decrypt_and_verify(
        self,
        protected_db: Union[str, Path],
        anon_db: Union[str, Path],
        original_db: Union[str, Path],
        uuids: List[str]
    ) -> DecryptAuditResult:
        """
        Decrypt specified UUIDs and verify against original data.

        Args:
            protected_db: Path to anonymized database with <PROTECTED> data
            anon_db: Path to anonymization mapping database
            original_db: Path to original database for verification
            uuids: List of public UUIDs to decrypt and verify

        Returns:
            DecryptAuditResult containing decryption and verification results
        """
        logger.info(f"Starting decrypt and verify for {len(uuids)} UUIDs")

        protected_db = Path(protected_db)
        anon_db = Path(anon_db)
        original_db = Path(original_db)

        # Validate inputs
        for db_path, name in [(protected_db, "protected"), (anon_db, "anon"), (original_db, "original")]:
            if not db_path.exists():
                raise FileNotFoundError(f"{name} database not found: {db_path}")

        result = DecryptAuditResult(
            total_requested=len(uuids),
            successfully_decrypted=0,
            verification_passed=0,
            verification_failed=0,
            errors=[],
            decrypted_data=[],
            verification_results=[],
            timestamp=datetime.now()
        )

        try:
            # Step 1: Load anonymization mappings for the requested UUIDs
            mappings = self._load_anonymization_mappings(anon_db, uuids)
            logger.info(f"Loaded {len(mappings)} anonymization mappings")

            # Step 2: Decrypt the user data for each UUID
            decrypted_users = self._decrypt_user_data(mappings)
            result.successfully_decrypted = len(decrypted_users)

            # Step 3: Find posts for these users in the protected database
            protected_posts = self._get_protected_posts_by_users(protected_db, list(decrypted_users.keys()))

            # Step 4: Find corresponding posts in original database
            original_posts = self._get_original_posts_by_users(original_db, list(decrypted_users.values()))

            # Step 5: Perform verification
            verification_results = self._verify_decrypted_data(
                protected_posts, original_posts, decrypted_users, mappings
            )

            result.verification_results = verification_results
            result.verification_passed = sum(1 for v in verification_results if v['verification_passed'])
            result.verification_failed = sum(1 for v in verification_results if not v['verification_passed'])

            # Step 6: Compile decrypted data for return
            result.decrypted_data = [
                {
                    'public_uuid': uuid,
                    'decrypted_user_id': decrypted_users.get(uuid, 'DECRYPTION_FAILED'),
                    'mapping_found': uuid in mappings,
                    'posts_found': len([p for p in protected_posts if self._extract_user_from_post(p) == uuid])
                }
                for uuid in uuids
            ]

            logger.info(f"Decrypt and verify completed: {result.verification_passed} passed, {result.verification_failed} failed")

        except Exception as e:
            error_msg = f"Decrypt and verify failed: {str(e)}"
            logger.error(error_msg)
            result.errors.append(error_msg)

        return result

    def _load_anonymization_mappings(self, anon_db: Path, uuids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Load anonymization mappings for specific UUIDs."""
        mappings = {}

        with sqlite3.connect(anon_db) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get mappings for requested UUIDs
            placeholders = ','.join('?' * len(uuids))
            cursor.execute(f"""
                SELECT user_id_hash, encrypted_user_id, public_id, encrypted_data, key_version
                FROM anonymize
                WHERE public_id IN ({placeholders})
            """, uuids)

            for row in cursor.fetchall():
                mappings[row['public_id']] = {
                    'user_id_hash': row['user_id_hash'],
                    'encrypted_user_id': row['encrypted_user_id'],
                    'public_id': row['public_id'],
                    'encrypted_data': row['encrypted_data'],
                    'key_version': row['key_version']
                }

        return mappings

    def _decrypt_user_data(self, mappings: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        """Decrypt user data using the private key."""
        decrypted_users = {}

        try:
            # Initialize database operations using clean imports
            from ...platform_db_mgmt import PlatformDB

            # Create a temporary database manager for decryption
            temp_db = PlatformDB.sqlite_db_from_path("temp", ":memory:", create=True, table_type="anon")
            db_ops = self.DatabaseOperations(temp_db)

            # Initialize decryption manager
            decryption_manager = self.DecryptionManager(
                config=self.security_config,
                db_ops=db_ops,
                authorized_by="decrypt_audit"
            )

            # Decrypt each user
            for public_uuid, mapping in mappings.items():
                try:
                    # Decrypt the user ID
                    decrypted_user_id = decryption_manager.decrypt_user_id(
                        mapping['encrypted_user_id']
                    )
                    decrypted_users[public_uuid] = decrypted_user_id
                    logger.debug(f"Successfully decrypted user for UUID {public_uuid}")

                except Exception as e:
                    logger.error(f"Failed to decrypt user for UUID {public_uuid}: {e}")
                    decrypted_users[public_uuid] = None

        except Exception as e:
            logger.error(f"Failed to initialize decryption: {e}")

        return decrypted_users

    def _get_protected_posts_by_users(self, protected_db: Path, uuids: List[str]) -> List[Dict[str, Any]]:
        """Get posts from protected database that contain the specified UUIDs."""
        posts = []

        with sqlite3.connect(protected_db) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Find posts that contain these UUIDs (they would be in anonymized form)
            # Since we can't directly query by UUID in protected DB, get all posts with <PROTECTED>
            cursor.execute("""
                SELECT id, platform_id, content, metadata_content, date_created, post_url
                FROM post
                WHERE content LIKE '%<PROTECTED>%' OR metadata_content LIKE '%<PROTECTED>%'
            """)

            for row in cursor.fetchall():
                post_data = self._parse_post_row(row)
                posts.append(post_data)

        return posts

    def _get_original_posts_by_users(self, original_db: Path, user_ids: List[str]) -> List[Dict[str, Any]]:
        """Get posts from original database for specific user IDs."""
        posts: List[Dict[str, Any]] = []

        # Filter out None values
        valid_user_ids = [uid for uid in user_ids if uid is not None]

        if not valid_user_ids:
            return posts

        with sqlite3.connect(original_db) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get posts for these user IDs
            placeholders = ','.join('?' * len(valid_user_ids))
            cursor.execute(f"""
                SELECT id, platform_id, content, metadata_content, date_created, post_url
                FROM post
                WHERE json_extract(content, '$.user.id') IN ({placeholders})
            """, valid_user_ids)

            for row in cursor.fetchall():
                post_data = self._parse_post_row(row)
                posts.append(post_data)

        return posts

    def _parse_post_row(self, row) -> Dict[str, Any]:
        """Parse a database row into a post dictionary."""
        post_data = dict(row)

        # Parse JSON fields
        if post_data['content']:
            try:
                post_data['content'] = json.loads(post_data['content'])
            except json.JSONDecodeError:
                pass

        if post_data['metadata_content']:
            try:
                post_data['metadata_content'] = json.loads(post_data['metadata_content'])
            except json.JSONDecodeError:
                pass

        return post_data

    def _extract_user_from_post(self, post: Dict[str, Any]) -> Optional[str]:
        """Extract user identifier from a post (either UUID or user ID)."""
        content = post.get('content', {})
        if isinstance(content, dict) and 'user' in content:
            user_data = content['user']
            if isinstance(user_data, dict):
                # Check if this is a protected post (contains <PROTECTED>)
                if user_data.get('id') == '<PROTECTED>':
                    # This is a protected post - we'd need to map it back to UUID
                    # For now, return None as we can't directly extract UUID
                    return None
                else:
                    # This is an original post - return the user ID
                    return str(user_data.get('id'))
        return None

    def _verify_decrypted_data(
        self,
        protected_posts: List[Dict[str, Any]],
        original_posts: List[Dict[str, Any]],
        decrypted_users: Dict[str, str],
        mappings: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Verify that decrypted data matches original data."""
        verification_results = []

        # Group original posts by user ID for quick lookup
        original_by_user: Dict[str, List[Dict[str, Any]]] = {}
        for post in original_posts:
            user_id = self._extract_user_from_post(post)
            if user_id:
                if user_id not in original_by_user:
                    original_by_user[user_id] = []
                original_by_user[user_id].append(post)

        # For each UUID that was successfully decrypted
        for public_uuid, decrypted_user_id in decrypted_users.items():
            if decrypted_user_id is None:
                verification_results.append({
                    'public_uuid': public_uuid,
                    'verification_passed': False,
                    'reason': 'Failed to decrypt user ID',
                    'decrypted_user_id': None,
                    'original_posts_found': 0,
                    'matches_found': 0
                })
                continue

            # Find original posts for this user
            user_original_posts = original_by_user.get(decrypted_user_id, [])

            # Count matches by timestamp with protected posts
            matches_found = 0
            for orig_post in user_original_posts:
                # Look for a protected post with the same timestamp
                matching_protected = [
                    p for p in protected_posts
                    if p['date_created'] == orig_post['date_created']
                ]
                if matching_protected:
                    matches_found += 1

            verification_results.append({
                'public_uuid': public_uuid,
                'verification_passed': matches_found > 0,
                'reason': f'Found {matches_found} matching posts' if matches_found > 0 else 'No matching posts found',
                'decrypted_user_id': decrypted_user_id,
                'original_posts_found': len(user_original_posts),
                'matches_found': matches_found
            })

        return verification_results

    def generate_decrypt_audit_report(self, result: DecryptAuditResult) -> str:
        """Generate a human-readable decrypt audit report."""
        report = []
        report.append("=" * 60)
        report.append("DECRYPT AND AUDIT REPORT")
        report.append("=" * 60)
        report.append(f"Timestamp: {result.timestamp}")
        report.append("")

        report.append("SUMMARY:")
        report.append(f"  UUIDs requested: {result.total_requested}")
        report.append(f"  Successfully decrypted: {result.successfully_decrypted}")
        report.append(f"  Verification passed: {result.verification_passed}")
        report.append(f"  Verification failed: {result.verification_failed}")
        report.append("")

        if result.errors:
            report.append("ERRORS:")
            for error in result.errors:
                report.append(f"  {error}")
            report.append("")

        report.append("DECRYPTED DATA:")
        for data in result.decrypted_data:
            report.append(f"  UUID: {data['public_uuid']}")
            report.append(f"    Mapping found: {data['mapping_found']}")
            report.append(f"    Decrypted user ID: {data['decrypted_user_id']}")
            report.append(f"    Posts found: {data['posts_found']}")
            report.append("")

        report.append("VERIFICATION RESULTS:")
        for verification in result.verification_results:
            status = "✅ PASSED" if verification['verification_passed'] else "❌ FAILED"
            report.append(f"  UUID: {verification['public_uuid']} - {status}")
            report.append(f"    Decrypted user ID: {verification['decrypted_user_id']}")
            report.append(f"    Original posts found: {verification['original_posts_found']}")
            report.append(f"    Matches found: {verification['matches_found']}")
            report.append(f"    Reason: {verification['reason']}")
            report.append("")

        report.append("=" * 60)
        return "\n".join(report)


# Convenience function
def decrypt_and_verify_uuids(
    protected_db: Union[str, Path],
    anon_db: Union[str, Path],
    original_db: Union[str, Path],
    uuids: List[str],
    private_key_env: Union[str, Path] = "test_private_decr.env"
) -> DecryptAuditResult:
    """
    Convenience function to decrypt and verify specific UUIDs.

    Args:
        protected_db: Path to anonymized database with <PROTECTED> data
        anon_db: Path to anonymization mapping database
        original_db: Path to original database for verification
        uuids: List of public UUIDs to decrypt and verify
        private_key_env: Path to environment file with private key

    Returns:
        DecryptAuditResult containing decryption and verification results
    """
    auditor = DecryptAuditor(private_key_env)
    return auditor.decrypt_and_verify(protected_db, anon_db, original_db, uuids)