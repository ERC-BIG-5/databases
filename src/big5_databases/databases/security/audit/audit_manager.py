"""
Audit Manager for Anonymization Verification

This module provides functionality to audit anonymized databases by comparing
them against original data and optionally attempting decryption where possible.

IMPORTANT: Use this ONLY for authorized audit operations.
This manager may load private keys and should be used sparingly.

SECURITY:
- Load private key only when needed for decryption
- Log all audit operations
- Restrict access via RBAC
- Time-limit private key exposure
"""

import sqlite3
import json
import random
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple
from dataclasses import dataclass
from datetime import datetime

# Configure audit logging
audit_logger = logging.getLogger("anonymization.audit")


@dataclass
class AuditResult:
    """Results from an audit operation."""
    total_posts_analyzed: int
    anonymized_posts_found: int
    protected_field_frequency: Dict[str, int]
    comparison_results: Dict[str, Any]
    decryption_attempted: bool
    decryption_successful: Optional[bool]
    timestamp: datetime
    examples: List[Dict[str, Any]]


@dataclass
class AuditConfig:
    """Configuration for audit operations."""
    sample_size: int = 20
    enable_decryption: bool = False
    private_key_env_file: Optional[Path] = None
    log_operations: bool = True
    max_examples: int = 3


class AnonymizationAuditor:
    """
    Handles audit operations for anonymized databases.

    Can operate in two modes:
    1. Analysis mode: Compares anonymized vs original data without decryption
    2. Decryption mode: Attempts to decrypt protected values using private key

    Usage:
        # Basic analysis audit
        auditor = AnonymizationAuditor()
        result = auditor.audit_database(
            anonymized_db="anon.sqlite",
            original_db="orig.sqlite"
        )

        # Full audit with decryption
        auditor = AnonymizationAuditor()
        result = auditor.audit_database(
            anonymized_db="anon.sqlite",
            original_db="orig.sqlite",
            config=AuditConfig(enable_decryption=True, private_key_env_file=Path("private.env"))
        )
    """

    def __init__(self, authorized_by: str = "system"):
        """
        Initialize audit manager.

        Args:
            authorized_by: Identifier of who authorized this audit operation
        """
        self.authorized_by = authorized_by
        self.audit_start_time = datetime.now()

        if audit_logger:
            audit_logger.info(f"Audit session initialized by: {authorized_by}")

    def audit_database(
        self,
        anonymized_db: Union[str, Path],
        original_db: Union[str, Path],
        config: Optional[AuditConfig] = None
    ) -> AuditResult:
        """
        Perform comprehensive audit of an anonymized database.

        Args:
            anonymized_db: Path to anonymized database
            original_db: Path to original database
            config: Audit configuration (uses defaults if not provided)

        Returns:
            AuditResult containing comprehensive audit findings

        Raises:
            FileNotFoundError: If databases don't exist
            ValueError: If databases are invalid or incompatible
        """
        config = config or AuditConfig()

        anonymized_db = Path(anonymized_db)
        original_db = Path(original_db)

        # Validate inputs
        if not anonymized_db.exists():
            raise FileNotFoundError(f"Anonymized database not found: {anonymized_db}")
        if not original_db.exists():
            raise FileNotFoundError(f"Original database not found: {original_db}")

        audit_logger.info(f"Starting audit: {anonymized_db} vs {original_db}")
        audit_logger.info(f"Config: sample_size={config.sample_size}, decryption={config.enable_decryption}")

        try:
            # Step 1: Sample posts from anonymized database
            anon_posts = self._sample_anonymized_posts(anonymized_db, config.sample_size)

            # Step 2: Find matching posts in original database
            matched_posts = self._find_matching_original_posts(original_db, anon_posts)

            # Step 3: Analyze anonymization patterns
            analysis = self._analyze_anonymization_patterns(matched_posts, config.max_examples)

            # Step 4: Attempt decryption if enabled
            decryption_result = None
            if config.enable_decryption and config.private_key_env_file:
                decryption_result = self._attempt_decryption(
                    anonymized_db, matched_posts, config.private_key_env_file
                )

            # Compile results
            result = AuditResult(
                total_posts_analyzed=len(matched_posts),
                anonymized_posts_found=len([p for p in anon_posts if self._has_protected_content(p)]),
                protected_field_frequency=analysis['protected_field_frequency'],
                comparison_results=analysis,
                decryption_attempted=config.enable_decryption,
                decryption_successful=decryption_result.get('success') if decryption_result else None,
                timestamp=datetime.now(),
                examples=analysis['examples']
            )

            audit_logger.info(f"Audit completed: {result.total_posts_analyzed} posts analyzed")
            return result

        except Exception as e:
            audit_logger.error(f"Audit failed: {str(e)}")
            raise

    def _sample_anonymized_posts(self, db_path: Path, sample_size: int) -> List[Dict[str, Any]]:
        """Sample posts from anonymized database, preferring those with <PROTECTED> content."""
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # First try to get posts with protected content
            cursor.execute("""
                SELECT id, platform_id, content, metadata_content, date_created, post_url
                FROM post
                WHERE content LIKE '%<PROTECTED>%' OR metadata_content LIKE '%<PROTECTED>%'
                ORDER BY RANDOM()
                LIMIT ?
            """, (sample_size,))

            posts = []
            for row in cursor.fetchall():
                posts.append(self._parse_post_row(row))

            # If we didn't get enough posts with protected content, sample randomly
            if len(posts) < sample_size:
                remaining = sample_size - len(posts)
                cursor.execute("""
                    SELECT id, platform_id, content, metadata_content, date_created, post_url
                    FROM post
                    WHERE id NOT IN ({})
                    ORDER BY RANDOM()
                    LIMIT ?
                """.format(','.join(str(p['id']) for p in posts) or '0'), (remaining,))

                for row in cursor.fetchall():
                    posts.append(self._parse_post_row(row))

            return posts

    def _find_matching_original_posts(self, db_path: Path, anon_posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Find matching posts in original database using timestamp matching."""
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            matched_posts = []
            for anon_post in anon_posts:
                # Match by timestamp and platform_id
                cursor.execute("""
                    SELECT id, platform_id, content, metadata_content, date_created, post_url
                    FROM post
                    WHERE date_created = ? AND platform_id = ?
                """, (anon_post['date_created'], anon_post['platform_id']))

                row = cursor.fetchone()
                if row:
                    original_post = self._parse_post_row(row)
                    matched_posts.append({
                        'anon_post': anon_post,
                        'original_post': original_post
                    })
                else:
                    # Fallback: try just timestamp matching
                    cursor.execute("""
                        SELECT id, platform_id, content, metadata_content, date_created, post_url
                        FROM post
                        WHERE date_created = ?
                        LIMIT 1
                    """, (anon_post['date_created'],))

                    row = cursor.fetchone()
                    if row:
                        original_post = self._parse_post_row(row)
                        matched_posts.append({
                            'anon_post': anon_post,
                            'original_post': original_post
                        })

            return matched_posts

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

    def _has_protected_content(self, post: Dict[str, Any]) -> bool:
        """Check if a post contains <PROTECTED> content."""
        content_str = json.dumps(post.get('content', {}))
        metadata_str = json.dumps(post.get('metadata_content', {}))
        return '<PROTECTED>' in content_str or '<PROTECTED>' in metadata_str

    def _analyze_anonymization_patterns(self, matched_posts: List[Dict[str, Any]], max_examples: int) -> Dict[str, Any]:
        """Analyze what fields were anonymized and how."""
        analysis: Dict[str, Any] = {
            'total_posts': len(matched_posts),
            'protected_field_frequency': {},
            'field_differences': {},
            'examples': []
        }

        for match in matched_posts:
            anon_post = match['anon_post']
            orig_post = match['original_post']

            # Find protected fields
            protected_content_fields = self._extract_protected_fields(anon_post.get('content', {}))
            protected_metadata_fields = self._extract_protected_fields(anon_post.get('metadata_content', {}), 'metadata_content')

            all_protected = protected_content_fields + protected_metadata_fields

            # Count frequency of protected fields
            for field in all_protected:
                analysis['protected_field_frequency'][field] = (
                    analysis['protected_field_frequency'].get(field, 0) + 1
                )

            # Compare all fields for differences
            self._compare_post_fields(anon_post, orig_post, analysis['field_differences'])

            # Add example if we haven't reached the limit
            if len(analysis['examples']) < max_examples and all_protected:
                example = self._create_audit_example(anon_post, orig_post, all_protected)
                analysis['examples'].append(example)

        return analysis

    def _extract_protected_fields(self, data, path=""):
        """Recursively extract fields that contain <PROTECTED>."""
        protected_fields = []

        if isinstance(data, dict):
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key
                if isinstance(value, str) and value == "<PROTECTED>":
                    protected_fields.append(current_path)
                elif isinstance(value, (dict, list)):
                    protected_fields.extend(self._extract_protected_fields(value, current_path))
        elif isinstance(data, list):
            for i, value in enumerate(data):
                current_path = f"{path}[{i}]"
                if isinstance(value, str) and value == "<PROTECTED>":
                    protected_fields.append(current_path)
                elif isinstance(value, (dict, list)):
                    protected_fields.extend(self._extract_protected_fields(value, current_path))

        return protected_fields

    def _compare_post_fields(self, anon_post: Dict[str, Any], orig_post: Dict[str, Any], differences: Dict[str, int]):
        """Compare all fields between anonymized and original posts."""
        # Compare top-level fields
        for field in ['platform_id', 'post_url']:
            if anon_post.get(field) != orig_post.get(field):
                differences[field] = differences.get(field, 0) + 1

        # Compare JSON content fields
        anon_content_str = json.dumps(anon_post.get('content', {}), sort_keys=True)
        orig_content_str = json.dumps(orig_post.get('content', {}), sort_keys=True)
        if anon_content_str != orig_content_str:
            differences['content'] = differences.get('content', 0) + 1

        # Compare metadata
        anon_metadata_str = json.dumps(anon_post.get('metadata_content', {}), sort_keys=True)
        orig_metadata_str = json.dumps(orig_post.get('metadata_content', {}), sort_keys=True)
        if anon_metadata_str != orig_metadata_str:
            differences['metadata_content'] = differences.get('metadata_content', 0) + 1

    def _create_audit_example(self, anon_post: Dict[str, Any], orig_post: Dict[str, Any], protected_fields: List[str]) -> Dict[str, Any]:
        """Create an audit example showing anonymization."""
        example = {
            'post_id': anon_post['id'],
            'timestamp': anon_post['date_created'],
            'protected_fields_count': len(protected_fields),
            'protected_fields': protected_fields[:5],  # Limit for readability
            'sample_original_values': {}
        }

        # Get sample original values for protected fields
        for field in protected_fields[:3]:  # Limit to first 3 for brevity
            original_value = self._get_field_value(orig_post, field)
            if original_value is not None:
                example['sample_original_values'][field] = str(original_value)[:100]  # Truncate

        return example

    def _get_field_value(self, post: Dict[str, Any], field_path: str) -> Any:
        """Get value from nested field path."""
        try:
            # Handle paths like "user.id", "metadata_content.user.name", etc.
            path_parts = field_path.split('.')

            # Start with appropriate root
            if field_path.startswith('metadata_content'):
                current = post.get('metadata_content', {})
                path_parts = path_parts[1:]  # Skip 'metadata_content'
            else:
                current = post.get('content', {})

            # Navigate through the path
            for part in path_parts:
                if '[' in part and ']' in part:
                    # Handle array indices like "items[0]"
                    key = part.split('[')[0]
                    index = int(part.split('[')[1].split(']')[0])
                    current = current[key][index]
                else:
                    current = current[part]

            return current
        except (KeyError, IndexError, TypeError, AttributeError):
            return None

    def _attempt_decryption(self, anon_db_path: Path, matched_posts: List[Dict[str, Any]], env_file: Path) -> Dict[str, Any]:
        """
        Attempt to decrypt protected values using private key.

        This is a placeholder for full decryption implementation.
        Would require:
        1. Loading the anonymization mapping database
        2. Loading private key from env file
        3. Using decryption modules to reverse the protection
        """
        audit_logger.warning("Decryption attempted but not fully implemented")

        # TODO: Implement full decryption
        # This would need:
        # 1. Find anonymization mapping database
        # 2. Load security modules
        # 3. Decrypt protected values
        # 4. Compare with originals

        return {
            'success': False,
            'reason': 'Decryption not yet implemented',
            'next_steps': [
                'Locate anonymization mapping database',
                'Load private key from env file',
                'Implement decryption logic using security modules'
            ]
        }

    def generate_audit_report(self, result: AuditResult) -> str:
        """Generate a human-readable audit report."""
        report = []
        report.append("=" * 60)
        report.append("ANONYMIZATION AUDIT REPORT")
        report.append("=" * 60)
        report.append(f"Timestamp: {result.timestamp}")
        report.append(f"Authorized by: {self.authorized_by}")
        report.append("")

        report.append("SUMMARY:")
        report.append(f"  Posts analyzed: {result.total_posts_analyzed}")
        report.append(f"  Posts with anonymized content: {result.anonymized_posts_found}")
        report.append(f"  Decryption attempted: {result.decryption_attempted}")
        if result.decryption_attempted:
            report.append(f"  Decryption successful: {result.decryption_successful}")
        report.append("")

        report.append("PROTECTED FIELDS (frequency):")
        sorted_fields = sorted(
            result.protected_field_frequency.items(),
            key=lambda x: x[1],
            reverse=True
        )
        for field, count in sorted_fields:
            percentage = (count / result.total_posts_analyzed) * 100 if result.total_posts_analyzed > 0 else 0
            report.append(f"  {field}: {count} posts ({percentage:.1f}%)")
        report.append("")

        report.append("SAMPLE ANONYMIZATION EXAMPLES:")
        for i, example in enumerate(result.examples, 1):
            report.append(f"  Example {i}:")
            report.append(f"    Post ID: {example['post_id']}")
            report.append(f"    Timestamp: {example['timestamp']}")
            report.append(f"    Protected fields: {example['protected_fields_count']}")
            for field, value in example.get('sample_original_values', {}).items():
                report.append(f"      {field}: {value}")
            report.append("")

        report.append("=" * 60)

        return "\n".join(report)


# Convenience functions for common use cases

def quick_audit(anonymized_db: Union[str, Path], original_db: Union[str, Path], sample_size: int = 20) -> AuditResult:
    """
    Perform a quick audit without decryption.

    Args:
        anonymized_db: Path to anonymized database
        original_db: Path to original database
        sample_size: Number of posts to sample

    Returns:
        AuditResult with analysis findings
    """
    auditor = AnonymizationAuditor(authorized_by="quick_audit")
    config = AuditConfig(sample_size=sample_size, enable_decryption=False)
    return auditor.audit_database(anonymized_db, original_db, config)


def full_audit_with_decryption(
    anonymized_db: Union[str, Path],
    original_db: Union[str, Path],
    private_key_env: Union[str, Path],
    sample_size: int = 20
) -> AuditResult:
    """
    Perform a full audit with decryption attempt.

    Args:
        anonymized_db: Path to anonymized database
        original_db: Path to original database
        private_key_env: Path to env file with private key
        sample_size: Number of posts to sample

    Returns:
        AuditResult with analysis and decryption findings
    """
    auditor = AnonymizationAuditor(authorized_by="full_audit")
    config = AuditConfig(
        sample_size=sample_size,
        enable_decryption=True,
        private_key_env_file=Path(private_key_env)
    )
    return auditor.audit_database(anonymized_db, original_db, config)