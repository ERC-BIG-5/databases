"""
Protection Marker for User Anonymization

This module provides functionality to mark posts where user data has been protected,
using the protection.protected_user field in metadata_content to avoid re-processing.
"""

import json
import logging
from typing import List, Dict, Any, Optional

from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm import Session

from ...db_models import DBPost

logger = logging.getLogger(__name__)


class ProtectionMarker:
    """
    Handles marking posts where user data has been protected.

    Uses the PostProtectionModel structure in metadata_content.protection.protected_user
    to track which posts have already been anonymized, avoiding duplicate processing.
    """

    def __init__(self, db_manager):
        """
        Initialize with database manager.

        Args:
            db_manager: DatabaseManager instance (PlatformDB or similar)
        """
        self.db = db_manager

    def is_post_protected(self, post_row: DBPost) -> bool:
        """
        Check if a post has already been protected.

        Args:
            post_row: Database post row with metadata_content

        Returns:
            bool: True if post is already marked as protected
        """
        if not post_row.metadata_content:
            return False

        metadata = post_row.metadata_content
        if not isinstance(metadata, dict):
            return False

        protection = metadata.get('protection', {})
        if not isinstance(protection, dict):
            return False

        return protection.get('protected_user', False)

    def mark_post_as_protected(self, post_row, session: Session):
        """
        Mark a single post as having protected user data.

        Args:
            post_row: Database post row to mark
            session: SQLAlchemy session
        """
        # Initialize metadata_content if None
        if post_row.metadata_content is None:
            post_row.metadata_content = {}

        # Initialize protection dict if not exists
        if 'protection' not in post_row.metadata_content:
            post_row.metadata_content['protection'] = {}

        # Set the protected_user flag
        post_row.metadata_content['protection']['protected_user'] = True

        # Mark the field as modified for SQLAlchemy
        flag_modified(post_row, "metadata_content")

        logger.debug(f"Marked post {post_row.id} as protected")

    def mark_posts_as_protected(self, post_rows: list[DBPost], session: Session) -> int:
        """
        Mark multiple posts as having protected user data.

        Args:
            post_rows: List of database post rows to mark
            session: SQLAlchemy session

        Returns:
            int: Number of posts marked
        """
        marked_count = 0

        for post_row in post_rows:
            if not self.is_post_protected(post_row):
                self.mark_post_as_protected(post_row, session)
                marked_count += 1

        logger.info(f"Marked {marked_count} posts as protected")
        return marked_count

    def get_unprotected_posts(self, session: Session, batch_size: int = 1000) -> List:
        """
        Get posts that haven't been protected yet.

        Args:
            session: SQLAlchemy session
            batch_size: Maximum number of posts to return

        Returns:
            List of unprotected post rows
        """
        # Query for posts that either:
        # 1. Have no metadata_content
        # 2. Have no protection field in metadata_content
        # 3. Have protection.protected_user = False

        posts = session.query(DBPost).filter(
            # Posts with no metadata or no protection field will need processing
            # We'll check the specific protection status in Python since JSON queries vary by DB
        ).limit(batch_size).all()

        # Filter in Python for posts that need protection
        unprotected_posts = []
        for post in posts:
            if not self.is_post_protected(post):
                unprotected_posts.append(post)

        logger.info(f"Found {len(unprotected_posts)} unprotected posts")
        return unprotected_posts

    def process_database_protection_marking(self, batch_size: int = 1000) -> Dict[str, int]:
        """
        Process entire database to mark posts with existing user protection.

        This should be run after anonymization to mark which posts have been processed.

        Args:
            batch_size: Number of posts to process in each batch

        Returns:
            dict: Statistics with counts of processed, marked, and skipped posts
        """
        stats = {
            'processed': 0,
            'marked': 0,
            'already_marked': 0,
            'errors': 0
        }

        logger.info("Starting database protection marking process")

        try:
            with self.db.get_session() as session:
                # Get total count for progress tracking
                total_posts = session.query(DBPost).count()
                logger.info(f"Processing {total_posts} total posts")

                # Process in batches
                offset = 0
                while True:
                    # Get batch of posts
                    posts = session.query(DBPost).offset(offset).limit(batch_size).all()

                    if not posts:
                        break

                    batch_marked = 0
                    batch_already_marked = 0

                    for post in posts:
                        try:
                            if self.is_post_protected(post):
                                batch_already_marked += 1
                            else:
                                # Check if this post actually has protected content
                                if self._post_has_protected_content(post):
                                    self.mark_post_as_protected(post, session)
                                    batch_marked += 1

                        except Exception as e:
                            logger.error(f"Error processing post {post.id}: {e}")
                            stats['errors'] += 1
                            continue

                    # Commit the batch
                    session.commit()

                    # Update stats
                    stats['processed'] += len(posts)
                    stats['marked'] += batch_marked
                    stats['already_marked'] += batch_already_marked

                    logger.info(f"Processed batch: {offset}-{offset + len(posts)}, "
                              f"marked: {batch_marked}, already marked: {batch_already_marked}")

                    offset += batch_size

        except Exception as e:
            logger.error(f"Database protection marking failed: {e}")
            stats['errors'] += 1

        logger.info(f"Protection marking complete. Stats: {stats}")
        return stats

    def _post_has_protected_content(self, post_row) -> bool:
        """
        Check if a post contains protected content (e.g., <PROTECTED> markers).

        Args:
            post_row: Database post row

        Returns:
            bool: True if post contains protected content
        """
        # Check content field for <PROTECTED> markers
        if post_row.content:
            try:
                if isinstance(post_row.content, str):
                    content = json.loads(post_row.content)
                else:
                    content = post_row.content

                content_str = json.dumps(content)
                if '<PROTECTED>' in content_str:
                    return True

            except (json.JSONDecodeError, TypeError):
                # If we can't parse content, assume it might need protection
                if post_row.content and '<PROTECTED>' in str(post_row.content):
                    return True

        # Check metadata_content for <PROTECTED> markers
        if post_row.metadata_content:
            try:
                metadata_str = json.dumps(post_row.metadata_content)
                if '<PROTECTED>' in metadata_str:
                    return True
            except (TypeError, json.JSONDecodeError):
                if '<PROTECTED>' in str(post_row.metadata_content):
                    return True

        return False

    def get_protection_statistics(self) -> Dict[str, int]:
        """
        Get statistics about protection status in the database.

        Returns:
            dict: Statistics with counts of protected, unprotected, and total posts
        """
        stats = {
            'total_posts': 0,
            'protected_posts': 0,
            'unprotected_posts': 0,
            'posts_with_protected_content': 0,
            'posts_marked_but_no_content': 0
        }

        try:
            with self.db.get_session() as session:
                all_posts = session.query(DBPost).all()
                stats['total_posts'] = len(all_posts)

                for post in all_posts:
                    is_marked = self.is_post_protected(post)
                    has_content = self._post_has_protected_content(post)

                    if is_marked:
                        stats['protected_posts'] += 1
                        if not has_content:
                            stats['posts_marked_but_no_content'] += 1
                    else:
                        stats['unprotected_posts'] += 1

                    if has_content:
                        stats['posts_with_protected_content'] += 1

        except Exception as e:
            logger.error(f"Failed to get protection statistics: {e}")

        return stats


# Convenience functions

def mark_posts_as_protected(db_manager, post_rows: List, session: Session) -> int:
    """
    Convenience function to mark posts as protected.

    Args:
        db_manager: Database manager instance
        post_rows: List of post rows to mark
        session: SQLAlchemy session

    Returns:
        int: Number of posts marked
    """
    marker = ProtectionMarker(db_manager)
    return marker.mark_posts_as_protected(post_rows, session)


def process_database_protection_marking(db_manager, batch_size: int = 1000) -> Dict[str, int]:
    """
    Convenience function to mark all posts in a database.

    Args:
        db_manager: Database manager instance
        batch_size: Batch size for processing

    Returns:
        dict: Processing statistics
    """
    marker = ProtectionMarker(db_manager)
    return marker.process_database_protection_marking(batch_size)


def is_post_protected(db_manager, post_row) -> bool:
    """
    Convenience function to check if a post is protected.

    Args:
        db_manager: Database manager instance
        post_row: Post row to check

    Returns:
        bool: True if post is marked as protected
    """
    marker = ProtectionMarker(db_manager)
    return marker.is_post_protected(post_row)