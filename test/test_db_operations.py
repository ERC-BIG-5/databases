import os
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost, DBPostProcessItem
from big5_databases.databases.db_operations import (
    filter_posts_with_existing_post_ids,
    filter_ppitems_with_existing_post_ids
)
from big5_databases.databases.external import DBConfig, SQliteConnection
from big5_databases.databases.model_conversion import PostModel, PostProcessModel


def setup_function(function):
    """Setup test database file."""
    if os.path.exists("test_db_operations.sqlite"):
        os.remove("test_db_operations.sqlite")


def teardown_function(function):
    """Teardown test database file."""
    if os.path.exists("test_db_operations.sqlite"):
        os.remove("test_db_operations.sqlite")


@pytest.fixture
def test_db_config() -> DBConfig:
    return DBConfig(db_connection=SQliteConnection(db_path=Path("test_db_operations.sqlite")))


@pytest.fixture
def db_manager(test_db_config):
    """Create and initialize a test database manager."""
    db = DatabaseManager(test_db_config)
    db.init_database()
    return db


@pytest.fixture
def sample_posts():
    """Create sample posts for testing."""
    return [
        DBPost(
            platform="test_platform",
            platform_id="post_1",
            date_created=datetime.now(),
            content="Test post 1"
        ),
        DBPost(
            platform="test_platform",
            platform_id="post_2",
            date_created=datetime.now(),
            content="Test post 2"
        ),
        DBPost(
            platform="test_platform",
            platform_id="post_3",
            date_created=datetime.now(),
            content="Test post 3"
        )
    ]


@pytest.fixture
def sample_ppitems():
    """Create sample post process items for testing."""
    return [
        DBPostProcessItem(
            platform_id="ppitem_1",
            input={"test": "input_1"},
            output={"result": "output_1"}
        ),
        DBPostProcessItem(
            platform_id="ppitem_2",
            input={"test": "input_2"},
            output={"result": "output_2"}
        ),
        DBPostProcessItem(
            platform_id="ppitem_3",
            input={"test": "input_3"},
            output={"result": "output_3"}
        )
    ]


class TestFilterPostsWithExistingPostIds:
    """Test the filter_posts_with_existing_post_ids function."""

    def test_filter_with_no_existing_posts(self, db_manager, sample_posts):
        """Test filtering when no posts exist in database."""
        result = filter_posts_with_existing_post_ids(sample_posts, db=db_manager)

        assert len(result) == 3
        assert all(isinstance(post, DBPost) for post in result)
        assert {post.platform_id for post in result} == {"post_1", "post_2", "post_3"}

    def test_filter_with_some_existing_posts(self, db_manager, sample_posts):
        """Test filtering when some posts already exist in database."""
        # Add one post to database
        with db_manager.get_session() as session:
            session.add(sample_posts[0])  # Add "post_1"
            session.commit()

        result = filter_posts_with_existing_post_ids(sample_posts, db=db_manager)

        assert len(result) == 2
        assert {post.platform_id for post in result} == {"post_2", "post_3"}

    def test_filter_with_all_existing_posts(self, db_manager, sample_posts):
        """Test filtering when all posts already exist in database."""
        # Add all posts to database
        with db_manager.get_session() as session:
            session.add_all(sample_posts)
            session.commit()

        result = filter_posts_with_existing_post_ids(sample_posts, db=db_manager)

        assert len(result) == 0

    def test_filter_with_session_parameter(self, db_manager, sample_posts):
        """Test filtering using session parameter instead of db parameter."""
        with db_manager.get_session() as session:
            # Add one post to database
            session.add(sample_posts[0])
            session.commit()

            result = filter_posts_with_existing_post_ids(sample_posts, session=session)

            assert len(result) == 2
            assert {post.platform_id for post in result} == {"post_2", "post_3"}

    def test_filter_with_postmodel_objects(self, db_manager, sample_posts):
        """Test filtering with PostModel objects instead of DBPost objects."""
        # Add one post to database
        with db_manager.get_session() as session:
            session.add(sample_posts[0])
            session.commit()

        # Convert to PostModel objects
        post_models = [post.model() for post in sample_posts]

        result = filter_posts_with_existing_post_ids(post_models, db=db_manager)

        assert len(result) == 2
        assert all(isinstance(post, PostModel) for post in result)
        assert {post.platform_id for post in result} == {"post_2", "post_3"}


class TestFilterPpitemsWithExistingPostIds:
    """Test the filter_ppitems_with_existing_post_ids function."""

    def test_filter_with_no_existing_ppitems(self, db_manager, sample_ppitems):
        """Test filtering when no ppitems exist in database."""
        result = filter_ppitems_with_existing_post_ids(sample_ppitems, db=db_manager)

        assert len(result) == 3
        assert all(isinstance(item, DBPostProcessItem) for item in result)
        assert {item.platform_id for item in result} == {"ppitem_1", "ppitem_2", "ppitem_3"}

    def test_filter_with_some_existing_ppitems(self, db_manager, sample_ppitems):
        """Test filtering when some ppitems already exist in database."""
        # Add one ppitem to database
        with db_manager.get_session() as session:
            session.add(sample_ppitems[0])  # Add "ppitem_1"
            session.commit()

        result = filter_ppitems_with_existing_post_ids(sample_ppitems, db=db_manager)

        assert len(result) == 2
        assert {item.platform_id for item in result} == {"ppitem_2", "ppitem_3"}

    def test_filter_with_all_existing_ppitems(self, db_manager, sample_ppitems):
        """Test filtering when all ppitems already exist in database."""
        # Add all ppitems to database
        with db_manager.get_session() as session:
            session.add_all(sample_ppitems)
            session.commit()

        result = filter_ppitems_with_existing_post_ids(sample_ppitems, db=db_manager)

        assert len(result) == 0

    def test_filter_with_session_parameter(self, db_manager, sample_ppitems):
        """Test filtering using session parameter instead of db parameter."""
        with db_manager.get_session() as session:
            # Add one ppitem to database
            session.add(sample_ppitems[0])
            session.commit()

            result = filter_ppitems_with_existing_post_ids(sample_ppitems, session=session)

            assert len(result) == 2
            assert {item.platform_id for item in result} == {"ppitem_2", "ppitem_3"}

    def test_filter_with_postprocessmodel_objects(self, db_manager, sample_ppitems):
        """Test filtering with PostProcessModel objects instead of DBPostProcessItem objects."""
        # Add one ppitem to database
        with db_manager.get_session() as session:
            session.add(sample_ppitems[0])
            session.commit()

        # Convert to PostProcessModel objects
        ppitem_models = [item.model() for item in sample_ppitems]

        result = filter_ppitems_with_existing_post_ids(ppitem_models, db=db_manager)

        assert len(result) == 2
        assert all(isinstance(item, PostProcessModel) for item in result)
        assert {item.platform_id for item in result} == {"ppitem_2", "ppitem_3"}


class TestFilterFunctionsIndependence:
    """Test that the two filter functions work independently."""

    def test_posts_and_ppitems_independent(self, db_manager, sample_posts, sample_ppitems):
        """Test that posts and ppitems with same platform_id are filtered independently."""
        # Create posts and ppitems with overlapping platform_ids
        posts = [
            DBPost(platform="test", platform_id="item_1", date_created=datetime.now()),
            DBPost(platform="test", platform_id="item_2", date_created=datetime.now())
        ]
        ppitems = [
            DBPostProcessItem(platform_id="item_1", input={}, output={}),
            DBPostProcessItem(platform_id="item_3", input={}, output={})
        ]

        # Add only one post to database
        with db_manager.get_session() as session:
            session.add(posts[0])  # Add post with platform_id "item_1"
            session.commit()

        # Filter posts - should exclude "item_1"
        filtered_posts = filter_posts_with_existing_post_ids(posts, db=db_manager)
        assert len(filtered_posts) == 1
        assert filtered_posts[0].platform_id == "item_2"

        # Filter ppitems - should include both since no ppitems exist in database
        filtered_ppitems = filter_ppitems_with_existing_post_ids(ppitems, db=db_manager)
        assert len(filtered_ppitems) == 2
        assert {item.platform_id for item in filtered_ppitems} == {"item_1", "item_3"}