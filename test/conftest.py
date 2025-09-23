"""Shared test fixtures and configuration for big5_databases tests."""

import tempfile
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from big5_databases.databases.model_conversion import PlatformDatabaseModel
from big5_databases.databases.external import MetaDatabaseContentModel


@pytest.fixture(scope="session")
def temp_test_dir():
    """Create a temporary directory for all tests in the session."""
    temp_dir = Path(tempfile.mkdtemp(prefix="big5_db_tests_"))
    yield temp_dir
    # Cleanup after all tests
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_db_file(temp_test_dir):
    """Create a temporary database file."""
    db_file = temp_test_dir / "test_db.sqlite"
    db_file.touch()
    yield db_file
    # Individual test cleanup happens automatically with temp_test_dir


@pytest.fixture
def sample_platform_db():
    """Create a sample PlatformDatabaseModel for testing."""
    return PlatformDatabaseModel(
        id=1,
        platform="twitter",
        name="sample_db",
        db_path=Path("/fake/sample.sqlite"),
        is_default=False,
        content=MetaDatabaseContentModel(
            tasks_states={"done": 100, "init": 5},
            post_count=105,
            file_size=1024000,
            last_modified=1234567890.0
        )
    )


@pytest.fixture
def mock_database_manager():
    """Mock DatabaseManager for testing."""
    manager = MagicMock()
    manager.metadata.name = "test_db"
    manager.db_exists.return_value = True
    return manager


@pytest.fixture
def mock_console():
    """Mock Rich console to avoid printing during tests."""
    with patch('big5_databases.commands.Console') as mock:
        yield mock.return_value


# Common test data
TEST_DATABASE_NAMES = ["test_db1", "test_db2", "sample_twitter", "sample_youtube"]

SAMPLE_DB_STATUS = [
    {
        "name": "test_db1",
        "platform": "twitter",
        "path": "/fake/test_db1.sqlite",
        "last mod": "2023-01-01 12:00",
        "total": "100",
        "size": "1 Mb",
        "done": "95",
        "init": "5"
    },
    {
        "name": "test_db2",
        "platform": "youtube",
        "path": "/fake/test_db2.sqlite",
        "last mod": "2023-01-02 15:30",
        "total": "50",
        "size": "2 Mb",
        "done": "48",
        "init": "2"
    }
]

SAMPLE_COLLECTED_POSTS = {
    "2023-01-01": {"tasks": 3, "found": 150, "added": 140},
    "2023-01-02": {"tasks": 2, "found": 75, "added": 70},
    "2023-01-03": {"tasks": 1, "found": 25, "added": 23}
}