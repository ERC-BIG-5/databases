import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
from typer.testing import CliRunner

from big5_databases.commands import app
from big5_databases.databases.meta_database import MetaDatabase
from big5_databases.databases.model_conversion import PlatformDatabaseModel
from big5_databases.databases.external import MetaDatabaseContentModel


runner = CliRunner()


@pytest.fixture
def temp_db_path():
    """Create a temporary database file for testing"""
    with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
        temp_path = Path(tmp.name)
    yield temp_path
    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def mock_meta_database():
    """Mock MetaDatabase for testing"""
    with patch('big5_databases.commands.MetaDatabase') as mock_db:
        mock_instance = MagicMock()
        mock_db.return_value = mock_instance

        # Create fully mocked database objects
        mock_db1 = MagicMock()
        mock_db1.id = 1
        mock_db1.platform = "twitter"
        mock_db1.name = "test_db1"
        mock_db1.db_path = Path("/fake/path1.sqlite")
        mock_db1.content = MagicMock()
        mock_db1.content.alternative_paths = {}

        mock_db2 = MagicMock()
        mock_db2.id = 2
        mock_db2.platform = "youtube"
        mock_db2.name = "test_db2"
        mock_db2.db_path = Path("/fake/path2.sqlite")
        mock_db2.content = MagicMock()
        mock_db2.content.alternative_paths = {}

        # Mock get_mgmt method for both databases
        mock_db_mgmt = MagicMock()
        mock_db_mgmt.metadata.name = "test_db"
        mock_db1.get_mgmt = MagicMock(return_value=mock_db_mgmt)
        mock_db2.get_mgmt = MagicMock(return_value=mock_db_mgmt)

        mock_instance.get_dbs.return_value = [mock_db1, mock_db2]
        mock_instance.get.return_value = mock_db1
        mock_instance.get_db_mgmt.return_value = mock_db_mgmt
        mock_instance.general_databases_status.return_value = [
            {"name": "test_db1", "platform": "twitter", "path": "/fake/path1.sqlite"},
            {"name": "test_db2", "platform": "youtube", "path": "/fake/path2.sqlite"}
        ]

        yield mock_instance


def test_status_command(mock_meta_database):
    """Test the status command"""
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    mock_meta_database.general_databases_status.assert_called_once()


def test_status_command_with_databases(mock_meta_database):
    """Test the status command with specific databases"""
    result = runner.invoke(app, ["status", "test_db1", "test_db2"])
    assert result.exit_code == 0
    mock_meta_database.general_databases_status.assert_called_once_with(
        ["test_db1", "test_db2"], True, False
    )


def test_add_command_success(temp_db_path, mock_meta_database):
    """Test the add command with valid database"""
    # Create an actual empty file to pass the exists check
    temp_db_path.touch()

    with patch('big5_databases.commands.PlatformDatabaseModel') as mock_model:
        mock_instance = MagicMock()
        mock_instance.exists.return_value = True
        mock_model.return_value = mock_instance

        result = runner.invoke(app, [
            "add", str(temp_db_path), "twitter", "test_db"
        ])

        assert result.exit_code == 0
        mock_meta_database.add_db.assert_called_once()


def test_add_command_nonexistent_db():
    """Test the add command with non-existent database"""
    result = runner.invoke(app, [
        "add", "/fake/nonexistent.sqlite", "twitter", "test_db"
    ])

    assert result.exit_code != 0
    assert "does not exist" in result.output or result.exception


def test_remove_command(mock_meta_database):
    """Test the remove command"""
    with patch('builtins.input', return_value='y'):
        result = runner.invoke(app, ["remove", "test_db1"])
        assert result.exit_code == 0
        mock_meta_database.delete.assert_called_once_with("test_db1")


def test_rename_command(mock_meta_database):
    """Test the rename command"""
    result = runner.invoke(app, ["rename", "test_db1", "new_name"])
    assert result.exit_code == 0
    mock_meta_database.rename.assert_called_once_with("test_db1", "new_name")


def test_set_path_command(mock_meta_database, temp_db_path):
    """Test the set_path command"""
    temp_db_path.touch()

    result = runner.invoke(app, ["set-path", "test_db1", str(temp_db_path)])
    assert result.exit_code == 0
    mock_meta_database.set_db_path.assert_called_once_with("test_db1", temp_db_path)


def test_set_alternative_path_command(mock_meta_database, temp_db_path):
    """Test the set_alternative_path command"""
    temp_db_path.touch()

    result = runner.invoke(app, [
        "set-alternative-path", "test_db1", "backup", str(temp_db_path)
    ])
    assert result.exit_code == 0
    mock_meta_database.set_alternative_path.assert_called_once()


def test_get_alternative_paths_command(mock_meta_database):
    """Test the get_alternative_paths command"""
    result = runner.invoke(app, ["get-alternative-paths", "test_db1"])
    assert result.exit_code == 0
    mock_meta_database.get.assert_called_once_with("test_db1")


def test_add_run_state_command(mock_meta_database):
    """Test the add_run_state command"""
    result = runner.invoke(app, [
        "add-run-state", "test_db1", "collect_posts"
    ])
    assert result.exit_code == 0
    mock_meta_database.add_run_state.assert_called_once()


def test_base_dbs_path_command():
    """Test the base_dbs_path command"""
    with patch('big5_databases.commands.SqliteSettings') as mock_settings:
        mock_settings.return_value.default_sqlite_dbs_base_path = Path("/fake/base/path")

        result = runner.invoke(app, ["base-dbs-path"])
        assert result.exit_code == 0
        assert "/fake/base/path" in result.output


def test_collected_per_day_command(mock_meta_database):
    """Test the collected_per_day command"""
    # Mock the db management and analytics
    mock_db_mgmt = MagicMock()
    mock_meta_database.get_db_mgmt.return_value = mock_db_mgmt
    mock_db_mgmt.metadata.name = "test_db1"

    with patch('big5_databases.commands.get_collected_posts_by_period') as mock_func:
        mock_func.return_value = {
            "2023-01-01": {"tasks": 5, "found": 100, "added": 90}
        }

        result = runner.invoke(app, ["collected-per-day", "test_db1", "day"])
        assert result.exit_code == 0
        mock_func.assert_called_once()


def test_posts_per_period_command(mock_meta_database):
    """Test the posts_per_period command"""
    mock_db_mgmt = MagicMock()
    mock_meta_database.get_db_mgmt.return_value = mock_db_mgmt
    mock_db_mgmt.metadata.name = "test_db1"

    with patch('big5_databases.commands.get_posts_by_period') as mock_func:
        mock_func.return_value = [("2023-01-01", "Monday", 50)]

        result = runner.invoke(app, ["posts-per-period", "test_db1", "day"])
        assert result.exit_code == 0
        mock_func.assert_called_once()


def test_recent_collection_command(mock_meta_database):
    """Test the recent_collection command"""
    with patch('big5_databases.commands.get_collected_posts_by_period') as mock_func:
        mock_func.return_value = {
            "2023-01-01": {"tasks": 2, "found": 50, "added": 45}
        }

        result = runner.invoke(app, ["recent-collection", "3"])
        assert result.exit_code == 0


def test_compare_dbs_command():
    """Test the compare_dbs command"""
    with patch('big5_databases.commands.check_for_conflicts') as mock_func:
        result = runner.invoke(app, [
            "compare-dbs", "/fake/db1.sqlite", "/fake/db2.sqlite"
        ])
        assert result.exit_code == 0
        mock_func.assert_called_once_with("/fake/db1.sqlite", "/fake/db2.sqlite")


def test_copy_posts_metadata_content_command(mock_meta_database):
    """Test the copy_posts_metadata_content command"""
    result = runner.invoke(app, [
        "copy-posts-metadata-content", "test_db1", "backup", "media_paths", "to_alternative", "false"
    ])
    assert result.exit_code == 0
    mock_meta_database.copy_posts_metadata_content.assert_called_once_with(
        "test_db1", "backup", "media_paths", "to_alternative", False
    )


def test_remove_alternative_path_command(mock_meta_database):
    """Test the remove_alternative_path command"""
    result = runner.invoke(app, ["remove-alternative-path", "test_db1", "backup"])
    assert result.exit_code == 0
    mock_meta_database.edit.assert_called_once()


# Integration test - only run if you have actual test databases
@pytest.mark.skip(reason="Requires actual test database setup")
def test_integration_status_real_db():
    """Integration test with real database - skip by default"""
    result = runner.invoke(app, ["status"])
    # This would test against real databases if they exist
    assert result.exit_code == 0