# Big5 Databases Tests

This directory contains tests for the big5_databases package, specifically focused on CLI command testing using Typer's testing framework.

## Test Files

- **`test_commands.py`** - Tests for all CLI commands in the databases package
- **`conftest.py`** - Shared pytest fixtures and test utilities
- **`test_db_mgmt.py`** - Existing tests for database management (already present)

## Running Tests

### Run all command tests:
```bash
# From the big5_databases directory
python run_tests.py

# Or using pytest directly
pytest test/test_commands.py -v
```

### Run specific test:
```bash
# Using the test runner
python run_tests.py test_status_command

# Or using pytest directly
pytest test/test_commands.py::test_status_command -v
```

### Run tests with different options:
```bash
# Run only non-integration tests
pytest test/test_commands.py -v -m "not integration"

# Run with coverage (if coverage is installed)
pytest test/test_commands.py --cov=big5_databases.commands

# Run with output capture disabled (see print statements)
pytest test/test_commands.py -s
```

## Test Coverage

The tests cover all major CLI commands:

### Database Management Commands:
- ✅ `status` - Get database status and statistics
- ✅ `add` - Add new database to meta database
- ✅ `remove` - Remove database from meta database
- ✅ `rename` - Rename a database
- ✅ `set-path` - Change database path
- ✅ `base-dbs-path` - Show base database path

### Analytics Commands:
- ✅ `collected-per-day` - Show collection statistics per day
- ✅ `posts-per-period` - Show posts by time period
- ✅ `recent-collection` - Show recent collection stats
- ✅ `compare-dbs` - Compare two databases

### Alternative Path Commands:
- ✅ `set-alternative-path` - Set alternative database path
- ✅ `get-alternative-paths` - Get alternative paths
- ✅ `remove-alternative-path` - Remove alternative path
- ✅ `copy-posts-metadata-content` - Copy metadata between databases

### Run State Commands:
- ✅ `add-run-state` - Add a run state to database

## Test Strategy

The tests use mocking extensively to avoid requiring real databases:

1. **`mock_meta_database`** fixture mocks the MetaDatabase class
2. **`temp_db_path`** fixture provides temporary files for path validation
3. **`mock_console`** prevents Rich output during tests

### Key Testing Patterns:

```python
def test_command_name(mock_meta_database):
    \"\"\"Test description\"\"\"
    result = runner.invoke(app, ["command", "args"])
    assert result.exit_code == 0
    mock_meta_database.method.assert_called_once_with("expected", "args")
```

## Integration Tests

Some tests are marked as integration tests and require real database setup:

```python
@pytest.mark.skip(reason="Requires actual test database setup")
def test_integration_status_real_db():
    # These test against real databases when enabled
```

To run integration tests, remove the `@pytest.mark.skip` decorator and ensure test databases exist.

## Dependencies

Tests require:
- `pytest` - Test framework
- `typer` - For CliRunner
- Standard library `unittest.mock` - For mocking

Install test dependencies:
```bash
pip install pytest typer
```