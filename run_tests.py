#!/usr/bin/env python3
"""Simple test runner for the big5_databases package."""

import sys
import subprocess
from pathlib import Path


def run_tests(test_pattern: str = "test_commands.py", verbose: bool = True):
    """Run tests using pytest."""
    test_dir = Path(__file__).parent / "test"

    cmd = [
        sys.executable, "-m", "pytest",
        str(test_dir / test_pattern)
    ]

    if verbose:
        cmd.append("-v")

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def run_specific_test(test_name: str):
    """Run a specific test function."""
    test_dir = Path(__file__).parent / "test"

    cmd = [
        sys.executable, "-m", "pytest",
        f"{test_dir}/test_commands.py::{test_name}",
        "-v"
    ]

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Run specific test
        test_name = sys.argv[1]
        exit_code = run_specific_test(test_name)
    else:
        # Run all command tests
        exit_code = run_tests()

    sys.exit(exit_code)