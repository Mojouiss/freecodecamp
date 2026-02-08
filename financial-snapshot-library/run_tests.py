"""
Test runner script for the Financial Snapshot Library.

Provides convenient test execution with coverage reporting.
"""

import sys
import subprocess
from pathlib import Path


def run_tests(test_path=None, coverage=True, verbose=False):
    """
    Run tests with optional coverage.
    
    Args:
        test_path: Specific test file or directory to run
        coverage: Whether to generate coverage report
        verbose: Whether to show verbose output
    """
    # Build pytest command
    cmd = ["python", "-m", "pytest"]
    
    # Add coverage options
    if coverage:
        cmd.extend(["--cov=financial_snapshot", "--cov-report=term-missing", "--cov-report=html"])
    
    # Add verbosity
    if verbose:
        cmd.append("-v")
    
    # Add specific test path if provided
    if test_path:
        cmd.append(test_path)
    else:
        cmd.append("tests/")
    
    # Run tests
    print("Running tests...")
    print(f"Command: {' '.join(cmd)}\n")
    
    result = subprocess.run(cmd, cwd=Path(__file__).parent)
    
    return result.returncode


if __name__ == "__main__":
    # Parse simple command line arguments
    coverage = "--no-cov" not in sys.argv
    verbose = "-v" in sys.argv or "--verbose" in sys.argv
    
    # Get test path if provided
    test_path = None
    for arg in sys.argv[1:]:
        if not arg.startswith("-"):
            test_path = arg
            break
    
    exit_code = run_tests(test_path=test_path, coverage=coverage, verbose=verbose)
    sys.exit(exit_code)
