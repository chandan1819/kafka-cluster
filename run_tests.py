#!/usr/bin/env python3
"""
Comprehensive test runner for Local Kafka Manager.

This script provides different test execution modes:
- Unit tests only
- Integration tests only
- Performance tests only
- All tests
- Coverage reporting
- Parallel execution
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd, description=""):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"Running: {description or ' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print(f"✅ {description or 'Command'} completed successfully")
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"❌ {description or 'Command'} failed with exit code {e.returncode}")
        return e.returncode
    except FileNotFoundError:
        print(f"❌ Command not found: {cmd[0]}")
        return 1


def run_unit_tests(args):
    """Run unit tests."""
    cmd = [
        "python", "-m", "pytest",
        "-m", "not integration and not performance",
        "--tb=short",
        "-v"
    ]
    
    if args.coverage:
        cmd.extend([
            "--cov=src",
            "--cov-report=html:htmlcov",
            "--cov-report=term-missing",
            "--cov-fail-under=80"
        ])
    
    if args.parallel:
        cmd.extend(["-n", "auto"])
    
    if args.verbose:
        cmd.append("-vv")
    
    return run_command(cmd, "Unit Tests")


def run_integration_tests(args):
    """Run integration tests."""
    cmd = [
        "python", "-m", "pytest",
        "-m", "integration",
        "--tb=short",
        "-v"
    ]
    
    if args.verbose:
        cmd.append("-vv")
    
    # Integration tests should not run in parallel by default
    # as they may interfere with each other
    
    return run_command(cmd, "Integration Tests")


def run_performance_tests(args):
    """Run performance tests."""
    cmd = [
        "python", "-m", "pytest",
        "-m", "performance",
        "--tb=short",
        "-v",
        "--benchmark-only",
        "--benchmark-sort=mean"
    ]
    
    if args.verbose:
        cmd.append("-vv")
    
    if args.benchmark_save:
        cmd.extend(["--benchmark-save", args.benchmark_save])
    
    return run_command(cmd, "Performance Tests")


def run_all_tests(args):
    """Run all tests."""
    cmd = [
        "python", "-m", "pytest",
        "--tb=short",
        "-v"
    ]
    
    if args.coverage:
        cmd.extend([
            "--cov=src",
            "--cov-report=html:htmlcov",
            "--cov-report=term-missing",
            "--cov-fail-under=70"  # Lower threshold for all tests
        ])
    
    if args.parallel:
        # Only run unit tests in parallel
        cmd.extend(["-n", "auto", "-m", "not integration and not performance"])
        
        # Run integration and performance tests separately
        integration_result = run_integration_tests(args)
        performance_result = run_performance_tests(args)
        
        unit_result = run_command(cmd, "Unit Tests (Parallel)")
        
        return max(unit_result, integration_result, performance_result)
    
    if args.verbose:
        cmd.append("-vv")
    
    return run_command(cmd, "All Tests")


def run_linting(args):
    """Run code linting."""
    commands = [
        (["python", "-m", "black", "--check", "src", "tests"], "Black Code Formatting Check"),
        (["python", "-m", "isort", "--check-only", "src", "tests"], "Import Sorting Check"),
        (["python", "-m", "flake8", "src", "tests"], "Flake8 Linting"),
    ]
    
    if args.mypy:
        commands.append((["python", "-m", "mypy", "src"], "MyPy Type Checking"))
    
    results = []
    for cmd, description in commands:
        result = run_command(cmd, description)
        results.append(result)
    
    return max(results) if results else 0


def generate_test_report(args):
    """Generate comprehensive test report."""
    cmd = [
        "python", "-m", "pytest",
        "--html=test_report.html",
        "--self-contained-html",
        "--tb=short",
        "-v"
    ]
    
    if args.coverage:
        cmd.extend([
            "--cov=src",
            "--cov-report=html:htmlcov",
            "--cov-report=xml:coverage.xml"
        ])
    
    return run_command(cmd, "Test Report Generation")


def check_dependencies():
    """Check if required dependencies are installed."""
    required_packages = [
        "pytest",
        "pytest-asyncio",
        "pytest-mock",
        "pytest-cov",
        "black",
        "isort",
        "flake8"
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ Missing required packages: {', '.join(missing_packages)}")
        print("Install them with: pip install -e .[dev]")
        return False
    
    return True


def main():
    """Main test runner function."""
    parser = argparse.ArgumentParser(
        description="Comprehensive test runner for Local Kafka Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_tests.py --unit                    # Run unit tests only
  python run_tests.py --integration             # Run integration tests only
  python run_tests.py --performance             # Run performance tests only
  python run_tests.py --all --coverage          # Run all tests with coverage
  python run_tests.py --lint                    # Run linting only
  python run_tests.py --unit --parallel         # Run unit tests in parallel
  python run_tests.py --report                  # Generate HTML test report
        """
    )
    
    # Test type selection
    test_group = parser.add_mutually_exclusive_group(required=True)
    test_group.add_argument("--unit", action="store_true", help="Run unit tests only")
    test_group.add_argument("--integration", action="store_true", help="Run integration tests only")
    test_group.add_argument("--performance", action="store_true", help="Run performance tests only")
    test_group.add_argument("--all", action="store_true", help="Run all tests")
    test_group.add_argument("--lint", action="store_true", help="Run linting only")
    test_group.add_argument("--report", action="store_true", help="Generate test report")
    
    # Test options
    parser.add_argument("--coverage", action="store_true", help="Generate coverage report")
    parser.add_argument("--parallel", action="store_true", help="Run tests in parallel")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--mypy", action="store_true", help="Include MyPy type checking")
    parser.add_argument("--benchmark-save", help="Save benchmark results with given name")
    
    # Environment options
    parser.add_argument("--no-deps-check", action="store_true", help="Skip dependency check")
    
    args = parser.parse_args()
    
    # Check dependencies unless skipped
    if not args.no_deps_check and not check_dependencies():
        return 1
    
    # Change to project root directory
    project_root = Path(__file__).parent
    os.chdir(project_root)
    
    # Run selected test type
    if args.unit:
        return run_unit_tests(args)
    elif args.integration:
        return run_integration_tests(args)
    elif args.performance:
        return run_performance_tests(args)
    elif args.all:
        return run_all_tests(args)
    elif args.lint:
        return run_linting(args)
    elif args.report:
        return generate_test_report(args)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())