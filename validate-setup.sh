#!/bin/bash

# Validation script for Local Kafka Manager setup
# This script validates that all installation and setup scripts work correctly

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if script exists and is executable
check_script() {
    local script=$1
    local description=$2
    
    if [[ -f "$script" ]]; then
        if [[ -x "$script" ]]; then
            log_success "$description script exists and is executable"
            return 0
        else
            log_error "$description script exists but is not executable"
            return 1
        fi
    else
        log_error "$description script not found: $script"
        return 1
    fi
}

# Validate script syntax
validate_syntax() {
    local script=$1
    local description=$2
    
    if bash -n "$script" 2>/dev/null; then
        log_success "$description script syntax is valid"
        return 0
    else
        log_error "$description script has syntax errors"
        return 1
    fi
}

# Check required files
check_required_files() {
    log_info "Checking required files..."
    
    local required_files=(
        "docker-compose.yml"
        "requirements.txt"
        "pyproject.toml"
        "README.md"
        "SETUP.md"
        "src/main.py"
    )
    
    local failed=0
    for file in "${required_files[@]}"; do
        if [[ -f "$file" ]]; then
            log_success "✓ $file"
        else
            log_error "✗ $file not found"
            failed=1
        fi
    done
    
    return $failed
}

# Validate Docker Compose configuration
validate_docker_compose() {
    log_info "Validating Docker Compose configuration..."
    
    if docker compose config >/dev/null 2>&1; then
        log_success "Docker Compose configuration is valid"
        return 0
    else
        log_error "Docker Compose configuration has errors"
        return 1
    fi
}

# Check Python requirements
validate_requirements() {
    log_info "Validating Python requirements..."
    
    if [[ -f "requirements.txt" ]]; then
        # Check if requirements file has valid format
        if grep -E "^[a-zA-Z0-9_-]+[>=<]" requirements.txt >/dev/null; then
            log_success "Requirements file format is valid"
            return 0
        else
            log_error "Requirements file format appears invalid"
            return 1
        fi
    else
        log_error "requirements.txt not found"
        return 1
    fi
}

# Test script help options
test_script_help() {
    local script=$1
    local description=$2
    
    log_info "Testing $description help option..."
    
    if "$script" --help >/dev/null 2>&1; then
        log_success "$description --help works correctly"
        return 0
    else
        log_error "$description --help failed"
        return 1
    fi
}

# Main validation function
main() {
    echo "========================================"
    echo "Local Kafka Manager Setup Validation"
    echo "========================================"
    echo ""
    
    local failed=0
    
    # Check required files
    check_required_files || failed=1
    
    echo ""
    log_info "Validating installation and setup scripts..."
    
    # Check script existence and permissions
    check_script "install.sh" "Installation" || failed=1
    check_script "start.sh" "Startup" || failed=1
    check_script "stop.sh" "Stop" || failed=1
    check_script "test-stack.sh" "Test" || failed=1
    
    echo ""
    log_info "Validating script syntax..."
    
    # Validate script syntax
    validate_syntax "install.sh" "Installation" || failed=1
    validate_syntax "start.sh" "Startup" || failed=1
    validate_syntax "stop.sh" "Stop" || failed=1
    validate_syntax "test-stack.sh" "Test" || failed=1
    
    echo ""
    log_info "Testing script help options..."
    
    # Test help options
    test_script_help "./install.sh" "Installation" || failed=1
    test_script_help "./start.sh" "Startup" || failed=1
    test_script_help "./stop.sh" "Stop" || failed=1
    
    echo ""
    log_info "Validating configuration files..."
    
    # Validate configurations
    validate_docker_compose || failed=1
    validate_requirements || failed=1
    
    echo ""
    
    if [[ $failed -eq 0 ]]; then
        log_success "All validation checks passed!"
        echo ""
        echo "Setup is ready. You can now run:"
        echo "  1. ./install.sh  - to install dependencies"
        echo "  2. ./start.sh    - to start all services"
        echo "  3. ./stop.sh     - to stop all services"
        echo ""
        return 0
    else
        log_error "Some validation checks failed. Please review the errors above."
        return 1
    fi
}

# Run main function
main "$@"