#!/bin/bash

# Local Kafka Manager Installation Script
# This script checks for and installs all required dependencies

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PYTHON_MIN_VERSION="3.8"
DOCKER_MIN_VERSION="20.0"
DOCKER_COMPOSE_MIN_VERSION="2.0"

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

# Version comparison function
version_compare() {
    if [[ $1 == $2 ]]; then
        return 0
    fi
    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++)); do
        ver1[i]=0
    done
    for ((i=0; i<${#ver1[@]}; i++)); do
        if [[ -z ${ver2[i]} ]]; then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]})); then
            return 1
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]})); then
            return 2
        fi
    done
    return 0
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check Python installation and version
check_python() {
    log_info "Checking Python installation..."
    
    if ! command_exists python3; then
        log_error "Python 3 is not installed"
        log_info "Please install Python 3.8 or higher:"
        log_info "  - macOS: brew install python3"
        log_info "  - Ubuntu/Debian: sudo apt-get install python3 python3-pip"
        log_info "  - CentOS/RHEL: sudo yum install python3 python3-pip"
        return 1
    fi
    
    local python_version=$(python3 --version | cut -d' ' -f2)
    version_compare $python_version $PYTHON_MIN_VERSION
    local result=$?
    
    if [[ $result -eq 2 ]]; then
        log_error "Python version $python_version is too old. Minimum required: $PYTHON_MIN_VERSION"
        return 1
    fi
    
    log_success "Python $python_version is installed and compatible"
    return 0
}

# Check pip installation
check_pip() {
    log_info "Checking pip installation..."
    
    if ! command_exists pip3; then
        log_error "pip3 is not installed"
        log_info "Please install pip3:"
        log_info "  - macOS: python3 -m ensurepip --upgrade"
        log_info "  - Ubuntu/Debian: sudo apt-get install python3-pip"
        log_info "  - CentOS/RHEL: sudo yum install python3-pip"
        return 1
    fi
    
    log_success "pip3 is installed"
    return 0
}

# Check Docker installation and version
check_docker() {
    log_info "Checking Docker installation..."
    
    if ! command_exists docker; then
        log_error "Docker is not installed"
        log_info "Please install Docker:"
        log_info "  - macOS: Download Docker Desktop from https://docker.com/products/docker-desktop"
        log_info "  - Ubuntu: sudo apt-get install docker.io"
        log_info "  - CentOS/RHEL: sudo yum install docker"
        return 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info >/dev/null 2>&1; then
        log_error "Docker daemon is not running"
        log_info "Please start Docker:"
        log_info "  - macOS: Start Docker Desktop application"
        log_info "  - Linux: sudo systemctl start docker"
        return 1
    fi
    
    local docker_version=$(docker --version | grep -oE '[0-9]+\.[0-9]+' | head -1)
    version_compare $docker_version $DOCKER_MIN_VERSION
    local result=$?
    
    if [[ $result -eq 2 ]]; then
        log_error "Docker version $docker_version is too old. Minimum required: $DOCKER_MIN_VERSION"
        return 1
    fi
    
    log_success "Docker $docker_version is installed and running"
    return 0
}

# Check Docker Compose installation and version
check_docker_compose() {
    log_info "Checking Docker Compose installation..."
    
    # Check for Docker Compose V2 (docker compose)
    if docker compose version >/dev/null 2>&1; then
        local compose_version=$(docker compose version --short)
        version_compare $compose_version $DOCKER_COMPOSE_MIN_VERSION
        local result=$?
        
        if [[ $result -eq 2 ]]; then
            log_error "Docker Compose version $compose_version is too old. Minimum required: $DOCKER_COMPOSE_MIN_VERSION"
            return 1
        fi
        
        log_success "Docker Compose $compose_version is installed"
        return 0
    fi
    
    # Check for Docker Compose V1 (docker-compose)
    if command_exists docker-compose; then
        local compose_version=$(docker-compose --version | grep -oE '[0-9]+\.[0-9]+' | head -1)
        version_compare $compose_version $DOCKER_COMPOSE_MIN_VERSION
        local result=$?
        
        if [[ $result -eq 2 ]]; then
            log_error "Docker Compose version $compose_version is too old. Minimum required: $DOCKER_COMPOSE_MIN_VERSION"
            return 1
        fi
        
        log_success "Docker Compose $compose_version is installed"
        return 0
    fi
    
    log_error "Docker Compose is not installed"
    log_info "Please install Docker Compose:"
    log_info "  - macOS: Included with Docker Desktop"
    log_info "  - Linux: sudo apt-get install docker-compose-plugin"
    return 1
}

# Check available disk space
check_disk_space() {
    log_info "Checking available disk space..."
    
    local available_space
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        available_space=$(df -h . | awk 'NR==2 {print $4}' | sed 's/G//')
    else
        # Linux
        available_space=$(df -h . | awk 'NR==2 {print $4}' | sed 's/G//')
    fi
    
    # Convert to numeric (remove any non-numeric characters)
    available_space=$(echo $available_space | sed 's/[^0-9.]//g')
    
    # Use awk for floating point comparison (more portable than bc)
    if awk "BEGIN {exit !($available_space < 2)}"; then
        log_warning "Low disk space: ${available_space}GB available. Recommended: 2GB+"
        log_info "Docker images will require approximately 1.5GB of space"
    else
        log_success "Sufficient disk space available: ${available_space}GB"
    fi
}

# Install Python dependencies
install_python_dependencies() {
    log_info "Installing Python dependencies..."
    
    # Create virtual environment if it doesn't exist
    if [[ ! -d "venv" ]]; then
        log_info "Creating Python virtual environment..."
        python3 -m venv venv
    fi
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip
    
    # Install dependencies
    if [[ -f "requirements.txt" ]]; then
        pip install -r requirements.txt
    else
        log_error "requirements.txt not found"
        return 1
    fi
    
    log_success "Python dependencies installed successfully"
    return 0
}

# Pull Docker images
pull_docker_images() {
    log_info "Pulling Docker images..."
    
    local images=(
        "confluentinc/cp-kafka:7.4.0"
        "confluentinc/cp-kafka-rest:7.4.0"
        "provectuslabs/kafka-ui:latest"
    )
    
    for image in "${images[@]}"; do
        log_info "Pulling $image..."
        if docker pull "$image"; then
            log_success "Successfully pulled $image"
        else
            log_error "Failed to pull $image"
            return 1
        fi
    done
    
    log_success "All Docker images pulled successfully"
    return 0
}

# Verify Docker images
verify_docker_images() {
    log_info "Verifying Docker images..."
    
    local images=(
        "confluentinc/cp-kafka:7.4.0"
        "confluentinc/cp-kafka-rest:7.4.0"
        "provectuslabs/kafka-ui:latest"
    )
    
    for image in "${images[@]}"; do
        if docker image inspect "$image" >/dev/null 2>&1; then
            log_success "✓ $image"
        else
            log_error "✗ $image not found"
            return 1
        fi
    done
    
    log_success "All Docker images verified"
    return 0
}

# Create necessary directories
create_directories() {
    log_info "Creating necessary directories..."
    
    local directories=(
        "logs"
        "data"
    )
    
    for dir in "${directories[@]}"; do
        if [[ ! -d "$dir" ]]; then
            mkdir -p "$dir"
            log_success "Created directory: $dir"
        fi
    done
}

# Main installation function
main() {
    echo "========================================"
    echo "Local Kafka Manager Installation Script"
    echo "========================================"
    echo ""
    
    local failed=0
    
    # Check prerequisites
    check_python || failed=1
    check_pip || failed=1
    check_docker || failed=1
    check_docker_compose || failed=1
    check_disk_space
    
    if [[ $failed -eq 1 ]]; then
        echo ""
        log_error "Prerequisites check failed. Please resolve the issues above and run the script again."
        exit 1
    fi
    
    echo ""
    log_success "All prerequisites satisfied!"
    echo ""
    
    # Install dependencies
    install_python_dependencies || exit 1
    create_directories
    pull_docker_images || exit 1
    verify_docker_images || exit 1
    
    echo ""
    echo "========================================"
    log_success "Installation completed successfully!"
    echo "========================================"
    echo ""
    echo "Next steps:"
    echo "1. Run './start.sh' to start the Kafka stack"
    echo "2. Run 'source venv/bin/activate' to activate the Python environment"
    echo "3. Run 'python src/main.py' to start the REST API server"
    echo ""
    echo "Services will be available at:"
    echo "  - REST API: http://localhost:8000"
    echo "  - Kafka Broker: localhost:9092"
    echo "  - Kafka REST Proxy: http://localhost:8082"
    echo "  - Kafka UI: http://localhost:8080"
    echo ""
}

# Run main function
main "$@"