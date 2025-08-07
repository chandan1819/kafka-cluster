#!/bin/bash

# Multi-cluster Kafka Manager Installation Script
# This script provides automated installation and setup for the multi-cluster system

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="${INSTALL_DIR:-$SCRIPT_DIR}"
PYTHON_CMD="${PYTHON_CMD:-python3}"
VENV_DIR="${VENV_DIR:-venv}"

# Functions
print_banner() {
    echo -e "${CYAN}"
    echo "╔══════════════════════════════════════════════════════════════════════════════╗"
    echo "║                                                                              ║"
    echo "║               🚀 Multi-cluster Kafka Manager Installer                      ║"
    echo "║                                                                              ║"
    echo "║    Automated installation script for Multi-cluster Kafka Manager           ║"
    echo "║    This script will set up your multi-cluster Kafka environment            ║"
    echo "║                                                                              ║"
    echo "╚══════════════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_help() {
    echo -e "${BLUE}Multi-cluster Kafka Manager Installation Script${NC}"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -q, --quick             Quick installation with defaults"
    echo "  -d, --dev               Development setup (single cluster)"
    echo "  -t, --test              Testing setup (multiple clusters)"
    echo "  -p, --prod              Production setup (high availability)"
    echo "  -c, --check             Check system requirements only"
    echo "  -v, --validate          Validate existing installation"
    echo "  -b, --backup            Create backup before installation"
    echo "  -r, --restore PATH      Restore from backup"
    echo "  -m, --migrate           Migrate from single-cluster setup"
    echo "  --repair                Repair broken installation"
    echo "  --clean                 Clean installation (remove existing)"
    echo "  --no-venv               Skip virtual environment creation"
    echo "  --no-deps               Skip dependency installation"
    echo "  --no-docker             Skip Docker checks"
    echo "  --install-dir DIR       Set installation directory"
    echo "  --python-cmd CMD        Set Python command (default: python3)"
    echo ""
    echo "Environment Variables:"
    echo "  INSTALL_DIR             Installation directory (default: current)"
    echo "  PYTHON_CMD              Python command (default: python3)"
    echo "  VENV_DIR                Virtual environment directory (default: venv)"
    echo ""
    echo "Examples:"
    echo "  $0 --quick              # Quick installation with defaults"
    echo "  $0 --dev                # Development setup"
    echo "  $0 --prod --backup      # Production setup with backup"
    echo "  $0 --check              # Check requirements only"
    echo "  $0 --migrate            # Migrate from single-cluster"
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "$1 is not installed or not in PATH"
        return 1
    fi
    return 0
}

check_python() {
    log_step "Checking Python installation..."
    
    if ! check_command "$PYTHON_CMD"; then
        log_error "Python 3.8+ is required but not found"
        log_info "Please install Python 3.8 or later and try again"
        return 1
    fi
    
    # Check Python version
    PYTHON_VERSION=$($PYTHON_CMD -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)
    
    if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 8 ]); then
        log_error "Python 3.8+ is required, found Python $PYTHON_VERSION"
        return 1
    fi
    
    log_info "Python $PYTHON_VERSION found"
    return 0
}

check_docker() {
    if [ "$SKIP_DOCKER" = "true" ]; then
        log_warn "Skipping Docker checks (--no-docker specified)"
        return 0
    fi
    
    log_step "Checking Docker installation..."
    
    if ! check_command "docker"; then
        log_error "Docker is required but not found"
        log_info "Please install Docker and try again"
        log_info "Visit: https://docs.docker.com/get-docker/"
        return 1
    fi
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        log_error "Docker is installed but not running"
        log_info "Please start Docker and try again"
        return 1
    fi
    
    # Check Docker Compose
    if ! docker compose version &> /dev/null && ! docker-compose --version &> /dev/null; then
        log_error "Docker Compose is required but not found"
        log_info "Please install Docker Compose and try again"
        return 1
    fi
    
    DOCKER_VERSION=$(docker --version | cut -d' ' -f3 | cut -d',' -f1)
    log_info "Docker $DOCKER_VERSION found and running"
    return 0
}

check_system_resources() {
    log_step "Checking system resources..."
    
    # Check available memory
    if command -v free &> /dev/null; then
        AVAILABLE_MEM=$(free -m | awk 'NR==2{printf "%.0f", $7}')
        if [ "$AVAILABLE_MEM" -lt 2048 ]; then
            log_warn "Low available memory: ${AVAILABLE_MEM}MB (4GB recommended)"
        else
            log_info "Available memory: ${AVAILABLE_MEM}MB"
        fi
    fi
    
    # Check available disk space
    AVAILABLE_DISK=$(df -BG "$INSTALL_DIR" | awk 'NR==2 {print $4}' | sed 's/G//')
    if [ "$AVAILABLE_DISK" -lt 10 ]; then
        log_warn "Low available disk space: ${AVAILABLE_DISK}GB (20GB recommended)"
    else
        log_info "Available disk space: ${AVAILABLE_DISK}GB"
    fi
    
    return 0
}

check_requirements() {
    log_step "Checking system requirements..."
    
    local requirements_met=true
    
    if ! check_python; then
        requirements_met=false
    fi
    
    if ! check_docker; then
        requirements_met=false
    fi
    
    check_system_resources
    
    if [ "$requirements_met" = true ]; then
        log_info "✅ All system requirements met"
        return 0
    else
        log_error "❌ System requirements not met"
        return 1
    fi
}

create_virtual_environment() {
    if [ "$SKIP_VENV" = "true" ]; then
        log_warn "Skipping virtual environment creation (--no-venv specified)"
        return 0
    fi
    
    log_step "Creating Python virtual environment..."
    
    cd "$INSTALL_DIR"
    
    if [ -d "$VENV_DIR" ]; then
        log_info "Virtual environment already exists, removing..."
        rm -rf "$VENV_DIR"
    fi
    
    $PYTHON_CMD -m venv "$VENV_DIR"
    
    # Activate virtual environment
    source "$VENV_DIR/bin/activate"
    
    # Upgrade pip
    pip install --upgrade pip
    
    log_info "Virtual environment created and activated"
    return 0
}

install_dependencies() {
    if [ "$SKIP_DEPS" = "true" ]; then
        log_warn "Skipping dependency installation (--no-deps specified)"
        return 0
    fi
    
    log_step "Installing Python dependencies..."
    
    cd "$INSTALL_DIR"
    
    # Activate virtual environment if it exists
    if [ -f "$VENV_DIR/bin/activate" ] && [ "$SKIP_VENV" != "true" ]; then
        source "$VENV_DIR/bin/activate"
    fi
    
    # Install dependencies
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
    elif [ -f "pyproject.toml" ]; then
        pip install -e .
    else
        log_warn "No requirements.txt or pyproject.toml found, installing basic dependencies..."
        pip install fastapi uvicorn pydantic pyyaml psutil aiofiles
    fi
    
    log_info "Dependencies installed successfully"
    return 0
}

run_setup_wizard() {
    log_step "Running setup wizard..."
    
    cd "$INSTALL_DIR"
    
    # Activate virtual environment if it exists
    if [ -f "$VENV_DIR/bin/activate" ] && [ "$SKIP_VENV" != "true" ]; then
        source "$VENV_DIR/bin/activate"
    fi
    
    # Determine wizard arguments based on options
    local wizard_args=""
    
    case "$SETUP_TYPE" in
        "quick")
            wizard_args="--quick-dev"
            ;;
        "dev")
            wizard_args="--quick-dev"
            ;;
        "test")
            wizard_args="--quick-test"
            ;;
        "prod")
            wizard_args="--quick-prod"
            ;;
        "validate")
            wizard_args="--validate"
            ;;
        "migrate")
            wizard_args="--migrate"
            ;;
        "repair")
            wizard_args="--repair"
            ;;
        "restore")
            wizard_args="--restore $RESTORE_PATH"
            ;;
        *)
            # Interactive wizard
            wizard_args=""
            ;;
    esac
    
    # Add backup option if requested
    if [ "$CREATE_BACKUP" = "true" ] && [ "$SETUP_TYPE" != "validate" ]; then
        # Create backup first
        log_info "Creating backup before installation..."
        $PYTHON_CMD setup_wizard.py --backup
    fi
    
    # Run the setup wizard
    if [ -f "setup_wizard.py" ]; then
        $PYTHON_CMD setup_wizard.py $wizard_args
    else
        log_error "setup_wizard.py not found"
        return 1
    fi
    
    return $?
}

create_startup_scripts() {
    log_step "Creating startup scripts..."
    
    cd "$INSTALL_DIR"
    
    # Create enhanced start script
    cat > start_multi_cluster.sh << 'EOF'
#!/bin/bash

# Multi-cluster Kafka Manager Start Script
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${VENV_DIR:-venv}"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

cd "$SCRIPT_DIR"

log_info "🚀 Starting Multi-cluster Kafka Manager..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    log_error "Docker is not running. Please start Docker first."
    exit 1
fi

# Activate virtual environment if it exists
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
    log_info "Activated virtual environment"
fi

# Start services
if [ -f "docker-compose.yml" ]; then
    log_info "Starting Docker services..."
    docker-compose up -d
    
    # Wait for services to be ready
    log_info "Waiting for services to be ready..."
    sleep 10
    
    # Check service health
    if docker-compose ps | grep -q "Up"; then
        log_info "✅ Services started successfully"
        echo ""
        log_info "🌐 Web interface: http://localhost:8000"
        log_info "📊 Monitoring: http://localhost:8000/monitoring"
        log_info "📖 API docs: http://localhost:8000/docs"
        echo ""
        log_info "Use './stop_multi_cluster.sh' to stop services"
    else
        log_error "❌ Some services failed to start"
        docker-compose logs
        exit 1
    fi
else
    log_error "docker-compose.yml not found. Please run the setup wizard first."
    exit 1
fi
EOF

    # Create enhanced stop script
    cat > stop_multi_cluster.sh << 'EOF'
#!/bin/bash

# Multi-cluster Kafka Manager Stop Script
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cd "$SCRIPT_DIR"

log_info "🛑 Stopping Multi-cluster Kafka Manager..."

if [ -f "docker-compose.yml" ]; then
    docker-compose down
    log_info "✅ Services stopped successfully"
else
    log_error "docker-compose.yml not found"
    exit 1
fi
EOF

    # Make scripts executable
    chmod +x start_multi_cluster.sh
    chmod +x stop_multi_cluster.sh
    
    log_info "Startup scripts created: start_multi_cluster.sh, stop_multi_cluster.sh"
    return 0
}

clean_installation() {
    log_step "Cleaning existing installation..."
    
    cd "$INSTALL_DIR"
    
    # Stop services if running
    if [ -f "docker-compose.yml" ]; then
        log_info "Stopping existing services..."
        docker-compose down 2>/dev/null || true
    fi
    
    # Remove virtual environment
    if [ -d "$VENV_DIR" ]; then
        log_info "Removing virtual environment..."
        rm -rf "$VENV_DIR"
    fi
    
    # Remove generated files (but keep source code)
    log_info "Removing generated configuration files..."
    rm -f docker-compose.yml docker-compose.override.yml .env
    rm -rf data config cluster_registry monitoring_data
    rm -f start_multi_cluster.sh stop_multi_cluster.sh
    
    log_info "Installation cleaned"
    return 0
}

main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                print_help
                exit 0
                ;;
            -q|--quick)
                SETUP_TYPE="quick"
                shift
                ;;
            -d|--dev)
                SETUP_TYPE="dev"
                shift
                ;;
            -t|--test)
                SETUP_TYPE="test"
                shift
                ;;
            -p|--prod)
                SETUP_TYPE="prod"
                shift
                ;;
            -c|--check)
                check_requirements
                exit $?
                ;;
            -v|--validate)
                SETUP_TYPE="validate"
                shift
                ;;
            -b|--backup)
                CREATE_BACKUP="true"
                shift
                ;;
            -r|--restore)
                SETUP_TYPE="restore"
                RESTORE_PATH="$2"
                shift 2
                ;;
            -m|--migrate)
                SETUP_TYPE="migrate"
                shift
                ;;
            --repair)
                SETUP_TYPE="repair"
                shift
                ;;
            --clean)
                clean_installation
                exit 0
                ;;
            --no-venv)
                SKIP_VENV="true"
                shift
                ;;
            --no-deps)
                SKIP_DEPS="true"
                shift
                ;;
            --no-docker)
                SKIP_DOCKER="true"
                shift
                ;;
            --install-dir)
                INSTALL_DIR="$2"
                shift 2
                ;;
            --python-cmd)
                PYTHON_CMD="$2"
                shift 2
                ;;
            *)
                log_error "Unknown option: $1"
                print_help
                exit 1
                ;;
        esac
    done
    
    # Print banner
    print_banner
    
    # Check requirements (unless skipping specific checks)
    if [ "$SETUP_TYPE" != "validate" ]; then
        if ! check_requirements; then
            log_error "System requirements not met. Use --check to see details."
            exit 1
        fi
    fi
    
    # Change to install directory
    cd "$INSTALL_DIR"
    log_info "Installation directory: $INSTALL_DIR"
    
    # Create virtual environment and install dependencies
    if [ "$SETUP_TYPE" != "validate" ] && [ "$SETUP_TYPE" != "restore" ]; then
        create_virtual_environment
        install_dependencies
    fi
    
    # Run setup wizard
    if ! run_setup_wizard; then
        log_error "Setup wizard failed"
        exit 1
    fi
    
    # Create startup scripts (unless just validating)
    if [ "$SETUP_TYPE" != "validate" ]; then
        create_startup_scripts
    fi
    
    # Final message
    echo ""
    log_info "🎉 Installation completed successfully!"
    
    if [ "$SETUP_TYPE" != "validate" ]; then
        echo ""
        log_info "Next steps:"
        log_info "1. Start services: ./start_multi_cluster.sh"
        log_info "2. Access web interface: http://localhost:8000"
        log_info "3. Check the documentation for advanced configuration"
        echo ""
        log_info "For help: $0 --help"
    fi
}

# Run main function
main "$@"