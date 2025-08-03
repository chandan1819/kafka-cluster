#!/bin/bash

# Local Kafka Manager Startup Script
# This script starts the entire Kafka stack and REST API

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
API_PORT=8000
KAFKA_PORT=9092
REST_PROXY_PORT=8082
UI_PORT=8080
HEALTH_CHECK_TIMEOUT=300  # 5 minutes
HEALTH_CHECK_INTERVAL=10  # 10 seconds

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

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if port is available
check_port() {
    local port=$1
    local service=$2
    
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        log_error "Port $port is already in use (required for $service)"
        log_info "Please stop the service using port $port or change the configuration"
        return 1
    fi
    return 0
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Docker
    if ! command_exists docker; then
        log_error "Docker is not installed. Please run ./install.sh first"
        return 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info >/dev/null 2>&1; then
        log_error "Docker daemon is not running. Please start Docker"
        return 1
    fi
    
    # Check Docker Compose
    if ! docker compose version >/dev/null 2>&1 && ! command_exists docker-compose; then
        log_error "Docker Compose is not installed. Please run ./install.sh first"
        return 1
    fi
    
    # Check Python virtual environment
    if [[ ! -d "venv" ]]; then
        log_error "Python virtual environment not found. Please run ./install.sh first"
        return 1
    fi
    
    # Check required files
    local required_files=(
        "docker-compose.yml"
        "src/main.py"
        "requirements.txt"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            log_error "Required file not found: $file"
            return 1
        fi
    done
    
    log_success "Prerequisites check passed"
    return 0
}

# Check port availability
check_ports() {
    log_info "Checking port availability..."
    
    local failed=0
    check_port $API_PORT "REST API" || failed=1
    check_port $KAFKA_PORT "Kafka Broker" || failed=1
    check_port $REST_PROXY_PORT "Kafka REST Proxy" || failed=1
    check_port $UI_PORT "Kafka UI" || failed=1
    
    if [[ $failed -eq 1 ]]; then
        return 1
    fi
    
    log_success "All required ports are available"
    return 0
}

# Start Docker Compose stack
start_docker_stack() {
    log_info "Starting Docker Compose stack..."
    
    # Use Docker Compose V2 if available, otherwise fall back to V1
    local compose_cmd="docker compose"
    if ! docker compose version >/dev/null 2>&1; then
        compose_cmd="docker-compose"
    fi
    
    # Start services in detached mode
    if $compose_cmd up -d; then
        log_success "Docker Compose stack started"
    else
        log_error "Failed to start Docker Compose stack"
        return 1
    fi
    
    return 0
}

# Wait for service to be healthy
wait_for_service() {
    local service_name=$1
    local health_check_cmd=$2
    local timeout=$3
    local interval=$4
    
    log_info "Waiting for $service_name to be healthy..."
    
    local elapsed=0
    while [[ $elapsed -lt $timeout ]]; do
        if eval "$health_check_cmd" >/dev/null 2>&1; then
            log_success "$service_name is healthy"
            return 0
        fi
        
        sleep $interval
        elapsed=$((elapsed + interval))
        
        # Show progress every 30 seconds
        if [[ $((elapsed % 30)) -eq 0 ]]; then
            log_info "Still waiting for $service_name... (${elapsed}s elapsed)"
        fi
    done
    
    log_error "$service_name failed to become healthy within ${timeout}s"
    return 1
}

# Check service health
check_service_health() {
    log_info "Checking service health..."
    
    # Wait for Kafka broker
    wait_for_service "Kafka Broker" \
        "docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list" \
        $HEALTH_CHECK_TIMEOUT $HEALTH_CHECK_INTERVAL || return 1
    
    # Wait for Kafka REST Proxy
    wait_for_service "Kafka REST Proxy" \
        "curl -f http://localhost:$REST_PROXY_PORT/topics" \
        $HEALTH_CHECK_TIMEOUT $HEALTH_CHECK_INTERVAL || return 1
    
    # Wait for Kafka UI
    wait_for_service "Kafka UI" \
        "curl -f http://localhost:$UI_PORT" \
        $HEALTH_CHECK_TIMEOUT $HEALTH_CHECK_INTERVAL || return 1
    
    log_success "All services are healthy"
    return 0
}

# Start REST API server
start_api_server() {
    log_info "Starting REST API server..."
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Check if main.py exists and is executable
    if [[ ! -f "src/main.py" ]]; then
        log_error "src/main.py not found"
        return 1
    fi
    
    # Start the API server in background
    log_info "Starting FastAPI server on port $API_PORT..."
    nohup python -m uvicorn src.main:app --host 0.0.0.0 --port $API_PORT --reload > logs/api.log 2>&1 &
    local api_pid=$!
    
    # Save PID for later cleanup
    echo $api_pid > .api.pid
    
    # Wait a moment for the server to start
    sleep 5
    
    # Check if the server is running
    if kill -0 $api_pid 2>/dev/null; then
        log_success "REST API server started (PID: $api_pid)"
        
        # Test API endpoint
        local retries=10
        while [[ $retries -gt 0 ]]; do
            if curl -f http://localhost:$API_PORT/docs >/dev/null 2>&1; then
                log_success "REST API is responding"
                break
            fi
            sleep 2
            retries=$((retries - 1))
        done
        
        if [[ $retries -eq 0 ]]; then
            log_warning "REST API server started but not responding to health checks"
        fi
    else
        log_error "REST API server failed to start"
        return 1
    fi
    
    return 0
}

# Run connectivity tests
run_connectivity_tests() {
    log_info "Running connectivity tests..."
    
    if [[ -f "test-stack.sh" ]]; then
        chmod +x test-stack.sh
        if ./test-stack.sh; then
            log_success "Connectivity tests passed"
        else
            log_warning "Some connectivity tests failed, but services may still be functional"
        fi
    else
        log_warning "test-stack.sh not found, skipping connectivity tests"
    fi
}

# Display service information
show_service_info() {
    echo ""
    echo "========================================"
    log_success "Local Kafka Manager started successfully!"
    echo "========================================"
    echo ""
    echo "Services are now available at:"
    echo "  🌐 REST API:          http://localhost:$API_PORT"
    echo "  📚 API Documentation: http://localhost:$API_PORT/docs"
    echo "  🔧 Kafka Broker:      localhost:$KAFKA_PORT"
    echo "  🌐 Kafka REST Proxy:  http://localhost:$REST_PROXY_PORT"
    echo "  🖥️  Kafka UI:          http://localhost:$UI_PORT"
    echo ""
    echo "Useful commands:"
    echo "  📊 Check status:       curl http://localhost:$API_PORT/catalog"
    echo "  📝 View API logs:      tail -f logs/api.log"
    echo "  🐳 View Docker logs:   docker compose logs -f"
    echo "  🛑 Stop services:      ./stop.sh"
    echo ""
    echo "Log files:"
    echo "  📄 API logs:           logs/api.log"
    echo "  📄 Docker logs:        docker compose logs"
    echo ""
}

# Cleanup function for graceful shutdown
cleanup() {
    log_info "Shutting down services..."
    
    # Stop API server if PID file exists
    if [[ -f ".api.pid" ]]; then
        local api_pid=$(cat .api.pid)
        if kill -0 $api_pid 2>/dev/null; then
            kill $api_pid
            log_info "Stopped REST API server"
        fi
        rm -f .api.pid
    fi
    
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Main startup function
main() {
    echo "========================================"
    echo "Local Kafka Manager Startup Script"
    echo "========================================"
    echo ""
    
    # Create logs directory if it doesn't exist
    mkdir -p logs
    
    # Check prerequisites
    check_prerequisites || exit 1
    
    # Check port availability
    check_ports || exit 1
    
    # Start Docker stack
    start_docker_stack || exit 1
    
    # Wait for services to be healthy
    check_service_health || {
        log_error "Service health check failed. Check Docker logs with: docker compose logs"
        exit 1
    }
    
    # Start REST API server
    start_api_server || {
        log_error "Failed to start REST API server. Check logs/api.log for details"
        exit 1
    }
    
    # Run connectivity tests
    run_connectivity_tests
    
    # Show service information
    show_service_info
    
    # Keep the script running
    log_info "Services are running. Press Ctrl+C to stop all services."
    
    # Wait for interrupt signal
    while true; do
        sleep 10
        
        # Check if API server is still running
        if [[ -f ".api.pid" ]]; then
            local api_pid=$(cat .api.pid)
            if ! kill -0 $api_pid 2>/dev/null; then
                log_error "REST API server has stopped unexpectedly"
                break
            fi
        fi
    done
}

# Parse command line arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Start the Local Kafka Manager stack"
        echo ""
        echo "Options:"
        echo "  -h, --help     Show this help message"
        echo "  --api-only     Start only the REST API (assumes Docker stack is running)"
        echo "  --docker-only  Start only the Docker stack (no REST API)"
        echo ""
        exit 0
        ;;
    --api-only)
        log_info "Starting REST API only..."
        check_prerequisites || exit 1
        start_api_server || exit 1
        show_service_info
        ;;
    --docker-only)
        log_info "Starting Docker stack only..."
        check_prerequisites || exit 1
        check_ports || exit 1
        start_docker_stack || exit 1
        check_service_health || exit 1
        run_connectivity_tests
        echo ""
        log_success "Docker stack started successfully!"
        echo "Run './start.sh --api-only' to start the REST API"
        ;;
    *)
        main "$@"
        ;;
esac