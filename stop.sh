#!/bin/bash

# Local Kafka Manager Stop Script
# This script stops the entire Kafka stack and REST API

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

# Stop REST API server
stop_api_server() {
    log_info "Stopping REST API server..."
    
    if [[ -f ".api.pid" ]]; then
        local api_pid=$(cat .api.pid)
        if kill -0 $api_pid 2>/dev/null; then
            kill $api_pid
            sleep 2
            
            # Force kill if still running
            if kill -0 $api_pid 2>/dev/null; then
                kill -9 $api_pid
                log_warning "Force killed REST API server"
            else
                log_success "REST API server stopped gracefully"
            fi
        else
            log_info "REST API server was not running"
        fi
        rm -f .api.pid
    else
        log_info "No API server PID file found"
    fi
}

# Stop Docker Compose stack
stop_docker_stack() {
    log_info "Stopping Docker Compose stack..."
    
    # Use Docker Compose V2 if available, otherwise fall back to V1
    local compose_cmd="docker compose"
    if ! docker compose version >/dev/null 2>&1; then
        compose_cmd="docker-compose"
    fi
    
    if $compose_cmd down; then
        log_success "Docker Compose stack stopped"
    else
        log_error "Failed to stop Docker Compose stack"
        return 1
    fi
    
    return 0
}

# Clean up Docker resources
cleanup_docker_resources() {
    log_info "Cleaning up Docker resources..."
    
    # Remove stopped containers
    local stopped_containers=$(docker ps -a -q --filter "status=exited" --filter "name=kafka")
    if [[ -n "$stopped_containers" ]]; then
        docker rm $stopped_containers
        log_success "Removed stopped containers"
    fi
    
    # Clean up networks (optional)
    local unused_networks=$(docker network ls -q --filter "dangling=true")
    if [[ -n "$unused_networks" ]]; then
        docker network rm $unused_networks 2>/dev/null || true
        log_success "Cleaned up unused networks"
    fi
}

# Main stop function
main() {
    echo "========================================"
    echo "Local Kafka Manager Stop Script"
    echo "========================================"
    echo ""
    
    # Stop REST API server
    stop_api_server
    
    # Stop Docker stack
    stop_docker_stack || exit 1
    
    # Optional cleanup
    if [[ "${1:-}" == "--cleanup" ]]; then
        cleanup_docker_resources
    fi
    
    echo ""
    log_success "All services stopped successfully!"
    echo ""
    
    if [[ "${1:-}" != "--cleanup" ]]; then
        echo "Note: Docker volumes are preserved. Use './stop.sh --cleanup' to remove containers and networks."
        echo "To remove all data including Kafka logs, run: docker volume rm \$(docker volume ls -q --filter name=kafka)"
    fi
}

# Parse command line arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Stop the Local Kafka Manager stack"
        echo ""
        echo "Options:"
        echo "  -h, --help     Show this help message"
        echo "  --cleanup      Also remove stopped containers and unused networks"
        echo "  --api-only     Stop only the REST API"
        echo "  --docker-only  Stop only the Docker stack"
        echo ""
        exit 0
        ;;
    --api-only)
        log_info "Stopping REST API only..."
        stop_api_server
        log_success "REST API stopped"
        ;;
    --docker-only)
        log_info "Stopping Docker stack only..."
        stop_docker_stack || exit 1
        log_success "Docker stack stopped"
        ;;
    *)
        main "$@"
        ;;
esac