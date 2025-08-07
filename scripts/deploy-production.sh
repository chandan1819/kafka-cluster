#!/bin/bash

# Production deployment script for multi-cluster Kafka manager
# This script handles the complete deployment process including validation,
# backup, deployment, and verification.

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEPLOYMENT_TYPE="${1:-docker-compose}"  # docker-compose or kubernetes
ENVIRONMENT="${2:-production}"
VERSION="${3:-latest}"

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

# Error handling
cleanup() {
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log_error "Deployment failed with exit code $exit_code"
        log_info "Check logs for details: ${PROJECT_ROOT}/logs/deployment.log"
    fi
    exit $exit_code
}

trap cleanup EXIT

# Create logs directory
mkdir -p "${PROJECT_ROOT}/logs"
DEPLOYMENT_LOG="${PROJECT_ROOT}/logs/deployment-$(date +%Y%m%d-%H%M%S).log"

# Redirect all output to log file while still showing on console
exec > >(tee -a "$DEPLOYMENT_LOG")
exec 2>&1

log_info "Starting production deployment"
log_info "Deployment type: $DEPLOYMENT_TYPE"
log_info "Environment: $ENVIRONMENT"
log_info "Version: $VERSION"
log_info "Log file: $DEPLOYMENT_LOG"

# Pre-deployment validation
validate_prerequisites() {
    log_info "Validating prerequisites..."
    
    # Check required tools
    local required_tools=()
    
    if [ "$DEPLOYMENT_TYPE" = "docker-compose" ]; then
        required_tools+=("docker" "docker-compose")
    elif [ "$DEPLOYMENT_TYPE" = "kubernetes" ]; then
        required_tools+=("kubectl" "helm")
    fi
    
    required_tools+=("curl" "jq" "openssl")
    
    for tool in "${required_tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            log_error "Required tool '$tool' is not installed"
            exit 1
        fi
    done
    
    # Check Docker daemon
    if [ "$DEPLOYMENT_TYPE" = "docker-compose" ]; then
        if ! docker info &> /dev/null; then
            log_error "Docker daemon is not running"
            exit 1
        fi
    fi
    
    # Check Kubernetes connection
    if [ "$DEPLOYMENT_TYPE" = "kubernetes" ]; then
        if ! kubectl cluster-info &> /dev/null; then
            log_error "Cannot connect to Kubernetes cluster"
            exit 1
        fi
    fi
    
    log_success "Prerequisites validation passed"
}

# Environment setup
setup_environment() {
    log_info "Setting up environment..."
    
    # Create environment file if it doesn't exist
    local env_file="${PROJECT_ROOT}/.env.${ENVIRONMENT}"
    
    if [ ! -f "$env_file" ]; then
        log_info "Creating environment file: $env_file"
        
        # Generate secure passwords
        local postgres_password=$(openssl rand -base64 32)
        local redis_password=$(openssl rand -base64 32)
        local secret_key=$(openssl rand -base64 64)
        local grafana_password=$(openssl rand -base64 16)
        
        cat > "$env_file" << EOF
# Multi-Cluster Kafka Manager Production Environment
ENVIRONMENT=${ENVIRONMENT}
VERSION=${VERSION}

# Database
POSTGRES_PASSWORD=${postgres_password}

# Cache
REDIS_PASSWORD=${redis_password}

# Security
SECRET_KEY=${secret_key}

# Monitoring
GRAFANA_PASSWORD=${grafana_password}

# Backup (configure as needed)
BACKUP_S3_BUCKET=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=

# Build info
BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
VCS_REF=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
EOF
        
        log_warning "Generated new environment file with random passwords"
        log_warning "Please review and update $env_file as needed"
        log_warning "Backup passwords: POSTGRES_PASSWORD, REDIS_PASSWORD, SECRET_KEY"
    fi
    
    # Source environment file
    set -a
    source "$env_file"
    set +a
    
    log_success "Environment setup completed"
}

# Build application images
build_images() {
    if [ "$DEPLOYMENT_TYPE" != "docker-compose" ]; then
        return 0
    fi
    
    log_info "Building application images..."
    
    cd "$PROJECT_ROOT"
    
    # Build main application image
    docker build \
        --build-arg BUILD_DATE="$BUILD_DATE" \
        --build-arg VERSION="$VERSION" \
        --build-arg VCS_REF="$VCS_REF" \
        -f Dockerfile.multi-cluster \
        -t "multi-cluster-kafka-manager:${VERSION}" \
        -t "multi-cluster-kafka-manager:latest" \
        .
    
    # Build backup service image
    docker build \
        -f Dockerfile.backup \
        -t "multi-cluster-backup:${VERSION}" \
        -t "multi-cluster-backup:latest" \
        .
    
    log_success "Images built successfully"
}

# Deploy with Docker Compose
deploy_docker_compose() {
    log_info "Deploying with Docker Compose..."
    
    cd "$PROJECT_ROOT"
    
    # Create necessary directories
    mkdir -p data/{clusters,backups} logs
    
    # Deploy services
    docker-compose -f docker-compose.multi-cluster.yml \
        --env-file ".env.${ENVIRONMENT}" \
        up -d --remove-orphans
    
    log_success "Docker Compose deployment completed"
}

# Deploy to Kubernetes
deploy_kubernetes() {
    log_info "Deploying to Kubernetes..."
    
    cd "$PROJECT_ROOT"
    
    # Create namespace
    kubectl apply -f k8s/namespace.yaml
    
    # Create secrets
    log_info "Creating Kubernetes secrets..."
    
    # Update secrets with actual values
    kubectl create secret generic multi-cluster-secrets \
        --namespace=multi-cluster-kafka \
        --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
        --from-literal=REDIS_PASSWORD="$REDIS_PASSWORD" \
        --from-literal=SECRET_KEY="$SECRET_KEY" \
        --from-literal=GRAFANA_PASSWORD="$GRAFANA_PASSWORD" \
        --dry-run=client -o yaml | kubectl apply -f -
    
    kubectl create secret generic postgres-secret \
        --namespace=multi-cluster-kafka \
        --from-literal=POSTGRES_DB="multi_cluster_db" \
        --from-literal=POSTGRES_USER="kafka_user" \
        --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
        --dry-run=client -o yaml | kubectl apply -f -
    
    # Apply configurations
    kubectl apply -f k8s/configmap.yaml
    
    # Deploy database
    kubectl apply -f k8s/postgres.yaml
    kubectl apply -f k8s/redis.yaml
    
    # Wait for database to be ready
    log_info "Waiting for database to be ready..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=postgres \
        --namespace=multi-cluster-kafka --timeout=300s
    
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=redis \
        --namespace=multi-cluster-kafka --timeout=300s
    
    # Deploy application
    kubectl apply -f k8s/multi-cluster-manager.yaml
    
    # Wait for application to be ready
    log_info "Waiting for application to be ready..."
    kubectl wait --for=condition=available deployment/multi-cluster-manager \
        --namespace=multi-cluster-kafka --timeout=600s
    
    log_success "Kubernetes deployment completed"
}

# Health check
health_check() {
    log_info "Performing health checks..."
    
    local base_url
    local max_attempts=30
    local attempt=1
    
    if [ "$DEPLOYMENT_TYPE" = "docker-compose" ]; then
        base_url="http://localhost:8000"
    else
        # For Kubernetes, use port-forward for health check
        kubectl port-forward -n multi-cluster-kafka service/multi-cluster-manager-service 8000:8000 &
        local port_forward_pid=$!
        sleep 5
        base_url="http://localhost:8000"
    fi
    
    while [ $attempt -le $max_attempts ]; do
        log_info "Health check attempt $attempt/$max_attempts"
        
        if curl -f -s "$base_url/health" > /dev/null; then
            log_success "Application is healthy"
            
            # Kill port-forward if it was started
            if [ -n "${port_forward_pid:-}" ]; then
                kill $port_forward_pid 2>/dev/null || true
            fi
            
            return 0
        fi
        
        sleep 10
        ((attempt++))
    done
    
    # Kill port-forward if it was started
    if [ -n "${port_forward_pid:-}" ]; then
        kill $port_forward_pid 2>/dev/null || true
    fi
    
    log_error "Health check failed after $max_attempts attempts"
    return 1
}

# Post-deployment verification
verify_deployment() {
    log_info "Verifying deployment..."
    
    local base_url
    if [ "$DEPLOYMENT_TYPE" = "docker-compose" ]; then
        base_url="http://localhost:8000"
    else
        kubectl port-forward -n multi-cluster-kafka service/multi-cluster-manager-service 8000:8000 &
        local port_forward_pid=$!
        sleep 5
        base_url="http://localhost:8000"
    fi
    
    # Test API endpoints
    local endpoints=("/health" "/ready" "/api/v1/clusters" "/api/v1/templates")
    
    for endpoint in "${endpoints[@]}"; do
        log_info "Testing endpoint: $endpoint"
        
        if ! curl -f -s "$base_url$endpoint" > /dev/null; then
            log_error "Endpoint $endpoint is not responding"
            
            # Kill port-forward if it was started
            if [ -n "${port_forward_pid:-}" ]; then
                kill $port_forward_pid 2>/dev/null || true
            fi
            
            return 1
        fi
    done
    
    # Kill port-forward if it was started
    if [ -n "${port_forward_pid:-}" ]; then
        kill $port_forward_pid 2>/dev/null || true
    fi
    
    log_success "Deployment verification completed"
}

# Backup current deployment (if exists)
backup_current_deployment() {
    log_info "Backing up current deployment..."
    
    local backup_dir="${PROJECT_ROOT}/backups/deployment-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$backup_dir"
    
    if [ "$DEPLOYMENT_TYPE" = "docker-compose" ]; then
        # Backup Docker volumes
        if docker volume ls | grep -q multi-cluster; then
            log_info "Backing up Docker volumes..."
            docker run --rm -v multi_cluster_data:/data -v "$backup_dir:/backup" \
                alpine tar czf /backup/data.tar.gz -C /data .
        fi
    else
        # Backup Kubernetes resources
        log_info "Backing up Kubernetes resources..."
        kubectl get all -n multi-cluster-kafka -o yaml > "$backup_dir/resources.yaml"
        
        # Backup persistent volumes
        kubectl get pvc -n multi-cluster-kafka -o yaml > "$backup_dir/pvc.yaml"
    fi
    
    log_success "Backup completed: $backup_dir"
}

# Main deployment function
main() {
    log_info "Multi-Cluster Kafka Manager Production Deployment"
    log_info "================================================"
    
    validate_prerequisites
    setup_environment
    backup_current_deployment
    build_images
    
    if [ "$DEPLOYMENT_TYPE" = "docker-compose" ]; then
        deploy_docker_compose
    elif [ "$DEPLOYMENT_TYPE" = "kubernetes" ]; then
        deploy_kubernetes
    else
        log_error "Unknown deployment type: $DEPLOYMENT_TYPE"
        exit 1
    fi
    
    health_check
    verify_deployment
    
    log_success "Deployment completed successfully!"
    log_info "Access the application at:"
    
    if [ "$DEPLOYMENT_TYPE" = "docker-compose" ]; then
        log_info "  - Main application: http://localhost:8000"
        log_info "  - Grafana dashboard: http://localhost:3000"
        log_info "  - Prometheus: http://localhost:9090"
    else
        log_info "  - Use kubectl port-forward to access services"
        log_info "  - kubectl port-forward -n multi-cluster-kafka service/multi-cluster-manager-service 8000:8000"
    fi
    
    log_info "Deployment log saved to: $DEPLOYMENT_LOG"
}

# Run main function
main "$@"