#!/bin/bash

# Production readiness validation script
# This script validates that the multi-cluster Kafka manager is ready for production deployment

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEPLOYMENT_TYPE="${1:-docker-compose}"
ENVIRONMENT="${2:-production}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Validation results
VALIDATION_RESULTS=()
CRITICAL_ISSUES=0
WARNING_ISSUES=0

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
    VALIDATION_RESULTS+=("PASS: $1")
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
    VALIDATION_RESULTS+=("WARN: $1")
    ((WARNING_ISSUES++))
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
    VALIDATION_RESULTS+=("FAIL: $1")
    ((CRITICAL_ISSUES++))
}

# Validation functions
validate_system_requirements() {
    log_info "Validating system requirements..."
    
    # Check available memory
    local available_memory_gb
    if command -v free &> /dev/null; then
        available_memory_gb=$(free -g | awk '/^Mem:/{print $2}')
    elif command -v vm_stat &> /dev/null; then
        # macOS
        local pages=$(vm_stat | grep "Pages free" | awk '{print $3}' | sed 's/\.//')
        available_memory_gb=$((pages * 4096 / 1024 / 1024 / 1024))
    else
        log_warning "Cannot determine available memory"
        return
    fi
    
    if [ "$available_memory_gb" -lt 8 ]; then
        log_error "Insufficient memory: ${available_memory_gb}GB (minimum: 8GB)"
    else
        log_success "Memory requirement met: ${available_memory_gb}GB"
    fi
    
    # Check available disk space
    local available_disk_gb=$(df -BG "$PROJECT_ROOT" | awk 'NR==2 {print $4}' | sed 's/G//')
    
    if [ "$available_disk_gb" -lt 50 ]; then
        log_error "Insufficient disk space: ${available_disk_gb}GB (minimum: 50GB)"
    else
        log_success "Disk space requirement met: ${available_disk_gb}GB"
    fi
    
    # Check CPU cores
    local cpu_cores
    if command -v nproc &> /dev/null; then
        cpu_cores=$(nproc)
    elif command -v sysctl &> /dev/null; then
        cpu_cores=$(sysctl -n hw.ncpu)
    else
        log_warning "Cannot determine CPU cores"
        return
    fi
    
    if [ "$cpu_cores" -lt 4 ]; then
        log_warning "Limited CPU cores: $cpu_cores (recommended: 4+)"
    else
        log_success "CPU requirement met: $cpu_cores cores"
    fi
}

validate_dependencies() {
    log_info "Validating dependencies..."
    
    local required_tools=("python3" "pip" "curl" "jq" "openssl")
    
    if [ "$DEPLOYMENT_TYPE" = "docker-compose" ]; then
        required_tools+=("docker" "docker-compose")
    elif [ "$DEPLOYMENT_TYPE" = "kubernetes" ]; then
        required_tools+=("kubectl" "helm")
    fi
    
    for tool in "${required_tools[@]}"; do
        if command -v "$tool" &> /dev/null; then
            local version
            case "$tool" in
                python3)
                    version=$(python3 --version 2>&1 | cut -d' ' -f2)
                    ;;
                docker)
                    version=$(docker --version | cut -d' ' -f3 | sed 's/,//')
                    ;;
                docker-compose)
                    version=$(docker-compose --version | cut -d' ' -f3 | sed 's/,//')
                    ;;
                kubectl)
                    version=$(kubectl version --client --short 2>/dev/null | cut -d' ' -f3 || echo "unknown")
                    ;;
                *)
                    version=$($tool --version 2>&1 | head -n1 | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -n1 || echo "unknown")
                    ;;
            esac
            log_success "$tool is installed (version: $version)"
        else
            log_error "$tool is not installed"
        fi
    done
    
    # Check Python version
    if command -v python3 &> /dev/null; then
        local python_version=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        if [[ $(echo "$python_version >= 3.8" | bc -l) -eq 1 ]]; then
            log_success "Python version requirement met: $python_version"
        else
            log_error "Python version too old: $python_version (minimum: 3.8)"
        fi
    fi
}

validate_docker_environment() {
    if [ "$DEPLOYMENT_TYPE" != "docker-compose" ]; then
        return 0
    fi
    
    log_info "Validating Docker environment..."
    
    # Check Docker daemon
    if docker info &> /dev/null; then
        log_success "Docker daemon is running"
        
        # Check Docker version
        local docker_version=$(docker version --format '{{.Server.Version}}')
        log_success "Docker version: $docker_version"
        
        # Check available Docker resources
        local docker_memory=$(docker system info --format '{{.MemTotal}}' 2>/dev/null || echo "0")
        if [ "$docker_memory" -gt 0 ]; then
            local docker_memory_gb=$((docker_memory / 1024 / 1024 / 1024))
            if [ "$docker_memory_gb" -lt 4 ]; then
                log_warning "Limited Docker memory: ${docker_memory_gb}GB (recommended: 4GB+)"
            else
                log_success "Docker memory allocation: ${docker_memory_gb}GB"
            fi
        fi
        
        # Check Docker Compose
        if docker-compose version &> /dev/null; then
            local compose_version=$(docker-compose version --short)
            log_success "Docker Compose version: $compose_version"
        else
            log_error "Docker Compose is not working"
        fi
        
    else
        log_error "Docker daemon is not running or accessible"
    fi
}

validate_kubernetes_environment() {
    if [ "$DEPLOYMENT_TYPE" != "kubernetes" ]; then
        return 0
    fi
    
    log_info "Validating Kubernetes environment..."
    
    # Check cluster connection
    if kubectl cluster-info &> /dev/null; then
        log_success "Kubernetes cluster is accessible"
        
        # Check cluster version
        local k8s_version=$(kubectl version --short 2>/dev/null | grep "Server Version" | cut -d' ' -f3 || echo "unknown")
        log_success "Kubernetes version: $k8s_version"
        
        # Check node resources
        local node_count=$(kubectl get nodes --no-headers | wc -l)
        log_success "Kubernetes nodes: $node_count"
        
        # Check available resources
        local total_cpu=$(kubectl top nodes 2>/dev/null | awk 'NR>1 {sum+=$3} END {print sum}' || echo "unknown")
        local total_memory=$(kubectl top nodes 2>/dev/null | awk 'NR>1 {sum+=$5} END {print sum}' || echo "unknown")
        
        if [ "$total_cpu" != "unknown" ] && [ "$total_memory" != "unknown" ]; then
            log_success "Cluster resources - CPU: ${total_cpu}m, Memory: ${total_memory}Mi"
        fi
        
        # Check storage classes
        local storage_classes=$(kubectl get storageclass --no-headers | wc -l)
        if [ "$storage_classes" -gt 0 ]; then
            log_success "Storage classes available: $storage_classes"
        else
            log_warning "No storage classes found"
        fi
        
        # Check RBAC permissions
        if kubectl auth can-i create pods --namespace=multi-cluster-kafka &> /dev/null; then
            log_success "RBAC permissions are sufficient"
        else
            log_error "Insufficient RBAC permissions"
        fi
        
    else
        log_error "Cannot connect to Kubernetes cluster"
    fi
}

validate_configuration_files() {
    log_info "Validating configuration files..."
    
    # Check main configuration files
    local config_files=(
        "docker-compose.multi-cluster.yml"
        "Dockerfile.multi-cluster"
        "Dockerfile.backup"
        "monitoring/prometheus.yml"
        "monitoring/rules/multi-cluster-alerts.yml"
    )
    
    if [ "$DEPLOYMENT_TYPE" = "kubernetes" ]; then
        config_files+=(
            "k8s/namespace.yaml"
            "k8s/configmap.yaml"
            "k8s/secrets.yaml"
            "k8s/postgres.yaml"
            "k8s/redis.yaml"
            "k8s/multi-cluster-manager.yaml"
        )
    fi
    
    for config_file in "${config_files[@]}"; do
        local file_path="${PROJECT_ROOT}/$config_file"
        if [ -f "$file_path" ]; then
            log_success "Configuration file exists: $config_file"
            
            # Validate YAML syntax for YAML files
            if [[ "$config_file" == *.yml ]] || [[ "$config_file" == *.yaml ]]; then
                if command -v python3 &> /dev/null; then
                    if python3 -c "import yaml; yaml.safe_load(open('$file_path'))" 2>/dev/null; then
                        log_success "YAML syntax valid: $config_file"
                    else
                        log_error "YAML syntax invalid: $config_file"
                    fi
                fi
            fi
        else
            log_error "Configuration file missing: $config_file"
        fi
    done
    
    # Check environment file
    local env_file="${PROJECT_ROOT}/.env.${ENVIRONMENT}"
    if [ -f "$env_file" ]; then
        log_success "Environment file exists: .env.${ENVIRONMENT}"
        
        # Check for required environment variables
        local required_vars=("POSTGRES_PASSWORD" "REDIS_PASSWORD" "SECRET_KEY")
        
        for var in "${required_vars[@]}"; do
            if grep -q "^${var}=" "$env_file"; then
                local value=$(grep "^${var}=" "$env_file" | cut -d'=' -f2)
                if [ -n "$value" ] && [ "$value" != "CHANGE_ME" ]; then
                    log_success "Environment variable set: $var"
                else
                    log_error "Environment variable not set or using default: $var"
                fi
            else
                log_error "Environment variable missing: $var"
            fi
        done
    else
        log_error "Environment file missing: .env.${ENVIRONMENT}"
    fi
}

validate_security_configuration() {
    log_info "Validating security configuration..."
    
    # Check SSL certificates (if configured)
    local ssl_cert_path="${PROJECT_ROOT}/nginx/ssl/cert.pem"
    local ssl_key_path="${PROJECT_ROOT}/nginx/ssl/key.pem"
    
    if [ -f "$ssl_cert_path" ] && [ -f "$ssl_key_path" ]; then
        log_success "SSL certificates found"
        
        # Validate certificate
        if openssl x509 -in "$ssl_cert_path" -text -noout &> /dev/null; then
            log_success "SSL certificate is valid"
            
            # Check certificate expiration
            local cert_expiry=$(openssl x509 -in "$ssl_cert_path" -enddate -noout | cut -d= -f2)
            local expiry_epoch=$(date -d "$cert_expiry" +%s 2>/dev/null || echo "0")
            local current_epoch=$(date +%s)
            local days_until_expiry=$(( (expiry_epoch - current_epoch) / 86400 ))
            
            if [ "$days_until_expiry" -lt 30 ]; then
                log_warning "SSL certificate expires in $days_until_expiry days"
            else
                log_success "SSL certificate valid for $days_until_expiry days"
            fi
        else
            log_error "SSL certificate is invalid"
        fi
    else
        log_warning "SSL certificates not configured (HTTP only)"
    fi
    
    # Check password strength (if environment file exists)
    local env_file="${PROJECT_ROOT}/.env.${ENVIRONMENT}"
    if [ -f "$env_file" ]; then
        local passwords=("POSTGRES_PASSWORD" "REDIS_PASSWORD" "SECRET_KEY")
        
        for password_var in "${passwords[@]}"; do
            local password=$(grep "^${password_var}=" "$env_file" | cut -d'=' -f2)
            if [ -n "$password" ]; then
                local password_length=${#password}
                if [ "$password_length" -lt 16 ]; then
                    log_warning "$password_var is too short ($password_length chars, recommended: 16+)"
                else
                    log_success "$password_var has adequate length ($password_length chars)"
                fi
            fi
        done
    fi
}

validate_monitoring_configuration() {
    log_info "Validating monitoring configuration..."
    
    # Check Prometheus configuration
    local prometheus_config="${PROJECT_ROOT}/monitoring/prometheus.yml"
    if [ -f "$prometheus_config" ]; then
        log_success "Prometheus configuration exists"
        
        # Validate Prometheus config syntax
        if command -v promtool &> /dev/null; then
            if promtool check config "$prometheus_config" &> /dev/null; then
                log_success "Prometheus configuration is valid"
            else
                log_error "Prometheus configuration is invalid"
            fi
        else
            log_warning "promtool not available, cannot validate Prometheus config"
        fi
    else
        log_error "Prometheus configuration missing"
    fi
    
    # Check alert rules
    local alert_rules="${PROJECT_ROOT}/monitoring/rules/multi-cluster-alerts.yml"
    if [ -f "$alert_rules" ]; then
        log_success "Alert rules exist"
        
        # Validate alert rules syntax
        if command -v promtool &> /dev/null; then
            if promtool check rules "$alert_rules" &> /dev/null; then
                log_success "Alert rules are valid"
            else
                log_error "Alert rules are invalid"
            fi
        fi
    else
        log_error "Alert rules missing"
    fi
    
    # Check Grafana dashboards
    local grafana_dashboards="${PROJECT_ROOT}/monitoring/grafana/dashboards"
    if [ -d "$grafana_dashboards" ]; then
        local dashboard_count=$(find "$grafana_dashboards" -name "*.json" | wc -l)
        if [ "$dashboard_count" -gt 0 ]; then
            log_success "Grafana dashboards found: $dashboard_count"
        else
            log_warning "No Grafana dashboards found"
        fi
    else
        log_warning "Grafana dashboards directory missing"
    fi
}

validate_backup_configuration() {
    log_info "Validating backup configuration..."
    
    # Check backup scripts
    local backup_scripts=(
        "scripts/backup/backup-database.sh"
        "scripts/backup/backup-data.sh"
        "scripts/backup/restore-backup.sh"
    )
    
    for script in "${backup_scripts[@]}"; do
        local script_path="${PROJECT_ROOT}/$script"
        if [ -f "$script_path" ]; then
            if [ -x "$script_path" ]; then
                log_success "Backup script exists and is executable: $script"
            else
                log_warning "Backup script exists but is not executable: $script"
            fi
        else
            log_warning "Backup script missing: $script"
        fi
    done
    
    # Check backup configuration
    local env_file="${PROJECT_ROOT}/.env.${ENVIRONMENT}"
    if [ -f "$env_file" ]; then
        if grep -q "BACKUP_S3_BUCKET=" "$env_file"; then
            local s3_bucket=$(grep "BACKUP_S3_BUCKET=" "$env_file" | cut -d'=' -f2)
            if [ -n "$s3_bucket" ]; then
                log_success "S3 backup bucket configured: $s3_bucket"
            else
                log_warning "S3 backup bucket not configured"
            fi
        fi
        
        if grep -q "AWS_ACCESS_KEY_ID=" "$env_file"; then
            local aws_key=$(grep "AWS_ACCESS_KEY_ID=" "$env_file" | cut -d'=' -f2)
            if [ -n "$aws_key" ]; then
                log_success "AWS credentials configured"
            else
                log_warning "AWS credentials not configured"
            fi
        fi
    fi
}

validate_performance_settings() {
    log_info "Validating performance settings..."
    
    # Check Docker Compose resource limits
    if [ "$DEPLOYMENT_TYPE" = "docker-compose" ]; then
        local compose_file="${PROJECT_ROOT}/docker-compose.multi-cluster.yml"
        if [ -f "$compose_file" ]; then
            if grep -q "deploy:" "$compose_file"; then
                log_success "Resource limits configured in Docker Compose"
            else
                log_warning "No resource limits configured in Docker Compose"
            fi
        fi
    fi
    
    # Check Kubernetes resource requests/limits
    if [ "$DEPLOYMENT_TYPE" = "kubernetes" ]; then
        local k8s_manifest="${PROJECT_ROOT}/k8s/multi-cluster-manager.yaml"
        if [ -f "$k8s_manifest" ]; then
            if grep -q "resources:" "$k8s_manifest"; then
                log_success "Resource requests/limits configured in Kubernetes"
            else
                log_warning "No resource requests/limits configured in Kubernetes"
            fi
        fi
    fi
    
    # Check application performance settings
    local config_files=("config/production.yml" "k8s/configmap.yaml")
    
    for config_file in "${config_files[@]}"; do
        local file_path="${PROJECT_ROOT}/$config_file"
        if [ -f "$file_path" ]; then
            if grep -q "workers:" "$file_path" || grep -q "WORKERS" "$file_path"; then
                log_success "Worker configuration found in $config_file"
            fi
            
            if grep -q "pool_size:" "$file_path" || grep -q "POOL_SIZE" "$file_path"; then
                log_success "Database pool configuration found in $config_file"
            fi
        fi
    done
}

generate_report() {
    log_info "Generating validation report..."
    
    local report_file="${PROJECT_ROOT}/production-readiness-report.txt"
    
    cat > "$report_file" << EOF
Multi-Cluster Kafka Manager - Production Readiness Report
=========================================================

Generated: $(date)
Deployment Type: $DEPLOYMENT_TYPE
Environment: $ENVIRONMENT

Summary:
--------
Total Checks: ${#VALIDATION_RESULTS[@]}
Critical Issues: $CRITICAL_ISSUES
Warnings: $WARNING_ISSUES

Detailed Results:
----------------
EOF
    
    for result in "${VALIDATION_RESULTS[@]}"; do
        echo "$result" >> "$report_file"
    done
    
    cat >> "$report_file" << EOF

Recommendations:
---------------
EOF
    
    if [ $CRITICAL_ISSUES -gt 0 ]; then
        cat >> "$report_file" << EOF
❌ CRITICAL ISSUES FOUND - DO NOT DEPLOY TO PRODUCTION
   Please resolve all critical issues before proceeding with deployment.
EOF
    elif [ $WARNING_ISSUES -gt 0 ]; then
        cat >> "$report_file" << EOF
⚠️  WARNINGS FOUND - REVIEW BEFORE DEPLOYMENT
   Consider addressing warnings for optimal production performance.
EOF
    else
        cat >> "$report_file" << EOF
✅ PRODUCTION READY
   All validation checks passed. System is ready for production deployment.
EOF
    fi
    
    log_info "Report saved to: $report_file"
}

# Main validation function
main() {
    log_info "Multi-Cluster Kafka Manager - Production Readiness Validation"
    log_info "============================================================"
    log_info "Deployment Type: $DEPLOYMENT_TYPE"
    log_info "Environment: $ENVIRONMENT"
    log_info ""
    
    validate_system_requirements
    validate_dependencies
    validate_docker_environment
    validate_kubernetes_environment
    validate_configuration_files
    validate_security_configuration
    validate_monitoring_configuration
    validate_backup_configuration
    validate_performance_settings
    
    log_info ""
    log_info "Validation Summary:"
    log_info "=================="
    log_info "Total Checks: ${#VALIDATION_RESULTS[@]}"
    log_info "Critical Issues: $CRITICAL_ISSUES"
    log_info "Warnings: $WARNING_ISSUES"
    
    generate_report
    
    if [ $CRITICAL_ISSUES -gt 0 ]; then
        log_error "CRITICAL ISSUES FOUND - DO NOT DEPLOY TO PRODUCTION"
        exit 1
    elif [ $WARNING_ISSUES -gt 0 ]; then
        log_warning "WARNINGS FOUND - REVIEW BEFORE DEPLOYMENT"
        exit 2
    else
        log_success "PRODUCTION READY - All validation checks passed"
        exit 0
    fi
}

# Run main function
main "$@"