# 🚀 Multi-Cluster Kafka Manager

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Docker](https://img.shields.io/badge/Docker-20.0+-blue.svg)](https://docker.com)
[![FastAPI](https://img.shields.io/badge/FastAPI-Latest-green.svg)](https://fastapi.tiangolo.com)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4.0-orange.svg)](https://kafka.apache.org)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A **production-ready**, enterprise-grade multi-cluster Kafka management solution that enables developers and operations teams to provision, control, and monitor multiple Kafka clusters from a single interface. **Perfect for local development, testing, staging, and production environments!**

> 🎯 **Perfect for**: Multi-environment development, testing pipelines, staging environments, production deployments, disaster recovery, and enterprise Kafka management

## 🆕 What's New in v2.0

- **🏢 Multi-Cluster Management**: Manage unlimited Kafka clusters from one interface
- **🔄 Advanced Operations**: Cluster cloning, snapshots, cross-cluster data migration
- **⏰ Automated Scheduling**: Schedule cluster operations, backups, and maintenance
- **🔐 Enterprise Security**: Role-based access control with API key management
- **📊 Enhanced Monitoring**: Comprehensive health monitoring with Prometheus integration
- **🎯 Resource Optimization**: Intelligent resource allocation and management
- **🔧 Configuration Management**: Version-controlled configurations with import/export
- **🌐 Production Ready**: Kubernetes deployment, Docker Compose, and bare metal support

## 🌟 Why Multi-Cluster Kafka Manager?

- **🔥 Zero Configuration**: One-command setup with automated health checks and installation wizard
- **🏢 Multi-Cluster Support**: Manage unlimited clusters across different environments
- **🎛️ Complete Control**: Full cluster lifecycle management via REST API and web interface
- **📊 Visual Management**: Beautiful web UI with advanced monitoring dashboards
- **🔄 Advanced Operations**: Cluster cloning, snapshots, cross-cluster data migration
- **⏰ Smart Automation**: Automated scheduling, backups, and maintenance operations
- **🔐 Enterprise Security**: Role-based access control, API key management, audit logging
- **🧪 Developer Friendly**: Perfect for testing event-driven architectures across environments
- **🐳 Deployment Flexible**: Docker Compose, Kubernetes, or bare metal deployment
- **📚 Self-Documenting**: Auto-generated interactive API documentation with examples
- **🔍 Production-Ready**: Enterprise-grade monitoring, alerting, and resource management

## 🚀 Core Features

### Multi-Cluster Management
- 🏢 **Unlimited Clusters**: Create and manage multiple Kafka clusters simultaneously
- 🎯 **Environment Support**: Development, testing, staging, and production environments
- 📋 **Cluster Templates**: Reusable templates for consistent deployments
- 🔄 **Cluster Registry**: Centralized registry with metadata and tagging
- 🌐 **Port Management**: Automatic port allocation and conflict resolution

### Advanced Operations
- 🔄 **Cluster Cloning**: Clone entire clusters with data and configurations
- 📸 **Snapshots & Backups**: Create point-in-time snapshots for disaster recovery
- 🔀 **Cross-Cluster Operations**: Migrate data and configurations between clusters
- ⏰ **Automated Scheduling**: Schedule start/stop, backups, and maintenance tasks
- 🔧 **Configuration Management**: Version-controlled configuration with import/export

### Enterprise Security & Access Control
- 🔐 **Role-Based Access Control**: Fine-grained permissions and user management
- 🔑 **API Key Management**: Secure API access with key rotation
- 📝 **Audit Logging**: Complete audit trail for compliance and security
- 🛡️ **Security Middleware**: Authentication and authorization layers
- 🔒 **Secure Communication**: HTTPS/TLS support for production deployments

### Monitoring & Observability
- 📊 **Real-time Monitoring**: Comprehensive health monitoring and metrics
- 🚨 **Intelligent Alerting**: Configurable alerts with multiple notification channels
- 📈 **Prometheus Integration**: Native Prometheus metrics and Grafana dashboards
- 🔍 **Enhanced Health Checks**: Multi-level health monitoring with auto-recovery
- 📋 **Resource Monitoring**: CPU, memory, disk, and network usage tracking

### Developer Experience
- 🚀 **One-click Setup** - Interactive installation wizard with multiple deployment options
- 🔧 **REST API** - Complete cluster lifecycle management with OpenAPI documentation
- 📊 **Topic Management** - Advanced topic operations with configuration management
- 💬 **Message Operations** - Produce and consume messages with schema support
- 🖥️ **Modern Web UI** - Responsive interface with real-time updates and dashboards
- 🐳 **Flexible Deployment** - Docker Compose, Kubernetes, or bare metal support

## 🏗️ Architecture

The Multi-Cluster Kafka Manager uses a modular, scalable architecture:

### Core Components
- **Multi-Cluster Manager** (Port 8000) - Central management API and web interface
- **Cluster Registry** - Centralized metadata store for all clusters
- **Resource Manager** - Intelligent resource allocation and optimization
- **Security Layer** - Authentication, authorization, and audit logging
- **Monitoring System** - Health monitoring with Prometheus integration

### Per-Cluster Components
- **Kafka Broker(s)** (Configurable ports) - Kafka clusters using KRaft mode
- **Kafka REST Proxy** (Configurable ports) - HTTP interface for Kafka operations
- **Kafka UI** (Configurable ports) - Web-based cluster management interface
- **JMX Monitoring** (Optional) - Metrics collection for monitoring

### Storage & Persistence
- **Configuration Storage** - File-based or database backend for cluster configurations
- **Metadata Database** - PostgreSQL or SQLite for cluster registry and audit logs
- **Backup Storage** - Local or cloud storage for snapshots and backups

### Deployment Options
- **Docker Compose** - Single-machine deployment with multiple clusters
- **Kubernetes** - Scalable deployment with Helm charts
- **Bare Metal** - Direct installation on servers or VMs

## 📋 Prerequisites

### System Requirements
- **Python 3.8+** - For the management server and CLI tools
- **Docker 20.0+** - For containerized cluster deployment
- **Docker Compose 2.0+** - For multi-service orchestration
- **20GB+ free disk space** - For Docker images, data, and backups
- **8GB+ RAM** - Recommended for multiple clusters (4GB minimum for single cluster)

### Optional Components
- **PostgreSQL 12+** - For production database backend (SQLite used by default)
- **Redis 6+** - For caching and session management
- **Prometheus** - For advanced monitoring and alerting
- **Grafana** - For monitoring dashboards

## 🚀 Quick Start

### Option 1: Interactive Installation (Recommended)

Run the interactive setup wizard:

```bash
# Clone the repository
git clone https://github.com/your-org/multi-cluster-kafka-manager.git
cd multi-cluster-kafka-manager

# Run the interactive setup wizard
python setup_wizard.py
```

The wizard will guide you through:
- System requirements checking
- Deployment scenario selection (dev/test/prod)
- Directory and security configuration
- Initial cluster creation

### Option 2: Quick Installation

For different deployment scenarios:

```bash
# Development setup (single cluster, minimal resources)
./install_multi_cluster.sh --dev

# Testing setup (multiple clusters)
./install_multi_cluster.sh --test

# Production setup (high availability, monitoring)
./install_multi_cluster.sh --prod --backup
```

### Option 3: Legacy Single-Cluster Mode

For backward compatibility with the original single-cluster setup:

```bash
# Install legacy mode
./install.sh

# Start single cluster
./start.sh
```

### 🌐 Access the System

Once installed and started, you can access:

- **Multi-Cluster Dashboard**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
- **Cluster Registry**: http://localhost:8000/api/v1/clusters
- **Monitoring Dashboard**: http://localhost:8000/monitoring
- **Individual Cluster UIs**: http://localhost:8080, 8081, 8082... (auto-assigned ports)

### 🎯 Create Your First Multi-Cluster Setup

```bash
# Create a development cluster
curl -X POST http://localhost:8000/api/v1/clusters \
  -H "Content-Type: application/json" \
  -d '{
    "id": "dev-cluster",
    "name": "Development Cluster",
    "environment": "development",
    "template_id": "development"
  }'

# Create a testing cluster
curl -X POST http://localhost:8000/api/v1/clusters \
  -H "Content-Type: application/json" \
  -d '{
    "id": "test-cluster",
    "name": "Testing Cluster",
    "environment": "testing",
    "template_id": "testing"
  }'

# List all clusters
curl http://localhost:8000/api/v1/clusters
```

## 📖 Usage Examples

### Multi-Cluster Management

#### List All Clusters
```bash
curl http://localhost:8000/api/v1/clusters
```

#### Create a New Cluster
```bash
curl -X POST http://localhost:8000/api/v1/clusters \
  -H "Content-Type: application/json" \
  -d '{
    "id": "my-prod-cluster",
    "name": "Production Cluster",
    "description": "Main production Kafka cluster",
    "environment": "production",
    "template_id": "production",
    "tags": {
      "team": "platform",
      "criticality": "high"
    }
  }'
```

#### Start/Stop Clusters
```bash
# Start a specific cluster
curl -X POST http://localhost:8000/api/v1/clusters/my-prod-cluster/start

# Stop a specific cluster
curl -X POST http://localhost:8000/api/v1/clusters/my-prod-cluster/stop

# Check cluster status
curl http://localhost:8000/api/v1/clusters/my-prod-cluster/status
```

### Advanced Operations

#### Clone a Cluster
```bash
curl -X POST http://localhost:8000/api/v1/advanced/clusters/clone \
  -H "Content-Type: application/json" \
  -d '{
    "source_cluster_id": "prod-cluster",
    "target_cluster_id": "staging-cluster",
    "target_name": "Staging Environment",
    "clone_data": true,
    "clone_config": true,
    "port_offset": 100
  }'
```

#### Create a Snapshot
```bash
curl -X POST http://localhost:8000/api/v1/advanced/snapshots \
  -H "Content-Type: application/json" \
  -d '{
    "cluster_id": "prod-cluster",
    "name": "Pre-deployment Backup",
    "description": "Full backup before major deployment",
    "snapshot_type": "full"
  }'
```

#### Schedule Automated Operations
```bash
# Schedule daily backups at 2 AM
curl -X POST http://localhost:8000/api/v1/advanced/schedules \
  -H "Content-Type: application/json" \
  -d '{
    "cluster_id": "prod-cluster",
    "name": "Daily Backup",
    "schedule_type": "backup",
    "frequency": "daily",
    "schedule_expression": "02:00",
    "enabled": true
  }'
```

### Cross-Cluster Data Migration
```bash
# Migrate data between clusters
curl -X POST http://localhost:8000/api/v1/cross-cluster/migrate \
  -H "Content-Type: application/json" \
  -d '{
    "source_cluster_id": "old-cluster",
    "target_cluster_id": "new-cluster",
    "topics": ["user-events", "order-events"],
    "migration_type": "copy",
    "preserve_offsets": true
  }'
```

### Topic Management (Per Cluster)

#### List Topics in a Cluster
```bash
curl http://localhost:8000/api/v1/clusters/my-cluster/topics
```

#### Create Topic in Specific Cluster
```bash
curl -X POST http://localhost:8000/api/v1/clusters/my-cluster/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "user-events",
    "partitions": 6,
    "replication_factor": 1,
    "config": {
      "retention.ms": "604800000",
      "cleanup.policy": "delete"
    }
  }'
```

### Monitoring and Health Checks

#### Get Cluster Health
```bash
curl http://localhost:8000/api/v1/clusters/my-cluster/health
```

#### Get System-wide Status
```bash
curl http://localhost:8000/api/v1/system/status
```

#### Get Resource Usage
```bash
curl http://localhost:8000/api/v1/system/resources
```

### Configuration Management

#### Export Cluster Configuration
```bash
curl http://localhost:8000/api/v1/clusters/my-cluster/config/export > cluster-config.json
```

#### Import Configuration to New Cluster
```bash
curl -X POST http://localhost:8000/api/v1/clusters/new-cluster/config/import \
  -H "Content-Type: application/json" \
  -d @cluster-config.json
```

### Security and Access Control

#### Create API Key
```bash
curl -X POST http://localhost:8000/api/v1/auth/api-keys \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-service-key",
    "description": "API key for my service",
    "permissions": ["cluster:read", "cluster:write"],
    "expires_at": "2024-12-31T23:59:59Z"
  }'
```

#### Use API Key for Authentication
```bash
curl -H "X-API-Key: your-api-key-here" \
  http://localhost:8000/api/v1/clusters
```

## Script Options

### Installation Script (`install.sh`)

```bash
./install.sh [OPTIONS]
```

The installation script automatically:
- Checks Python 3.8+ installation
- Verifies Docker and Docker Compose
- Creates Python virtual environment
- Installs all dependencies
- Pulls Docker images
- Creates necessary directories

### Startup Script (`start.sh`)

```bash
./start.sh [OPTIONS]

Options:
  --help         Show help message
  --api-only     Start only the REST API (assumes Docker stack is running)
  --docker-only  Start only the Docker stack (no REST API)
```

### Stop Script (`stop.sh`)

```bash
./stop.sh [OPTIONS]

Options:
  --help         Show help message
  --cleanup      Also remove stopped containers and unused networks
  --api-only     Stop only the REST API
  --docker-only  Stop only the Docker stack
```

### Test Script (`test-stack.sh`)

```bash
./test-stack.sh
```

Runs comprehensive connectivity tests:
- Kafka broker accessibility
- REST Proxy functionality
- Kafka UI availability
- Topic creation/deletion
- Message production

## Configuration

### Environment Variables

You can customize the setup using environment variables:

```bash
# API Configuration
export API_PORT=8000
export API_HOST=0.0.0.0

# Kafka Configuration
export KAFKA_PORT=9092
export KAFKA_REST_PROXY_PORT=8082
export KAFKA_UI_PORT=8080

# Health Check Configuration
export HEALTH_CHECK_TIMEOUT=300
export HEALTH_CHECK_INTERVAL=10
```

### Docker Compose Customization

Edit `docker-compose.yml` to customize:
- Resource limits
- Volume mounts
- Network configuration
- Environment variables

## Troubleshooting

### Common Issues

**Port conflicts:**
```bash
# Check what's using a port
lsof -i :8000

# Kill process using port
kill -9 $(lsof -t -i:8000)
```

**Docker issues:**
```bash
# Check Docker status
docker info

# Restart Docker (macOS)
# Restart Docker Desktop application

# Restart Docker (Linux)
sudo systemctl restart docker
```

**Service health checks failing:**
```bash
# Check Docker logs
docker compose logs

# Check specific service
docker compose logs kafka

# Check API logs
tail -f logs/api.log
```

### Log Files

- **API logs**: `logs/api.log`
- **Docker logs**: `docker compose logs`
- **Individual service logs**: `docker compose logs <service_name>`

### Reset Everything

To completely reset the environment:

```bash
# Stop all services
./stop.sh --cleanup

# Remove all Docker volumes (WARNING: This deletes all data)
docker volume rm $(docker volume ls -q --filter name=kafka)

# Remove virtual environment
rm -rf venv

# Reinstall
./install.sh
```

## Development

### Setting up Development Environment

1. Install development dependencies:
```bash
source venv/bin/activate
pip install -e ".[dev]"
```

2. Run tests:
```bash
pytest
```

3. Format code:
```bash
black src/ tests/
isort src/ tests/
```

4. Type checking:
```bash
mypy src/
```

5. Linting:
```bash
flake8 src/ tests/
```

### Project Structure

```
├── src/                           # Source code
│   ├── api/                      # FastAPI routes and endpoints
│   │   ├── multi_cluster_routes.py    # Multi-cluster management API
│   │   ├── advanced_cluster_routes.py # Advanced operations API
│   │   ├── auth_routes.py             # Authentication and authorization
│   │   ├── configuration_routes.py    # Configuration management
│   │   └── web_interface_routes.py    # Web UI backend
│   ├── models/                   # Pydantic data models
│   │   ├── multi_cluster.py          # Multi-cluster data models
│   │   ├── cluster.py                # Single cluster models
│   │   └── catalog.py                # Service catalog models
│   ├── services/                 # Business logic services
│   │   ├── multi_cluster_manager.py   # Core multi-cluster management
│   │   ├── cluster_factory.py         # Cluster creation and templates
│   │   ├── advanced_cluster_features.py # Cloning, snapshots, scheduling
│   │   ├── cross_cluster_operations.py  # Cross-cluster data migration
│   │   ├── resource_manager.py         # Resource allocation and optimization
│   │   └── configuration_manager.py    # Configuration management
│   ├── registry/                 # Cluster registry and storage
│   │   └── cluster_registry.py        # Centralized cluster metadata
│   ├── storage/                  # Storage backends
│   │   ├── base.py                    # Storage interface
│   │   ├── file_backend.py            # File-based storage
│   │   └── database_backend.py        # Database storage
│   ├── networking/               # Network management
│   │   ├── port_allocator.py          # Automatic port allocation
│   │   ├── network_manager.py         # Network configuration
│   │   └── isolation.py               # Network isolation
│   ├── monitoring/               # Monitoring and health checks
│   │   ├── multi_cluster_monitor.py   # Multi-cluster monitoring
│   │   ├── enhanced_health_monitor.py # Advanced health monitoring
│   │   └── cluster_health_monitor.py  # Per-cluster health checks
│   ├── security/                 # Security and access control
│   │   ├── access_control.py          # Role-based access control
│   │   └── auth_middleware.py         # Authentication middleware
│   ├── setup/                    # Installation and setup
│   │   └── multi_cluster_installer.py # Installation wizard
│   ├── recovery/                 # Error recovery and resilience
│   │   └── error_recovery.py          # Automatic error recovery
│   ├── exceptions/               # Custom exceptions
│   │   └── multi_cluster_exceptions.py # Multi-cluster specific exceptions
│   └── main.py                   # Application entry point
├── tests/                        # Comprehensive test suite
│   ├── test_multi_cluster_*.py        # Multi-cluster tests
│   ├── test_advanced_*.py             # Advanced features tests
│   ├── test_*_e2e.py                  # End-to-end integration tests
│   └── conftest.py                    # Test configuration
├── docs/                         # Documentation
│   ├── MULTI_CLUSTER_GUIDE.md         # Complete multi-cluster guide
│   ├── ARCHITECTURE.md                # System architecture
│   ├── DEPLOYMENT.md                  # Deployment guide
│   └── FAQ.md                         # Frequently asked questions
├── examples/                     # Usage examples
│   ├── api_examples.py                # API usage examples
│   └── complete_examples.py           # Complete workflow examples
├── k8s/                          # Kubernetes deployment files
│   ├── multi-cluster-manager.yaml     # Main application deployment
│   ├── postgres.yaml                  # Database deployment
│   └── redis.yaml                     # Cache deployment
├── monitoring/                   # Monitoring configuration
│   ├── prometheus.yml                 # Prometheus configuration
│   └── rules/                         # Alerting rules
├── scripts/                      # Deployment and operations scripts
│   ├── deploy-production.sh           # Production deployment
│   └── validate-production-readiness.sh # Production readiness checks
├── docker-compose.yml            # Single-cluster Docker Compose (legacy)
├── docker-compose.multi-cluster.yml   # Multi-cluster Docker Compose
├── install.sh                    # Legacy single-cluster installation
├── install_multi_cluster.sh      # Multi-cluster installation
├── setup_wizard.py               # Interactive setup wizard
├── start.sh                      # Legacy startup script
├── stop.sh                       # Stop script
└── requirements.txt              # Python dependencies
```

## 🚀 Deployment Options

### Docker Compose (Recommended for Development)

```bash
# Multi-cluster setup
docker-compose -f docker-compose.multi-cluster.yml up -d

# Legacy single-cluster setup
docker-compose up -d
```

### Kubernetes (Production)

```bash
# Deploy to Kubernetes
kubectl apply -f k8s/

# Access via port-forward
kubectl port-forward service/multi-cluster-manager 8000:8000
```

### Bare Metal Installation

```bash
# Install system dependencies
sudo apt-get update
sudo apt-get install python3.8 python3-pip docker.io docker-compose

# Run installation
./install_multi_cluster.sh --prod --bare-metal
```

### Production Deployment

```bash
# Validate production readiness
./scripts/validate-production-readiness.sh

# Deploy to production
./scripts/deploy-production.sh --environment prod --backup-enabled
```

## 🔄 Migration from Single-Cluster

If you're upgrading from the original single-cluster version:

```bash
# Backup existing data
./scripts/backup-legacy-cluster.sh

# Run migration wizard
python setup_wizard.py --migrate-from-legacy

# Verify migration
./scripts/verify-migration.sh
```

## 📚 Documentation

- **[Multi-Cluster Guide](docs/MULTI_CLUSTER_GUIDE.md)** - Complete guide to multi-cluster features
- **[Architecture](docs/ARCHITECTURE.md)** - System architecture and design decisions
- **[Deployment Guide](docs/DEPLOYMENT.md)** - Production deployment best practices
- **[FAQ](docs/FAQ.md)** - Frequently asked questions and troubleshooting
- **[API Reference](http://localhost:8000/docs)** - Interactive API documentation

## 🤝 Contributing

We welcome contributions! Here's how to get started:

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **Make your changes** and add tests
4. **Run the test suite**: `pytest tests/`
5. **Run code quality checks**:
   ```bash
   black src/ tests/
   isort src/ tests/
   flake8 src/ tests/
   mypy src/
   ```
6. **Submit a pull request**

### Development Setup

```bash
# Clone and setup development environment
git clone https://github.com/your-org/multi-cluster-kafka-manager.git
cd multi-cluster-kafka-manager

# Install development dependencies
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Start development server
python src/main.py --dev
```

### Testing

```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/test_multi_cluster_*.py  # Multi-cluster tests
pytest tests/test_*_e2e.py           # End-to-end tests
pytest tests/test_advanced_*.py      # Advanced features

# Run with coverage
pytest --cov=src tests/
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the logs in `logs/api.log`
3. Check Docker logs with `docker compose logs`
4. Open an issue on GitHub with detailed error information
## 📤 
Pushing Code to Repository

### Initial Setup

If this is a new repository or you're setting up for the first time:

```bash
# Initialize git repository (if not already done)
git init

# Add remote repository
git remote add origin https://github.com/your-username/multi-cluster-kafka-manager.git

# Check current status
git status
```

### Committing Changes

```bash
# Add all changes
git add .

# Or add specific files
git add src/ docs/ README.md

# Commit with descriptive message
git commit -m "feat: Add multi-cluster support with advanced features

- Implement multi-cluster management with unlimited cluster support
- Add cluster cloning, snapshots, and cross-cluster operations
- Integrate automated scheduling and resource management
- Add enterprise security with RBAC and API key management
- Implement comprehensive monitoring with Prometheus integration
- Add production-ready deployment options (Docker, K8s, bare metal)
- Include interactive setup wizard and migration tools
- Add extensive documentation and examples"
```

### Pushing to Repository

```bash
# Push to main branch
git push origin main

# Or push to a feature branch
git checkout -b feature/multi-cluster-v2
git push origin feature/multi-cluster-v2
```

### Creating a Release

```bash
# Tag the release
git tag -a v2.0.0 -m "Multi-Cluster Kafka Manager v2.0.0

Major release with multi-cluster support:
- Multi-cluster management
- Advanced operations (cloning, snapshots)
- Enterprise security and monitoring
- Production deployment options
- Comprehensive documentation"

# Push tags
git push origin --tags
```

### Branch Management

```bash
# Create and switch to development branch
git checkout -b develop

# Create feature branches from develop
git checkout -b feature/monitoring-dashboard
git checkout -b feature/kubernetes-deployment

# Merge feature back to develop
git checkout develop
git merge feature/monitoring-dashboard

# Merge develop to main for release
git checkout main
git merge develop
```

### Pre-commit Hooks (Recommended)

Set up pre-commit hooks to ensure code quality:

```bash
# Install pre-commit
pip install pre-commit

# Create .pre-commit-config.yaml
cat > .pre-commit-config.yaml << EOF
repos:
  - repo: https://github.com/psf/black
    rev: 22.3.0
    hooks:
      - id: black
        language_version: python3.8
  - repo: https://github.com/pycqa/isort
    rev: 5.10.1
    hooks:
      - id: isort
  - repo: https://github.com/pycqa/flake8
    rev: 4.0.1
    hooks:
      - id: flake8
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v0.950
    hooks:
      - id: mypy
EOF

# Install hooks
pre-commit install
```

### Continuous Integration

Example GitHub Actions workflow (`.github/workflows/ci.yml`):

```yaml
name: CI

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.8, 3.9, 3.10]

    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v3
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e ".[dev]"
    
    - name: Run tests
      run: |
        pytest tests/ --cov=src --cov-report=xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
```

### Repository Structure for Teams

```bash
# Recommended branch structure
main                    # Production-ready code
├── develop            # Integration branch
├── feature/*          # Feature branches
├── hotfix/*           # Critical fixes
└── release/*          # Release preparation

# Example workflow
git checkout develop
git checkout -b feature/advanced-monitoring
# ... make changes ...
git push origin feature/advanced-monitoring
# ... create pull request to develop ...
```

### Environment-Specific Configurations

```bash
# Create environment-specific configuration files
mkdir -p config/environments

# Development configuration
cat > config/environments/development.yml << EOF
clusters:
  default_template: development
  max_clusters: 5
  resource_limits:
    memory: "2Gi"
    cpu: "1"
monitoring:
  enabled: true
  prometheus: false
security:
  auth_required: false
EOF

# Production configuration  
cat > config/environments/production.yml << EOF
clusters:
  default_template: production
  max_clusters: 50
  resource_limits:
    memory: "8Gi"
    cpu: "4"
monitoring:
  enabled: true
  prometheus: true
  alerting: true
security:
  auth_required: true
  rbac_enabled: true
EOF
```

### Deployment Automation

```bash
# Create deployment script
cat > scripts/deploy.sh << EOF
#!/bin/bash
set -e

ENVIRONMENT=${1:-development}
VERSION=${2:-latest}

echo "Deploying Multi-Cluster Kafka Manager v${VERSION} to ${ENVIRONMENT}"

# Build and tag image
docker build -t multi-cluster-kafka-manager:${VERSION} .

# Deploy based on environment
case ${ENVIRONMENT} in
  "development")
    docker-compose -f docker-compose.multi-cluster.yml up -d
    ;;
  "staging"|"production")
    kubectl set image deployment/multi-cluster-manager \
      multi-cluster-manager=multi-cluster-kafka-manager:${VERSION}
    kubectl rollout status deployment/multi-cluster-manager
    ;;
esac

echo "Deployment completed successfully!"
EOF

chmod +x scripts/deploy.sh
```

This comprehensive update transforms your README from a single-cluster local development tool into a full-featured enterprise multi-cluster Kafka management platform. The documentation now reflects all the advanced features implemented and provides clear guidance for deployment, usage, and contribution.