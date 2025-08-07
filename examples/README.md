# Multi-Cluster Kafka Manager Examples

This directory contains comprehensive examples and usage patterns for the Multi-Cluster Kafka Manager system.

## Overview

The examples are organized into several categories:

1. **Complete Examples** (`complete_examples.py`) - Full-featured Python examples
2. **API Examples** (`api_examples.py`) - REST API usage examples
3. **Configuration Examples** - YAML and JSON configuration files
4. **Integration Examples** - End-to-end integration scenarios

## Quick Start

### Running Complete Examples

```bash
# Run all examples
python examples/complete_examples.py

# Run specific example interactively
python examples/complete_examples.py
# Then choose from the menu
```

### Running API Examples

```bash
# Run all API examples
python examples/api_examples.py

# Run specific API example
python examples/api_examples.py --example 1

# Use custom API endpoint and authentication
python examples/api_examples.py --base-url http://your-server:8000 --api-key your-api-key
```

## Complete Examples (`complete_examples.py`)

### Example 1: Development Environment Setup
- Creates multiple clusters for different services
- Configures development-specific settings
- Demonstrates basic cluster lifecycle management

```python
# Creates clusters for:
# - user-service-dev (port 9092)
# - order-service-dev (port 9093)
# - notification-service-dev (port 9094)
# - payment-service-dev (port 9095)
```

### Example 2: Production Deployment with Monitoring
- Production cluster with security and monitoring
- Automated backup scheduling
- User management and permissions
- Alert handling setup

```python
# Features demonstrated:
# - Production-grade configuration
# - Daily backup schedules
# - Role-based access control
# - Custom alert handlers
# - API key management
```

### Example 3: Disaster Recovery Setup
- Cross-cluster replication
- DR cluster configuration
- Automated DR testing
- Baseline snapshots

```python
# DR capabilities:
# - Async replication to DR site
# - Weekly DR testing schedule
# - Configuration snapshots
# - Failover procedures
```

### Example 4: CI/CD Pipeline
- Multi-environment deployment pipeline
- Automated testing integration
- Snapshot-based rollbacks
- Environment promotion

```python
# Pipeline stages:
# - Development → Staging → Production
# - Pre/post deployment snapshots
# - Automated testing checkpoints
# - Version tagging and tracking
```

### Example 5: Resource Optimization and Cleanup
- Resource usage monitoring
- Automated cleanup policies
- Scaling recommendations
- Quota management

```python
# Resource management:
# - CPU, memory, disk monitoring
# - Cleanup policies for old clusters
# - Resource quota enforcement
# - Optimization recommendations
```

### Example 6: Configuration Management
- Configuration versioning
- Import/export capabilities
- Rollback functionality
- Validation and comparison

```python
# Config features:
# - YAML/JSON export/import
# - Version history tracking
# - Configuration comparison
# - Automated rollback
```

### Example 7: Comprehensive Monitoring
- System-wide health monitoring
- Custom alert handlers
- Metrics collection
- Health trend analysis

```python
# Monitoring capabilities:
# - Real-time health status
# - Custom alert routing
# - Performance metrics
# - Trend analysis
```

## API Examples (`api_examples.py`)

### Example 1: Basic Cluster Operations
- CRUD operations for clusters
- Cluster lifecycle management
- Status monitoring

```bash
# API endpoints covered:
# POST /api/v1/clusters
# GET /api/v1/clusters/{id}
# PUT /api/v1/clusters/{id}
# DELETE /api/v1/clusters/{id}
# POST /api/v1/clusters/{id}/start
# POST /api/v1/clusters/{id}/stop
```

### Example 2: Advanced Cluster Features
- Cluster cloning
- Snapshot management
- Automated scheduling
- Tag-based operations

```bash
# Advanced endpoints:
# POST /api/v1/advanced/clusters/clone
# POST /api/v1/advanced/snapshots
# POST /api/v1/advanced/schedules
# POST /api/v1/advanced/clusters/tags
# POST /api/v1/advanced/clusters/search
```

### Example 3: Configuration Management
- Configuration export/import
- Version management
- Validation and rollback

```bash
# Configuration endpoints:
# GET /api/v1/config/export/{cluster_id}
# POST /api/v1/config/import
# POST /api/v1/config/validate
# GET /api/v1/config/versions/{cluster_id}
# POST /api/v1/config/rollback
```

### Example 4: Monitoring and Health
- Health status checking
- Metrics collection
- Alert management

```bash
# Monitoring endpoints:
# GET /api/v1/monitoring/health
# GET /api/v1/monitoring/clusters/{id}/health
# GET /api/v1/monitoring/metrics
# GET /api/v1/monitoring/alerts
# POST /api/v1/monitoring/alerts/{id}/acknowledge
```

### Example 5: Security and Access Control
- User management
- Permission handling
- API key management
- Audit logging

```bash
# Security endpoints:
# POST /api/v1/auth/users
# GET /api/v1/auth/users
# POST /api/v1/auth/permissions
# POST /api/v1/auth/api-keys
# GET /api/v1/auth/audit
```

### Example 6: Resource Management
- Resource usage monitoring
- Quota management
- Optimization recommendations
- Cleanup operations

```bash
# Resource endpoints:
# GET /api/v1/resources/usage
# GET /api/v1/resources/clusters/{id}/usage
# POST /api/v1/resources/quotas
# GET /api/v1/resources/optimization
# POST /api/v1/resources/cleanup
```

## Configuration Examples

### Cluster Templates

#### Development Template
```yaml
# templates/development.yaml
name: "Development Template"
environment: "development"
resources:
  cpu_limit: "1"
  memory_limit: "2Gi"
  disk_limit: "10Gi"
kafka_config:
  log.retention.hours: "24"
  log.segment.bytes: "104857600"
  auto.create.topics.enable: "true"
services:
  - kafka
  - rest-proxy
  - ui
```

#### Production Template
```yaml
# templates/production.yaml
name: "Production Template"
environment: "production"
resources:
  cpu_limit: "4"
  memory_limit: "8Gi"
  disk_limit: "100Gi"
kafka_config:
  log.retention.hours: "168"
  log.segment.bytes: "1073741824"
  auto.create.topics.enable: "false"
  min.insync.replicas: "2"
  default.replication.factor: "3"
services:
  - kafka
  - rest-proxy
  - ui
  - jmx-exporter
monitoring:
  enabled: true
  alerts:
    - high_cpu_usage
    - high_memory_usage
    - disk_space_low
```

### Docker Compose Examples

#### Multi-Cluster Setup
```yaml
# docker-compose.multi-cluster.yml
version: '3.8'

services:
  multi-cluster-manager:
    build: .
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@postgres:5432/multi_cluster
      - REDIS_URL=redis://redis:6379
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    depends_on:
      - postgres
      - redis

  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: multi_cluster
      POSTGRES_USER: user
      POSTGRES_PASSWORD: pass
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:6-alpine
    volumes:
      - redis_data:/data

volumes:
  postgres_data:
  redis_data:
```

## Integration Examples

### Kubernetes Integration
```yaml
# k8s/multi-cluster-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: multi-cluster-kafka-manager
spec:
  replicas: 3
  selector:
    matchLabels:
      app: multi-cluster-kafka-manager
  template:
    metadata:
      labels:
        app: multi-cluster-kafka-manager
    spec:
      containers:
      - name: manager
        image: multi-cluster-kafka-manager:latest
        ports:
        - containerPort: 8000
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-secret
              key: url
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
```

### Terraform Integration
```hcl
# terraform/multi-cluster.tf
resource "aws_instance" "kafka_cluster" {
  count         = var.cluster_count
  ami           = var.kafka_ami
  instance_type = var.instance_type
  
  tags = {
    Name = "kafka-cluster-${count.index + 1}"
    Environment = var.environment
    ManagedBy = "multi-cluster-kafka-manager"
  }
}

resource "aws_security_group" "kafka" {
  name_prefix = "kafka-cluster-"
  
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  
  ingress {
    from_port   = 8082
    to_port     = 8082
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
}
```

## Testing Examples

### Unit Test Examples
```python
# tests/examples/test_cluster_operations.py
import pytest
from examples.complete_examples import MultiClusterExamples

@pytest.mark.asyncio
async def test_development_environment_setup():
    """Test development environment setup example."""
    examples = MultiClusterExamples()
    
    # Run the example
    clusters = await examples.example_1_development_environment_setup()
    
    # Verify clusters were created
    assert len(clusters) == 4
    assert all(cluster.environment == "development" for cluster in clusters)
    
    # Cleanup
    for cluster in clusters:
        await examples.manager.stop_cluster(cluster.id)
        await examples.manager.delete_cluster(cluster.id)
```

### Integration Test Examples
```python
# tests/examples/test_api_integration.py
import pytest
from examples.api_examples import MultiClusterAPIExamples

def test_basic_cluster_operations():
    """Test basic cluster operations via API."""
    api_examples = MultiClusterAPIExamples()
    
    # This will create, start, stop, and delete a cluster
    cluster_id = api_examples.example_1_basic_cluster_operations()
    
    # Verify cluster was properly cleaned up
    response = api_examples._make_request("GET", "/api/v1/clusters")
    clusters = response.json()
    
    # Cluster should not exist anymore
    assert not any(cluster["id"] == cluster_id for cluster in clusters)
```

## Performance Examples

### Load Testing
```python
# examples/performance/load_test.py
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from examples.api_examples import MultiClusterAPIExamples

async def load_test_cluster_creation():
    """Load test cluster creation."""
    api_examples = MultiClusterAPIExamples()
    
    async def create_test_cluster(cluster_num):
        cluster_data = {
            "id": f"load-test-cluster-{cluster_num}",
            "name": f"Load Test Cluster {cluster_num}",
            "environment": "testing",
            "template_id": "testing",
            "port_allocation": {
                "kafka_port": 9200 + cluster_num,
                "rest_proxy_port": 8200 + cluster_num,
                "ui_port": 8300 + cluster_num
            }
        }
        
        start_time = time.time()
        response = api_examples._make_request("POST", "/api/v1/clusters", json=cluster_data)
        end_time = time.time()
        
        return end_time - start_time, response.json()
    
    # Create 10 clusters concurrently
    tasks = [create_test_cluster(i) for i in range(10)]
    results = await asyncio.gather(*tasks)
    
    # Calculate statistics
    creation_times = [result[0] for result in results]
    avg_time = sum(creation_times) / len(creation_times)
    max_time = max(creation_times)
    min_time = min(creation_times)
    
    print(f"Cluster creation performance:")
    print(f"  Average: {avg_time:.2f}s")
    print(f"  Maximum: {max_time:.2f}s")
    print(f"  Minimum: {min_time:.2f}s")
    
    # Cleanup
    for i, (_, cluster) in enumerate(results):
        api_examples._make_request("DELETE", f"/api/v1/clusters/{cluster['id']}")
```

## Troubleshooting Examples

### Common Issues and Solutions

#### Port Conflicts
```python
# examples/troubleshooting/port_conflicts.py
import socket
from examples.complete_examples import MultiClusterExamples

def find_available_port(start_port=9092, max_attempts=100):
    """Find an available port starting from start_port."""
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', port))
                return port
        except OSError:
            continue
    raise RuntimeError(f"No available ports found in range {start_port}-{start_port + max_attempts}")

# Use in cluster creation
available_port = find_available_port()
print(f"Using available port: {available_port}")
```

#### Resource Constraints
```python
# examples/troubleshooting/resource_monitoring.py
import psutil
import asyncio
from examples.complete_examples import MultiClusterExamples

async def monitor_resources_during_cluster_creation():
    """Monitor system resources during cluster creation."""
    examples = MultiClusterExamples()
    
    # Start monitoring
    initial_memory = psutil.virtual_memory().percent
    initial_cpu = psutil.cpu_percent(interval=1)
    
    print(f"Initial resource usage:")
    print(f"  Memory: {initial_memory}%")
    print(f"  CPU: {initial_cpu}%")
    
    # Create cluster while monitoring
    clusters = await examples.example_1_development_environment_setup()
    
    # Check final resource usage
    final_memory = psutil.virtual_memory().percent
    final_cpu = psutil.cpu_percent(interval=1)
    
    print(f"Final resource usage:")
    print(f"  Memory: {final_memory}% (delta: +{final_memory - initial_memory}%)")
    print(f"  CPU: {final_cpu}% (delta: +{final_cpu - initial_cpu}%)")
    
    # Cleanup
    for cluster in clusters:
        await examples.manager.stop_cluster(cluster.id)
        await examples.manager.delete_cluster(cluster.id)
```

## Best Practices Examples

### Error Handling
```python
# examples/best_practices/error_handling.py
import asyncio
import logging
from examples.complete_examples import MultiClusterExamples

async def robust_cluster_creation():
    """Example of robust cluster creation with error handling."""
    examples = MultiClusterExamples()
    logger = logging.getLogger(__name__)
    
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            clusters = await examples.example_1_development_environment_setup()
            logger.info(f"Successfully created {len(clusters)} clusters")
            return clusters
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error("All attempts failed")
                raise
```

### Configuration Validation
```python
# examples/best_practices/config_validation.py
from pydantic import BaseModel, validator
from typing import Dict, Any

class ClusterConfigValidator(BaseModel):
    """Validate cluster configuration before creation."""
    
    id: str
    name: str
    environment: str
    port_allocation: Dict[str, int]
    tags: Dict[str, str] = {}
    
    @validator('id')
    def validate_id(cls, v):
        if not v.replace('-', '').replace('_', '').isalnum():
            raise ValueError('ID must contain only alphanumeric characters, hyphens, and underscores')
        if len(v) > 50:
            raise ValueError('ID must be 50 characters or less')
        return v
    
    @validator('environment')
    def validate_environment(cls, v):
        valid_environments = ['development', 'testing', 'staging', 'production']
        if v not in valid_environments:
            raise ValueError(f'Environment must be one of: {valid_environments}')
        return v
    
    @validator('port_allocation')
    def validate_ports(cls, v):
        required_ports = ['kafka_port', 'rest_proxy_port', 'ui_port']
        for port_name in required_ports:
            if port_name not in v:
                raise ValueError(f'Missing required port: {port_name}')
            
            port = v[port_name]
            if not (1024 <= port <= 65535):
                raise ValueError(f'Port {port_name} must be between 1024 and 65535')
        
        return v

# Usage example
def validate_cluster_config(config_dict: Dict[str, Any]) -> ClusterConfigValidator:
    """Validate cluster configuration."""
    try:
        return ClusterConfigValidator(**config_dict)
    except Exception as e:
        print(f"Configuration validation failed: {str(e)}")
        raise
```

## Running the Examples

### Prerequisites
```bash
# Install dependencies
pip install -r requirements.txt

# Start the multi-cluster manager
python -m src.main

# Or use Docker
docker-compose up -d
```

### Environment Setup
```bash
# Set environment variables
export MULTI_CLUSTER_API_URL=http://localhost:8000
export MULTI_CLUSTER_API_KEY=your-api-key-here

# Or create a .env file
echo "MULTI_CLUSTER_API_URL=http://localhost:8000" > .env
echo "MULTI_CLUSTER_API_KEY=your-api-key-here" >> .env
```

### Running Examples
```bash
# Run all complete examples
python examples/complete_examples.py

# Run all API examples
python examples/api_examples.py

# Run specific example
python examples/complete_examples.py
# Then select from the interactive menu

# Run with custom configuration
python examples/api_examples.py --base-url http://your-server:8000 --api-key your-key
```

For more detailed information, see the [Multi-Cluster Guide](../docs/MULTI_CLUSTER_GUIDE.md).