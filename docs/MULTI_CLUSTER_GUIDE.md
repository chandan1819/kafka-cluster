# Multi-Cluster Kafka Manager - Complete Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Quick Start](#quick-start)
3. [Installation](#installation)
4. [Core Concepts](#core-concepts)
5. [Basic Operations](#basic-operations)
6. [Advanced Features](#advanced-features)
7. [Configuration Management](#configuration-management)
8. [Security and Access Control](#security-and-access-control)
9. [Monitoring and Observability](#monitoring-and-observability)
10. [Resource Management](#resource-management)
11. [API Reference](#api-reference)
12. [Troubleshooting](#troubleshooting)
13. [Best Practices](#best-practices)
14. [Examples](#examples)

## Introduction

The Multi-Cluster Kafka Manager is a comprehensive solution for managing multiple Kafka clusters from a single interface. It provides advanced features like cluster cloning, automated scheduling, cross-cluster operations, and enterprise-grade monitoring.

### Key Features

- **Multi-Cluster Management**: Create, manage, and monitor multiple Kafka clusters
- **Cluster Templates**: Reusable templates for consistent cluster deployment
- **Advanced Operations**: Cluster cloning, snapshots, and cross-cluster data migration
- **Automated Scheduling**: Schedule cluster operations like start/stop, backups, and maintenance
- **Security & Access Control**: Role-based access control with API key management
- **Monitoring & Observability**: Comprehensive health monitoring and alerting
- **Resource Management**: Intelligent resource allocation and optimization
- **Configuration Management**: Version-controlled configuration with import/export

## Quick Start

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- 4GB+ RAM recommended
- 20GB+ disk space

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/multi-cluster-kafka-manager.git
cd multi-cluster-kafka-manager

# Run the installation wizard
./install_multi_cluster.sh --quick --dev

# Start the services
./start_multi_cluster.sh
```

### First Steps

1. **Access the Web Interface**: Open http://localhost:8000
2. **Create Your First Cluster**: Use the cluster creation wizard
3. **Explore the Dashboard**: Monitor cluster health and metrics
4. **Try Advanced Features**: Clone clusters, create snapshots, set up schedules

## Installation

### Interactive Installation

The easiest way to install is using the interactive wizard:

```bash
# Run the setup wizard
python setup_wizard.py
```

The wizard will guide you through:
- System requirements checking
- Deployment scenario selection
- Directory configuration
- Security settings
- Resource limits
- Initial cluster creation

### Quick Installation Options

```bash
# Development setup (single cluster, minimal resources)
./install_multi_cluster.sh --dev

# Testing setup (multiple clusters)
./install_multi_cluster.sh --test

# Production setup (high availability, monitoring)
./install_multi_cluster.sh --prod --backup
```

### Manual Installation

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run setup wizard
python setup_wizard.py --install
```

### Docker Installation

```bash
# Build and run with Docker Compose
docker-compose up -d

# Access the web interface
open http://localhost:8000
```

## Core Concepts

### Clusters

A **cluster** is a complete Kafka deployment including:
- Kafka broker(s)
- REST Proxy for HTTP API access
- Kafka UI for web-based management
- Optional JMX monitoring

### Templates

**Templates** define reusable cluster configurations:
- Resource requirements (CPU, memory, disk)
- Kafka configuration parameters
- Service configurations
- Default settings for different environments

### Environments

Clusters are organized by **environment**:
- **Development**: Single broker, minimal resources
- **Testing**: Multiple brokers, moderate resources
- **Staging**: Production-like setup for final testing
- **Production**: High availability, monitoring, security

### Port Allocation

Each cluster requires unique ports:
- **Kafka Port**: Broker communication (default: 9092)
- **REST Proxy Port**: HTTP API access (default: 8082)
- **UI Port**: Web interface (default: 8080)
- **JMX Port**: Monitoring (optional)

## Basic Operations

### Creating a Cluster

#### Via Web Interface

1. Navigate to "Clusters" → "Create New"
2. Select a template (Development, Testing, Production)
3. Configure cluster details:
   - Name and description
   - Environment
   - Port allocation
   - Tags
4. Click "Create Cluster"

#### Via API

```bash
curl -X POST http://localhost:8000/api/v1/clusters \
  -H "Content-Type: application/json" \
  -d '{
    "id": "my-dev-cluster",
    "name": "My Development Cluster",
    "description": "Development cluster for testing",
    "environment": "development",
    "template_id": "development",
    "port_allocation": {
      "kafka_port": 9092,
      "rest_proxy_port": 8082,
      "ui_port": 8080
    },
    "tags": {
      "team": "backend",
      "project": "user-service"
    }
  }'
```

#### Via Python SDK

```python
from src.services.multi_cluster_manager import MultiClusterManager
from src.models.multi_cluster import ClusterDefinition, PortAllocation

# Create cluster definition
cluster = ClusterDefinition(
    id="my-dev-cluster",
    name="My Development Cluster",
    description="Development cluster for testing",
    environment="development",
    template_id="development",
    port_allocation=PortAllocation(
        kafka_port=9092,
        rest_proxy_port=8082,
        ui_port=8080
    ),
    tags={"team": "backend", "project": "user-service"}
)

# Create cluster
manager = MultiClusterManager()
result = await manager.create_cluster(cluster)
print(f"Created cluster: {result.id}")
```

### Managing Clusters

#### Starting a Cluster

```bash
# Via API
curl -X POST http://localhost:8000/api/v1/clusters/my-dev-cluster/start

# Via CLI
python -m src.cli cluster start my-dev-cluster
```

#### Stopping a Cluster

```bash
# Via API
curl -X POST http://localhost:8000/api/v1/clusters/my-dev-cluster/stop

# Via CLI
python -m src.cli cluster stop my-dev-cluster
```

#### Checking Cluster Status

```bash
# Via API
curl http://localhost:8000/api/v1/clusters/my-dev-cluster/status

# Via CLI
python -m src.cli cluster status my-dev-cluster
```

#### Listing All Clusters

```bash
# Via API
curl http://localhost:8000/api/v1/clusters

# Via CLI
python -m src.cli cluster list
```

### Working with Topics

#### Creating Topics

```bash
# Create topic via REST Proxy
curl -X POST http://localhost:8082/topics/my-topic \
  -H "Content-Type: application/json" \
  -d '{
    "partitions": 3,
    "replication_factor": 1,
    "config": {
      "retention.ms": "604800000"
    }
  }'
```

#### Producing Messages

```bash
# Produce messages via REST Proxy
curl -X POST http://localhost:8082/topics/my-topic \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{
    "records": [
      {"value": {"message": "Hello, Kafka!"}},
      {"value": {"message": "Multi-cluster is awesome!"}}
    ]
  }'
```

#### Consuming Messages

```bash
# Create consumer
curl -X POST http://localhost:8082/consumers/my-consumer-group \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{
    "name": "my-consumer",
    "format": "json",
    "auto.offset.reset": "earliest"
  }'

# Subscribe to topic
curl -X POST http://localhost:8082/consumers/my-consumer-group/instances/my-consumer/subscription \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"topics": ["my-topic"]}'

# Consume messages
curl -X GET http://localhost:8082/consumers/my-consumer-group/instances/my-consumer/records \
  -H "Accept: application/vnd.kafka.json.v2+json"
```

## Advanced Features

### Cluster Cloning

Clone existing clusters to create identical environments:

```python
# Clone a production cluster for staging
cloned_cluster = await advanced_features.clone_cluster(
    source_cluster_id="prod-cluster",
    target_cluster_id="staging-cluster",
    target_name="Staging Environment",
    clone_data=True,
    clone_config=True,
    port_offset=100,
    tags={"purpose": "staging", "cloned_from": "prod-cluster"}
)
```

```bash
# Via API
curl -X POST http://localhost:8000/api/v1/advanced/clusters/clone \
  -H "Content-Type: application/json" \
  -d '{
    "source_cluster_id": "prod-cluster",
    "target_cluster_id": "staging-cluster",
    "target_name": "Staging Environment",
    "clone_data": true,
    "clone_config": true,
    "port_offset": 100,
    "tags": {"purpose": "staging"}
  }'
```

### Snapshots and Backups

#### Creating Snapshots

```python
# Create full snapshot
snapshot = await advanced_features.create_snapshot(
    cluster_id="prod-cluster",
    name="Pre-deployment Backup",
    description="Full backup before major deployment",
    snapshot_type=SnapshotType.FULL,
    tags={"deployment", "backup", "critical"}
)
```

```bash
# Via API
curl -X POST http://localhost:8000/api/v1/advanced/snapshots \
  -H "Content-Type: application/json" \
  -d '{
    "cluster_id": "prod-cluster",
    "name": "Pre-deployment Backup",
    "description": "Full backup before major deployment",
    "snapshot_type": "full",
    "tags": ["deployment", "backup", "critical"]
  }'
```

#### Restoring from Snapshots

```python
# Restore snapshot to recovery cluster
await advanced_features.restore_snapshot(
    snapshot_id="snapshot-123",
    target_cluster_id="recovery-cluster",
    restore_config=True,
    restore_data=True
)
```

### Automated Scheduling

#### Schedule Daily Backups

```python
# Schedule automated daily backups
backup_schedule = await advanced_features.create_schedule(
    cluster_id="prod-cluster",
    name="Daily Backup",
    schedule_type=ScheduleType.BACKUP,
    frequency=ScheduleFrequency.DAILY,
    schedule_expression="02:00",  # 2 AM daily
    description="Automated daily backup",
    enabled=True,
    tags={"backup", "automated"}
)
```

#### Schedule Business Hours Operation

```python
# Start cluster on Monday morning
start_schedule = await advanced_features.create_schedule(
    cluster_id="dev-cluster",
    name="Business Hours Start",
    schedule_type=ScheduleType.START,
    frequency=ScheduleFrequency.WEEKLY,
    schedule_expression="MON 09:00",
    enabled=True
)

# Stop cluster on Friday evening
stop_schedule = await advanced_features.create_schedule(
    cluster_id="dev-cluster",
    name="Business Hours Stop",
    schedule_type=ScheduleType.STOP,
    frequency=ScheduleFrequency.WEEKLY,
    schedule_expression="FRI 18:00",
    enabled=True
)
```

## Best Practices

### Cluster Naming

- Use descriptive, consistent naming conventions
- Include environment in the name (e.g., `user-service-dev`, `user-service-prod`)
- Avoid special characters except hyphens
- Keep names under 50 characters

### Resource Management

- Set appropriate resource quotas for each environment
- Monitor resource usage regularly
- Use cleanup policies to remove unused clusters
- Schedule regular backups for production clusters

### Security

- Use strong passwords and API keys
- Implement least-privilege access control
- Regularly rotate API keys
- Enable audit logging for compliance
- Use HTTPS in production environments

### Monitoring

- Set up comprehensive health monitoring
- Configure appropriate alert thresholds
- Use custom alert handlers for notifications
- Monitor both system and cluster-level metrics
- Implement log aggregation for troubleshooting

### Configuration Management

- Use version control for cluster configurations
- Export configurations before major changes
- Test configuration changes in non-production environments
- Document configuration decisions and rationales

---

For more detailed information, see the [Architecture](ARCHITECTURE.md), [Deployment](DEPLOYMENT.md), and [FAQ](FAQ.md) documentation.