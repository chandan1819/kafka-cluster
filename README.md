# 🚀 Local Kafka Manager

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Docker](https://img.shields.io/badge/Docker-20.0+-blue.svg)](https://docker.com)
[![FastAPI](https://img.shields.io/badge/FastAPI-Latest-green.svg)](https://fastapi.tiangolo.com)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4.0-orange.svg)](https://kafka.apache.org)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A **production-ready**, self-service local Kafka cluster management solution that enables developers to provision, control, and monitor a full-featured Kafka stack entirely on their local machine. **No Kubernetes or cloud dependencies required!**

> 🎯 **Perfect for**: Local development, testing, learning Kafka, microservices development, event-driven architecture prototyping

## 🌟 Why Local Kafka Manager?

- **🔥 Zero Configuration**: One-command setup with automated health checks
- **🎛️ Complete Control**: Full Kafka cluster lifecycle management via REST API
- **📊 Visual Management**: Beautiful web UI for topic and message management  
- **🧪 Developer Friendly**: Perfect for testing event-driven architectures
- **🐳 Containerized**: Fully isolated Docker environment
- **📚 Self-Documenting**: Auto-generated interactive API documentation
- **🔍 Production-Like**: Uses real Kafka (not embedded) with proper configurations

## Features

- 🚀 **One-click setup** - Automated installation and startup scripts
- 🔧 **REST API** - Complete Kafka cluster lifecycle management
- 📊 **Topic Management** - Create, list, delete topics via API
- 💬 **Message Operations** - Produce and consume messages through REST endpoints
- 📋 **Service Catalog** - JSON-based service discovery and status
- 🖥️ **Web UI** - Visual Kafka cluster management interface
- 🐳 **Docker Orchestration** - Fully containerized stack with Docker Compose
- 📚 **Auto-generated Documentation** - Interactive API docs with Swagger/OpenAPI

## Architecture

The system consists of several components working together:

- **FastAPI REST Server** (Port 8000) - Main API gateway
- **Kafka Broker** (Port 9092) - Single-node Kafka cluster using KRaft mode
- **Kafka REST Proxy** (Port 8082) - HTTP interface for Kafka operations
- **Kafka UI** (Port 8080) - Web-based management interface

## Prerequisites

- **Python 3.8+** - For the REST API server
- **Docker 20.0+** - For containerized services
- **Docker Compose 2.0+** - For service orchestration
- **2GB+ free disk space** - For Docker images and data
- **4GB+ RAM** - Recommended for optimal performance

## Quick Start

### 1. Installation

Run the automated installation script:

```bash
./install.sh
```

This script will:
- ✅ Check all prerequisites (Python, Docker, Docker Compose)
- ✅ Create Python virtual environment
- ✅ Install Python dependencies
- ✅ Pull required Docker images
- ✅ Verify installation

### 2. Start Services

Start the entire stack:

```bash
./start.sh
```

This will:
- 🚀 Start Docker Compose stack (Kafka, REST Proxy, UI)
- 🔍 Wait for all services to be healthy
- 🌐 Start the FastAPI REST server
- ✅ Run connectivity tests
- 📊 Display service URLs and status

### 3. Verify Installation

Once started, you can access:

- **REST API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
- **Service Catalog**: http://localhost:8000/catalog
- **Kafka UI**: http://localhost:8080
- **Kafka REST Proxy**: http://localhost:8082

### 4. Stop Services

Stop all services:

```bash
./stop.sh
```

## Usage Examples

### Service Catalog

Get real-time status of all services:

```bash
curl http://localhost:8000/catalog
```

### Cluster Management

Start the Kafka cluster:
```bash
curl -X POST http://localhost:8000/cluster/start
```

Check cluster status:
```bash
curl http://localhost:8000/cluster/status
```

Stop the cluster:
```bash
curl -X POST http://localhost:8000/cluster/stop
```

### Topic Management

List all topics:
```bash
curl http://localhost:8000/topics
```

Create a new topic:
```bash
curl -X POST http://localhost:8000/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-topic",
    "partitions": 3,
    "replication_factor": 1
  }'
```

Delete a topic:
```bash
curl -X DELETE http://localhost:8000/topics/my-topic
```

### Message Operations

Produce a message:
```bash
curl -X POST http://localhost:8000/produce \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "my-topic",
    "key": "user123",
    "value": {"action": "login", "timestamp": 1642234567}
  }'
```

Consume messages:
```bash
curl "http://localhost:8000/consume?topic=my-topic&consumer_group=my-group&max_messages=10"
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
├── src/                    # Source code
│   ├── api/               # FastAPI routes
│   ├── models/            # Pydantic data models
│   ├── services/          # Business logic services
│   ├── utils/             # Utility functions
│   └── main.py           # Application entry point
├── tests/                 # Test suite
├── logs/                  # Log files
├── docker-compose.yml     # Docker services definition
├── install.sh            # Installation script
├── start.sh              # Startup script
├── stop.sh               # Stop script
├── test-stack.sh         # Connectivity tests
└── requirements.txt      # Python dependencies
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the logs in `logs/api.log`
3. Check Docker logs with `docker compose logs`
4. Open an issue on GitHub with detailed error information