# Local Kafka Manager Setup Guide

This guide provides detailed instructions for setting up and configuring the Local Kafka Manager on different operating systems.

## Table of Contents

1. [System Requirements](#system-requirements)
2. [Platform-Specific Setup](#platform-specific-setup)
3. [Installation Process](#installation-process)
4. [Configuration](#configuration)
5. [Verification](#verification)
6. [Troubleshooting](#troubleshooting)

## System Requirements

### Minimum Requirements

- **CPU**: 2 cores
- **RAM**: 4GB (8GB recommended)
- **Disk Space**: 2GB free space
- **Network**: Internet connection for Docker image downloads

### Software Requirements

- **Python**: 3.8 or higher
- **Docker**: 20.0 or higher
- **Docker Compose**: 2.0 or higher (V1 also supported)

## Platform-Specific Setup

### macOS

#### Prerequisites Installation

1. **Install Homebrew** (if not already installed):
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

2. **Install Python 3.8+**:
   ```bash
   brew install python3
   ```

3. **Install Docker Desktop**:
   - Download from: https://www.docker.com/products/docker-desktop
   - Install and start Docker Desktop
   - Verify installation: `docker --version`

#### Resource Configuration

Configure Docker Desktop resources:
1. Open Docker Desktop preferences
2. Go to "Resources" → "Advanced"
3. Set memory to at least 4GB
4. Set CPU to at least 2 cores
5. Click "Apply & Restart"

### Ubuntu/Debian Linux

#### Prerequisites Installation

1. **Update package index**:
   ```bash
   sudo apt-get update
   ```

2. **Install Python 3.8+**:
   ```bash
   sudo apt-get install python3 python3-pip python3-venv
   ```

3. **Install Docker**:
   ```bash
   # Add Docker's official GPG key
   curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
   
   # Add Docker repository
   echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
   
   # Install Docker
   sudo apt-get update
   sudo apt-get install docker-ce docker-ce-cli containerd.io
   
   # Add user to docker group
   sudo usermod -aG docker $USER
   
   # Start Docker service
   sudo systemctl start docker
   sudo systemctl enable docker
   ```

4. **Install Docker Compose**:
   ```bash
   sudo apt-get install docker-compose-plugin
   ```

5. **Log out and back in** to apply group changes.

### CentOS/RHEL/Fedora

#### Prerequisites Installation

1. **Install Python 3.8+**:
   ```bash
   # CentOS/RHEL
   sudo yum install python3 python3-pip
   
   # Fedora
   sudo dnf install python3 python3-pip
   ```

2. **Install Docker**:
   ```bash
   # Add Docker repository
   sudo yum install -y yum-utils
   sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
   
   # Install Docker
   sudo yum install docker-ce docker-ce-cli containerd.io
   
   # Start Docker service
   sudo systemctl start docker
   sudo systemctl enable docker
   
   # Add user to docker group
   sudo usermod -aG docker $USER
   ```

3. **Install Docker Compose**:
   ```bash
   sudo yum install docker-compose-plugin
   ```

### Windows (WSL2)

#### Prerequisites

1. **Install WSL2** with Ubuntu distribution
2. **Install Docker Desktop** with WSL2 backend enabled
3. Follow Ubuntu instructions within WSL2 environment

## Installation Process

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd local-kafka-manager
```

### Step 2: Run Installation Script

```bash
./install.sh
```

The installation script performs the following checks and installations:

#### Prerequisite Checks
- ✅ Python 3.8+ installation and version
- ✅ pip3 availability
- ✅ Docker installation and daemon status
- ✅ Docker Compose installation and version
- ✅ Available disk space (minimum 2GB)

#### Installation Steps
- 🐍 Create Python virtual environment
- 📦 Install Python dependencies from requirements.txt
- 📁 Create necessary directories (logs, data)
- 🐳 Pull required Docker images:
  - `confluentinc/cp-kafka:7.4.0`
  - `confluentinc/cp-kafka-rest:7.4.0`
  - `provectuslabs/kafka-ui:latest`
- ✅ Verify Docker image integrity

### Step 3: Start Services

```bash
./start.sh
```

The startup process includes:

#### Pre-flight Checks
- ✅ Verify prerequisites are still met
- ✅ Check port availability (8000, 8080, 8082, 9092)
- ✅ Validate required files exist

#### Service Startup
- 🐳 Start Docker Compose stack
- ⏳ Wait for service health checks (up to 5 minutes)
- 🌐 Start FastAPI REST server
- 🧪 Run connectivity tests
- 📊 Display service information

## Configuration

### Environment Variables

Create a `.env` file in the project root to customize configuration:

```bash
# API Server Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_RELOAD=true

# Kafka Configuration
KAFKA_PORT=9092
KAFKA_JMX_PORT=9101

# Kafka REST Proxy Configuration
KAFKA_REST_PROXY_PORT=8082

# Kafka UI Configuration
KAFKA_UI_PORT=8080

# Health Check Configuration
HEALTH_CHECK_TIMEOUT=300
HEALTH_CHECK_INTERVAL=10

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=logs/api.log
```

### Docker Compose Customization

#### Resource Limits

Edit `docker-compose.yml` to set resource limits:

```yaml
services:
  kafka:
    # ... existing configuration
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'
        reservations:
          memory: 1G
          cpus: '0.5'
```

#### Volume Configuration

Customize data persistence:

```yaml
volumes:
  kafka-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /path/to/your/kafka/data
```

#### Network Configuration

Customize networking:

```yaml
networks:
  kafka-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16
```

### Python Configuration

#### Virtual Environment Location

By default, the virtual environment is created in `./venv`. To use a different location:

```bash
export VENV_PATH=/path/to/your/venv
python3 -m venv $VENV_PATH
source $VENV_PATH/bin/activate
pip install -r requirements.txt
```

#### Development Dependencies

For development work, install additional dependencies:

```bash
source venv/bin/activate
pip install -e ".[dev]"
```

## Verification

### Automated Tests

Run the connectivity test suite:

```bash
./test-stack.sh
```

This tests:
- Kafka broker connectivity
- REST Proxy functionality
- Kafka UI accessibility
- Topic creation/deletion
- Message production/consumption

### Manual Verification

#### 1. Check Service Status

```bash
# Check all services
curl http://localhost:8000/catalog

# Check cluster status
curl http://localhost:8000/cluster/status

# Check Docker containers
docker compose ps
```

#### 2. Test Topic Operations

```bash
# List topics
curl http://localhost:8000/topics

# Create a test topic
curl -X POST http://localhost:8000/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "test-topic", "partitions": 1, "replication_factor": 1}'

# Verify topic creation
curl http://localhost:8000/topics
```

#### 3. Test Message Operations

```bash
# Produce a message
curl -X POST http://localhost:8000/produce \
  -H "Content-Type: application/json" \
  -d '{"topic": "test-topic", "value": {"message": "Hello Kafka!"}}'

# Consume messages
curl "http://localhost:8000/consume?topic=test-topic&consumer_group=test-group"
```

#### 4. Access Web Interfaces

- **API Documentation**: http://localhost:8000/docs
- **Kafka UI**: http://localhost:8080
- **Service Catalog**: http://localhost:8000/catalog

## Troubleshooting

### Common Installation Issues

#### Python Version Issues

**Problem**: `python3: command not found`

**Solution**:
```bash
# macOS
brew install python3

# Ubuntu/Debian
sudo apt-get install python3

# CentOS/RHEL
sudo yum install python3
```

#### Docker Issues

**Problem**: `docker: command not found`

**Solution**: Install Docker following platform-specific instructions above.

**Problem**: `Cannot connect to the Docker daemon`

**Solution**:
```bash
# Start Docker service (Linux)
sudo systemctl start docker

# macOS: Start Docker Desktop application

# Add user to docker group (Linux)
sudo usermod -aG docker $USER
# Then log out and back in
```

#### Port Conflicts

**Problem**: `Port already in use`

**Solution**:
```bash
# Find process using port
lsof -i :8000

# Kill process
kill -9 <PID>

# Or use different ports by editing .env file
```

### Runtime Issues

#### Services Not Starting

**Problem**: Docker containers fail to start

**Solution**:
```bash
# Check Docker logs
docker compose logs

# Check specific service
docker compose logs kafka

# Restart with fresh containers
docker compose down
docker compose up -d
```

#### Health Check Failures

**Problem**: Services fail health checks

**Solution**:
```bash
# Increase timeout in start.sh
export HEALTH_CHECK_TIMEOUT=600

# Check service logs
docker compose logs kafka
docker compose logs kafka-rest-proxy
docker compose logs kafka-ui

# Manual health check
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

#### API Server Issues

**Problem**: REST API not responding

**Solution**:
```bash
# Check API logs
tail -f logs/api.log

# Check if virtual environment is activated
source venv/bin/activate

# Restart API server
./stop.sh --api-only
./start.sh --api-only
```

### Performance Issues

#### High Memory Usage

**Solution**:
```bash
# Reduce Kafka memory in docker-compose.yml
environment:
  KAFKA_HEAP_OPTS: "-Xmx512M -Xms512M"

# Or increase Docker Desktop memory allocation
```

#### Slow Startup

**Solution**:
```bash
# Pre-pull images
docker compose pull

# Increase health check timeout
export HEALTH_CHECK_TIMEOUT=600
```

### Getting Help

If you encounter issues not covered here:

1. **Check logs**:
   - API logs: `logs/api.log`
   - Docker logs: `docker compose logs`

2. **Verify prerequisites**:
   ```bash
   ./install.sh  # Re-run to check prerequisites
   ```

3. **Reset environment**:
   ```bash
   ./stop.sh --cleanup
   docker volume rm $(docker volume ls -q --filter name=kafka)
   rm -rf venv
   ./install.sh
   ```

4. **Report issues** with:
   - Operating system and version
   - Python version (`python3 --version`)
   - Docker version (`docker --version`)
   - Error messages from logs
   - Steps to reproduce the issue