# Design Document

## Overview

The simplified Local Kafka Manager will be a lightweight FastAPI application that provides essential Kafka management capabilities for local development. The design focuses on simplicity, ease of use, and minimal configuration while maintaining core functionality for cluster, topic, and message management.

## Architecture

### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Browser   │    │   FastAPI App   │    │  Docker Compose │
│                 │◄──►│   (Port 5000)   │◄──►│   Kafka Stack   │
│  Simple UI      │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │  Kafka Services │
                       │  - Kafka:9092   │
                       │  - UI:8080      │
                       │  - REST:8082    │
                       └─────────────────┘
```

### Simplified Component Structure

```
src/
├── main.py              # FastAPI app entry point
├── config.py            # Simple configuration
├── models/              # Pydantic models
│   ├── cluster.py
│   ├── topic.py
│   └── message.py
├── services/            # Core business logic
│   ├── cluster_service.py
│   ├── topic_service.py
│   └── message_service.py
└── api/                 # API routes
    └── routes.py        # All routes in one file
```

## Components and Interfaces

### 1. Configuration Management

**Simple Configuration Class:**
- Port configuration (5000 with 5001 fallback)
- Kafka connection settings
- Docker compose file path
- Basic logging configuration

**Key Features:**
- Environment variable support
- Sensible defaults
- No complex validation or multiple environments

### 2. Core Services

**ClusterService:**
- Start/stop Kafka cluster via Docker Compose
- Get cluster status
- Simple health checks

**TopicService:**
- List topics
- Create topics with basic configuration
- Delete topics
- Get topic metadata

**MessageService:**
- Produce messages to topics
- Consume messages from topics
- Simple consumer group management

### 3. API Layer

**Single Routes File:**
- All endpoints in one file for simplicity
- RESTful design
- JSON responses
- Basic error handling

**Core Endpoints:**
- `GET /` - API information
- `GET /health` - Health check
- `POST /cluster/start` - Start cluster
- `POST /cluster/stop` - Stop cluster
- `GET /cluster/status` - Cluster status
- `GET /topics` - List topics
- `POST /topics` - Create topic
- `DELETE /topics/{name}` - Delete topic
- `POST /messages/produce` - Produce message
- `GET /messages/consume` - Consume messages

### 4. Data Models

**Simplified Pydantic Models:**
- ClusterStatus
- TopicInfo
- MessageRequest/Response
- Basic error responses

## Data Models

### Core Models

```python
class ClusterStatus(BaseModel):
    status: str  # "running", "stopped", "error"
    services: Dict[str, str]  # service -> status
    uptime: Optional[str]

class TopicInfo(BaseModel):
    name: str
    partitions: int
    replication_factor: int
    config: Dict[str, str]

class MessageRequest(BaseModel):
    topic: str
    key: Optional[str]
    value: str
    partition: Optional[int]

class MessageResponse(BaseModel):
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: str
    timestamp: datetime
```

## Error Handling

### Simple Error Strategy

1. **HTTP Status Codes:**
   - 200: Success
   - 400: Bad Request (validation errors)
   - 404: Not Found (topic/resource not found)
   - 500: Internal Server Error
   - 503: Service Unavailable (Kafka not running)

2. **Error Response Format:**
```python
class ErrorResponse(BaseModel):
    error: str
    message: str
    details: Optional[Dict[str, Any]]
```

3. **Error Categories:**
   - Docker/Kafka connection errors
   - Topic operation errors
   - Message operation errors
   - Validation errors

## Testing Strategy

### Minimal Testing Approach

1. **Basic Unit Tests:**
   - Core service functionality
   - Model validation
   - Configuration loading

2. **Simple Integration Tests:**
   - API endpoint responses
   - Docker compose operations
   - Kafka connectivity

3. **No Complex Testing:**
   - No performance testing
   - No load testing
   - No extensive mocking
   - No benchmark tests

### Test Structure

```
tests/
├── test_services.py     # Service layer tests
├── test_api.py          # API endpoint tests
└── conftest.py          # Basic test configuration
```

## Deployment and Configuration

### Port Configuration

1. **Primary Port:** 5000
2. **Fallback Port:** 5001
3. **Port Detection Logic:**
   - Try to bind to port 5000
   - If occupied, try 5001
   - If both occupied, exit with error

### Docker Integration

1. **Simplified docker-compose.yml:**
   - Kafka broker
   - Kafka UI
   - REST Proxy
   - No additional services

2. **Container Management:**
   - Simple start/stop operations
   - Basic health checks
   - No complex orchestration

### Environment Variables

```bash
# Optional overrides
KAFKA_PORT=5000
KAFKA_HOST=localhost
DOCKER_COMPOSE_FILE=docker-compose.yml
LOG_LEVEL=INFO
```

## Removed Features

### Multi-Cluster Support
- No cluster registry
- No template management
- No cross-cluster operations
- No cluster factory patterns

### Advanced Monitoring
- No Prometheus metrics
- No detailed health monitoring
- No performance tracking
- No system resource monitoring

### Security Features
- No authentication
- No authorization
- No SSL/TLS
- No access control

### Complex Middleware
- No logging middleware
- No backward compatibility
- No request tracking
- No rate limiting

### Advanced Storage
- No database backends
- No file storage abstraction
- No migration system
- No backup/restore

## Implementation Priorities

### Phase 1: Core Infrastructure
1. Simplified configuration system
2. Basic FastAPI application structure
3. Port detection and binding logic
4. Docker compose integration

### Phase 2: Core Services
1. Cluster management service
2. Topic management service
3. Message management service
4. Basic error handling

### Phase 3: API Layer
1. REST API endpoints
2. Request/response models
3. Basic validation
4. Simple web interface (optional)

### Phase 4: Cleanup
1. Remove unnecessary files
2. Update documentation
3. Simplify startup scripts
4. Clean up dependencies