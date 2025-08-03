# ❓ Frequently Asked Questions

Common questions and answers about Local Kafka Manager.

## 🚀 Getting Started

### Q: What is Local Kafka Manager?
**A:** Local Kafka Manager is a comprehensive development environment for Apache Kafka that provides a REST API, web UI, and complete management capabilities. It's designed to make Kafka development, testing, and learning easier with a one-command setup.

### Q: What are the system requirements?
**A:** 
- **Minimum**: 4GB RAM, 2GB disk space, Python 3.8+, Docker & Docker Compose
- **Recommended**: 8GB RAM, 10GB disk space for better performance
- **Operating Systems**: macOS, Linux (Ubuntu/CentOS), Windows (with WSL2)

### Q: How long does setup take?
**A:** 
- **First-time setup**: 2-5 minutes (depending on internet speed for Docker images)
- **Subsequent starts**: 30-60 seconds
- **Complete installation**: Under 2 minutes with `./install.sh`

### Q: Do I need prior Kafka experience?
**A:** No! Local Kafka Manager is designed for both beginners and experts. It includes:
- Interactive API documentation
- Visual web interface
- Comprehensive examples
- Step-by-step guides

---

## 🔧 Installation & Setup

### Q: Installation fails with "Docker not found" error
**A:** 
```bash
# Install Docker first
# macOS: Download Docker Desktop
# Ubuntu/Debian:
sudo apt update
sudo apt install docker.io docker-compose-plugin

# Start Docker service
sudo systemctl start docker
sudo usermod -aG docker $USER
# Logout and login again
```

### Q: Port 8000 is already in use
**A:** 
```bash
# Find what's using the port
lsof -i :8000

# Kill the process
kill -9 <PID>

# Or use a different port
echo "API_PORT=8001" >> .env
./start.sh
```

### Q: Docker Compose command not found
**A:** 
```bash
# Install Docker Compose plugin
sudo apt install docker-compose-plugin

# Or use standalone version
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

### Q: Permission denied errors with Docker
**A:** 
```bash
# Add user to docker group
sudo usermod -aG docker $USER

# Logout and login again, or run:
newgrp docker

# Test Docker access
docker run hello-world
```

---

## 🏗️ Architecture & Components

### Q: What components are included?
**A:** 
- **FastAPI REST Server**: HTTP API for all Kafka operations
- **Apache Kafka**: Single-node cluster using KRaft mode
- **Kafka REST Proxy**: HTTP interface for Kafka operations
- **Kafka UI**: Web-based visual management interface
- **Docker Compose**: Orchestrates all services

### Q: Why KRaft mode instead of Zookeeper?
**A:** 
- **Simpler**: No separate Zookeeper cluster needed
- **Faster**: Reduced latency and better performance
- **Modern**: KRaft is the future of Kafka (Zookeeper is being deprecated)
- **Lightweight**: Perfect for development and testing

### Q: Can I use this in production?
**A:** 
Local Kafka Manager is designed for development and testing. For production:
- Use a proper Kafka cluster with multiple brokers
- Implement proper security (authentication, encryption)
- Set up monitoring and alerting
- Consider managed services (Confluent Cloud, AWS MSK, etc.)

### Q: How does the REST API work?
**A:** 
The FastAPI server acts as a gateway that:
1. Receives HTTP requests
2. Translates them to Kafka operations
3. Uses Kafka REST Proxy for some operations
4. Returns structured JSON responses
5. Provides interactive documentation at `/docs`

---

## 🔌 API Usage

### Q: How do I create a topic?
**A:** 
```bash
curl -X POST http://localhost:8000/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-topic",
    "partitions": 3,
    "replication_factor": 1
  }'
```

### Q: How do I produce messages?
**A:** 
```bash
curl -X POST http://localhost:8000/produce \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "my-topic",
    "key": "user123",
    "value": {"action": "login", "timestamp": 1642234567}
  }'
```

### Q: How do I consume messages?
**A:** 
```bash
curl "http://localhost:8000/consume?topic=my-topic&consumer_group=my-group&max_messages=10"
```

### Q: Where is the API documentation?
**A:** 
Visit http://localhost:8000/docs for interactive Swagger UI documentation with:
- All available endpoints
- Request/response schemas
- Try-it-out functionality
- Code examples in multiple languages

### Q: Can I use the API from different programming languages?
**A:** 
Yes! The REST API works with any language that can make HTTP requests:
- **Python**: `requests`, `httpx`, `aiohttp`
- **JavaScript**: `fetch`, `axios`
- **Java**: `HttpClient`, `OkHttp`
- **Go**: `net/http`
- **cURL**: Command line testing

---

## 🖥️ Web Interface

### Q: How do I access the web UI?
**A:** 
Visit http://localhost:8080 after starting the services with `./start.sh`

### Q: What can I do in the web UI?
**A:** 
- **Browse Topics**: View all topics and their configurations
- **Message Explorer**: Browse and search messages
- **Consumer Groups**: Monitor consumer lag and status
- **Cluster Metrics**: Real-time performance monitoring
- **Configuration**: Manage cluster and topic settings

### Q: Web UI shows "Connection Error"
**A:** 
```bash
# Check if Kafka UI service is running
docker-compose ps

# Check logs
docker-compose logs kafka-ui

# Restart the service
docker-compose restart kafka-ui

# Verify Kafka is accessible
curl http://localhost:9092
```

### Q: Can I customize the web UI?
**A:** 
The Kafka UI is a separate service. You can:
- Configure it via environment variables in `docker-compose.yml`
- Use a different Kafka UI (like Kafdrop or AKHQ)
- Build your own interface using the REST API

---

## 🔍 Troubleshooting

### Q: Services won't start
**A:** 
```bash
# Check Docker status
docker info

# Check available resources
docker system df
free -h

# Clean up if needed
docker system prune
./stop.sh --cleanup

# Try starting again
./start.sh
```

### Q: Kafka connection timeouts
**A:** 
```bash
# Check if Kafka is running
docker-compose ps kafka

# Check Kafka logs
docker-compose logs kafka

# Test connectivity
docker exec kafka-manager_kafka_1 kafka-topics --bootstrap-server localhost:9092 --list

# Restart Kafka
docker-compose restart kafka
```

### Q: API returns 500 errors
**A:** 
```bash
# Check API logs
docker-compose logs api

# Check if all services are healthy
curl http://localhost:8000/health
curl http://localhost:8000/catalog

# Restart API service
docker-compose restart api
```

### Q: High memory usage
**A:** 
```bash
# Check memory usage
docker stats

# Reduce Kafka memory
echo 'KAFKA_HEAP_OPTS="-Xmx512M -Xms512M"' >> .env

# Restart services
./stop.sh
./start.sh
```

### Q: Disk space issues
**A:** 
```bash
# Check disk usage
df -h
docker system df

# Clean up Docker
docker system prune -a
docker volume prune

# Clean up logs
find logs/ -name "*.log" -mtime +7 -delete

# Reduce log retention
echo 'KAFKA_LOG_RETENTION_HOURS=24' >> .env
```

---

## 📊 Performance & Scaling

### Q: How many messages can it handle?
**A:** 
Performance depends on your system, but typical numbers:
- **Development**: 1,000-10,000 messages/second
- **Testing**: 10,000-50,000 messages/second
- **Single machine limit**: ~100,000 messages/second

For higher throughput, consider:
- Increasing Kafka memory allocation
- Using multiple partitions
- Optimizing message size
- Moving to a multi-broker cluster

### Q: Can I add more Kafka brokers?
**A:** 
The current setup is single-broker for simplicity. To add brokers:
1. Modify `docker-compose.yml` to add more Kafka services
2. Update replication factor for topics
3. Configure proper networking between brokers
4. Consider using Kafka's built-in clustering features

### Q: How do I optimize performance?
**A:** 
```bash
# Increase Kafka memory
echo 'KAFKA_HEAP_OPTS="-Xmx2G -Xms2G"' >> .env

# Optimize JVM settings
echo 'KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC"' >> .env

# Increase API workers
echo 'API_WORKERS=4' >> .env

# Use more partitions for topics
curl -X POST http://localhost:8000/topics \
  -d '{"name": "high-throughput", "partitions": 12}'
```

### Q: Monitoring and metrics?
**A:** 
Built-in monitoring available at:
- **Health**: http://localhost:8000/health
- **Metrics**: http://localhost:8000/metrics
- **Service Status**: http://localhost:8000/catalog

For advanced monitoring:
- Prometheus integration available
- Grafana dashboards included
- Custom metrics via API

---

## 🔒 Security

### Q: Is this secure for production?
**A:** 
**No**, this is designed for development. Production requires:
- Authentication and authorization
- SSL/TLS encryption
- Network security (firewalls, VPNs)
- Access logging and monitoring
- Regular security updates

### Q: Can I add authentication?
**A:** 
Yes, you can extend the FastAPI application:
```python
# Add to main.py
from fastapi.security import HTTPBearer
from fastapi import Depends, HTTPException

security = HTTPBearer()

def verify_token(token: str = Depends(security)):
    # Implement your token verification logic
    if not is_valid_token(token.credentials):
        raise HTTPException(status_code=401, detail="Invalid token")
    return token
```

### Q: How do I secure the network?
**A:** 
```bash
# Use firewall to restrict access
sudo ufw allow from 192.168.1.0/24 to any port 8000
sudo ufw deny 8000

# Use reverse proxy with SSL
# Configure nginx with SSL certificates
# Restrict Docker network access
```

---

## 🧪 Testing & Development

### Q: How do I run tests?
**A:** 
```bash
# Run all tests
python run_tests.py

# Run specific test categories
python -m pytest tests/test_integration.py -v
python -m pytest tests/test_performance.py -v

# Run with coverage
python -m pytest --cov=src tests/
```

### Q: How do I add custom functionality?
**A:** 
1. **Add API endpoints**: Modify `src/api/routes.py`
2. **Add business logic**: Create services in `src/services/`
3. **Add data models**: Define models in `src/models/`
4. **Add tests**: Create tests in `tests/`
5. **Update documentation**: Update README and API docs

### Q: Can I use this with my application?
**A:** 
Absolutely! Common integration patterns:
- **Microservices**: Use as message broker between services
- **Event Sourcing**: Store events in Kafka topics
- **Data Pipeline**: Stream data processing
- **Testing**: Mock Kafka for integration tests

### Q: How do I debug issues?
**A:** 
```bash
# Enable debug logging
echo 'LOG_LEVEL=DEBUG' >> .env

# Check all service logs
docker-compose logs

# Test individual components
curl http://localhost:8000/health
curl http://localhost:8082/topics
curl http://localhost:8080

# Use the test script
./test-stack.sh
```

---

## 🔄 Updates & Maintenance

### Q: How do I update to the latest version?
**A:** 
```bash
# Stop services
./stop.sh

# Pull latest changes
git pull origin main

# Update dependencies
pip install -r requirements.txt

# Restart services
./start.sh

# Verify everything works
./test-stack.sh
```

### Q: How do I backup my data?
**A:** 
```bash
# Backup topics list
docker exec kafka-manager_kafka_1 kafka-topics --bootstrap-server localhost:9092 --list > topics-backup.txt

# Backup configuration
cp .env .env.backup

# For message data, consider using Kafka's built-in tools
# or implement custom backup via the API
```

### Q: Can I contribute to the project?
**A:** 
Yes! We welcome contributions:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

See [CONTRIBUTING.md](../CONTRIBUTING.md) for detailed guidelines.

---

## 🌐 Integration & Ecosystem

### Q: Can I use this with Kafka Connect?
**A:** 
Yes, you can add Kafka Connect to the Docker Compose setup:
```yaml
kafka-connect:
  image: confluentinc/cp-kafka-connect:7.4.0
  ports:
    - "8083:8083"
  environment:
    CONNECT_BOOTSTRAP_SERVERS: kafka:9092
    CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
    # ... other Connect configuration
```

### Q: Does it work with Schema Registry?
**A:** 
You can add Confluent Schema Registry:
```yaml
schema-registry:
  image: confluentinc/cp-schema-registry:7.4.0
  ports:
    - "8081:8081"
  environment:
    SCHEMA_REGISTRY_HOST_NAME: schema-registry
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
```

### Q: Can I use it with streaming frameworks?
**A:** 
Yes! It works with:
- **Kafka Streams**: Java/Scala stream processing
- **Apache Flink**: Real-time stream processing
- **Apache Spark**: Batch and stream processing
- **Python**: `kafka-python`, `confluent-kafka-python`
- **Node.js**: `kafkajs`, `node-rdkafka`

---

## 📞 Support & Community

### Q: Where can I get help?
**A:** 
- **Documentation**: Check the `/docs` directory
- **GitHub Issues**: Report bugs and request features
- **GitHub Discussions**: Community Q&A
- **Stack Overflow**: Tag questions with `kafka-local-manager`

### Q: How do I report a bug?
**A:** 
1. Check existing issues first
2. Gather relevant information (logs, system info, steps to reproduce)
3. Create a detailed issue on GitHub
4. Include error messages and configuration

### Q: Can I request new features?
**A:** 
Yes! Create a feature request issue with:
- Clear description of the feature
- Use case and benefits
- Proposed implementation (if you have ideas)
- Willingness to contribute

### Q: Is there a roadmap?
**A:** 
Check the GitHub repository for:
- Open issues and milestones
- Project boards
- Release notes
- Future plans in discussions

---

## 🎓 Learning Resources

### Q: Where can I learn more about Kafka?
**A:** 
- **Official Docs**: https://kafka.apache.org/documentation/
- **Confluent Tutorials**: https://developer.confluent.io/
- **Books**: "Kafka: The Definitive Guide", "Kafka Streams in Action"
- **Courses**: Confluent Developer courses, Udemy, Coursera

### Q: What are some common Kafka patterns?
**A:** 
- **Event Sourcing**: Store all changes as events
- **CQRS**: Separate read and write models
- **Saga Pattern**: Distributed transaction management
- **Outbox Pattern**: Reliable event publishing
- **Stream Processing**: Real-time data transformation

### Q: How do I design topics and partitions?
**A:** 
**Topic Design:**
- Use clear, descriptive names
- Group related events in the same topic
- Consider data retention requirements
- Plan for schema evolution

**Partition Strategy:**
- More partitions = higher parallelism
- Consider consumer group size
- Use meaningful partition keys
- Balance between throughput and complexity

---

*Still have questions? Check our [GitHub Discussions](https://github.com/chandan1819/kafka-cluster/discussions) or create an issue!*