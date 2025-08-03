# 🚀 Deployment Guide

Comprehensive guide for deploying Local Kafka Manager in different environments.

## 🎯 Deployment Options

### 1. 🏠 Local Development (Recommended)
- **Use Case**: Development, testing, learning
- **Resources**: 4GB RAM, 2GB disk
- **Setup Time**: 2 minutes
- **Complexity**: Low

### 2. 🖥️ Single Server Deployment
- **Use Case**: Small teams, staging environments
- **Resources**: 8GB RAM, 10GB disk
- **Setup Time**: 10 minutes
- **Complexity**: Medium

### 3. ☁️ Cloud Deployment
- **Use Case**: Production-like environments
- **Resources**: Scalable
- **Setup Time**: 30 minutes
- **Complexity**: High

---

## 🏠 Local Development Deployment

### Quick Start
```bash
# Clone repository
git clone https://github.com/chandan1819/kafka-cluster.git
cd kafka-cluster

# Install and start
./install.sh
./start.sh
```

### Custom Configuration
Create `.env` file for customization:
```bash
# API Configuration
API_PORT=8000
API_HOST=0.0.0.0
API_WORKERS=1

# Kafka Configuration
KAFKA_PORT=9092
KAFKA_REST_PROXY_PORT=8082
KAFKA_UI_PORT=8080

# Resource Limits
KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC"

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

### Development Mode
```bash
# Start with hot reload
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

# Start only Docker services
./start.sh --docker-only
```

---

## 🖥️ Single Server Deployment

### Prerequisites
- Ubuntu 20.04+ / CentOS 8+ / macOS 10.15+
- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM, 10GB disk space
- Python 3.8+

### Installation Steps

#### 1. System Preparation
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install -y python3 python3-pip docker.io docker-compose-plugin
sudo usermod -aG docker $USER

# CentOS/RHEL
sudo yum update -y
sudo yum install -y python3 python3-pip docker docker-compose
sudo systemctl enable --now docker
sudo usermod -aG docker $USER

# Logout and login again for group changes
```

#### 2. Application Setup
```bash
# Clone and setup
git clone https://github.com/chandan1819/kafka-cluster.git
cd kafka-cluster

# Create production environment file
cp .env.example .env.production

# Edit configuration
vim .env.production
```

#### 3. Production Configuration
```bash
# .env.production
ENVIRONMENT=production
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4

# Security
API_CORS_ORIGINS=["http://localhost:3000", "https://yourdomain.com"]
API_RATE_LIMIT=100

# Kafka Production Settings
KAFKA_HEAP_OPTS="-Xmx2G -Xms2G"
KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20"
KAFKA_LOG_RETENTION_HOURS=168
KAFKA_LOG_SEGMENT_BYTES=1073741824

# Monitoring
LOG_LEVEL=INFO
METRICS_ENABLED=true
HEALTH_CHECK_INTERVAL=30
```

#### 4. Start Services
```bash
# Install with production config
ENV_FILE=.env.production ./install.sh

# Start with production settings
ENV_FILE=.env.production ./start.sh
```

#### 5. Setup Systemd Service (Optional)
```bash
# Create service file
sudo tee /etc/systemd/system/kafka-manager.service > /dev/null <<EOF
[Unit]
Description=Local Kafka Manager
After=docker.service
Requires=docker.service

[Service]
Type=forking
User=$USER
WorkingDirectory=/home/$USER/kafka-cluster
ExecStart=/home/$USER/kafka-cluster/start.sh
ExecStop=/home/$USER/kafka-cluster/stop.sh
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable kafka-manager
sudo systemctl start kafka-manager
```

---

## ☁️ Cloud Deployment

### AWS EC2 Deployment

#### 1. Launch EC2 Instance
```bash
# Recommended instance: t3.large (2 vCPU, 8GB RAM)
# AMI: Ubuntu 20.04 LTS
# Storage: 20GB GP3
# Security Group: Allow ports 22, 8000, 8080, 8082, 9092
```

#### 2. Setup Script
```bash
#!/bin/bash
# setup-aws.sh

# Update system
sudo apt update && sudo apt upgrade -y

# Install dependencies
sudo apt install -y python3 python3-pip docker.io docker-compose-plugin git

# Configure Docker
sudo usermod -aG docker ubuntu
sudo systemctl enable docker
sudo systemctl start docker

# Clone and setup application
cd /home/ubuntu
git clone https://github.com/chandan1819/kafka-cluster.git
cd kafka-cluster

# Create production config
cat > .env.production << EOF
ENVIRONMENT=production
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4
KAFKA_HEAP_OPTS="-Xmx4G -Xms4G"
LOG_LEVEL=INFO
EOF

# Install and start
ENV_FILE=.env.production ./install.sh
ENV_FILE=.env.production ./start.sh
```

#### 3. Load Balancer Setup (ALB)
```yaml
# alb-config.yaml
TargetGroup:
  Name: kafka-manager-targets
  Port: 8000
  Protocol: HTTP
  HealthCheck:
    Path: /health
    IntervalSeconds: 30
    TimeoutSeconds: 5
    HealthyThresholdCount: 2
    UnhealthyThresholdCount: 3

LoadBalancer:
  Name: kafka-manager-alb
  Scheme: internet-facing
  Type: application
  SecurityGroups:
    - sg-web-access
  Subnets:
    - subnet-public-1a
    - subnet-public-1b
```

### Google Cloud Platform (GCP)

#### 1. Create VM Instance
```bash
# Create instance
gcloud compute instances create kafka-manager \
  --zone=us-central1-a \
  --machine-type=e2-standard-2 \
  --image-family=ubuntu-2004-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=20GB \
  --tags=kafka-manager

# Create firewall rules
gcloud compute firewall-rules create kafka-manager-ports \
  --allow tcp:8000,tcp:8080,tcp:8082,tcp:9092 \
  --source-ranges 0.0.0.0/0 \
  --target-tags kafka-manager
```

#### 2. Setup Application
```bash
# SSH to instance
gcloud compute ssh kafka-manager --zone=us-central1-a

# Run setup script (same as AWS)
curl -sSL https://raw.githubusercontent.com/chandan1819/kafka-cluster/main/scripts/setup-cloud.sh | bash
```

### Azure Deployment

#### 1. Create Resource Group and VM
```bash
# Create resource group
az group create --name kafka-manager-rg --location eastus

# Create VM
az vm create \
  --resource-group kafka-manager-rg \
  --name kafka-manager-vm \
  --image UbuntuLTS \
  --size Standard_B2s \
  --admin-username azureuser \
  --generate-ssh-keys

# Open ports
az vm open-port --resource-group kafka-manager-rg --name kafka-manager-vm --port 8000,8080,8082,9092
```

---

## 🐳 Docker Deployment

### Standalone Docker
```bash
# Build custom image
docker build -t kafka-manager:latest .

# Run with Docker Compose
docker-compose -f docker-compose.prod.yml up -d
```

### Docker Swarm
```yaml
# docker-stack.yml
version: '3.8'
services:
  kafka-manager-api:
    image: kafka-manager:latest
    ports:
      - "8000:8000"
    environment:
      - ENVIRONMENT=production
      - API_WORKERS=4
    deploy:
      replicas: 2
      update_config:
        parallelism: 1
        delay: 10s
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
    networks:
      - kafka-network

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    environment:
      KAFKA_KRAFT_CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:9092'
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://kafka:9092,CONTROLLER://kafka:29093'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
    deploy:
      replicas: 1
      placement:
        constraints:
          - node.role == manager
    volumes:
      - kafka-data:/tmp/kraft-combined-logs
    networks:
      - kafka-network

volumes:
  kafka-data:

networks:
  kafka-network:
    driver: overlay
```

### Kubernetes Deployment
```yaml
# k8s-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-manager
  labels:
    app: kafka-manager
spec:
  replicas: 2
  selector:
    matchLabels:
      app: kafka-manager
  template:
    metadata:
      labels:
        app: kafka-manager
    spec:
      containers:
      - name: kafka-manager
        image: kafka-manager:latest
        ports:
        - containerPort: 8000
        env:
        - name: ENVIRONMENT
          value: "production"
        - name: API_WORKERS
          value: "4"
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-manager-service
spec:
  selector:
    app: kafka-manager
  ports:
  - protocol: TCP
    port: 80
    targetPort: 8000
  type: LoadBalancer
```

---

## 🔒 Security Considerations

### Network Security
```bash
# Firewall rules (Ubuntu/Debian)
sudo ufw allow 22/tcp    # SSH
sudo ufw allow 8000/tcp  # API
sudo ufw allow 8080/tcp  # Kafka UI
sudo ufw deny 9092/tcp   # Kafka (internal only)
sudo ufw deny 8082/tcp   # REST Proxy (internal only)
sudo ufw enable
```

### SSL/TLS Configuration
```yaml
# nginx-ssl.conf
server {
    listen 443 ssl http2;
    server_name your-domain.com;
    
    ssl_certificate /etc/ssl/certs/your-domain.crt;
    ssl_certificate_key /etc/ssl/private/your-domain.key;
    
    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
    
    location /ws {
        proxy_pass http://localhost:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

### Authentication Setup
```python
# Add to .env.production
API_AUTH_ENABLED=true
API_AUTH_SECRET_KEY="your-secret-key-here"
API_AUTH_ALGORITHM="HS256"
API_AUTH_ACCESS_TOKEN_EXPIRE_MINUTES=30
```

---

## 📊 Monitoring & Observability

### Prometheus Configuration
```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'kafka-manager'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
    scrape_interval: 10s
```

### Grafana Dashboard
```json
{
  "dashboard": {
    "title": "Kafka Manager Metrics",
    "panels": [
      {
        "title": "API Request Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(api_requests_total[5m])",
            "legendFormat": "Requests/sec"
          }
        ]
      },
      {
        "title": "Kafka Message Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(kafka_messages_total[5m])",
            "legendFormat": "Messages/sec"
          }
        ]
      }
    ]
  }
}
```

### Log Aggregation
```yaml
# docker-compose.logging.yml
version: '3.8'
services:
  kafka-manager:
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
        labels: "service=kafka-manager"
  
  fluentd:
    image: fluent/fluentd:v1.14
    volumes:
      - ./fluentd.conf:/fluentd/etc/fluent.conf
      - /var/log:/var/log:ro
    ports:
      - "24224:24224"
```

---

## 🔧 Maintenance

### Backup Strategy
```bash
#!/bin/bash
# backup.sh

# Backup Kafka data
docker exec kafka-manager_kafka_1 kafka-topics --bootstrap-server localhost:9092 --list > topics-backup.txt

# Backup configuration
cp .env.production .env.production.backup.$(date +%Y%m%d)

# Backup logs
tar -czf logs-backup-$(date +%Y%m%d).tar.gz logs/

# Upload to S3 (optional)
# aws s3 cp topics-backup.txt s3://your-backup-bucket/
```

### Update Procedure
```bash
#!/bin/bash
# update.sh

# Stop services
./stop.sh

# Backup current version
cp -r . ../kafka-cluster-backup-$(date +%Y%m%d)

# Pull latest changes
git pull origin main

# Update dependencies
pip install -r requirements.txt

# Run database migrations (if any)
# python manage.py migrate

# Start services
./start.sh

# Verify deployment
./test-stack.sh
```

### Health Monitoring
```bash
#!/bin/bash
# health-check.sh

# Check API health
if curl -f http://localhost:8000/health > /dev/null 2>&1; then
    echo "✅ API is healthy"
else
    echo "❌ API is unhealthy"
    # Send alert or restart service
    systemctl restart kafka-manager
fi

# Check Kafka connectivity
if docker exec kafka-manager_kafka_1 kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo "✅ Kafka is accessible"
else
    echo "❌ Kafka is not accessible"
    # Restart Kafka service
    docker-compose restart kafka
fi

# Check disk space
DISK_USAGE=$(df / | tail -1 | awk '{print $5}' | sed 's/%//')
if [ $DISK_USAGE -gt 80 ]; then
    echo "⚠️ Disk usage is ${DISK_USAGE}% - cleanup required"
    # Cleanup old logs
    find logs/ -name "*.log" -mtime +7 -delete
fi
```

---

## 🚨 Troubleshooting

### Common Deployment Issues

#### Port Conflicts
```bash
# Check what's using ports
sudo netstat -tulpn | grep :8000
sudo lsof -i :8000

# Kill conflicting processes
sudo kill -9 <PID>

# Use alternative ports
echo "API_PORT=8001" >> .env.production
```

#### Memory Issues
```bash
# Check memory usage
free -h
docker stats

# Increase swap (if needed)
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

#### Docker Issues
```bash
# Restart Docker daemon
sudo systemctl restart docker

# Clean up Docker resources
docker system prune -a
docker volume prune

# Check Docker logs
journalctl -u docker.service
```

#### Network Connectivity
```bash
# Test internal connectivity
docker exec kafka-manager_api_1 curl http://kafka:9092
docker exec kafka-manager_api_1 curl http://kafka-rest-proxy:8082

# Check DNS resolution
docker exec kafka-manager_api_1 nslookup kafka
```

---

## 📈 Performance Tuning

### Kafka Optimization
```bash
# High-throughput configuration
KAFKA_NUM_NETWORK_THREADS=8
KAFKA_NUM_IO_THREADS=16
KAFKA_SOCKET_SEND_BUFFER_BYTES=102400
KAFKA_SOCKET_RECEIVE_BUFFER_BYTES=102400
KAFKA_SOCKET_REQUEST_MAX_BYTES=104857600
KAFKA_NUM_PARTITIONS=3
KAFKA_DEFAULT_REPLICATION_FACTOR=1
KAFKA_LOG_RETENTION_HOURS=168
KAFKA_LOG_SEGMENT_BYTES=1073741824
KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=300000
```

### API Performance
```bash
# FastAPI optimization
API_WORKERS=4  # CPU cores * 2
UVICORN_WORKER_CLASS=uvicorn.workers.UvicornWorker
UVICORN_MAX_REQUESTS=1000
UVICORN_MAX_REQUESTS_JITTER=50
```

### System Optimization
```bash
# Kernel parameters
echo 'vm.swappiness=1' | sudo tee -a /etc/sysctl.conf
echo 'vm.max_map_count=262144' | sudo tee -a /etc/sysctl.conf
echo 'net.core.somaxconn=65535' | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

---

## 🔄 CI/CD Integration

### GitHub Actions
```yaml
# .github/workflows/deploy.yml
name: Deploy to Production

on:
  push:
    branches: [main]
  release:
    types: [published]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup SSH
        uses: webfactory/ssh-agent@v0.7.0
        with:
          ssh-private-key: ${{ secrets.SSH_PRIVATE_KEY }}
      
      - name: Deploy to server
        run: |
          ssh -o StrictHostKeyChecking=no ${{ secrets.SERVER_USER }}@${{ secrets.SERVER_HOST }} '
            cd /home/ubuntu/kafka-cluster
            git pull origin main
            ./stop.sh
            ENV_FILE=.env.production ./start.sh
            ./test-stack.sh
          '
      
      - name: Health Check
        run: |
          sleep 30
          curl -f http://${{ secrets.SERVER_HOST }}:8000/health
```

### GitLab CI
```yaml
# .gitlab-ci.yml
stages:
  - test
  - deploy

test:
  stage: test
  script:
    - python run_tests.py
  only:
    - merge_requests
    - main

deploy_production:
  stage: deploy
  script:
    - ssh $SERVER_USER@$SERVER_HOST "cd /opt/kafka-cluster && git pull && ./deploy.sh"
  only:
    - main
  environment:
    name: production
    url: http://$SERVER_HOST:8000
```

---

## 📊 Scaling Considerations

### Horizontal Scaling
```yaml
# docker-compose.scale.yml
version: '3.8'
services:
  kafka-manager-api:
    image: kafka-manager:latest
    deploy:
      replicas: 3
    environment:
      - API_WORKERS=2
    
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
    depends_on:
      - kafka-manager-api
```

### Load Balancer Configuration
```nginx
# nginx.conf
upstream kafka_manager {
    least_conn;
    server kafka-manager-api_1:8000;
    server kafka-manager-api_2:8000;
    server kafka-manager-api_3:8000;
}

server {
    listen 80;
    
    location / {
        proxy_pass http://kafka_manager;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
    
    location /health {
        access_log off;
        proxy_pass http://kafka_manager;
    }
}
```

---

## 🔐 Production Security Checklist

- [ ] **Network Security**
  - [ ] Firewall configured
  - [ ] Only necessary ports exposed
  - [ ] Internal services not publicly accessible

- [ ] **Application Security**
  - [ ] Authentication enabled
  - [ ] HTTPS/TLS configured
  - [ ] CORS properly configured
  - [ ] Rate limiting enabled

- [ ] **Infrastructure Security**
  - [ ] Regular security updates
  - [ ] SSH key-based authentication
  - [ ] Fail2ban or similar protection
  - [ ] Log monitoring enabled

- [ ] **Data Security**
  - [ ] Backup strategy implemented
  - [ ] Data encryption at rest
  - [ ] Secure credential management
  - [ ] Access logging enabled

---

## 📞 Support & Maintenance

### Monitoring Checklist
- [ ] API health endpoint responding
- [ ] Kafka cluster accessible
- [ ] Disk space sufficient (< 80%)
- [ ] Memory usage normal (< 80%)
- [ ] Error rates acceptable (< 1%)
- [ ] Response times normal (< 500ms)

### Regular Maintenance Tasks
- **Daily**: Check logs, monitor metrics
- **Weekly**: Review performance, update dependencies
- **Monthly**: Security updates, backup verification
- **Quarterly**: Capacity planning, disaster recovery testing

### Emergency Contacts
```bash
# Create emergency runbook
cat > EMERGENCY_RUNBOOK.md << EOF
# Emergency Response Runbook

## Service Down
1. Check service status: systemctl status kafka-manager
2. Check logs: journalctl -u kafka-manager -f
3. Restart service: systemctl restart kafka-manager
4. Verify health: curl http://localhost:8000/health

## High CPU/Memory
1. Check processes: top, htop
2. Check Docker stats: docker stats
3. Scale down if needed: docker-compose scale api=1
4. Investigate root cause in logs

## Disk Full
1. Check usage: df -h
2. Clean logs: find logs/ -name "*.log" -mtime +7 -delete
3. Clean Docker: docker system prune
4. Add more storage if needed

## Contact Information
- Primary: your-email@company.com
- Secondary: backup-email@company.com
- Escalation: manager@company.com
EOF
```

---

*This deployment guide covers comprehensive production deployment scenarios. For specific questions or custom requirements, please refer to the troubleshooting section or create an issue in the repository.*