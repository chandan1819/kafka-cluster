#!/bin/bash
# Multi-cluster Kafka Manager Start Script
# Generated on 2025-08-07T19:32:36.288548

set -e

echo "🚀 Starting Multi-cluster Kafka Manager..."

# Change to installation directory
cd "/Users/aadityasinha/code/kafka-cluster"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Start services
if [ -f "docker-compose.yml" ]; then
    docker-compose up -d
    echo "✅ Services started successfully"
    echo "🌐 Web interface available at: http://localhost:8000"
else
    echo "❌ docker-compose.yml not found"
    exit 1
fi
