#!/bin/bash
# Multi-cluster Kafka Manager Stop Script
# Generated on 2025-08-09T10:56:17.751157

set -e

echo "🛑 Stopping Multi-cluster Kafka Manager..."

# Change to installation directory
cd "/Users/aadityasinha/code/kafka-cluster"

# Stop services
if [ -f "docker-compose.yml" ]; then
    docker-compose down
    echo "✅ Services stopped successfully"
else
    echo "❌ docker-compose.yml not found"
    exit 1
fi
