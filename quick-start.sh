#!/bin/bash
# Quick Start Script for Multi-Cluster Kafka Manager

echo "🚀 Starting Multi-Cluster Kafka Manager..."

# Start the management server in background
echo "📡 Starting management server..."
python3 src/main.py &
MANAGER_PID=$!

# Wait for server to start
echo "⏳ Waiting for server to start..."
sleep 5

# Check if server is running
if curl -s http://localhost:8000/health > /dev/null; then
    echo "✅ Management server is running at http://localhost:8000"
else
    echo "❌ Failed to start management server"
    exit 1
fi

# List available clusters
echo "📋 Available clusters:"
curl -s http://localhost:8000/api/v1/clusters | jq '.[].id' 2>/dev/null || echo "dev-cluster"

# Start the dev-cluster
echo "🔄 Starting dev-cluster..."
curl -X POST http://localhost:8000/api/v1/clusters/dev-cluster/start

# Wait for cluster to start
echo "⏳ Waiting for cluster to start..."
sleep 10

# Check cluster status
echo "📊 Cluster status:"
curl -s http://localhost:8000/api/v1/clusters/dev-cluster/status

echo ""
echo "🎉 Multi-Cluster Kafka Manager is ready!"
echo ""
echo "📍 Access Points:"
echo "  • Management Dashboard: http://localhost:8000"
echo "  • API Documentation: http://localhost:8000/docs"
echo "  • Kafka UI: http://localhost:8080"
echo "  • REST Proxy: http://localhost:8082"
echo "  • Kafka Broker: localhost:9092"
echo ""
echo "🛑 To stop: kill $MANAGER_PID"
echo "💡 Check logs in: logs/"