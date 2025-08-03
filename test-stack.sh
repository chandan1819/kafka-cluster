#!/bin/bash

echo "Testing Kafka Stack Connectivity..."
echo "=================================="

# Test Kafka broker
echo "1. Testing Kafka broker connectivity..."
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "   ✓ Kafka broker is accessible"
else
    echo "   ✗ Kafka broker is not accessible"
    exit 1
fi

# Test Kafka REST Proxy
echo "2. Testing Kafka REST Proxy..."
response=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8082/topics)
if [ "$response" = "200" ]; then
    echo "   ✓ Kafka REST Proxy is accessible"
else
    echo "   ✗ Kafka REST Proxy is not accessible (HTTP $response)"
    exit 1
fi

# Test Kafka UI
echo "3. Testing Kafka UI..."
response=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080)
if [ "$response" = "200" ]; then
    echo "   ✓ Kafka UI is accessible"
else
    echo "   ✗ Kafka UI is not accessible (HTTP $response)"
    exit 1
fi

# Test topic creation
echo "4. Testing topic creation..."
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic connectivity-test --partitions 1 --replication-factor 1 > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "   ✓ Topic creation successful"
else
    echo "   ✗ Topic creation failed"
    exit 1
fi

# Test message production via REST Proxy
echo "5. Testing message production via REST Proxy..."
response=$(curl -s -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" -d '{"records":[{"value":{"test":"connectivity"}}]}' http://localhost:8082/topics/connectivity-test)
if [[ $response == *"offsets"* ]]; then
    echo "   ✓ Message production successful"
else
    echo "   ✗ Message production failed"
    exit 1
fi

# Clean up test topic
echo "6. Cleaning up test topic..."
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic connectivity-test > /dev/null 2>&1

echo ""
echo "✓ All tests passed! Kafka stack is fully functional."
echo ""
echo "Services available at:"
echo "  - Kafka Broker: localhost:9092"
echo "  - Kafka REST Proxy: http://localhost:8082"
echo "  - Kafka UI: http://localhost:8080"