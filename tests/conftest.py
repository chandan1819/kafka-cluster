"""
Pytest configuration and shared fixtures
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock
from fastapi.testclient import TestClient
from typing import Dict, List, Any

from src.main import app
from src.models.base import ServiceStatus
from src.models.cluster import ClusterStatus, ServiceHealth
from src.models.topic import TopicInfo, TopicConfig
from src.models.message import Message, ProduceResult, ConsumeResponse
from src.models.catalog import CatalogResponse


@pytest.fixture
def client():
    """Test client for FastAPI application"""
    return TestClient(app)


@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# Data fixtures
@pytest.fixture
def sample_topic_config():
    """Sample topic configuration for testing"""
    return {
        "name": "test-topic",
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": "86400000"}
    }


@pytest.fixture
def sample_topic_config_model():
    """Sample TopicConfig model for testing"""
    return TopicConfig(
        name="test-topic",
        partitions=3,
        replication_factor=1,
        config={"retention.ms": "86400000"}
    )


@pytest.fixture
def sample_message():
    """Sample message for testing"""
    return {
        "topic": "test-topic",
        "key": "test-key",
        "value": {"action": "test", "timestamp": 1642234567}
    }


@pytest.fixture
def sample_message_model():
    """Sample Message model for testing"""
    return Message(
        topic="test-topic",
        partition=0,
        offset=123,
        key="test-key",
        value={"action": "test", "timestamp": 1642234567},
        timestamp=1642234567000
    )


@pytest.fixture
def sample_messages_batch():
    """Sample batch of messages for testing"""
    return [
        {
            "topic": "test-topic",
            "key": f"key-{i}",
            "value": {"id": i, "data": f"message-{i}", "timestamp": 1642234567 + i}
        }
        for i in range(10)
    ]


@pytest.fixture
def sample_topic_info():
    """Sample TopicInfo model for testing"""
    return TopicInfo(
        name="test-topic",
        partitions=3,
        replication_factor=1,
        size_bytes=1024
    )


@pytest.fixture
def sample_cluster_status():
    """Sample ClusterStatus model for testing"""
    return ClusterStatus(
        status=ServiceStatus.RUNNING,
        broker_count=1,
        version="7.4.0",
        endpoints={
            "kafka": "localhost:9092",
            "kafka-rest-proxy": "localhost:8082",
            "kafka-ui": "localhost:8080"
        },
        uptime=3600,
        services={
            "kafka": ServiceHealth(status=ServiceStatus.RUNNING),
            "kafka-rest-proxy": ServiceHealth(status=ServiceStatus.RUNNING),
            "kafka-ui": ServiceHealth(status=ServiceStatus.RUNNING)
        }
    )


@pytest.fixture
def sample_service_health():
    """Sample ServiceHealth model for testing"""
    return ServiceHealth(
        status=ServiceStatus.RUNNING,
        last_check="2024-01-15T10:30:00Z",
        error_message=None
    )


@pytest.fixture
def sample_produce_result():
    """Sample ProduceResult model for testing"""
    return ProduceResult(
        topic="test-topic",
        partition=0,
        offset=123,
        key="test-key",
        timestamp=1642234567000
    )


@pytest.fixture
def sample_consume_response():
    """Sample ConsumeResponse model for testing"""
    return ConsumeResponse(
        messages=[],
        consumer_group="test-group",
        topic="test-topic",
        total_consumed=0,
        has_more=False
    )


@pytest.fixture
def sample_catalog_response(sample_cluster_status, sample_topic_info):
    """Sample CatalogResponse model for testing"""
    return CatalogResponse(
        cluster=sample_cluster_status,
        topics=[sample_topic_info],
        available_apis=[
            {"path": "/catalog", "method": "GET", "description": "Get service catalog"},
            {"path": "/cluster/start", "method": "POST", "description": "Start cluster"},
            {"path": "/topics", "method": "GET", "description": "List topics"}
        ],
        services={
            "kafka": ServiceStatus.RUNNING,
            "kafka-rest-proxy": ServiceStatus.RUNNING,
            "kafka-ui": ServiceStatus.RUNNING
        }
    )


# Mock fixtures
@pytest.fixture
def mock_docker_client():
    """Mock Docker client for testing"""
    mock_client = Mock()
    mock_client.ping.return_value = True
    mock_client.containers = Mock()
    return mock_client


@pytest.fixture
def mock_docker_container():
    """Mock Docker container for testing"""
    container = Mock()
    container.status = "running"
    container.attrs = {
        "State": {
            "StartedAt": "2024-01-15T10:30:00.000000000Z",
            "Health": {
                "Status": "healthy",
                "Log": []
            }
        }
    }
    container.reload = Mock()
    return container


@pytest.fixture
def mock_kafka_admin():
    """Mock Kafka admin client for testing"""
    admin = Mock()
    admin.list_topics = AsyncMock()
    admin.create_topics = AsyncMock()
    admin.delete_topics = AsyncMock()
    return admin


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for testing"""
    producer = Mock()
    producer.send = Mock()
    producer.flush = Mock()
    return producer


@pytest.fixture
def mock_kafka_consumer():
    """Mock Kafka consumer for testing"""
    consumer = Mock()
    consumer.subscribe = Mock()
    consumer.poll = Mock()
    consumer.close = Mock()
    return consumer


# Performance testing fixtures
@pytest.fixture
def performance_test_config():
    """Configuration for performance tests"""
    return {
        "message_count": 1000,
        "batch_size": 100,
        "timeout_seconds": 30,
        "concurrent_producers": 5,
        "concurrent_consumers": 3
    }


@pytest.fixture
def load_test_scenarios():
    """Load test scenarios configuration"""
    return [
        {"name": "light_load", "messages_per_second": 10, "duration_seconds": 30},
        {"name": "medium_load", "messages_per_second": 100, "duration_seconds": 60},
        {"name": "heavy_load", "messages_per_second": 1000, "duration_seconds": 30}
    ]


# Integration test fixtures
@pytest.fixture
def integration_test_config():
    """Configuration for integration tests"""
    return {
        "compose_file": "docker-compose.yml",
        "test_topic": "integration-test-topic",
        "test_consumer_group": "integration-test-group",
        "startup_timeout": 120,
        "cleanup_timeout": 60
    }


# Test data generators
@pytest.fixture
def generate_test_topics():
    """Generate test topics with various configurations"""
    def _generate(count: int = 5) -> List[Dict[str, Any]]:
        return [
            {
                "name": f"test-topic-{i}",
                "partitions": (i % 5) + 1,
                "replication_factor": 1,
                "config": {"retention.ms": str((i + 1) * 86400000)}
            }
            for i in range(count)
        ]
    return _generate


@pytest.fixture
def generate_test_messages():
    """Generate test messages with various payloads"""
    def _generate(count: int = 100, topic: str = "test-topic") -> List[Dict[str, Any]]:
        return [
            {
                "topic": topic,
                "key": f"key-{i}",
                "value": {
                    "id": i,
                    "timestamp": 1642234567 + i,
                    "data": f"test-message-{i}",
                    "metadata": {"batch": i // 10, "sequence": i % 10}
                }
            }
            for i in range(count)
        ]
    return _generate


# Error simulation fixtures
@pytest.fixture
def error_scenarios():
    """Common error scenarios for testing"""
    return {
        "docker_not_available": Exception("Docker daemon not running"),
        "kafka_connection_failed": Exception("Failed to connect to Kafka broker"),
        "topic_creation_failed": Exception("Topic already exists"),
        "message_production_failed": Exception("Failed to produce message"),
        "consumer_group_error": Exception("Consumer group coordination failed"),
        "timeout_error": asyncio.TimeoutError("Operation timed out"),
        "validation_error": ValueError("Invalid configuration")
    }