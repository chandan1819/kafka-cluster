"""
Additional test fixtures and utilities for comprehensive testing.
"""

import pytest
import json
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Any, Generator
from unittest.mock import Mock, AsyncMock
from datetime import datetime, timedelta

from src.models.base import ServiceStatus
from src.models.cluster import ClusterStatus, ServiceHealth
from src.models.topic import TopicInfo, TopicConfig
from src.models.message import Message, ProduceResult, ConsumeResponse
from src.models.catalog import CatalogResponse


class DataManager:
    """Manages test data and provides utilities for test scenarios."""
    
    def __init__(self):
        self.temp_dir = None
        self.test_files = []
    
    def create_temp_directory(self) -> Path:
        """Create a temporary directory for test files."""
        if not self.temp_dir:
            self.temp_dir = Path(tempfile.mkdtemp(prefix="kafka_manager_test_"))
        return self.temp_dir
    
    def create_test_compose_file(self, services: List[str] = None) -> Path:
        """Create a test docker-compose.yml file."""
        services = services or ["kafka", "kafka-rest-proxy", "kafka-ui"]
        
        compose_content = {
            "version": "3.8",
            "services": {}
        }
        
        for service in services:
            compose_content["services"][service] = {
                "image": f"test/{service}:latest",
                "ports": [f"{8000 + len(compose_content['services'])}:8080"]
            }
        
        temp_dir = self.create_temp_directory()
        compose_file = temp_dir / "docker-compose.test.yml"
        
        with open(compose_file, 'w') as f:
            import yaml
            yaml.dump(compose_content, f)
        
        self.test_files.append(compose_file)
        return compose_file
    
    def create_test_config_file(self, config: Dict[str, Any]) -> Path:
        """Create a test configuration file."""
        temp_dir = self.create_temp_directory()
        config_file = temp_dir / "test_config.json"
        
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        self.test_files.append(config_file)
        return config_file
    
    def cleanup(self):
        """Clean up test files and directories."""
        if self.temp_dir and self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        self.temp_dir = None
        self.test_files = []


@pytest.fixture
def test_data_manager():
    """Provide a test data manager with automatic cleanup."""
    manager = DataManager()
    yield manager
    manager.cleanup()


@pytest.fixture
def mock_docker_environment():
    """Provide a comprehensive mock Docker environment."""
    class MockDockerEnvironment:
        def __init__(self):
            self.containers = {}
            self.networks = {}
            self.volumes = {}
            self.images = {}
        
        def create_container(self, name: str, status: str = "running", health: str = "healthy"):
            """Create a mock container."""
            container = Mock()
            container.name = name
            container.status = status
            container.attrs = {
                "State": {
                    "StartedAt": datetime.now().isoformat() + "Z",
                    "Health": {
                        "Status": health,
                        "Log": []
                    }
                }
            }
            container.reload = Mock()
            self.containers[name] = container
            return container
        
        def get_container(self, name: str):
            """Get a mock container by name."""
            return self.containers.get(name)
        
        def list_containers(self, all: bool = False):
            """List all mock containers."""
            if all:
                return list(self.containers.values())
            return [c for c in self.containers.values() if c.status == "running"]
    
    return MockDockerEnvironment()


@pytest.fixture
def mock_kafka_environment():
    """Provide a comprehensive mock Kafka environment."""
    class MockKafkaEnvironment:
        def __init__(self):
            self.topics = {}
            self.consumer_groups = {}
            self.messages = {}
            self.brokers = ["localhost:9092"]
        
        def create_topic(self, name: str, partitions: int = 1, replication_factor: int = 1):
            """Create a mock topic."""
            self.topics[name] = TopicInfo(
                name=name,
                partitions=partitions,
                replication_factor=replication_factor,
                size_bytes=0
            )
            self.messages[name] = []
        
        def delete_topic(self, name: str):
            """Delete a mock topic."""
            if name in self.topics:
                del self.topics[name]
            if name in self.messages:
                del self.messages[name]
        
        def produce_message(self, topic: str, key: str, value: Dict[str, Any]) -> ProduceResult:
            """Produce a mock message."""
            if topic not in self.messages:
                self.messages[topic] = []
            
            offset = len(self.messages[topic])
            message = Message(
                topic=topic,
                partition=0,
                offset=offset,
                key=key,
                value=value,
                timestamp=int(datetime.now().timestamp() * 1000)
            )
            
            self.messages[topic].append(message)
            
            return ProduceResult(
                topic=topic,
                partition=0,
                offset=offset,
                key=key,
                timestamp=message.timestamp
            )
        
        def consume_messages(self, topic: str, consumer_group: str, max_messages: int = 10) -> ConsumeResponse:
            """Consume mock messages."""
            messages = self.messages.get(topic, [])
            consumed_messages = messages[:max_messages]
            
            return ConsumeResponse(
                messages=consumed_messages,
                consumer_group=consumer_group,
                topic=topic,
                total_consumed=len(consumed_messages),
                has_more=len(messages) > max_messages
            )
        
        def list_topics(self) -> List[TopicInfo]:
            """List all mock topics."""
            return list(self.topics.values())
    
    return MockKafkaEnvironment()


@pytest.fixture
def sample_cluster_scenarios():
    """Provide various cluster status scenarios for testing."""
    return {
        "healthy": ClusterStatus(
            status=ServiceStatus.RUNNING,
            broker_count=3,
            version="7.4.0",
            endpoints={
                "kafka": "localhost:9092",
                "kafka-rest-proxy": "localhost:8082",
                "kafka-ui": "localhost:8080"
            },
            uptime=7200,
            services={
                "kafka": ServiceHealth(status=ServiceStatus.RUNNING),
                "kafka-rest-proxy": ServiceHealth(status=ServiceStatus.RUNNING),
                "kafka-ui": ServiceHealth(status=ServiceStatus.RUNNING)
            }
        ),
        "starting": ClusterStatus(
            status=ServiceStatus.STARTING,
            broker_count=0,
            version="7.4.0",
            endpoints={},
            uptime=None,
            services={
                "kafka": ServiceHealth(status=ServiceStatus.STARTING),
                "kafka-rest-proxy": ServiceHealth(status=ServiceStatus.STOPPED),
                "kafka-ui": ServiceHealth(status=ServiceStatus.STOPPED)
            }
        ),
        "partial_failure": ClusterStatus(
            status=ServiceStatus.ERROR,
            broker_count=1,
            version="7.4.0",
            endpoints={"kafka": "localhost:9092"},
            uptime=3600,
            services={
                "kafka": ServiceHealth(status=ServiceStatus.RUNNING),
                "kafka-rest-proxy": ServiceHealth(
                    status=ServiceStatus.ERROR,
                    error_message="Connection refused"
                ),
                "kafka-ui": ServiceHealth(status=ServiceStatus.STOPPED)
            }
        ),
        "stopped": ClusterStatus(
            status=ServiceStatus.STOPPED,
            broker_count=0,
            version="",
            endpoints={},
            uptime=None,
            services={}
        )
    }


@pytest.fixture
def sample_topic_scenarios():
    """Provide various topic scenarios for testing."""
    return {
        "simple": TopicInfo(
            name="simple-topic",
            partitions=1,
            replication_factor=1,
            size_bytes=1024
        ),
        "multi_partition": TopicInfo(
            name="multi-partition-topic",
            partitions=10,
            replication_factor=3,
            size_bytes=1048576
        ),
        "large": TopicInfo(
            name="large-topic",
            partitions=50,
            replication_factor=3,
            size_bytes=1073741824
        ),
        "empty": TopicInfo(
            name="empty-topic",
            partitions=1,
            replication_factor=1,
            size_bytes=0
        )
    }


@pytest.fixture
def sample_message_scenarios():
    """Provide various message scenarios for testing."""
    base_timestamp = int(datetime.now().timestamp() * 1000)
    
    return {
        "simple": Message(
            topic="test-topic",
            partition=0,
            offset=0,
            key="simple-key",
            value={"message": "Hello, World!"},
            timestamp=base_timestamp
        ),
        "complex": Message(
            topic="test-topic",
            partition=2,
            offset=12345,
            key="complex-key",
            value={
                "user_id": "user123",
                "event_type": "user_action",
                "timestamp": base_timestamp,
                "data": {
                    "action": "click",
                    "element": "button",
                    "page": "/dashboard"
                },
                "metadata": {
                    "session_id": "session456",
                    "ip_address": "192.168.1.1",
                    "user_agent": "Mozilla/5.0..."
                }
            },
            timestamp=base_timestamp
        ),
        "large_payload": Message(
            topic="test-topic",
            partition=1,
            offset=999,
            key="large-key",
            value={
                "data": "x" * 10000,  # Large payload
                "metadata": {"size": 10000}
            },
            timestamp=base_timestamp
        ),
        "null_key": Message(
            topic="test-topic",
            partition=0,
            offset=5,
            key=None,
            value={"message": "Message with null key"},
            timestamp=base_timestamp
        )
    }


@pytest.fixture
def error_simulation_scenarios():
    """Provide error scenarios for testing error handling."""
    return {
        "docker_not_available": {
            "exception": Exception("Docker daemon not running"),
            "expected_status": 500,
            "expected_error": "DOCKER_ERROR"
        },
        "kafka_connection_failed": {
            "exception": Exception("Failed to connect to Kafka broker at localhost:9092"),
            "expected_status": 503,
            "expected_error": "KAFKA_CONNECTION_ERROR"
        },
        "topic_already_exists": {
            "exception": Exception("Topic 'test-topic' already exists"),
            "expected_status": 409,
            "expected_error": "TOPIC_EXISTS"
        },
        "topic_not_found": {
            "exception": Exception("Topic 'nonexistent-topic' not found"),
            "expected_status": 404,
            "expected_error": "TOPIC_NOT_FOUND"
        },
        "insufficient_replicas": {
            "exception": Exception("Insufficient replicas available"),
            "expected_status": 400,
            "expected_error": "INSUFFICIENT_REPLICAS"
        },
        "timeout": {
            "exception": TimeoutError("Operation timed out after 30 seconds"),
            "expected_status": 504,
            "expected_error": "TIMEOUT"
        },
        "validation_error": {
            "exception": ValueError("Invalid partition count: -1"),
            "expected_status": 422,
            "expected_error": "VALIDATION_ERROR"
        }
    }


@pytest.fixture
def performance_test_data():
    """Provide data for performance testing."""
    def generate_messages(count: int, topic: str = "perf-test-topic") -> List[Dict[str, Any]]:
        """Generate test messages for performance testing."""
        return [
            {
                "topic": topic,
                "key": f"perf-key-{i:06d}",
                "value": {
                    "id": i,
                    "timestamp": datetime.now().isoformat(),
                    "data": f"performance-test-message-{i}",
                    "payload": "x" * 100,  # 100 byte payload
                    "metadata": {
                        "batch": i // 100,
                        "sequence": i % 100,
                        "test_run": "performance_test"
                    }
                }
            }
            for i in range(count)
        ]
    
    def generate_topics(count: int) -> List[Dict[str, Any]]:
        """Generate test topics for performance testing."""
        return [
            {
                "name": f"perf-topic-{i:04d}",
                "partitions": (i % 10) + 1,
                "replication_factor": min((i % 3) + 1, 3),
                "config": {
                    "retention.ms": str((i + 1) * 86400000),
                    "segment.ms": "604800000"
                }
            }
            for i in range(count)
        ]
    
    return {
        "generate_messages": generate_messages,
        "generate_topics": generate_topics,
        "small_batch": 100,
        "medium_batch": 1000,
        "large_batch": 10000,
        "concurrent_producers": 5,
        "concurrent_consumers": 3
    }


@pytest.fixture
def integration_test_environment():
    """Provide environment setup for integration tests."""
    class IntegrationTestEnvironment:
        def __init__(self):
            self.test_topics = []
            self.test_consumer_groups = []
            self.cleanup_tasks = []
        
        def register_topic(self, topic_name: str):
            """Register a topic for cleanup."""
            self.test_topics.append(topic_name)
        
        def register_consumer_group(self, group_name: str):
            """Register a consumer group for cleanup."""
            self.test_consumer_groups.append(group_name)
        
        def add_cleanup_task(self, task):
            """Add a cleanup task."""
            self.cleanup_tasks.append(task)
        
        async def cleanup(self):
            """Clean up test resources."""
            for task in self.cleanup_tasks:
                try:
                    if callable(task):
                        await task()
                except Exception:
                    pass  # Ignore cleanup errors
    
    env = IntegrationTestEnvironment()
    yield env
    # Cleanup is handled by the test itself


@pytest.fixture
def mock_time_progression():
    """Provide utilities for mocking time progression in tests."""
    class MockTimeProgression:
        def __init__(self):
            self.current_time = datetime.now()
            self.time_patches = []
        
        def advance_time(self, seconds: int):
            """Advance mock time by specified seconds."""
            self.current_time += timedelta(seconds=seconds)
        
        def get_current_timestamp(self) -> int:
            """Get current timestamp in milliseconds."""
            return int(self.current_time.timestamp() * 1000)
        
        def get_current_iso(self) -> str:
            """Get current time in ISO format."""
            return self.current_time.isoformat() + "Z"
    
    return MockTimeProgression()


@pytest.fixture
def test_metrics_collector():
    """Provide a metrics collector for test analysis."""
    class TestMetricsCollector:
        def __init__(self):
            self.metrics = {
                "response_times": [],
                "error_counts": {},
                "throughput_measurements": [],
                "resource_usage": []
            }
        
        def record_response_time(self, operation: str, time_ms: float):
            """Record response time for an operation."""
            self.metrics["response_times"].append({
                "operation": operation,
                "time_ms": time_ms,
                "timestamp": datetime.now().isoformat()
            })
        
        def record_error(self, error_type: str):
            """Record an error occurrence."""
            if error_type not in self.metrics["error_counts"]:
                self.metrics["error_counts"][error_type] = 0
            self.metrics["error_counts"][error_type] += 1
        
        def record_throughput(self, operation: str, ops_per_second: float):
            """Record throughput measurement."""
            self.metrics["throughput_measurements"].append({
                "operation": operation,
                "ops_per_second": ops_per_second,
                "timestamp": datetime.now().isoformat()
            })
        
        def get_summary(self) -> Dict[str, Any]:
            """Get metrics summary."""
            response_times = [m["time_ms"] for m in self.metrics["response_times"]]
            
            summary = {
                "total_operations": len(response_times),
                "total_errors": sum(self.metrics["error_counts"].values()),
                "error_rate": 0,
                "avg_response_time": 0,
                "max_response_time": 0,
                "throughput_measurements": len(self.metrics["throughput_measurements"])
            }
            
            if response_times:
                summary["avg_response_time"] = sum(response_times) / len(response_times)
                summary["max_response_time"] = max(response_times)
            
            total_ops = summary["total_operations"] + summary["total_errors"]
            if total_ops > 0:
                summary["error_rate"] = (summary["total_errors"] / total_ops) * 100
            
            return summary
    
    return TestMetricsCollector()