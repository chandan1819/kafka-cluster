"""
Tests for custom exception classes and error handling.
"""

import pytest
from unittest.mock import Mock

from src.exceptions import (
    LocalKafkaManagerError,
    ErrorCode,
    ServiceUnavailableError,
    DockerNotAvailableError,
    KafkaNotAvailableError,
    KafkaRestProxyNotAvailableError,
    ClusterManagerError,
    ClusterStartError,
    ClusterStopError,
    DockerComposeError,
    ContainerHealthError,
    TopicManagerError,
    TopicNotFoundError,
    TopicAlreadyExistsError,
    TopicCreationError,
    TopicDeletionError,
    TopicConfigurationError,
    MessageManagerError,
    MessageProduceError,
    MessageConsumeError,
    ConsumerGroupError,
    MessageSerializationError,
    ServiceCatalogError,
    CatalogRefreshError,
    ServiceDiscoveryError,
    ValidationError,
    TimeoutError
)


@pytest.mark.unit
class TestLocalKafkaManagerError:
    """Test the base exception class."""
    
    def test_basic_exception_creation(self):
        """Test creating a basic exception."""
        error = LocalKafkaManagerError(
            message="Test error",
            error_code=ErrorCode.INTERNAL_SERVER_ERROR
        )
        
        assert str(error) == "Test error"
        assert error.message == "Test error"
        assert error.error_code == ErrorCode.INTERNAL_SERVER_ERROR
        assert error.details == {}
        assert error.cause is None
    
    def test_exception_with_details_and_cause(self):
        """Test creating an exception with details and cause."""
        cause = ValueError("Original error")
        details = {"key": "value", "number": 42}
        
        error = LocalKafkaManagerError(
            message="Test error with details",
            error_code=ErrorCode.VALIDATION_ERROR,
            details=details,
            cause=cause
        )
        
        assert error.message == "Test error with details"
        assert error.error_code == ErrorCode.VALIDATION_ERROR
        assert error.details == details
        assert error.cause == cause
    
    def test_to_dict_method(self):
        """Test converting exception to dictionary."""
        cause = ValueError("Original error")
        details = {"key": "value"}
        
        error = LocalKafkaManagerError(
            message="Test error",
            error_code=ErrorCode.TIMEOUT,
            details=details,
            cause=cause
        )
        
        result = error.to_dict()
        
        assert result["error"] == ErrorCode.TIMEOUT.value
        assert result["message"] == "Test error"
        assert result["details"]["key"] == "value"
        assert result["details"]["cause"] == "Original error"
        assert result["details"]["cause_type"] == "ValueError"
    
    def test_to_dict_without_cause(self):
        """Test converting exception to dictionary without cause."""
        error = LocalKafkaManagerError(
            message="Test error",
            error_code=ErrorCode.NOT_FOUND,
            details={"key": "value"}
        )
        
        result = error.to_dict()
        
        assert result["error"] == ErrorCode.NOT_FOUND.value
        assert result["message"] == "Test error"
        assert result["details"]["key"] == "value"
        assert "cause" not in result["details"]
        assert "cause_type" not in result["details"]


@pytest.mark.unit
class TestServiceAvailabilityExceptions:
    """Test service availability exception classes."""
    
    def test_docker_not_available_error(self):
        """Test DockerNotAvailableError."""
        error = DockerNotAvailableError("Docker daemon not running")
        
        assert "Docker is not available: Docker daemon not running" in error.message
        assert error.error_code == ErrorCode.DOCKER_NOT_AVAILABLE
        assert error.details["service"] == "Docker"
    
    def test_kafka_not_available_error(self):
        """Test KafkaNotAvailableError."""
        cause = ConnectionError("Connection refused")
        error = KafkaNotAvailableError("Broker unreachable", cause=cause)
        
        assert "Kafka is not available: Broker unreachable" in error.message
        assert error.error_code == ErrorCode.KAFKA_NOT_AVAILABLE
        assert error.details["service"] == "Kafka"
        assert error.cause == cause
    
    def test_kafka_rest_proxy_not_available_error(self):
        """Test KafkaRestProxyNotAvailableError."""
        error = KafkaRestProxyNotAvailableError("REST Proxy down")
        
        assert "Kafka REST Proxy is not available: REST Proxy down" in error.message
        assert error.error_code == ErrorCode.KAFKA_REST_PROXY_NOT_AVAILABLE
        assert error.details["service"] == "Kafka REST Proxy"


@pytest.mark.unit
class TestClusterManagementExceptions:
    """Test cluster management exception classes."""
    
    def test_cluster_start_error(self):
        """Test ClusterStartError."""
        details = {"timeout": 60, "force": True}
        cause = TimeoutError("startup", 60)
        
        error = ClusterStartError(
            message="Containers failed to start",
            details=details,
            cause=cause
        )
        
        assert "Failed to start cluster: Containers failed to start" in error.message
        assert error.error_code == ErrorCode.CLUSTER_START_FAILED
        assert error.details == details
        assert error.cause == cause
    
    def test_cluster_stop_error(self):
        """Test ClusterStopError."""
        error = ClusterStopError("Force stop failed")
        
        assert "Failed to stop cluster: Force stop failed" in error.message
        assert error.error_code == ErrorCode.CLUSTER_STOP_FAILED
    
    def test_docker_compose_error(self):
        """Test DockerComposeError."""
        error = DockerComposeError(
            command="up -d",
            exit_code=1,
            stderr="Service 'kafka' failed to start"
        )
        
        assert "Docker Compose command 'up -d' failed with exit code 1" in error.message
        assert error.error_code == ErrorCode.DOCKER_COMPOSE_ERROR
        assert error.details["command"] == "up -d"
        assert error.details["exit_code"] == 1
        assert error.details["stderr"] == "Service 'kafka' failed to start"
    
    def test_container_health_error(self):
        """Test ContainerHealthError."""
        error = ContainerHealthError(
            container_name="kafka",
            status="unhealthy",
            message="Health check failed"
        )
        
        assert "Container 'kafka' health check failed: Health check failed" in error.message
        assert error.error_code == ErrorCode.CONTAINER_HEALTH_ERROR
        assert error.details["container"] == "kafka"
        assert error.details["status"] == "unhealthy"


@pytest.mark.unit
class TestTopicManagementExceptions:
    """Test topic management exception classes."""
    
    def test_topic_not_found_error(self):
        """Test TopicNotFoundError."""
        error = TopicNotFoundError("my-topic")
        
        assert "Topic 'my-topic' not found" in error.message
        assert error.error_code == ErrorCode.TOPIC_NOT_FOUND
        assert error.details["topic"] == "my-topic"
    
    def test_topic_already_exists_error(self):
        """Test TopicAlreadyExistsError."""
        error = TopicAlreadyExistsError("existing-topic")
        
        assert "Topic 'existing-topic' already exists" in error.message
        assert error.error_code == ErrorCode.TOPIC_ALREADY_EXISTS
        assert error.details["topic"] == "existing-topic"
    
    def test_topic_creation_error(self):
        """Test TopicCreationError."""
        error = TopicCreationError(
            topic_name="test-topic",
            reason="Invalid partition count",
            cause=ValueError("Partitions must be > 0")
        )
        
        assert "Failed to create topic 'test-topic': Invalid partition count" in error.message
        assert error.error_code == ErrorCode.TOPIC_CREATION_FAILED
        assert error.details["topic"] == "test-topic"
        assert error.details["reason"] == "Invalid partition count"
    
    def test_topic_deletion_error(self):
        """Test TopicDeletionError."""
        error = TopicDeletionError(
            topic_name="test-topic",
            reason="Topic has active consumers"
        )
        
        assert "Failed to delete topic 'test-topic': Topic has active consumers" in error.message
        assert error.error_code == ErrorCode.TOPIC_DELETION_FAILED
        assert error.details["topic"] == "test-topic"
        assert error.details["reason"] == "Topic has active consumers"
    
    def test_topic_configuration_error(self):
        """Test TopicConfigurationError."""
        config_errors = {
            "retention.ms": "Invalid value",
            "cleanup.policy": "Unknown policy"
        }
        
        error = TopicConfigurationError(
            topic_name="test-topic",
            config_errors=config_errors
        )
        
        assert "Invalid configuration for topic 'test-topic'" in error.message
        assert error.error_code == ErrorCode.TOPIC_CONFIG_INVALID
        assert error.details["topic"] == "test-topic"
        assert error.details["config_errors"] == config_errors


@pytest.mark.unit
class TestMessageOperationExceptions:
    """Test message operation exception classes."""
    
    def test_message_produce_error(self):
        """Test MessageProduceError."""
        error = MessageProduceError(
            topic="test-topic",
            reason="Serialization failed",
            cause=ValueError("Invalid JSON")
        )
        
        assert "Failed to produce message to topic 'test-topic': Serialization failed" in error.message
        assert error.error_code == ErrorCode.MESSAGE_PRODUCE_FAILED
        assert error.details["topic"] == "test-topic"
        assert error.details["reason"] == "Serialization failed"
    
    def test_message_consume_error(self):
        """Test MessageConsumeError."""
        error = MessageConsumeError(
            topic="test-topic",
            consumer_group="test-group",
            reason="Consumer timeout"
        )
        
        assert "Failed to consume messages from topic 'test-topic' with group 'test-group': Consumer timeout" in error.message
        assert error.error_code == ErrorCode.MESSAGE_CONSUME_FAILED
        assert error.details["topic"] == "test-topic"
        assert error.details["consumer_group"] == "test-group"
        assert error.details["reason"] == "Consumer timeout"
    
    def test_consumer_group_error(self):
        """Test ConsumerGroupError."""
        error = ConsumerGroupError(
            consumer_group="test-group",
            operation="create",
            reason="Group already exists"
        )
        
        assert "Consumer group 'test-group' create failed: Group already exists" in error.message
        assert error.error_code == ErrorCode.CONSUMER_GROUP_ERROR
        assert error.details["consumer_group"] == "test-group"
        assert error.details["operation"] == "create"
        assert error.details["reason"] == "Group already exists"
    
    def test_message_serialization_error(self):
        """Test MessageSerializationError."""
        error = MessageSerializationError(
            operation="deserialization",
            reason="Invalid JSON format"
        )
        
        assert "Message deserialization failed: Invalid JSON format" in error.message
        assert error.error_code == ErrorCode.MESSAGE_SERIALIZATION_ERROR
        assert error.details["operation"] == "deserialization"
        assert error.details["reason"] == "Invalid JSON format"


@pytest.mark.unit
class TestServiceCatalogExceptions:
    """Test service catalog exception classes."""
    
    def test_catalog_refresh_error(self):
        """Test CatalogRefreshError."""
        cause = ConnectionError("Network error")
        error = CatalogRefreshError(
            reason="Network connectivity issues",
            cause=cause
        )
        
        assert "Failed to refresh service catalog: Network connectivity issues" in error.message
        assert error.error_code == ErrorCode.CATALOG_REFRESH_FAILED
        assert error.details["reason"] == "Network connectivity issues"
        assert error.cause == cause
    
    def test_service_discovery_error(self):
        """Test ServiceDiscoveryError."""
        error = ServiceDiscoveryError(
            service_name="kafka-ui",
            reason="Service not responding"
        )
        
        assert "Failed to discover service 'kafka-ui': Service not responding" in error.message
        assert error.error_code == ErrorCode.SERVICE_DISCOVERY_ERROR
        assert error.details["service"] == "kafka-ui"
        assert error.details["reason"] == "Service not responding"


@pytest.mark.unit
class TestValidationAndTimeoutExceptions:
    """Test validation and timeout exception classes."""
    
    def test_validation_error(self):
        """Test ValidationError."""
        error = ValidationError(
            field="partitions",
            value=-1,
            reason="Must be positive integer"
        )
        
        assert "Validation failed for field 'partitions': Must be positive integer" in error.message
        assert error.error_code == ErrorCode.VALIDATION_ERROR
        assert error.details["field"] == "partitions"
        assert error.details["value"] == "-1"
        assert error.details["reason"] == "Must be positive integer"
    
    def test_timeout_error(self):
        """Test TimeoutError."""
        error = TimeoutError(
            operation="cluster_startup",
            timeout_seconds=60
        )
        
        assert "Operation 'cluster_startup' timed out after 60 seconds" in error.message
        assert error.error_code == ErrorCode.TIMEOUT
        assert error.details["operation"] == "cluster_startup"
        assert error.details["timeout_seconds"] == 60


@pytest.mark.unit
class TestErrorCodeEnum:
    """Test the ErrorCode enumeration."""
    
    def test_all_error_codes_are_strings(self):
        """Test that all error codes are string values."""
        for error_code in ErrorCode:
            assert isinstance(error_code.value, str)
            assert error_code.value.isupper()
            assert "_" in error_code.value or error_code.value in ["TIMEOUT", "CONFLICT"]
    
    def test_error_code_uniqueness(self):
        """Test that all error codes are unique."""
        values = [error_code.value for error_code in ErrorCode]
        assert len(values) == len(set(values))
    
    def test_specific_error_codes_exist(self):
        """Test that specific error codes exist."""
        required_codes = [
            "INTERNAL_SERVER_ERROR",
            "VALIDATION_ERROR",
            "NOT_FOUND",
            "DOCKER_NOT_AVAILABLE",
            "KAFKA_NOT_AVAILABLE",
            "TOPIC_NOT_FOUND",
            "MESSAGE_PRODUCE_FAILED",
            "TIMEOUT"
        ]
        
        existing_codes = [code.value for code in ErrorCode]
        for required_code in required_codes:
            assert required_code in existing_codes