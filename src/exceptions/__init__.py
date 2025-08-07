"""
Multi-cluster exceptions module.
"""

# Import all multi-cluster specific exceptions
from .multi_cluster_exceptions import *

# Import original exceptions for backward compatibility
# These are needed by existing services like cluster_manager.py
import sys
import os

# Add the parent directory to path to import from the original exceptions.py
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from exceptions import (
        LocalKafkaManagerError,
        ClusterManagerError,
        DockerNotAvailableError,
        ClusterStartError,
        ClusterStopError,
        DockerComposeError,
        ContainerHealthError,
        TimeoutError,
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
        ServiceUnavailableError,
        KafkaNotAvailableError,
        KafkaRestProxyNotAvailableError,
        ClusterNotFoundError,
        ClusterAlreadyExistsError,
        PortAllocationError as OriginalPortAllocationError,
        CrossClusterOperationError as OriginalCrossClusterOperationError,
        TemplateNotFoundError,
        TemplateAlreadyExistsError,
        ClusterOperationError,
        ClusterValidationError,
        ClusterConflictError as OriginalClusterConflictError,
        NetworkIsolationError as OriginalNetworkIsolationError,
        StorageBackendError,
        ConfigurationError as OriginalConfigurationError,
        SecurityError as OriginalSecurityError,
        ErrorCode
    )
except ImportError:
    # If we can't import from the original exceptions.py, define the essential ones here
    from enum import Enum
    
    class ErrorCode(str, Enum):
        INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR"
        CLUSTER_START_FAILED = "CLUSTER_START_FAILED"
        CLUSTER_STOP_FAILED = "CLUSTER_STOP_FAILED"
        DOCKER_NOT_AVAILABLE = "DOCKER_NOT_AVAILABLE"
        DOCKER_COMPOSE_ERROR = "DOCKER_COMPOSE_ERROR"
        CONTAINER_HEALTH_ERROR = "CONTAINER_HEALTH_ERROR"
        TIMEOUT = "TIMEOUT"
    
    class LocalKafkaManagerError(Exception):
        def __init__(self, message: str, error_code=None, details=None, cause=None):
            super().__init__(message)
            self.message = message
            self.error_code = error_code
            self.details = details or {}
            self.cause = cause
    
    class ClusterManagerError(LocalKafkaManagerError):
        pass
    
    class DockerNotAvailableError(LocalKafkaManagerError):
        def __init__(self, message="Docker is not running or not accessible", cause=None):
            super().__init__(message, ErrorCode.DOCKER_NOT_AVAILABLE, cause=cause)
    
    class ClusterStartError(ClusterManagerError):
        def __init__(self, message, details=None, cause=None):
            super().__init__(f"Failed to start cluster: {message}", ErrorCode.CLUSTER_START_FAILED, details, cause)
    
    class ClusterStopError(ClusterManagerError):
        def __init__(self, message, details=None, cause=None):
            super().__init__(f"Failed to stop cluster: {message}", ErrorCode.CLUSTER_STOP_FAILED, details, cause)
    
    class DockerComposeError(ClusterManagerError):
        def __init__(self, command, exit_code, stderr, cause=None):
            super().__init__(
                f"Docker Compose command '{command}' failed with exit code {exit_code}",
                ErrorCode.DOCKER_COMPOSE_ERROR,
                {"command": command, "exit_code": exit_code, "stderr": stderr},
                cause
            )
    
    class ContainerHealthError(ClusterManagerError):
        def __init__(self, container_name, status, message, cause=None):
            super().__init__(
                f"Container '{container_name}' health check failed: {message}",
                ErrorCode.CONTAINER_HEALTH_ERROR,
                {"container": container_name, "status": status},
                cause
            )
    
    class TimeoutError(LocalKafkaManagerError):
        def __init__(self, operation, timeout_seconds, cause=None):
            super().__init__(
                f"Operation '{operation}' timed out after {timeout_seconds} seconds",
                ErrorCode.TIMEOUT,
                {"operation": operation, "timeout_seconds": timeout_seconds},
                cause
            )
    
    # Topic management exceptions
    class TopicManagerError(LocalKafkaManagerError):
        pass
    
    class KafkaNotAvailableError(LocalKafkaManagerError):
        def __init__(self, message="Kafka broker is not available", cause=None):
            super().__init__(message, cause=cause)
    
    class TopicNotFoundError(TopicManagerError):
        def __init__(self, topic_name, cause=None):
            super().__init__(f"Topic '{topic_name}' not found", cause=cause)
    
    class TopicAlreadyExistsError(TopicManagerError):
        def __init__(self, topic_name, cause=None):
            super().__init__(f"Topic '{topic_name}' already exists", cause=cause)
    
    class TopicCreationError(TopicManagerError):
        def __init__(self, topic_name, reason, cause=None):
            super().__init__(f"Failed to create topic '{topic_name}': {reason}", cause=cause)
    
    class TopicDeletionError(TopicManagerError):
        def __init__(self, topic_name, reason, cause=None):
            super().__init__(f"Failed to delete topic '{topic_name}': {reason}", cause=cause)
    
    class TopicConfigurationError(TopicManagerError):
        def __init__(self, topic_name, config_errors, cause=None):
            super().__init__(f"Invalid configuration for topic '{topic_name}'", cause=cause)
    
    # Message management exceptions
    class MessageManagerError(LocalKafkaManagerError):
        pass
    
    class MessageProduceError(MessageManagerError):
        def __init__(self, topic, reason, cause=None):
            super().__init__(f"Failed to produce message to topic '{topic}': {reason}", cause=cause)
    
    class MessageConsumeError(MessageManagerError):
        def __init__(self, topic, consumer_group, reason, cause=None):
            super().__init__(f"Failed to consume messages from topic '{topic}' with group '{consumer_group}': {reason}", cause=cause)
    
    class ConsumerGroupError(MessageManagerError):
        def __init__(self, consumer_group, operation, reason, cause=None):
            super().__init__(f"Consumer group '{consumer_group}' {operation} failed: {reason}", cause=cause)
    
    class MessageSerializationError(MessageManagerError):
        def __init__(self, operation, reason, cause=None):
            super().__init__(f"Message {operation} failed: {reason}", cause=cause)
    
    # Service catalog exceptions
    class ServiceCatalogError(LocalKafkaManagerError):
        pass
    
    class CatalogRefreshError(ServiceCatalogError):
        def __init__(self, reason, cause=None):
            super().__init__(f"Failed to refresh service catalog: {reason}", cause=cause)
    
    class ServiceDiscoveryError(ServiceCatalogError):
        def __init__(self, service_name, reason, cause=None):
            super().__init__(f"Failed to discover service '{service_name}': {reason}", cause=cause)
    
    class ValidationError(LocalKafkaManagerError):
        def __init__(self, field, value, reason, cause=None):
            super().__init__(f"Validation failed for field '{field}': {reason}", cause=cause)
    
    class ServiceUnavailableError(LocalKafkaManagerError):
        def __init__(self, service_name, message, cause=None):
            super().__init__(f"{service_name} is not available: {message}", cause=cause)
    
    class KafkaRestProxyNotAvailableError(ServiceUnavailableError):
        def __init__(self, message="Kafka REST Proxy is not available", cause=None):
            super().__init__("Kafka REST Proxy", message, cause)
    
    # Multi-cluster exceptions for backward compatibility
    class ClusterNotFoundError(LocalKafkaManagerError):
        def __init__(self, cluster_id, details=None):
            super().__init__(f"Cluster '{cluster_id}' not found", details=details)
            self.cluster_id = cluster_id
    
    class ClusterAlreadyExistsError(LocalKafkaManagerError):
        def __init__(self, cluster_id, details=None):
            super().__init__(f"Cluster '{cluster_id}' already exists", details=details)
            self.cluster_id = cluster_id
    
    class OriginalPortAllocationError(LocalKafkaManagerError):
        def __init__(self, message, requested_ports=None, conflicting_ports=None, details=None):
            super().__init__(message, details=details)
            self.requested_ports = requested_ports or []
            self.conflicting_ports = conflicting_ports or []
    
    class OriginalCrossClusterOperationError(LocalKafkaManagerError):
        def __init__(self, message, operation_id=None, operation_type=None, details=None, cause=None):
            super().__init__(message, details=details, cause=cause)
            self.operation_id = operation_id
            self.operation_type = operation_type
    
    class TemplateNotFoundError(LocalKafkaManagerError):
        def __init__(self, template_id, details=None):
            super().__init__(f"Template '{template_id}' not found", details=details)
            self.template_id = template_id
    
    class TemplateAlreadyExistsError(LocalKafkaManagerError):
        def __init__(self, template_id, details=None):
            super().__init__(f"Template '{template_id}' already exists", details=details)
            self.template_id = template_id
    
    class ClusterOperationError(LocalKafkaManagerError):
        def __init__(self, cluster_id, operation, message, details=None, cause=None):
            super().__init__(f"Cluster '{cluster_id}' {operation} operation failed: {message}", details=details, cause=cause)
            self.cluster_id = cluster_id
            self.operation = operation
    
    class ClusterValidationError(LocalKafkaManagerError):
        def __init__(self, cluster_id, validation_errors, details=None):
            super().__init__(f"Cluster '{cluster_id}' validation failed: {'; '.join(validation_errors)}", details=details)
            self.cluster_id = cluster_id
            self.validation_errors = validation_errors
    
    class OriginalClusterConflictError(LocalKafkaManagerError):
        def __init__(self, cluster_id, conflicts, details=None):
            super().__init__(f"Cluster '{cluster_id}' conflicts with existing clusters: {'; '.join(conflicts)}", details=details)
            self.cluster_id = cluster_id
            self.conflicts = conflicts
    
    class OriginalNetworkIsolationError(LocalKafkaManagerError):
        def __init__(self, message, cluster_id=None, network_name=None, details=None, cause=None):
            super().__init__(message, details=details, cause=cause)
            self.cluster_id = cluster_id
            self.network_name = network_name
    
    class StorageBackendError(LocalKafkaManagerError):
        def __init__(self, message, backend_type=None, operation=None, details=None, cause=None):
            super().__init__(message, details=details, cause=cause)
            self.backend_type = backend_type
            self.operation = operation
    
    class OriginalConfigurationError(LocalKafkaManagerError):
        def __init__(self, message, details=None):
            super().__init__(message, details=details)
    
    class OriginalSecurityError(LocalKafkaManagerError):
        def __init__(self, message, details=None):
            super().__init__(message, details=details)
    
    class InstallationError(LocalKafkaManagerError):
        def __init__(self, message, details=None, cause=None):
            super().__init__(message, details=details, cause=cause)