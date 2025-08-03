"""
Custom exception classes for the Local Kafka Manager.

This module defines a hierarchy of custom exceptions that provide structured
error handling throughout the application.
"""

import logging
from typing import Optional, Dict, Any
from enum import Enum


logger = logging.getLogger(__name__)


class ErrorCode(str, Enum):
    """Standard error codes for the application."""
    
    # General errors
    INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    TIMEOUT = "TIMEOUT"
    
    # Service availability errors
    DOCKER_NOT_AVAILABLE = "DOCKER_NOT_AVAILABLE"
    KAFKA_NOT_AVAILABLE = "KAFKA_NOT_AVAILABLE"
    KAFKA_REST_PROXY_NOT_AVAILABLE = "KAFKA_REST_PROXY_NOT_AVAILABLE"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    
    # Cluster management errors
    CLUSTER_START_FAILED = "CLUSTER_START_FAILED"
    CLUSTER_STOP_FAILED = "CLUSTER_STOP_FAILED"
    CLUSTER_STATUS_ERROR = "CLUSTER_STATUS_ERROR"
    DOCKER_COMPOSE_ERROR = "DOCKER_COMPOSE_ERROR"
    CONTAINER_HEALTH_ERROR = "CONTAINER_HEALTH_ERROR"
    
    # Topic management errors
    TOPIC_CREATION_FAILED = "TOPIC_CREATION_FAILED"
    TOPIC_DELETION_FAILED = "TOPIC_DELETION_FAILED"
    TOPIC_NOT_FOUND = "TOPIC_NOT_FOUND"
    TOPIC_ALREADY_EXISTS = "TOPIC_ALREADY_EXISTS"
    TOPIC_CONFIG_INVALID = "TOPIC_CONFIG_INVALID"
    
    # Message operation errors
    MESSAGE_PRODUCE_FAILED = "MESSAGE_PRODUCE_FAILED"
    MESSAGE_CONSUME_FAILED = "MESSAGE_CONSUME_FAILED"
    CONSUMER_GROUP_ERROR = "CONSUMER_GROUP_ERROR"
    MESSAGE_SERIALIZATION_ERROR = "MESSAGE_SERIALIZATION_ERROR"
    
    # Service catalog errors
    CATALOG_REFRESH_FAILED = "CATALOG_REFRESH_FAILED"
    SERVICE_DISCOVERY_ERROR = "SERVICE_DISCOVERY_ERROR"


class LocalKafkaManagerError(Exception):
    """Base exception class for all Local Kafka Manager errors.
    
    This is the root exception class that all other custom exceptions inherit from.
    It provides structured error information including error codes, messages, and
    additional context details.
    """
    
    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.INTERNAL_SERVER_ERROR,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None
    ):
        """Initialize the exception.
        
        Args:
            message: Human-readable error message
            error_code: Standardized error code
            details: Additional error context and details
            cause: The underlying exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.cause = cause
        
        # Log the error for debugging
        logger.debug(f"Exception created: {error_code.value} - {message}", exc_info=cause)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary format for API responses."""
        result = {
            "error": self.error_code.value,
            "message": self.message,
            "details": self.details
        }
        
        if self.cause:
            result["details"]["cause"] = str(self.cause)
            result["details"]["cause_type"] = type(self.cause).__name__
        
        return result


# Service Availability Exceptions

class ServiceUnavailableError(LocalKafkaManagerError):
    """Raised when a required service is not available."""
    
    def __init__(self, service_name: str, message: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"{service_name} is not available: {message}",
            error_code=ErrorCode.SERVICE_UNAVAILABLE,
            details={"service": service_name},
            cause=cause
        )


class DockerNotAvailableError(ServiceUnavailableError):
    """Raised when Docker is not available or accessible."""
    
    def __init__(self, message: str = "Docker is not running or not accessible", cause: Optional[Exception] = None):
        super().__init__(
            service_name="Docker",
            message=message,
            cause=cause
        )
        self.error_code = ErrorCode.DOCKER_NOT_AVAILABLE


class KafkaNotAvailableError(ServiceUnavailableError):
    """Raised when Kafka is not available or accessible."""
    
    def __init__(self, message: str = "Kafka broker is not available", cause: Optional[Exception] = None):
        super().__init__(
            service_name="Kafka",
            message=message,
            cause=cause
        )
        self.error_code = ErrorCode.KAFKA_NOT_AVAILABLE


class KafkaRestProxyNotAvailableError(ServiceUnavailableError):
    """Raised when Kafka REST Proxy is not available or accessible."""
    
    def __init__(self, message: str = "Kafka REST Proxy is not available", cause: Optional[Exception] = None):
        super().__init__(
            service_name="Kafka REST Proxy",
            message=message,
            cause=cause
        )
        self.error_code = ErrorCode.KAFKA_REST_PROXY_NOT_AVAILABLE


# Cluster Management Exceptions

class ClusterManagerError(LocalKafkaManagerError):
    """Base exception for cluster management operations."""
    
    def __init__(self, message: str, error_code: ErrorCode = ErrorCode.CLUSTER_STATUS_ERROR, 
                 details: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(message, error_code, details, cause)


class ClusterStartError(ClusterManagerError):
    """Raised when cluster fails to start."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Failed to start cluster: {message}",
            error_code=ErrorCode.CLUSTER_START_FAILED,
            details=details,
            cause=cause
        )


class ClusterStopError(ClusterManagerError):
    """Raised when cluster fails to stop."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Failed to stop cluster: {message}",
            error_code=ErrorCode.CLUSTER_STOP_FAILED,
            details=details,
            cause=cause
        )


class DockerComposeError(ClusterManagerError):
    """Raised when docker-compose operations fail."""
    
    def __init__(self, command: str, exit_code: int, stderr: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Docker Compose command '{command}' failed with exit code {exit_code}",
            error_code=ErrorCode.DOCKER_COMPOSE_ERROR,
            details={
                "command": command,
                "exit_code": exit_code,
                "stderr": stderr
            },
            cause=cause
        )


class ContainerHealthError(ClusterManagerError):
    """Raised when container health checks fail."""
    
    def __init__(self, container_name: str, status: str, message: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Container '{container_name}' health check failed: {message}",
            error_code=ErrorCode.CONTAINER_HEALTH_ERROR,
            details={
                "container": container_name,
                "status": status
            },
            cause=cause
        )


# Topic Management Exceptions

class TopicManagerError(LocalKafkaManagerError):
    """Base exception for topic management operations."""
    
    def __init__(self, message: str, error_code: ErrorCode = ErrorCode.TOPIC_CONFIG_INVALID,
                 details: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(message, error_code, details, cause)


class TopicNotFoundError(TopicManagerError):
    """Raised when a topic is not found."""
    
    def __init__(self, topic_name: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Topic '{topic_name}' not found",
            error_code=ErrorCode.TOPIC_NOT_FOUND,
            details={"topic": topic_name},
            cause=cause
        )


class TopicAlreadyExistsError(TopicManagerError):
    """Raised when trying to create a topic that already exists."""
    
    def __init__(self, topic_name: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Topic '{topic_name}' already exists",
            error_code=ErrorCode.TOPIC_ALREADY_EXISTS,
            details={"topic": topic_name},
            cause=cause
        )


class TopicCreationError(TopicManagerError):
    """Raised when topic creation fails."""
    
    def __init__(self, topic_name: str, reason: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Failed to create topic '{topic_name}': {reason}",
            error_code=ErrorCode.TOPIC_CREATION_FAILED,
            details={"topic": topic_name, "reason": reason},
            cause=cause
        )


class TopicDeletionError(TopicManagerError):
    """Raised when topic deletion fails."""
    
    def __init__(self, topic_name: str, reason: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Failed to delete topic '{topic_name}': {reason}",
            error_code=ErrorCode.TOPIC_DELETION_FAILED,
            details={"topic": topic_name, "reason": reason},
            cause=cause
        )


class TopicConfigurationError(TopicManagerError):
    """Raised when topic configuration is invalid."""
    
    def __init__(self, topic_name: str, config_errors: Dict[str, str], cause: Optional[Exception] = None):
        super().__init__(
            message=f"Invalid configuration for topic '{topic_name}'",
            error_code=ErrorCode.TOPIC_CONFIG_INVALID,
            details={"topic": topic_name, "config_errors": config_errors},
            cause=cause
        )


# Message Operation Exceptions

class MessageManagerError(LocalKafkaManagerError):
    """Base exception for message management operations."""
    
    def __init__(self, message: str, error_code: ErrorCode = ErrorCode.MESSAGE_PRODUCE_FAILED,
                 details: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(message, error_code, details, cause)


class MessageProduceError(MessageManagerError):
    """Raised when message production fails."""
    
    def __init__(self, topic: str, reason: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Failed to produce message to topic '{topic}': {reason}",
            error_code=ErrorCode.MESSAGE_PRODUCE_FAILED,
            details={"topic": topic, "reason": reason},
            cause=cause
        )


class MessageConsumeError(MessageManagerError):
    """Raised when message consumption fails."""
    
    def __init__(self, topic: str, consumer_group: str, reason: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Failed to consume messages from topic '{topic}' with group '{consumer_group}': {reason}",
            error_code=ErrorCode.MESSAGE_CONSUME_FAILED,
            details={"topic": topic, "consumer_group": consumer_group, "reason": reason},
            cause=cause
        )


class ConsumerGroupError(MessageManagerError):
    """Raised when consumer group operations fail."""
    
    def __init__(self, consumer_group: str, operation: str, reason: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Consumer group '{consumer_group}' {operation} failed: {reason}",
            error_code=ErrorCode.CONSUMER_GROUP_ERROR,
            details={"consumer_group": consumer_group, "operation": operation, "reason": reason},
            cause=cause
        )


class MessageSerializationError(MessageManagerError):
    """Raised when message serialization/deserialization fails."""
    
    def __init__(self, operation: str, reason: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Message {operation} failed: {reason}",
            error_code=ErrorCode.MESSAGE_SERIALIZATION_ERROR,
            details={"operation": operation, "reason": reason},
            cause=cause
        )


# Service Catalog Exceptions

class ServiceCatalogError(LocalKafkaManagerError):
    """Base exception for service catalog operations."""
    
    def __init__(self, message: str, error_code: ErrorCode = ErrorCode.SERVICE_DISCOVERY_ERROR,
                 details: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(message, error_code, details, cause)


class CatalogRefreshError(ServiceCatalogError):
    """Raised when service catalog refresh fails."""
    
    def __init__(self, reason: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Failed to refresh service catalog: {reason}",
            error_code=ErrorCode.CATALOG_REFRESH_FAILED,
            details={"reason": reason},
            cause=cause
        )


class ServiceDiscoveryError(ServiceCatalogError):
    """Raised when service discovery fails."""
    
    def __init__(self, service_name: str, reason: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Failed to discover service '{service_name}': {reason}",
            error_code=ErrorCode.SERVICE_DISCOVERY_ERROR,
            details={"service": service_name, "reason": reason},
            cause=cause
        )


# Validation Exceptions

class ValidationError(LocalKafkaManagerError):
    """Raised when input validation fails."""
    
    def __init__(self, field: str, value: Any, reason: str, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Validation failed for field '{field}': {reason}",
            error_code=ErrorCode.VALIDATION_ERROR,
            details={"field": field, "value": str(value), "reason": reason},
            cause=cause
        )


# Timeout Exceptions

class TimeoutError(LocalKafkaManagerError):
    """Raised when operations timeout."""
    
    def __init__(self, operation: str, timeout_seconds: int, cause: Optional[Exception] = None):
        super().__init__(
            message=f"Operation '{operation}' timed out after {timeout_seconds} seconds",
            error_code=ErrorCode.TIMEOUT,
            details={"operation": operation, "timeout_seconds": timeout_seconds},
            cause=cause
        )