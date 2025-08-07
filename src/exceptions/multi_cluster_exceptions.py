"""
Multi-Cluster specific exceptions and error handling.

This module provides comprehensive error handling for multi-cluster operations
including cluster-specific errors, recovery mechanisms, and graceful degradation.
"""

from enum import Enum
from typing import Optional, Dict, Any, List
from datetime import datetime


class ErrorSeverity(Enum):
    """Error severity levels for multi-cluster operations."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Categories of multi-cluster errors."""
    CLUSTER_LIFECYCLE = "cluster_lifecycle"
    NETWORK = "network"
    RESOURCE = "resource"
    CONFIGURATION = "configuration"
    SECURITY = "security"
    STORAGE = "storage"
    CROSS_CLUSTER = "cross_cluster"
    TEMPLATE = "template"
    MONITORING = "monitoring"


class RecoveryAction(Enum):
    """Available recovery actions for different error types."""
    RETRY = "retry"
    RESTART_CLUSTER = "restart_cluster"
    RECREATE_CLUSTER = "recreate_cluster"
    FALLBACK_CLUSTER = "fallback_cluster"
    SCALE_DOWN = "scale_down"
    MANUAL_INTERVENTION = "manual_intervention"
    IGNORE = "ignore"
    GRACEFUL_DEGRADATION = "graceful_degradation"


class MultiClusterError(Exception):
    """Base exception for all multi-cluster operations."""
    
    def __init__(
        self,
        message: str,
        cluster_id: Optional[str] = None,
        error_code: Optional[str] = None,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        category: ErrorCategory = ErrorCategory.CLUSTER_LIFECYCLE,
        recovery_actions: Optional[List[RecoveryAction]] = None,
        context: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None
    ):
        super().__init__(message)
        self.message = message
        self.cluster_id = cluster_id
        self.error_code = error_code
        self.severity = severity
        self.category = category
        self.recovery_actions = recovery_actions or []
        self.context = context or {}
        self.cause = cause
        self.timestamp = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for logging/serialization."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "cluster_id": self.cluster_id,
            "error_code": self.error_code,
            "severity": self.severity.value,
            "category": self.category.value,
            "recovery_actions": [action.value for action in self.recovery_actions],
            "context": self.context,
            "timestamp": self.timestamp.isoformat(),
            "cause": str(self.cause) if self.cause else None
        }


class ClusterCreationError(MultiClusterError):
    """Error during cluster creation."""
    
    def __init__(self, message: str, cluster_id: str, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="CLUSTER_CREATION_FAILED",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.CLUSTER_LIFECYCLE,
            recovery_actions=[RecoveryAction.RETRY, RecoveryAction.MANUAL_INTERVENTION],
            **kwargs
        )


class ClusterStartupError(MultiClusterError):
    """Error during cluster startup."""
    
    def __init__(self, message: str, cluster_id: str, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="CLUSTER_STARTUP_FAILED",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.CLUSTER_LIFECYCLE,
            recovery_actions=[RecoveryAction.RETRY, RecoveryAction.RESTART_CLUSTER],
            **kwargs
        )


class ClusterShutdownError(MultiClusterError):
    """Error during cluster shutdown."""
    
    def __init__(self, message: str, cluster_id: str, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="CLUSTER_SHUTDOWN_FAILED",
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.CLUSTER_LIFECYCLE,
            recovery_actions=[RecoveryAction.RETRY, RecoveryAction.MANUAL_INTERVENTION],
            **kwargs
        )


class ClusterHealthError(MultiClusterError):
    """Error related to cluster health monitoring."""
    
    def __init__(self, message: str, cluster_id: str, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="CLUSTER_HEALTH_CHECK_FAILED",
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.MONITORING,
            recovery_actions=[RecoveryAction.RETRY, RecoveryAction.RESTART_CLUSTER],
            **kwargs
        )


class PortAllocationError(MultiClusterError):
    """Error during port allocation."""
    
    def __init__(self, message: str, cluster_id: Optional[str] = None, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="PORT_ALLOCATION_FAILED",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.NETWORK,
            recovery_actions=[RecoveryAction.RETRY, RecoveryAction.MANUAL_INTERVENTION],
            **kwargs
        )


class NetworkIsolationError(MultiClusterError):
    """Error during network isolation setup."""
    
    def __init__(self, message: str, cluster_id: str, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="NETWORK_ISOLATION_FAILED",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.NETWORK,
            recovery_actions=[RecoveryAction.RETRY, RecoveryAction.RECREATE_CLUSTER],
            **kwargs
        )


class ResourceExhaustionError(MultiClusterError):
    """Error due to resource exhaustion."""
    
    def __init__(self, message: str, resource_type: str, cluster_id: Optional[str] = None, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="RESOURCE_EXHAUSTION",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.RESOURCE,
            recovery_actions=[RecoveryAction.SCALE_DOWN, RecoveryAction.MANUAL_INTERVENTION],
            context={"resource_type": resource_type},
            **kwargs
        )


class ConfigurationError(MultiClusterError):
    """Error in cluster configuration."""
    
    def __init__(self, message: str, cluster_id: Optional[str] = None, config_key: Optional[str] = None, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="CONFIGURATION_ERROR",
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.CONFIGURATION,
            recovery_actions=[RecoveryAction.MANUAL_INTERVENTION],
            context={"config_key": config_key} if config_key else {},
            **kwargs
        )


class TemplateError(MultiClusterError):
    """Error related to cluster templates."""
    
    def __init__(self, message: str, template_id: str, **kwargs):
        super().__init__(
            message=message,
            error_code="TEMPLATE_ERROR",
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.TEMPLATE,
            recovery_actions=[RecoveryAction.MANUAL_INTERVENTION],
            context={"template_id": template_id},
            **kwargs
        )


class CrossClusterOperationError(MultiClusterError):
    """Error during cross-cluster operations."""
    
    def __init__(self, message: str, source_cluster: str, target_cluster: str, operation: str, **kwargs):
        super().__init__(
            message=message,
            error_code="CROSS_CLUSTER_OPERATION_FAILED",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.CROSS_CLUSTER,
            recovery_actions=[RecoveryAction.RETRY, RecoveryAction.FALLBACK_CLUSTER],
            context={
                "source_cluster": source_cluster,
                "target_cluster": target_cluster,
                "operation": operation
            },
            **kwargs
        )


class StorageError(MultiClusterError):
    """Error related to storage operations."""
    
    def __init__(self, message: str, storage_type: str, cluster_id: Optional[str] = None, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="STORAGE_ERROR",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.STORAGE,
            recovery_actions=[RecoveryAction.RETRY, RecoveryAction.MANUAL_INTERVENTION],
            context={"storage_type": storage_type},
            **kwargs
        )


class SecurityError(MultiClusterError):
    """Error related to security and access control."""
    
    def __init__(self, message: str, cluster_id: Optional[str] = None, user_id: Optional[str] = None, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="SECURITY_ERROR",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.SECURITY,
            recovery_actions=[RecoveryAction.MANUAL_INTERVENTION],
            context={"user_id": user_id} if user_id else {},
            **kwargs
        )


class ClusterUnavailableError(MultiClusterError):
    """Error when cluster becomes unavailable."""
    
    def __init__(self, message: str, cluster_id: str, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="CLUSTER_UNAVAILABLE",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.CLUSTER_LIFECYCLE,
            recovery_actions=[
                RecoveryAction.RESTART_CLUSTER,
                RecoveryAction.FALLBACK_CLUSTER,
                RecoveryAction.GRACEFUL_DEGRADATION
            ],
            **kwargs
        )


class ClusterTimeoutError(MultiClusterError):
    """Error when cluster operations timeout."""
    
    def __init__(self, message: str, cluster_id: str, operation: str, timeout_seconds: int, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="CLUSTER_TIMEOUT",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.CLUSTER_LIFECYCLE,
            recovery_actions=[RecoveryAction.RETRY, RecoveryAction.RESTART_CLUSTER],
            context={
                "operation": operation,
                "timeout_seconds": timeout_seconds
            },
            **kwargs
        )


class ClusterConflictError(MultiClusterError):
    """Error when cluster conflicts occur (e.g., duplicate names, port conflicts)."""
    
    def __init__(self, message: str, cluster_id: str, conflict_type: str, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="CLUSTER_CONFLICT",
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.CLUSTER_LIFECYCLE,
            recovery_actions=[RecoveryAction.MANUAL_INTERVENTION],
            context={"conflict_type": conflict_type},
            **kwargs
        )


class RecoveryError(MultiClusterError):
    """Error during recovery operations."""
    
    def __init__(self, message: str, cluster_id: str, recovery_action: RecoveryAction, **kwargs):
        super().__init__(
            message=message,
            cluster_id=cluster_id,
            error_code="RECOVERY_FAILED",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.CLUSTER_LIFECYCLE,
            recovery_actions=[RecoveryAction.MANUAL_INTERVENTION],
            context={"failed_recovery_action": recovery_action.value},
            **kwargs
        )