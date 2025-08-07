"""
Exceptions for cluster registry operations.
"""

from typing import Optional, List
from ..exceptions import LocalKafkaManagerError


class ClusterRegistryError(LocalKafkaManagerError):
    """Base exception for cluster registry operations."""
    pass


class ClusterNotFoundError(ClusterRegistryError):
    """Raised when cluster ID is not found in registry."""
    
    def __init__(self, cluster_id: str, available_clusters: Optional[List[str]] = None):
        message = f"Cluster '{cluster_id}' not found"
        if available_clusters:
            message += f". Available clusters: {', '.join(available_clusters)}"
        super().__init__(message)
        self.cluster_id = cluster_id
        self.available_clusters = available_clusters or []


class ClusterAlreadyExistsError(ClusterRegistryError):
    """Raised when trying to register a cluster with existing ID."""
    
    def __init__(self, cluster_id: str):
        super().__init__(f"Cluster '{cluster_id}' already exists in registry")
        self.cluster_id = cluster_id


class ClusterValidationError(ClusterRegistryError):
    """Raised when cluster definition fails validation."""
    
    def __init__(self, cluster_id: str, validation_errors: List[str]):
        message = f"Cluster '{cluster_id}' validation failed: {'; '.join(validation_errors)}"
        super().__init__(message)
        self.cluster_id = cluster_id
        self.validation_errors = validation_errors


class ClusterConflictError(ClusterRegistryError):
    """Raised when cluster configuration conflicts with existing clusters."""
    
    def __init__(self, cluster_id: str, conflicts: List[str]):
        message = f"Cluster '{cluster_id}' conflicts with existing clusters: {'; '.join(conflicts)}"
        super().__init__(message)
        self.cluster_id = cluster_id
        self.conflicts = conflicts


class ClusterOperationError(ClusterRegistryError):
    """Raised when cluster operation fails."""
    
    def __init__(self, cluster_id: str, operation: str, reason: str):
        super().__init__(f"Failed to {operation} cluster '{cluster_id}': {reason}")
        self.cluster_id = cluster_id
        self.operation = operation
        self.reason = reason