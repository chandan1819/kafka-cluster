"""
Abstract base class for storage backends.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from ..models.multi_cluster import ClusterDefinition, ClusterTemplate


class StorageBackend(ABC):
    """Abstract interface for cluster registry storage."""
    
    @abstractmethod
    async def initialize(self) -> bool:
        """Initialize the storage backend.
        
        Returns:
            True if initialization was successful
            
        Raises:
            StorageBackendError: If initialization fails
        """
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close the storage backend and cleanup resources."""
        pass
    
    # Cluster operations
    
    @abstractmethod
    async def save_cluster(self, cluster: ClusterDefinition) -> bool:
        """Save cluster definition.
        
        Args:
            cluster: Cluster definition to save
            
        Returns:
            True if save was successful
            
        Raises:
            StorageBackendError: If save operation fails
            ClusterAlreadyExistsError: If cluster already exists (for create operations)
        """
        pass
    
    @abstractmethod
    async def load_cluster(self, cluster_id: str) -> Optional[ClusterDefinition]:
        """Load cluster definition by ID.
        
        Args:
            cluster_id: Unique cluster identifier
            
        Returns:
            ClusterDefinition if found, None otherwise
            
        Raises:
            StorageBackendError: If load operation fails
        """
        pass
    
    @abstractmethod
    async def update_cluster(self, cluster: ClusterDefinition) -> bool:
        """Update existing cluster definition.
        
        Args:
            cluster: Updated cluster definition
            
        Returns:
            True if update was successful
            
        Raises:
            StorageBackendError: If update operation fails
            ClusterNotFoundError: If cluster doesn't exist
        """
        pass
    
    @abstractmethod
    async def delete_cluster(self, cluster_id: str) -> bool:
        """Delete cluster definition.
        
        Args:
            cluster_id: Unique cluster identifier
            
        Returns:
            True if deletion was successful
            
        Raises:
            StorageBackendError: If delete operation fails
            ClusterNotFoundError: If cluster doesn't exist
        """
        pass
    
    @abstractmethod
    async def list_clusters(self, filters: Optional[Dict[str, Any]] = None) -> List[ClusterDefinition]:
        """List all cluster definitions with optional filtering.
        
        Args:
            filters: Optional filters to apply (e.g., {"environment": "development"})
            
        Returns:
            List of cluster definitions
            
        Raises:
            StorageBackendError: If list operation fails
        """
        pass
    
    @abstractmethod
    async def cluster_exists(self, cluster_id: str) -> bool:
        """Check if cluster exists.
        
        Args:
            cluster_id: Unique cluster identifier
            
        Returns:
            True if cluster exists
            
        Raises:
            StorageBackendError: If check operation fails
        """
        pass
    
    # Template operations
    
    @abstractmethod
    async def save_template(self, template: ClusterTemplate) -> bool:
        """Save cluster template.
        
        Args:
            template: Cluster template to save
            
        Returns:
            True if save was successful
            
        Raises:
            StorageBackendError: If save operation fails
            TemplateAlreadyExistsError: If template already exists (for create operations)
        """
        pass
    
    @abstractmethod
    async def load_template(self, template_id: str) -> Optional[ClusterTemplate]:
        """Load cluster template by ID.
        
        Args:
            template_id: Unique template identifier
            
        Returns:
            ClusterTemplate if found, None otherwise
            
        Raises:
            StorageBackendError: If load operation fails
        """
        pass
    
    @abstractmethod
    async def update_template(self, template: ClusterTemplate) -> bool:
        """Update existing cluster template.
        
        Args:
            template: Updated cluster template
            
        Returns:
            True if update was successful
            
        Raises:
            StorageBackendError: If update operation fails
            TemplateNotFoundError: If template doesn't exist
        """
        pass
    
    @abstractmethod
    async def delete_template(self, template_id: str) -> bool:
        """Delete cluster template.
        
        Args:
            template_id: Unique template identifier
            
        Returns:
            True if deletion was successful
            
        Raises:
            StorageBackendError: If delete operation fails
            TemplateNotFoundError: If template doesn't exist
        """
        pass
    
    @abstractmethod
    async def list_templates(self, filters: Optional[Dict[str, Any]] = None) -> List[ClusterTemplate]:
        """List all cluster templates with optional filtering.
        
        Args:
            filters: Optional filters to apply (e.g., {"category": "development"})
            
        Returns:
            List of cluster templates
            
        Raises:
            StorageBackendError: If list operation fails
        """
        pass
    
    @abstractmethod
    async def template_exists(self, template_id: str) -> bool:
        """Check if template exists.
        
        Args:
            template_id: Unique template identifier
            
        Returns:
            True if template exists
            
        Raises:
            StorageBackendError: If check operation fails
        """
        pass
    
    # Utility methods
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on storage backend.
        
        Returns:
            Dictionary with health status information
        """
        pass
    
    @abstractmethod
    async def get_stats(self) -> Dict[str, Any]:
        """Get storage backend statistics.
        
        Returns:
            Dictionary with storage statistics
        """
        pass
    
    @abstractmethod
    async def backup(self, backup_path: str) -> bool:
        """Create backup of storage data.
        
        Args:
            backup_path: Path where backup should be created
            
        Returns:
            True if backup was successful
            
        Raises:
            StorageBackendError: If backup operation fails
        """
        pass
    
    @abstractmethod
    async def restore(self, backup_path: str) -> bool:
        """Restore storage data from backup.
        
        Args:
            backup_path: Path to backup file
            
        Returns:
            True if restore was successful
            
        Raises:
            StorageBackendError: If restore operation fails
        """
        pass