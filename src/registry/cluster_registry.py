"""
Central registry for managing multiple Kafka clusters.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Set, Any, Tuple
from pathlib import Path

from ..models.multi_cluster import ClusterDefinition, ClusterLifecycleStatus, PortAllocation
from ..storage.base import StorageBackend
from ..storage.file_backend import FileStorageBackend
from ..networking.port_allocator import PortAllocator, GlobalPortAllocator
from ..networking.network_manager import NetworkManager, GlobalNetworkManager
from ..utils.logging import get_logger
from .exceptions import (
    ClusterRegistryError,
    ClusterNotFoundError,
    ClusterAlreadyExistsError,
    ClusterValidationError,
    ClusterConflictError,
    ClusterOperationError
)

logger = get_logger(__name__)


class ClusterRegistry:
    """Central registry for managing multiple Kafka clusters."""
    
    def __init__(self, 
                 storage_backend: Optional[StorageBackend] = None,
                 port_allocator: Optional[PortAllocator] = None,
                 network_manager: Optional[NetworkManager] = None):
        """Initialize cluster registry.
        
        Args:
            storage_backend: Storage backend for persistence (uses FileStorageBackend if None)
            port_allocator: Port allocator for managing ports (uses global if None)
            network_manager: Network manager for Docker networks (uses global if None)
        """
        self.storage = storage_backend or FileStorageBackend()
        self.port_allocator = port_allocator
        self.network_manager = network_manager
        
        # In-memory cache of cluster definitions
        self._clusters: Dict[str, ClusterDefinition] = {}
        self._cluster_status: Dict[str, ClusterLifecycleStatus] = {}
        
        # Thread safety
        self._lock = asyncio.Lock()
        
        # Validation rules
        self._reserved_names = {"default", "admin", "system", "internal"}
        
        logger.info("Cluster registry initialized")
    
    async def _get_port_allocator(self) -> PortAllocator:
        """Get port allocator instance."""
        if self.port_allocator is None:
            self.port_allocator = await GlobalPortAllocator.get_instance()
        return self.port_allocator
    
    async def _get_network_manager(self) -> NetworkManager:
        """Get network manager instance."""
        if self.network_manager is None:
            self.network_manager = await GlobalNetworkManager.get_instance()
        return self.network_manager
    
    async def initialize(self) -> None:
        """Initialize the registry by loading existing clusters."""
        async with self._lock:
            try:
                # Load existing clusters from storage
                clusters = await self.storage.list_clusters()
                
                for cluster in clusters:
                    self._clusters[cluster.id] = cluster
                    # Initialize status as stopped (will be updated by health checks)
                    self._cluster_status[cluster.id] = ClusterLifecycleStatus.STOPPED
                
                logger.info(f"Loaded {len(clusters)} clusters from storage")
                
                # Validate loaded clusters for conflicts
                await self._validate_all_clusters()
                
            except Exception as e:
                logger.error(f"Failed to initialize cluster registry: {e}")
                raise ClusterRegistryError(f"Registry initialization failed: {e}")
    
    async def register_cluster(self, definition: ClusterDefinition) -> bool:
        """Register a new cluster definition.
        
        Args:
            definition: Cluster definition to register
            
        Returns:
            True if cluster was registered successfully
            
        Raises:
            ClusterAlreadyExistsError: If cluster ID already exists
            ClusterValidationError: If cluster definition is invalid
            ClusterConflictError: If cluster conflicts with existing clusters
        """
        async with self._lock:
            # Check if cluster already exists
            if definition.id in self._clusters:
                raise ClusterAlreadyExistsError(definition.id)
            
            # Validate cluster definition
            validation_errors = await self._validate_cluster_definition(definition)
            if validation_errors:
                raise ClusterValidationError(definition.id, validation_errors)
            
            # Check for conflicts with existing clusters
            conflicts = await self._check_cluster_conflicts(definition)
            if conflicts:
                raise ClusterConflictError(definition.id, conflicts)
            
            try:
                # Set registration metadata
                definition.created_at = datetime.utcnow()
                definition.updated_at = definition.created_at
                definition.status = ClusterLifecycleStatus.REGISTERED
                logger.debug(f"Set cluster status to REGISTERED for '{definition.id}'")
                
                # Allocate ports if not already allocated
                if not definition.port_allocation:
                    logger.debug(f"Allocating ports for cluster '{definition.id}'")
                    port_allocator = await self._get_port_allocator()
                    definition.port_allocation = await port_allocator.allocate_ports(definition.id)
                    logger.debug(f"Allocated ports for cluster '{definition.id}': {definition.port_allocation}")
                
                # Create network name if not set
                if not definition.network_name:
                    definition.network_name = f"kafka-cluster-{definition.id}"
                    logger.debug(f"Set network name for cluster '{definition.id}': {definition.network_name}")
                
                # Save to storage
                logger.debug(f"Saving cluster '{definition.id}' to storage")
                await self.storage.save_cluster(definition)
                logger.debug(f"Successfully saved cluster '{definition.id}' to storage")
                
                # Add to in-memory cache
                self._clusters[definition.id] = definition
                self._cluster_status[definition.id] = ClusterLifecycleStatus.REGISTERED
                
                logger.info(f"Registered cluster '{definition.id}' successfully")
                return True
                
            except Exception as e:
                logger.error(f"Failed to register cluster '{definition.id}': {e} (type: {type(e).__name__})")
                logger.debug(f"Exception details: {repr(e)}")
                
                # Cleanup on failure
                await self._cleanup_failed_registration(definition.id)
                
                raise ClusterOperationError(definition.id, "register", str(e))
    
    async def unregister_cluster(self, cluster_id: str, force: bool = False) -> bool:
        """Remove cluster from registry.
        
        Args:
            cluster_id: ID of cluster to unregister
            force: Force removal even if cluster is running
            
        Returns:
            True if cluster was unregistered successfully
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
            ClusterOperationError: If cluster is running and force=False
        """
        async with self._lock:
            if cluster_id not in self._clusters:
                raise ClusterNotFoundError(cluster_id, list(self._clusters.keys()))
            
            cluster = self._clusters[cluster_id]
            status = self._cluster_status.get(cluster_id, ClusterLifecycleStatus.ERROR)
            
            # Check if cluster is running
            if status in [ClusterLifecycleStatus.STARTING, ClusterLifecycleStatus.RUNNING] and not force:
                raise ClusterOperationError(
                    cluster_id, 
                    "unregister", 
                    "Cluster is running. Stop cluster first or use force=True"
                )
            
            try:
                # Release allocated ports
                if cluster.port_allocation:
                    port_allocator = await self._get_port_allocator()
                    await port_allocator.release_ports(cluster_id)
                
                # Delete network if it exists
                network_manager = await self._get_network_manager()
                if await network_manager.network_exists(cluster_id):
                    await network_manager.delete_cluster_network(cluster_id, force=True)
                
                # Remove from storage
                await self.storage.delete_cluster(cluster_id)
                
                # Remove from in-memory cache
                del self._clusters[cluster_id]
                if cluster_id in self._cluster_status:
                    del self._cluster_status[cluster_id]
                
                logger.info(f"Unregistered cluster '{cluster_id}' successfully")
                return True
                
            except Exception as e:
                logger.error(f"Failed to unregister cluster '{cluster_id}': {e}")
                raise ClusterOperationError(cluster_id, "unregister", str(e))
    
    async def get_cluster(self, cluster_id: str) -> ClusterDefinition:
        """Get cluster definition by ID.
        
        Args:
            cluster_id: ID of cluster to retrieve
            
        Returns:
            Cluster definition
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
        """
        if cluster_id not in self._clusters:
            raise ClusterNotFoundError(cluster_id, list(self._clusters.keys()))
        
        return self._clusters[cluster_id].copy()
    
    async def list_clusters(self, 
                          status_filter: Optional[ClusterLifecycleStatus] = None,
                          environment_filter: Optional[str] = None,
                          tag_filter: Optional[Dict[str, str]] = None) -> List[ClusterDefinition]:
        """List all registered clusters with optional filtering.
        
        Args:
            status_filter: Filter by cluster status
            environment_filter: Filter by environment
            tag_filter: Filter by tags (all specified tags must match)
            
        Returns:
            List of cluster definitions matching filters
        """
        clusters = []
        
        for cluster in self._clusters.values():
            # Apply status filter
            if status_filter is not None:
                cluster_status = self._cluster_status.get(cluster.id, ClusterLifecycleStatus.ERROR)
                if cluster_status != status_filter:
                    continue
            
            # Apply environment filter
            if environment_filter is not None and cluster.environment != environment_filter:
                continue
            
            # Apply tag filter
            if tag_filter is not None:
                if not all(cluster.tags.get(k) == v for k, v in tag_filter.items()):
                    continue
            
            clusters.append(cluster.copy())
        
        return clusters
    
    async def update_cluster(self, cluster_id: str, updates: Dict[str, Any]) -> ClusterDefinition:
        """Update cluster configuration.
        
        Args:
            cluster_id: ID of cluster to update
            updates: Dictionary of fields to update
            
        Returns:
            Updated cluster definition
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
            ClusterValidationError: If updates are invalid
            ClusterConflictError: If updates cause conflicts
        """
        async with self._lock:
            if cluster_id not in self._clusters:
                raise ClusterNotFoundError(cluster_id, list(self._clusters.keys()))
            
            cluster = self._clusters[cluster_id].copy()
            
            # Apply updates
            for field, value in updates.items():
                if hasattr(cluster, field):
                    setattr(cluster, field, value)
                else:
                    logger.warning(f"Ignoring unknown field '{field}' in cluster update")
            
            # Update timestamp
            cluster.updated_at = datetime.utcnow()
            
            # Validate updated cluster
            validation_errors = await self._validate_cluster_definition(cluster)
            if validation_errors:
                raise ClusterValidationError(cluster_id, validation_errors)
            
            # Check for conflicts (excluding self)
            conflicts = await self._check_cluster_conflicts(cluster, exclude_cluster_id=cluster_id)
            if conflicts:
                raise ClusterConflictError(cluster_id, conflicts)
            
            try:
                # Save to storage
                await self.storage.save_cluster(cluster)
                
                # Update in-memory cache
                self._clusters[cluster_id] = cluster
                
                logger.info(f"Updated cluster '{cluster_id}' successfully")
                return cluster.copy()
                
            except Exception as e:
                logger.error(f"Failed to update cluster '{cluster_id}': {e}")
                raise ClusterOperationError(cluster_id, "update", str(e))
    
    async def get_cluster_status(self, cluster_id: str) -> ClusterLifecycleStatus:
        """Get current status of a cluster.
        
        Args:
            cluster_id: ID of cluster
            
        Returns:
            Current cluster status
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
        """
        if cluster_id not in self._clusters:
            raise ClusterNotFoundError(cluster_id, list(self._clusters.keys()))
        
        return self._cluster_status.get(cluster_id, ClusterLifecycleStatus.ERROR)
    
    async def update_cluster_status(self, cluster_id: str, status: ClusterLifecycleStatus) -> None:
        """Update cluster status.
        
        Args:
            cluster_id: ID of cluster
            status: New status
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
        """
        if cluster_id not in self._clusters:
            raise ClusterNotFoundError(cluster_id, list(self._clusters.keys()))
        
        old_status = self._cluster_status.get(cluster_id, ClusterLifecycleStatus.ERROR)
        self._cluster_status[cluster_id] = status
        
        # Update cluster definition status and timestamps
        cluster = self._clusters[cluster_id]
        cluster.status = status
        
        if status == ClusterLifecycleStatus.RUNNING:
            cluster.last_started = datetime.utcnow()
        elif status == ClusterLifecycleStatus.STOPPED:
            cluster.last_stopped = datetime.utcnow()
        
        # Save updated cluster to storage
        try:
            await self.storage.save_cluster(cluster)
        except Exception as e:
            logger.warning(f"Failed to save cluster status update: {e}")
        
        if old_status != status:
            logger.info(f"Cluster '{cluster_id}' status changed from {old_status} to {status}")
    
    async def get_all_cluster_status(self) -> Dict[str, ClusterLifecycleStatus]:
        """Get status of all registered clusters.
        
        Returns:
            Dictionary mapping cluster IDs to their status
        """
        return self._cluster_status.copy()
    
    async def cluster_exists(self, cluster_id: str) -> bool:
        """Check if cluster exists in registry.
        
        Args:
            cluster_id: ID of cluster to check
            
        Returns:
            True if cluster exists
        """
        return cluster_id in self._clusters
    
    async def get_cluster_count(self) -> int:
        """Get total number of registered clusters.
        
        Returns:
            Number of registered clusters
        """
        return len(self._clusters)
    
    async def get_clusters_by_environment(self, environment: str) -> List[ClusterDefinition]:
        """Get all clusters in a specific environment.
        
        Args:
            environment: Environment name
            
        Returns:
            List of clusters in the environment
        """
        return await self.list_clusters(environment_filter=environment)
    
    async def get_clusters_by_tag(self, tag_key: str, tag_value: Optional[str] = None) -> List[ClusterDefinition]:
        """Get clusters by tag.
        
        Args:
            tag_key: Tag key to search for
            tag_value: Tag value to match (if None, matches any value)
            
        Returns:
            List of clusters with matching tag
        """
        clusters = []
        
        for cluster in self._clusters.values():
            if tag_key in cluster.tags:
                if tag_value is None or cluster.tags[tag_key] == tag_value:
                    clusters.append(cluster.copy())
        
        return clusters
    
    async def search_clusters(self, query: str) -> List[ClusterDefinition]:
        """Search clusters by name, description, or tags.
        
        Args:
            query: Search query
            
        Returns:
            List of clusters matching the search query
        """
        query_lower = query.lower()
        clusters = []
        
        for cluster in self._clusters.values():
            # Search in name
            if query_lower in cluster.name.lower():
                clusters.append(cluster.copy())
                continue
            
            # Search in description
            if cluster.description and query_lower in cluster.description.lower():
                clusters.append(cluster.copy())
                continue
            
            # Search in tags
            for tag_key, tag_value in cluster.tags.items():
                if query_lower in tag_key.lower() or query_lower in tag_value.lower():
                    clusters.append(cluster.copy())
                    break
        
        return clusters
    
    async def get_registry_stats(self) -> Dict[str, Any]:
        """Get registry statistics.
        
        Returns:
            Dictionary with registry statistics
        """
        status_counts = {}
        environment_counts = {}
        
        for cluster in self._clusters.values():
            # Count by status
            status = self._cluster_status.get(cluster.id, ClusterLifecycleStatus.ERROR)
            status_counts[status.value] = status_counts.get(status.value, 0) + 1
            
            # Count by environment
            environment_counts[cluster.environment] = environment_counts.get(cluster.environment, 0) + 1
        
        return {
            "total_clusters": len(self._clusters),
            "status_distribution": status_counts,
            "environment_distribution": environment_counts,
            "storage_backend": type(self.storage).__name__,
            "last_updated": datetime.utcnow().isoformat()
        }
    
    async def validate_cluster_health(self, cluster_id: str) -> Dict[str, Any]:
        """Validate cluster health and configuration.
        
        Args:
            cluster_id: ID of cluster to validate
            
        Returns:
            Health validation results
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
        """
        if cluster_id not in self._clusters:
            raise ClusterNotFoundError(cluster_id, list(self._clusters.keys()))
        
        cluster = self._clusters[cluster_id]
        health_result = {
            "cluster_id": cluster_id,
            "overall_health": "healthy",
            "issues": [],
            "warnings": [],
            "checks": {}
        }
        
        try:
            # Check port allocation
            if cluster.port_allocation:
                port_allocator = await self._get_port_allocator()
                port_valid = await port_allocator.validate_allocation(cluster_id)
                health_result["checks"]["port_allocation"] = port_valid
                
                if not port_valid:
                    health_result["issues"].append("Port allocation is invalid")
                    health_result["overall_health"] = "unhealthy"
            
            # Check network configuration
            network_manager = await self._get_network_manager()
            network_exists = await network_manager.network_exists(cluster_id)
            health_result["checks"]["network_exists"] = network_exists
            
            if not network_exists and self._cluster_status.get(cluster_id) == ClusterLifecycleStatus.RUNNING:
                health_result["issues"].append("Network does not exist but cluster is marked as running")
                health_result["overall_health"] = "unhealthy"
            
            # Check data directory
            if cluster.data_directory:
                data_dir_exists = cluster.data_directory.exists()
                health_result["checks"]["data_directory_exists"] = data_dir_exists
                
                if not data_dir_exists:
                    health_result["warnings"].append("Data directory does not exist")
            
            # Check configuration consistency
            validation_errors = await self._validate_cluster_definition(cluster)
            health_result["checks"]["configuration_valid"] = len(validation_errors) == 0
            
            if validation_errors:
                health_result["issues"].extend(validation_errors)
                health_result["overall_health"] = "unhealthy"
            
            return health_result
            
        except Exception as e:
            logger.error(f"Failed to validate cluster health for '{cluster_id}': {e}")
            health_result["overall_health"] = "error"
            health_result["issues"].append(f"Health check failed: {e}")
            return health_result
    
    # Private helper methods
    
    async def _validate_cluster_definition(self, cluster: ClusterDefinition) -> List[str]:
        """Validate cluster definition."""
        errors = []
        
        # Validate cluster ID
        if not cluster.id:
            errors.append("Cluster ID is required")
        elif cluster.id in self._reserved_names:
            errors.append(f"Cluster ID '{cluster.id}' is reserved")
        elif not cluster.id.replace("-", "").replace("_", "").isalnum():
            errors.append("Cluster ID must contain only alphanumeric characters, hyphens, and underscores")
        
        # Validate cluster name
        if not cluster.name:
            errors.append("Cluster name is required")
        elif len(cluster.name) > 100:
            errors.append("Cluster name must be 100 characters or less")
        
        # Validate environment
        valid_environments = {"dev", "test", "staging", "prod", "development", "testing", "production"}
        if cluster.environment not in valid_environments:
            errors.append(f"Environment must be one of: {', '.join(valid_environments)}")
        
        # Validate port allocation
        if cluster.port_allocation:
            port_allocator = await self._get_port_allocator()
            for port in cluster.port_allocation.get_all_ports():
                if not await port_allocator.is_port_available(port):
                    errors.append(f"Port {port} is not available")
        
        # Validate data directory
        if cluster.data_directory:
            try:
                # Check if parent directory exists and is writable
                parent_dir = cluster.data_directory.parent
                if not parent_dir.exists():
                    errors.append(f"Parent directory {parent_dir} does not exist")
                elif not parent_dir.is_dir():
                    errors.append(f"Parent path {parent_dir} is not a directory")
            except Exception as e:
                errors.append(f"Invalid data directory path: {e}")
        
        return errors
    
    async def _check_cluster_conflicts(self, cluster: ClusterDefinition, 
                                     exclude_cluster_id: Optional[str] = None) -> List[str]:
        """Check for conflicts with existing clusters."""
        conflicts = []
        
        for existing_id, existing_cluster in self._clusters.items():
            if exclude_cluster_id and existing_id == exclude_cluster_id:
                continue
            
            # Check name conflicts
            if cluster.name == existing_cluster.name:
                conflicts.append(f"Cluster name '{cluster.name}' already used by cluster '{existing_id}'")
            
            # Check port conflicts
            if cluster.port_allocation and existing_cluster.port_allocation:
                cluster_ports = set(cluster.port_allocation.get_all_ports())
                existing_ports = set(existing_cluster.port_allocation.get_all_ports())
                
                port_conflicts = cluster_ports & existing_ports
                if port_conflicts:
                    conflicts.append(f"Port conflicts with cluster '{existing_id}': {sorted(port_conflicts)}")
            
            # Check network name conflicts
            if cluster.network_name and cluster.network_name == existing_cluster.network_name:
                conflicts.append(f"Network name '{cluster.network_name}' already used by cluster '{existing_id}'")
            
            # Check data directory conflicts
            if cluster.data_directory and existing_cluster.data_directory:
                if cluster.data_directory == existing_cluster.data_directory:
                    conflicts.append(f"Data directory conflicts with cluster '{existing_id}'")
        
        return conflicts
    
    async def _validate_all_clusters(self) -> None:
        """Validate all loaded clusters for consistency."""
        for cluster_id, cluster in self._clusters.items():
            try:
                validation_errors = await self._validate_cluster_definition(cluster)
                if validation_errors:
                    logger.warning(f"Cluster '{cluster_id}' has validation errors: {validation_errors}")
            except Exception as e:
                logger.error(f"Failed to validate cluster '{cluster_id}': {e}")
    
    async def _cleanup_failed_registration(self, cluster_id: str) -> None:
        """Clean up resources after failed cluster registration."""
        try:
            # Release ports if allocated
            port_allocator = await self._get_port_allocator()
            await port_allocator.release_ports(cluster_id)
            
            # Delete network if created
            network_manager = await self._get_network_manager()
            if await network_manager.network_exists(cluster_id):
                await network_manager.delete_cluster_network(cluster_id, force=True)
            
            # Remove from storage if saved
            try:
                await self.storage.delete_cluster(cluster_id)
            except:
                pass  # Ignore if not saved yet
            
            # Remove from cache if added
            if cluster_id in self._clusters:
                del self._clusters[cluster_id]
            if cluster_id in self._cluster_status:
                del self._cluster_status[cluster_id]
            
            logger.info(f"Cleaned up failed registration for cluster '{cluster_id}'")
            
        except Exception as e:
            logger.warning(f"Failed to cleanup after failed registration for '{cluster_id}': {e}")


class GlobalClusterRegistry:
    """Singleton cluster registry for global use."""
    
    _instance: Optional[ClusterRegistry] = None
    _lock = asyncio.Lock()
    
    @classmethod
    async def get_instance(cls, 
                          storage_backend: Optional[StorageBackend] = None,
                          port_allocator: Optional[PortAllocator] = None,
                          network_manager: Optional[NetworkManager] = None) -> ClusterRegistry:
        """Get the global cluster registry instance.
        
        Args:
            storage_backend: Storage backend (only used on first call)
            port_allocator: Port allocator (only used on first call)
            network_manager: Network manager (only used on first call)
            
        Returns:
            Global ClusterRegistry instance
        """
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = ClusterRegistry(storage_backend, port_allocator, network_manager)
                    await cls._instance.initialize()
                    logger.info("Created global cluster registry instance")
        
        return cls._instance
    
    @classmethod
    async def reset_instance(cls) -> None:
        """Reset the global instance (mainly for testing)."""
        async with cls._lock:
            cls._instance = None
            logger.info("Reset global cluster registry instance")


# Convenience functions

async def get_cluster_registry() -> ClusterRegistry:
    """Get the global cluster registry instance.
    
    Returns:
        Global ClusterRegistry instance
    """
    return await GlobalClusterRegistry.get_instance()


async def register_cluster(definition: ClusterDefinition) -> bool:
    """Register a cluster using the global registry.
    
    Args:
        definition: Cluster definition to register
        
    Returns:
        True if cluster was registered successfully
    """
    registry = await get_cluster_registry()
    return await registry.register_cluster(definition)


async def get_cluster(cluster_id: str) -> ClusterDefinition:
    """Get cluster definition using the global registry.
    
    Args:
        cluster_id: ID of cluster to retrieve
        
    Returns:
        Cluster definition
    """
    registry = await get_cluster_registry()
    return await registry.get_cluster(cluster_id)


async def list_clusters(**filters) -> List[ClusterDefinition]:
    """List clusters using the global registry.
    
    Args:
        **filters: Optional filters (status_filter, environment_filter, tag_filter)
        
    Returns:
        List of cluster definitions
    """
    registry = await get_cluster_registry()
    return await registry.list_clusters(**filters)