"""
Multi-cluster manager for orchestrating multiple Kafka clusters.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from pathlib import Path

from ..models.multi_cluster import (
    ClusterDefinition, 
    ClusterSummary,
    CrossClusterOperation,
    OperationStatus
)
from ..models.base import ServiceStatus
from ..registry.cluster_registry import ClusterRegistry
from ..services.cluster_factory import ClusterFactory
from ..services.cluster_manager import ClusterManager
from ..services.template_manager import TemplateManager
from ..exceptions.multi_cluster_exceptions import (
    ClusterCreationError, ClusterStartupError, ClusterShutdownError,
    ClusterUnavailableError, ClusterTimeoutError, MultiClusterError
)
from ..recovery.error_recovery import error_recovery_manager
from ..monitoring.cluster_health_monitor import cluster_health_monitor
from ..services.graceful_degradation import graceful_degradation_service
from ..exceptions import (
    ClusterManagerError,
    ValidationError
)
from ..registry.exceptions import (
    ClusterNotFoundError,
    ClusterAlreadyExistsError,
    ClusterOperationError
)
from ..utils.logging import get_logger, LogContext
from ..utils.metrics import counter, timer, gauge

logger = get_logger(__name__)


class MultiClusterManagerError(ClusterManagerError):
    """Base exception for multi-cluster manager operations."""
    
    def __init__(self, message: str, cluster_id: Optional[str] = None, 
                 details: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(message)
        self.cluster_id = cluster_id
        self.details = details or {}
        self.cause = cause


class ConcurrentOperationError(MultiClusterManagerError):
    """Raised when concurrent operations conflict."""
    pass


class MultiClusterManager:
    """Manages multiple Kafka clusters simultaneously."""
    
    def __init__(self, 
                 registry: ClusterRegistry,
                 factory: ClusterFactory,
                 template_manager: TemplateManager):
        """Initialize multi-cluster manager.
        
        Args:
            registry: Cluster registry for managing cluster definitions
            factory: Cluster factory for creating cluster instances
            template_manager: Template manager for cluster templates
        """
        self.registry = registry
        self.factory = factory
        self.template_manager = template_manager
        
        # Active cluster managers
        self._cluster_managers: Dict[str, ClusterManager] = {}
        
        # Operation tracking
        self._active_operations: Dict[str, CrossClusterOperation] = {}
        self._operation_locks: Dict[str, asyncio.Lock] = {}
        
        # Concurrency control
        self._cluster_locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()
        
        # Health monitoring
        self._health_check_interval = 30  # seconds
        self._health_check_task: Optional[asyncio.Task] = None
        
        logger.info("Multi-cluster manager initialized")
    
    async def initialize(self) -> None:
        """Initialize the multi-cluster manager."""
        async with self._global_lock:
            try:
                # Initialize registry if not already done
                await self.registry.initialize()
                
                # Load existing clusters and create managers for running ones
                clusters = await self.registry.list_clusters()
                
                for cluster in clusters:
                    if cluster.status == ServiceStatus.RUNNING:
                        try:
                            # Create cluster manager for running clusters
                            cluster_manager = await self.factory.create_cluster_manager(cluster)
                            self._cluster_managers[cluster.id] = cluster_manager
                            
                            # Verify cluster is actually running
                            status = await cluster_manager.get_status()
                            if status.status != ServiceStatus.RUNNING:
                                # Update registry status
                                await self.registry.update_cluster_status(cluster.id, status.status)
                                
                        except Exception as e:
                            logger.warning(f"Failed to initialize cluster manager for {cluster.id}: {e}")
                            # Update status to error
                            await self.registry.update_cluster_status(cluster.id, ServiceStatus.ERROR)
                
                # Start health monitoring
                await self._start_health_monitoring()
                
                # Initialize error recovery and degradation services
                await self._initialize_error_handling()
                
                logger.info(f"Multi-cluster manager initialized with {len(self._cluster_managers)} active clusters")
                
            except Exception as e:
                logger.error(f"Failed to initialize multi-cluster manager: {e}")
                raise MultiClusterManagerError(f"Initialization failed: {e}", cause=e)
    
    async def shutdown(self) -> None:
        """Shutdown the multi-cluster manager."""
        async with self._global_lock:
            try:
                # Stop health monitoring
                if self._health_check_task:
                    self._health_check_task.cancel()
                    try:
                        await self._health_check_task
                    except asyncio.CancelledError:
                        pass
                
                # Cancel active operations
                for operation in self._active_operations.values():
                    if operation.status == OperationStatus.RUNNING:
                        operation.status = OperationStatus.CANCELLED
                        operation.completed_at = datetime.utcnow()
                
                logger.info("Multi-cluster manager shutdown complete")
                
            except Exception as e:
                logger.error(f"Error during multi-cluster manager shutdown: {e}")
    
    async def create_cluster(self, definition: ClusterDefinition, auto_start: bool = True) -> ClusterDefinition:
        """Create and optionally start a new cluster.
        
        Args:
            definition: Cluster definition
            auto_start: Whether to start the cluster after creation
            
        Returns:
            Created cluster definition
            
        Raises:
            ClusterAlreadyExistsError: If cluster ID already exists
            ClusterCreationError: If cluster creation fails
        """
        with LogContext("create_cluster", cluster_id=definition.id, auto_start=auto_start):
            logger.info(f"Creating cluster: {definition.id}")
            
            async with self._get_cluster_lock(definition.id):
                try:
                    # Register cluster in registry
                    await self.registry.register_cluster(definition)
                    
                    # Create cluster manager
                    cluster_manager = await self.factory.create_cluster_manager(definition)
                    self._cluster_managers[definition.id] = cluster_manager
                    
                    # Auto-start if requested
                    if auto_start:
                        await self._start_cluster_internal(definition.id, cluster_manager)
                    
                    # Get updated definition from registry
                    created_cluster = await self.registry.get_cluster(definition.id)
                    
                    logger.info(f"Created cluster {definition.id} successfully")
                    counter('multi_cluster.create.success', 1, tags={'cluster_id': definition.id})
                    
                    return created_cluster
                    
                except Exception as e:
                    logger.error(f"Failed to create cluster {definition.id}: {e}")
                    counter('multi_cluster.create.errors', 1, tags={'cluster_id': definition.id})
                    
                    # Cleanup on failure
                    await self._cleanup_failed_cluster_creation(definition.id)
                    
                    if isinstance(e, (ClusterAlreadyExistsError, ValidationError)):
                        raise
                    else:
                        # Create specific error for recovery system
                        creation_error = ClusterCreationError(
                            message=f"Failed to create cluster {definition.id}: {str(e)}",
                            cluster_id=definition.id,
                            context={
                                "template_id": definition.template_id,
                                "environment": definition.environment,
                                "auto_start": auto_start
                            },
                            cause=e
                        )
                        
                        # Let error recovery manager handle the error
                        recovery_success = await error_recovery_manager.handle_error(creation_error)
                        
                        if not recovery_success:
                            raise creation_error
                        
                        # If recovery succeeded, try to get the cluster again
                        try:
                            return await self.registry.get_cluster(definition.id)
                        except ClusterNotFoundError:
                            raise creation_error
    
    async def start_cluster(self, cluster_id: str, force: bool = False, timeout: int = 60) -> ServiceStatus:
        """Start a specific cluster.
        
        Args:
            cluster_id: ID of cluster to start
            force: Force start even if already running
            timeout: Startup timeout in seconds
            
        Returns:
            Cluster status after start attempt
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
            ClusterStartupError: If cluster fails to start
        """
        with LogContext("start_cluster", cluster_id=cluster_id, force=force, timeout=timeout):
            logger.info(f"Starting cluster: {cluster_id}")
            
            async with self._get_cluster_lock(cluster_id):
                try:
                    # Check if cluster is degraded
                    if graceful_degradation_service.is_cluster_degraded(cluster_id):
                        logger.warning(f"Attempting to start degraded cluster: {cluster_id}")
                    
                    # Verify cluster exists
                    cluster = await self.registry.get_cluster(cluster_id)
                    
                    # Get or create cluster manager
                    cluster_manager = self._cluster_managers.get(cluster_id)
                    if not cluster_manager:
                        cluster_manager = await self.factory.create_cluster_manager(cluster)
                        self._cluster_managers[cluster_id] = cluster_manager
                    
                    # Start cluster with timeout
                    start_time = datetime.utcnow()
                    try:
                        status = await asyncio.wait_for(
                            self._start_cluster_internal(cluster_id, cluster_manager, force, timeout),
                            timeout=timeout
                        )
                    except asyncio.TimeoutError:
                        # Handle timeout as specific error
                        timeout_error = ClusterTimeoutError(
                            message=f"Cluster {cluster_id} failed to start within {timeout} seconds",
                            cluster_id=cluster_id,
                            operation="start",
                            timeout_seconds=timeout,
                            context={"force": force}
                        )
                        
                        # Let error recovery handle timeout
                        recovery_success = await error_recovery_manager.handle_error(timeout_error)
                        if not recovery_success:
                            raise timeout_error
                        
                        # Try to get status after recovery
                        status = await self._get_cluster_status_internal(cluster_id)
                    
                    # If cluster started successfully, restore from degradation
                    if status.status == ServiceStatus.RUNNING:
                        await graceful_degradation_service.handle_cluster_available(cluster_id)
                    
                    logger.info(f"Started cluster {cluster_id} successfully")
                    counter('multi_cluster.start.success', 1, tags={'cluster_id': cluster_id})
                    
                    return status.status
                    
                except Exception as e:
                    logger.error(f"Failed to start cluster {cluster_id}: {e}")
                    counter('multi_cluster.start.errors', 1, tags={'cluster_id': cluster_id})
                    
                    if isinstance(e, (ClusterNotFoundError, ClusterStartupError, ClusterTimeoutError)):
                        raise
                    else:
                        # Create specific startup error
                        startup_error = ClusterStartupError(
                            message=f"Failed to start cluster {cluster_id}: {str(e)}",
                            cluster_id=cluster_id,
                            context={"force": force, "timeout": timeout},
                            cause=e
                        )
                        
                        # Let error recovery manager handle the error
                        recovery_success = await error_recovery_manager.handle_error(startup_error)
                        
                        if not recovery_success:
                            # Mark cluster as unavailable for degradation
                            await graceful_degradation_service.handle_cluster_unavailable(
                                cluster_id, f"Startup failed: {str(e)}"
                            )
                            raise startup_error
                        
                        # If recovery succeeded, try to get status
                        try:
                            status = await self._get_cluster_status_internal(cluster_id)
                            return status.status
                        except Exception:
                            raise startup_error
    
    async def stop_cluster(self, cluster_id: str, force: bool = False, cleanup: bool = False, timeout: int = 30) -> bool:
        """Stop a specific cluster.
        
        Args:
            cluster_id: ID of cluster to stop
            force: Force stop containers
            cleanup: Remove containers and volumes
            timeout: Stop timeout in seconds
            
        Returns:
            True if successfully stopped
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
            MultiClusterManagerError: If cluster fails to stop
        """
        with LogContext("stop_cluster", cluster_id=cluster_id, force=force, cleanup=cleanup, timeout=timeout):
            logger.info(f"Stopping cluster: {cluster_id}")
            
            async with self._get_cluster_lock(cluster_id):
                try:
                    # Verify cluster exists
                    await self.registry.get_cluster(cluster_id)
                    
                    # Get cluster manager
                    cluster_manager = self._cluster_managers.get(cluster_id)
                    if not cluster_manager:
                        logger.warning(f"No cluster manager found for {cluster_id}, cluster may already be stopped")
                        await self.registry.update_cluster_status(cluster_id, ServiceStatus.STOPPED)
                        return True
                    
                    # Stop cluster
                    success = await cluster_manager.stop_cluster(force=force, cleanup=cleanup, timeout=timeout)
                    
                    if success:
                        # Update registry status
                        await self.registry.update_cluster_status(cluster_id, ServiceStatus.STOPPED)
                        
                        # Remove from active managers if cleanup was requested
                        if cleanup:
                            del self._cluster_managers[cluster_id]
                    
                    logger.info(f"Stopped cluster {cluster_id} successfully")
                    counter('multi_cluster.stop.success', 1, tags={'cluster_id': cluster_id})
                    
                    return success
                    
                except Exception as e:
                    logger.error(f"Failed to stop cluster {cluster_id}: {e}")
                    counter('multi_cluster.stop.errors', 1, tags={'cluster_id': cluster_id})
                    
                    if isinstance(e, ClusterNotFoundError):
                        raise
                    else:
                        raise MultiClusterManagerError(
                            f"Failed to stop cluster {cluster_id}: {e}",
                            cluster_id=cluster_id,
                            cause=e
                        )
    
    async def restart_cluster(self, cluster_id: str, force: bool = False, timeout: int = 60) -> ServiceStatus:
        """Restart a specific cluster.
        
        Args:
            cluster_id: ID of cluster to restart
            force: Force restart even if cluster is healthy
            timeout: Restart timeout in seconds
            
        Returns:
            Cluster status after restart
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
            ClusterStartupError: If cluster fails to restart
        """
        with LogContext("restart_cluster", cluster_id=cluster_id, force=force, timeout=timeout):
            logger.info(f"Restarting cluster: {cluster_id}")
            
            async with self._get_cluster_lock(cluster_id):
                try:
                    # Stop cluster first
                    await self.stop_cluster(cluster_id, force=True, timeout=30)
                    
                    # Wait a moment for graceful shutdown
                    await asyncio.sleep(5)
                    
                    # Start cluster
                    status = await self.start_cluster(cluster_id, force=True, timeout=timeout)
                    
                    logger.info(f"Restarted cluster {cluster_id} successfully")
                    counter('multi_cluster.restart.success', 1, tags={'cluster_id': cluster_id})
                    
                    return status
                    
                except Exception as e:
                    logger.error(f"Failed to restart cluster {cluster_id}: {e}")
                    counter('multi_cluster.restart.errors', 1, tags={'cluster_id': cluster_id})
                    
                    # Create restart error for recovery system
                    restart_error = ClusterStartupError(
                        message=f"Failed to restart cluster {cluster_id}: {str(e)}",
                        cluster_id=cluster_id,
                        context={"operation": "restart", "force": force, "timeout": timeout},
                        cause=e
                    )
                    
                    # Mark cluster as unavailable for degradation
                    await graceful_degradation_service.handle_cluster_unavailable(
                        cluster_id, f"Restart failed: {str(e)}"
                    )
                    
                    raise restart_error
    
    async def delete_cluster(self, cluster_id: str, force: bool = False, cleanup_data: bool = True) -> bool:
        """Delete a cluster and all its data.
        
        Args:
            cluster_id: ID of cluster to delete
            force: Force deletion even if cluster is running
            cleanup_data: Whether to remove data directories
            
        Returns:
            True if successfully deleted
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
            MultiClusterManagerError: If cluster fails to delete
        """
        with LogContext("delete_cluster", cluster_id=cluster_id, force=force, cleanup_data=cleanup_data):
            logger.info(f"Deleting cluster: {cluster_id}")
            
            async with self._get_cluster_lock(cluster_id):
                try:
                    # Verify cluster exists
                    cluster = await self.registry.get_cluster(cluster_id)
                    
                    # Stop cluster if running
                    current_status = await self.registry.get_cluster_status(cluster_id)
                    if current_status in [ServiceStatus.RUNNING, ServiceStatus.STARTING]:
                        if not force:
                            raise MultiClusterManagerError(
                                f"Cluster {cluster_id} is running. Stop it first or use force=True",
                                cluster_id=cluster_id
                            )
                        
                        # Force stop the cluster
                        await self.stop_cluster(cluster_id, force=True, cleanup=True)
                    
                    # Destroy cluster instance
                    await self.factory.destroy_cluster_instance(cluster_id, cleanup_data=cleanup_data)
                    
                    # Remove from active managers
                    if cluster_id in self._cluster_managers:
                        del self._cluster_managers[cluster_id]
                    
                    # Unregister from registry
                    await self.registry.unregister_cluster(cluster_id, force=True)
                    
                    logger.info(f"Deleted cluster {cluster_id} successfully")
                    counter('multi_cluster.delete.success', 1, tags={'cluster_id': cluster_id})
                    
                    return True
                    
                except Exception as e:
                    logger.error(f"Failed to delete cluster {cluster_id}: {e}")
                    counter('multi_cluster.delete.errors', 1, tags={'cluster_id': cluster_id})
                    
                    if isinstance(e, ClusterNotFoundError):
                        raise
                    else:
                        raise MultiClusterManagerError(
                            f"Failed to delete cluster {cluster_id}: {e}",
                            cluster_id=cluster_id,
                            cause=e
                        )
    
    async def get_cluster_status(self, cluster_id: str) -> ServiceStatus:
        """Get current status of a specific cluster.
        
        Args:
            cluster_id: ID of cluster
            
        Returns:
            Current cluster status
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
        """
        # Verify cluster exists
        await self.registry.get_cluster(cluster_id)
        
        # Get status from cluster manager if available
        cluster_manager = self._cluster_managers.get(cluster_id)
        if cluster_manager:
            try:
                status = await cluster_manager.get_status()
                
                # Update registry status if different
                registry_status = await self.registry.get_cluster_status(cluster_id)
                if status.status != registry_status:
                    await self.registry.update_cluster_status(cluster_id, status.status)
                
                return status.status
            except Exception as e:
                logger.warning(f"Failed to get status from cluster manager for {cluster_id}: {e}")
        
        # Fall back to registry status
        return await self.registry.get_cluster_status(cluster_id)
    
    async def get_all_cluster_status(self) -> Dict[str, ServiceStatus]:
        """Get status of all registered clusters.
        
        Returns:
            Dictionary mapping cluster IDs to their status
        """
        try:
            # Get all clusters from registry
            clusters = await self.registry.list_clusters()
            status_map = {}
            
            # Get status for each cluster
            for cluster in clusters:
                try:
                    status = await self.get_cluster_status(cluster.id)
                    status_map[cluster.id] = status
                except Exception as e:
                    logger.warning(f"Failed to get status for cluster {cluster.id}: {e}")
                    status_map[cluster.id] = ServiceStatus.ERROR
            
            return status_map
            
        except Exception as e:
            logger.error(f"Failed to get all cluster status: {e}")
            return {}
    
    async def list_clusters(self, 
                          status_filter: Optional[ServiceStatus] = None,
                          environment_filter: Optional[str] = None,
                          tag_filter: Optional[Dict[str, str]] = None) -> List[ClusterSummary]:
        """List all clusters with optional filtering.
        
        Args:
            status_filter: Filter by cluster status
            environment_filter: Filter by environment
            tag_filter: Filter by tags
            
        Returns:
            List of cluster summaries
        """
        try:
            # Get clusters from registry
            clusters = await self.registry.list_clusters(
                status_filter=status_filter,
                environment_filter=environment_filter,
                tag_filter=tag_filter
            )
            
            summaries = []
            for cluster in clusters:
                try:
                    # Get current status
                    current_status = await self.get_cluster_status(cluster.id)
                    
                    # Get endpoints if cluster is running
                    endpoints = {}
                    if current_status == ServiceStatus.RUNNING:
                        endpoints = await self.factory.get_cluster_endpoints(cluster)
                    
                    summary = ClusterSummary(
                        id=cluster.id,
                        name=cluster.name,
                        environment=cluster.environment,
                        status=current_status,
                        created_at=cluster.created_at,
                        last_started=cluster.last_started,
                        endpoints=endpoints,
                        tags=cluster.tags
                    )
                    summaries.append(summary)
                    
                except Exception as e:
                    logger.warning(f"Failed to create summary for cluster {cluster.id}: {e}")
                    # Create error summary
                    summary = ClusterSummary(
                        id=cluster.id,
                        name=cluster.name,
                        environment=cluster.environment,
                        status=ServiceStatus.ERROR,
                        created_at=cluster.created_at,
                        last_started=cluster.last_started,
                        endpoints={},
                        tags=cluster.tags
                    )
                    summaries.append(summary)
            
            return summaries
            
        except Exception as e:
            logger.error(f"Failed to list clusters: {e}")
            return []
    
    async def start_multiple_clusters(self, cluster_ids: List[str], 
                                    max_concurrent: int = 3,
                                    timeout: int = 60) -> Dict[str, ServiceStatus]:
        """Start multiple clusters concurrently.
        
        Args:
            cluster_ids: List of cluster IDs to start
            max_concurrent: Maximum number of concurrent starts
            timeout: Startup timeout per cluster in seconds
            
        Returns:
            Dictionary mapping cluster IDs to their final status
        """
        logger.info(f"Starting {len(cluster_ids)} clusters concurrently (max_concurrent={max_concurrent})")
        
        semaphore = asyncio.Semaphore(max_concurrent)
        results = {}
        
        async def start_single_cluster(cluster_id: str) -> None:
            async with semaphore:
                try:
                    status = await self.start_cluster(cluster_id, timeout=timeout)
                    results[cluster_id] = status
                except Exception as e:
                    logger.error(f"Failed to start cluster {cluster_id}: {e}")
                    results[cluster_id] = ServiceStatus.ERROR
        
        # Start all clusters concurrently
        tasks = [start_single_cluster(cluster_id) for cluster_id in cluster_ids]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"Completed starting {len(cluster_ids)} clusters")
        return results
    
    async def stop_multiple_clusters(self, cluster_ids: List[str], 
                                   max_concurrent: int = 5,
                                   force: bool = False,
                                   timeout: int = 30) -> Dict[str, bool]:
        """Stop multiple clusters concurrently.
        
        Args:
            cluster_ids: List of cluster IDs to stop
            max_concurrent: Maximum number of concurrent stops
            force: Force stop containers
            timeout: Stop timeout per cluster in seconds
            
        Returns:
            Dictionary mapping cluster IDs to success status
        """
        logger.info(f"Stopping {len(cluster_ids)} clusters concurrently (max_concurrent={max_concurrent})")
        
        semaphore = asyncio.Semaphore(max_concurrent)
        results = {}
        
        async def stop_single_cluster(cluster_id: str) -> None:
            async with semaphore:
                try:
                    success = await self.stop_cluster(cluster_id, force=force, timeout=timeout)
                    results[cluster_id] = success
                except Exception as e:
                    logger.error(f"Failed to stop cluster {cluster_id}: {e}")
                    results[cluster_id] = False
        
        # Stop all clusters concurrently
        tasks = [stop_single_cluster(cluster_id) for cluster_id in cluster_ids]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"Completed stopping {len(cluster_ids)} clusters")
        return results
    
    async def get_cluster_health(self, cluster_id: str) -> Dict[str, Any]:
        """Get detailed health information for a cluster.
        
        Args:
            cluster_id: ID of cluster
            
        Returns:
            Health information dictionary
            
        Raises:
            ClusterNotFoundError: If cluster ID is not found
        """
        # Verify cluster exists
        await self.registry.get_cluster(cluster_id)
        
        health_info = {
            "cluster_id": cluster_id,
            "timestamp": datetime.utcnow().isoformat(),
            "overall_health": "unknown",
            "registry_health": {},
            "manager_health": {},
            "factory_health": {}
        }
        
        try:
            # Get registry health
            registry_health = await self.registry.validate_cluster_health(cluster_id)
            health_info["registry_health"] = registry_health
            
            # Get manager health if available
            cluster_manager = self._cluster_managers.get(cluster_id)
            if cluster_manager:
                manager_status = await cluster_manager.get_status()
                health_info["manager_health"] = {
                    "status": manager_status.status.value,
                    "services": {name: service.dict() for name, service in manager_status.services.items()},
                    "uptime": manager_status.uptime,
                    "endpoints": manager_status.endpoints
                }
            
            # Get factory validation
            cluster = await self.registry.get_cluster(cluster_id)
            factory_validation = await self.factory.validate_cluster_resources(cluster)
            health_info["factory_health"] = factory_validation
            
            # Determine overall health
            if (registry_health.get("overall_health") == "healthy" and
                factory_validation.get("valid", False) and
                (not cluster_manager or health_info["manager_health"]["status"] == "running")):
                health_info["overall_health"] = "healthy"
            elif any("error" in str(h).lower() for h in [registry_health, factory_validation]):
                health_info["overall_health"] = "unhealthy"
            else:
                health_info["overall_health"] = "degraded"
            
            return health_info
            
        except Exception as e:
            logger.error(f"Failed to get cluster health for {cluster_id}: {e}")
            health_info["overall_health"] = "error"
            health_info["error"] = str(e)
            return health_info
    
    async def get_manager_stats(self) -> Dict[str, Any]:
        """Get multi-cluster manager statistics.
        
        Returns:
            Statistics dictionary
        """
        try:
            # Get registry stats
            registry_stats = await self.registry.get_registry_stats()
            
            # Get active manager count
            active_managers = len(self._cluster_managers)
            
            # Get operation stats
            operation_stats = {
                "total_operations": len(self._active_operations),
                "running_operations": len([op for op in self._active_operations.values() 
                                         if op.status == OperationStatus.RUNNING]),
                "completed_operations": len([op for op in self._active_operations.values() 
                                           if op.status == OperationStatus.COMPLETED]),
                "failed_operations": len([op for op in self._active_operations.values() 
                                        if op.status == OperationStatus.FAILED])
            }
            
            return {
                "manager_info": {
                    "active_cluster_managers": active_managers,
                    "health_monitoring_active": self._health_check_task is not None and not self._health_check_task.done(),
                    "operation_locks": len(self._operation_locks),
                    "cluster_locks": len(self._cluster_locks)
                },
                "registry_stats": registry_stats,
                "operation_stats": operation_stats,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get manager stats: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    # Private helper methods
    
    def _get_cluster_lock(self, cluster_id: str) -> asyncio.Lock:
        """Get or create a lock for a specific cluster."""
        if cluster_id not in self._cluster_locks:
            self._cluster_locks[cluster_id] = asyncio.Lock()
        return self._cluster_locks[cluster_id]
    
    async def _start_cluster_internal(self, cluster_id: str, cluster_manager: ClusterManager, 
                                    force: bool = False, timeout: int = 60) -> Any:
        """Internal method to start a cluster."""
        # Update status to starting
        await self.registry.update_cluster_status(cluster_id, ServiceStatus.STARTING)
        
        try:
            # Start the cluster
            status = await cluster_manager.start_cluster(force=force, timeout=timeout)
            
            # Update registry status
            await self.registry.update_cluster_status(cluster_id, status.status)
            
            return status
            
        except Exception as e:
            # Update status to error
            await self.registry.update_cluster_status(cluster_id, ServiceStatus.ERROR)
            raise
    
    async def _cleanup_failed_cluster_creation(self, cluster_id: str) -> None:
        """Cleanup resources after failed cluster creation."""
        try:
            # Remove from active managers
            if cluster_id in self._cluster_managers:
                del self._cluster_managers[cluster_id]
            
            # Try to destroy cluster instance
            try:
                await self.factory.destroy_cluster_instance(cluster_id, cleanup_data=True)
            except Exception as e:
                logger.warning(f"Failed to destroy cluster instance during cleanup for {cluster_id}: {e}")
            
            # Try to unregister from registry
            try:
                await self.registry.unregister_cluster(cluster_id, force=True)
            except Exception as e:
                logger.warning(f"Failed to unregister cluster during cleanup for {cluster_id}: {e}")
            
            logger.info(f"Cleaned up failed cluster creation for {cluster_id}")
            
        except Exception as e:
            logger.error(f"Failed to cleanup after failed cluster creation for {cluster_id}: {e}")
    
    async def _start_health_monitoring(self) -> None:
        """Start background health monitoring task."""
        if self._health_check_task and not self._health_check_task.done():
            return  # Already running
        
        self._health_check_task = asyncio.create_task(self._health_monitoring_loop())
        logger.info("Started health monitoring task")
    
    async def _health_monitoring_loop(self) -> None:
        """Background health monitoring loop."""
        logger.info("Health monitoring loop started")
        
        try:
            while True:
                try:
                    await self._perform_health_checks()
                    await asyncio.sleep(self._health_check_interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in health monitoring loop: {e}")
                    await asyncio.sleep(self._health_check_interval)
        
        except asyncio.CancelledError:
            logger.info("Health monitoring loop cancelled")
        except Exception as e:
            logger.error(f"Health monitoring loop failed: {e}")
    
    async def _perform_health_checks(self) -> None:
        """Perform health checks on all active clusters."""
        try:
            # Get all registered clusters
            clusters = await self.registry.list_clusters()
            
            for cluster in clusters:
                try:
                    # Check cluster status
                    current_status = await self.get_cluster_status(cluster.id)
                    
                    # Update metrics
                    gauge('multi_cluster.cluster.status', 
                          1 if current_status == ServiceStatus.RUNNING else 0,
                          tags={'cluster_id': cluster.id, 'status': current_status.value})
                    
                except Exception as e:
                    logger.warning(f"Health check failed for cluster {cluster.id}: {e}")
                    
                    # Update status to error if we can't determine it
                    try:
                        await self.registry.update_cluster_status(cluster.id, ServiceStatus.ERROR)
                    except Exception:
                        pass  # Ignore registry update errors during health checks
            
            # Update overall metrics
            gauge('multi_cluster.active_managers', len(self._cluster_managers))
            gauge('multi_cluster.total_clusters', len(clusters))
            
        except Exception as e:
            logger.error(f"Failed to perform health checks: {e}")
    
    async def _initialize_error_handling(self) -> None:
        """Initialize error recovery and degradation services."""
        try:
            # Get list of all clusters for health monitoring
            clusters = await self.registry.list_clusters()
            cluster_ids = [cluster.id for cluster in clusters]
            
            # Start cluster health monitoring
            if cluster_ids:
                await cluster_health_monitor.start_monitoring(cluster_ids)
                logger.info(f"Started health monitoring for {len(cluster_ids)} clusters")
            
            # Set up health monitoring listener for degradation
            def health_change_listener(cluster_id: str, health_state):
                """Handle health state changes for degradation."""
                if not health_state.is_healthy():
                    # Schedule degradation handling
                    asyncio.create_task(
                        graceful_degradation_service.handle_cluster_unavailable(
                            cluster_id, 
                            f"Health check failed: {health_state.overall_status.value}"
                        )
                    )
                else:
                    # Schedule restoration from degradation
                    asyncio.create_task(
                        graceful_degradation_service.handle_cluster_available(cluster_id)
                    )
            
            cluster_health_monitor.add_health_listener(health_change_listener)
            
            # Register custom recovery handlers
            self._register_custom_recovery_handlers()
            
            # Set up degradation rules for clusters
            await self._setup_degradation_rules(clusters)
            
            logger.info("Error handling and recovery systems initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize error handling: {e}")
            # Don't fail initialization if error handling setup fails
    
    def _register_custom_recovery_handlers(self) -> None:
        """Register custom recovery handlers for multi-cluster operations."""
        
        async def custom_restart_handler(error: MultiClusterError) -> bool:
            """Custom cluster restart handler."""
            if not error.cluster_id:
                return False
            
            try:
                # Use our own restart method
                await self.restart_cluster(error.cluster_id)
                return True
            except Exception as e:
                logger.error(f"Custom restart handler failed: {e}")
                return False
        
        async def custom_recreate_handler(error: MultiClusterError) -> bool:
            """Custom cluster recreation handler."""
            if not error.cluster_id:
                return False
            
            try:
                # Get cluster definition
                cluster = await self.registry.get_cluster(error.cluster_id)
                
                # Delete and recreate
                await self.delete_cluster(error.cluster_id, force=True)
                await asyncio.sleep(10)  # Wait for cleanup
                
                # Recreate with same definition
                await self.create_cluster(cluster, auto_start=True)
                return True
            except Exception as e:
                logger.error(f"Custom recreate handler failed: {e}")
                return False
        
        # Register handlers
        from src.exceptions.multi_cluster_exceptions import RecoveryAction
        error_recovery_manager.register_recovery_handler(
            RecoveryAction.RESTART_CLUSTER, 
            custom_restart_handler
        )
        error_recovery_manager.register_recovery_handler(
            RecoveryAction.RECREATE_CLUSTER, 
            custom_recreate_handler
        )
    
    async def _setup_degradation_rules(self, clusters: List[ClusterDefinition]) -> None:
        """Set up degradation rules for clusters."""
        from src.services.graceful_degradation import DegradationRule, DegradationLevel, ServiceMode
        
        for cluster in clusters:
            # Create degradation rule based on environment
            if cluster.environment == "production":
                rule = DegradationRule(
                    cluster_id=cluster.id,
                    unavailable_threshold_minutes=2,
                    degradation_level=DegradationLevel.MINIMAL,
                    service_mode=ServiceMode.READ_ONLY,
                    disabled_features={"advanced_features"}
                )
            elif cluster.environment == "staging":
                rule = DegradationRule(
                    cluster_id=cluster.id,
                    unavailable_threshold_minutes=5,
                    degradation_level=DegradationLevel.MODERATE,
                    service_mode=ServiceMode.ESSENTIAL_ONLY,
                    disabled_features={"advanced_features", "cross_cluster_operations"}
                )
            else:  # development, testing
                rule = DegradationRule(
                    cluster_id=cluster.id,
                    unavailable_threshold_minutes=10,
                    degradation_level=DegradationLevel.SEVERE,
                    service_mode=ServiceMode.EMERGENCY,
                    disabled_features={"advanced_features", "cross_cluster_operations", "monitoring"}
                )
            
            graceful_degradation_service.register_degradation_rule(rule)
        
        logger.info(f"Set up degradation rules for {len(clusters)} clusters")