"""
Backward compatibility middleware for multi-cluster support.

This module provides backward compatibility for existing single-cluster APIs
by implementing a default cluster concept and automatic routing.
"""

import logging
from typing import Optional, Dict, Any, List
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
import asyncio
from datetime import datetime

from ..models.multi_cluster import ClusterDefinition, ClusterStatus
from ..services.multi_cluster_manager import MultiClusterManager
from ..services.cluster_factory import ClusterFactory
from ..services.template_manager import TemplateManager
from ..registry.cluster_registry import ClusterRegistry
from ..exceptions import ClusterNotFoundError, MultiClusterError

logger = logging.getLogger(__name__)

class BackwardCompatibilityManager:
    """
    Manages backward compatibility for single-cluster to multi-cluster migration.
    
    This class handles:
    - Default cluster creation and management
    - Legacy API routing to default cluster
    - Automatic migration from single-cluster setup
    - Compatibility layer for existing client code
    """
    
    DEFAULT_CLUSTER_ID = "default"
    DEFAULT_CLUSTER_NAME = "Default Cluster"
    
    def __init__(
        self,
        multi_cluster_manager: MultiClusterManager,
        cluster_registry: ClusterRegistry,
        template_manager: TemplateManager
    ):
        self.multi_cluster_manager = multi_cluster_manager
        self.cluster_registry = cluster_registry
        self.template_manager = template_manager
        self._default_cluster_initialized = False
        self._migration_lock = asyncio.Lock()
    
    async def ensure_default_cluster(self) -> bool:
        """
        Ensure default cluster exists and is available.
        
        Returns:
            bool: True if default cluster is ready, False otherwise
        """
        if self._default_cluster_initialized:
            return True
        
        async with self._migration_lock:
            if self._default_cluster_initialized:
                return True
            
            try:
                # Check if default cluster already exists
                default_cluster = await self.cluster_registry.get_cluster(self.DEFAULT_CLUSTER_ID)
                
                if default_cluster is None:
                    # Create default cluster from development template
                    await self._create_default_cluster()
                    logger.info("Created default cluster for backward compatibility")
                
                # Ensure default cluster is running
                status = await self.multi_cluster_manager.get_cluster_status(self.DEFAULT_CLUSTER_ID)
                if status.status != "running":
                    await self.multi_cluster_manager.start_cluster(self.DEFAULT_CLUSTER_ID)
                    logger.info("Started default cluster for backward compatibility")
                
                self._default_cluster_initialized = True
                return True
                
            except Exception as e:
                logger.error(f"Failed to ensure default cluster: {e}")
                return False
    
    async def _create_default_cluster(self) -> ClusterDefinition:
        """Create the default cluster using development template."""
        try:
            # Get development template
            dev_template = await self.template_manager.get_template("development")
            if dev_template is None:
                # Create basic development template if it doesn't exist
                dev_template = self.template_manager.get_builtin_templates()[0]  # First builtin template
            
            # Create cluster definition from template
            cluster_definition = await self.template_manager.apply_template(
                dev_template.id,
                overrides={
                    "id": self.DEFAULT_CLUSTER_ID,
                    "name": self.DEFAULT_CLUSTER_NAME,
                    "description": "Default cluster for backward compatibility with single-cluster APIs",
                    "environment": "development",
                    "tags": {
                        "type": "default",
                        "compatibility": "legacy",
                        "auto_created": "true"
                    }
                }
            )
            
            # Register the cluster
            await self.cluster_registry.register_cluster(cluster_definition)
            
            # Create and start the cluster
            await self.multi_cluster_manager.create_cluster(cluster_definition)
            
            return cluster_definition
            
        except Exception as e:
            logger.error(f"Failed to create default cluster: {e}")
            raise MultiClusterError(f"Could not create default cluster: {e}")
    
    async def get_default_cluster_id(self) -> str:
        """
        Get the default cluster ID for legacy API routing.
        
        Returns:
            str: Default cluster ID
            
        Raises:
            HTTPException: If default cluster is not available
        """
        if not await self.ensure_default_cluster():
            raise HTTPException(
                status_code=503,
                detail="Default cluster is not available. Please check system status."
            )
        return self.DEFAULT_CLUSTER_ID
    
    async def migrate_single_cluster_config(self, config_data: Dict[str, Any]) -> bool:
        """
        Migrate existing single-cluster configuration to multi-cluster setup.
        
        Args:
            config_data: Existing single-cluster configuration
            
        Returns:
            bool: True if migration successful
        """
        try:
            logger.info("Starting migration from single-cluster to multi-cluster setup")
            
            # Extract configuration for default cluster
            cluster_config = {
                "id": self.DEFAULT_CLUSTER_ID,
                "name": self.DEFAULT_CLUSTER_NAME,
                "description": "Migrated from single-cluster setup",
                "environment": config_data.get("environment", "development"),
                "kafka_config": config_data.get("kafka", {}),
                "rest_proxy_config": config_data.get("rest_proxy", {}),
                "ui_config": config_data.get("ui", {}),
                "tags": {
                    "migrated": "true",
                    "migration_date": datetime.now().isoformat(),
                    "source": "single_cluster"
                }
            }
            
            # Create cluster definition
            dev_template = await self.template_manager.get_template("development")
            cluster_definition = await self.template_manager.apply_template(
                dev_template.id,
                overrides=cluster_config
            )
            
            # Register and create the cluster
            await self.cluster_registry.register_cluster(cluster_definition)
            await self.multi_cluster_manager.create_cluster(cluster_definition)
            
            logger.info("Successfully migrated single-cluster configuration")
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False
    
    def is_legacy_endpoint(self, path: str) -> bool:
        """
        Check if the request path is a legacy single-cluster endpoint.
        
        Args:
            path: Request path
            
        Returns:
            bool: True if legacy endpoint
        """
        legacy_patterns = [
            "/topics",
            "/produce",
            "/consume",
            "/consumer-groups",
            "/brokers",
            "/cluster-info",
            "/health"
        ]
        
        # Check if path matches legacy patterns (not starting with /clusters/)
        if path.startswith("/clusters/"):
            return False
        
        for pattern in legacy_patterns:
            if path.startswith(pattern):
                return True
        
        return False
    
    def convert_legacy_path_to_cluster_path(self, path: str, cluster_id: str) -> str:
        """
        Convert legacy API path to cluster-specific path.
        
        Args:
            path: Original legacy path
            cluster_id: Target cluster ID
            
        Returns:
            str: Converted cluster-specific path
        """
        # Remove leading slash if present
        clean_path = path.lstrip("/")
        
        # Convert to cluster-specific path
        return f"/clusters/{cluster_id}/{clean_path}"
    
    async def handle_legacy_request(self, request: Request) -> Optional[str]:
        """
        Handle legacy API request by routing to default cluster.
        
        Args:
            request: FastAPI request object
            
        Returns:
            Optional[str]: Cluster ID to route to, or None if not a legacy request
        """
        path = request.url.path
        
        if not self.is_legacy_endpoint(path):
            return None
        
        # Ensure default cluster is available
        try:
            cluster_id = await self.get_default_cluster_id()
            logger.debug(f"Routing legacy request {path} to cluster {cluster_id}")
            return cluster_id
        except HTTPException:
            logger.error(f"Cannot route legacy request {path}: default cluster unavailable")
            raise
    
    async def get_cluster_endpoints_info(self, cluster_id: str) -> Dict[str, str]:
        """
        Get endpoint information for a cluster.
        
        Args:
            cluster_id: Cluster ID
            
        Returns:
            Dict[str, str]: Endpoint URLs for the cluster
        """
        try:
            cluster = await self.cluster_registry.get_cluster(cluster_id)
            if cluster is None:
                raise ClusterNotFoundError(cluster_id)
            
            # Build endpoint URLs based on port allocation
            port_allocation = cluster.port_allocation
            base_url = "http://localhost"
            
            endpoints = {
                "kafka": f"{base_url}:{port_allocation.kafka_port}",
                "rest_proxy": f"{base_url}:{port_allocation.rest_proxy_port}",
                "ui": f"{base_url}:{port_allocation.ui_port}"
            }
            
            if port_allocation.jmx_port:
                endpoints["jmx"] = f"{base_url}:{port_allocation.jmx_port}"
            
            return endpoints
            
        except Exception as e:
            logger.error(f"Failed to get endpoints for cluster {cluster_id}: {e}")
            return {}
    
    async def get_compatibility_status(self) -> Dict[str, Any]:
        """
        Get backward compatibility status information.
        
        Returns:
            Dict[str, Any]: Compatibility status details
        """
        try:
            default_cluster = await self.cluster_registry.get_cluster(self.DEFAULT_CLUSTER_ID)
            default_cluster_status = None
            
            if default_cluster:
                status = await self.multi_cluster_manager.get_cluster_status(self.DEFAULT_CLUSTER_ID)
                default_cluster_status = {
                    "id": self.DEFAULT_CLUSTER_ID,
                    "name": default_cluster.name,
                    "status": status.status,
                    "endpoints": await self.get_cluster_endpoints_info(self.DEFAULT_CLUSTER_ID)
                }
            
            return {
                "backward_compatibility_enabled": True,
                "default_cluster_available": default_cluster is not None,
                "default_cluster_initialized": self._default_cluster_initialized,
                "default_cluster": default_cluster_status,
                "legacy_endpoints_supported": [
                    "/topics",
                    "/produce", 
                    "/consume",
                    "/consumer-groups",
                    "/brokers",
                    "/cluster-info",
                    "/health"
                ]
            }
            
        except Exception as e:
            logger.error(f"Failed to get compatibility status: {e}")
            return {
                "backward_compatibility_enabled": False,
                "error": str(e)
            }
    
    async def get_legacy_service_catalog(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Get service catalog for legacy API compatibility.
        
        Args:
            force_refresh: Force refresh of cached data
            
        Returns:
            Dict[str, Any]: Legacy service catalog data
        """
        try:
            cluster_id = await self.get_default_cluster_id()
            
            # Get cluster status
            status = await self.multi_cluster_manager.get_cluster_status(cluster_id)
            
            # Get cluster endpoints
            endpoints = await self.get_cluster_endpoints_info(cluster_id)
            
            # Build legacy catalog response
            return {
                "cluster_status": status.status,
                "broker_count": 1,  # Default for single cluster compatibility
                "version": "7.4.0",
                "endpoints": endpoints,
                "services": {
                    "kafka": {
                        "status": status.status,
                        "url": endpoints.get("kafka", "")
                    },
                    "rest_proxy": {
                        "status": status.status,
                        "url": endpoints.get("rest_proxy", "")
                    },
                    "ui": {
                        "status": status.status,
                        "url": endpoints.get("ui", "")
                    }
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get legacy service catalog: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def route_legacy_cluster_operation(self, operation: str, **kwargs) -> Any:
        """
        Route legacy cluster operations to default cluster.
        
        Args:
            operation: Operation type (start, stop, status)
            **kwargs: Operation parameters
            
        Returns:
            Any: Operation result
        """
        try:
            cluster_id = await self.get_default_cluster_id()
            
            if operation == "start":
                await self.multi_cluster_manager.start_cluster(cluster_id)
                status = await self.multi_cluster_manager.get_cluster_status(cluster_id)
                return status.status
            elif operation == "stop":
                success = await self.multi_cluster_manager.stop_cluster(
                    cluster_id,
                    cleanup=kwargs.get("cleanup", False)
                )
                return success
            elif operation == "status":
                status = await self.multi_cluster_manager.get_cluster_status(cluster_id)
                return status.status
            else:
                raise ValueError(f"Unknown operation: {operation}")
                
        except Exception as e:
            logger.error(f"Failed to route legacy cluster operation {operation}: {e}")
            raise
    
    async def get_default_cluster_manager(self) -> Dict[str, Any]:
        """
        Get cluster-specific managers for the default cluster.
        
        Returns:
            Dict[str, Any]: Dictionary containing cluster-specific managers
        """
        try:
            cluster_id = await self.get_default_cluster_id()
            
            # Get cluster definition
            cluster = await self.cluster_registry.get_cluster(cluster_id)
            if cluster is None:
                raise ClusterNotFoundError(cluster_id)
            
            # Create cluster-specific managers
            # Note: This is a simplified implementation for backward compatibility
            # In a full implementation, you would create actual cluster-specific managers
            from ..services.topic_manager import TopicManager
            from ..services.message_manager import MessageManager
            
            topic_manager = TopicManager()
            message_manager = MessageManager()
            
            return {
                "topic_manager": topic_manager,
                "message_manager": message_manager,
                "cluster": cluster
            }
            
        except Exception as e:
            logger.error(f"Failed to get default cluster manager: {e}")
            raise


class LegacyAPIMiddleware:
    """
    Middleware to handle legacy API requests and route them to default cluster.
    """
    
    def __init__(self, compatibility_manager: BackwardCompatibilityManager):
        self.compatibility_manager = compatibility_manager
    
    async def __call__(self, request: Request, call_next):
        """
        Process request and handle legacy API routing.
        
        Args:
            request: FastAPI request object
            call_next: Next middleware/handler in chain
            
        Returns:
            Response object
        """
        try:
            # Check if this is a legacy API request
            cluster_id = await self.compatibility_manager.handle_legacy_request(request)
            
            if cluster_id:
                # Modify request to include cluster context
                request.state.cluster_id = cluster_id
                request.state.is_legacy_request = True
                
                # Convert path to cluster-specific path for internal routing
                original_path = request.url.path
                cluster_path = self.compatibility_manager.convert_legacy_path_to_cluster_path(
                    original_path, cluster_id
                )
                
                # Update request path for internal routing
                request.scope["path"] = cluster_path
                request.scope["raw_path"] = cluster_path.encode('utf-8')
                
                logger.debug(f"Converted legacy path {original_path} to {cluster_path}")
            
            # Continue with request processing
            response = await call_next(request)
            return response
            
        except HTTPException:
            # Re-raise HTTP exceptions
            raise
        except Exception as e:
            logger.error(f"Error in legacy API middleware: {e}")
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal server error in backward compatibility layer",
                    "detail": str(e)
                }
            )


# Utility functions for backward compatibility

async def ensure_backward_compatibility(
    multi_cluster_manager: MultiClusterManager,
    cluster_registry: ClusterRegistry,
    template_manager: TemplateManager
) -> BackwardCompatibilityManager:
    """
    Initialize backward compatibility manager and ensure default cluster exists.
    
    Args:
        multi_cluster_manager: Multi-cluster manager instance
        cluster_registry: Cluster registry instance
        template_manager: Template manager instance
        
    Returns:
        BackwardCompatibilityManager: Initialized compatibility manager
    """
    compatibility_manager = BackwardCompatibilityManager(
        multi_cluster_manager,
        cluster_registry,
        template_manager
    )
    
    # Ensure default cluster is available
    await compatibility_manager.ensure_default_cluster()
    
    return compatibility_manager


def create_legacy_api_middleware(compatibility_manager: BackwardCompatibilityManager) -> LegacyAPIMiddleware:
    """
    Create legacy API middleware instance.
    
    Args:
        compatibility_manager: Backward compatibility manager
        
    Returns:
        LegacyAPIMiddleware: Middleware instance
    """
    return LegacyAPIMiddleware(compatibility_manager)


# Global instance for access from routes
_global_compatibility_manager: Optional[BackwardCompatibilityManager] = None


def set_global_compatibility_manager(manager: BackwardCompatibilityManager) -> None:
    """Set the global compatibility manager instance."""
    global _global_compatibility_manager
    _global_compatibility_manager = manager


def get_backward_compatibility_manager() -> BackwardCompatibilityManager:
    """
    Get the global backward compatibility manager instance.
    
    Returns:
        BackwardCompatibilityManager: The global instance
        
    Raises:
        RuntimeError: If the manager is not initialized
    """
    if _global_compatibility_manager is None:
        raise RuntimeError("Backward compatibility manager not initialized")
    return _global_compatibility_manager