"""
Multi-cluster service catalog for aggregating information from multiple clusters.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

from ..models.base import ServiceStatus
from ..models.catalog import CatalogResponse, APIEndpoint, ServiceInfo
from ..models.multi_cluster import ClusterSummary
from ..services.service_catalog import ServiceCatalog
from ..services.multi_cluster_manager import MultiClusterManager
from ..services.topic_manager import TopicManager
from ..services.message_manager import MessageManager
from ..exceptions import (
    ServiceCatalogError,
    CatalogRefreshError,
    ServiceDiscoveryError
)
from ..utils.logging import get_logger

logger = get_logger(__name__)


class MultiClusterServiceCatalog:
    """Service catalog that aggregates information from multiple clusters."""
    
    def __init__(self, multi_cluster_manager: MultiClusterManager, cache_ttl: int = 30):
        """Initialize the multi-cluster service catalog.
        
        Args:
            multi_cluster_manager: Multi-cluster manager instance
            cache_ttl: Cache time-to-live in seconds
        """
        self.multi_cluster_manager = multi_cluster_manager
        self.cache_ttl = cache_ttl
        
        # Cache for cluster-specific catalogs
        self._cluster_catalogs: Dict[str, CatalogResponse] = {}
        self._cache_timestamps: Dict[str, float] = {}
        
        # API endpoints for multi-cluster operations
        self._multi_cluster_endpoints = self._build_multi_cluster_endpoints()
    
    async def get_aggregated_catalog(self, force_refresh: bool = False) -> Dict[str, Any]:
        """Get aggregated catalog from all clusters.
        
        Args:
            force_refresh: Force refresh of cached data
            
        Returns:
            Aggregated catalog information
            
        Raises:
            CatalogRefreshError: If catalog generation fails
        """
        logger.debug("Generating aggregated multi-cluster catalog...")
        
        try:
            # Get all cluster summaries
            clusters = await self.multi_cluster_manager.list_clusters()
            
            # Get catalog for each cluster
            cluster_catalogs = {}
            cluster_errors = {}
            
            for cluster in clusters:
                try:
                    catalog = await self._get_cluster_catalog(cluster.id, force_refresh)
                    cluster_catalogs[cluster.id] = catalog
                except Exception as e:
                    logger.warning(f"Failed to get catalog for cluster {cluster.id}: {e}")
                    cluster_errors[cluster.id] = str(e)
            
            # Aggregate information
            aggregated_info = {
                "timestamp": datetime.utcnow().isoformat(),
                "total_clusters": len(clusters),
                "active_clusters": len([c for c in clusters if c.status == ServiceStatus.RUNNING]),
                "clusters": {
                    cluster.id: {
                        "name": cluster.name,
                        "environment": cluster.environment.value,
                        "status": cluster.status.value,
                        "endpoints": cluster.endpoints,
                        "tags": cluster.tags,
                        "created_at": cluster.created_at.isoformat(),
                        "last_started": cluster.last_started.isoformat() if cluster.last_started else None
                    }
                    for cluster in clusters
                },
                "cluster_catalogs": cluster_catalogs,
                "cluster_errors": cluster_errors,
                "available_apis": self._multi_cluster_endpoints,
                "aggregated_stats": await self._get_aggregated_stats(clusters, cluster_catalogs)
            }
            
            logger.debug(f"Generated aggregated catalog for {len(clusters)} clusters")
            return aggregated_info
            
        except Exception as e:
            logger.error(f"Failed to generate aggregated catalog: {e}")
            raise CatalogRefreshError(
                reason=f"Failed to generate aggregated catalog: {e}",
                cause=e
            )
    
    async def get_cluster_catalog(self, cluster_id: str, force_refresh: bool = False) -> CatalogResponse:
        """Get catalog for a specific cluster.
        
        Args:
            cluster_id: ID of the cluster
            force_refresh: Force refresh of cached data
            
        Returns:
            CatalogResponse for the specific cluster
            
        Raises:
            ServiceDiscoveryError: If cluster catalog cannot be retrieved
        """
        try:
            return await self._get_cluster_catalog(cluster_id, force_refresh)
        except Exception as e:
            logger.error(f"Failed to get catalog for cluster {cluster_id}: {e}")
            raise ServiceDiscoveryError(
                service_name=f"cluster-{cluster_id}",
                reason=str(e),
                cause=e
            )
    
    async def get_cluster_services(self, cluster_id: str) -> Dict[str, ServiceInfo]:
        """Get service information for a specific cluster.
        
        Args:
            cluster_id: ID of the cluster
            
        Returns:
            Dictionary mapping service names to ServiceInfo objects
            
        Raises:
            ServiceDiscoveryError: If cluster services cannot be retrieved
        """
        try:
            catalog = await self._get_cluster_catalog(cluster_id)
            return catalog.services
        except Exception as e:
            logger.error(f"Failed to get services for cluster {cluster_id}: {e}")
            raise ServiceDiscoveryError(
                service_name=f"cluster-{cluster_id}-services",
                reason=str(e),
                cause=e
            )
    
    async def get_cluster_topics(self, cluster_id: str) -> List[Dict[str, Any]]:
        """Get topics for a specific cluster.
        
        Args:
            cluster_id: ID of the cluster
            
        Returns:
            List of topic information dictionaries
            
        Raises:
            ServiceDiscoveryError: If cluster topics cannot be retrieved
        """
        try:
            catalog = await self._get_cluster_catalog(cluster_id)
            return [
                {
                    "name": topic.name,
                    "partitions": topic.partitions,
                    "replication_factor": topic.replication_factor,
                    "size_bytes": topic.size_bytes,
                    "config": topic.config,
                    "cluster_id": cluster_id
                }
                for topic in catalog.topics
            ]
        except Exception as e:
            logger.error(f"Failed to get topics for cluster {cluster_id}: {e}")
            raise ServiceDiscoveryError(
                service_name=f"cluster-{cluster_id}-topics",
                reason=str(e),
                cause=e
            )
    
    async def create_cluster_topic_manager(self, cluster_id: str) -> TopicManager:
        """Create a topic manager for a specific cluster.
        
        Args:
            cluster_id: ID of the cluster
            
        Returns:
            TopicManager instance configured for the cluster
            
        Raises:
            ServiceDiscoveryError: If cluster configuration cannot be retrieved
        """
        try:
            # Get cluster definition
            cluster = await self.multi_cluster_manager.registry.get_cluster(cluster_id)
            
            # Get cluster endpoints
            endpoints = await self.multi_cluster_manager.factory.get_cluster_endpoints(cluster)
            
            # Extract Kafka bootstrap servers
            kafka_endpoint = endpoints.get("kafka")
            if not kafka_endpoint:
                raise ServiceDiscoveryError(
                    service_name=f"cluster-{cluster_id}-kafka",
                    reason="Kafka endpoint not available"
                )
            
            # Create topic manager
            return TopicManager(
                bootstrap_servers=kafka_endpoint,
                cluster_id=cluster_id
            )
            
        except Exception as e:
            logger.error(f"Failed to create topic manager for cluster {cluster_id}: {e}")
            raise ServiceDiscoveryError(
                service_name=f"cluster-{cluster_id}-topic-manager",
                reason=str(e),
                cause=e
            )
    
    async def create_cluster_message_manager(self, cluster_id: str) -> MessageManager:
        """Create a message manager for a specific cluster.
        
        Args:
            cluster_id: ID of the cluster
            
        Returns:
            MessageManager instance configured for the cluster
            
        Raises:
            ServiceDiscoveryError: If cluster configuration cannot be retrieved
        """
        try:
            # Get cluster definition
            cluster = await self.multi_cluster_manager.registry.get_cluster(cluster_id)
            
            # Get cluster endpoints
            endpoints = await self.multi_cluster_manager.factory.get_cluster_endpoints(cluster)
            
            # Extract REST Proxy endpoint
            rest_proxy_endpoint = endpoints.get("kafka-rest-proxy")
            if not rest_proxy_endpoint:
                raise ServiceDiscoveryError(
                    service_name=f"cluster-{cluster_id}-rest-proxy",
                    reason="REST Proxy endpoint not available"
                )
            
            # Create message manager
            return MessageManager(
                rest_proxy_url=rest_proxy_endpoint,
                cluster_id=cluster_id
            )
            
        except Exception as e:
            logger.error(f"Failed to create message manager for cluster {cluster_id}: {e}")
            raise ServiceDiscoveryError(
                service_name=f"cluster-{cluster_id}-message-manager",
                reason=str(e),
                cause=e
            )
    
    async def refresh_all_clusters(self) -> Dict[str, bool]:
        """Refresh catalog data for all clusters.
        
        Returns:
            Dictionary mapping cluster IDs to refresh success status
        """
        logger.info("Refreshing catalog data for all clusters...")
        
        # Clear all caches
        self._cluster_catalogs.clear()
        self._cache_timestamps.clear()
        
        # Get all clusters
        clusters = await self.multi_cluster_manager.list_clusters()
        
        # Refresh each cluster
        results = {}
        for cluster in clusters:
            try:
                await self._get_cluster_catalog(cluster.id, force_refresh=True)
                results[cluster.id] = True
                logger.debug(f"Refreshed catalog for cluster {cluster.id}")
            except Exception as e:
                logger.warning(f"Failed to refresh catalog for cluster {cluster.id}: {e}")
                results[cluster.id] = False
        
        logger.info(f"Refreshed catalog data for {sum(results.values())}/{len(results)} clusters")
        return results
    
    # Private helper methods
    
    async def _get_cluster_catalog(self, cluster_id: str, force_refresh: bool = False) -> CatalogResponse:
        """Get catalog for a specific cluster with caching."""
        import time
        
        # Check cache validity
        if not force_refresh and self._is_cache_valid(cluster_id):
            return self._cluster_catalogs[cluster_id]
        
        try:
            # Get cluster status
            cluster_status = await self.multi_cluster_manager.get_cluster_status(cluster_id)
            
            if cluster_status != ServiceStatus.RUNNING:
                # Return minimal catalog for non-running clusters
                from ..models.cluster import ClusterStatus
                minimal_cluster_status = ClusterStatus(
                    status=cluster_status,
                    broker_count=0,
                    services={}
                )
                return CatalogResponse(
                    cluster=minimal_cluster_status,
                    topics=[],
                    available_apis=[],
                    services={},
                    system_info={
                        "cluster_id": cluster_id,
                        "status": cluster_status.value,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
            
            # Create cluster-specific service managers
            topic_manager = await self.create_cluster_topic_manager(cluster_id)
            
            # Create single-cluster service catalog
            service_catalog = ServiceCatalog(
                topic_manager=topic_manager,
                cache_ttl=self.cache_ttl
            )
            
            # Get catalog
            catalog = await service_catalog.get_catalog(force_refresh=True)
            
            # Add cluster context to system info
            if catalog.system_info:
                catalog.system_info["cluster_id"] = cluster_id
            else:
                catalog.system_info = {"cluster_id": cluster_id}
            
            # Update cache
            self._cluster_catalogs[cluster_id] = catalog
            self._cache_timestamps[cluster_id] = time.time()
            
            # Cleanup
            topic_manager.close()
            service_catalog.close()
            
            return catalog
            
        except Exception as e:
            logger.error(f"Failed to get catalog for cluster {cluster_id}: {e}")
            # Return error catalog
            from ..models.cluster import ClusterStatus
            error_cluster_status = ClusterStatus(
                status=ServiceStatus.ERROR,
                broker_count=0,
                services={}
            )
            return CatalogResponse(
                cluster=error_cluster_status,
                topics=[],
                available_apis=[],
                services={},
                system_info={
                    "cluster_id": cluster_id,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    def _is_cache_valid(self, cluster_id: str) -> bool:
        """Check if cached data is still valid for a cluster."""
        import time
        
        if cluster_id not in self._cluster_catalogs:
            return False
        
        if cluster_id not in self._cache_timestamps:
            return False
        
        age = time.time() - self._cache_timestamps[cluster_id]
        return age < self.cache_ttl
    
    async def _get_aggregated_stats(self, clusters: List[ClusterSummary], 
                                  cluster_catalogs: Dict[str, CatalogResponse]) -> Dict[str, Any]:
        """Get aggregated statistics across all clusters."""
        stats = {
            "total_topics": 0,
            "total_services": 0,
            "running_services": 0,
            "cluster_distribution": {},
            "service_types": {},
            "environments": {}
        }
        
        try:
            # Count topics across all clusters
            for catalog in cluster_catalogs.values():
                stats["total_topics"] += len(catalog.topics)
                stats["total_services"] += len(catalog.services)
                
                # Count running services
                for service in catalog.services.values():
                    if service.status == ServiceStatus.RUNNING:
                        stats["running_services"] += 1
                    
                    # Count service types
                    service_type = service.name.lower().replace(" ", "_")
                    stats["service_types"][service_type] = stats["service_types"].get(service_type, 0) + 1
            
            # Count cluster distribution by status
            for cluster in clusters:
                status = cluster.status.value
                stats["cluster_distribution"][status] = stats["cluster_distribution"].get(status, 0) + 1
                
                # Count environments
                env = cluster.environment.value
                stats["environments"][env] = stats["environments"].get(env, 0) + 1
            
            return stats
            
        except Exception as e:
            logger.warning(f"Failed to calculate aggregated stats: {e}")
            return stats
    
    def _build_multi_cluster_endpoints(self) -> List[APIEndpoint]:
        """Build API endpoints specific to multi-cluster operations."""
        endpoints = [
            # Multi-cluster catalog endpoints
            APIEndpoint(
                path="/multi-cluster/catalog",
                method="GET",
                description="Get aggregated catalog from all clusters",
                parameters={
                    "force_refresh": {
                        "type": "boolean",
                        "description": "Force refresh of cached data",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/multi-cluster/clusters/{cluster_id}/catalog",
                method="GET",
                description="Get catalog for a specific cluster",
                parameters={
                    "cluster_id": {
                        "type": "string",
                        "description": "Cluster ID",
                        "required": True
                    },
                    "force_refresh": {
                        "type": "boolean",
                        "description": "Force refresh of cached data",
                        "required": False
                    }
                }
            ),
            
            # Multi-cluster management endpoints
            APIEndpoint(
                path="/multi-cluster/clusters",
                method="GET",
                description="List all registered clusters",
                parameters={
                    "status_filter": {
                        "type": "string",
                        "description": "Filter by cluster status",
                        "required": False
                    },
                    "environment_filter": {
                        "type": "string",
                        "description": "Filter by environment",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/multi-cluster/clusters",
                method="POST",
                description="Create a new cluster",
                parameters={
                    "name": {
                        "type": "string",
                        "description": "Cluster name",
                        "required": True
                    },
                    "environment": {
                        "type": "string",
                        "description": "Cluster environment",
                        "required": True
                    },
                    "template_id": {
                        "type": "string",
                        "description": "Template to use",
                        "required": False
                    },
                    "auto_start": {
                        "type": "boolean",
                        "description": "Auto-start cluster after creation",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/multi-cluster/clusters/{cluster_id}/start",
                method="POST",
                description="Start a specific cluster",
                parameters={
                    "cluster_id": {
                        "type": "string",
                        "description": "Cluster ID",
                        "required": True
                    },
                    "force": {
                        "type": "boolean",
                        "description": "Force start even if already running",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/multi-cluster/clusters/{cluster_id}/stop",
                method="POST",
                description="Stop a specific cluster",
                parameters={
                    "cluster_id": {
                        "type": "string",
                        "description": "Cluster ID",
                        "required": True
                    },
                    "force": {
                        "type": "boolean",
                        "description": "Force stop containers",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/multi-cluster/clusters/{cluster_id}",
                method="DELETE",
                description="Delete a cluster",
                parameters={
                    "cluster_id": {
                        "type": "string",
                        "description": "Cluster ID",
                        "required": True
                    },
                    "force": {
                        "type": "boolean",
                        "description": "Force deletion even if running",
                        "required": False
                    }
                }
            ),
            
            # Cluster-specific topic operations
            APIEndpoint(
                path="/multi-cluster/clusters/{cluster_id}/topics",
                method="GET",
                description="List topics in a specific cluster",
                parameters={
                    "cluster_id": {
                        "type": "string",
                        "description": "Cluster ID",
                        "required": True
                    },
                    "include_internal": {
                        "type": "boolean",
                        "description": "Include internal topics",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/multi-cluster/clusters/{cluster_id}/topics",
                method="POST",
                description="Create a topic in a specific cluster",
                parameters={
                    "cluster_id": {
                        "type": "string",
                        "description": "Cluster ID",
                        "required": True
                    },
                    "name": {
                        "type": "string",
                        "description": "Topic name",
                        "required": True
                    },
                    "partitions": {
                        "type": "integer",
                        "description": "Number of partitions",
                        "required": False
                    },
                    "replication_factor": {
                        "type": "integer",
                        "description": "Replication factor",
                        "required": False
                    }
                }
            ),
            
            # Cluster-specific message operations
            APIEndpoint(
                path="/multi-cluster/clusters/{cluster_id}/produce",
                method="POST",
                description="Produce messages to a topic in a specific cluster",
                parameters={
                    "cluster_id": {
                        "type": "string",
                        "description": "Cluster ID",
                        "required": True
                    },
                    "topic": {
                        "type": "string",
                        "description": "Target topic name",
                        "required": True
                    },
                    "key": {
                        "type": "string",
                        "description": "Message key",
                        "required": False
                    },
                    "value": {
                        "type": "object",
                        "description": "Message value (JSON)",
                        "required": True
                    }
                }
            ),
            APIEndpoint(
                path="/multi-cluster/clusters/{cluster_id}/consume",
                method="GET",
                description="Consume messages from a topic in a specific cluster",
                parameters={
                    "cluster_id": {
                        "type": "string",
                        "description": "Cluster ID",
                        "required": True
                    },
                    "topic": {
                        "type": "string",
                        "description": "Source topic name",
                        "required": True
                    },
                    "consumer_group": {
                        "type": "string",
                        "description": "Consumer group ID",
                        "required": True
                    },
                    "max_messages": {
                        "type": "integer",
                        "description": "Maximum number of messages to consume",
                        "required": False
                    }
                }
            ),
            
            # Multi-cluster operations
            APIEndpoint(
                path="/multi-cluster/operations/start-multiple",
                method="POST",
                description="Start multiple clusters concurrently",
                parameters={
                    "cluster_ids": {
                        "type": "array",
                        "description": "List of cluster IDs to start",
                        "required": True
                    },
                    "max_concurrent": {
                        "type": "integer",
                        "description": "Maximum concurrent operations",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/multi-cluster/operations/stop-multiple",
                method="POST",
                description="Stop multiple clusters concurrently",
                parameters={
                    "cluster_ids": {
                        "type": "array",
                        "description": "List of cluster IDs to stop",
                        "required": True
                    },
                    "max_concurrent": {
                        "type": "integer",
                        "description": "Maximum concurrent operations",
                        "required": False
                    },
                    "force": {
                        "type": "boolean",
                        "description": "Force stop containers",
                        "required": False
                    }
                }
            ),
            
            # Health and monitoring
            APIEndpoint(
                path="/multi-cluster/health",
                method="GET",
                description="Get health status of all clusters"
            ),
            APIEndpoint(
                path="/multi-cluster/clusters/{cluster_id}/health",
                method="GET",
                description="Get detailed health information for a specific cluster",
                parameters={
                    "cluster_id": {
                        "type": "string",
                        "description": "Cluster ID",
                        "required": True
                    }
                }
            ),
            APIEndpoint(
                path="/multi-cluster/stats",
                method="GET",
                description="Get multi-cluster manager statistics"
            )
        ]
        
        return endpoints