"""Service Catalog for real-time service discovery and API documentation."""

import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

from ..models.base import ServiceStatus
from ..models.catalog import CatalogResponse, APIEndpoint, ServiceInfo
from ..models.cluster import ClusterStatus
from ..models.topic import TopicInfo
from .cluster_manager import ClusterManager
from .topic_manager import TopicManager
from ..exceptions import (
    ServiceCatalogError,
    CatalogRefreshError,
    ServiceDiscoveryError,
    ClusterManagerError,
    TopicManagerError,
    KafkaNotAvailableError
)
from ..utils.retry import retry_async, STANDARD_RETRY


logger = logging.getLogger(__name__)


class ServiceCatalog:
    """Provides real-time discovery of services and API capabilities."""
    
    def __init__(self, cluster_manager: Optional[ClusterManager] = None,
                 topic_manager: Optional[TopicManager] = None,
                 cache_ttl: int = 30):
        """Initialize the service catalog.
        
        Args:
            cluster_manager: Cluster manager instance
            topic_manager: Topic manager instance  
            cache_ttl: Cache time-to-live in seconds
        """
        self.cluster_manager = cluster_manager or ClusterManager()
        self.topic_manager = topic_manager or TopicManager()
        self.cache_ttl = cache_ttl
        
        # Cache for status information
        self._status_cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, float] = {}
        
        # API endpoint definitions
        self._api_endpoints = self._build_api_endpoints()
        
        # Service definitions with configuration-based ports
        self._init_service_definitions()
    
    def _init_service_definitions(self):
        """Initialize service definitions with configuration values."""
        from ..config import settings
        
        self._service_definitions = {
            "kafka": {
                "name": "Apache Kafka",
                "description": "Distributed streaming platform",
                "default_port": settings.kafka.broker_port,
                "health_path": None  # Kafka doesn't have HTTP health endpoint
            },
            "kafka-rest-proxy": {
                "name": "Kafka REST Proxy",
                "description": "RESTful interface to Kafka",
                "default_port": settings.kafka_rest_proxy.port,
                "health_path": "/v3"
            },
            "kafka-ui": {
                "name": "Kafka UI",
                "description": "Web-based Kafka management interface",
                "default_port": settings.kafka_ui.port,
                "health_path": settings.kafka_ui.health_check_path
            },
            "local-kafka-manager": {
                "name": "Local Kafka Manager API",
                "description": "REST API for managing local Kafka cluster",
                "default_port": settings.api.port,
                "health_path": "/health"
            }
        }

    @retry_async(STANDARD_RETRY)
    async def get_catalog(self, force_refresh: bool = False) -> CatalogResponse:
        """Get the complete service catalog with real-time status.
        
        Args:
            force_refresh: Force refresh of cached data
            
        Returns:
            CatalogResponse with complete catalog information
            
        Raises:
            CatalogRefreshError: If catalog generation fails
        """
        logger.debug("Generating service catalog...")
        
        try:
            # Get cluster status (with caching)
            cluster_status = await self._get_cached_cluster_status(force_refresh)
            
            # Get topics list (with caching)
            topics = await self._get_cached_topics(force_refresh)
            
            # Get service information
            services = await self._get_services_info(cluster_status)
            
            # Get system information
            system_info = await self._get_system_info()
            
            # Build catalog response
            catalog = CatalogResponse(
                cluster=cluster_status,
                topics=topics,
                available_apis=self._api_endpoints,
                services=services,
                system_info=system_info
            )
            
            logger.debug(f"Generated catalog with {len(topics)} topics and {len(services)} services")
            return catalog
            
        except Exception as e:
            logger.error(f"Failed to generate service catalog: {e}")
            raise CatalogRefreshError(
                reason=str(e),
                cause=e
            )

    async def refresh_status(self) -> None:
        """Refresh all cached status information.
        
        Raises:
            CatalogRefreshError: If refresh fails
        """
        logger.debug("Refreshing service catalog status...")
        
        # Clear all caches
        self._status_cache.clear()
        self._cache_timestamps.clear()
        
        # Pre-populate cache with fresh data
        try:
            await self._get_cached_cluster_status(force_refresh=True)
            await self._get_cached_topics(force_refresh=True)
            logger.debug("Service catalog status refreshed successfully")
        except Exception as e:
            logger.error(f"Failed to refresh status information: {e}")
            raise CatalogRefreshError(
                reason=f"Failed to refresh status information: {e}",
                cause=e
            )

    def get_api_specs(self) -> Dict[str, List[APIEndpoint]]:
        """Get API endpoint specifications grouped by category.
        
        Returns:
            Dictionary mapping categories to lists of API endpoints
        """
        specs = {}
        
        for endpoint in self._api_endpoints:
            # Extract category from path (first path segment)
            path_parts = endpoint.path.strip('/').split('/')
            category = path_parts[0] if path_parts and path_parts[0] else 'general'
            
            if category not in specs:
                specs[category] = []
            specs[category].append(endpoint)
        
        return specs

    async def _get_cached_cluster_status(self, force_refresh: bool = False) -> ClusterStatus:
        """Get cluster status with caching.
        
        Args:
            force_refresh: Force refresh of cached data
            
        Returns:
            ClusterStatus object
        """
        cache_key = "cluster_status"
        
        # Check cache validity
        if not force_refresh and self._is_cache_valid(cache_key):
            return self._status_cache[cache_key]
        
        try:
            # Get fresh status
            status = await self.cluster_manager.get_status()
            
            # Update cache
            self._status_cache[cache_key] = status
            self._cache_timestamps[cache_key] = time.time()
            
            return status
            
        except ClusterManagerError as e:
            logger.warning(f"Failed to get cluster status: {e}")
            # Return cached data if available, otherwise error status
            if cache_key in self._status_cache:
                return self._status_cache[cache_key]
            
            return ClusterStatus(
                status=ServiceStatus.ERROR,
                broker_count=0
            )

    async def _get_cached_topics(self, force_refresh: bool = False) -> List[TopicInfo]:
        """Get topics list with caching.
        
        Args:
            force_refresh: Force refresh of cached data
            
        Returns:
            List of TopicInfo objects
        """
        cache_key = "topics"
        
        # Check cache validity
        if not force_refresh and self._is_cache_valid(cache_key):
            return self._status_cache[cache_key]
        
        try:
            # Get fresh topics list
            topics = await self.topic_manager.list_topics()
            
            # Update cache
            self._status_cache[cache_key] = topics
            self._cache_timestamps[cache_key] = time.time()
            
            return topics
            
        except (TopicManagerError, KafkaNotAvailableError) as e:
            logger.warning(f"Failed to get topics list: {e}")
            # Return cached data if available, otherwise empty list
            if cache_key in self._status_cache:
                return self._status_cache[cache_key]
            
            return []

    async def _get_services_info(self, cluster_status: ClusterStatus) -> Dict[str, ServiceInfo]:
        """Get information about all services with enhanced UI and REST Proxy details.
        
        Args:
            cluster_status: Current cluster status
            
        Returns:
            Dictionary mapping service names to ServiceInfo objects
        """
        services = {}
        
        # Process each service definition
        for service_id, service_def in self._service_definitions.items():
            try:
                # Determine service status
                if service_id == "local-kafka-manager":
                    # This API is always running if we can execute this code
                    status = ServiceStatus.RUNNING
                    url = f"http://localhost:{service_def['default_port']}"
                    version = "1.0.0"  # Could be read from package info
                    
                elif service_id in cluster_status.services:
                    # Get status from cluster manager
                    service_health = cluster_status.services[service_id]
                    status = service_health.status
                    
                    if status == ServiceStatus.RUNNING:
                        url = cluster_status.endpoints.get(service_id, "")
                        if not url and service_def['default_port']:
                            if service_id == "kafka":
                                url = f"localhost:{service_def['default_port']}"
                            else:
                                url = f"http://localhost:{service_def['default_port']}"
                    else:
                        url = ""
                    
                    # Try to determine version (simplified)
                    version = cluster_status.version if service_id == "kafka" else "unknown"
                    
                else:
                    # Service not found in cluster status
                    status = ServiceStatus.STOPPED
                    url = ""
                    version = "unknown"
                
                # Build health check URL
                health_check_url = ""
                if url and service_def['health_path']:
                    if url.startswith('http'):
                        health_check_url = f"{url.rstrip('/')}{service_def['health_path']}"
                    else:
                        health_check_url = f"http://{url}{service_def['health_path']}"
                
                # Enhanced service info for UI and REST Proxy
                service_info = ServiceInfo(
                    name=service_def['name'],
                    status=status,
                    url=url,
                    version=version,
                    health_check_url=health_check_url
                )
                
                # Add service-specific metadata
                if service_id == "kafka-ui" and status == ServiceStatus.RUNNING:
                    service_info.metadata = {
                        "description": "Web-based Kafka management interface",
                        "features": [
                            "Topic management",
                            "Message browsing", 
                            "Consumer group monitoring",
                            "Cluster overview"
                        ],
                        "access_url": url,
                        "dependencies": ["kafka"]
                    }
                elif service_id == "kafka-rest-proxy" and status == ServiceStatus.RUNNING:
                    service_info.metadata = {
                        "description": "RESTful interface to Kafka cluster",
                        "features": [
                            "Message production via HTTP",
                            "Message consumption via HTTP",
                            "Topic metadata access",
                            "Consumer group management"
                        ],
                        "api_version": "v3",
                        "base_url": url,
                        "dependencies": ["kafka"]
                    }
                elif service_id == "kafka":
                    service_info.metadata = {
                        "description": "Apache Kafka distributed streaming platform",
                        "protocol": "Kafka native protocol",
                        "port": service_def['default_port'],
                        "kraft_mode": True
                    }
                
                services[service_id] = service_info
                
            except Exception as e:
                logger.warning(f"Failed to get info for service {service_id}: {e}")
                services[service_id] = ServiceInfo(
                    name=service_def['name'],
                    status=ServiceStatus.ERROR,
                    url="",
                    version="unknown"
                )
        
        return services

    async def _get_system_info(self) -> Dict[str, Any]:
        """Get system information.
        
        Returns:
            Dictionary with system information
        """
        try:
            system_info = {
                "timestamp": datetime.now().isoformat(),
                "cache_ttl": self.cache_ttl,
                "api_version": "1.0.0",
                "supported_kafka_version": "7.4.0"
            }
            
            # Add Docker information if available
            try:
                docker_client = self.cluster_manager.docker_client
                docker_info = docker_client.info()
                system_info["docker"] = {
                    "version": docker_info.get("ServerVersion", "unknown"),
                    "containers_running": docker_info.get("ContainersRunning", 0),
                    "containers_total": docker_info.get("Containers", 0)
                }
            except Exception as e:
                logger.debug(f"Could not get Docker info: {e}")
                system_info["docker"] = {"error": "Docker not available"}
            
            return system_info
            
        except Exception as e:
            logger.warning(f"Failed to get system info: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid.
        
        Args:
            cache_key: Cache key to check
            
        Returns:
            True if cache is valid, False otherwise
        """
        if cache_key not in self._status_cache:
            return False
        
        if cache_key not in self._cache_timestamps:
            return False
        
        age = time.time() - self._cache_timestamps[cache_key]
        return age < self.cache_ttl

    def _build_api_endpoints(self) -> List[APIEndpoint]:
        """Build the list of available API endpoints.
        
        Returns:
            List of APIEndpoint objects
        """
        endpoints = [
            # Service Catalog endpoints
            APIEndpoint(
                path="/catalog",
                method="GET",
                description="Get complete service catalog with real-time status",
                parameters={
                    "force_refresh": {
                        "type": "boolean",
                        "description": "Force refresh of cached data",
                        "required": False
                    }
                }
            ),
            
            # Cluster management endpoints
            APIEndpoint(
                path="/cluster/start",
                method="POST",
                description="Start the Kafka cluster",
                parameters={
                    "force": {
                        "type": "boolean", 
                        "description": "Force start even if already running",
                        "required": False
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Startup timeout in seconds",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/cluster/stop",
                method="POST", 
                description="Stop the Kafka cluster",
                parameters={
                    "force": {
                        "type": "boolean",
                        "description": "Force stop containers",
                        "required": False
                    },
                    "cleanup": {
                        "type": "boolean",
                        "description": "Remove containers and volumes",
                        "required": False
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Stop timeout in seconds", 
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/cluster/status",
                method="GET",
                description="Get current cluster status and health information"
            ),
            
            # Topic management endpoints
            APIEndpoint(
                path="/topics",
                method="GET",
                description="List all Kafka topics",
                parameters={
                    "include_internal": {
                        "type": "boolean",
                        "description": "Include internal topics",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/topics",
                method="POST",
                description="Create a new Kafka topic",
                parameters={
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
                    },
                    "config": {
                        "type": "object",
                        "description": "Topic configuration properties",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/topics/{name}",
                method="DELETE",
                description="Delete a Kafka topic",
                parameters={
                    "name": {
                        "type": "string",
                        "description": "Topic name",
                        "required": True
                    },
                    "force": {
                        "type": "boolean",
                        "description": "Force delete even if topic has data",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/topics/{name}/metadata",
                method="GET",
                description="Get detailed metadata for a specific topic",
                parameters={
                    "name": {
                        "type": "string",
                        "description": "Topic name",
                        "required": True
                    }
                }
            ),
            
            # Message operations endpoints
            APIEndpoint(
                path="/produce",
                method="POST",
                description="Produce messages to a Kafka topic",
                parameters={
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
                    },
                    "partition": {
                        "type": "integer",
                        "description": "Target partition",
                        "required": False
                    }
                }
            ),
            APIEndpoint(
                path="/consume",
                method="GET",
                description="Consume messages from a Kafka topic",
                parameters={
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
                    },
                    "timeout_ms": {
                        "type": "integer",
                        "description": "Consumer timeout in milliseconds",
                        "required": False
                    }
                }
            ),
            
            # Health and documentation endpoints
            APIEndpoint(
                path="/health",
                method="GET",
                description="Health check endpoint for the API service"
            ),
            APIEndpoint(
                path="/docs",
                method="GET", 
                description="Interactive API documentation (Swagger UI)"
            ),
            APIEndpoint(
                path="/openapi.json",
                method="GET",
                description="OpenAPI specification in JSON format"
            )
        ]
        
        return endpoints

    def close(self) -> None:
        """Close any resources used by the service catalog."""
        try:
            if hasattr(self.topic_manager, 'close'):
                self.topic_manager.close()
        except Exception as e:
            logger.warning(f"Error closing topic manager: {e}")