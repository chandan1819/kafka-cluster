"""
Network isolation utilities combining port allocation and network management.
"""

import asyncio
from typing import Dict, List, Optional, Any, Tuple
from ..models.multi_cluster import PortAllocation, ClusterDefinition
from ..exceptions import NetworkIsolationError, PortAllocationError
from ..utils.logging import get_logger
from .port_allocator import PortAllocator, GlobalPortAllocator
from .network_manager import NetworkManager, GlobalNetworkManager

logger = get_logger(__name__)


class NetworkIsolation:
    """Manages complete network isolation for clusters."""
    
    def __init__(self, 
                 port_allocator: Optional[PortAllocator] = None,
                 network_manager: Optional[NetworkManager] = None):
        """Initialize network isolation manager.
        
        Args:
            port_allocator: Port allocator instance (uses global if None)
            network_manager: Network manager instance (uses global if None)
        """
        self.port_allocator = port_allocator
        self.network_manager = network_manager
        self._lock = asyncio.Lock()
        
        logger.info("Network isolation manager initialized")
    
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
    
    async def setup_cluster_isolation(self, 
                                    cluster_definition: ClusterDefinition,
                                    preferred_ports: Optional[Dict[str, int]] = None,
                                    network_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Set up complete network isolation for a cluster.
        
        Args:
            cluster_definition: Cluster definition
            preferred_ports: Preferred port assignments
            network_config: Network configuration
            
        Returns:
            Dictionary with isolation setup results
            
        Raises:
            NetworkIsolationError: If isolation setup fails
        """
        async with self._lock:
            cluster_id = cluster_definition.id
            
            try:
                logger.info(f"Setting up network isolation for cluster {cluster_id}")
                
                # Step 1: Allocate ports
                port_allocator = await self._get_port_allocator()
                
                # Use existing port allocation if available
                if cluster_definition.port_allocation:
                    logger.info(f"Using existing port allocation for cluster {cluster_id}")
                    port_allocation = cluster_definition.port_allocation
                    
                    # Validate that ports are still available
                    for port in port_allocation.get_all_ports():
                        if not await port_allocator.is_port_available(port):
                            # Try to allocate new ports if existing ones are not available
                            logger.warning(f"Port {port} no longer available, allocating new ports")
                            port_allocation = await port_allocator.allocate_ports(cluster_id, preferred_ports)
                            break
                else:
                    # Allocate new ports
                    port_allocation = await port_allocator.allocate_ports(cluster_id, preferred_ports)
                
                # Step 2: Create network
                network_manager = await self._get_network_manager()
                network_name = await network_manager.create_cluster_network(cluster_id, network_config)
                
                # Step 3: Update cluster definition
                cluster_definition.port_allocation = port_allocation
                cluster_definition.network_name = network_name
                
                # Step 4: Generate Docker Compose configuration
                docker_config = await self._generate_docker_config(cluster_definition)
                
                isolation_result = {
                    "cluster_id": cluster_id,
                    "port_allocation": port_allocation.to_dict(),
                    "network_name": network_name,
                    "docker_config": docker_config,
                    "isolation_complete": True
                }
                
                logger.info(f"Network isolation setup complete for cluster {cluster_id}")
                return isolation_result
                
            except Exception as e:
                logger.error(f"Failed to setup network isolation for cluster {cluster_id}: {e}")
                
                # Attempt cleanup on failure
                await self._cleanup_partial_setup(cluster_id)
                
                raise NetworkIsolationError(
                    f"Failed to setup network isolation for cluster {cluster_id}",
                    cluster_id=cluster_id,
                    cause=e
                )
    
    async def teardown_cluster_isolation(self, cluster_id: str, force: bool = False) -> bool:
        """Tear down network isolation for a cluster.
        
        Args:
            cluster_id: Cluster identifier
            force: Force teardown even if containers are running
            
        Returns:
            True if teardown was successful
        """
        async with self._lock:
            logger.info(f"Tearing down network isolation for cluster {cluster_id}")
            
            success = True
            errors = []
            
            try:
                # Step 1: Release ports
                port_allocator = await self._get_port_allocator()
                if not await port_allocator.release_ports(cluster_id):
                    errors.append("Failed to release ports")
                    success = False
                
                # Step 2: Delete network
                network_manager = await self._get_network_manager()
                if not await network_manager.delete_cluster_network(cluster_id, force):
                    errors.append("Failed to delete network")
                    success = False
                
                if success:
                    logger.info(f"Network isolation teardown complete for cluster {cluster_id}")
                else:
                    logger.warning(f"Network isolation teardown had errors for cluster {cluster_id}: {errors}")
                
                return success
                
            except Exception as e:
                logger.error(f"Failed to teardown network isolation for cluster {cluster_id}: {e}")
                return False
    
    async def validate_cluster_isolation(self, cluster_id: str) -> Dict[str, Any]:
        """Validate that cluster isolation is properly configured.
        
        Args:
            cluster_id: Cluster identifier
            
        Returns:
            Validation results
        """
        validation_result = {
            "cluster_id": cluster_id,
            "port_allocation_valid": False,
            "network_exists": False,
            "isolation_valid": False,
            "issues": []
        }
        
        try:
            # Validate port allocation
            port_allocator = await self._get_port_allocator()
            if await port_allocator.validate_allocation(cluster_id):
                validation_result["port_allocation_valid"] = True
            else:
                validation_result["issues"].append("Port allocation is invalid")
            
            # Validate network existence
            network_manager = await self._get_network_manager()
            if await network_manager.network_exists(cluster_id):
                validation_result["network_exists"] = True
            else:
                validation_result["issues"].append("Cluster network does not exist")
            
            # Overall validation
            validation_result["isolation_valid"] = (
                validation_result["port_allocation_valid"] and
                validation_result["network_exists"]
            )
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Failed to validate cluster isolation for {cluster_id}: {e}")
            validation_result["issues"].append(f"Validation error: {e}")
            return validation_result
    
    async def get_cluster_isolation_info(self, cluster_id: str) -> Optional[Dict[str, Any]]:
        """Get complete isolation information for a cluster.
        
        Args:
            cluster_id: Cluster identifier
            
        Returns:
            Isolation information or None if not found
        """
        try:
            port_allocator = await self._get_port_allocator()
            network_manager = await self._get_network_manager()
            
            # Get port allocation
            port_allocation = await port_allocator.get_cluster_allocation(cluster_id)
            if not port_allocation:
                return None
            
            # Get network config
            network_config = await network_manager.get_network_config(cluster_id)
            if not network_config:
                return None
            
            # Get network containers
            containers = await network_manager.get_network_containers(cluster_id)
            
            return {
                "cluster_id": cluster_id,
                "port_allocation": port_allocation.to_dict(),
                "network_config": network_config,
                "containers": containers,
                "isolation_active": True
            }
            
        except Exception as e:
            logger.error(f"Failed to get cluster isolation info for {cluster_id}: {e}")
            return None
    
    async def list_isolated_clusters(self) -> List[Dict[str, Any]]:
        """List all clusters with network isolation.
        
        Returns:
            List of cluster isolation information
        """
        try:
            port_allocator = await self._get_port_allocator()
            network_manager = await self._get_network_manager()
            
            # Get all port allocations
            allocations = await port_allocator.list_allocations()
            
            # Get all network configs
            networks = await network_manager.list_cluster_networks()
            
            # Combine information
            isolated_clusters = []
            
            for cluster_id in set(allocations.keys()) | set(networks.keys()):
                cluster_info = {
                    "cluster_id": cluster_id,
                    "has_port_allocation": cluster_id in allocations,
                    "has_network": cluster_id in networks
                }
                
                if cluster_id in allocations:
                    cluster_info["port_allocation"] = allocations[cluster_id].to_dict()
                
                if cluster_id in networks:
                    cluster_info["network_config"] = networks[cluster_id]
                
                cluster_info["isolation_complete"] = (
                    cluster_info["has_port_allocation"] and 
                    cluster_info["has_network"]
                )
                
                isolated_clusters.append(cluster_info)
            
            return isolated_clusters
            
        except Exception as e:
            logger.error(f"Failed to list isolated clusters: {e}")
            return []
    
    async def cleanup_orphaned_resources(self) -> Dict[str, List[str]]:
        """Clean up orphaned isolation resources.
        
        Returns:
            Dictionary with lists of cleaned up resources
        """
        cleanup_result = {
            "networks": [],
            "port_allocations": []
        }
        
        try:
            # Clean up orphaned networks
            network_manager = await self._get_network_manager()
            cleaned_networks = await network_manager.cleanup_orphaned_networks()
            cleanup_result["networks"] = cleaned_networks
            
            # Note: Port allocations are cleaned up when clusters are deleted
            # No orphaned port allocations should exist without corresponding clusters
            
            logger.info(f"Cleanup complete: {len(cleaned_networks)} networks")
            return cleanup_result
            
        except Exception as e:
            logger.error(f"Failed to cleanup orphaned resources: {e}")
            return cleanup_result
    
    async def get_isolation_stats(self) -> Dict[str, Any]:
        """Get network isolation statistics.
        
        Returns:
            Statistics dictionary
        """
        try:
            port_allocator = await self._get_port_allocator()
            network_manager = await self._get_network_manager()
            
            # Get port stats
            port_stats = await port_allocator.get_port_usage_stats()
            
            # Get network stats
            network_stats = await network_manager.get_network_stats()
            
            # Get isolated clusters count
            isolated_clusters = await self.list_isolated_clusters()
            complete_isolations = sum(1 for cluster in isolated_clusters if cluster["isolation_complete"])
            
            return {
                "port_statistics": port_stats,
                "network_statistics": network_stats,
                "isolated_clusters_total": len(isolated_clusters),
                "complete_isolations": complete_isolations,
                "partial_isolations": len(isolated_clusters) - complete_isolations
            }
            
        except Exception as e:
            logger.error(f"Failed to get isolation stats: {e}")
            return {"error": str(e)}
    
    # Private helper methods
    
    async def _generate_docker_config(self, cluster_definition: ClusterDefinition) -> Dict[str, Any]:
        """Generate Docker Compose configuration for cluster."""
        if not cluster_definition.port_allocation or not cluster_definition.network_name:
            raise NetworkIsolationError(
                "Cannot generate Docker config without port allocation and network",
                cluster_id=cluster_definition.id
            )
        
        ports = cluster_definition.port_allocation
        network_name = cluster_definition.network_name
        
        # Generate service configurations
        services = {
            "kafka": {
                "image": "confluentinc/cp-kafka:7.4.0",
                "container_name": f"{cluster_definition.id}-kafka",
                "ports": [f"{ports.kafka_port}:9092"],
                "networks": [network_name],
                "environment": self._generate_kafka_env(cluster_definition),
                "volumes": [f"{cluster_definition.id}-kafka-data:/tmp/kafka-logs"]
            }
        }
        
        # Add REST proxy if port is allocated
        if ports.rest_proxy_port:
            services["rest-proxy"] = {
                "image": "confluentinc/cp-kafka-rest:7.4.0",
                "container_name": f"{cluster_definition.id}-rest-proxy",
                "ports": [f"{ports.rest_proxy_port}:8082"],
                "networks": [network_name],
                "environment": self._generate_rest_proxy_env(cluster_definition),
                "depends_on": ["kafka"]
            }
        
        # Add UI if port is allocated
        if ports.ui_port:
            services["ui"] = {
                "image": "provectuslabs/kafka-ui:latest",
                "container_name": f"{cluster_definition.id}-ui",
                "ports": [f"{ports.ui_port}:8080"],
                "networks": [network_name],
                "environment": self._generate_ui_env(cluster_definition),
                "depends_on": ["kafka"]
            }
        
        # Generate networks configuration
        networks = {
            network_name: {
                "external": True
            }
        }
        
        # Generate volumes configuration
        volumes = {
            f"{cluster_definition.id}-kafka-data": {}
        }
        
        return {
            "version": "3.8",
            "services": services,
            "networks": networks,
            "volumes": volumes
        }
    
    def _generate_kafka_env(self, cluster_definition: ClusterDefinition) -> Dict[str, str]:
        """Generate Kafka environment variables."""
        ports = cluster_definition.port_allocation
        
        env = {
            "KAFKA_BROKER_ID": "1",
            "KAFKA_ZOOKEEPER_CONNECT": "zookeeper:2181",
            "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://localhost:{ports.kafka_port}",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
            "KAFKA_DELETE_TOPIC_ENABLE": "true"
        }
        
        # Add JMX configuration if port is allocated
        if ports.jmx_port:
            env.update({
                "KAFKA_JMX_PORT": str(ports.jmx_port),
                "KAFKA_JMX_HOSTNAME": "localhost"
            })
        
        return env
    
    def _generate_rest_proxy_env(self, cluster_definition: ClusterDefinition) -> Dict[str, str]:
        """Generate REST proxy environment variables."""
        ports = cluster_definition.port_allocation
        
        return {
            "KAFKA_REST_HOST_NAME": "rest-proxy",
            "KAFKA_REST_BOOTSTRAP_SERVERS": f"kafka:9092",
            "KAFKA_REST_LISTENERS": "http://0.0.0.0:8082"
        }
    
    def _generate_ui_env(self, cluster_definition: ClusterDefinition) -> Dict[str, str]:
        """Generate UI environment variables."""
        ports = cluster_definition.port_allocation
        
        env = {
            "KAFKA_CLUSTERS_0_NAME": cluster_definition.name or cluster_definition.id,
            "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS": "kafka:9092"
        }
        
        # Add REST proxy configuration if available
        if ports.rest_proxy_port:
            env["KAFKA_CLUSTERS_0_KAFKAREST"] = "http://rest-proxy:8082"
        
        return env
    
    async def _cleanup_partial_setup(self, cluster_id: str) -> None:
        """Clean up partial setup on failure."""
        try:
            # Try to release ports
            port_allocator = await self._get_port_allocator()
            await port_allocator.release_ports(cluster_id)
            
            # Try to delete network
            network_manager = await self._get_network_manager()
            await network_manager.delete_cluster_network(cluster_id, force=True)
            
            logger.info(f"Cleaned up partial setup for cluster {cluster_id}")
            
        except Exception as e:
            logger.warning(f"Failed to cleanup partial setup for cluster {cluster_id}: {e}")


class GlobalNetworkIsolation:
    """Singleton network isolation manager for global use."""
    
    _instance: Optional[NetworkIsolation] = None
    _lock = asyncio.Lock()
    
    @classmethod
    async def get_instance(cls, 
                          port_allocator: Optional[PortAllocator] = None,
                          network_manager: Optional[NetworkManager] = None) -> NetworkIsolation:
        """Get the global network isolation instance.
        
        Args:
            port_allocator: Port allocator (only used on first call)
            network_manager: Network manager (only used on first call)
            
        Returns:
            Global NetworkIsolation instance
        """
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = NetworkIsolation(port_allocator, network_manager)
                    logger.info("Created global network isolation instance")
        
        return cls._instance
    
    @classmethod
    async def reset_instance(cls) -> None:
        """Reset the global instance (mainly for testing)."""
        async with cls._lock:
            cls._instance = None
            logger.info("Reset global network isolation instance")


# Convenience functions

async def setup_cluster_isolation(cluster_definition: ClusterDefinition,
                                preferred_ports: Optional[Dict[str, int]] = None,
                                network_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Set up network isolation for a cluster using the global manager.
    
    Args:
        cluster_definition: Cluster definition
        preferred_ports: Preferred port assignments
        network_config: Network configuration
        
    Returns:
        Dictionary with isolation setup results
    """
    isolation = await GlobalNetworkIsolation.get_instance()
    return await isolation.setup_cluster_isolation(cluster_definition, preferred_ports, network_config)


async def teardown_cluster_isolation(cluster_id: str, force: bool = False) -> bool:
    """Tear down network isolation for a cluster using the global manager.
    
    Args:
        cluster_id: Cluster identifier
        force: Force teardown even if containers are running
        
    Returns:
        True if teardown was successful
    """
    isolation = await GlobalNetworkIsolation.get_instance()
    return await isolation.teardown_cluster_isolation(cluster_id, force)


async def get_isolation_stats() -> Dict[str, Any]:
    """Get network isolation statistics from the global manager.
    
    Returns:
        Statistics dictionary
    """
    isolation = await GlobalNetworkIsolation.get_instance()
    return await isolation.get_isolation_stats()