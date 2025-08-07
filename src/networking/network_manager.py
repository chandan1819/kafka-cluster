"""
Network manager for Docker network isolation between clusters.
"""

import asyncio
import docker
from typing import Dict, List, Optional, Any
from docker.errors import APIError, NotFound
from ..exceptions import NetworkIsolationError, DockerNotAvailableError
from ..utils.logging import get_logger

logger = get_logger(__name__)


class NetworkManager:
    """Manages Docker networks for cluster isolation."""
    
    def __init__(self, network_prefix: str = "kafka-cluster"):
        """Initialize network manager.
        
        Args:
            network_prefix: Prefix for network names
        """
        self.network_prefix = network_prefix
        self._docker_client: Optional[docker.DockerClient] = None
        self._created_networks: Dict[str, str] = {}  # cluster_id -> network_name
        self._lock = asyncio.Lock()
        
        logger.info(f"Network manager initialized with prefix: {network_prefix}")
    
    @property
    def docker_client(self) -> docker.DockerClient:
        """Get Docker client, creating it if necessary."""
        if self._docker_client is None:
            try:
                self._docker_client = docker.from_env()
                # Test connection
                self._docker_client.ping()
            except Exception as e:
                raise DockerNotAvailableError(
                    message=f"Docker is not available or not accessible: {e}",
                    cause=e
                )
        return self._docker_client
    
    async def create_cluster_network(self, cluster_id: str, 
                                   network_config: Optional[Dict[str, Any]] = None) -> str:
        """Create isolated network for cluster.
        
        Args:
            cluster_id: Unique cluster identifier
            network_config: Optional network configuration
            
        Returns:
            Network name that was created
            
        Raises:
            NetworkIsolationError: If network creation fails
        """
        async with self._lock:
            network_name = f"{self.network_prefix}-{cluster_id}"
            
            try:
                logger.info(f"Creating network for cluster {cluster_id}: {network_name}")
                
                # Check if network already exists
                if await self._network_exists(network_name):
                    logger.warning(f"Network {network_name} already exists")
                    self._created_networks[cluster_id] = network_name
                    return network_name
                
                # Default network configuration
                default_config = {
                    "driver": "bridge",
                    "check_duplicate": True,
                    "enable_ipv6": False,
                    "internal": False,
                    "labels": {
                        "kafka-cluster.cluster-id": cluster_id,
                        "kafka-cluster.managed": "true",
                        "kafka-cluster.created-by": "local-kafka-manager"
                    },
                    "options": {
                        "com.docker.network.bridge.enable_icc": "true",
                        "com.docker.network.bridge.enable_ip_masquerade": "true",
                        "com.docker.network.driver.mtu": "1500"
                    }
                }
                
                # Merge with provided config
                if network_config:
                    default_config.update(network_config)
                
                # Create the network
                network = self.docker_client.networks.create(
                    name=network_name,
                    **default_config
                )
                
                self._created_networks[cluster_id] = network_name
                
                logger.info(f"Created network {network_name} for cluster {cluster_id}")
                return network_name
                
            except APIError as e:
                logger.error(f"Docker API error creating network {network_name}: {e}")
                raise NetworkIsolationError(
                    f"Failed to create network {network_name}: {e}",
                    cluster_id=cluster_id,
                    network_name=network_name,
                    cause=e
                )
            except Exception as e:
                logger.error(f"Unexpected error creating network {network_name}: {e}")
                raise NetworkIsolationError(
                    f"Failed to create network {network_name}: {e}",
                    cluster_id=cluster_id,
                    network_name=network_name,
                    cause=e
                )
    
    async def delete_cluster_network(self, cluster_id: str, force: bool = False) -> bool:
        """Delete cluster network.
        
        Args:
            cluster_id: Unique cluster identifier
            force: Force deletion even if containers are connected
            
        Returns:
            True if network was deleted successfully
            
        Raises:
            NetworkIsolationError: If network deletion fails
        """
        async with self._lock:
            network_name = self._created_networks.get(cluster_id)
            if not network_name:
                network_name = f"{self.network_prefix}-{cluster_id}"
            
            try:
                logger.info(f"Deleting network for cluster {cluster_id}: {network_name}")
                
                # Check if network exists
                if not await self._network_exists(network_name):
                    logger.warning(f"Network {network_name} does not exist")
                    self._created_networks.pop(cluster_id, None)
                    return True
                
                # Get network object
                network = self.docker_client.networks.get(network_name)
                
                # If force is True, disconnect all containers first
                if force:
                    await self._disconnect_all_containers(network)
                
                # Remove the network
                network.remove()
                
                # Remove from tracking
                self._created_networks.pop(cluster_id, None)
                
                logger.info(f"Deleted network {network_name} for cluster {cluster_id}")
                return True
                
            except NotFound:
                logger.warning(f"Network {network_name} not found during deletion")
                self._created_networks.pop(cluster_id, None)
                return True
            except APIError as e:
                logger.error(f"Docker API error deleting network {network_name}: {e}")
                raise NetworkIsolationError(
                    f"Failed to delete network {network_name}: {e}",
                    cluster_id=cluster_id,
                    network_name=network_name,
                    cause=e
                )
            except Exception as e:
                logger.error(f"Unexpected error deleting network {network_name}: {e}")
                raise NetworkIsolationError(
                    f"Failed to delete network {network_name}: {e}",
                    cluster_id=cluster_id,
                    network_name=network_name,
                    cause=e
                )
    
    async def get_network_config(self, cluster_id: str) -> Optional[Dict[str, Any]]:
        """Get network configuration for cluster.
        
        Args:
            cluster_id: Unique cluster identifier
            
        Returns:
            Network configuration dictionary or None if not found
        """
        network_name = self._created_networks.get(cluster_id)
        if not network_name:
            network_name = f"{self.network_prefix}-{cluster_id}"
        
        try:
            if not await self._network_exists(network_name):
                return None
            
            network = self.docker_client.networks.get(network_name)
            
            return {
                "name": network.name,
                "id": network.id,
                "driver": network.attrs.get("Driver", "unknown"),
                "scope": network.attrs.get("Scope", "unknown"),
                "created": network.attrs.get("Created", "unknown"),
                "labels": network.attrs.get("Labels", {}),
                "options": network.attrs.get("Options", {}),
                "containers": len(network.attrs.get("Containers", {})),
                "ipam": network.attrs.get("IPAM", {}),
                "internal": network.attrs.get("Internal", False),
                "enable_ipv6": network.attrs.get("EnableIPv6", False)
            }
            
        except Exception as e:
            logger.error(f"Failed to get network config for {cluster_id}: {e}")
            return None
    
    async def list_cluster_networks(self) -> List[Dict[str, Any]]:
        """List all cluster networks.
        
        Returns:
            List of network information dictionaries
        """
        try:
            networks = []
            
            # Get all networks with our label
            all_networks = self.docker_client.networks.list(
                filters={"label": "kafka-cluster.managed=true"}
            )
            
            for network in all_networks:
                cluster_id = network.attrs.get("Labels", {}).get("kafka-cluster.cluster-id")
                
                network_info = {
                    "cluster_id": cluster_id,
                    "name": network.name,
                    "id": network.id,
                    "driver": network.attrs.get("Driver", "unknown"),
                    "created": network.attrs.get("Created", "unknown"),
                    "containers": len(network.attrs.get("Containers", {})),
                    "internal": network.attrs.get("Internal", False)
                }
                
                networks.append(network_info)
            
            return networks
            
        except Exception as e:
            logger.error(f"Failed to list cluster networks: {e}")
            return []
    
    async def connect_container_to_network(self, cluster_id: str, container_name: str,
                                         aliases: Optional[List[str]] = None) -> bool:
        """Connect container to cluster network.
        
        Args:
            cluster_id: Unique cluster identifier
            container_name: Name of container to connect
            aliases: Optional network aliases for the container
            
        Returns:
            True if connection was successful
        """
        network_name = self._created_networks.get(cluster_id)
        if not network_name:
            network_name = f"{self.network_prefix}-{cluster_id}"
        
        try:
            logger.info(f"Connecting container {container_name} to network {network_name}")
            
            # Get network and container
            network = self.docker_client.networks.get(network_name)
            container = self.docker_client.containers.get(container_name)
            
            # Connect container to network
            network.connect(container, aliases=aliases)
            
            logger.info(f"Connected container {container_name} to network {network_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect container {container_name} to network {network_name}: {e}")
            return False
    
    async def disconnect_container_from_network(self, cluster_id: str, container_name: str,
                                              force: bool = False) -> bool:
        """Disconnect container from cluster network.
        
        Args:
            cluster_id: Unique cluster identifier
            container_name: Name of container to disconnect
            force: Force disconnection
            
        Returns:
            True if disconnection was successful
        """
        network_name = self._created_networks.get(cluster_id)
        if not network_name:
            network_name = f"{self.network_prefix}-{cluster_id}"
        
        try:
            logger.info(f"Disconnecting container {container_name} from network {network_name}")
            
            # Get network and container
            network = self.docker_client.networks.get(network_name)
            container = self.docker_client.containers.get(container_name)
            
            # Disconnect container from network
            network.disconnect(container, force=force)
            
            logger.info(f"Disconnected container {container_name} from network {network_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to disconnect container {container_name} from network {network_name}: {e}")
            return False
    
    async def cleanup_orphaned_networks(self) -> int:
        """Clean up orphaned cluster networks.
        
        Returns:
            Number of networks cleaned up
        """
        try:
            logger.info("Cleaning up orphaned cluster networks")
            
            cleaned_count = 0
            all_networks = self.docker_client.networks.list(
                filters={"label": "kafka-cluster.managed=true"}
            )
            
            for network in all_networks:
                try:
                    # Check if network has any containers
                    containers = network.attrs.get("Containers", {})
                    
                    if not containers:
                        logger.info(f"Removing orphaned network: {network.name}")
                        network.remove()
                        cleaned_count += 1
                        
                        # Remove from tracking if present
                        cluster_id = network.attrs.get("Labels", {}).get("kafka-cluster.cluster-id")
                        if cluster_id and cluster_id in self._created_networks:
                            del self._created_networks[cluster_id]
                
                except Exception as e:
                    logger.warning(f"Failed to clean up network {network.name}: {e}")
                    continue
            
            logger.info(f"Cleaned up {cleaned_count} orphaned networks")
            return cleaned_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup orphaned networks: {e}")
            return 0
    
    async def get_network_stats(self) -> Dict[str, Any]:
        """Get network usage statistics.
        
        Returns:
            Dictionary with network statistics
        """
        try:
            cluster_networks = await self.list_cluster_networks()
            
            total_networks = len(cluster_networks)
            active_networks = len([n for n in cluster_networks if n["containers"] > 0])
            inactive_networks = total_networks - active_networks
            
            # Group by driver
            drivers = {}
            for network in cluster_networks:
                driver = network["driver"]
                drivers[driver] = drivers.get(driver, 0) + 1
            
            return {
                "total_networks": total_networks,
                "active_networks": active_networks,
                "inactive_networks": inactive_networks,
                "drivers": drivers,
                "network_prefix": self.network_prefix,
                "tracked_networks": len(self._created_networks)
            }
            
        except Exception as e:
            logger.error(f"Failed to get network stats: {e}")
            return {
                "total_networks": 0,
                "active_networks": 0,
                "inactive_networks": 0,
                "drivers": {},
                "network_prefix": self.network_prefix,
                "tracked_networks": len(self._created_networks),
                "error": str(e)
            }
    
    async def network_exists(self, cluster_id: str) -> bool:
        """Check if network exists for cluster.
        
        Args:
            cluster_id: ID of cluster to check
            
        Returns:
            True if network exists, False otherwise
        """
        try:
            network_name = self._created_networks.get(cluster_id)
            if not network_name:
                network_name = f"{self.network_prefix}-{cluster_id}"
            
            return await self._network_exists(network_name)
        except Exception as e:
            logger.error(f"Failed to check network existence for cluster '{cluster_id}': {e}")
            return False

    async def validate_network_isolation(self, cluster_id: str) -> Dict[str, Any]:
        """Validate network isolation for a cluster.
        
        Args:
            cluster_id: Unique cluster identifier
            
        Returns:
            Validation results
        """
        network_name = self._created_networks.get(cluster_id)
        if not network_name:
            network_name = f"{self.network_prefix}-{cluster_id}"
        
        validation_results = {
            "cluster_id": cluster_id,
            "network_name": network_name,
            "network_exists": False,
            "isolation_valid": False,
            "issues": []
        }
        
        try:
            # Check if network exists
            if not await self._network_exists(network_name):
                validation_results["issues"].append("Network does not exist")
                return validation_results
            
            validation_results["network_exists"] = True
            
            # Get network details
            network = self.docker_client.networks.get(network_name)
            network_attrs = network.attrs
            
            # Check network configuration
            if network_attrs.get("Driver") != "bridge":
                validation_results["issues"].append("Network is not using bridge driver")
            
            # Check labels
            labels = network_attrs.get("Labels", {})
            if labels.get("kafka-cluster.cluster-id") != cluster_id:
                validation_results["issues"].append("Network has incorrect cluster ID label")
            
            if labels.get("kafka-cluster.managed") != "true":
                validation_results["issues"].append("Network is not marked as managed")
            
            # Check if network is internal (should not be for Kafka clusters)
            if network_attrs.get("Internal", False):
                validation_results["issues"].append("Network is marked as internal")
            
            # Validation passes if no issues found
            validation_results["isolation_valid"] = len(validation_results["issues"]) == 0
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Failed to validate network isolation for {cluster_id}: {e}")
            validation_results["issues"].append(f"Validation error: {e}")
            return validation_results
    
    async def close(self) -> None:
        """Close the network manager and cleanup resources."""
        if self._docker_client:
            self._docker_client.close()
            self._docker_client = None
        
        logger.info("Network manager closed")
    
    # Private helper methods
    
    async def _network_exists(self, network_name: str) -> bool:
        """Check if network exists."""
        try:
            self.docker_client.networks.get(network_name)
            return True
        except NotFound:
            return False
        except Exception as e:
            logger.error(f"Error checking network existence {network_name}: {e}")
            return False
    
    async def _disconnect_all_containers(self, network) -> None:
        """Disconnect all containers from network."""
        try:
            containers = network.attrs.get("Containers", {})
            
            for container_id in containers:
                try:
                    container = self.docker_client.containers.get(container_id)
                    network.disconnect(container, force=True)
                    logger.info(f"Forcefully disconnected container {container.name} from network {network.name}")
                except Exception as e:
                    logger.warning(f"Failed to disconnect container {container_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to disconnect all containers from network {network.name}: {e}")


class GlobalNetworkManager:
    """Global singleton network manager for backward compatibility."""
    
    _instance: Optional[NetworkManager] = None
    _lock = asyncio.Lock()
    
    @classmethod
    async def get_instance(cls) -> NetworkManager:
        """Get the global network manager instance."""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = NetworkManager()
                    logger.info("Created global network manager instance")
        return cls._instance
    
    @classmethod
    async def reset_instance(cls) -> None:
        """Reset the global instance (mainly for testing)."""
        async with cls._lock:
            cls._instance = None
            logger.info("Reset global network manager instance")


async def create_cluster_network(cluster_id: str, 
                               network_config: Optional[Dict[str, Any]] = None) -> str:
    """Convenience function to create a cluster network.
    
    Args:
        cluster_id: Unique cluster identifier
        network_config: Optional network configuration
        
    Returns:
        Network name that was created
    """
    manager = await GlobalNetworkManager.get_instance()
    return await manager.create_cluster_network(cluster_id, network_config)


async def delete_cluster_network(cluster_id: str, force: bool = False) -> bool:
    """Convenience function to delete a cluster network.
    
    Args:
        cluster_id: Unique cluster identifier
        force: Force deletion even if containers are connected
        
    Returns:
        True if network was deleted successfully
    """
    manager = await GlobalNetworkManager.get_instance()
    return await manager.delete_cluster_network(cluster_id, force)


class NetworkPool:
    """Pool of pre-created networks for faster cluster deployment."""
    
    def __init__(self, network_manager: NetworkManager, pool_size: int = 5):
        """Initialize network pool.
        
        Args:
            network_manager: Network manager instance
            pool_size: Number of networks to keep in pool
        """
        self.network_manager = network_manager
        self.pool_size = pool_size
        self.available_networks: List[str] = []
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """Initialize the network pool."""
        async with self._lock:
            for i in range(self.pool_size):
                try:
                    network_name = await self.network_manager.create_cluster_network(
                        f"pool-{i}",
                        {"labels": {"kafka-cluster.pool": "true"}}
                    )
                    self.available_networks.append(network_name)
                except Exception as e:
                    logger.warning(f"Failed to create pool network {i}: {e}")
            
            logger.info(f"Network pool initialized with {len(self.available_networks)} networks")
    
    async def get_network(self, cluster_id: str) -> Optional[str]:
        """Get a network from the pool.
        
        Args:
            cluster_id: Cluster ID to assign network to
            
        Returns:
            Network name or None if pool is empty
        """
        async with self._lock:
            if self.available_networks:
                network_name = self.available_networks.pop(0)
                
                # Update network labels to assign to cluster
                try:
                    network = self.network_manager.docker_client.networks.get(network_name)
                    # Note: Docker doesn't support updating labels after creation
                    # So we'll need to recreate the network with proper labels
                    
                    # For now, just return the network name
                    # In a production implementation, you might want to recreate
                    # the network with proper cluster labels
                    
                    return network_name
                except Exception as e:
                    logger.error(f"Failed to assign pool network to cluster {cluster_id}: {e}")
                    return None
            
            return None
    
    async def return_network(self, network_name: str) -> None:
        """Return a network to the pool.
        
        Args:
            network_name: Network name to return
        """
        async with self._lock:
            if network_name not in self.available_networks:
                self.available_networks.append(network_name)
    
    async def cleanup(self) -> None:
        """Clean up all pool networks."""
        async with self._lock:
            for network_name in self.available_networks:
                try:
                    network = self.network_manager.docker_client.networks.get(network_name)
                    network.remove()
                except Exception as e:
                    logger.warning(f"Failed to cleanup pool network {network_name}: {e}")
            
            self.available_networks.clear()
            logger.info("Network pool cleaned up")