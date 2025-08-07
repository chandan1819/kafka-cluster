"""
Cluster factory for creating isolated cluster instances.
"""

import os
import yaml
import asyncio
from pathlib import Path
from typing import Dict, Optional, Any
from datetime import datetime

from ..models.multi_cluster import ClusterDefinition, PortAllocation
from ..services.template_manager import TemplateManager
from ..services.cluster_manager import ClusterManager
from ..networking.port_allocator import PortAllocator
from ..networking.network_manager import NetworkManager
from ..exceptions import (
    PortAllocationError,
    NetworkIsolationError,
    ValidationError
)
from ..utils.logging import get_logger
from ..config import settings

logger = get_logger(__name__)


class ClusterFactoryError(Exception):
    """Base exception for cluster factory operations."""
    
    def __init__(self, message: str, cluster_id: Optional[str] = None, 
                 details: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(message)
        self.cluster_id = cluster_id
        self.details = details or {}
        self.cause = cause


class ClusterFactory:
    """Factory for creating isolated cluster instances."""
    
    def __init__(self, 
                 template_manager: TemplateManager,
                 port_allocator: PortAllocator,
                 network_manager: NetworkManager,
                 base_data_dir: Optional[Path] = None):
        """Initialize cluster factory.
        
        Args:
            template_manager: Template manager for cluster templates
            port_allocator: Port allocator for unique port assignments
            network_manager: Network manager for Docker network isolation
            base_data_dir: Base directory for cluster data (uses config if None)
        """
        self.template_manager = template_manager
        self.port_allocator = port_allocator
        self.network_manager = network_manager
        self.base_data_dir = Path(base_data_dir or settings.data_directory) / "clusters"
        
        # Ensure base data directory exists
        self.base_data_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Cluster factory initialized with base data dir: {self.base_data_dir}")
    
    async def create_cluster_manager(self, definition: ClusterDefinition) -> ClusterManager:
        """Create a new cluster manager instance.
        
        Args:
            definition: Cluster definition
            
        Returns:
            ClusterManager instance for the cluster
            
        Raises:
            ClusterFactoryError: If cluster creation fails
        """
        logger.info(f"Creating cluster manager for: {definition.id}")
        
        try:
            # Validate cluster definition
            await self._validate_cluster_definition(definition)
            
            # Allocate ports if not already allocated
            if not definition.port_allocation:
                definition.port_allocation = await self.port_allocator.allocate_ports(
                    definition.id
                )
                logger.info(f"Allocated ports for {definition.id}: {definition.port_allocation.to_dict()}")
            
            # Create network if not already created
            if not definition.network_name:
                definition.network_name = await self.network_manager.create_cluster_network(
                    definition.id
                )
                logger.info(f"Created network for {definition.id}: {definition.network_name}")
            
            # Create data directories
            if not definition.data_directory:
                definition.data_directory = str(await self._create_data_directories(definition.id))
                logger.info(f"Created data directory for {definition.id}: {definition.data_directory}")
            
            # Generate Docker Compose configuration
            compose_config = await self._generate_cluster_config(definition)
            compose_file_path = await self._write_compose_file(definition.id, compose_config)
            
            # Create cluster manager instance
            cluster_manager = ClusterManager(compose_file_path=str(compose_file_path))
            
            # Update definition timestamps
            definition.updated_at = datetime.utcnow()
            
            logger.info(f"Created cluster manager for {definition.id}")
            return cluster_manager
            
        except Exception as e:
            logger.error(f"Failed to create cluster manager for {definition.id}: {e}")
            
            # Cleanup on failure
            await self._cleanup_failed_cluster(definition.id)
            
            if isinstance(e, (PortAllocationError, NetworkIsolationError, ValidationError)):
                raise ClusterFactoryError(
                    f"Failed to create cluster {definition.id}: {e}",
                    cluster_id=definition.id,
                    cause=e
                )
            else:
                raise ClusterFactoryError(
                    f"Unexpected error creating cluster {definition.id}: {e}",
                    cluster_id=definition.id,
                    cause=e
                )
    
    async def destroy_cluster_instance(self, cluster_id: str, 
                                     cleanup_data: bool = False) -> bool:
        """Destroy a cluster instance and cleanup resources.
        
        Args:
            cluster_id: Unique cluster identifier
            cleanup_data: Whether to remove data directories
            
        Returns:
            True if destruction was successful
        """
        logger.info(f"Destroying cluster instance: {cluster_id}")
        
        try:
            success = True
            
            # Release ports
            try:
                await self.port_allocator.release_ports(cluster_id)
                logger.info(f"Released ports for cluster {cluster_id}")
            except Exception as e:
                logger.warning(f"Failed to release ports for {cluster_id}: {e}")
                success = False
            
            # Delete network
            try:
                await self.network_manager.delete_cluster_network(cluster_id, force=True)
                logger.info(f"Deleted network for cluster {cluster_id}")
            except Exception as e:
                logger.warning(f"Failed to delete network for {cluster_id}: {e}")
                success = False
            
            # Remove compose file
            try:
                compose_file_path = self._get_compose_file_path(cluster_id)
                if compose_file_path.exists():
                    compose_file_path.unlink()
                    logger.info(f"Removed compose file for cluster {cluster_id}")
            except Exception as e:
                logger.warning(f"Failed to remove compose file for {cluster_id}: {e}")
                success = False
            
            # Remove data directories if requested
            if cleanup_data:
                try:
                    data_dir = self._get_cluster_data_dir(cluster_id)
                    if data_dir.exists():
                        import shutil
                        shutil.rmtree(data_dir)
                        logger.info(f"Removed data directory for cluster {cluster_id}")
                except Exception as e:
                    logger.warning(f"Failed to remove data directory for {cluster_id}: {e}")
                    success = False
            
            logger.info(f"Cluster instance {cluster_id} destroyed (success: {success})")
            return success
            
        except Exception as e:
            logger.error(f"Failed to destroy cluster instance {cluster_id}: {e}")
            return False
    
    async def get_cluster_endpoints(self, definition: ClusterDefinition) -> Dict[str, str]:
        """Get service endpoints for a cluster.
        
        Args:
            definition: Cluster definition
            
        Returns:
            Dictionary mapping service names to endpoints
        """
        if not definition.port_allocation:
            return {}
        
        ports = definition.port_allocation
        
        endpoints = {
            "kafka": f"localhost:{ports.kafka_port}",
            "kafka-rest-proxy": f"http://localhost:{ports.rest_proxy_port}",
            "kafka-ui": f"http://localhost:{ports.ui_port}"
        }
        
        if ports.jmx_port:
            endpoints["jmx"] = f"localhost:{ports.jmx_port}"
        
        return endpoints
    
    async def validate_cluster_resources(self, definition: ClusterDefinition) -> Dict[str, Any]:
        """Validate that cluster resources are available.
        
        Args:
            definition: Cluster definition to validate
            
        Returns:
            Validation results
        """
        validation_results = {
            "cluster_id": definition.id,
            "valid": True,
            "issues": [],
            "warnings": []
        }
        
        try:
            # Validate ports
            if definition.port_allocation:
                port_errors = await self.port_allocator.validate_allocation(definition.port_allocation)
                if port_errors:
                    validation_results["issues"].extend(port_errors)
                    validation_results["valid"] = False
            
            # Validate network
            if definition.network_name:
                network_validation = await self.network_manager.validate_network_isolation(definition.id)
                if not network_validation["isolation_valid"]:
                    validation_results["issues"].extend(network_validation["issues"])
                    validation_results["valid"] = False
            
            # Validate data directory
            if definition.data_directory:
                data_dir = Path(definition.data_directory)
                if not data_dir.exists():
                    validation_results["warnings"].append(f"Data directory does not exist: {data_dir}")
                elif not os.access(data_dir, os.W_OK):
                    validation_results["issues"].append(f"Data directory is not writable: {data_dir}")
                    validation_results["valid"] = False
            
            # Validate resource requirements (if template is specified)
            if definition.template_id:
                try:
                    template = await self.template_manager.get_template(definition.template_id)
                    
                    # Check available system resources (simplified check)
                    import psutil
                    available_memory_mb = psutil.virtual_memory().available // (1024 * 1024)
                    available_disk_gb = psutil.disk_usage(str(self.base_data_dir)).free // (1024 ** 3)
                    
                    if available_memory_mb < template.min_memory_mb:
                        validation_results["issues"].append(
                            f"Insufficient memory: {available_memory_mb}MB available, "
                            f"{template.min_memory_mb}MB required"
                        )
                        validation_results["valid"] = False
                    elif available_memory_mb < template.recommended_memory_mb:
                        validation_results["warnings"].append(
                            f"Memory below recommended: {available_memory_mb}MB available, "
                            f"{template.recommended_memory_mb}MB recommended"
                        )
                    
                    if available_disk_gb < template.min_disk_gb:
                        validation_results["issues"].append(
                            f"Insufficient disk space: {available_disk_gb}GB available, "
                            f"{template.min_disk_gb}GB required"
                        )
                        validation_results["valid"] = False
                    elif available_disk_gb < template.recommended_disk_gb:
                        validation_results["warnings"].append(
                            f"Disk space below recommended: {available_disk_gb}GB available, "
                            f"{template.recommended_disk_gb}GB recommended"
                        )
                        
                except Exception as e:
                    validation_results["warnings"].append(f"Could not validate template requirements: {e}")
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Failed to validate cluster resources for {definition.id}: {e}")
            validation_results["valid"] = False
            validation_results["issues"].append(f"Validation error: {e}")
            return validation_results
    
    # Private helper methods
    
    async def _validate_cluster_definition(self, definition: ClusterDefinition) -> None:
        """Validate cluster definition."""
        if not definition.id:
            raise ValidationError("Cluster ID is required")
        
        if not definition.name:
            raise ValidationError("Cluster name is required")
        
        # Validate ID format (already handled by Pydantic, but double-check)
        import re
        if not re.match(r'^[a-z0-9][a-z0-9-]*[a-z0-9]$', definition.id):
            raise ValidationError("Invalid cluster ID format")
        
        if len(definition.id) < 3 or len(definition.id) > 63:
            raise ValidationError("Cluster ID must be between 3 and 63 characters")
    
    async def _create_data_directories(self, cluster_id: str) -> Path:
        """Create isolated data directories for cluster."""
        cluster_data_dir = self._get_cluster_data_dir(cluster_id)
        
        # Create directory structure
        directories = [
            cluster_data_dir,
            cluster_data_dir / "kafka" / "data",
            cluster_data_dir / "kafka" / "logs",
            cluster_data_dir / "zookeeper" / "data",
            cluster_data_dir / "zookeeper" / "logs",
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            
            # Set appropriate permissions
            os.chmod(directory, 0o755)
        
        logger.info(f"Created data directories for cluster {cluster_id}")
        return cluster_data_dir
    
    def _get_cluster_data_dir(self, cluster_id: str) -> Path:
        """Get data directory path for cluster."""
        return self.base_data_dir / cluster_id
    
    def _get_compose_file_path(self, cluster_id: str) -> Path:
        """Get Docker Compose file path for cluster."""
        return self.base_data_dir / cluster_id / "docker-compose.yml"
    
    async def _generate_cluster_config(self, definition: ClusterDefinition) -> Dict[str, Any]:
        """Generate Docker Compose configuration for cluster."""
        if not definition.port_allocation:
            raise ClusterFactoryError("Port allocation is required", cluster_id=definition.id)
        
        ports = definition.port_allocation
        data_dir = Path(definition.data_directory) if definition.data_directory else self._get_cluster_data_dir(definition.id)
        
        # Generate environment variables for Kafka
        kafka_env = {
            "KAFKA_BROKER_ID": "1",
            "KAFKA_ZOOKEEPER_CONNECT": "zookeeper:2181",
            "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://localhost:{ports.kafka_port}",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": str(definition.kafka_config.default_replication_factor),
            "KAFKA_NUM_PARTITIONS": str(definition.kafka_config.num_partitions),
            "KAFKA_LOG_RETENTION_HOURS": str(definition.kafka_config.log_retention_hours),
            "KAFKA_LOG_SEGMENT_BYTES": str(definition.kafka_config.log_segment_bytes),
            "KAFKA_NUM_NETWORK_THREADS": str(definition.kafka_config.num_network_threads),
            "KAFKA_NUM_IO_THREADS": str(definition.kafka_config.num_io_threads),
            "KAFKA_SOCKET_SEND_BUFFER_BYTES": str(definition.kafka_config.socket_send_buffer_bytes),
            "KAFKA_SOCKET_RECEIVE_BUFFER_BYTES": str(definition.kafka_config.socket_receive_buffer_bytes),
            "KAFKA_HEAP_OPTS": f"-Xmx{definition.kafka_config.heap_size} -Xms{definition.kafka_config.heap_size}",
        }
        
        # Add custom properties
        for key, value in definition.kafka_config.custom_properties.items():
            kafka_env[f"KAFKA_{key.upper().replace('.', '_')}"] = value
        
        # Generate Docker Compose configuration
        compose_config = {
            "version": "3.8",
            "services": {
                "zookeeper": {
                    "image": "confluentinc/cp-zookeeper:7.4.0",
                    "container_name": f"{definition.id}-zookeeper",
                    "environment": {
                        "ZOOKEEPER_CLIENT_PORT": "2181",
                        "ZOOKEEPER_TICK_TIME": "2000"
                    },
                    "volumes": [
                        f"{data_dir}/zookeeper/data:/var/lib/zookeeper/data",
                        f"{data_dir}/zookeeper/logs:/var/lib/zookeeper/log"
                    ],
                    "networks": [definition.network_name] if definition.network_name else ["default"]
                },
                "kafka": {
                    "image": "confluentinc/cp-kafka:7.4.0",
                    "container_name": f"{definition.id}-kafka",
                    "depends_on": ["zookeeper"],
                    "ports": [f"{ports.kafka_port}:9092"],
                    "environment": kafka_env,
                    "volumes": [
                        f"{data_dir}/kafka/data:/var/lib/kafka/data",
                        f"{data_dir}/kafka/logs:/var/log/kafka"
                    ],
                    "networks": [definition.network_name] if definition.network_name else ["default"],
                    "healthcheck": {
                        "test": ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"],
                        "interval": "30s",
                        "timeout": "10s",
                        "retries": 3,
                        "start_period": "60s"
                    }
                },
                "kafka-rest-proxy": {
                    "image": "confluentinc/cp-kafka-rest:7.4.0",
                    "container_name": f"{definition.id}-kafka-rest-proxy",
                    "depends_on": ["kafka"],
                    "ports": [f"{ports.rest_proxy_port}:8082"],
                    "environment": {
                        "KAFKA_REST_ZOOKEEPER_CONNECT": "zookeeper:2181",
                        "KAFKA_REST_LISTENERS": "http://0.0.0.0:8082",
                        "KAFKA_REST_SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
                        "KAFKA_REST_HOST_NAME": "kafka-rest-proxy",
                        "KAFKA_REST_BOOTSTRAP_SERVERS": "kafka:9092",
                        "KAFKA_REST_CONSUMER_REQUEST_TIMEOUT_MS": str(definition.rest_proxy_config.consumer_request_timeout_ms),
                        "KAFKA_REST_PRODUCER_REQUEST_TIMEOUT_MS": str(definition.rest_proxy_config.producer_request_timeout_ms),
                        "KAFKAREST_HEAP_OPTS": f"-Xmx{definition.rest_proxy_config.heap_size} -Xms{definition.rest_proxy_config.heap_size}"
                    },
                    "networks": [definition.network_name] if definition.network_name else ["default"],
                    "healthcheck": {
                        "test": ["CMD", "curl", "-f", "http://localhost:8082/"],
                        "interval": "30s",
                        "timeout": "10s",
                        "retries": 3,
                        "start_period": "30s"
                    }
                },
                "kafka-ui": {
                    "image": "provectuslabs/kafka-ui:latest",
                    "container_name": f"{definition.id}-kafka-ui",
                    "depends_on": ["kafka", "kafka-rest-proxy"],
                    "ports": [f"{ports.ui_port}:8080"],
                    "environment": {
                        "KAFKA_CLUSTERS_0_NAME": definition.name,
                        "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS": "kafka:9092",
                        "KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME": "first",
                        "KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS": "http://kafka-rest-proxy:8082",
                        "SERVER_SERVLET_CONTEXT_PATH": "/",
                        "KAFKA_CLUSTERS_0_READONLY": "true" if definition.ui_config.readonly_mode else "false",
                        "DYNAMIC_CONFIG_ENABLED": "true",
                        "JAVA_OPTS": f"-Xmx{definition.ui_config.heap_size} -Xms{definition.ui_config.heap_size}"
                    },
                    "networks": [definition.network_name] if definition.network_name else ["default"],
                    "healthcheck": {
                        "test": ["CMD", "wget", "--quiet", "--tries=1", "--spider", "http://localhost:8080/actuator/health"],
                        "interval": "30s",
                        "timeout": "10s",
                        "retries": 3,
                        "start_period": "30s"
                    }
                }
            }
        }
        
        # Add JMX port if specified
        if ports.jmx_port:
            kafka_env.update({
                "KAFKA_JMX_PORT": str(ports.jmx_port),
                "KAFKA_JMX_HOSTNAME": "localhost"
            })
            compose_config["services"]["kafka"]["ports"].append(f"{ports.jmx_port}:{ports.jmx_port}")
        
        # Add custom network configuration if specified
        if definition.network_name:
            compose_config["networks"] = {
                definition.network_name: {
                    "external": True
                }
            }
        
        return compose_config
    
    async def _write_compose_file(self, cluster_id: str, compose_config: Dict[str, Any]) -> Path:
        """Write Docker Compose configuration to file."""
        compose_file_path = self._get_compose_file_path(cluster_id)
        
        # Ensure directory exists
        compose_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write compose file
        with open(compose_file_path, 'w') as f:
            yaml.dump(compose_config, f, default_flow_style=False, indent=2)
        
        logger.info(f"Wrote compose file for cluster {cluster_id}: {compose_file_path}")
        return compose_file_path
    
    async def _cleanup_failed_cluster(self, cluster_id: str) -> None:
        """Cleanup resources for a failed cluster creation."""
        logger.info(f"Cleaning up failed cluster creation: {cluster_id}")
        
        try:
            # Release ports
            await self.port_allocator.release_ports(cluster_id)
        except Exception as e:
            logger.warning(f"Failed to release ports during cleanup for {cluster_id}: {e}")
        
        try:
            # Delete network
            await self.network_manager.delete_cluster_network(cluster_id, force=True)
        except Exception as e:
            logger.warning(f"Failed to delete network during cleanup for {cluster_id}: {e}")
        
        try:
            # Remove compose file
            compose_file_path = self._get_compose_file_path(cluster_id)
            if compose_file_path.exists():
                compose_file_path.unlink()
        except Exception as e:
            logger.warning(f"Failed to remove compose file during cleanup for {cluster_id}: {e}")