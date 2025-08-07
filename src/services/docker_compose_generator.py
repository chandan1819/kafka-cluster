"""
Docker Compose configuration generator for multi-cluster support.

This module generates Docker Compose configurations for isolated
Kafka clusters with their own UI instances and network isolation.
"""

import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
import yaml
import json

from ..models.multi_cluster import ClusterDefinition, PortAllocation
from ..exceptions import ValidationError

logger = logging.getLogger(__name__)


class DockerComposeGenerator:
    """Generates Docker Compose configurations for multi-cluster deployments."""
    
    def __init__(self, base_config_path: Optional[Path] = None):
        """
        Initialize Docker Compose generator.
        
        Args:
            base_config_path: Path to base docker-compose.yml template
        """
        self.base_config_path = base_config_path or Path("docker-compose.yml")
        self.cluster_configs_dir = Path("cluster-configs")
        self.cluster_configs_dir.mkdir(exist_ok=True)
    
    def generate_cluster_compose(self, cluster: ClusterDefinition) -> Dict[str, Any]:
        """
        Generate Docker Compose configuration for a specific cluster.
        
        Args:
            cluster: Cluster definition
            
        Returns:
            Dict containing Docker Compose configuration
        """
        try:
            # Generate unique service names
            kafka_service = f"kafka-{cluster.id}"
            rest_proxy_service = f"kafka-rest-proxy-{cluster.id}"
            ui_service = f"kafka-ui-{cluster.id}"
            network_name = f"kafka-network-{cluster.id}"
            volume_name = f"kafka-data-{cluster.id}"
            
            # Get port allocation
            ports = cluster.port_allocation
            
            # Generate cluster ID for Kafka
            cluster_id = self._generate_cluster_id(cluster.id)
            
            compose_config = {
                "version": "3.8",
                "services": {
                    kafka_service: self._generate_kafka_service(
                        cluster, kafka_service, ports, cluster_id, network_name, volume_name
                    ),
                    rest_proxy_service: self._generate_rest_proxy_service(
                        cluster, rest_proxy_service, kafka_service, ports, network_name
                    ),
                    ui_service: self._generate_ui_service(
                        cluster, ui_service, kafka_service, ports, network_name
                    )
                },
                "volumes": {
                    volume_name: {
                        "driver": "local"
                    }
                },
                "networks": {
                    network_name: {
                        "driver": "bridge",
                        "name": network_name
                    }
                }
            }
            
            return compose_config
            
        except Exception as e:
            logger.error(f"Failed to generate compose config for cluster {cluster.id}: {e}")
            raise ValidationError(f"Failed to generate Docker Compose configuration: {e}")
    
    def _generate_kafka_service(
        self, 
        cluster: ClusterDefinition, 
        service_name: str, 
        ports: PortAllocation,
        cluster_id: str,
        network_name: str,
        volume_name: str
    ) -> Dict[str, Any]:
        """Generate Kafka service configuration."""
        kafka_config = cluster.kafka_config
        
        # Base Kafka configuration
        environment = {
            "KAFKA_NODE_ID": "1",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
            "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{service_name}:29092,PLAINTEXT_HOST://localhost:{ports.kafka_port}",
            "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@{service_name}:29093",
            "KAFKA_LISTENERS": f"PLAINTEXT://{service_name}:29092,CONTROLLER://{service_name}:29093,PLAINTEXT_HOST://0.0.0.0:{ports.kafka_port}",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
            "CLUSTER_ID": cluster_id,
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": str(kafka_config.default_replication_factor) if kafka_config else "1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1"
        }
        
        # Add JMX configuration if port is specified
        if ports.jmx_port:
            environment.update({
                "KAFKA_JMX_PORT": str(ports.jmx_port),
                "KAFKA_JMX_HOSTNAME": "localhost"
            })
        
        # Add custom Kafka configuration
        if kafka_config:
            if kafka_config.log_retention_hours:
                environment["KAFKA_LOG_RETENTION_HOURS"] = str(kafka_config.log_retention_hours)
            if kafka_config.log_segment_bytes:
                environment["KAFKA_LOG_SEGMENT_BYTES"] = str(kafka_config.log_segment_bytes)
            if kafka_config.num_partitions:
                environment["KAFKA_NUM_PARTITIONS"] = str(kafka_config.num_partitions)
            if kafka_config.num_network_threads:
                environment["KAFKA_NUM_NETWORK_THREADS"] = str(kafka_config.num_network_threads)
            if kafka_config.num_io_threads:
                environment["KAFKA_NUM_IO_THREADS"] = str(kafka_config.num_io_threads)
            
            # Add custom properties
            if hasattr(kafka_config, 'custom_properties') and kafka_config.custom_properties:
                for key, value in kafka_config.custom_properties.items():
                    environment[f"KAFKA_{key.upper()}"] = str(value)
        
        service_config = {
            "image": "confluentinc/cp-kafka:7.4.0",
            "hostname": service_name,
            "container_name": service_name,
            "ports": [f"{ports.kafka_port}:{ports.kafka_port}"],
            "environment": environment,
            "volumes": [f"{volume_name}:/var/lib/kafka/data"],
            "healthcheck": {
                "test": ["CMD-SHELL", "ps aux | grep kafka | grep -v grep || exit 1"],
                "interval": "30s",
                "timeout": "10s",
                "retries": 3,
                "start_period": "60s"
            },
            "networks": [network_name]
        }
        
        # Add JMX port to ports if specified
        if ports.jmx_port:
            service_config["ports"].append(f"{ports.jmx_port}:{ports.jmx_port}")
        
        return service_config
    
    def _generate_rest_proxy_service(
        self,
        cluster: ClusterDefinition,
        service_name: str,
        kafka_service: str,
        ports: PortAllocation,
        network_name: str
    ) -> Dict[str, Any]:
        """Generate Kafka REST Proxy service configuration."""
        rest_proxy_config = cluster.rest_proxy_config
        
        environment = {
            "KAFKA_REST_HOST_NAME": service_name,
            "KAFKA_REST_BOOTSTRAP_SERVERS": f"{kafka_service}:29092",
            "KAFKA_REST_LISTENERS": f"http://0.0.0.0:{ports.rest_proxy_port}"
        }
        
        # Add custom REST Proxy configuration
        if rest_proxy_config:
            if hasattr(rest_proxy_config, 'consumer_request_timeout_ms'):
                environment["KAFKA_REST_CONSUMER_REQUEST_TIMEOUT_MS"] = str(rest_proxy_config.consumer_request_timeout_ms)
            if hasattr(rest_proxy_config, 'producer_request_timeout_ms'):
                environment["KAFKA_REST_PRODUCER_REQUEST_TIMEOUT_MS"] = str(rest_proxy_config.producer_request_timeout_ms)
            
            # Add custom properties
            if hasattr(rest_proxy_config, 'custom_properties') and rest_proxy_config.custom_properties:
                for key, value in rest_proxy_config.custom_properties.items():
                    environment[f"KAFKA_REST_{key.upper()}"] = str(value)
        
        return {
            "image": "confluentinc/cp-kafka-rest:7.4.0",
            "hostname": service_name,
            "container_name": service_name,
            "depends_on": {
                kafka_service: {
                    "condition": "service_healthy"
                }
            },
            "ports": [f"{ports.rest_proxy_port}:{ports.rest_proxy_port}"],
            "environment": environment,
            "healthcheck": {
                "test": ["CMD-SHELL", f"curl -f http://localhost:{ports.rest_proxy_port}/topics || exit 1"],
                "interval": "30s",
                "timeout": "10s",
                "retries": 3,
                "start_period": "30s"
            },
            "networks": [network_name]
        }
    
    def _generate_ui_service(
        self,
        cluster: ClusterDefinition,
        service_name: str,
        kafka_service: str,
        ports: PortAllocation,
        network_name: str
    ) -> Dict[str, Any]:
        """Generate Kafka UI service configuration."""
        ui_config = cluster.ui_config
        
        environment = {
            "KAFKA_CLUSTERS_0_NAME": cluster.name,
            "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS": f"{kafka_service}:29092",
            "DYNAMIC_CONFIG_ENABLED": "true"
        }
        
        # Add custom UI configuration
        if ui_config:
            if hasattr(ui_config, 'readonly_mode') and ui_config.readonly_mode:
                environment["KAFKA_CLUSTERS_0_READONLY"] = "true"
            
            # Add custom clusters configuration
            if hasattr(ui_config, 'clusters_config') and ui_config.clusters_config:
                for key, value in ui_config.clusters_config.items():
                    environment[f"KAFKA_CLUSTERS_0_{key.upper()}"] = str(value)
        
        return {
            "image": "provectuslabs/kafka-ui:latest",
            "hostname": service_name,
            "container_name": service_name,
            "depends_on": {
                kafka_service: {
                    "condition": "service_healthy"
                }
            },
            "ports": [f"{ports.ui_port}:{ports.ui_port}"],
            "environment": environment,
            "healthcheck": {
                "test": ["CMD-SHELL", f"wget --no-verbose --tries=1 --spider http://localhost:{ports.ui_port} || exit 1"],
                "interval": "30s",
                "timeout": "10s",
                "retries": 3,
                "start_period": "30s"
            },
            "networks": [network_name]
        }
    
    def _generate_cluster_id(self, cluster_name: str) -> str:
        """Generate a unique cluster ID for Kafka."""
        # Generate a deterministic cluster ID based on cluster name
        import hashlib
        hash_object = hashlib.md5(cluster_name.encode())
        hex_dig = hash_object.hexdigest()
        # Take first 22 characters and make it base64-like
        cluster_id = hex_dig[:22].upper()
        return cluster_id
    
    def save_cluster_compose(self, cluster: ClusterDefinition, compose_config: Dict[str, Any]) -> Path:
        """
        Save Docker Compose configuration to file.
        
        Args:
            cluster: Cluster definition
            compose_config: Docker Compose configuration
            
        Returns:
            Path to saved configuration file
        """
        try:
            config_file = self.cluster_configs_dir / f"docker-compose-{cluster.id}.yml"
            
            with open(config_file, 'w') as f:
                yaml.dump(compose_config, f, default_flow_style=False, sort_keys=False)
            
            logger.info(f"Saved Docker Compose configuration for cluster {cluster.id} to {config_file}")
            return config_file
            
        except Exception as e:
            logger.error(f"Failed to save compose config for cluster {cluster.id}: {e}")
            raise ValidationError(f"Failed to save Docker Compose configuration: {e}")
    
    def generate_multi_cluster_compose(self, clusters: List[ClusterDefinition]) -> Dict[str, Any]:
        """
        Generate a single Docker Compose configuration for multiple clusters.
        
        Args:
            clusters: List of cluster definitions
            
        Returns:
            Dict containing combined Docker Compose configuration
        """
        try:
            combined_config = {
                "version": "3.8",
                "services": {},
                "volumes": {},
                "networks": {}
            }
            
            for cluster in clusters:
                cluster_config = self.generate_cluster_compose(cluster)
                
                # Merge services, volumes, and networks
                combined_config["services"].update(cluster_config["services"])
                combined_config["volumes"].update(cluster_config["volumes"])
                combined_config["networks"].update(cluster_config["networks"])
            
            return combined_config
            
        except Exception as e:
            logger.error(f"Failed to generate multi-cluster compose config: {e}")
            raise ValidationError(f"Failed to generate multi-cluster Docker Compose configuration: {e}")
    
    def save_multi_cluster_compose(self, clusters: List[ClusterDefinition]) -> Path:
        """
        Save multi-cluster Docker Compose configuration to file.
        
        Args:
            clusters: List of cluster definitions
            
        Returns:
            Path to saved configuration file
        """
        try:
            compose_config = self.generate_multi_cluster_compose(clusters)
            config_file = self.cluster_configs_dir / "docker-compose-multi-cluster.yml"
            
            with open(config_file, 'w') as f:
                yaml.dump(compose_config, f, default_flow_style=False, sort_keys=False)
            
            logger.info(f"Saved multi-cluster Docker Compose configuration to {config_file}")
            return config_file
            
        except Exception as e:
            logger.error(f"Failed to save multi-cluster compose config: {e}")
            raise ValidationError(f"Failed to save multi-cluster Docker Compose configuration: {e}")
    
    def generate_cluster_env_file(self, cluster: ClusterDefinition) -> Dict[str, str]:
        """
        Generate environment file content for a cluster.
        
        Args:
            cluster: Cluster definition
            
        Returns:
            Dict containing environment variables
        """
        env_vars = {
            "CLUSTER_ID": cluster.id,
            "CLUSTER_NAME": cluster.name,
            "CLUSTER_ENVIRONMENT": cluster.environment,
            "KAFKA_PORT": str(cluster.port_allocation.kafka_port),
            "REST_PROXY_PORT": str(cluster.port_allocation.rest_proxy_port),
            "UI_PORT": str(cluster.port_allocation.ui_port)
        }
        
        if cluster.port_allocation.jmx_port:
            env_vars["JMX_PORT"] = str(cluster.port_allocation.jmx_port)
        
        # Add configuration-specific environment variables
        if cluster.kafka_config:
            if hasattr(cluster.kafka_config, 'heap_size'):
                env_vars["KAFKA_HEAP_OPTS"] = f"-Xmx{cluster.kafka_config.heap_size} -Xms{cluster.kafka_config.heap_size}"
        
        if cluster.rest_proxy_config:
            if hasattr(cluster.rest_proxy_config, 'heap_size'):
                env_vars["KAFKAREST_HEAP_OPTS"] = f"-Xmx{cluster.rest_proxy_config.heap_size} -Xms{cluster.rest_proxy_config.heap_size}"
        
        return env_vars
    
    def save_cluster_env_file(self, cluster: ClusterDefinition) -> Path:
        """
        Save cluster environment file.
        
        Args:
            cluster: Cluster definition
            
        Returns:
            Path to saved environment file
        """
        try:
            env_vars = self.generate_cluster_env_file(cluster)
            env_file = self.cluster_configs_dir / f".env-{cluster.id}"
            
            with open(env_file, 'w') as f:
                for key, value in env_vars.items():
                    f.write(f"{key}={value}\n")
            
            logger.info(f"Saved environment file for cluster {cluster.id} to {env_file}")
            return env_file
            
        except Exception as e:
            logger.error(f"Failed to save env file for cluster {cluster.id}: {e}")
            raise ValidationError(f"Failed to save environment file: {e}")
    
    def cleanup_cluster_files(self, cluster_id: str) -> bool:
        """
        Clean up Docker Compose and environment files for a cluster.
        
        Args:
            cluster_id: ID of cluster to clean up
            
        Returns:
            True if cleanup successful
        """
        try:
            # Remove Docker Compose file
            compose_file = self.cluster_configs_dir / f"docker-compose-{cluster_id}.yml"
            if compose_file.exists():
                compose_file.unlink()
                logger.info(f"Removed Docker Compose file: {compose_file}")
            
            # Remove environment file
            env_file = self.cluster_configs_dir / f".env-{cluster_id}"
            if env_file.exists():
                env_file.unlink()
                logger.info(f"Removed environment file: {env_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to cleanup files for cluster {cluster_id}: {e}")
            return False
    
    def get_cluster_compose_file(self, cluster_id: str) -> Optional[Path]:
        """
        Get path to Docker Compose file for a cluster.
        
        Args:
            cluster_id: Cluster ID
            
        Returns:
            Path to compose file if it exists, None otherwise
        """
        compose_file = self.cluster_configs_dir / f"docker-compose-{cluster_id}.yml"
        return compose_file if compose_file.exists() else None
    
    def list_cluster_configs(self) -> List[str]:
        """
        List all cluster configuration files.
        
        Returns:
            List of cluster IDs that have configuration files
        """
        try:
            cluster_ids = []
            for file in self.cluster_configs_dir.glob("docker-compose-*.yml"):
                # Extract cluster ID from filename
                filename = file.stem
                if filename.startswith("docker-compose-") and not filename.endswith("-multi-cluster"):
                    cluster_id = filename[len("docker-compose-"):]
                    cluster_ids.append(cluster_id)
            
            return cluster_ids
            
        except Exception as e:
            logger.error(f"Failed to list cluster configs: {e}")
            return []