"""
Tests for Docker Compose generator.

This module tests the Docker Compose configuration generator
for multi-cluster Kafka deployments.
"""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import Mock, patch

from src.services.docker_compose_generator import DockerComposeGenerator
from src.models.multi_cluster import ClusterDefinition, PortAllocation
from src.exceptions import ValidationError


class TestDockerComposeGenerator:
    """Test Docker Compose generator functionality."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def generator(self, temp_dir):
        """Create Docker Compose generator with temp directory."""
        generator = DockerComposeGenerator()
        generator.cluster_configs_dir = temp_dir / "cluster-configs"
        generator.cluster_configs_dir.mkdir(exist_ok=True)
        return generator
    
    @pytest.fixture
    def sample_cluster(self):
        """Create sample cluster definition."""
        cluster = Mock(spec=ClusterDefinition)
        cluster.id = "test-cluster"
        cluster.name = "Test Cluster"
        cluster.environment = "development"
        
        # Mock port allocation
        cluster.port_allocation = Mock(spec=PortAllocation)
        cluster.port_allocation.kafka_port = 9092
        cluster.port_allocation.rest_proxy_port = 8082
        cluster.port_allocation.ui_port = 8080
        cluster.port_allocation.jmx_port = 9999
        
        # Mock configurations
        cluster.kafka_config = Mock()
        cluster.kafka_config.default_replication_factor = 1
        cluster.kafka_config.log_retention_hours = 168
        cluster.kafka_config.log_segment_bytes = 1073741824
        cluster.kafka_config.num_partitions = 3
        cluster.kafka_config.num_network_threads = 3
        cluster.kafka_config.num_io_threads = 8
        cluster.kafka_config.heap_size = "1G"
        cluster.kafka_config.custom_properties = {"auto.create.topics.enable": "true"}
        
        cluster.rest_proxy_config = Mock()
        cluster.rest_proxy_config.consumer_request_timeout_ms = 30000
        cluster.rest_proxy_config.producer_request_timeout_ms = 30000
        cluster.rest_proxy_config.heap_size = "512M"
        cluster.rest_proxy_config.custom_properties = {}
        
        cluster.ui_config = Mock()
        cluster.ui_config.readonly_mode = False
        cluster.ui_config.clusters_config = {}
        
        return cluster    

    def test_generate_cluster_compose(self, generator, sample_cluster):
        """Test generating Docker Compose configuration for a cluster."""
        config = generator.generate_cluster_compose(sample_cluster)
        
        # Check structure
        assert "version" in config
        assert "services" in config
        assert "volumes" in config
        assert "networks" in config
        
        # Check services
        services = config["services"]
        assert f"kafka-{sample_cluster.id}" in services
        assert f"kafka-rest-proxy-{sample_cluster.id}" in services
        assert f"kafka-ui-{sample_cluster.id}" in services
        
        # Check Kafka service
        kafka_service = services[f"kafka-{sample_cluster.id}"]
        assert kafka_service["image"] == "confluentinc/cp-kafka:7.4.0"
        assert f"{sample_cluster.port_allocation.kafka_port}:{sample_cluster.port_allocation.kafka_port}" in kafka_service["ports"]
        assert "environment" in kafka_service
        assert "healthcheck" in kafka_service
        
        # Check environment variables
        env = kafka_service["environment"]
        assert "KAFKA_NODE_ID" in env
        assert "CLUSTER_ID" in env
        assert env["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] == "1"
        
        # Check volumes and networks
        assert f"kafka-data-{sample_cluster.id}" in config["volumes"]
        assert f"kafka-network-{sample_cluster.id}" in config["networks"]
    
    def test_generate_kafka_service(self, generator, sample_cluster):
        """Test generating Kafka service configuration."""
        service_name = f"kafka-{sample_cluster.id}"
        ports = sample_cluster.port_allocation
        cluster_id = generator._generate_cluster_id(sample_cluster.id)
        network_name = f"kafka-network-{sample_cluster.id}"
        volume_name = f"kafka-data-{sample_cluster.id}"
        
        kafka_service = generator._generate_kafka_service(
            sample_cluster, service_name, ports, cluster_id, network_name, volume_name
        )
        
        # Check basic configuration
        assert kafka_service["image"] == "confluentinc/cp-kafka:7.4.0"
        assert kafka_service["hostname"] == service_name
        assert kafka_service["container_name"] == service_name
        
        # Check ports
        expected_ports = [f"{ports.kafka_port}:{ports.kafka_port}"]
        if ports.jmx_port:
            expected_ports.append(f"{ports.jmx_port}:{ports.jmx_port}")
        assert kafka_service["ports"] == expected_ports
        
        # Check environment
        env = kafka_service["environment"]
        assert env["KAFKA_NODE_ID"] == "1"
        assert env["CLUSTER_ID"] == cluster_id
        assert env["KAFKA_LOG_RETENTION_HOURS"] == "168"
        assert env["KAFKA_NUM_PARTITIONS"] == "3"
        
        # Check custom properties
        assert env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "true"
        
        # Check volumes and networks
        assert f"{volume_name}:/var/lib/kafka/data" in kafka_service["volumes"]
        assert network_name in kafka_service["networks"]
    
    def test_generate_rest_proxy_service(self, generator, sample_cluster):
        """Test generating REST Proxy service configuration."""
        service_name = f"kafka-rest-proxy-{sample_cluster.id}"
        kafka_service = f"kafka-{sample_cluster.id}"
        ports = sample_cluster.port_allocation
        network_name = f"kafka-network-{sample_cluster.id}"
        
        rest_proxy_service = generator._generate_rest_proxy_service(
            sample_cluster, service_name, kafka_service, ports, network_name
        )
        
        # Check basic configuration
        assert rest_proxy_service["image"] == "confluentinc/cp-kafka-rest:7.4.0"
        assert rest_proxy_service["hostname"] == service_name
        assert rest_proxy_service["container_name"] == service_name
        
        # Check dependencies
        assert kafka_service in rest_proxy_service["depends_on"]
        assert rest_proxy_service["depends_on"][kafka_service]["condition"] == "service_healthy"
        
        # Check ports
        assert f"{ports.rest_proxy_port}:{ports.rest_proxy_port}" in rest_proxy_service["ports"]
        
        # Check environment
        env = rest_proxy_service["environment"]
        assert env["KAFKA_REST_HOST_NAME"] == service_name
        assert env["KAFKA_REST_BOOTSTRAP_SERVERS"] == f"{kafka_service}:29092"
        assert env["KAFKA_REST_CONSUMER_REQUEST_TIMEOUT_MS"] == "30000"
        
        # Check healthcheck and network
        assert "healthcheck" in rest_proxy_service
        assert network_name in rest_proxy_service["networks"]
    
    def test_generate_ui_service(self, generator, sample_cluster):
        """Test generating UI service configuration."""
        service_name = f"kafka-ui-{sample_cluster.id}"
        kafka_service = f"kafka-{sample_cluster.id}"
        ports = sample_cluster.port_allocation
        network_name = f"kafka-network-{sample_cluster.id}"
        
        ui_service = generator._generate_ui_service(
            sample_cluster, service_name, kafka_service, ports, network_name
        )
        
        # Check basic configuration
        assert ui_service["image"] == "provectuslabs/kafka-ui:latest"
        assert ui_service["hostname"] == service_name
        assert ui_service["container_name"] == service_name
        
        # Check dependencies
        assert kafka_service in ui_service["depends_on"]
        
        # Check ports
        assert f"{ports.ui_port}:{ports.ui_port}" in ui_service["ports"]
        
        # Check environment
        env = ui_service["environment"]
        assert env["KAFKA_CLUSTERS_0_NAME"] == sample_cluster.name
        assert env["KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS"] == f"{kafka_service}:29092"
        assert env["DYNAMIC_CONFIG_ENABLED"] == "true"
        
        # Check healthcheck and network
        assert "healthcheck" in ui_service
        assert network_name in ui_service["networks"]
    
    def test_generate_cluster_id(self, generator):
        """Test cluster ID generation."""
        cluster_id1 = generator._generate_cluster_id("test-cluster")
        cluster_id2 = generator._generate_cluster_id("test-cluster")
        cluster_id3 = generator._generate_cluster_id("different-cluster")
        
        # Same input should produce same output
        assert cluster_id1 == cluster_id2
        
        # Different input should produce different output
        assert cluster_id1 != cluster_id3
        
        # Should be 22 characters long
        assert len(cluster_id1) == 22
        assert cluster_id1.isupper()
    
    def test_save_cluster_compose(self, generator, sample_cluster):
        """Test saving cluster Docker Compose configuration."""
        config = generator.generate_cluster_compose(sample_cluster)
        saved_path = generator.save_cluster_compose(sample_cluster, config)
        
        # Check file was created
        assert saved_path.exists()
        assert saved_path.name == f"docker-compose-{sample_cluster.id}.yml"
        
        # Check file content
        with open(saved_path, 'r') as f:
            loaded_config = yaml.safe_load(f)
        
        assert loaded_config["version"] == config["version"]
        assert len(loaded_config["services"]) == len(config["services"])
    
    def test_generate_multi_cluster_compose(self, generator, sample_cluster):
        """Test generating multi-cluster Docker Compose configuration."""
        # Create second cluster
        cluster2 = Mock(spec=ClusterDefinition)
        cluster2.id = "test-cluster-2"
        cluster2.name = "Test Cluster 2"
        cluster2.environment = "testing"
        cluster2.port_allocation = Mock(spec=PortAllocation)
        cluster2.port_allocation.kafka_port = 9093
        cluster2.port_allocation.rest_proxy_port = 8083
        cluster2.port_allocation.ui_port = 8081
        cluster2.port_allocation.jmx_port = 10000
        cluster2.kafka_config = sample_cluster.kafka_config
        cluster2.rest_proxy_config = sample_cluster.rest_proxy_config
        cluster2.ui_config = sample_cluster.ui_config
        
        clusters = [sample_cluster, cluster2]
        config = generator.generate_multi_cluster_compose(clusters)
        
        # Check structure
        assert "version" in config
        assert "services" in config
        assert "volumes" in config
        assert "networks" in config
        
        # Check services from both clusters
        services = config["services"]
        assert f"kafka-{sample_cluster.id}" in services
        assert f"kafka-{cluster2.id}" in services
        assert f"kafka-ui-{sample_cluster.id}" in services
        assert f"kafka-ui-{cluster2.id}" in services
        
        # Check volumes and networks from both clusters
        volumes = config["volumes"]
        assert f"kafka-data-{sample_cluster.id}" in volumes
        assert f"kafka-data-{cluster2.id}" in volumes
        
        networks = config["networks"]
        assert f"kafka-network-{sample_cluster.id}" in networks
        assert f"kafka-network-{cluster2.id}" in networks
    
    def test_generate_cluster_env_file(self, generator, sample_cluster):
        """Test generating cluster environment file."""
        env_vars = generator.generate_cluster_env_file(sample_cluster)
        
        # Check basic variables
        assert env_vars["CLUSTER_ID"] == sample_cluster.id
        assert env_vars["CLUSTER_NAME"] == sample_cluster.name
        assert env_vars["CLUSTER_ENVIRONMENT"] == sample_cluster.environment
        assert env_vars["KAFKA_PORT"] == str(sample_cluster.port_allocation.kafka_port)
        assert env_vars["REST_PROXY_PORT"] == str(sample_cluster.port_allocation.rest_proxy_port)
        assert env_vars["UI_PORT"] == str(sample_cluster.port_allocation.ui_port)
        assert env_vars["JMX_PORT"] == str(sample_cluster.port_allocation.jmx_port)
        
        # Check heap configuration
        assert "KAFKA_HEAP_OPTS" in env_vars
        assert "1G" in env_vars["KAFKA_HEAP_OPTS"]
        assert "KAFKAREST_HEAP_OPTS" in env_vars
        assert "512M" in env_vars["KAFKAREST_HEAP_OPTS"]
    
    def test_save_cluster_env_file(self, generator, sample_cluster):
        """Test saving cluster environment file."""
        saved_path = generator.save_cluster_env_file(sample_cluster)
        
        # Check file was created
        assert saved_path.exists()
        assert saved_path.name == f".env-{sample_cluster.id}"
        
        # Check file content
        with open(saved_path, 'r') as f:
            content = f.read()
        
        assert f"CLUSTER_ID={sample_cluster.id}" in content
        assert f"CLUSTER_NAME={sample_cluster.name}" in content
        assert f"KAFKA_PORT={sample_cluster.port_allocation.kafka_port}" in content
    
    def test_cleanup_cluster_files(self, generator, sample_cluster):
        """Test cleaning up cluster files."""
        # Create files first
        config = generator.generate_cluster_compose(sample_cluster)
        compose_path = generator.save_cluster_compose(sample_cluster, config)
        env_path = generator.save_cluster_env_file(sample_cluster)
        
        # Verify files exist
        assert compose_path.exists()
        assert env_path.exists()
        
        # Cleanup
        success = generator.cleanup_cluster_files(sample_cluster.id)
        
        # Verify cleanup
        assert success is True
        assert not compose_path.exists()
        assert not env_path.exists()
    
    def test_get_cluster_compose_file(self, generator, sample_cluster):
        """Test getting cluster compose file path."""
        # File doesn't exist initially
        path = generator.get_cluster_compose_file(sample_cluster.id)
        assert path is None
        
        # Create file
        config = generator.generate_cluster_compose(sample_cluster)
        saved_path = generator.save_cluster_compose(sample_cluster, config)
        
        # File should now be found
        path = generator.get_cluster_compose_file(sample_cluster.id)
        assert path == saved_path
        assert path.exists()
    
    def test_list_cluster_configs(self, generator, sample_cluster):
        """Test listing cluster configuration files."""
        # No configs initially
        cluster_ids = generator.list_cluster_configs()
        assert len(cluster_ids) == 0
        
        # Create config
        config = generator.generate_cluster_compose(sample_cluster)
        generator.save_cluster_compose(sample_cluster, config)
        
        # Should find the config
        cluster_ids = generator.list_cluster_configs()
        assert len(cluster_ids) == 1
        assert sample_cluster.id in cluster_ids
    
    def test_error_handling(self, generator):
        """Test error handling in Docker Compose generator."""
        # Test with invalid cluster (missing required attributes)
        invalid_cluster = Mock()
        invalid_cluster.id = "invalid"
        # Missing other required attributes
        
        with pytest.raises(ValidationError):
            generator.generate_cluster_compose(invalid_cluster)


class TestDockerComposeGeneratorIntegration:
    """Integration tests for Docker Compose generator."""
    
    @pytest.mark.asyncio
    async def test_full_cluster_compose_generation_flow(self):
        """Test complete flow of generating and managing cluster configs."""
        # This would be an integration test that tests the full flow
        # of generating, saving, and managing Docker Compose configurations
        pass
    
    @pytest.mark.asyncio
    async def test_multi_cluster_deployment_generation(self):
        """Test generating configurations for multi-cluster deployment."""
        # This would test generating configurations for multiple clusters
        # and ensuring they don't conflict with each other
        pass