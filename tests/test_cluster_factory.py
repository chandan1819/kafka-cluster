"""
Unit tests for cluster factory.
"""

import pytest
import yaml
import tempfile
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, mock_open
from datetime import datetime

from src.services.cluster_factory import ClusterFactory, ClusterFactoryError
from src.services.template_manager import TemplateManager
from src.services.cluster_manager import ClusterManager
from src.networking.port_allocator import PortAllocator
from src.networking.network_manager import NetworkManager
from src.models.multi_cluster import (
    ClusterDefinition,
    ClusterEnvironment,
    PortAllocation,
    KafkaConfig,
    RestProxyConfig,
    UIConfig,
    RetentionPolicy
)
from src.exceptions import PortAllocationError, NetworkIsolationError, ValidationError


@pytest.fixture
def mock_template_manager():
    """Create mock template manager."""
    manager = AsyncMock(spec=TemplateManager)
    return manager


@pytest.fixture
def mock_port_allocator():
    """Create mock port allocator."""
    allocator = AsyncMock(spec=PortAllocator)
    return allocator


@pytest.fixture
def mock_network_manager():
    """Create mock network manager."""
    manager = AsyncMock(spec=NetworkManager)
    return manager


@pytest.fixture
def temp_data_dir():
    """Create temporary data directory."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def cluster_factory(mock_template_manager, mock_port_allocator, 
                   mock_network_manager, temp_data_dir):
    """Create cluster factory with mocked dependencies."""
    return ClusterFactory(
        template_manager=mock_template_manager,
        port_allocator=mock_port_allocator,
        network_manager=mock_network_manager,
        base_data_dir=temp_data_dir
    )


@pytest.fixture
def sample_cluster_definition():
    """Create sample cluster definition."""
    return ClusterDefinition(
        id="test-cluster",
        name="Test Cluster",
        environment=ClusterEnvironment.DEVELOPMENT,
        kafka_config=KafkaConfig(heap_size="1G"),
        rest_proxy_config=RestProxyConfig(heap_size="512M"),
        ui_config=UIConfig(heap_size="512M"),
        retention_policy=RetentionPolicy(log_retention_hours=24)
    )


@pytest.fixture
def sample_port_allocation():
    """Create sample port allocation."""
    return PortAllocation(
        kafka_port=9092,
        rest_proxy_port=8082,
        ui_port=8080,
        jmx_port=9999
    )


class TestClusterFactory:
    """Test cluster factory functionality."""
    
    def test_init(self, mock_template_manager, mock_port_allocator, 
                  mock_network_manager, temp_data_dir):
        """Test cluster factory initialization."""
        factory = ClusterFactory(
            template_manager=mock_template_manager,
            port_allocator=mock_port_allocator,
            network_manager=mock_network_manager,
            base_data_dir=temp_data_dir
        )
        
        assert factory.template_manager == mock_template_manager
        assert factory.port_allocator == mock_port_allocator
        assert factory.network_manager == mock_network_manager
        assert factory.base_data_dir == temp_data_dir / "clusters"
        
        # Check that base data directory was created
        assert factory.base_data_dir.exists()
    
    @pytest.mark.asyncio
    async def test_create_cluster_manager_success(self, cluster_factory, 
                                                sample_cluster_definition,
                                                sample_port_allocation,
                                                mock_port_allocator,
                                                mock_network_manager):
        """Test successful cluster manager creation."""
        # Setup mocks
        mock_port_allocator.allocate_ports.return_value = sample_port_allocation
        mock_network_manager.create_cluster_network.return_value = "test-cluster-network"
        
        with patch('src.services.cluster_factory.ClusterManager') as mock_cluster_manager_class:
            mock_cluster_manager = MagicMock(spec=ClusterManager)
            mock_cluster_manager_class.return_value = mock_cluster_manager
            
            result = await cluster_factory.create_cluster_manager(sample_cluster_definition)
            
            # Verify cluster manager was created
            assert result == mock_cluster_manager
            
            # Verify ports were allocated
            mock_port_allocator.allocate_ports.assert_called_once_with("test-cluster")
            
            # Verify network was created
            mock_network_manager.create_cluster_network.assert_called_once_with("test-cluster")
            
            # Verify definition was updated
            assert sample_cluster_definition.port_allocation == sample_port_allocation
            assert sample_cluster_definition.network_name == "test-cluster-network"
            assert sample_cluster_definition.data_directory is not None
            assert sample_cluster_definition.updated_at is not None
    
    @pytest.mark.asyncio
    async def test_create_cluster_manager_with_existing_allocation(self, cluster_factory,
                                                                 sample_cluster_definition,
                                                                 sample_port_allocation,
                                                                 mock_port_allocator,
                                                                 mock_network_manager):
        """Test cluster manager creation with existing port allocation."""
        # Set existing allocation
        sample_cluster_definition.port_allocation = sample_port_allocation
        sample_cluster_definition.network_name = "existing-network"
        sample_cluster_definition.data_directory = "/existing/data"
        
        with patch('src.services.cluster_factory.ClusterManager') as mock_cluster_manager_class:
            mock_cluster_manager = MagicMock(spec=ClusterManager)
            mock_cluster_manager_class.return_value = mock_cluster_manager
            
            result = await cluster_factory.create_cluster_manager(sample_cluster_definition)
            
            # Verify cluster manager was created
            assert result == mock_cluster_manager
            
            # Verify ports were NOT allocated (already existed)
            mock_port_allocator.allocate_ports.assert_not_called()
            
            # Verify network was NOT created (already existed)
            mock_network_manager.create_cluster_network.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_create_cluster_manager_port_allocation_failure(self, cluster_factory,
                                                                sample_cluster_definition,
                                                                mock_port_allocator):
        """Test cluster manager creation with port allocation failure."""
        # Setup port allocation failure
        mock_port_allocator.allocate_ports.side_effect = PortAllocationError("No ports available")
        
        with pytest.raises(ClusterFactoryError, match="Failed to create cluster test-cluster"):
            await cluster_factory.create_cluster_manager(sample_cluster_definition)
    
    @pytest.mark.asyncio
    async def test_create_cluster_manager_network_creation_failure(self, cluster_factory,
                                                                 sample_cluster_definition,
                                                                 sample_port_allocation,
                                                                 mock_port_allocator,
                                                                 mock_network_manager):
        """Test cluster manager creation with network creation failure."""
        # Setup mocks
        mock_port_allocator.allocate_ports.return_value = sample_port_allocation
        mock_network_manager.create_cluster_network.side_effect = NetworkIsolationError(
            "Network creation failed", cluster_id="test-cluster"
        )
        
        with pytest.raises(ClusterFactoryError, match="Failed to create cluster test-cluster"):
            await cluster_factory.create_cluster_manager(sample_cluster_definition)
    
    @pytest.mark.asyncio
    async def test_create_cluster_manager_validation_failure(self, cluster_factory):
        """Test cluster manager creation with validation failure."""
        # Create cluster definition that will fail validation
        from pydantic import ValidationError as PydanticValidationError
        
        # Test with invalid ID - this will fail at Pydantic level
        with pytest.raises(PydanticValidationError):
            ClusterDefinition(
                id="",  # Invalid empty ID
                name="Test Cluster",
                environment=ClusterEnvironment.DEVELOPMENT
            )
    
    @pytest.mark.asyncio
    async def test_destroy_cluster_instance_success(self, cluster_factory,
                                                   mock_port_allocator,
                                                   mock_network_manager,
                                                   temp_data_dir):
        """Test successful cluster instance destruction."""
        cluster_id = "test-cluster"
        
        # Create some files to cleanup
        cluster_dir = temp_data_dir / "clusters" / cluster_id
        cluster_dir.mkdir(parents=True)
        compose_file = cluster_dir / "docker-compose.yml"
        compose_file.write_text("test compose content")
        
        # Setup mocks
        mock_port_allocator.release_ports.return_value = True
        mock_network_manager.delete_cluster_network.return_value = True
        
        result = await cluster_factory.destroy_cluster_instance(cluster_id, cleanup_data=True)
        
        assert result is True
        
        # Verify cleanup calls
        mock_port_allocator.release_ports.assert_called_once_with(cluster_id)
        mock_network_manager.delete_cluster_network.assert_called_once_with(cluster_id, force=True)
        
        # Verify files were removed
        assert not compose_file.exists()
        assert not cluster_dir.exists()
    
    @pytest.mark.asyncio
    async def test_destroy_cluster_instance_partial_failure(self, cluster_factory,
                                                           mock_port_allocator,
                                                           mock_network_manager):
        """Test cluster instance destruction with partial failures."""
        cluster_id = "test-cluster"
        
        # Setup partial failures
        mock_port_allocator.release_ports.side_effect = Exception("Port release failed")
        mock_network_manager.delete_cluster_network.return_value = True
        
        result = await cluster_factory.destroy_cluster_instance(cluster_id)
        
        # Should return False due to port release failure
        assert result is False
    
    @pytest.mark.asyncio
    async def test_get_cluster_endpoints(self, cluster_factory, sample_cluster_definition,
                                       sample_port_allocation):
        """Test getting cluster endpoints."""
        sample_cluster_definition.port_allocation = sample_port_allocation
        
        endpoints = await cluster_factory.get_cluster_endpoints(sample_cluster_definition)
        
        expected_endpoints = {
            "kafka": "localhost:9092",
            "kafka-rest-proxy": "http://localhost:8082",
            "kafka-ui": "http://localhost:8080",
            "jmx": "localhost:9999"
        }
        
        assert endpoints == expected_endpoints
    
    @pytest.mark.asyncio
    async def test_get_cluster_endpoints_no_allocation(self, cluster_factory, sample_cluster_definition):
        """Test getting cluster endpoints with no port allocation."""
        endpoints = await cluster_factory.get_cluster_endpoints(sample_cluster_definition)
        
        assert endpoints == {}
    
    @pytest.mark.asyncio
    async def test_validate_cluster_resources_success(self, cluster_factory,
                                                    sample_cluster_definition,
                                                    sample_port_allocation,
                                                    mock_port_allocator,
                                                    mock_network_manager,
                                                    mock_template_manager):
        """Test successful cluster resource validation."""
        # Setup cluster definition
        sample_cluster_definition.port_allocation = sample_port_allocation
        sample_cluster_definition.network_name = "test-network"
        sample_cluster_definition.data_directory = str(cluster_factory.base_data_dir / "test-cluster")
        sample_cluster_definition.template_id = "development"
        
        # Create data directory
        Path(sample_cluster_definition.data_directory).mkdir(parents=True)
        
        # Setup mocks
        mock_port_allocator.validate_allocation.return_value = []
        mock_network_manager.validate_network_isolation.return_value = {
            "isolation_valid": True,
            "issues": []
        }
        
        # Mock template
        mock_template = MagicMock()
        mock_template.min_memory_mb = 1024
        mock_template.recommended_memory_mb = 2048
        mock_template.min_disk_gb = 5
        mock_template.recommended_disk_gb = 10
        mock_template_manager.get_template.return_value = mock_template
        
        with patch('psutil.virtual_memory') as mock_memory, \
             patch('psutil.disk_usage') as mock_disk:
            
            # Mock sufficient resources
            mock_memory.return_value.available = 4096 * 1024 * 1024  # 4GB
            mock_disk.return_value.free = 20 * 1024 ** 3  # 20GB
            
            result = await cluster_factory.validate_cluster_resources(sample_cluster_definition)
            
            assert result["valid"] is True
            assert len(result["issues"]) == 0
            assert result["cluster_id"] == "test-cluster"
    
    @pytest.mark.asyncio
    async def test_validate_cluster_resources_port_issues(self, cluster_factory,
                                                        sample_cluster_definition,
                                                        sample_port_allocation,
                                                        mock_port_allocator):
        """Test cluster resource validation with port issues."""
        sample_cluster_definition.port_allocation = sample_port_allocation
        
        # Setup port validation failure
        mock_port_allocator.validate_allocation.return_value = ["Port 9092 is already allocated"]
        
        result = await cluster_factory.validate_cluster_resources(sample_cluster_definition)
        
        assert result["valid"] is False
        assert "Port 9092 is already allocated" in result["issues"]
    
    @pytest.mark.asyncio
    async def test_validate_cluster_resources_insufficient_memory(self, cluster_factory,
                                                                sample_cluster_definition,
                                                                mock_template_manager):
        """Test cluster resource validation with insufficient memory."""
        sample_cluster_definition.template_id = "production"
        
        # Mock template with high memory requirements
        mock_template = MagicMock()
        mock_template.min_memory_mb = 8192  # 8GB
        mock_template.recommended_memory_mb = 16384  # 16GB
        mock_template.min_disk_gb = 5
        mock_template.recommended_disk_gb = 10
        mock_template_manager.get_template.return_value = mock_template
        
        with patch('psutil.virtual_memory') as mock_memory, \
             patch('psutil.disk_usage') as mock_disk:
            
            # Mock insufficient memory
            mock_memory.return_value.available = 4096 * 1024 * 1024  # 4GB
            mock_disk.return_value.free = 20 * 1024 ** 3  # 20GB
            
            result = await cluster_factory.validate_cluster_resources(sample_cluster_definition)
            
            assert result["valid"] is False
            assert any("Insufficient memory" in issue for issue in result["issues"])
    
    @pytest.mark.asyncio
    async def test_generate_cluster_config(self, cluster_factory, sample_cluster_definition,
                                         sample_port_allocation, temp_data_dir):
        """Test Docker Compose configuration generation."""
        # Setup cluster definition
        sample_cluster_definition.port_allocation = sample_port_allocation
        sample_cluster_definition.network_name = "test-network"
        sample_cluster_definition.data_directory = str(temp_data_dir / "test-cluster")
        
        config = await cluster_factory._generate_cluster_config(sample_cluster_definition)
        
        # Verify basic structure
        assert "version" in config
        assert "services" in config
        assert "networks" in config
        
        # Verify services
        services = config["services"]
        assert "zookeeper" in services
        assert "kafka" in services
        assert "kafka-rest-proxy" in services
        assert "kafka-ui" in services
        
        # Verify Kafka configuration
        kafka_service = services["kafka"]
        assert f"{sample_port_allocation.kafka_port}:9092" in kafka_service["ports"]
        assert kafka_service["container_name"] == "test-cluster-kafka"
        
        # Verify environment variables
        kafka_env = kafka_service["environment"]
        assert kafka_env["KAFKA_ADVERTISED_LISTENERS"] == f"PLAINTEXT://localhost:{sample_port_allocation.kafka_port}"
        assert kafka_env["KAFKA_HEAP_OPTS"] == "-Xmx1G -Xms1G"
        
        # Verify JMX port
        assert f"{sample_port_allocation.jmx_port}:{sample_port_allocation.jmx_port}" in kafka_service["ports"]
        assert kafka_env["KAFKA_JMX_PORT"] == str(sample_port_allocation.jmx_port)
        
        # Verify network configuration
        assert config["networks"]["test-network"]["external"] is True
    
    @pytest.mark.asyncio
    async def test_generate_cluster_config_no_ports(self, cluster_factory, sample_cluster_definition):
        """Test cluster config generation without port allocation."""
        with pytest.raises(ClusterFactoryError, match="Port allocation is required"):
            await cluster_factory._generate_cluster_config(sample_cluster_definition)
    
    @pytest.mark.asyncio
    async def test_write_compose_file(self, cluster_factory, temp_data_dir):
        """Test writing Docker Compose file."""
        cluster_id = "test-cluster"
        compose_config = {
            "version": "3.8",
            "services": {
                "test": {
                    "image": "test:latest"
                }
            }
        }
        
        compose_file_path = await cluster_factory._write_compose_file(cluster_id, compose_config)
        
        # Verify file was created
        assert compose_file_path.exists()
        assert compose_file_path.name == "docker-compose.yml"
        
        # Verify content
        with open(compose_file_path, 'r') as f:
            written_config = yaml.safe_load(f)
        
        assert written_config == compose_config
    
    @pytest.mark.asyncio
    async def test_create_data_directories(self, cluster_factory):
        """Test data directory creation."""
        cluster_id = "test-cluster"
        
        data_dir = await cluster_factory._create_data_directories(cluster_id)
        
        # Verify directory structure
        assert data_dir.exists()
        assert (data_dir / "kafka" / "data").exists()
        assert (data_dir / "kafka" / "logs").exists()
        assert (data_dir / "zookeeper" / "data").exists()
        assert (data_dir / "zookeeper" / "logs").exists()
        
        # Verify permissions
        import stat
        assert stat.filemode(data_dir.stat().st_mode) == "drwxr-xr-x"
    
    @pytest.mark.asyncio
    async def test_validate_cluster_definition_success(self, cluster_factory, sample_cluster_definition):
        """Test successful cluster definition validation."""
        # Should not raise any exception
        await cluster_factory._validate_cluster_definition(sample_cluster_definition)
    
    @pytest.mark.asyncio
    async def test_validate_cluster_definition_no_id(self, cluster_factory):
        """Test cluster definition validation with missing ID."""
        from pydantic import ValidationError as PydanticValidationError
        
        # This will fail at Pydantic level before reaching our validation
        with pytest.raises(PydanticValidationError):
            ClusterDefinition(
                id="",
                name="Test Cluster",
                environment=ClusterEnvironment.DEVELOPMENT
            )
    
    @pytest.mark.asyncio
    async def test_validate_cluster_definition_no_name(self, cluster_factory):
        """Test cluster definition validation with missing name."""
        from pydantic import ValidationError as PydanticValidationError
        
        # This will fail at Pydantic level before reaching our validation
        with pytest.raises(PydanticValidationError):
            ClusterDefinition(
                id="test-cluster",
                name="",
                environment=ClusterEnvironment.DEVELOPMENT
            )
    
    @pytest.mark.asyncio
    async def test_validate_cluster_definition_invalid_id_format(self, cluster_factory):
        """Test cluster definition validation with invalid ID format."""
        from pydantic import ValidationError as PydanticValidationError
        
        # This will fail at Pydantic level before reaching our validation
        with pytest.raises(PydanticValidationError):
            ClusterDefinition(
                id="Test_Cluster",  # Invalid characters
                name="Test Cluster",
                environment=ClusterEnvironment.DEVELOPMENT
            )
    
    @pytest.mark.asyncio
    async def test_validate_cluster_definition_id_too_short(self, cluster_factory):
        """Test cluster definition validation with ID too short."""
        from pydantic import ValidationError as PydanticValidationError
        
        # This will fail at Pydantic level before reaching our validation
        with pytest.raises(PydanticValidationError):
            ClusterDefinition(
                id="ab",  # Too short
                name="Test Cluster",
                environment=ClusterEnvironment.DEVELOPMENT
            )
    
    @pytest.mark.asyncio
    async def test_cleanup_failed_cluster(self, cluster_factory, mock_port_allocator,
                                        mock_network_manager, temp_data_dir):
        """Test cleanup of failed cluster creation."""
        cluster_id = "failed-cluster"
        
        # Create compose file to cleanup
        cluster_dir = temp_data_dir / "clusters" / cluster_id
        cluster_dir.mkdir(parents=True)
        compose_file = cluster_dir / "docker-compose.yml"
        compose_file.write_text("test content")
        
        await cluster_factory._cleanup_failed_cluster(cluster_id)
        
        # Verify cleanup calls
        mock_port_allocator.release_ports.assert_called_once_with(cluster_id)
        mock_network_manager.delete_cluster_network.assert_called_once_with(cluster_id, force=True)
        
        # Verify compose file was removed
        assert not compose_file.exists()
    
    @pytest.mark.asyncio
    async def test_cleanup_failed_cluster_with_exceptions(self, cluster_factory,
                                                        mock_port_allocator,
                                                        mock_network_manager):
        """Test cleanup of failed cluster creation with exceptions."""
        cluster_id = "failed-cluster"
        
        # Setup exceptions
        mock_port_allocator.release_ports.side_effect = Exception("Port release failed")
        mock_network_manager.delete_cluster_network.side_effect = Exception("Network delete failed")
        
        # Should not raise exceptions, just log warnings
        await cluster_factory._cleanup_failed_cluster(cluster_id)
        
        # Verify cleanup was attempted
        mock_port_allocator.release_ports.assert_called_once_with(cluster_id)
        mock_network_manager.delete_cluster_network.assert_called_once_with(cluster_id, force=True)


class TestClusterFactoryIntegration:
    """Integration tests for cluster factory."""
    
    @pytest.mark.asyncio
    async def test_full_cluster_lifecycle(self, temp_data_dir):
        """Test complete cluster lifecycle from creation to destruction."""
        # Create real dependencies (but still mocked for external services)
        template_manager = AsyncMock(spec=TemplateManager)
        port_allocator = AsyncMock(spec=PortAllocator)
        network_manager = AsyncMock(spec=NetworkManager)
        
        factory = ClusterFactory(
            template_manager=template_manager,
            port_allocator=port_allocator,
            network_manager=network_manager,
            base_data_dir=temp_data_dir
        )
        
        # Create cluster definition
        definition = ClusterDefinition(
            id="integration-test",
            name="Integration Test Cluster",
            environment=ClusterEnvironment.TESTING,
            kafka_config=KafkaConfig(heap_size="2G"),
            rest_proxy_config=RestProxyConfig(heap_size="1G"),
            ui_config=UIConfig(heap_size="1G")
        )
        
        # Setup mocks
        port_allocation = PortAllocation(
            kafka_port=9093,
            rest_proxy_port=8083,
            ui_port=8081,
            jmx_port=9998
        )
        port_allocator.allocate_ports.return_value = port_allocation
        network_manager.create_cluster_network.return_value = "integration-test-network"
        
        # Create cluster manager
        with patch('src.services.cluster_factory.ClusterManager') as mock_cluster_manager_class:
            mock_cluster_manager = MagicMock(spec=ClusterManager)
            mock_cluster_manager_class.return_value = mock_cluster_manager
            
            cluster_manager = await factory.create_cluster_manager(definition)
            
            # Verify cluster was created
            assert cluster_manager == mock_cluster_manager
            assert definition.port_allocation == port_allocation
            assert definition.network_name == "integration-test-network"
            
            # Verify data directory was created
            data_dir = Path(definition.data_directory)
            assert data_dir.exists()
            assert (data_dir / "kafka" / "data").exists()
            
            # Verify compose file was created
            compose_file = temp_data_dir / "clusters" / "integration-test" / "docker-compose.yml"
            assert compose_file.exists()
            
            # Verify compose file content
            with open(compose_file, 'r') as f:
                compose_config = yaml.safe_load(f)
            
            assert compose_config["services"]["kafka"]["ports"] == ["9093:9092", "9998:9998"]
            assert compose_config["services"]["kafka-rest-proxy"]["ports"] == ["8083:8082"]
            assert compose_config["services"]["kafka-ui"]["ports"] == ["8081:8080"]
        
        # Setup destruction mocks
        port_allocator.release_ports.return_value = True
        network_manager.delete_cluster_network.return_value = True
        
        # Destroy cluster
        result = await factory.destroy_cluster_instance("integration-test", cleanup_data=True)
        
        # Verify destruction
        assert result is True
        assert not data_dir.exists()
        assert not compose_file.exists()