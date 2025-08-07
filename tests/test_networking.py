"""
Tests for networking components including port allocation, network management, and isolation.
"""

import pytest
import asyncio
import socket
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from docker.errors import APIError, NotFound

from src.networking.port_allocator import (
    PortAllocator, PortRange, GlobalPortAllocator,
    allocate_cluster_ports, release_cluster_ports
)
from src.networking.network_manager import (
    NetworkManager, GlobalNetworkManager,
    create_cluster_network, delete_cluster_network
)
from src.networking.isolation import (
    NetworkIsolation, GlobalNetworkIsolation,
    setup_cluster_isolation, teardown_cluster_isolation
)
from src.models.multi_cluster import PortAllocation, ClusterDefinition
from src.exceptions import PortAllocationError, NetworkIsolationError


class TestPortRange:
    """Test PortRange class."""
    
    def test_valid_port_range(self):
        """Test creating valid port range."""
        port_range = PortRange(9000, 9999)
        assert port_range.start == 9000
        assert port_range.end == 9999
        assert port_range.size() == 1000
    
    def test_invalid_port_range_order(self):
        """Test invalid port range with start > end."""
        with pytest.raises(ValueError, match="Invalid port range"):
            PortRange(9999, 9000)
    
    def test_invalid_port_range_values(self):
        """Test invalid port range values."""
        with pytest.raises(ValueError, match="outside valid range"):
            PortRange(100, 200)  # Below 1024
        
        with pytest.raises(ValueError, match="outside valid range"):
            PortRange(70000, 80000)  # Above 65535
    
    def test_contains(self):
        """Test port containment check."""
        port_range = PortRange(9000, 9999)
        assert port_range.contains(9500)
        assert not port_range.contains(8999)
        assert not port_range.contains(10000)
    
    def test_to_list(self):
        """Test converting range to list."""
        port_range = PortRange(9000, 9002)
        assert port_range.to_list() == [9000, 9001, 9002]


class TestPortAllocator:
    """Test PortAllocator class."""
    
    @pytest.fixture
    def port_allocator(self):
        """Create port allocator for testing."""
        ranges = [PortRange(9000, 9010), PortRange(8000, 8010)]
        reserved = {9005, 8005}
        return PortAllocator(ranges, reserved)
    
    @pytest.fixture
    def mock_socket(self):
        """Mock socket for port availability testing."""
        with patch('src.networking.port_allocator.socket') as mock:
            yield mock
    
    @pytest.mark.asyncio
    async def test_allocate_ports_success(self, port_allocator, mock_socket):
        """Test successful port allocation."""
        # Mock socket to return available ports
        mock_socket.socket.return_value.__enter__.return_value.bind.return_value = None
        
        allocation = await port_allocator.allocate_ports("test-cluster")
        
        assert allocation.kafka_port is not None
        assert allocation.rest_proxy_port is not None
        assert allocation.ui_port is not None
        assert len(port_allocator.allocated_ports) >= 3
        assert "test-cluster" in port_allocator.cluster_allocations
    
    @pytest.mark.asyncio
    async def test_allocate_ports_with_preferences(self, port_allocator, mock_socket):
        """Test port allocation with preferred ports."""
        mock_socket.socket.return_value.__enter__.return_value.bind.return_value = None
        
        preferred = {"kafka_port": 9001, "rest_proxy_port": 8001}
        allocation = await port_allocator.allocate_ports("test-cluster", preferred)
        
        assert allocation.kafka_port == 9001
        assert allocation.rest_proxy_port == 8001
    
    @pytest.mark.asyncio
    async def test_allocate_ports_duplicate_cluster(self, port_allocator, mock_socket):
        """Test allocating ports for existing cluster."""
        mock_socket.socket.return_value.__enter__.return_value.bind.return_value = None
        
        # First allocation
        allocation1 = await port_allocator.allocate_ports("test-cluster")
        
        # Second allocation should return same ports
        allocation2 = await port_allocator.allocate_ports("test-cluster")
        
        assert allocation1.kafka_port == allocation2.kafka_port
        assert allocation1.rest_proxy_port == allocation2.rest_proxy_port
    
    @pytest.mark.asyncio
    async def test_allocate_ports_no_available_ports(self, port_allocator, mock_socket):
        """Test port allocation when no ports are available."""
        # Mock socket to return port unavailable
        mock_socket.socket.return_value.__enter__.return_value.bind.side_effect = OSError("Port in use")
        
        with pytest.raises(PortAllocationError):
            await port_allocator.allocate_ports("test-cluster")
    
    @pytest.mark.asyncio
    async def test_release_ports(self, port_allocator, mock_socket):
        """Test releasing allocated ports."""
        mock_socket.socket.return_value.__enter__.return_value.bind.return_value = None
        
        # Allocate ports
        await port_allocator.allocate_ports("test-cluster")
        initial_allocated = len(port_allocator.allocated_ports)
        
        # Release ports
        result = await port_allocator.release_ports("test-cluster")
        
        assert result is True
        assert len(port_allocator.allocated_ports) < initial_allocated
        assert "test-cluster" not in port_allocator.cluster_allocations
    
    @pytest.mark.asyncio
    async def test_release_ports_nonexistent_cluster(self, port_allocator):
        """Test releasing ports for nonexistent cluster."""
        result = await port_allocator.release_ports("nonexistent-cluster")
        assert result is False
    
    @pytest.mark.asyncio
    async def test_is_port_available(self, port_allocator, mock_socket):
        """Test port availability check."""
        mock_socket.socket.return_value.__enter__.return_value.bind.return_value = None
        
        # Port in range and not reserved
        assert await port_allocator.is_port_available(9001) is True
        
        # Reserved port
        assert await port_allocator.is_port_available(9005) is False
        
        # Port outside range
        assert await port_allocator.is_port_available(7000) is False
    
    @pytest.mark.asyncio
    async def test_get_port_usage_stats(self, port_allocator, mock_socket):
        """Test getting port usage statistics."""
        mock_socket.socket.return_value.__enter__.return_value.bind.return_value = None
        
        # Allocate some ports
        await port_allocator.allocate_ports("test-cluster")
        
        stats = await port_allocator.get_port_usage_stats()
        
        assert "total_ports" in stats
        assert "allocated_ports" in stats
        assert "reserved_ports" in stats
        assert "available_ports" in stats
        assert "cluster_count" in stats
        assert stats["cluster_count"] == 1
    
    @pytest.mark.asyncio
    async def test_validate_allocation(self, port_allocator, mock_socket):
        """Test validating port allocation."""
        mock_socket.socket.return_value.__enter__.return_value.bind.return_value = None
        
        # Allocate ports
        await port_allocator.allocate_ports("test-cluster")
        
        # Validation should pass
        assert await port_allocator.validate_allocation("test-cluster") is True
        
        # Validation should fail for nonexistent cluster
        assert await port_allocator.validate_allocation("nonexistent") is False
    
    @pytest.mark.asyncio
    async def test_reserve_and_unreserve_port(self, port_allocator):
        """Test reserving and unreserving ports."""
        # Reserve port
        result = await port_allocator.reserve_port(9003)
        assert result is True
        assert 9003 in port_allocator.reserved_ports
        
        # Try to reserve already allocated port
        port_allocator.allocated_ports.add(9004)
        result = await port_allocator.reserve_port(9004)
        assert result is False
        
        # Unreserve port
        result = await port_allocator.unreserve_port(9003)
        assert result is True
        assert 9003 not in port_allocator.reserved_ports


class TestGlobalPortAllocator:
    """Test GlobalPortAllocator singleton."""
    
    @pytest.mark.asyncio
    async def test_singleton_behavior(self):
        """Test that GlobalPortAllocator returns same instance."""
        # Reset instance first
        await GlobalPortAllocator.reset_instance()
        
        instance1 = await GlobalPortAllocator.get_instance()
        instance2 = await GlobalPortAllocator.get_instance()
        
        assert instance1 is instance2
    
    @pytest.mark.asyncio
    async def test_reset_instance(self):
        """Test resetting global instance."""
        instance1 = await GlobalPortAllocator.get_instance()
        await GlobalPortAllocator.reset_instance()
        instance2 = await GlobalPortAllocator.get_instance()
        
        assert instance1 is not instance2


class TestNetworkManager:
    """Test NetworkManager class."""
    
    @pytest.fixture
    def mock_docker_client(self):
        """Create mock Docker client."""
        client = Mock()
        client.networks = Mock()
        client.containers = Mock()
        return client
    
    @pytest.fixture
    def network_manager(self, mock_docker_client):
        """Create network manager for testing."""
        return NetworkManager(mock_docker_client)
    
    @pytest.mark.asyncio
    async def test_create_cluster_network_success(self, network_manager, mock_docker_client):
        """Test successful network creation."""
        mock_network = Mock()
        mock_network.name = "kafka-cluster-test"
        mock_docker_client.networks.create.return_value = mock_network
        
        network_name = await network_manager.create_cluster_network("test")
        
        assert network_name == "kafka-cluster-test"
        assert "test" in network_manager.cluster_networks
        mock_docker_client.networks.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_cluster_network_duplicate(self, network_manager, mock_docker_client):
        """Test creating network for existing cluster."""
        # First creation
        mock_network = Mock()
        mock_network.name = "kafka-cluster-test"
        mock_docker_client.networks.create.return_value = mock_network
        
        network_name1 = await network_manager.create_cluster_network("test")
        network_name2 = await network_manager.create_cluster_network("test")
        
        assert network_name1 == network_name2
        # Should only call create once
        mock_docker_client.networks.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_cluster_network_failure(self, network_manager, mock_docker_client):
        """Test network creation failure."""
        mock_docker_client.networks.create.side_effect = APIError("Network creation failed")
        
        with pytest.raises(NetworkIsolationError):
            await network_manager.create_cluster_network("test")
    
    @pytest.mark.asyncio
    async def test_delete_cluster_network_success(self, network_manager, mock_docker_client):
        """Test successful network deletion."""
        # Setup existing network
        network_manager.cluster_networks["test"] = "kafka-cluster-test"
        
        mock_network = Mock()
        mock_network.attrs = {"Containers": {}}
        mock_docker_client.networks.get.return_value = mock_network
        
        result = await network_manager.delete_cluster_network("test")
        
        assert result is True
        assert "test" not in network_manager.cluster_networks
        mock_network.remove.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_cluster_network_with_containers(self, network_manager, mock_docker_client):
        """Test network deletion with connected containers."""
        network_manager.cluster_networks["test"] = "kafka-cluster-test"
        
        mock_network = Mock()
        mock_network.attrs = {
            "Containers": {
                "container1": {"Name": "test-container"}
            }
        }
        mock_docker_client.networks.get.return_value = mock_network
        
        # Should fail without force
        with pytest.raises(NetworkIsolationError):
            await network_manager.delete_cluster_network("test", force=False)
        
        # Should succeed with force
        mock_container = Mock()
        mock_container.name = "test-container"
        mock_docker_client.containers.get.return_value = mock_container
        
        result = await network_manager.delete_cluster_network("test", force=True)
        assert result is True
    
    @pytest.mark.asyncio
    async def test_delete_cluster_network_not_found(self, network_manager, mock_docker_client):
        """Test deleting nonexistent network."""
        network_manager.cluster_networks["test"] = "kafka-cluster-test"
        mock_docker_client.networks.get.side_effect = NotFound("Network not found")
        
        result = await network_manager.delete_cluster_network("test")
        
        assert result is True
        assert "test" not in network_manager.cluster_networks
    
    @pytest.mark.asyncio
    async def test_get_network_config(self, network_manager, mock_docker_client):
        """Test getting network configuration."""
        network_manager.cluster_networks["test"] = "kafka-cluster-test"
        
        mock_network = Mock()
        mock_network.name = "kafka-cluster-test"
        mock_network.id = "network123"
        mock_network.attrs = {
            "Driver": "bridge",
            "Scope": "local",
            "Created": "2023-01-01T00:00:00Z",
            "Labels": {"test": "label"},
            "Containers": {"container1": {}},
            "IPAM": {"Driver": "default"},
            "Internal": False,
            "Attachable": True,
            "EnableIPv6": False
        }
        mock_docker_client.networks.get.return_value = mock_network
        
        config = await network_manager.get_network_config("test")
        
        assert config is not None
        assert config["name"] == "kafka-cluster-test"
        assert config["id"] == "network123"
        assert config["driver"] == "bridge"
    
    @pytest.mark.asyncio
    async def test_network_exists(self, network_manager, mock_docker_client):
        """Test checking if network exists."""
        network_manager.cluster_networks["test"] = "kafka-cluster-test"
        
        # Network exists
        mock_docker_client.networks.get.return_value = Mock()
        assert await network_manager.network_exists("test") is True
        
        # Network doesn't exist
        mock_docker_client.networks.get.side_effect = NotFound("Network not found")
        assert await network_manager.network_exists("test") is False
        assert "test" not in network_manager.cluster_networks  # Should clean up
    
    @pytest.mark.asyncio
    async def test_connect_container(self, network_manager, mock_docker_client):
        """Test connecting container to network."""
        network_manager.cluster_networks["test"] = "kafka-cluster-test"
        
        mock_network = Mock()
        mock_container = Mock()
        mock_docker_client.networks.get.return_value = mock_network
        mock_docker_client.containers.get.return_value = mock_container
        
        result = await network_manager.connect_container("test", "test-container", ["alias1"])
        
        assert result is True
        mock_network.connect.assert_called_once_with(mock_container, aliases=["alias1"])
    
    @pytest.mark.asyncio
    async def test_disconnect_container(self, network_manager, mock_docker_client):
        """Test disconnecting container from network."""
        network_manager.cluster_networks["test"] = "kafka-cluster-test"
        
        mock_network = Mock()
        mock_container = Mock()
        mock_docker_client.networks.get.return_value = mock_network
        mock_docker_client.containers.get.return_value = mock_container
        
        result = await network_manager.disconnect_container("test", "test-container")
        
        assert result is True
        mock_network.disconnect.assert_called_once_with(mock_container, force=False)
    
    @pytest.mark.asyncio
    async def test_cleanup_orphaned_networks(self, network_manager, mock_docker_client):
        """Test cleaning up orphaned networks."""
        mock_network1 = Mock()
        mock_network1.name = "kafka-cluster-orphan1"
        mock_network1.attrs = {
            "Labels": {
                "kafka-manager.cluster-id": "orphan1",
                "kafka-manager.managed": "true"
            },
            "Containers": {}  # No containers
        }
        
        mock_network2 = Mock()
        mock_network2.name = "kafka-cluster-orphan2"
        mock_network2.attrs = {
            "Labels": {
                "kafka-manager.cluster-id": "orphan2",
                "kafka-manager.managed": "true"
            },
            "Containers": {"container1": {}}  # Has containers
        }
        
        mock_docker_client.networks.list.return_value = [mock_network1, mock_network2]
        
        cleaned = await network_manager.cleanup_orphaned_networks()
        
        assert "kafka-cluster-orphan1" in cleaned
        assert "kafka-cluster-orphan2" not in cleaned
        mock_network1.remove.assert_called_once()
        mock_network2.remove.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_health_check(self, network_manager, mock_docker_client):
        """Test network manager health check."""
        mock_docker_client.ping.return_value = True
        mock_docker_client.networks.list.return_value = []
        
        health = await network_manager.health_check()
        
        assert health["status"] == "healthy"
        assert health["docker_connected"] is True


class TestNetworkIsolation:
    """Test NetworkIsolation class."""
    
    @pytest.fixture
    def mock_port_allocator(self):
        """Create mock port allocator."""
        allocator = AsyncMock()
        allocator.allocate_ports.return_value = PortAllocation(
            kafka_port=9092,
            rest_proxy_port=8082,
            ui_port=8080,
            jmx_port=7000
        )
        allocator.release_ports.return_value = True
        allocator.validate_allocation.return_value = True
        return allocator
    
    @pytest.fixture
    def mock_network_manager(self):
        """Create mock network manager."""
        manager = AsyncMock()
        manager.create_cluster_network.return_value = "kafka-cluster-test"
        manager.delete_cluster_network.return_value = True
        manager.network_exists.return_value = True
        return manager
    
    @pytest.fixture
    def network_isolation(self, mock_port_allocator, mock_network_manager):
        """Create network isolation manager for testing."""
        return NetworkIsolation(mock_port_allocator, mock_network_manager)
    
    @pytest.fixture
    def cluster_definition(self):
        """Create test cluster definition."""
        return ClusterDefinition(
            id="test-cluster",
            name="Test Cluster",
            description="Test cluster for unit tests"
        )
    
    @pytest.mark.asyncio
    async def test_setup_cluster_isolation_success(self, network_isolation, cluster_definition, 
                                                  mock_port_allocator, mock_network_manager):
        """Test successful cluster isolation setup."""
        result = await network_isolation.setup_cluster_isolation(cluster_definition)
        
        assert result["cluster_id"] == "test-cluster"
        assert result["isolation_complete"] is True
        assert "port_allocation" in result
        assert "network_name" in result
        assert "docker_config" in result
        
        mock_port_allocator.allocate_ports.assert_called_once()
        mock_network_manager.create_cluster_network.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_setup_cluster_isolation_with_existing_ports(self, network_isolation, 
                                                              cluster_definition, mock_port_allocator):
        """Test setup with existing port allocation."""
        # Set existing port allocation
        cluster_definition.port_allocation = PortAllocation(
            kafka_port=9092,
            rest_proxy_port=8082,
            ui_port=8080
        )
        
        mock_port_allocator.is_port_available.return_value = True
        
        result = await network_isolation.setup_cluster_isolation(cluster_definition)
        
        assert result["isolation_complete"] is True
        # Should not allocate new ports if existing ones are available
        mock_port_allocator.allocate_ports.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_setup_cluster_isolation_port_allocation_failure(self, network_isolation, 
                                                                  cluster_definition, mock_port_allocator):
        """Test setup failure during port allocation."""
        mock_port_allocator.allocate_ports.side_effect = PortAllocationError("No ports available")
        
        with pytest.raises(NetworkIsolationError):
            await network_isolation.setup_cluster_isolation(cluster_definition)
    
    @pytest.mark.asyncio
    async def test_teardown_cluster_isolation_success(self, network_isolation, 
                                                     mock_port_allocator, mock_network_manager):
        """Test successful cluster isolation teardown."""
        result = await network_isolation.teardown_cluster_isolation("test-cluster")
        
        assert result is True
        mock_port_allocator.release_ports.assert_called_once_with("test-cluster")
        mock_network_manager.delete_cluster_network.assert_called_once_with("test-cluster", False)
    
    @pytest.mark.asyncio
    async def test_teardown_cluster_isolation_partial_failure(self, network_isolation, 
                                                             mock_port_allocator, mock_network_manager):
        """Test teardown with partial failures."""
        mock_port_allocator.release_ports.return_value = False
        mock_network_manager.delete_cluster_network.return_value = True
        
        result = await network_isolation.teardown_cluster_isolation("test-cluster")
        
        assert result is False  # Should return False due to port release failure
    
    @pytest.mark.asyncio
    async def test_validate_cluster_isolation(self, network_isolation, 
                                             mock_port_allocator, mock_network_manager):
        """Test cluster isolation validation."""
        result = await network_isolation.validate_cluster_isolation("test-cluster")
        
        assert result["cluster_id"] == "test-cluster"
        assert result["port_allocation_valid"] is True
        assert result["network_exists"] is True
        assert result["isolation_valid"] is True
        assert len(result["issues"]) == 0
    
    @pytest.mark.asyncio
    async def test_validate_cluster_isolation_with_issues(self, network_isolation, 
                                                         mock_port_allocator, mock_network_manager):
        """Test validation with issues."""
        mock_port_allocator.validate_allocation.return_value = False
        mock_network_manager.network_exists.return_value = False
        
        result = await network_isolation.validate_cluster_isolation("test-cluster")
        
        assert result["isolation_valid"] is False
        assert len(result["issues"]) == 2
        assert "Port allocation is invalid" in result["issues"]
        assert "Cluster network does not exist" in result["issues"]
    
    @pytest.mark.asyncio
    async def test_get_cluster_isolation_info(self, network_isolation, 
                                             mock_port_allocator, mock_network_manager):
        """Test getting cluster isolation information."""
        mock_port_allocator.get_cluster_allocation.return_value = PortAllocation(
            kafka_port=9092,
            rest_proxy_port=8082,
            ui_port=8080
        )
        mock_network_manager.get_network_config.return_value = {"name": "kafka-cluster-test"}
        mock_network_manager.get_network_containers.return_value = []
        
        info = await network_isolation.get_cluster_isolation_info("test-cluster")
        
        assert info is not None
        assert info["cluster_id"] == "test-cluster"
        assert "port_allocation" in info
        assert "network_config" in info
        assert "containers" in info
        assert info["isolation_active"] is True
    
    @pytest.mark.asyncio
    async def test_list_isolated_clusters(self, network_isolation, 
                                         mock_port_allocator, mock_network_manager):
        """Test listing isolated clusters."""
        mock_port_allocator.list_allocations.return_value = {
            "cluster1": PortAllocation(kafka_port=9092, rest_proxy_port=8082, ui_port=8080)
        }
        mock_network_manager.list_cluster_networks.return_value = {
            "cluster1": {"name": "kafka-cluster-cluster1"}
        }
        
        clusters = await network_isolation.list_isolated_clusters()
        
        assert len(clusters) == 1
        assert clusters[0]["cluster_id"] == "cluster1"
        assert clusters[0]["has_port_allocation"] is True
        assert clusters[0]["has_network"] is True
        assert clusters[0]["isolation_complete"] is True
    
    @pytest.mark.asyncio
    async def test_cleanup_orphaned_resources(self, network_isolation, mock_network_manager):
        """Test cleaning up orphaned resources."""
        mock_network_manager.cleanup_orphaned_networks.return_value = ["network1", "network2"]
        
        result = await network_isolation.cleanup_orphaned_resources()
        
        assert result["networks"] == ["network1", "network2"]
        assert result["port_allocations"] == []
    
    @pytest.mark.asyncio
    async def test_get_isolation_stats(self, network_isolation, 
                                      mock_port_allocator, mock_network_manager):
        """Test getting isolation statistics."""
        mock_port_allocator.get_port_usage_stats.return_value = {
            "total_ports": 100,
            "allocated_ports": 10
        }
        mock_network_manager.get_network_stats.return_value = {
            "managed_networks": 5
        }
        
        # Mock list_isolated_clusters
        network_isolation.list_isolated_clusters = AsyncMock(return_value=[
            {"isolation_complete": True},
            {"isolation_complete": False}
        ])
        
        stats = await network_isolation.get_isolation_stats()
        
        assert "port_statistics" in stats
        assert "network_statistics" in stats
        assert stats["isolated_clusters_total"] == 2
        assert stats["complete_isolations"] == 1
        assert stats["partial_isolations"] == 1


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    @pytest.mark.asyncio
    async def test_allocate_cluster_ports(self):
        """Test allocate_cluster_ports convenience function."""
        with patch('src.networking.port_allocator.GlobalPortAllocator.get_instance') as mock_get:
            mock_allocator = AsyncMock()
            mock_allocator.allocate_ports.return_value = PortAllocation(
                kafka_port=9092, rest_proxy_port=8082, ui_port=8080
            )
            mock_get.return_value = mock_allocator
            
            result = await allocate_cluster_ports("test-cluster")
            
            assert result.kafka_port == 9092
            mock_allocator.allocate_ports.assert_called_once_with("test-cluster", None)
    
    @pytest.mark.asyncio
    async def test_release_cluster_ports(self):
        """Test release_cluster_ports convenience function."""
        with patch('src.networking.port_allocator.GlobalPortAllocator.get_instance') as mock_get:
            mock_allocator = AsyncMock()
            mock_allocator.release_ports.return_value = True
            mock_get.return_value = mock_allocator
            
            result = await release_cluster_ports("test-cluster")
            
            assert result is True
            mock_allocator.release_ports.assert_called_once_with("test-cluster")
    
    @pytest.mark.asyncio
    async def test_create_cluster_network(self):
        """Test create_cluster_network convenience function."""
        with patch('src.networking.network_manager.GlobalNetworkManager.get_instance') as mock_get:
            mock_manager = AsyncMock()
            mock_manager.create_cluster_network.return_value = "kafka-cluster-test"
            mock_get.return_value = mock_manager
            
            result = await create_cluster_network("test-cluster")
            
            assert result == "kafka-cluster-test"
            mock_manager.create_cluster_network.assert_called_once_with("test-cluster", None)
    
    @pytest.mark.asyncio
    async def test_setup_cluster_isolation(self):
        """Test setup_cluster_isolation convenience function."""
        with patch('src.networking.isolation.GlobalNetworkIsolation.get_instance') as mock_get:
            mock_isolation = AsyncMock()
            mock_isolation.setup_cluster_isolation.return_value = {"isolation_complete": True}
            mock_get.return_value = mock_isolation
            
            cluster_def = ClusterDefinition(id="test", name="Test")
            result = await setup_cluster_isolation(cluster_def)
            
            assert result["isolation_complete"] is True
            mock_isolation.setup_cluster_isolation.assert_called_once_with(cluster_def, None, None)


if __name__ == "__main__":
    pytest.main([__file__])