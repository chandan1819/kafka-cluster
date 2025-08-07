"""
Tests for cluster registry functionality.
"""

import pytest
import asyncio
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
from typing import Dict, Any

from src.registry.cluster_registry import ClusterRegistry, GlobalClusterRegistry
from src.registry.exceptions import (
    ClusterNotFoundError,
    ClusterAlreadyExistsError,
    ClusterValidationError,
    ClusterConflictError,
    ClusterOperationError
)
from src.models.multi_cluster import ClusterDefinition, ClusterStatus, PortAllocation
from src.storage.base import StorageBackend
from src.networking.port_allocator import PortAllocator
from src.networking.network_manager import NetworkManager


class MockStorageBackend(StorageBackend):
    """Mock storage backend for testing."""
    
    def __init__(self):
        self.clusters: Dict[str, ClusterDefinition] = {}
    
    async def save_cluster(self, cluster: ClusterDefinition) -> bool:
        self.clusters[cluster.id] = cluster
        return True
    
    async def load_cluster(self, cluster_id: str) -> ClusterDefinition:
        if cluster_id not in self.clusters:
            raise KeyError(f"Cluster {cluster_id} not found")
        return self.clusters[cluster_id]
    
    async def delete_cluster(self, cluster_id: str) -> bool:
        if cluster_id in self.clusters:
            del self.clusters[cluster_id]
            return True
        return False
    
    async def list_clusters(self) -> list[ClusterDefinition]:
        return list(self.clusters.values())


@pytest.fixture
def mock_storage():
    """Create mock storage backend."""
    return MockStorageBackend()


@pytest.fixture
def mock_port_allocator():
    """Create mock port allocator."""
    allocator = AsyncMock(spec=PortAllocator)
    allocator.allocate_ports.return_value = PortAllocation(
        kafka_port=9092,
        rest_proxy_port=8082,
        ui_port=8080,
        jmx_port=7000
    )
    allocator.release_ports.return_value = True
    allocator.validate_allocation.return_value = True
    allocator.is_port_available.return_value = True
    return allocator


@pytest.fixture
def mock_network_manager():
    """Create mock network manager."""
    manager = AsyncMock(spec=NetworkManager)
    manager.network_exists.return_value = False
    manager.delete_cluster_network.return_value = True
    return manager


@pytest.fixture
def cluster_registry(mock_storage, mock_port_allocator, mock_network_manager):
    """Create cluster registry for testing."""
    return ClusterRegistry(mock_storage, mock_port_allocator, mock_network_manager)


@pytest.fixture
def sample_cluster_definition():
    """Create sample cluster definition."""
    return ClusterDefinition(
        id="test-cluster",
        name="Test Cluster",
        description="Test cluster for unit tests",
        environment="dev",
        data_directory=Path("/tmp/test-cluster"),
        tags={"purpose": "testing", "team": "dev"}
    )


class TestClusterRegistry:
    """Test ClusterRegistry class."""
    
    @pytest.mark.asyncio
    async def test_initialize_empty_registry(self, cluster_registry):
        """Test initializing empty registry."""
        await cluster_registry.initialize()
        
        assert await cluster_registry.get_cluster_count() == 0
        assert await cluster_registry.list_clusters() == []
    
    @pytest.mark.asyncio
    async def test_initialize_with_existing_clusters(self, mock_storage, mock_port_allocator, mock_network_manager):
        """Test initializing registry with existing clusters."""
        # Add cluster to storage
        cluster = ClusterDefinition(
            id="existing-cluster",
            name="Existing Cluster",
            environment="dev"
        )
        mock_storage.clusters["existing-cluster"] = cluster
        
        registry = ClusterRegistry(mock_storage, mock_port_allocator, mock_network_manager)
        await registry.initialize()
        
        assert await registry.get_cluster_count() == 1
        loaded_cluster = await registry.get_cluster("existing-cluster")
        assert loaded_cluster.id == "existing-cluster"
    
    @pytest.mark.asyncio
    async def test_register_cluster_success(self, cluster_registry, sample_cluster_definition):
        """Test successful cluster registration."""
        await cluster_registry.initialize()
        
        result = await cluster_registry.register_cluster(sample_cluster_definition)
        
        assert result is True
        assert await cluster_registry.cluster_exists("test-cluster")
        
        # Check cluster was saved with metadata
        cluster = await cluster_registry.get_cluster("test-cluster")
        assert cluster.created_at is not None
        assert cluster.updated_at is not None
        assert cluster.status == ClusterStatus.REGISTERED
        assert cluster.port_allocation is not None
        assert cluster.network_name == "kafka-cluster-test-cluster"
    
    @pytest.mark.asyncio
    async def test_register_cluster_duplicate_id(self, cluster_registry, sample_cluster_definition):
        """Test registering cluster with duplicate ID."""
        await cluster_registry.initialize()
        
        # Register first cluster
        await cluster_registry.register_cluster(sample_cluster_definition)
        
        # Try to register duplicate
        with pytest.raises(ClusterAlreadyExistsError) as exc_info:
            await cluster_registry.register_cluster(sample_cluster_definition)
        
        assert exc_info.value.cluster_id == "test-cluster"
    
    @pytest.mark.asyncio
    async def test_register_cluster_validation_error(self, cluster_registry):
        """Test cluster registration with validation errors."""
        await cluster_registry.initialize()
        
        # Create invalid cluster (empty ID)
        invalid_cluster = ClusterDefinition(
            id="",  # Invalid empty ID
            name="Invalid Cluster",
            environment="dev"
        )
        
        with pytest.raises(ClusterValidationError) as exc_info:
            await cluster_registry.register_cluster(invalid_cluster)
        
        assert "Cluster ID is required" in exc_info.value.validation_errors
    
    @pytest.mark.asyncio
    async def test_register_cluster_reserved_name(self, cluster_registry):
        """Test registering cluster with reserved name."""
        await cluster_registry.initialize()
        
        reserved_cluster = ClusterDefinition(
            id="default",  # Reserved name
            name="Default Cluster",
            environment="dev"
        )
        
        with pytest.raises(ClusterValidationError) as exc_info:
            await cluster_registry.register_cluster(reserved_cluster)
        
        assert any("reserved" in error for error in exc_info.value.validation_errors)
    
    @pytest.mark.asyncio
    async def test_register_cluster_port_conflict(self, cluster_registry, mock_port_allocator):
        """Test cluster registration with port conflicts."""
        await cluster_registry.initialize()
        
        # First cluster
        cluster1 = ClusterDefinition(
            id="cluster1",
            name="Cluster 1",
            environment="dev",
            port_allocation=PortAllocation(kafka_port=9092, rest_proxy_port=8082, ui_port=8080)
        )
        await cluster_registry.register_cluster(cluster1)
        
        # Second cluster with same ports
        cluster2 = ClusterDefinition(
            id="cluster2",
            name="Cluster 2",
            environment="dev",
            port_allocation=PortAllocation(kafka_port=9092, rest_proxy_port=8082, ui_port=8080)
        )
        
        with pytest.raises(ClusterConflictError) as exc_info:
            await cluster_registry.register_cluster(cluster2)
        
        assert "Port conflicts" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_unregister_cluster_success(self, cluster_registry, sample_cluster_definition):
        """Test successful cluster unregistration."""
        await cluster_registry.initialize()
        
        # Register cluster
        await cluster_registry.register_cluster(sample_cluster_definition)
        assert await cluster_registry.cluster_exists("test-cluster")
        
        # Unregister cluster
        result = await cluster_registry.unregister_cluster("test-cluster")
        
        assert result is True
        assert not await cluster_registry.cluster_exists("test-cluster")
    
    @pytest.mark.asyncio
    async def test_unregister_cluster_not_found(self, cluster_registry):
        """Test unregistering non-existent cluster."""
        await cluster_registry.initialize()
        
        with pytest.raises(ClusterNotFoundError) as exc_info:
            await cluster_registry.unregister_cluster("nonexistent")
        
        assert exc_info.value.cluster_id == "nonexistent"
    
    @pytest.mark.asyncio
    async def test_unregister_running_cluster_without_force(self, cluster_registry, sample_cluster_definition):
        """Test unregistering running cluster without force."""
        await cluster_registry.initialize()
        
        # Register and mark as running
        await cluster_registry.register_cluster(sample_cluster_definition)
        await cluster_registry.update_cluster_status("test-cluster", ClusterStatus.RUNNING)
        
        with pytest.raises(ClusterOperationError) as exc_info:
            await cluster_registry.unregister_cluster("test-cluster", force=False)
        
        assert "running" in str(exc_info.value).lower()
    
    @pytest.mark.asyncio
    async def test_unregister_running_cluster_with_force(self, cluster_registry, sample_cluster_definition):
        """Test force unregistering running cluster."""
        await cluster_registry.initialize()
        
        # Register and mark as running
        await cluster_registry.register_cluster(sample_cluster_definition)
        await cluster_registry.update_cluster_status("test-cluster", ClusterStatus.RUNNING)
        
        # Force unregister should succeed
        result = await cluster_registry.unregister_cluster("test-cluster", force=True)
        
        assert result is True
        assert not await cluster_registry.cluster_exists("test-cluster")
    
    @pytest.mark.asyncio
    async def test_get_cluster_success(self, cluster_registry, sample_cluster_definition):
        """Test getting cluster definition."""
        await cluster_registry.initialize()
        await cluster_registry.register_cluster(sample_cluster_definition)
        
        cluster = await cluster_registry.get_cluster("test-cluster")
        
        assert cluster.id == "test-cluster"
        assert cluster.name == "Test Cluster"
        assert cluster.environment == "dev"
    
    @pytest.mark.asyncio
    async def test_get_cluster_not_found(self, cluster_registry):
        """Test getting non-existent cluster."""
        await cluster_registry.initialize()
        
        with pytest.raises(ClusterNotFoundError):
            await cluster_registry.get_cluster("nonexistent")
    
    @pytest.mark.asyncio
    async def test_list_clusters_no_filter(self, cluster_registry):
        """Test listing all clusters without filters."""
        await cluster_registry.initialize()
        
        # Register multiple clusters
        clusters = [
            ClusterDefinition(id="cluster1", name="Cluster 1", environment="dev"),
            ClusterDefinition(id="cluster2", name="Cluster 2", environment="test"),
            ClusterDefinition(id="cluster3", name="Cluster 3", environment="dev")
        ]
        
        for cluster in clusters:
            await cluster_registry.register_cluster(cluster)
        
        all_clusters = await cluster_registry.list_clusters()
        
        assert len(all_clusters) == 3
        cluster_ids = {c.id for c in all_clusters}
        assert cluster_ids == {"cluster1", "cluster2", "cluster3"}
    
    @pytest.mark.asyncio
    async def test_list_clusters_with_environment_filter(self, cluster_registry):
        """Test listing clusters with environment filter."""
        await cluster_registry.initialize()
        
        # Register clusters in different environments
        clusters = [
            ClusterDefinition(id="dev1", name="Dev 1", environment="dev"),
            ClusterDefinition(id="dev2", name="Dev 2", environment="dev"),
            ClusterDefinition(id="test1", name="Test 1", environment="test")
        ]
        
        for cluster in clusters:
            await cluster_registry.register_cluster(cluster)
        
        dev_clusters = await cluster_registry.list_clusters(environment_filter="dev")
        
        assert len(dev_clusters) == 2
        assert all(c.environment == "dev" for c in dev_clusters)
    
    @pytest.mark.asyncio
    async def test_list_clusters_with_status_filter(self, cluster_registry):
        """Test listing clusters with status filter."""
        await cluster_registry.initialize()
        
        # Register clusters
        clusters = [
            ClusterDefinition(id="cluster1", name="Cluster 1", environment="dev"),
            ClusterDefinition(id="cluster2", name="Cluster 2", environment="dev")
        ]
        
        for cluster in clusters:
            await cluster_registry.register_cluster(cluster)
        
        # Update one cluster status
        await cluster_registry.update_cluster_status("cluster1", ClusterStatus.RUNNING)
        
        running_clusters = await cluster_registry.list_clusters(status_filter=ClusterStatus.RUNNING)
        
        assert len(running_clusters) == 1
        assert running_clusters[0].id == "cluster1"
    
    @pytest.mark.asyncio
    async def test_list_clusters_with_tag_filter(self, cluster_registry):
        """Test listing clusters with tag filter."""
        await cluster_registry.initialize()
        
        # Register clusters with different tags
        clusters = [
            ClusterDefinition(id="cluster1", name="Cluster 1", environment="dev", 
                            tags={"team": "backend", "purpose": "testing"}),
            ClusterDefinition(id="cluster2", name="Cluster 2", environment="dev",
                            tags={"team": "frontend", "purpose": "testing"}),
            ClusterDefinition(id="cluster3", name="Cluster 3", environment="dev",
                            tags={"team": "backend", "purpose": "development"})
        ]
        
        for cluster in clusters:
            await cluster_registry.register_cluster(cluster)
        
        backend_clusters = await cluster_registry.list_clusters(tag_filter={"team": "backend"})
        
        assert len(backend_clusters) == 2
        assert all(c.tags.get("team") == "backend" for c in backend_clusters)
    
    @pytest.mark.asyncio
    async def test_update_cluster_success(self, cluster_registry, sample_cluster_definition):
        """Test successful cluster update."""
        await cluster_registry.initialize()
        await cluster_registry.register_cluster(sample_cluster_definition)
        
        updates = {
            "name": "Updated Test Cluster",
            "description": "Updated description",
            "tags": {"purpose": "updated-testing"}
        }
        
        updated_cluster = await cluster_registry.update_cluster("test-cluster", updates)
        
        assert updated_cluster.name == "Updated Test Cluster"
        assert updated_cluster.description == "Updated description"
        assert updated_cluster.tags["purpose"] == "updated-testing"
        assert updated_cluster.updated_at > updated_cluster.created_at
    
    @pytest.mark.asyncio
    async def test_update_cluster_not_found(self, cluster_registry):
        """Test updating non-existent cluster."""
        await cluster_registry.initialize()
        
        with pytest.raises(ClusterNotFoundError):
            await cluster_registry.update_cluster("nonexistent", {"name": "New Name"})
    
    @pytest.mark.asyncio
    async def test_update_cluster_validation_error(self, cluster_registry, sample_cluster_definition):
        """Test cluster update with validation errors."""
        await cluster_registry.initialize()
        await cluster_registry.register_cluster(sample_cluster_definition)
        
        # Try to update with invalid environment
        updates = {"environment": "invalid-env"}
        
        with pytest.raises(ClusterValidationError):
            await cluster_registry.update_cluster("test-cluster", updates)
    
    @pytest.mark.asyncio
    async def test_cluster_status_management(self, cluster_registry, sample_cluster_definition):
        """Test cluster status management."""
        await cluster_registry.initialize()
        await cluster_registry.register_cluster(sample_cluster_definition)
        
        # Initial status should be REGISTERED
        status = await cluster_registry.get_cluster_status("test-cluster")
        assert status == ClusterStatus.REGISTERED
        
        # Update status to RUNNING
        await cluster_registry.update_cluster_status("test-cluster", ClusterStatus.RUNNING)
        status = await cluster_registry.get_cluster_status("test-cluster")
        assert status == ClusterStatus.RUNNING
        
        # Check that timestamps are updated
        cluster = await cluster_registry.get_cluster("test-cluster")
        assert cluster.last_started is not None
        
        # Update status to STOPPED
        await cluster_registry.update_cluster_status("test-cluster", ClusterStatus.STOPPED)
        cluster = await cluster_registry.get_cluster("test-cluster")
        assert cluster.last_stopped is not None
    
    @pytest.mark.asyncio
    async def test_get_all_cluster_status(self, cluster_registry):
        """Test getting all cluster statuses."""
        await cluster_registry.initialize()
        
        # Register multiple clusters
        clusters = [
            ClusterDefinition(id="cluster1", name="Cluster 1", environment="dev"),
            ClusterDefinition(id="cluster2", name="Cluster 2", environment="test")
        ]
        
        for cluster in clusters:
            await cluster_registry.register_cluster(cluster)
        
        # Update statuses
        await cluster_registry.update_cluster_status("cluster1", ClusterStatus.RUNNING)
        await cluster_registry.update_cluster_status("cluster2", ClusterStatus.STOPPED)
        
        all_status = await cluster_registry.get_all_cluster_status()
        
        assert len(all_status) == 2
        assert all_status["cluster1"] == ClusterStatus.RUNNING
        assert all_status["cluster2"] == ClusterStatus.STOPPED
    
    @pytest.mark.asyncio
    async def test_search_clusters(self, cluster_registry):
        """Test cluster search functionality."""
        await cluster_registry.initialize()
        
        # Register clusters with different names and descriptions
        clusters = [
            ClusterDefinition(id="kafka-dev", name="Kafka Development", 
                            description="Development Kafka cluster", environment="dev"),
            ClusterDefinition(id="kafka-test", name="Kafka Testing",
                            description="Testing environment", environment="test"),
            ClusterDefinition(id="redis-dev", name="Redis Development",
                            description="Redis cache cluster", environment="dev")
        ]
        
        for cluster in clusters:
            await cluster_registry.register_cluster(cluster)
        
        # Search by name
        kafka_clusters = await cluster_registry.search_clusters("kafka")
        assert len(kafka_clusters) == 2
        
        # Search by description
        dev_clusters = await cluster_registry.search_clusters("development")
        assert len(dev_clusters) == 2
        
        # Search by environment
        test_clusters = await cluster_registry.search_clusters("testing")
        assert len(test_clusters) == 2  # One by name, one by description
    
    @pytest.mark.asyncio
    async def test_get_clusters_by_environment(self, cluster_registry):
        """Test getting clusters by environment."""
        await cluster_registry.initialize()
        
        # Register clusters in different environments
        clusters = [
            ClusterDefinition(id="dev1", name="Dev 1", environment="dev"),
            ClusterDefinition(id="dev2", name="Dev 2", environment="dev"),
            ClusterDefinition(id="prod1", name="Prod 1", environment="prod")
        ]
        
        for cluster in clusters:
            await cluster_registry.register_cluster(cluster)
        
        dev_clusters = await cluster_registry.get_clusters_by_environment("dev")
        
        assert len(dev_clusters) == 2
        assert all(c.environment == "dev" for c in dev_clusters)
    
    @pytest.mark.asyncio
    async def test_get_clusters_by_tag(self, cluster_registry):
        """Test getting clusters by tag."""
        await cluster_registry.initialize()
        
        # Register clusters with different tags
        clusters = [
            ClusterDefinition(id="cluster1", name="Cluster 1", environment="dev",
                            tags={"team": "backend", "version": "v1"}),
            ClusterDefinition(id="cluster2", name="Cluster 2", environment="dev",
                            tags={"team": "backend", "version": "v2"}),
            ClusterDefinition(id="cluster3", name="Cluster 3", environment="dev",
                            tags={"team": "frontend", "version": "v1"})
        ]
        
        for cluster in clusters:
            await cluster_registry.register_cluster(cluster)
        
        # Get clusters by team tag
        backend_clusters = await cluster_registry.get_clusters_by_tag("team", "backend")
        assert len(backend_clusters) == 2
        
        # Get clusters by version tag (any value)
        v1_clusters = await cluster_registry.get_clusters_by_tag("version", "v1")
        assert len(v1_clusters) == 2
        
        # Get clusters with team tag (any value)
        team_clusters = await cluster_registry.get_clusters_by_tag("team")
        assert len(team_clusters) == 3
    
    @pytest.mark.asyncio
    async def test_get_registry_stats(self, cluster_registry):
        """Test getting registry statistics."""
        await cluster_registry.initialize()
        
        # Register clusters in different environments and statuses
        clusters = [
            ClusterDefinition(id="dev1", name="Dev 1", environment="dev"),
            ClusterDefinition(id="dev2", name="Dev 2", environment="dev"),
            ClusterDefinition(id="prod1", name="Prod 1", environment="prod")
        ]
        
        for cluster in clusters:
            await cluster_registry.register_cluster(cluster)
        
        # Update some statuses
        await cluster_registry.update_cluster_status("dev1", ClusterStatus.RUNNING)
        await cluster_registry.update_cluster_status("dev2", ClusterStatus.STOPPED)
        
        stats = await cluster_registry.get_registry_stats()
        
        assert stats["total_clusters"] == 3
        assert stats["environment_distribution"]["dev"] == 2
        assert stats["environment_distribution"]["prod"] == 1
        assert stats["status_distribution"]["running"] == 1
        assert stats["status_distribution"]["stopped"] == 1
        assert stats["status_distribution"]["registered"] == 1
        assert "storage_backend" in stats
        assert "last_updated" in stats
    
    @pytest.mark.asyncio
    async def test_validate_cluster_health(self, cluster_registry, sample_cluster_definition):
        """Test cluster health validation."""
        await cluster_registry.initialize()
        await cluster_registry.register_cluster(sample_cluster_definition)
        
        health = await cluster_registry.validate_cluster_health("test-cluster")
        
        assert health["cluster_id"] == "test-cluster"
        assert health["overall_health"] in ["healthy", "unhealthy", "error"]
        assert "checks" in health
        assert "issues" in health
        assert "warnings" in health
        
        # Should have checks for port allocation, network, data directory, and configuration
        assert "port_allocation" in health["checks"]
        assert "network_exists" in health["checks"]
        assert "data_directory_exists" in health["checks"]
        assert "configuration_valid" in health["checks"]
    
    @pytest.mark.asyncio
    async def test_validate_cluster_health_not_found(self, cluster_registry):
        """Test health validation for non-existent cluster."""
        await cluster_registry.initialize()
        
        with pytest.raises(ClusterNotFoundError):
            await cluster_registry.validate_cluster_health("nonexistent")


class TestGlobalClusterRegistry:
    """Test GlobalClusterRegistry singleton."""
    
    @pytest.mark.asyncio
    async def test_singleton_behavior(self):
        """Test that GlobalClusterRegistry returns same instance."""
        # Reset instance first
        await GlobalClusterRegistry.reset_instance()
        
        instance1 = await GlobalClusterRegistry.get_instance()
        instance2 = await GlobalClusterRegistry.get_instance()
        
        assert instance1 is instance2
    
    @pytest.mark.asyncio
    async def test_reset_instance(self):
        """Test resetting global instance."""
        instance1 = await GlobalClusterRegistry.get_instance()
        await GlobalClusterRegistry.reset_instance()
        instance2 = await GlobalClusterRegistry.get_instance()
        
        assert instance1 is not instance2


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    @pytest.mark.asyncio
    async def test_convenience_functions(self):
        """Test convenience functions work correctly."""
        from src.registry.cluster_registry import (
            get_cluster_registry, register_cluster, get_cluster, list_clusters
        )
        
        # Reset global instance
        await GlobalClusterRegistry.reset_instance()
        
        # Test get_cluster_registry
        registry = await get_cluster_registry()
        assert isinstance(registry, ClusterRegistry)
        
        # Test register_cluster
        cluster = ClusterDefinition(
            id="test-convenience",
            name="Test Convenience",
            environment="dev"
        )
        
        with patch.object(registry, 'register_cluster', return_value=True) as mock_register:
            result = await register_cluster(cluster)
            assert result is True
            mock_register.assert_called_once_with(cluster)
        
        # Test get_cluster
        with patch.object(registry, 'get_cluster', return_value=cluster) as mock_get:
            result = await get_cluster("test-convenience")
            assert result == cluster
            mock_get.assert_called_once_with("test-convenience")
        
        # Test list_clusters
        with patch.object(registry, 'list_clusters', return_value=[cluster]) as mock_list:
            result = await list_clusters(environment_filter="dev")
            assert result == [cluster]
            mock_list.assert_called_once_with(environment_filter="dev")


if __name__ == "__main__":
    pytest.main([__file__])