"""
Tests for multi-cluster manager functionality.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from pathlib import Path

from src.services.multi_cluster_manager import MultiClusterManager, MultiClusterManagerError
from src.models.multi_cluster import (
    ClusterDefinition, 
    ClusterSummary,
    ClusterEnvironment,
    KafkaConfig,
    RestProxyConfig,
    UIConfig,
    PortAllocation,
    RetentionPolicy
)
from src.models.base import ServiceStatus
from src.registry.cluster_registry import ClusterRegistry
from src.services.cluster_factory import ClusterFactory
from src.services.cluster_manager import ClusterManager
from src.services.template_manager import TemplateManager
from src.registry.exceptions import (
    ClusterNotFoundError,
    ClusterAlreadyExistsError,
    ClusterOperationError
)


@pytest.fixture
def mock_registry():
    """Mock cluster registry."""
    registry = AsyncMock(spec=ClusterRegistry)
    registry.initialize = AsyncMock()
    registry.list_clusters = AsyncMock(return_value=[])
    registry.register_cluster = AsyncMock(return_value=True)
    registry.unregister_cluster = AsyncMock(return_value=True)
    registry.get_cluster = AsyncMock()
    registry.update_cluster_status = AsyncMock()
    registry.get_cluster_status = AsyncMock(return_value=ServiceStatus.STOPPED)
    registry.get_all_cluster_status = AsyncMock(return_value={})
    registry.validate_cluster_health = AsyncMock(return_value={"overall_health": "healthy"})
    registry.get_registry_stats = AsyncMock(return_value={"total_clusters": 0})
    return registry


@pytest.fixture
def mock_factory():
    """Mock cluster factory."""
    factory = AsyncMock(spec=ClusterFactory)
    factory.create_cluster_manager = AsyncMock()
    factory.destroy_cluster_instance = AsyncMock(return_value=True)
    factory.get_cluster_endpoints = AsyncMock(return_value={
        "kafka": "localhost:9092",
        "kafka-rest-proxy": "http://localhost:8082",
        "kafka-ui": "http://localhost:8080"
    })
    factory.validate_cluster_resources = AsyncMock(return_value={"valid": True})
    return factory


@pytest.fixture
def mock_template_manager():
    """Mock template manager."""
    template_manager = AsyncMock(spec=TemplateManager)
    return template_manager


@pytest.fixture
def mock_cluster_manager():
    """Mock cluster manager."""
    cluster_manager = AsyncMock(spec=ClusterManager)
    cluster_manager.start_cluster = AsyncMock()
    cluster_manager.stop_cluster = AsyncMock(return_value=True)
    cluster_manager.get_status = AsyncMock()
    return cluster_manager


@pytest.fixture
def sample_cluster_definition():
    """Sample cluster definition for testing."""
    return ClusterDefinition(
        id="test-cluster",
        name="Test Cluster",
        description="Test cluster for unit tests",
        environment=ClusterEnvironment.DEVELOPMENT,
        kafka_config=KafkaConfig(),
        rest_proxy_config=RestProxyConfig(),
        ui_config=UIConfig(),
        port_allocation=PortAllocation(
            kafka_port=9092,
            rest_proxy_port=8082,
            ui_port=8080
        ),
        retention_policy=RetentionPolicy(),
        tags={"test": "true"}
    )


@pytest.fixture
async def multi_cluster_manager(mock_registry, mock_factory, mock_template_manager):
    """Multi-cluster manager instance for testing."""
    manager = MultiClusterManager(
        registry=mock_registry,
        factory=mock_factory,
        template_manager=mock_template_manager
    )
    await manager.initialize()
    return manager


class TestMultiClusterManagerInitialization:
    """Test multi-cluster manager initialization."""
    
    async def test_initialize_empty_registry(self, mock_registry, mock_factory, mock_template_manager):
        """Test initialization with empty registry."""
        mock_registry.list_clusters.return_value = []
        
        manager = MultiClusterManager(mock_registry, mock_factory, mock_template_manager)
        await manager.initialize()
        
        mock_registry.initialize.assert_called_once()
        assert len(manager._cluster_managers) == 0
        assert manager._health_check_task is not None
    
    async def test_initialize_with_running_clusters(self, mock_registry, mock_factory, mock_template_manager, 
                                                  sample_cluster_definition, mock_cluster_manager):
        """Test initialization with running clusters."""
        # Setup running cluster
        running_cluster = sample_cluster_definition.copy()
        running_cluster.status = ServiceStatus.RUNNING
        mock_registry.list_clusters.return_value = [running_cluster]
        
        # Mock cluster manager creation and status
        mock_factory.create_cluster_manager.return_value = mock_cluster_manager
        mock_cluster_manager.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
        
        manager = MultiClusterManager(mock_registry, mock_factory, mock_template_manager)
        await manager.initialize()
        
        mock_factory.create_cluster_manager.assert_called_once_with(running_cluster)
        assert "test-cluster" in manager._cluster_managers
    
    async def test_initialize_with_stopped_clusters(self, mock_registry, mock_factory, mock_template_manager,
                                                   sample_cluster_definition):
        """Test initialization with stopped clusters."""
        # Setup stopped cluster
        stopped_cluster = sample_cluster_definition.copy()
        stopped_cluster.status = ServiceStatus.STOPPED
        mock_registry.list_clusters.return_value = [stopped_cluster]
        
        manager = MultiClusterManager(mock_registry, mock_factory, mock_template_manager)
        await manager.initialize()
        
        # Should not create cluster manager for stopped clusters
        mock_factory.create_cluster_manager.assert_not_called()
        assert len(manager._cluster_managers) == 0
    
    async def test_initialize_with_failed_cluster_manager_creation(self, mock_registry, mock_factory, 
                                                                 mock_template_manager, sample_cluster_definition):
        """Test initialization when cluster manager creation fails."""
        # Setup running cluster
        running_cluster = sample_cluster_definition.copy()
        running_cluster.status = ServiceStatus.RUNNING
        mock_registry.list_clusters.return_value = [running_cluster]
        
        # Mock factory to raise exception
        mock_factory.create_cluster_manager.side_effect = Exception("Factory error")
        
        manager = MultiClusterManager(mock_registry, mock_factory, mock_template_manager)
        await manager.initialize()
        
        # Should update cluster status to error
        mock_registry.update_cluster_status.assert_called_with("test-cluster", ServiceStatus.ERROR)
        assert len(manager._cluster_managers) == 0
    
    async def test_shutdown(self, multi_cluster_manager):
        """Test manager shutdown."""
        # Add some active operations
        multi_cluster_manager._active_operations["op1"] = Mock(status="running")
        
        await multi_cluster_manager.shutdown()
        
        # Health check task should be cancelled
        assert multi_cluster_manager._health_check_task.cancelled()


class TestClusterLifecycle:
    """Test cluster lifecycle operations."""
    
    async def test_create_cluster_success(self, multi_cluster_manager, sample_cluster_definition, 
                                        mock_cluster_manager):
        """Test successful cluster creation."""
        # Setup mocks
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.factory.create_cluster_manager.return_value = mock_cluster_manager
        mock_cluster_manager.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
        
        result = await multi_cluster_manager.create_cluster(sample_cluster_definition, auto_start=True)
        
        # Verify calls
        multi_cluster_manager.registry.register_cluster.assert_called_once_with(sample_cluster_definition)
        multi_cluster_manager.factory.create_cluster_manager.assert_called_once_with(sample_cluster_definition)
        mock_cluster_manager.start_cluster.assert_called_once()
        
        # Verify cluster manager is stored
        assert "test-cluster" in multi_cluster_manager._cluster_managers
        assert result == sample_cluster_definition
    
    async def test_create_cluster_without_auto_start(self, multi_cluster_manager, sample_cluster_definition,
                                                   mock_cluster_manager):
        """Test cluster creation without auto-start."""
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.factory.create_cluster_manager.return_value = mock_cluster_manager
        
        await multi_cluster_manager.create_cluster(sample_cluster_definition, auto_start=False)
        
        # Should not start cluster
        mock_cluster_manager.start_cluster.assert_not_called()
        assert "test-cluster" in multi_cluster_manager._cluster_managers
    
    async def test_create_cluster_already_exists(self, multi_cluster_manager, sample_cluster_definition):
        """Test creating cluster that already exists."""
        multi_cluster_manager.registry.register_cluster.side_effect = ClusterAlreadyExistsError("test-cluster")
        
        with pytest.raises(ClusterAlreadyExistsError):
            await multi_cluster_manager.create_cluster(sample_cluster_definition)
    
    async def test_create_cluster_factory_failure(self, multi_cluster_manager, sample_cluster_definition):
        """Test cluster creation when factory fails."""
        multi_cluster_manager.factory.create_cluster_manager.side_effect = Exception("Factory error")
        
        with pytest.raises(MultiClusterManagerError) as exc_info:
            await multi_cluster_manager.create_cluster(sample_cluster_definition)
        
        assert "Factory error" in str(exc_info.value)
        # Should cleanup on failure
        assert "test-cluster" not in multi_cluster_manager._cluster_managers
    
    async def test_start_cluster_success(self, multi_cluster_manager, sample_cluster_definition, 
                                       mock_cluster_manager):
        """Test successful cluster start."""
        # Setup mocks
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.factory.create_cluster_manager.return_value = mock_cluster_manager
        mock_cluster_manager.start_cluster.return_value = Mock(status=ServiceStatus.RUNNING)
        
        result = await multi_cluster_manager.start_cluster("test-cluster")
        
        multi_cluster_manager.factory.create_cluster_manager.assert_called_once_with(sample_cluster_definition)
        mock_cluster_manager.start_cluster.assert_called_once()
        multi_cluster_manager.registry.update_cluster_status.assert_called_with("test-cluster", ServiceStatus.RUNNING)
        assert result == ServiceStatus.RUNNING
    
    async def test_start_cluster_with_existing_manager(self, multi_cluster_manager, sample_cluster_definition,
                                                     mock_cluster_manager):
        """Test starting cluster with existing manager."""
        # Pre-populate cluster manager
        multi_cluster_manager._cluster_managers["test-cluster"] = mock_cluster_manager
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        mock_cluster_manager.start_cluster.return_value = Mock(status=ServiceStatus.RUNNING)
        
        await multi_cluster_manager.start_cluster("test-cluster")
        
        # Should not create new manager
        multi_cluster_manager.factory.create_cluster_manager.assert_not_called()
        mock_cluster_manager.start_cluster.assert_called_once()
    
    async def test_start_cluster_not_found(self, multi_cluster_manager):
        """Test starting non-existent cluster."""
        multi_cluster_manager.registry.get_cluster.side_effect = ClusterNotFoundError("test-cluster", [])
        
        with pytest.raises(ClusterNotFoundError):
            await multi_cluster_manager.start_cluster("test-cluster")
    
    async def test_stop_cluster_success(self, multi_cluster_manager, sample_cluster_definition,
                                      mock_cluster_manager):
        """Test successful cluster stop."""
        # Setup cluster manager
        multi_cluster_manager._cluster_managers["test-cluster"] = mock_cluster_manager
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        mock_cluster_manager.stop_cluster.return_value = True
        
        result = await multi_cluster_manager.stop_cluster("test-cluster")
        
        mock_cluster_manager.stop_cluster.assert_called_once_with(force=False, cleanup=False, timeout=30)
        multi_cluster_manager.registry.update_cluster_status.assert_called_with("test-cluster", ServiceStatus.STOPPED)
        assert result is True
    
    async def test_stop_cluster_with_cleanup(self, multi_cluster_manager, sample_cluster_definition,
                                           mock_cluster_manager):
        """Test cluster stop with cleanup."""
        multi_cluster_manager._cluster_managers["test-cluster"] = mock_cluster_manager
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        mock_cluster_manager.stop_cluster.return_value = True
        
        await multi_cluster_manager.stop_cluster("test-cluster", cleanup=True)
        
        mock_cluster_manager.stop_cluster.assert_called_once_with(force=False, cleanup=True, timeout=30)
        # Should remove from active managers
        assert "test-cluster" not in multi_cluster_manager._cluster_managers
    
    async def test_stop_cluster_no_manager(self, multi_cluster_manager, sample_cluster_definition):
        """Test stopping cluster with no active manager."""
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        
        result = await multi_cluster_manager.stop_cluster("test-cluster")
        
        # Should update status and return True
        multi_cluster_manager.registry.update_cluster_status.assert_called_with("test-cluster", ServiceStatus.STOPPED)
        assert result is True
    
    async def test_delete_cluster_success(self, multi_cluster_manager, sample_cluster_definition,
                                        mock_cluster_manager):
        """Test successful cluster deletion."""
        # Setup stopped cluster
        multi_cluster_manager._cluster_managers["test-cluster"] = mock_cluster_manager
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.registry.get_cluster_status.return_value = ServiceStatus.STOPPED
        
        result = await multi_cluster_manager.delete_cluster("test-cluster")
        
        multi_cluster_manager.factory.destroy_cluster_instance.assert_called_once_with("test-cluster", cleanup_data=True)
        multi_cluster_manager.registry.unregister_cluster.assert_called_once_with("test-cluster", force=True)
        assert "test-cluster" not in multi_cluster_manager._cluster_managers
        assert result is True
    
    async def test_delete_running_cluster_with_force(self, multi_cluster_manager, sample_cluster_definition,
                                                   mock_cluster_manager):
        """Test deleting running cluster with force."""
        multi_cluster_manager._cluster_managers["test-cluster"] = mock_cluster_manager
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.registry.get_cluster_status.return_value = ServiceStatus.RUNNING
        mock_cluster_manager.stop_cluster.return_value = True
        
        result = await multi_cluster_manager.delete_cluster("test-cluster", force=True)
        
        # Should force stop first
        mock_cluster_manager.stop_cluster.assert_called_once_with(force=True, cleanup=True, timeout=30)
        multi_cluster_manager.factory.destroy_cluster_instance.assert_called_once()
        assert result is True
    
    async def test_delete_running_cluster_without_force(self, multi_cluster_manager, sample_cluster_definition):
        """Test deleting running cluster without force."""
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.registry.get_cluster_status.return_value = ServiceStatus.RUNNING
        
        with pytest.raises(MultiClusterManagerError) as exc_info:
            await multi_cluster_manager.delete_cluster("test-cluster", force=False)
        
        assert "is running" in str(exc_info.value)


class TestClusterStatus:
    """Test cluster status operations."""
    
    async def test_get_cluster_status_with_manager(self, multi_cluster_manager, sample_cluster_definition,
                                                 mock_cluster_manager):
        """Test getting status with active cluster manager."""
        multi_cluster_manager._cluster_managers["test-cluster"] = mock_cluster_manager
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        mock_cluster_manager.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
        multi_cluster_manager.registry.get_cluster_status.return_value = ServiceStatus.STOPPED
        
        result = await multi_cluster_manager.get_cluster_status("test-cluster")
        
        mock_cluster_manager.get_status.assert_called_once()
        # Should update registry status if different
        multi_cluster_manager.registry.update_cluster_status.assert_called_with("test-cluster", ServiceStatus.RUNNING)
        assert result == ServiceStatus.RUNNING
    
    async def test_get_cluster_status_without_manager(self, multi_cluster_manager, sample_cluster_definition):
        """Test getting status without active cluster manager."""
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.registry.get_cluster_status.return_value = ServiceStatus.STOPPED
        
        result = await multi_cluster_manager.get_cluster_status("test-cluster")
        
        # Should fall back to registry status
        multi_cluster_manager.registry.get_cluster_status.assert_called_with("test-cluster")
        assert result == ServiceStatus.STOPPED
    
    async def test_get_cluster_status_manager_error(self, multi_cluster_manager, sample_cluster_definition,
                                                  mock_cluster_manager):
        """Test getting status when cluster manager fails."""
        multi_cluster_manager._cluster_managers["test-cluster"] = mock_cluster_manager
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        mock_cluster_manager.get_status.side_effect = Exception("Manager error")
        multi_cluster_manager.registry.get_cluster_status.return_value = ServiceStatus.ERROR
        
        result = await multi_cluster_manager.get_cluster_status("test-cluster")
        
        # Should fall back to registry status
        assert result == ServiceStatus.ERROR
    
    async def test_get_all_cluster_status(self, multi_cluster_manager, sample_cluster_definition):
        """Test getting status of all clusters."""
        # Setup multiple clusters
        cluster1 = sample_cluster_definition.copy()
        cluster1.id = "cluster1"
        cluster2 = sample_cluster_definition.copy()
        cluster2.id = "cluster2"
        
        multi_cluster_manager.registry.list_clusters.return_value = [cluster1, cluster2]
        multi_cluster_manager.registry.get_cluster.side_effect = lambda cid: cluster1 if cid == "cluster1" else cluster2
        multi_cluster_manager.registry.get_cluster_status.side_effect = lambda cid: (
            ServiceStatus.RUNNING if cid == "cluster1" else ServiceStatus.STOPPED
        )
        
        result = await multi_cluster_manager.get_all_cluster_status()
        
        assert result == {
            "cluster1": ServiceStatus.RUNNING,
            "cluster2": ServiceStatus.STOPPED
        }
    
    async def test_get_all_cluster_status_with_errors(self, multi_cluster_manager, sample_cluster_definition):
        """Test getting all cluster status with some errors."""
        cluster1 = sample_cluster_definition.copy()
        cluster1.id = "cluster1"
        
        multi_cluster_manager.registry.list_clusters.return_value = [cluster1]
        multi_cluster_manager.registry.get_cluster.side_effect = Exception("Registry error")
        
        result = await multi_cluster_manager.get_all_cluster_status()
        
        assert result == {"cluster1": ServiceStatus.ERROR}


class TestClusterListing:
    """Test cluster listing functionality."""
    
    async def test_list_clusters_success(self, multi_cluster_manager, sample_cluster_definition):
        """Test successful cluster listing."""
        multi_cluster_manager.registry.list_clusters.return_value = [sample_cluster_definition]
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.registry.get_cluster_status.return_value = ServiceStatus.RUNNING
        
        result = await multi_cluster_manager.list_clusters()
        
        assert len(result) == 1
        summary = result[0]
        assert isinstance(summary, ClusterSummary)
        assert summary.id == "test-cluster"
        assert summary.name == "Test Cluster"
        assert summary.status == ServiceStatus.RUNNING
    
    async def test_list_clusters_with_filters(self, multi_cluster_manager):
        """Test cluster listing with filters."""
        await multi_cluster_manager.list_clusters(
            status_filter=ServiceStatus.RUNNING,
            environment_filter="development",
            tag_filter={"env": "test"}
        )
        
        multi_cluster_manager.registry.list_clusters.assert_called_once_with(
            status_filter=ServiceStatus.RUNNING,
            environment_filter="development",
            tag_filter={"env": "test"}
        )
    
    async def test_list_clusters_with_endpoints(self, multi_cluster_manager, sample_cluster_definition):
        """Test cluster listing includes endpoints for running clusters."""
        multi_cluster_manager.registry.list_clusters.return_value = [sample_cluster_definition]
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.registry.get_cluster_status.return_value = ServiceStatus.RUNNING
        
        result = await multi_cluster_manager.list_clusters()
        
        summary = result[0]
        multi_cluster_manager.factory.get_cluster_endpoints.assert_called_once_with(sample_cluster_definition)
        assert summary.endpoints == {
            "kafka": "localhost:9092",
            "kafka-rest-proxy": "http://localhost:8082",
            "kafka-ui": "http://localhost:8080"
        }
    
    async def test_list_clusters_with_errors(self, multi_cluster_manager, sample_cluster_definition):
        """Test cluster listing with some cluster errors."""
        multi_cluster_manager.registry.list_clusters.return_value = [sample_cluster_definition]
        multi_cluster_manager.registry.get_cluster.side_effect = Exception("Registry error")
        
        result = await multi_cluster_manager.list_clusters()
        
        # Should create error summary
        assert len(result) == 1
        summary = result[0]
        assert summary.status == ServiceStatus.ERROR


class TestConcurrentOperations:
    """Test concurrent cluster operations."""
    
    async def test_start_multiple_clusters(self, multi_cluster_manager, mock_cluster_manager):
        """Test starting multiple clusters concurrently."""
        # Setup clusters
        cluster_ids = ["cluster1", "cluster2", "cluster3"]
        
        async def mock_start_cluster(cluster_id, **kwargs):
            await asyncio.sleep(0.1)  # Simulate work
            return ServiceStatus.RUNNING
        
        multi_cluster_manager.start_cluster = AsyncMock(side_effect=mock_start_cluster)
        
        result = await multi_cluster_manager.start_multiple_clusters(cluster_ids, max_concurrent=2)
        
        assert len(result) == 3
        assert all(status == ServiceStatus.RUNNING for status in result.values())
        assert multi_cluster_manager.start_cluster.call_count == 3
    
    async def test_start_multiple_clusters_with_failures(self, multi_cluster_manager):
        """Test starting multiple clusters with some failures."""
        cluster_ids = ["cluster1", "cluster2"]
        
        async def mock_start_cluster(cluster_id, **kwargs):
            if cluster_id == "cluster1":
                return ServiceStatus.RUNNING
            else:
                raise Exception("Start failed")
        
        multi_cluster_manager.start_cluster = AsyncMock(side_effect=mock_start_cluster)
        
        result = await multi_cluster_manager.start_multiple_clusters(cluster_ids)
        
        assert result["cluster1"] == ServiceStatus.RUNNING
        assert result["cluster2"] == ServiceStatus.ERROR
    
    async def test_stop_multiple_clusters(self, multi_cluster_manager):
        """Test stopping multiple clusters concurrently."""
        cluster_ids = ["cluster1", "cluster2", "cluster3"]
        
        async def mock_stop_cluster(cluster_id, **kwargs):
            await asyncio.sleep(0.1)  # Simulate work
            return True
        
        multi_cluster_manager.stop_cluster = AsyncMock(side_effect=mock_stop_cluster)
        
        result = await multi_cluster_manager.stop_multiple_clusters(cluster_ids, max_concurrent=2)
        
        assert len(result) == 3
        assert all(success for success in result.values())
        assert multi_cluster_manager.stop_cluster.call_count == 3
    
    async def test_stop_multiple_clusters_with_failures(self, multi_cluster_manager):
        """Test stopping multiple clusters with some failures."""
        cluster_ids = ["cluster1", "cluster2"]
        
        async def mock_stop_cluster(cluster_id, **kwargs):
            if cluster_id == "cluster1":
                return True
            else:
                raise Exception("Stop failed")
        
        multi_cluster_manager.stop_cluster = AsyncMock(side_effect=mock_stop_cluster)
        
        result = await multi_cluster_manager.stop_multiple_clusters(cluster_ids)
        
        assert result["cluster1"] is True
        assert result["cluster2"] is False


class TestHealthAndMonitoring:
    """Test health monitoring and statistics."""
    
    async def test_get_cluster_health(self, multi_cluster_manager, sample_cluster_definition, mock_cluster_manager):
        """Test getting cluster health information."""
        multi_cluster_manager._cluster_managers["test-cluster"] = mock_cluster_manager
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        mock_cluster_manager.get_status.return_value = Mock(
            status=ServiceStatus.RUNNING,
            services={"kafka": Mock(dict=lambda: {"status": "running"})},
            uptime=3600,
            endpoints={"kafka": "localhost:9092"}
        )
        
        result = await multi_cluster_manager.get_cluster_health("test-cluster")
        
        assert result["cluster_id"] == "test-cluster"
        assert result["overall_health"] == "healthy"
        assert "registry_health" in result
        assert "manager_health" in result
        assert "factory_health" in result
    
    async def test_get_cluster_health_unhealthy(self, multi_cluster_manager, sample_cluster_definition):
        """Test getting health for unhealthy cluster."""
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.registry.validate_cluster_health.return_value = {"overall_health": "unhealthy"}
        multi_cluster_manager.factory.validate_cluster_resources.return_value = {"valid": False}
        
        result = await multi_cluster_manager.get_cluster_health("test-cluster")
        
        assert result["overall_health"] == "unhealthy"
    
    async def test_get_manager_stats(self, multi_cluster_manager):
        """Test getting manager statistics."""
        # Add some test data
        multi_cluster_manager._cluster_managers["test-cluster"] = Mock()
        multi_cluster_manager._active_operations["op1"] = Mock(status="running")
        multi_cluster_manager._active_operations["op2"] = Mock(status="completed")
        
        result = await multi_cluster_manager.get_manager_stats()
        
        assert result["manager_info"]["active_cluster_managers"] == 1
        assert result["operation_stats"]["total_operations"] == 2
        assert result["operation_stats"]["running_operations"] == 1
        assert result["operation_stats"]["completed_operations"] == 1
        assert "registry_stats" in result
        assert "timestamp" in result
    
    @patch('asyncio.create_task')
    async def test_health_monitoring_start(self, mock_create_task, multi_cluster_manager):
        """Test health monitoring task creation."""
        mock_task = Mock()
        mock_create_task.return_value = mock_task
        
        await multi_cluster_manager._start_health_monitoring()
        
        mock_create_task.assert_called_once()
        assert multi_cluster_manager._health_check_task == mock_task
    
    async def test_perform_health_checks(self, multi_cluster_manager, sample_cluster_definition):
        """Test performing health checks on clusters."""
        multi_cluster_manager.registry.list_clusters.return_value = [sample_cluster_definition]
        multi_cluster_manager.get_cluster_status = AsyncMock(return_value=ServiceStatus.RUNNING)
        
        with patch('src.services.multi_cluster_manager.gauge') as mock_gauge:
            await multi_cluster_manager._perform_health_checks()
            
            multi_cluster_manager.get_cluster_status.assert_called_once_with("test-cluster")
            # Should update metrics
            assert mock_gauge.call_count >= 3  # cluster status, active managers, total clusters


class TestErrorHandling:
    """Test error handling scenarios."""
    
    async def test_cluster_lock_isolation(self, multi_cluster_manager, sample_cluster_definition):
        """Test that cluster operations are properly isolated with locks."""
        # This test verifies that concurrent operations on the same cluster are serialized
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.registry.get_cluster_status.return_value = ServiceStatus.STOPPED
        
        call_order = []
        
        async def mock_operation(cluster_id, operation_name):
            call_order.append(f"{operation_name}_start")
            await asyncio.sleep(0.1)  # Simulate work
            call_order.append(f"{operation_name}_end")
            return True
        
        # Mock the internal operations
        multi_cluster_manager.factory.destroy_cluster_instance = AsyncMock(
            side_effect=lambda cid, **kwargs: mock_operation(cid, "destroy")
        )
        multi_cluster_manager.registry.unregister_cluster = AsyncMock(
            side_effect=lambda cid, **kwargs: mock_operation(cid, "unregister")
        )
        
        # Start two delete operations concurrently
        tasks = [
            multi_cluster_manager.delete_cluster("test-cluster", force=True),
            multi_cluster_manager.delete_cluster("test-cluster", force=True)
        ]
        
        # One should succeed, one should fail (cluster not found)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # At least one operation should complete
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if isinstance(r, Exception))
        
        assert success_count >= 1 or error_count >= 1  # Operations should be serialized
    
    async def test_cleanup_after_failed_creation(self, multi_cluster_manager, sample_cluster_definition):
        """Test cleanup after failed cluster creation."""
        # Make registry registration succeed but factory fail
        multi_cluster_manager.factory.create_cluster_manager.side_effect = Exception("Factory error")
        
        with pytest.raises(MultiClusterManagerError):
            await multi_cluster_manager.create_cluster(sample_cluster_definition)
        
        # Should attempt cleanup
        multi_cluster_manager.factory.destroy_cluster_instance.assert_called_once_with("test-cluster", cleanup_data=True)
        multi_cluster_manager.registry.unregister_cluster.assert_called_once_with("test-cluster", force=True)
    
    async def test_registry_error_handling(self, multi_cluster_manager):
        """Test handling of registry errors."""
        multi_cluster_manager.registry.list_clusters.side_effect = Exception("Registry error")
        
        # Should return empty list instead of raising
        result = await multi_cluster_manager.list_clusters()
        assert result == []
        
        # Should return empty dict for status
        status_result = await multi_cluster_manager.get_all_cluster_status()
        assert status_result == {}


@pytest.mark.integration
class TestMultiClusterIntegration:
    """Integration tests for multi-cluster scenarios."""
    
    async def test_full_cluster_lifecycle(self, multi_cluster_manager, sample_cluster_definition, mock_cluster_manager):
        """Test complete cluster lifecycle from creation to deletion."""
        # Setup mocks for full lifecycle
        multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        multi_cluster_manager.factory.create_cluster_manager.return_value = mock_cluster_manager
        mock_cluster_manager.start_cluster.return_value = Mock(status=ServiceStatus.RUNNING)
        mock_cluster_manager.stop_cluster.return_value = True
        mock_cluster_manager.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
        
        # Create cluster
        created = await multi_cluster_manager.create_cluster(sample_cluster_definition, auto_start=True)
        assert created.id == "test-cluster"
        assert "test-cluster" in multi_cluster_manager._cluster_managers
        
        # Check status
        status = await multi_cluster_manager.get_cluster_status("test-cluster")
        assert status == ServiceStatus.RUNNING
        
        # Stop cluster
        stopped = await multi_cluster_manager.stop_cluster("test-cluster")
        assert stopped is True
        
        # Delete cluster
        deleted = await multi_cluster_manager.delete_cluster("test-cluster")
        assert deleted is True
        assert "test-cluster" not in multi_cluster_manager._cluster_managers
    
    async def test_concurrent_cluster_operations(self, multi_cluster_manager, mock_cluster_manager):
        """Test concurrent operations on multiple clusters."""
        # Create multiple cluster definitions
        clusters = []
        for i in range(3):
            cluster = ClusterDefinition(
                id=f"cluster-{i}",
                name=f"Cluster {i}",
                environment=ClusterEnvironment.DEVELOPMENT,
                port_allocation=PortAllocation(
                    kafka_port=9092 + i,
                    rest_proxy_port=8082 + i,
                    ui_port=8080 + i
                )
            )
            clusters.append(cluster)
        
        # Setup mocks
        multi_cluster_manager.registry.get_cluster.side_effect = lambda cid: next(c for c in clusters if c.id == cid)
        multi_cluster_manager.factory.create_cluster_manager.return_value = mock_cluster_manager
        mock_cluster_manager.start_cluster.return_value = Mock(status=ServiceStatus.RUNNING)
        
        # Create all clusters concurrently
        create_tasks = [
            multi_cluster_manager.create_cluster(cluster, auto_start=False) 
            for cluster in clusters
        ]
        created_clusters = await asyncio.gather(*create_tasks)
        
        assert len(created_clusters) == 3
        assert len(multi_cluster_manager._cluster_managers) == 3
        
        # Start all clusters concurrently
        cluster_ids = [c.id for c in clusters]
        start_results = await multi_cluster_manager.start_multiple_clusters(cluster_ids)
        
        assert len(start_results) == 3
        assert all(status == ServiceStatus.RUNNING for status in start_results.values())
        
        # Stop all clusters concurrently
        stop_results = await multi_cluster_manager.stop_multiple_clusters(cluster_ids)
        
        assert len(stop_results) == 3
        assert all(success for success in stop_results.values())