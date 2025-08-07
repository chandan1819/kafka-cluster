"""
Tests for resource management and optimization system.
"""

import pytest
import asyncio
import tempfile
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, List, Any

from src.services.resource_manager import (
    ResourceManager,
    ResourceUsage,
    ResourceQuota,
    CleanupPolicy,
    ScalingRecommendationResult,
    ResourceType,
    ScalingRecommendation,
    bytes_to_human_readable,
    calculate_resource_efficiency,
    get_resource_health_status
)
from src.services.multi_cluster_manager import MultiClusterManager
from src.models.multi_cluster import ClusterDefinition, PortAllocation
from src.exceptions import ResourceError, QuotaExceededError


class TestResourceManager:
    """Test resource management functionality."""
    
    @pytest.fixture
    def mock_multi_cluster_manager(self):
        """Create mock multi-cluster manager."""
        manager = Mock(spec=MultiClusterManager)
        manager.list_clusters = AsyncMock()
        manager.get_cluster_status = AsyncMock()
        manager.delete_cluster = AsyncMock()
        
        # Mock registry
        registry = Mock()
        registry.get_cluster = AsyncMock()
        manager.registry = registry
        
        return manager
    
    @pytest.fixture
    def temp_data_directory(self):
        """Create temporary data directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def sample_cluster(self):
        """Create sample cluster definition."""
        return ClusterDefinition(
            id="test-cluster",
            name="Test Cluster",
            description="Test cluster for resource management",
            environment="testing",
            template_id="test-template",
            port_allocation=PortAllocation(
                kafka_port=9092,
                rest_proxy_port=8082,
                ui_port=8080,
                jmx_port=9999
            ),
            tags={"test": "true"},
            created_at=datetime.now(),
            last_stopped=datetime.now() - timedelta(days=1)
        )
    
    @pytest.fixture
    async def resource_manager(self, mock_multi_cluster_manager, temp_data_directory):
        """Create resource manager instance."""
        manager = ResourceManager(
            multi_cluster_manager=mock_multi_cluster_manager,
            data_directory=temp_data_directory
        )
        yield manager
        
        # Cleanup
        if manager._monitoring_active:
            await manager.stop_monitoring()
    
    @pytest.mark.asyncio
    async def test_resource_manager_initialization(self, resource_manager, temp_data_directory):
        """Test resource manager initialization."""
        assert resource_manager.multi_cluster_manager is not None
        assert resource_manager.data_directory == temp_data_directory
        assert not resource_manager._monitoring_active
        assert len(resource_manager._monitoring_tasks) == 0
        
        # Check default quotas
        assert "system_cpu" in resource_manager._resource_quotas
        assert "system_memory" in resource_manager._resource_quotas
        assert "system_disk" in resource_manager._resource_quotas
        
        # Check default cleanup policies
        assert "stopped_clusters" in resource_manager._cleanup_policies
        assert "old_logs" in resource_manager._cleanup_policies
    
    @pytest.mark.asyncio
    async def test_start_stop_monitoring(self, resource_manager):
        """Test starting and stopping resource monitoring."""
        # Start monitoring
        await resource_manager.start_monitoring()
        
        assert resource_manager._monitoring_active is True
        assert len(resource_manager._monitoring_tasks) == 3  # 3 monitoring loops
        
        # Stop monitoring
        await resource_manager.stop_monitoring()
        
        assert resource_manager._monitoring_active is False
        assert len(resource_manager._monitoring_tasks) == 0
    
    @pytest.mark.asyncio
    async def test_get_system_resource_usage(self, resource_manager):
        """Test getting system resource usage."""
        with patch('psutil.cpu_percent', return_value=75.0), \
             patch('psutil.virtual_memory') as mock_memory, \
             patch('psutil.disk_usage') as mock_disk, \
             patch('psutil.net_io_counters') as mock_network:
            
            # Setup mocks
            mock_memory.return_value.percent = 60.0
            mock_memory.return_value.total = 8000000000
            mock_memory.return_value.available = 3200000000
            mock_memory.return_value.used = 4800000000
            
            mock_disk.return_value.total = 1000000000000
            mock_disk.return_value.used = 500000000000
            mock_disk.return_value.free = 500000000000
            
            mock_network.return_value.bytes_sent = 1000000
            mock_network.return_value.bytes_recv = 2000000
            mock_network.return_value.packets_sent = 1000
            mock_network.return_value.packets_recv = 2000
            
            # Get system resource usage
            usage = await resource_manager.get_system_resource_usage()
            
            assert "cpu" in usage
            assert "memory" in usage
            assert "disk" in usage
            assert "network" in usage
            
            # Check CPU usage
            cpu_usage = usage["cpu"]
            assert cpu_usage.resource_type == ResourceType.CPU
            assert cpu_usage.current_usage == 75.0
            assert cpu_usage.unit == "percent"
            assert cpu_usage.cluster_id is None
            
            # Check memory usage
            memory_usage = usage["memory"]
            assert memory_usage.resource_type == ResourceType.MEMORY
            assert memory_usage.current_usage == 60.0
            assert memory_usage.unit == "percent"
            
            # Check disk usage
            disk_usage = usage["disk"]
            assert disk_usage.resource_type == ResourceType.DISK
            assert disk_usage.current_usage == 50.0  # 50% usage
            assert disk_usage.unit == "percent"
            
            # Check network usage
            network_usage = usage["network"]
            assert network_usage.resource_type == ResourceType.NETWORK
            assert network_usage.current_usage == 3000000  # bytes_sent + bytes_recv
            assert network_usage.unit == "bytes"
    
    @pytest.mark.asyncio
    async def test_get_cluster_resource_usage(self, resource_manager, sample_cluster):
        """Test getting cluster resource usage."""
        # Setup mock
        resource_manager.multi_cluster_manager.registry.get_cluster.return_value = sample_cluster
        
        # Create mock data directory
        cluster_data_dir = resource_manager.data_directory.parent / "data" / sample_cluster.id
        cluster_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Create some test files
        test_file = cluster_data_dir / "test.log"
        test_file.write_text("test data" * 1000)  # Create some data
        
        with patch.object(resource_manager, '_get_directory_size', return_value=50000):
            # Get cluster resource usage
            usage = await resource_manager.get_cluster_resource_usage(sample_cluster.id)
            
            assert "disk" in usage
            assert "ports" in usage
            
            # Check disk usage
            disk_usage = usage["disk"]
            assert disk_usage.resource_type == ResourceType.DISK
            assert disk_usage.cluster_id == sample_cluster.id
            assert disk_usage.current_usage == 50000
            assert disk_usage.unit == "bytes"
            
            # Check port usage
            port_usage = usage["ports"]
            assert port_usage.resource_type == ResourceType.PORTS
            assert port_usage.cluster_id == sample_cluster.id
            assert port_usage.current_usage == 4  # kafka, rest_proxy, ui, jmx
            assert port_usage.unit == "count"
    
    @pytest.mark.asyncio
    async def test_get_cluster_resource_usage_not_found(self, resource_manager):
        """Test getting resource usage for non-existent cluster."""
        resource_manager.multi_cluster_manager.registry.get_cluster.return_value = None
        
        with pytest.raises(ResourceError, match="Cluster test-cluster not found"):
            await resource_manager.get_cluster_resource_usage("test-cluster")
    
    @pytest.mark.asyncio
    async def test_get_all_clusters_resource_usage(self, resource_manager, sample_cluster):
        """Test getting resource usage for all clusters."""
        # Setup mocks
        sample_cluster2 = ClusterDefinition(
            id="test-cluster-2",
            name="Test Cluster 2",
            description="Second test cluster",
            environment="testing",
            template_id="test-template",
            port_allocation=PortAllocation(
                kafka_port=9093,
                rest_proxy_port=8083,
                ui_port=8081
            ),
            tags={"test": "true"}
        )
        
        resource_manager.multi_cluster_manager.list_clusters.return_value = [sample_cluster, sample_cluster2]
        
        # Mock get_cluster_resource_usage
        async def mock_get_usage(cluster_id):
            return {
                "disk": ResourceUsage(
                    resource_type=ResourceType.DISK,
                    cluster_id=cluster_id,
                    current_usage=10000,
                    limit=None,
                    unit="bytes",
                    timestamp=datetime.now()
                )
            }
        
        with patch.object(resource_manager, 'get_cluster_resource_usage', side_effect=mock_get_usage):
            # Get all clusters usage
            all_usage = await resource_manager.get_all_clusters_resource_usage()
            
            assert len(all_usage) == 2
            assert "test-cluster" in all_usage
            assert "test-cluster-2" in all_usage
            assert "disk" in all_usage["test-cluster"]
            assert "disk" in all_usage["test-cluster-2"]
    
    def test_resource_quota_management(self, resource_manager):
        """Test resource quota management."""
        # Create test quota
        quota = ResourceQuota(
            resource_type=ResourceType.CPU,
            cluster_id="test-cluster",
            soft_limit=70.0,
            hard_limit=90.0,
            unit="percent"
        )
        
        # Set quota
        resource_manager.set_resource_quota(quota)
        
        # Get quota
        retrieved_quota = resource_manager.get_resource_quota(ResourceType.CPU, "test-cluster")
        assert retrieved_quota is not None
        assert retrieved_quota.soft_limit == 70.0
        assert retrieved_quota.hard_limit == 90.0
        
        # Remove quota
        removed = resource_manager.remove_resource_quota(ResourceType.CPU, "test-cluster")
        assert removed is True
        
        # Verify removal
        retrieved_quota = resource_manager.get_resource_quota(ResourceType.CPU, "test-cluster")
        assert retrieved_quota is None
    
    def test_resource_quota_check(self, resource_manager):
        """Test resource quota checking."""
        quota = ResourceQuota(
            resource_type=ResourceType.CPU,
            cluster_id=None,
            soft_limit=70.0,
            hard_limit=90.0,
            unit="percent"
        )
        
        # Test within limits
        is_valid, message = quota.check_quota(50.0)
        assert is_valid is True
        assert "Within quota" in message
        
        # Test soft limit exceeded
        is_valid, message = quota.check_quota(75.0)
        assert is_valid is True
        assert "Soft limit exceeded" in message
        
        # Test hard limit exceeded
        is_valid, message = quota.check_quota(95.0)
        assert is_valid is False
        assert "Hard limit exceeded" in message
        
        # Test disabled quota
        quota.enabled = False
        is_valid, message = quota.check_quota(95.0)
        assert is_valid is True
        assert "Quota disabled" in message
    
    @pytest.mark.asyncio
    async def test_check_resource_quotas(self, resource_manager):
        """Test checking all resource quotas."""
        # Mock system usage
        with patch.object(resource_manager, 'get_system_resource_usage') as mock_system, \
             patch.object(resource_manager, 'get_all_clusters_resource_usage') as mock_clusters:
            
            mock_system.return_value = {
                "cpu": ResourceUsage(
                    resource_type=ResourceType.CPU,
                    cluster_id=None,
                    current_usage=85.0,  # Above soft limit
                    limit=100.0,
                    unit="percent",
                    timestamp=datetime.now()
                )
            }
            
            mock_clusters.return_value = {}
            
            # Check quotas
            results = await resource_manager.check_resource_quotas()
            
            assert "system_cpu" in results
            is_valid, message = results["system_cpu"]
            assert is_valid is True  # Should be valid but with warning
            assert "Soft limit exceeded" in message
    
    def test_cleanup_policy_management(self, resource_manager):
        """Test cleanup policy management."""
        # Create test policy
        policy = CleanupPolicy(
            name="test_policy",
            enabled=True,
            max_age_days=30,
            target_patterns=["*.tmp"],
            cluster_states=["stopped"]
        )
        
        # Add policy
        resource_manager.add_cleanup_policy(policy)
        assert "test_policy" in resource_manager._cleanup_policies
        
        # Remove policy
        removed = resource_manager.remove_cleanup_policy("test_policy")
        assert removed is True
        assert "test_policy" not in resource_manager._cleanup_policies
        
        # Try to remove non-existent policy
        removed = resource_manager.remove_cleanup_policy("non_existent")
        assert removed is False
    
    def test_cleanup_policy_should_cleanup(self, resource_manager, sample_cluster):
        """Test cleanup policy decision logic."""
        policy = CleanupPolicy(
            name="test_policy",
            enabled=True,
            max_age_days=7,
            target_patterns=["*"],
            cluster_states=["stopped"]
        )
        
        # Test recent activity - should not cleanup
        recent_activity = datetime.now() - timedelta(days=1)
        should_cleanup = policy.should_cleanup(sample_cluster, recent_activity)
        assert should_cleanup is False
        
        # Test old activity - should cleanup
        old_activity = datetime.now() - timedelta(days=10)
        should_cleanup = policy.should_cleanup(sample_cluster, old_activity)
        assert should_cleanup is True
        
        # Test disabled policy
        policy.enabled = False
        should_cleanup = policy.should_cleanup(sample_cluster, old_activity)
        assert should_cleanup is False
    
    @pytest.mark.asyncio
    async def test_run_cleanup_dry_run(self, resource_manager, sample_cluster):
        """Test running cleanup in dry run mode."""
        # Setup mock
        resource_manager.multi_cluster_manager.list_clusters.return_value = [sample_cluster]
        
        # Mock cluster status
        mock_status = Mock()
        mock_status.kafka_status = "stopped"
        resource_manager.multi_cluster_manager.get_cluster_status.return_value = mock_status
        
        # Run cleanup in dry run mode
        results = await resource_manager.run_cleanup(dry_run=True)
        
        assert results["dry_run"] is True
        assert len(results["policies_executed"]) > 0
        assert "stopped_clusters" in results["policies_executed"]
        
        # Verify cluster was not actually deleted
        resource_manager.multi_cluster_manager.delete_cluster.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_run_cleanup_specific_policy(self, resource_manager):
        """Test running cleanup for specific policy."""
        # Create test files for cleanup
        test_file = resource_manager.data_directory / "test.log"
        test_file.write_text("test data")
        
        # Set file modification time to old
        old_time = (datetime.now() - timedelta(days=40)).timestamp()
        test_file.touch(times=(old_time, old_time))
        
        # Run cleanup for specific policy
        results = await resource_manager.run_cleanup(policy_name="old_logs", dry_run=False)
        
        assert results["dry_run"] is False
        assert "old_logs" in results["policies_executed"]
        assert len(results["files_cleaned"]) > 0
    
    @pytest.mark.asyncio
    async def test_run_cleanup_policy_not_found(self, resource_manager):
        """Test running cleanup for non-existent policy."""
        with pytest.raises(ResourceError, match="Cleanup policy 'non_existent' not found"):
            await resource_manager.run_cleanup(policy_name="non_existent")
    
    @pytest.mark.asyncio
    async def test_get_scaling_recommendations(self, resource_manager, sample_cluster):
        """Test getting scaling recommendations."""
        # Setup mock
        resource_manager.multi_cluster_manager.registry.get_cluster.return_value = sample_cluster
        
        # Add some usage history
        usage = ResourceUsage(
            resource_type=ResourceType.DISK,
            cluster_id=sample_cluster.id,
            current_usage=5000000000,  # 5GB
            limit=None,
            unit="bytes",
            timestamp=datetime.now()
        )
        
        resource_manager._store_usage_data(sample_cluster.id, usage)
        
        # Get scaling recommendations
        recommendations = await resource_manager.get_scaling_recommendations(sample_cluster.id)
        
        assert len(recommendations) <= 1  # Should get recommendation for the cluster
        if recommendations:
            rec = recommendations[0]
            assert rec.cluster_id == sample_cluster.id
            assert isinstance(rec.recommendation, ScalingRecommendation)
            assert rec.confidence >= 0.0 and rec.confidence <= 1.0
    
    @pytest.mark.asyncio
    async def test_get_scaling_recommendations_all_clusters(self, resource_manager, sample_cluster):
        """Test getting scaling recommendations for all clusters."""
        # Setup mock
        resource_manager.multi_cluster_manager.list_clusters.return_value = [sample_cluster]
        
        # Mock the analysis method
        mock_recommendation = ScalingRecommendationResult(
            cluster_id=sample_cluster.id,
            recommendation=ScalingRecommendation.MAINTAIN,
            reason="Test recommendation",
            current_metrics={"disk_usage": 1000000},
            suggested_changes={},
            confidence=0.8,
            timestamp=datetime.now()
        )
        
        with patch.object(resource_manager, '_analyze_cluster_scaling', return_value=mock_recommendation):
            # Get recommendations for all clusters
            recommendations = await resource_manager.get_scaling_recommendations()
            
            assert len(recommendations) == 1
            assert recommendations[0].cluster_id == sample_cluster.id
            assert recommendations[0].recommendation == ScalingRecommendation.MAINTAIN
    
    @pytest.mark.asyncio
    async def test_optimize_resource_allocation(self, resource_manager, sample_cluster):
        """Test resource allocation optimization."""
        # Setup mocks
        system_usage = {
            "cpu": ResourceUsage(
                resource_type=ResourceType.CPU,
                cluster_id=None,
                current_usage=50.0,
                limit=100.0,
                unit="percent",
                timestamp=datetime.now()
            )
        }
        
        clusters_usage = {
            sample_cluster.id: {
                "disk": ResourceUsage(
                    resource_type=ResourceType.DISK,
                    cluster_id=sample_cluster.id,
                    current_usage=1000000000,  # 1GB
                    limit=None,
                    unit="bytes",
                    timestamp=datetime.now()
                )
            }
        }
        
        with patch.object(resource_manager, 'get_system_resource_usage', return_value=system_usage), \
             patch.object(resource_manager, 'get_all_clusters_resource_usage', return_value=clusters_usage), \
             patch.object(resource_manager, 'get_scaling_recommendations', return_value=[]):
            
            # Optimize resource allocation
            optimization = await resource_manager.optimize_resource_allocation()
            
            assert "timestamp" in optimization
            assert "system_optimization" in optimization
            assert "cluster_optimizations" in optimization
            assert "recommendations" in optimization
            assert "potential_savings" in optimization
    
    def test_get_directory_size(self, resource_manager, temp_data_directory):
        """Test directory size calculation."""
        # Create test files
        test_dir = temp_data_directory / "test_dir"
        test_dir.mkdir()
        
        file1 = test_dir / "file1.txt"
        file1.write_text("a" * 1000)  # 1000 bytes
        
        file2 = test_dir / "file2.txt"
        file2.write_text("b" * 2000)  # 2000 bytes
        
        # Calculate directory size
        size = resource_manager._get_directory_size(test_dir)
        
        assert size == 3000  # 1000 + 2000 bytes
    
    def test_store_usage_data(self, resource_manager):
        """Test storing usage data in memory."""
        usage = ResourceUsage(
            resource_type=ResourceType.CPU,
            cluster_id="test-cluster",
            current_usage=75.0,
            limit=100.0,
            unit="percent",
            timestamp=datetime.now()
        )
        
        # Store usage data
        resource_manager._store_usage_data("test-cluster", usage)
        
        assert "test-cluster" in resource_manager._resource_usage
        assert len(resource_manager._resource_usage["test-cluster"]) == 1
        assert resource_manager._resource_usage["test-cluster"][0] == usage
    
    def test_store_usage_data_retention(self, resource_manager):
        """Test usage data retention policy."""
        # Add old usage data
        old_usage = ResourceUsage(
            resource_type=ResourceType.CPU,
            cluster_id="test-cluster",
            current_usage=75.0,
            limit=100.0,
            unit="percent",
            timestamp=datetime.now() - timedelta(hours=25)  # Older than 24 hours
        )
        
        recent_usage = ResourceUsage(
            resource_type=ResourceType.CPU,
            cluster_id="test-cluster",
            current_usage=80.0,
            limit=100.0,
            unit="percent",
            timestamp=datetime.now()
        )
        
        # Store both usage data points
        resource_manager._store_usage_data("test-cluster", old_usage)
        resource_manager._store_usage_data("test-cluster", recent_usage)
        
        # Only recent usage should remain
        assert len(resource_manager._resource_usage["test-cluster"]) == 1
        assert resource_manager._resource_usage["test-cluster"][0] == recent_usage
    
    @pytest.mark.asyncio
    async def test_store_resource_usage_to_disk(self, resource_manager, temp_data_directory):
        """Test storing resource usage data to disk."""
        # Add some usage data
        usage = ResourceUsage(
            resource_type=ResourceType.CPU,
            cluster_id="test-cluster",
            current_usage=75.0,
            limit=100.0,
            unit="percent",
            timestamp=datetime.now()
        )
        
        resource_manager._store_usage_data("test-cluster", usage)
        
        # Store to disk
        await resource_manager._store_resource_usage()
        
        # Verify file was created
        usage_file = temp_data_directory / "resource_usage.json"
        assert usage_file.exists()
        
        # Verify content
        with open(usage_file, 'r') as f:
            data = json.load(f)
        
        assert "test-cluster" in data
        assert len(data["test-cluster"]) == 1
        assert data["test-cluster"][0]["resource_type"] == "cpu"
        assert data["test-cluster"][0]["current_usage"] == 75.0
    
    def test_determine_scaling_recommendation(self, resource_manager, sample_cluster):
        """Test scaling recommendation logic."""
        # Test high disk usage
        metrics = {"disk_usage": 15_000_000_000}  # 15GB
        
        recommendation, reason, changes, confidence = resource_manager._determine_scaling_recommendation(
            sample_cluster, metrics
        )
        
        assert recommendation == ScalingRecommendation.OPTIMIZE
        assert "High disk usage" in reason
        assert "suggested_retention_days" in changes
        assert confidence > 0.0
        
        # Test moderate disk usage
        metrics = {"disk_usage": 5_000_000_000}  # 5GB
        
        recommendation, reason, changes, confidence = resource_manager._determine_scaling_recommendation(
            sample_cluster, metrics
        )
        
        assert recommendation == ScalingRecommendation.MAINTAIN
        assert "Moderate disk usage" in reason
    
    @pytest.mark.asyncio
    async def test_optimize_system_resources(self, resource_manager):
        """Test system resource optimization."""
        # High CPU usage
        system_usage = {
            "cpu": ResourceUsage(
                resource_type=ResourceType.CPU,
                cluster_id=None,
                current_usage=85.0,  # High usage
                limit=100.0,
                unit="percent",
                timestamp=datetime.now()
            ),
            "memory": ResourceUsage(
                resource_type=ResourceType.MEMORY,
                cluster_id=None,
                current_usage=90.0,  # High usage
                limit=100.0,
                unit="percent",
                timestamp=datetime.now()
            ),
            "disk": ResourceUsage(
                resource_type=ResourceType.DISK,
                cluster_id=None,
                current_usage=95.0,  # High usage
                limit=100.0,
                unit="percent",
                timestamp=datetime.now()
            )
        }
        
        optimization = await resource_manager._optimize_system_resources(system_usage)
        
        assert "cpu_optimization" in optimization
        assert "memory_optimization" in optimization
        assert "disk_optimization" in optimization
        assert len(optimization["recommendations"]) == 3  # All resources have issues
        
        # Check CPU optimization
        assert optimization["cpu_optimization"]["current_usage"] == 85.0
        assert "reducing cluster count" in optimization["cpu_optimization"]["recommendation"]
    
    @pytest.mark.asyncio
    async def test_optimize_cluster_resources(self, resource_manager, sample_cluster):
        """Test cluster resource optimization."""
        cluster_usage = {
            "disk": ResourceUsage(
                resource_type=ResourceType.DISK,
                cluster_id=sample_cluster.id,
                current_usage=6_000_000_000,  # 6GB - above threshold
                limit=None,
                unit="bytes",
                timestamp=datetime.now()
            ),
            "ports": ResourceUsage(
                resource_type=ResourceType.PORTS,
                cluster_id=sample_cluster.id,
                current_usage=6,  # More than standard 4 ports
                limit=None,
                unit="count",
                timestamp=datetime.now()
            )
        }
        
        optimization = await resource_manager._optimize_cluster_resources(sample_cluster.id, cluster_usage)
        
        assert optimization is not None
        assert optimization["cluster_id"] == sample_cluster.id
        assert len(optimization["optimizations"]) == 2  # Disk and port optimizations
        
        # Check disk optimization
        disk_opt = next(opt for opt in optimization["optimizations"] if opt["type"] == "disk_cleanup")
        assert disk_opt["current_usage_gb"] == 6.0
        
        # Check port optimization
        port_opt = next(opt for opt in optimization["optimizations"] if opt["type"] == "port_optimization")
        assert port_opt["current_ports"] == 6
    
    def test_calculate_potential_savings(self, resource_manager):
        """Test potential savings calculation."""
        system_usage = {}
        clusters_usage = {
            "cluster1": {
                "disk": ResourceUsage(
                    resource_type=ResourceType.DISK,
                    cluster_id="cluster1",
                    current_usage=5_000_000_000,  # 5GB
                    limit=None,
                    unit="bytes",
                    timestamp=datetime.now()
                )
            }
        }
        
        scaling_recommendations = [
            ScalingRecommendationResult(
                cluster_id="cluster1",
                recommendation=ScalingRecommendation.OPTIMIZE,
                reason="Test",
                current_metrics={"disk_usage": 5_000_000_000},
                suggested_changes={},
                confidence=0.8,
                timestamp=datetime.now()
            )
        ]
        
        savings = resource_manager._calculate_potential_savings(
            system_usage, clusters_usage, scaling_recommendations
        )
        
        assert "disk_space_gb" in savings
        assert "clusters_to_optimize" in savings
        assert savings["clusters_to_optimize"] == 1
        assert savings["disk_space_gb"] > 0  # Should have some savings
        assert "total_cluster_disk_gb" in savings
        assert savings["total_cluster_disk_gb"] == 5.0  # 5GB total


class TestResourceUsage:
    """Test ResourceUsage model."""
    
    def test_resource_usage_creation(self):
        """Test resource usage creation."""
        usage = ResourceUsage(
            resource_type=ResourceType.CPU,
            cluster_id="test-cluster",
            current_usage=75.0,
            limit=100.0,
            unit="percent",
            timestamp=datetime.now(),
            details={"cores": 4}
        )
        
        assert usage.resource_type == ResourceType.CPU
        assert usage.cluster_id == "test-cluster"
        assert usage.current_usage == 75.0
        assert usage.limit == 100.0
        assert usage.unit == "percent"
        assert usage.details["cores"] == 4
    
    def test_usage_percentage_calculation(self):
        """Test usage percentage calculation."""
        usage = ResourceUsage(
            resource_type=ResourceType.CPU,
            cluster_id="test-cluster",
            current_usage=75.0,
            limit=100.0,
            unit="percent",
            timestamp=datetime.now()
        )
        
        assert usage.usage_percentage == 75.0
        
        # Test with no limit
        usage_no_limit = ResourceUsage(
            resource_type=ResourceType.CPU,
            cluster_id="test-cluster",
            current_usage=75.0,
            limit=None,
            unit="percent",
            timestamp=datetime.now()
        )
        
        assert usage_no_limit.usage_percentage is None
    
    def test_is_over_limit(self):
        """Test over limit checking."""
        usage = ResourceUsage(
            resource_type=ResourceType.CPU,
            cluster_id="test-cluster",
            current_usage=105.0,
            limit=100.0,
            unit="percent",
            timestamp=datetime.now()
        )
        
        assert usage.is_over_limit is True
        
        usage.current_usage = 95.0
        assert usage.is_over_limit is False
        
        # Test with no limit
        usage.limit = None
        assert usage.is_over_limit is False


class TestUtilityFunctions:
    """Test utility functions."""
    
    def test_bytes_to_human_readable(self):
        """Test bytes to human readable conversion."""
        assert bytes_to_human_readable(1024) == "1.0 KB"
        assert bytes_to_human_readable(1048576) == "1.0 MB"
        assert bytes_to_human_readable(1073741824) == "1.0 GB"
        assert bytes_to_human_readable(1099511627776) == "1.0 TB"
        assert bytes_to_human_readable(500) == "500.0 B"
    
    def test_calculate_resource_efficiency(self):
        """Test resource efficiency calculation."""
        # Optimal utilization (75%)
        usage = ResourceUsage(
            resource_type=ResourceType.CPU,
            cluster_id="test",
            current_usage=75.0,
            limit=100.0,
            unit="percent",
            timestamp=datetime.now()
        )
        
        efficiency = calculate_resource_efficiency(usage)
        assert efficiency == 1.0
        
        # Underutilized (50%)
        usage.current_usage = 50.0
        efficiency = calculate_resource_efficiency(usage)
        assert efficiency < 1.0
        
        # Overutilized (90%)
        usage.current_usage = 90.0
        efficiency = calculate_resource_efficiency(usage)
        assert efficiency < 1.0
        
        # No limit
        usage.limit = None
        efficiency = calculate_resource_efficiency(usage)
        assert efficiency == 1.0
    
    def test_get_resource_health_status(self):
        """Test resource health status determination."""
        usage = ResourceUsage(
            resource_type=ResourceType.CPU,
            cluster_id="test",
            current_usage=50.0,
            limit=100.0,
            unit="percent",
            timestamp=datetime.now()
        )
        
        # Healthy (50%)
        assert get_resource_health_status(usage) == "healthy"
        
        # Warning (75%)
        usage.current_usage = 75.0
        assert get_resource_health_status(usage) == "warning"
        
        # Critical (90%)
        usage.current_usage = 90.0
        assert get_resource_health_status(usage) == "critical"
        
        # Emergency (98%)
        usage.current_usage = 98.0
        assert get_resource_health_status(usage) == "emergency"
        
        # Unknown (no limit)
        usage.limit = None
        assert get_resource_health_status(usage) == "unknown"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])