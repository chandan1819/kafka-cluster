"""
Tests for enhanced health monitoring system.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
from typing import Dict, List, Any

from src.monitoring.enhanced_health_monitor import (
    EnhancedHealthMonitor,
    EnhancedHealthSummary,
    create_enhanced_monitor
)
from src.services.health_monitor import HealthMonitor, ServiceHealth, HealthStatus
from src.monitoring.multi_cluster_monitor import (
    MultiClusterMonitor,
    MonitoringConfig,
    Alert,
    AlertSeverity
)
from src.services.multi_cluster_manager import MultiClusterManager
from src.models.multi_cluster import ClusterDefinition, PortAllocation
from src.exceptions import MonitoringError


class TestEnhancedHealthMonitor:
    """Test enhanced health monitoring functionality."""
    
    @pytest.fixture
    def mock_single_cluster_monitor(self):
        """Create mock single-cluster health monitor."""
        monitor = Mock(spec=HealthMonitor)
        monitor.start_monitoring = AsyncMock()
        monitor.stop_monitoring = AsyncMock()
        monitor.get_health_status = AsyncMock()
        monitor.check_all_services = AsyncMock()
        monitor.is_monitoring = True
        return monitor
    
    @pytest.fixture
    def mock_multi_cluster_manager(self):
        """Create mock multi-cluster manager."""
        manager = Mock(spec=MultiClusterManager)
        manager.list_clusters = AsyncMock()
        manager.get_cluster_status = AsyncMock()
        
        # Mock registry
        registry = Mock()
        registry.get_cluster = AsyncMock()
        manager.registry = registry
        
        return manager
    
    @pytest.fixture
    def sample_clusters(self):
        """Create sample cluster definitions."""
        return [
            ClusterDefinition(
                id="dev-cluster",
                name="Development Cluster",
                description="Development environment",
                environment="development",
                template_id="dev-template",
                port_allocation=PortAllocation(
                    kafka_port=9092,
                    rest_proxy_port=8082,
                    ui_port=8080
                ),
                tags={"env": "dev"}
            ),
            ClusterDefinition(
                id="prod-cluster",
                name="Production Cluster",
                description="Production environment",
                environment="production",
                template_id="prod-template",
                port_allocation=PortAllocation(
                    kafka_port=9093,
                    rest_proxy_port=8083,
                    ui_port=8081
                ),
                tags={"env": "prod"}
            )
        ]
    
    @pytest.fixture
    async def enhanced_monitor(self, mock_single_cluster_monitor, mock_multi_cluster_manager):
        """Create enhanced health monitor instance."""
        config = MonitoringConfig(
            health_check_interval=1,
            metrics_collection_interval=1,
            alert_evaluation_interval=1
        )
        
        monitor = EnhancedHealthMonitor(
            single_cluster_monitor=mock_single_cluster_monitor,
            multi_cluster_manager=mock_multi_cluster_manager,
            monitoring_config=config
        )
        
        yield monitor
        
        # Cleanup
        if monitor._is_monitoring:
            await monitor.stop_monitoring()
    
    @pytest.fixture
    async def enhanced_monitor_no_multi(self, mock_single_cluster_monitor):
        """Create enhanced health monitor without multi-cluster support."""
        monitor = EnhancedHealthMonitor(
            single_cluster_monitor=mock_single_cluster_monitor,
            multi_cluster_manager=None
        )
        
        yield monitor
        
        # Cleanup
        if monitor._is_monitoring:
            await monitor.stop_monitoring()
    
    @pytest.mark.asyncio
    async def test_enhanced_monitor_initialization(self, enhanced_monitor):
        """Test enhanced monitor initialization."""
        assert enhanced_monitor.single_cluster_monitor is not None
        assert enhanced_monitor.multi_cluster_manager is not None
        assert enhanced_monitor.multi_cluster_monitor is not None
        assert not enhanced_monitor._is_monitoring
    
    @pytest.mark.asyncio
    async def test_enhanced_monitor_initialization_no_multi(self, enhanced_monitor_no_multi):
        """Test enhanced monitor initialization without multi-cluster support."""
        assert enhanced_monitor_no_multi.single_cluster_monitor is not None
        assert enhanced_monitor_no_multi.multi_cluster_manager is None
        assert enhanced_monitor_no_multi.multi_cluster_monitor is None
        assert not enhanced_monitor_no_multi._is_monitoring
    
    @pytest.mark.asyncio
    async def test_start_stop_monitoring(self, enhanced_monitor):
        """Test starting and stopping monitoring."""
        # Start monitoring
        await enhanced_monitor.start_monitoring()
        
        assert enhanced_monitor._is_monitoring is True
        enhanced_monitor.single_cluster_monitor.start_monitoring.assert_called_once()
        
        # Stop monitoring
        await enhanced_monitor.stop_monitoring()
        
        assert enhanced_monitor._is_monitoring is False
        enhanced_monitor.single_cluster_monitor.stop_monitoring.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_start_stop_monitoring_no_multi(self, enhanced_monitor_no_multi):
        """Test starting and stopping monitoring without multi-cluster support."""
        # Start monitoring
        await enhanced_monitor_no_multi.start_monitoring()
        
        assert enhanced_monitor_no_multi._is_monitoring is True
        enhanced_monitor_no_multi.single_cluster_monitor.start_monitoring.assert_called_once()
        
        # Stop monitoring
        await enhanced_monitor_no_multi.stop_monitoring()
        
        assert enhanced_monitor_no_multi._is_monitoring is False
        enhanced_monitor_no_multi.single_cluster_monitor.stop_monitoring.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_comprehensive_health(self, enhanced_monitor, sample_clusters):
        """Test getting comprehensive health status."""
        # Setup mocks
        single_health = {
            "kafka": ServiceHealth(
                name="kafka",
                status=HealthStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=50.0
            ),
            "rest_proxy": ServiceHealth(
                name="rest_proxy",
                status=HealthStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=30.0
            )
        }
        
        enhanced_monitor.single_cluster_monitor.get_health_status.return_value = single_health
        enhanced_monitor.multi_cluster_manager.list_clusters.return_value = sample_clusters
        
        # Mock multi-cluster monitor methods
        with patch.object(enhanced_monitor.multi_cluster_monitor, 'get_all_clusters_health') as mock_get_health, \
             patch.object(enhanced_monitor.multi_cluster_monitor, 'get_system_metrics') as mock_get_metrics, \
             patch.object(enhanced_monitor.multi_cluster_monitor, 'get_active_alerts') as mock_get_alerts:
            
            mock_get_health.return_value = {
                "dev-cluster": {
                    "overall_status": "healthy",
                    "components": {"kafka": {"status": "healthy"}}
                },
                "prod-cluster": {
                    "overall_status": "degraded",
                    "components": {"kafka": {"status": "degraded"}}
                }
            }
            
            mock_get_metrics.return_value = {
                "cpu": {"usage_percent": 50.0},
                "memory": {"usage_percent": 60.0}
            }
            
            mock_get_alerts.return_value = [
                {
                    "id": "test_alert",
                    "severity": "warning",
                    "message": "Test alert"
                }
            ]
            
            # Mock cluster status checks
            mock_status = Mock()
            mock_status.kafka_status = "running"
            enhanced_monitor.multi_cluster_manager.get_cluster_status.return_value = mock_status
            
            # Get comprehensive health
            health_summary = await enhanced_monitor.get_comprehensive_health()
            
            assert isinstance(health_summary, EnhancedHealthSummary)
            assert health_summary.overall_status == HealthStatus.DEGRADED  # Due to degraded cluster
            assert len(health_summary.single_cluster_health) == 2
            assert len(health_summary.multi_cluster_health) == 2
            assert len(health_summary.active_alerts) == 1
            assert health_summary.cluster_count == 2
            assert health_summary.running_clusters == 2
    
    @pytest.mark.asyncio
    async def test_get_comprehensive_health_no_multi(self, enhanced_monitor_no_multi):
        """Test getting comprehensive health without multi-cluster support."""
        # Setup mock
        single_health = {
            "kafka": ServiceHealth(
                name="kafka",
                status=HealthStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=50.0
            )
        }
        
        enhanced_monitor_no_multi.single_cluster_monitor.get_health_status.return_value = single_health
        
        # Get comprehensive health
        health_summary = await enhanced_monitor_no_multi.get_comprehensive_health()
        
        assert isinstance(health_summary, EnhancedHealthSummary)
        assert health_summary.overall_status == HealthStatus.HEALTHY
        assert len(health_summary.single_cluster_health) == 1
        assert len(health_summary.multi_cluster_health) == 0
        assert len(health_summary.active_alerts) == 0
        assert health_summary.cluster_count == 0
        assert health_summary.running_clusters == 0
    
    @pytest.mark.asyncio
    async def test_get_cluster_specific_health(self, enhanced_monitor):
        """Test getting health for a specific cluster."""
        cluster_id = "test-cluster"
        
        # Mock multi-cluster monitor methods
        with patch.object(enhanced_monitor.multi_cluster_monitor, 'get_cluster_health') as mock_get_health, \
             patch.object(enhanced_monitor.multi_cluster_monitor, 'get_cluster_metrics') as mock_get_metrics, \
             patch.object(enhanced_monitor.multi_cluster_monitor, 'get_active_alerts') as mock_get_alerts:
            
            mock_get_health.return_value = {
                "cluster_id": cluster_id,
                "overall_status": "healthy",
                "components": {"kafka": {"status": "healthy"}}
            }
            
            mock_get_metrics.return_value = {
                "cluster_status": [{"value": 1.0, "timestamp": datetime.now().isoformat()}]
            }
            
            mock_get_alerts.return_value = []
            
            # Get cluster-specific health
            cluster_health = await enhanced_monitor.get_cluster_specific_health(cluster_id)
            
            assert "health" in cluster_health
            assert "metrics" in cluster_health
            assert "alerts" in cluster_health
            assert "last_updated" in cluster_health
            assert cluster_health["health"]["cluster_id"] == cluster_id
            assert len(cluster_health["alerts"]) == 0
    
    @pytest.mark.asyncio
    async def test_get_cluster_specific_health_no_multi(self, enhanced_monitor_no_multi):
        """Test getting cluster-specific health without multi-cluster support."""
        with pytest.raises(MonitoringError, match="Multi-cluster monitoring not available"):
            await enhanced_monitor_no_multi.get_cluster_specific_health("test-cluster")
    
    @pytest.mark.asyncio
    async def test_get_system_overview(self, enhanced_monitor, sample_clusters):
        """Test getting system overview."""
        # Setup mocks
        single_health = {
            "kafka": ServiceHealth(
                name="kafka",
                status=HealthStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=50.0
            )
        }
        
        enhanced_monitor.single_cluster_monitor.get_health_status.return_value = single_health
        enhanced_monitor.multi_cluster_manager.list_clusters.return_value = sample_clusters
        
        # Mock multi-cluster monitor methods
        with patch.object(enhanced_monitor.multi_cluster_monitor, 'get_system_metrics') as mock_get_metrics, \
             patch.object(enhanced_monitor.multi_cluster_monitor, 'get_active_alerts') as mock_get_alerts:
            
            mock_get_metrics.return_value = {
                "cpu": {"usage_percent": 50.0},
                "memory": {"usage_percent": 60.0}
            }
            
            mock_get_alerts.return_value = [
                {"severity": "warning", "message": "Warning alert"},
                {"severity": "critical", "message": "Critical alert"}
            ]
            
            # Mock cluster status
            mock_status = Mock()
            mock_status.kafka_status = "running"
            enhanced_monitor.multi_cluster_manager.get_cluster_status.return_value = mock_status
            
            # Get system overview
            overview = await enhanced_monitor.get_system_overview()
            
            assert "timestamp" in overview
            assert "monitoring_status" in overview
            assert "single_cluster" in overview
            assert "system_resources" in overview
            assert "alerts" in overview
            assert "clusters" in overview
            
            # Check monitoring status
            assert overview["monitoring_status"]["single_cluster_monitoring"] is True
            assert overview["monitoring_status"]["multi_cluster_monitoring"] is False  # Not started
            
            # Check alerts summary
            assert overview["alerts"]["total_active"] == 2
            assert overview["alerts"]["by_severity"]["warning"] == 1
            assert overview["alerts"]["by_severity"]["critical"] == 1
            
            # Check clusters summary
            assert overview["clusters"]["total"] == 2
            assert overview["clusters"]["by_environment"]["development"] == 1
            assert overview["clusters"]["by_environment"]["production"] == 1
            assert overview["clusters"]["by_status"]["running"] == 2
    
    @pytest.mark.asyncio
    async def test_get_performance_metrics(self, enhanced_monitor):
        """Test getting performance metrics."""
        cluster_id = "test-cluster"
        
        # Mock multi-cluster monitor method
        with patch.object(enhanced_monitor.multi_cluster_monitor, 'get_cluster_metrics') as mock_get_metrics:
            mock_get_metrics.return_value = {
                "cluster_status": [
                    {"value": 1.0, "timestamp": datetime.now().isoformat()},
                    {"value": 1.0, "timestamp": (datetime.now() - timedelta(minutes=5)).isoformat()}
                ]
            }
            
            # Get performance metrics for specific cluster
            metrics = await enhanced_monitor.get_performance_metrics(
                time_range=timedelta(hours=1),
                cluster_id=cluster_id
            )
            
            assert "cluster_id" in metrics
            assert "time_range" in metrics
            assert "metrics" in metrics
            assert metrics["cluster_id"] == cluster_id
            assert "cluster_status" in metrics["metrics"]
            assert len(metrics["metrics"]["cluster_status"]) == 2
    
    @pytest.mark.asyncio
    async def test_get_performance_metrics_system(self, enhanced_monitor):
        """Test getting system-wide performance metrics."""
        # Mock multi-cluster monitor method
        with patch.object(enhanced_monitor.multi_cluster_monitor, 'get_cluster_metrics') as mock_get_metrics:
            mock_get_metrics.return_value = {
                "system_cpu_usage_percent": [
                    {"value": 50.0, "timestamp": datetime.now().isoformat()}
                ]
            }
            
            # Get system-wide performance metrics
            metrics = await enhanced_monitor.get_performance_metrics(
                time_range=timedelta(hours=2)
            )
            
            assert "time_range" in metrics
            assert "system_metrics" in metrics
            assert "system_cpu_usage_percent" in metrics["system_metrics"]
    
    @pytest.mark.asyncio
    async def test_get_performance_metrics_no_multi(self, enhanced_monitor_no_multi):
        """Test getting performance metrics without multi-cluster support."""
        metrics = await enhanced_monitor_no_multi.get_performance_metrics()
        
        assert "error" in metrics
        assert metrics["error"] == "Multi-cluster monitoring not available"
    
    @pytest.mark.asyncio
    async def test_trigger_health_check_all(self, enhanced_monitor, sample_clusters):
        """Test triggering health check for all services."""
        # Setup mocks
        single_health = {
            "kafka": ServiceHealth(
                name="kafka",
                status=HealthStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=50.0
            )
        }
        
        enhanced_monitor.single_cluster_monitor.check_all_services.return_value = single_health
        enhanced_monitor.multi_cluster_manager.list_clusters.return_value = sample_clusters
        
        # Mock multi-cluster monitor methods
        with patch.object(enhanced_monitor.multi_cluster_monitor, '_check_all_clusters_health') as mock_check_all, \
             patch.object(enhanced_monitor.multi_cluster_monitor, 'get_all_clusters_health') as mock_get_all:
            
            mock_get_all.return_value = {
                "dev-cluster": {"overall_status": "healthy"},
                "prod-cluster": {"overall_status": "healthy"}
            }
            
            # Trigger health check
            results = await enhanced_monitor.trigger_health_check()
            
            assert "single_cluster" in results
            assert "all_clusters" in results
            assert "timestamp" in results
            assert len(results["single_cluster"]) == 1
            assert len(results["all_clusters"]) == 2
            
            mock_check_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_trigger_health_check_specific_cluster(self, enhanced_monitor, sample_clusters):
        """Test triggering health check for specific cluster."""
        cluster_id = "dev-cluster"
        cluster = sample_clusters[0]  # dev-cluster
        
        # Setup mocks
        single_health = {
            "kafka": ServiceHealth(
                name="kafka",
                status=HealthStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=50.0
            )
        }
        
        enhanced_monitor.single_cluster_monitor.check_all_services.return_value = single_health
        enhanced_monitor.multi_cluster_manager.registry.get_cluster.return_value = cluster
        
        # Mock multi-cluster monitor methods
        with patch.object(enhanced_monitor.multi_cluster_monitor, '_check_cluster_health') as mock_check_cluster, \
             patch.object(enhanced_monitor.multi_cluster_monitor, 'get_cluster_health') as mock_get_health:
            
            mock_get_health.return_value = {
                "cluster_id": cluster_id,
                "overall_status": "healthy"
            }
            
            # Trigger health check for specific cluster
            results = await enhanced_monitor.trigger_health_check(cluster_id=cluster_id)
            
            assert "single_cluster" in results
            assert "cluster" in results
            assert "timestamp" in results
            assert results["cluster"]["cluster_id"] == cluster_id
            
            mock_check_cluster.assert_called_once_with(cluster)
    
    @pytest.mark.asyncio
    async def test_trigger_health_check_cluster_not_found(self, enhanced_monitor):
        """Test triggering health check for non-existent cluster."""
        cluster_id = "non-existent-cluster"
        
        # Setup mocks
        single_health = {
            "kafka": ServiceHealth(
                name="kafka",
                status=HealthStatus.HEALTHY,
                last_check=datetime.now(),
                response_time_ms=50.0
            )
        }
        
        enhanced_monitor.single_cluster_monitor.check_all_services.return_value = single_health
        enhanced_monitor.multi_cluster_manager.registry.get_cluster.return_value = None
        
        # Trigger health check for non-existent cluster
        results = await enhanced_monitor.trigger_health_check(cluster_id=cluster_id)
        
        assert "single_cluster" in results
        assert "error" in results
        assert results["error"] == f"Cluster {cluster_id} not found"
    
    @pytest.mark.asyncio
    async def test_alert_handler_management(self, enhanced_monitor):
        """Test adding and removing alert handlers."""
        # Create test handler
        handler_calls = []
        
        def test_handler(alert):
            handler_calls.append(alert)
        
        # Add handler
        enhanced_monitor.add_alert_handler(test_handler)
        
        # Verify handler was added (we can't directly test this without triggering alerts)
        # This test mainly ensures the methods don't raise exceptions
        assert True
        
        # Remove handler
        enhanced_monitor.remove_alert_handler(test_handler)
        
        # Verify handler was removed
        assert True
    
    @pytest.mark.asyncio
    async def test_alert_handler_management_no_multi(self, enhanced_monitor_no_multi):
        """Test alert handler management without multi-cluster support."""
        def test_handler(alert):
            pass
        
        # Should not raise exceptions even without multi-cluster monitor
        enhanced_monitor_no_multi.add_alert_handler(test_handler)
        enhanced_monitor_no_multi.remove_alert_handler(test_handler)
    
    def test_determine_overall_status_healthy(self, enhanced_monitor):
        """Test determining overall status when all systems are healthy."""
        single_health = {
            "kafka": ServiceHealth(
                name="kafka",
                status=HealthStatus.HEALTHY,
                last_check=datetime.now()
            )
        }
        
        multi_health = {
            "cluster1": {"overall_status": "healthy"},
            "cluster2": {"overall_status": "healthy"}
        }
        
        alerts = []
        
        status = enhanced_monitor._determine_overall_status(single_health, multi_health, alerts)
        assert status == HealthStatus.HEALTHY
    
    def test_determine_overall_status_degraded(self, enhanced_monitor):
        """Test determining overall status when some systems are degraded."""
        single_health = {
            "kafka": ServiceHealth(
                name="kafka",
                status=HealthStatus.HEALTHY,
                last_check=datetime.now()
            )
        }
        
        multi_health = {
            "cluster1": {"overall_status": "healthy"},
            "cluster2": {"overall_status": "degraded"}
        }
        
        alerts = []
        
        status = enhanced_monitor._determine_overall_status(single_health, multi_health, alerts)
        assert status == HealthStatus.DEGRADED
    
    def test_determine_overall_status_unhealthy(self, enhanced_monitor):
        """Test determining overall status when systems are unhealthy."""
        single_health = {
            "kafka": ServiceHealth(
                name="kafka",
                status=HealthStatus.UNHEALTHY,
                last_check=datetime.now()
            )
        }
        
        multi_health = {
            "cluster1": {"overall_status": "healthy"}
        }
        
        alerts = []
        
        status = enhanced_monitor._determine_overall_status(single_health, multi_health, alerts)
        assert status == HealthStatus.UNHEALTHY
    
    def test_determine_overall_status_critical_alerts(self, enhanced_monitor):
        """Test determining overall status with critical alerts."""
        single_health = {
            "kafka": ServiceHealth(
                name="kafka",
                status=HealthStatus.HEALTHY,
                last_check=datetime.now()
            )
        }
        
        multi_health = {
            "cluster1": {"overall_status": "healthy"}
        }
        
        alerts = [
            {"severity": "critical", "message": "Critical alert"}
        ]
        
        status = enhanced_monitor._determine_overall_status(single_health, multi_health, alerts)
        assert status == HealthStatus.UNHEALTHY
    
    def test_count_alerts_by_severity(self, enhanced_monitor):
        """Test counting alerts by severity."""
        alerts = [
            {"severity": "warning"},
            {"severity": "critical"},
            {"severity": "warning"},
            {"severity": "error"},
            {"severity": "info"}
        ]
        
        counts = enhanced_monitor._count_alerts_by_severity(alerts)
        
        assert counts["info"] == 1
        assert counts["warning"] == 2
        assert counts["error"] == 1
        assert counts["critical"] == 1
    
    def test_count_clusters_by_environment(self, enhanced_monitor, sample_clusters):
        """Test counting clusters by environment."""
        counts = enhanced_monitor._count_clusters_by_environment(sample_clusters)
        
        assert counts["development"] == 1
        assert counts["production"] == 1
    
    @pytest.mark.asyncio
    async def test_count_clusters_by_status(self, enhanced_monitor, sample_clusters):
        """Test counting clusters by status."""
        # Mock cluster status
        def mock_get_status(cluster_id):
            if cluster_id == "dev-cluster":
                status = Mock()
                status.kafka_status = "running"
                return status
            elif cluster_id == "prod-cluster":
                status = Mock()
                status.kafka_status = "stopped"
                return status
            return None
        
        enhanced_monitor.multi_cluster_manager.get_cluster_status.side_effect = mock_get_status
        
        counts = await enhanced_monitor._count_clusters_by_status(sample_clusters)
        
        assert counts["running"] == 1
        assert counts["stopped"] == 1
        assert counts["unknown"] == 0
    
    @pytest.mark.asyncio
    async def test_error_handling(self, enhanced_monitor):
        """Test error handling in various methods."""
        # Setup mock to raise exception
        enhanced_monitor.single_cluster_monitor.get_health_status.side_effect = Exception("Test error")
        
        # Should raise MonitoringError
        with pytest.raises(MonitoringError, match="Failed to get comprehensive health"):
            await enhanced_monitor.get_comprehensive_health()


class TestCreateEnhancedMonitor:
    """Test the convenience function for creating enhanced monitors."""
    
    def test_create_enhanced_monitor_with_multi_cluster(self):
        """Test creating enhanced monitor with multi-cluster support."""
        single_monitor = Mock(spec=HealthMonitor)
        multi_manager = Mock(spec=MultiClusterManager)
        config = MonitoringConfig()
        
        enhanced_monitor = create_enhanced_monitor(
            single_cluster_monitor=single_monitor,
            multi_cluster_manager=multi_manager,
            monitoring_config=config
        )
        
        assert isinstance(enhanced_monitor, EnhancedHealthMonitor)
        assert enhanced_monitor.single_cluster_monitor == single_monitor
        assert enhanced_monitor.multi_cluster_manager == multi_manager
        assert enhanced_monitor.multi_cluster_monitor is not None
    
    def test_create_enhanced_monitor_without_multi_cluster(self):
        """Test creating enhanced monitor without multi-cluster support."""
        single_monitor = Mock(spec=HealthMonitor)
        
        enhanced_monitor = create_enhanced_monitor(
            single_cluster_monitor=single_monitor
        )
        
        assert isinstance(enhanced_monitor, EnhancedHealthMonitor)
        assert enhanced_monitor.single_cluster_monitor == single_monitor
        assert enhanced_monitor.multi_cluster_manager is None
        assert enhanced_monitor.multi_cluster_monitor is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])