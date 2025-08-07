"""
Tests for multi-cluster monitoring and observability system.
"""

import pytest
import asyncio
import tempfile
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, List, Any

from src.monitoring.multi_cluster_monitor import (
    MultiClusterMonitor,
    MonitoringConfig,
    HealthStatus,
    Metric,
    Alert,
    AlertSeverity,
    MetricType,
    console_alert_handler,
    log_alert_handler
)
from src.models.multi_cluster import ClusterDefinition, PortAllocation
from src.services.multi_cluster_manager import MultiClusterManager
from src.exceptions import MonitoringError


class TestMultiClusterMonitor:
    """Test multi-cluster monitoring functionality."""
    
    @pytest.fixture
    def monitoring_config(self):
        """Create test monitoring configuration."""
        return MonitoringConfig(
            health_check_interval=1,  # Fast intervals for testing
            metrics_collection_interval=1,
            alert_evaluation_interval=1,
            cpu_usage_warning_threshold=70.0,
            cpu_usage_critical_threshold=90.0,
            memory_usage_warning_threshold=80.0,
            memory_usage_critical_threshold=95.0
        )
    
    @pytest.fixture
    def mock_multi_cluster_manager(self):
        """Create mock multi-cluster manager."""
        manager = Mock(spec=MultiClusterManager)
        manager.list_clusters = AsyncMock()
        manager.get_cluster_status = AsyncMock()
        return manager
    
    @pytest.fixture
    def temp_storage_path(self):
        """Create temporary storage path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def sample_cluster(self):
        """Create sample cluster definition."""
        return ClusterDefinition(
            id="test-cluster",
            name="Test Cluster",
            description="Test cluster for monitoring",
            environment="testing",
            template_id="test-template",
            port_allocation=PortAllocation(
                kafka_port=9092,
                rest_proxy_port=8082,
                ui_port=8080
            ),
            tags={"test": "true"}
        )
    
    @pytest.fixture
    async def monitor(self, mock_multi_cluster_manager, monitoring_config, temp_storage_path):
        """Create multi-cluster monitor instance."""
        monitor = MultiClusterMonitor(
            multi_cluster_manager=mock_multi_cluster_manager,
            config=monitoring_config,
            storage_path=temp_storage_path
        )
        yield monitor
        
        # Cleanup
        if monitor._is_monitoring:
            await monitor.stop_monitoring()
    
    @pytest.mark.asyncio
    async def test_monitor_initialization(self, monitor, temp_storage_path):
        """Test monitor initialization."""
        assert monitor.config is not None
        assert monitor.storage_path == temp_storage_path
        assert not monitor._is_monitoring
        assert len(monitor._monitoring_tasks) == 0
        assert len(monitor._health_status) == 0
        assert len(monitor._metrics) == 0
        assert len(monitor._active_alerts) == 0
    
    @pytest.mark.asyncio
    async def test_start_stop_monitoring(self, monitor):
        """Test starting and stopping monitoring."""
        # Start monitoring
        await monitor.start_monitoring()
        
        assert monitor._is_monitoring is True
        assert len(monitor._monitoring_tasks) > 0
        
        # Stop monitoring
        await monitor.stop_monitoring()
        
        assert monitor._is_monitoring is False
        assert len(monitor._monitoring_tasks) == 0
    
    @pytest.mark.asyncio
    async def test_health_status_collection(self, monitor, sample_cluster, mock_multi_cluster_manager):
        """Test health status collection."""
        # Setup mock
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster]
        mock_status = Mock()
        mock_status.kafka_status = "running"
        mock_multi_cluster_manager.get_cluster_status.return_value = mock_status
        
        # Check cluster health
        await monitor._check_cluster_health(sample_cluster)
        
        # Verify health status was recorded
        assert "test-cluster" in monitor._health_status
        health_statuses = monitor._health_status["test-cluster"]
        
        # Should have health status for kafka, rest_proxy, ui, and overall cluster
        components = [h.component for h in health_statuses]
        assert "kafka" in components
        assert "rest_proxy" in components
        assert "ui" in components
        assert "cluster" in components
    
    @pytest.mark.asyncio
    async def test_kafka_health_check_success(self, monitor, sample_cluster, mock_multi_cluster_manager):
        """Test successful Kafka health check."""
        # Setup mock
        mock_status = Mock()
        mock_status.kafka_status = "running"
        mock_multi_cluster_manager.get_cluster_status.return_value = mock_status
        
        # Check Kafka health
        health = await monitor._check_kafka_health(sample_cluster)
        
        assert health.component == "kafka"
        assert health.cluster_id == "test-cluster"
        assert health.status == "healthy"
        assert health.message == "Kafka broker is running"
        assert health.response_time_ms is not None
        assert health.response_time_ms >= 0
    
    @pytest.mark.asyncio
    async def test_kafka_health_check_failure(self, monitor, sample_cluster, mock_multi_cluster_manager):
        """Test failed Kafka health check."""
        # Setup mock to raise exception
        mock_multi_cluster_manager.get_cluster_status.side_effect = Exception("Connection failed")
        
        # Check Kafka health
        health = await monitor._check_kafka_health(sample_cluster)
        
        assert health.component == "kafka"
        assert health.cluster_id == "test-cluster"
        assert health.status == "unhealthy"
        assert "Connection failed" in health.message
        assert health.response_time_ms is not None
    
    @pytest.mark.asyncio
    async def test_kafka_health_check_timeout(self, monitor, sample_cluster, mock_multi_cluster_manager):
        """Test Kafka health check timeout."""
        # Setup mock to timeout
        async def slow_response(*args, **kwargs):
            await asyncio.sleep(2)  # Longer than timeout
            return Mock()
        
        mock_multi_cluster_manager.get_cluster_status.side_effect = slow_response
        
        # Check Kafka health
        health = await monitor._check_kafka_health(sample_cluster)
        
        assert health.component == "kafka"
        assert health.cluster_id == "test-cluster"
        assert health.status == "unhealthy"
        assert "timed out" in health.message.lower()
    
    @pytest.mark.asyncio
    async def test_overall_cluster_health_calculation(self, monitor, sample_cluster):
        """Test overall cluster health calculation."""
        cluster_id = sample_cluster.id
        
        # Add some component health statuses
        monitor._health_status[cluster_id] = [
            HealthStatus(
                component="kafka",
                cluster_id=cluster_id,
                status="healthy",
                message="Kafka is running",
                last_check=datetime.now()
            ),
            HealthStatus(
                component="rest_proxy",
                cluster_id=cluster_id,
                status="healthy",
                message="REST Proxy is running",
                last_check=datetime.now()
            ),
            HealthStatus(
                component="ui",
                cluster_id=cluster_id,
                status="degraded",
                message="UI is slow",
                last_check=datetime.now()
            )
        ]
        
        # Check overall health
        overall_health = await monitor._check_overall_cluster_health(sample_cluster)
        
        assert overall_health.component == "cluster"
        assert overall_health.cluster_id == cluster_id
        assert overall_health.status == "degraded"  # Because UI is degraded
        assert "1/3 components degraded" in overall_health.message
        assert overall_health.details["healthy_components"] == 2
        assert overall_health.details["degraded_components"] == 1
        assert overall_health.details["unhealthy_components"] == 0
    
    @pytest.mark.asyncio
    async def test_get_cluster_health(self, monitor, sample_cluster):
        """Test getting cluster health summary."""
        cluster_id = sample_cluster.id
        
        # Add health status
        monitor._health_status[cluster_id] = [
            HealthStatus(
                component="kafka",
                cluster_id=cluster_id,
                status="healthy",
                message="Kafka is running",
                last_check=datetime.now(),
                response_time_ms=50.0
            ),
            HealthStatus(
                component="rest_proxy",
                cluster_id=cluster_id,
                status="unhealthy",
                message="REST Proxy is down",
                last_check=datetime.now(),
                response_time_ms=None
            )
        ]
        
        # Get cluster health
        health_summary = await monitor.get_cluster_health(cluster_id)
        
        assert health_summary["cluster_id"] == cluster_id
        assert health_summary["overall_status"] == "unhealthy"
        assert "kafka" in health_summary["components"]
        assert "rest_proxy" in health_summary["components"]
        assert health_summary["summary"]["healthy_components"] == 1
        assert health_summary["summary"]["unhealthy_components"] == 1
        
        # Check component details
        kafka_health = health_summary["components"]["kafka"]
        assert kafka_health["status"] == "healthy"
        assert kafka_health["response_time_ms"] == 50.0
        
        rest_proxy_health = health_summary["components"]["rest_proxy"]
        assert rest_proxy_health["status"] == "unhealthy"
        assert rest_proxy_health["response_time_ms"] is None
    
    @pytest.mark.asyncio
    async def test_metrics_collection(self, monitor, sample_cluster, mock_multi_cluster_manager):
        """Test metrics collection."""
        # Setup mock
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster]
        mock_status = Mock()
        mock_status.kafka_status = "running"
        mock_multi_cluster_manager.get_cluster_status.return_value = mock_status
        
        # Add some health status for metrics
        monitor._health_status[sample_cluster.id] = [
            HealthStatus(
                component="kafka",
                cluster_id=sample_cluster.id,
                status="healthy",
                message="Running",
                last_check=datetime.now(),
                response_time_ms=100.0
            )
        ]
        
        # Collect metrics
        with patch('psutil.cpu_percent', return_value=50.0), \
             patch('psutil.virtual_memory') as mock_memory, \
             patch('psutil.disk_usage') as mock_disk, \
             patch('psutil.net_io_counters') as mock_network:
            
            # Setup mocks
            mock_memory.return_value.percent = 60.0
            mock_memory.return_value.available = 1000000000
            mock_disk.return_value.total = 2000000000
            mock_disk.return_value.used = 1000000000
            mock_network.return_value.bytes_sent = 1000000
            mock_network.return_value.bytes_recv = 2000000
            
            await monitor._collect_all_metrics()
        
        # Verify system metrics were collected
        system_metrics = monitor._metrics.get("system", [])
        metric_names = [m.name for m in system_metrics]
        
        assert "system_cpu_usage_percent" in metric_names
        assert "system_memory_usage_percent" in metric_names
        assert "system_disk_usage_percent" in metric_names
        assert "system_network_bytes_sent" in metric_names
        
        # Verify cluster metrics were collected
        cluster_metrics = monitor._metrics.get(sample_cluster.id, [])
        cluster_metric_names = [m.name for m in cluster_metrics]
        
        assert "cluster_healthy_components" in cluster_metric_names
        assert "cluster_kafka_response_time_ms" in cluster_metric_names
        assert "cluster_status" in cluster_metric_names
    
    @pytest.mark.asyncio
    async def test_get_cluster_metrics(self, monitor, sample_cluster):
        """Test getting cluster metrics."""
        cluster_id = sample_cluster.id
        
        # Add some metrics
        now = datetime.now()
        monitor._metrics[cluster_id] = [
            Metric(
                name="cluster_status",
                type=MetricType.GAUGE,
                value=1.0,
                timestamp=now,
                cluster_id=cluster_id,
                description="Cluster status"
            ),
            Metric(
                name="cluster_healthy_components",
                type=MetricType.GAUGE,
                value=3.0,
                timestamp=now,
                cluster_id=cluster_id,
                description="Healthy components"
            )
        ]
        
        # Get metrics
        metrics = await monitor.get_cluster_metrics(cluster_id)
        
        assert "cluster_status" in metrics
        assert "cluster_healthy_components" in metrics
        assert len(metrics["cluster_status"]) == 1
        assert metrics["cluster_status"][0]["value"] == 1.0
        assert len(metrics["cluster_healthy_components"]) == 1
        assert metrics["cluster_healthy_components"][0]["value"] == 3.0
    
    @pytest.mark.asyncio
    async def test_get_cluster_metrics_with_filters(self, monitor, sample_cluster):
        """Test getting cluster metrics with filters."""
        cluster_id = sample_cluster.id
        
        # Add metrics with different timestamps
        base_time = datetime.now()
        monitor._metrics[cluster_id] = [
            Metric(
                name="metric1",
                type=MetricType.GAUGE,
                value=1.0,
                timestamp=base_time - timedelta(hours=2),
                cluster_id=cluster_id
            ),
            Metric(
                name="metric2",
                type=MetricType.GAUGE,
                value=2.0,
                timestamp=base_time - timedelta(minutes=30),
                cluster_id=cluster_id
            ),
            Metric(
                name="metric1",
                type=MetricType.GAUGE,
                value=3.0,
                timestamp=base_time,
                cluster_id=cluster_id
            )
        ]
        
        # Get metrics with time filter
        start_time = base_time - timedelta(hours=1)
        metrics = await monitor.get_cluster_metrics(
            cluster_id,
            start_time=start_time
        )
        
        # Should only get metrics after start_time
        assert "metric1" in metrics
        assert "metric2" in metrics
        assert len(metrics["metric1"]) == 1  # Only the recent one
        assert len(metrics["metric2"]) == 1
        assert metrics["metric1"][0]["value"] == 3.0
        
        # Get metrics with name filter
        metrics = await monitor.get_cluster_metrics(
            cluster_id,
            metric_names=["metric1"]
        )
        
        # Should only get metric1
        assert "metric1" in metrics
        assert "metric2" not in metrics
        assert len(metrics["metric1"]) == 2  # Both metric1 entries
    
    @pytest.mark.asyncio
    async def test_system_metrics_collection(self, monitor):
        """Test system metrics collection."""
        with patch('psutil.cpu_percent', return_value=75.0), \
             patch('psutil.virtual_memory') as mock_memory, \
             patch('psutil.disk_usage') as mock_disk, \
             patch('psutil.net_io_counters') as mock_network:
            
            # Setup mocks
            mock_memory.return_value.percent = 85.0
            mock_memory.return_value.available = 500000000
            mock_disk.return_value.total = 1000000000
            mock_disk.return_value.used = 800000000
            mock_network.return_value.bytes_sent = 5000000
            mock_network.return_value.bytes_recv = 10000000
            
            # Get system metrics
            system_metrics = await monitor.get_system_metrics()
        
        assert system_metrics["cpu"]["usage_percent"] == 75.0
        assert system_metrics["memory"]["usage_percent"] == 85.0
        assert system_metrics["memory"]["available_bytes"] == 500000000
        assert system_metrics["disk"]["usage_percent"] == 80.0  # 800M/1000M * 100
        assert system_metrics["network"]["bytes_sent"] == 5000000
        assert system_metrics["network"]["bytes_recv"] == 10000000
    
    @pytest.mark.asyncio
    async def test_alert_triggering(self, monitor):
        """Test alert triggering."""
        # Add alert handler to capture alerts
        triggered_alerts = []
        
        def test_alert_handler(alert: Alert):
            triggered_alerts.append(alert)
        
        monitor.add_alert_handler(test_alert_handler)
        
        # Trigger an alert
        await monitor._trigger_alert(
            alert_id="test_alert",
            name="Test Alert",
            severity=AlertSeverity.WARNING,
            message="This is a test alert",
            cluster_id="test-cluster",
            component="test",
            metadata={"test": True}
        )
        
        # Verify alert was triggered
        assert len(triggered_alerts) == 1
        alert = triggered_alerts[0]
        assert alert.id == "test_alert"
        assert alert.name == "Test Alert"
        assert alert.severity == AlertSeverity.WARNING
        assert alert.message == "This is a test alert"
        assert alert.cluster_id == "test-cluster"
        assert alert.component == "test"
        assert alert.metadata["test"] is True
        
        # Verify alert is in active alerts
        assert "test_alert" in monitor._active_alerts
    
    @pytest.mark.asyncio
    async def test_alert_resolution(self, monitor):
        """Test alert resolution."""
        # Trigger an alert first
        await monitor._trigger_alert(
            alert_id="test_alert",
            name="Test Alert",
            severity=AlertSeverity.WARNING,
            message="This is a test alert",
            cluster_id="test-cluster",
            component="test",
            metadata={}
        )
        
        assert "test_alert" in monitor._active_alerts
        
        # Resolve the alert
        await monitor._resolve_alert("test_alert")
        
        # Verify alert was resolved
        assert "test_alert" not in monitor._active_alerts
    
    @pytest.mark.asyncio
    async def test_system_cpu_alert_evaluation(self, monitor):
        """Test system CPU usage alert evaluation."""
        # Add high CPU usage metric
        high_cpu_metric = Metric(
            name="system_cpu_usage_percent",
            type=MetricType.GAUGE,
            value=95.0,  # Above critical threshold
            timestamp=datetime.now(),
            description="System CPU usage"
        )
        monitor._add_metric(high_cpu_metric)
        
        # Evaluate alerts
        await monitor._evaluate_system_alerts()
        
        # Should have triggered critical CPU alert
        assert "system_cpu_critical" in monitor._active_alerts
        alert = monitor._active_alerts["system_cpu_critical"]
        assert alert.severity == AlertSeverity.CRITICAL
        assert "95.0%" in alert.message
    
    @pytest.mark.asyncio
    async def test_system_memory_alert_evaluation(self, monitor):
        """Test system memory usage alert evaluation."""
        # Add high memory usage metric
        high_memory_metric = Metric(
            name="system_memory_usage_percent",
            type=MetricType.GAUGE,
            value=85.0,  # Above warning threshold
            timestamp=datetime.now(),
            description="System memory usage"
        )
        monitor._add_metric(high_memory_metric)
        
        # Evaluate alerts
        await monitor._evaluate_system_alerts()
        
        # Should have triggered warning memory alert
        assert "system_memory_warning" in monitor._active_alerts
        alert = monitor._active_alerts["system_memory_warning"]
        assert alert.severity == AlertSeverity.WARNING
        assert "85.0%" in alert.message
    
    @pytest.mark.asyncio
    async def test_cluster_health_alert_evaluation(self, monitor, sample_cluster):
        """Test cluster health alert evaluation."""
        cluster_id = sample_cluster.id
        
        # Add unhealthy component
        unhealthy_health = HealthStatus(
            component="kafka",
            cluster_id=cluster_id,
            status="unhealthy",
            message="Kafka broker is down",
            last_check=datetime.now()
        )
        monitor._update_health_status(cluster_id, unhealthy_health)
        
        # Evaluate alerts
        await monitor._evaluate_cluster_alerts(sample_cluster)
        
        # Should have triggered cluster component alert
        alert_id = f"cluster_{cluster_id}_kafka_unhealthy"
        assert alert_id in monitor._active_alerts
        alert = monitor._active_alerts[alert_id]
        assert alert.severity == AlertSeverity.ERROR
        assert alert.cluster_id == cluster_id
        assert alert.component == "kafka"
    
    @pytest.mark.asyncio
    async def test_slow_response_time_alert(self, monitor, sample_cluster):
        """Test slow response time alert evaluation."""
        cluster_id = sample_cluster.id
        
        # Add slow response time health status
        slow_health = HealthStatus(
            component="kafka",
            cluster_id=cluster_id,
            status="healthy",
            message="Kafka is running but slow",
            last_check=datetime.now(),
            response_time_ms=6000.0  # Above critical threshold
        )
        monitor._update_health_status(cluster_id, slow_health)
        
        # Evaluate alerts
        await monitor._evaluate_cluster_alerts(sample_cluster)
        
        # Should have triggered slow response alert
        alert_id = f"cluster_{cluster_id}_kafka_slow_response"
        assert alert_id in monitor._active_alerts
        alert = monitor._active_alerts[alert_id]
        assert alert.severity == AlertSeverity.WARNING
        assert "6000.0ms" in alert.message
    
    @pytest.mark.asyncio
    async def test_get_active_alerts(self, monitor):
        """Test getting active alerts."""
        # Add some alerts
        await monitor._trigger_alert(
            alert_id="alert1",
            name="Alert 1",
            severity=AlertSeverity.CRITICAL,
            message="Critical alert",
            cluster_id="cluster1",
            component="kafka",
            metadata={}
        )
        
        await monitor._trigger_alert(
            alert_id="alert2",
            name="Alert 2",
            severity=AlertSeverity.WARNING,
            message="Warning alert",
            cluster_id="cluster2",
            component="ui",
            metadata={}
        )
        
        # Get all active alerts
        alerts = await monitor.get_active_alerts()
        assert len(alerts) == 2
        
        # Should be sorted by severity (critical first)
        assert alerts[0]["severity"] == "critical"
        assert alerts[1]["severity"] == "warning"
        
        # Get alerts filtered by cluster
        cluster1_alerts = await monitor.get_active_alerts(cluster_id="cluster1")
        assert len(cluster1_alerts) == 1
        assert cluster1_alerts[0]["cluster_id"] == "cluster1"
        
        # Get alerts filtered by severity
        critical_alerts = await monitor.get_active_alerts(severity=AlertSeverity.CRITICAL)
        assert len(critical_alerts) == 1
        assert critical_alerts[0]["severity"] == "critical"
    
    @pytest.mark.asyncio
    async def test_get_all_clusters_health(self, monitor, sample_cluster, mock_multi_cluster_manager):
        """Test getting health for all clusters."""
        # Setup mock
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
        
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster, sample_cluster2]
        
        # Add health status for both clusters
        monitor._health_status["test-cluster"] = [
            HealthStatus(
                component="kafka",
                cluster_id="test-cluster",
                status="healthy",
                message="Running",
                last_check=datetime.now()
            )
        ]
        
        monitor._health_status["test-cluster-2"] = [
            HealthStatus(
                component="kafka",
                cluster_id="test-cluster-2",
                status="unhealthy",
                message="Down",
                last_check=datetime.now()
            )
        ]
        
        # Get all clusters health
        all_health = await monitor.get_all_clusters_health()
        
        assert len(all_health) == 2
        assert "test-cluster" in all_health
        assert "test-cluster-2" in all_health
        assert all_health["test-cluster"]["overall_status"] == "healthy"
        assert all_health["test-cluster-2"]["overall_status"] == "unhealthy"
    
    @pytest.mark.asyncio
    async def test_performance_metrics_collection(self, monitor, sample_cluster, mock_multi_cluster_manager):
        """Test performance metrics collection."""
        # Setup mock
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster]
        mock_status = Mock()
        mock_status.kafka_status = "running"
        mock_multi_cluster_manager.get_cluster_status.return_value = mock_status
        
        # Add some active alerts
        await monitor._trigger_alert(
            alert_id="test_alert",
            name="Test Alert",
            severity=AlertSeverity.WARNING,
            message="Test",
            cluster_id="test-cluster",
            component="test",
            metadata={}
        )
        
        # Collect performance metrics
        await monitor._collect_performance_metrics()
        
        # Verify performance metrics were collected
        system_metrics = monitor._metrics.get("system", [])
        metric_names = [m.name for m in system_metrics]
        
        assert "total_clusters" in metric_names
        assert "running_clusters" in metric_names
        assert "active_alerts" in metric_names
        
        # Check values
        total_clusters_metric = next(m for m in system_metrics if m.name == "total_clusters")
        assert total_clusters_metric.value == 1.0
        
        active_alerts_metric = next(m for m in system_metrics if m.name == "active_alerts")
        assert active_alerts_metric.value == 1.0  # One active alert
    
    @pytest.mark.asyncio
    async def test_data_cleanup(self, monitor, sample_cluster):
        """Test old data cleanup."""
        cluster_id = sample_cluster.id
        
        # Add old health status
        old_health = HealthStatus(
            component="kafka",
            cluster_id=cluster_id,
            status="healthy",
            message="Old status",
            last_check=datetime.now() - timedelta(days=10)  # Very old
        )
        
        recent_health = HealthStatus(
            component="kafka",
            cluster_id=cluster_id,
            status="healthy",
            message="Recent status",
            last_check=datetime.now()
        )
        
        monitor._health_status[cluster_id] = [old_health, recent_health]
        
        # Add old metrics
        old_metric = Metric(
            name="old_metric",
            type=MetricType.GAUGE,
            value=1.0,
            timestamp=datetime.now() - timedelta(days=40),  # Very old
            cluster_id=cluster_id
        )
        
        recent_metric = Metric(
            name="recent_metric",
            type=MetricType.GAUGE,
            value=2.0,
            timestamp=datetime.now(),
            cluster_id=cluster_id
        )
        
        monitor._metrics[cluster_id] = [old_metric, recent_metric]
        
        # Run cleanup
        await monitor._cleanup_old_data()
        
        # Verify old data was cleaned up
        remaining_health = monitor._health_status[cluster_id]
        assert len(remaining_health) == 1
        assert remaining_health[0].message == "Recent status"
        
        remaining_metrics = monitor._metrics[cluster_id]
        assert len(remaining_metrics) == 1
        assert remaining_metrics[0].name == "recent_metric"
    
    @pytest.mark.asyncio
    async def test_alert_handler_management(self, monitor):
        """Test adding and removing alert handlers."""
        # Create test handlers
        handler1_calls = []
        handler2_calls = []
        
        def handler1(alert: Alert):
            handler1_calls.append(alert)
        
        def handler2(alert: Alert):
            handler2_calls.append(alert)
        
        # Add handlers
        monitor.add_alert_handler(handler1)
        monitor.add_alert_handler(handler2)
        
        # Trigger alert
        await monitor._trigger_alert(
            alert_id="test_alert",
            name="Test Alert",
            severity=AlertSeverity.INFO,
            message="Test",
            cluster_id=None,
            component="test",
            metadata={}
        )
        
        # Both handlers should be called
        assert len(handler1_calls) == 1
        assert len(handler2_calls) == 1
        
        # Remove one handler
        monitor.remove_alert_handler(handler1)
        
        # Trigger another alert
        await monitor._trigger_alert(
            alert_id="test_alert2",
            name="Test Alert 2",
            severity=AlertSeverity.INFO,
            message="Test 2",
            cluster_id=None,
            component="test",
            metadata={}
        )
        
        # Only handler2 should be called
        assert len(handler1_calls) == 1  # Still 1
        assert len(handler2_calls) == 2  # Now 2
    
    @pytest.mark.asyncio
    async def test_monitoring_error_handling(self, monitor, mock_multi_cluster_manager):
        """Test error handling in monitoring."""
        # Setup mock to raise exception
        mock_multi_cluster_manager.list_clusters.side_effect = Exception("Test error")
        
        # This should not raise an exception
        await monitor._check_all_clusters_health()
        await monitor._collect_all_metrics()
        await monitor._evaluate_alerts()
        
        # Monitor should still be functional
        assert monitor._is_monitoring is False  # Not started yet


class TestAlertHandlers:
    """Test alert handlers."""
    
    def test_console_alert_handler(self, capsys):
        """Test console alert handler."""
        alert = Alert(
            id="test_alert",
            name="Test Alert",
            severity=AlertSeverity.WARNING,
            message="This is a test alert",
            cluster_id="test-cluster",
            component="test",
            triggered_at=datetime.now()
        )
        
        console_alert_handler(alert)
        
        captured = capsys.readouterr()
        assert "WARNING" in captured.out
        assert "Test Alert" in captured.out
        assert "This is a test alert" in captured.out
    
    def test_log_alert_handler(self, caplog):
        """Test log alert handler."""
        alert = Alert(
            id="test_alert",
            name="Test Alert",
            severity=AlertSeverity.ERROR,
            message="This is a test alert",
            cluster_id="test-cluster",
            component="test",
            triggered_at=datetime.now()
        )
        
        log_alert_handler(alert)
        
        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelname == "ERROR"
        assert "Test Alert" in record.message
        assert "test-cluster" in record.message


class TestMonitoringConfig:
    """Test monitoring configuration."""
    
    def test_default_config(self):
        """Test default monitoring configuration."""
        config = MonitoringConfig()
        
        assert config.health_check_interval == 30
        assert config.metrics_collection_interval == 15
        assert config.alert_evaluation_interval == 60
        assert config.cpu_usage_warning_threshold == 70.0
        assert config.cpu_usage_critical_threshold == 90.0
        assert config.memory_usage_warning_threshold == 80.0
        assert config.memory_usage_critical_threshold == 95.0
        assert config.enable_health_monitoring is True
        assert config.enable_metrics_collection is True
        assert config.enable_alerting is True
    
    def test_custom_config(self):
        """Test custom monitoring configuration."""
        config = MonitoringConfig(
            health_check_interval=60,
            cpu_usage_warning_threshold=80.0,
            enable_alerting=False
        )
        
        assert config.health_check_interval == 60
        assert config.cpu_usage_warning_threshold == 80.0
        assert config.enable_alerting is False
        # Other values should be defaults
        assert config.metrics_collection_interval == 15


class TestHealthStatus:
    """Test health status model."""
    
    def test_health_status_creation(self):
        """Test health status creation."""
        health = HealthStatus(
            component="kafka",
            cluster_id="test-cluster",
            status="healthy",
            message="Kafka is running",
            last_check=datetime.now(),
            response_time_ms=100.0,
            details={"port": 9092}
        )
        
        assert health.component == "kafka"
        assert health.cluster_id == "test-cluster"
        assert health.status == "healthy"
        assert health.message == "Kafka is running"
        assert health.response_time_ms == 100.0
        assert health.details["port"] == 9092


class TestMetric:
    """Test metric model."""
    
    def test_metric_creation(self):
        """Test metric creation."""
        metric = Metric(
            name="cpu_usage",
            type=MetricType.GAUGE,
            value=75.0,
            timestamp=datetime.now(),
            cluster_id="test-cluster",
            labels={"component": "kafka"},
            description="CPU usage percentage"
        )
        
        assert metric.name == "cpu_usage"
        assert metric.type == MetricType.GAUGE
        assert metric.value == 75.0
        assert metric.cluster_id == "test-cluster"
        assert metric.labels["component"] == "kafka"
        assert metric.description == "CPU usage percentage"


class TestAlert:
    """Test alert model."""
    
    def test_alert_creation(self):
        """Test alert creation."""
        now = datetime.now()
        alert = Alert(
            id="test_alert",
            name="Test Alert",
            severity=AlertSeverity.CRITICAL,
            message="This is a critical alert",
            cluster_id="test-cluster",
            component="kafka",
            triggered_at=now,
            metadata={"threshold": 90.0}
        )
        
        assert alert.id == "test_alert"
        assert alert.name == "Test Alert"
        assert alert.severity == AlertSeverity.CRITICAL
        assert alert.message == "This is a critical alert"
        assert alert.cluster_id == "test-cluster"
        assert alert.component == "kafka"
        assert alert.triggered_at == now
        assert alert.resolved_at is None
        assert alert.metadata["threshold"] == 90.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])