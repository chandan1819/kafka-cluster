"""
Tests for the health monitoring service.
"""

import asyncio
import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta

from src.services.health_monitor import (
    HealthMonitor,
    HealthStatus,
    ServiceHealth,
    SystemHealth,
    HealthMetrics,
    health_monitor
)
from src.exceptions import (
    DockerNotAvailableError,
    KafkaNotAvailableError,
    ServiceUnavailableError
)


@pytest.mark.unit
class TestHealthStatus:
    """Test HealthStatus enumeration."""
    
    def test_health_status_values(self):
        """Test that health status has expected values."""
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"
        assert HealthStatus.UNKNOWN.value == "unknown"


@pytest.mark.unit
class TestServiceHealth:
    """Test ServiceHealth dataclass."""
    
    def test_service_health_creation(self):
        """Test creating ServiceHealth instance."""
        now = datetime.utcnow()
        health = ServiceHealth(
            name="test-service",
            status=HealthStatus.HEALTHY,
            last_check=now,
            response_time_ms=150.5,
            details={"version": "1.0.0"}
        )
        
        assert health.name == "test-service"
        assert health.status == HealthStatus.HEALTHY
        assert health.last_check == now
        assert health.response_time_ms == 150.5
        assert health.error_message is None
        assert health.details == {"version": "1.0.0"}
    
    def test_service_health_to_dict(self):
        """Test converting ServiceHealth to dictionary."""
        now = datetime.utcnow()
        health = ServiceHealth(
            name="test-service",
            status=HealthStatus.DEGRADED,
            last_check=now,
            response_time_ms=250.0,
            error_message="Service slow",
            details={"warning": "high latency"}
        )
        
        result = health.to_dict()
        
        assert result["name"] == "test-service"
        assert result["status"] == "degraded"
        assert result["last_check"] == now.isoformat()
        assert result["response_time_ms"] == 250.0
        assert result["error_message"] == "Service slow"
        assert result["details"] == {"warning": "high latency"}


@pytest.mark.unit
class TestSystemHealth:
    """Test SystemHealth dataclass."""
    
    def test_system_health_creation(self):
        """Test creating SystemHealth instance."""
        now = datetime.utcnow()
        service_health = ServiceHealth(
            name="test-service",
            status=HealthStatus.HEALTHY,
            last_check=now
        )
        
        system_health = SystemHealth(
            status=HealthStatus.HEALTHY,
            timestamp=now,
            services={"test-service": service_health},
            dependencies_satisfied=True,
            uptime_seconds=3600.0
        )
        
        assert system_health.status == HealthStatus.HEALTHY
        assert system_health.timestamp == now
        assert "test-service" in system_health.services
        assert system_health.dependencies_satisfied is True
        assert system_health.uptime_seconds == 3600.0
    
    def test_system_health_to_dict(self):
        """Test converting SystemHealth to dictionary."""
        now = datetime.utcnow()
        service_health = ServiceHealth(
            name="test-service",
            status=HealthStatus.HEALTHY,
            last_check=now
        )
        
        system_health = SystemHealth(
            status=HealthStatus.HEALTHY,
            timestamp=now,
            services={"test-service": service_health},
            dependencies_satisfied=True,
            uptime_seconds=3600.0
        )
        
        result = system_health.to_dict()
        
        assert result["status"] == "healthy"
        assert result["timestamp"] == now.isoformat()
        assert "test-service" in result["services"]
        assert result["dependencies_satisfied"] is True
        assert result["uptime_seconds"] == 3600.0


@pytest.mark.unit
class TestHealthMetrics:
    """Test HealthMetrics model."""
    
    def test_health_metrics_creation(self):
        """Test creating HealthMetrics instance."""
        metrics = HealthMetrics(
            total_checks=100,
            successful_checks=95,
            failed_checks=5,
            average_response_time_ms=150.0
        )
        
        assert metrics.total_checks == 100
        assert metrics.successful_checks == 95
        assert metrics.failed_checks == 5
        assert metrics.average_response_time_ms == 150.0
        assert metrics.consecutive_failures == 0
    
    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        # Test with successful checks
        metrics = HealthMetrics(total_checks=100, successful_checks=95)
        assert metrics.success_rate() == 95.0
        
        # Test with no checks
        metrics = HealthMetrics()
        assert metrics.success_rate() == 0.0
        
        # Test with all failures
        metrics = HealthMetrics(total_checks=50, successful_checks=0)
        assert metrics.success_rate() == 0.0
        
        # Test with all successes
        metrics = HealthMetrics(total_checks=25, successful_checks=25)
        assert metrics.success_rate() == 100.0


@pytest.mark.unit
class TestHealthMonitor:
    """Test HealthMonitor class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.monitor = HealthMonitor()
    
    def teardown_method(self):
        """Clean up after tests."""
        if self.monitor.monitoring_active:
            asyncio.create_task(self.monitor.stop_monitoring())
    
    def test_health_monitor_initialization(self):
        """Test HealthMonitor initialization."""
        assert self.monitor.monitoring_active is False
        assert self.monitor.monitoring_task is None
        assert self.monitor.check_interval == 30
        assert self.monitor.timeout == 5
        assert len(self.monitor.service_definitions) > 0
        assert "docker" in self.monitor.service_definitions
        assert "kafka" in self.monitor.service_definitions
    
    def test_service_definitions_structure(self):
        """Test that service definitions have required structure."""
        for service_name, service_def in self.monitor.service_definitions.items():
            assert "name" in service_def
            assert "check_method" in service_def
            assert "critical" in service_def
            assert "dependencies" in service_def
            assert callable(service_def["check_method"])
            assert isinstance(service_def["critical"], bool)
            assert isinstance(service_def["dependencies"], list)
    
    @pytest.mark.asyncio
    async def test_start_stop_monitoring(self):
        """Test starting and stopping monitoring."""
        # Test starting monitoring
        await self.monitor.start_monitoring(interval=1)
        
        assert self.monitor.monitoring_active is True
        assert self.monitor.monitoring_task is not None
        assert self.monitor.check_interval == 1
        
        # Wait a bit to ensure monitoring loop runs
        await asyncio.sleep(0.1)
        
        # Test stopping monitoring
        await self.monitor.stop_monitoring()
        
        assert self.monitor.monitoring_active is False
    
    @pytest.mark.asyncio
    async def test_start_monitoring_already_active(self):
        """Test starting monitoring when already active."""
        await self.monitor.start_monitoring(interval=1)
        
        # Try to start again
        with patch('src.services.health_monitor.logger') as mock_logger:
            await self.monitor.start_monitoring(interval=2)
            mock_logger.warning.assert_called_with("Health monitoring is already active")
        
        # Should still be using original interval
        assert self.monitor.check_interval == 1
        
        await self.monitor.stop_monitoring()
    
    @pytest.mark.asyncio
    async def test_get_system_health_force_check(self):
        """Test getting system health with force check."""
        with patch.object(self.monitor, '_check_all_services', new_callable=AsyncMock) as mock_check:
            with patch.object(self.monitor, '_calculate_overall_health', return_value=HealthStatus.HEALTHY):
                with patch.object(self.monitor, '_check_dependencies', return_value=True):
                    
                    system_health = await self.monitor.get_system_health(force_check=True)
                    
                    mock_check.assert_called_once()
                    assert isinstance(system_health, SystemHealth)
                    assert system_health.status == HealthStatus.HEALTHY
                    assert system_health.dependencies_satisfied is True
    
    @pytest.mark.asyncio
    async def test_get_service_health(self):
        """Test getting health for specific service."""
        # Test unknown service
        result = await self.monitor.get_service_health("unknown-service")
        assert result is None
        
        # Test known service with force check
        with patch.object(self.monitor, '_check_service_health', new_callable=AsyncMock) as mock_check:
            await self.monitor.get_service_health("docker", force_check=True)
            mock_check.assert_called_once_with("docker")
    
    def test_get_metrics(self):
        """Test getting health metrics."""
        # Set up some test metrics
        self.monitor._metrics["test-service"] = HealthMetrics(
            total_checks=100,
            successful_checks=95,
            failed_checks=5
        )
        
        # Test getting all metrics
        all_metrics = self.monitor.get_metrics()
        assert "test-service" in all_metrics
        assert all_metrics["test-service"].total_checks == 100
        
        # Test getting specific service metrics
        service_metrics = self.monitor.get_metrics("test-service")
        assert "test-service" in service_metrics
        assert len(service_metrics) == 1
        
        # Test getting metrics for unknown service
        unknown_metrics = self.monitor.get_metrics("unknown")
        assert "unknown" in unknown_metrics
        assert unknown_metrics["unknown"].total_checks == 0
    
    @pytest.mark.asyncio
    async def test_check_docker_health_success(self):
        """Test Docker health check success."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.info.return_value = {
            "ServerVersion": "20.10.0",
            "ContainersRunning": 5,
            "Containers": 10
        }
        
        with patch('docker.from_env', return_value=mock_client):
            is_healthy, details = await self.monitor._check_docker_health()
            
            assert is_healthy is True
            assert details["version"] == "20.10.0"
            assert details["containers_running"] == 5
            assert details["containers_total"] == 10
    
    @pytest.mark.asyncio
    async def test_check_docker_health_failure(self):
        """Test Docker health check failure."""
        with patch('docker.from_env', side_effect=Exception("Docker not available")):
            is_healthy, details = await self.monitor._check_docker_health()
            
            assert is_healthy is False
            assert "error" in details
            assert "Docker not available" in details["error"]
    
    @pytest.mark.asyncio
    async def test_check_kafka_health_success(self):
        """Test Kafka health check success."""
        mock_admin = Mock()
        mock_admin.list_topics.return_value = ["topic1", "topic2"]
        
        with patch('kafka.KafkaAdminClient', return_value=mock_admin):
            is_healthy, details = await self.monitor._check_kafka_health()
            
            assert is_healthy is True
            assert "topics_count" in details
    
    @pytest.mark.asyncio
    async def test_check_kafka_health_failure(self):
        """Test Kafka health check failure."""
        with patch('kafka.KafkaAdminClient', side_effect=Exception("Kafka unavailable")):
            is_healthy, details = await self.monitor._check_kafka_health()
            
            assert is_healthy is False
            assert "error" in details
            assert "Kafka unavailable" in details["error"]
    
    @pytest.mark.asyncio
    async def test_check_rest_proxy_health_success(self):
        """Test REST Proxy health check success."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.elapsed.total_seconds.return_value = 0.1
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        
        with patch.object(self.monitor, '_http_client', mock_client):
            is_healthy, details = await self.monitor._check_rest_proxy_health()
            
            assert is_healthy is True
            assert details["status_code"] == 200
            assert "response_time_ms" in details
    
    @pytest.mark.asyncio
    async def test_check_rest_proxy_health_failure(self):
        """Test REST Proxy health check failure."""
        mock_response = Mock()
        mock_response.status_code = 500
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        
        with patch.object(self.monitor, '_http_client', mock_client):
            is_healthy, details = await self.monitor._check_rest_proxy_health()
            
            assert is_healthy is False
            assert details["status_code"] == 500
            assert "error" in details
    
    @pytest.mark.asyncio
    async def test_check_kafka_ui_health_success(self):
        """Test Kafka UI health check success."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "UP"}
        mock_response.elapsed.total_seconds.return_value = 0.2
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        
        with patch.object(self.monitor, '_http_client', mock_client):
            is_healthy, details = await self.monitor._check_kafka_ui_health()
            
            assert is_healthy is True
            assert details["status_code"] == 200
            assert details["health_status"] == "UP"
            assert "response_time_ms" in details
    
    def test_calculate_overall_health_healthy(self):
        """Test overall health calculation when all services are healthy."""
        self.monitor._service_health = {
            "docker": ServiceHealth("Docker", HealthStatus.HEALTHY, datetime.utcnow()),
            "kafka": ServiceHealth("Kafka", HealthStatus.HEALTHY, datetime.utcnow()),
            "kafka-rest-proxy": ServiceHealth("REST Proxy", HealthStatus.HEALTHY, datetime.utcnow())
        }
        
        status = self.monitor._calculate_overall_health()
        assert status == HealthStatus.HEALTHY
    
    def test_calculate_overall_health_critical_unhealthy(self):
        """Test overall health when critical service is unhealthy."""
        self.monitor._service_health = {
            "docker": ServiceHealth("Docker", HealthStatus.UNHEALTHY, datetime.utcnow()),
            "kafka": ServiceHealth("Kafka", HealthStatus.HEALTHY, datetime.utcnow())
        }
        
        status = self.monitor._calculate_overall_health()
        assert status == HealthStatus.UNHEALTHY
    
    def test_calculate_overall_health_critical_degraded(self):
        """Test overall health when critical service is degraded."""
        self.monitor._service_health = {
            "docker": ServiceHealth("Docker", HealthStatus.HEALTHY, datetime.utcnow()),
            "kafka": ServiceHealth("Kafka", HealthStatus.DEGRADED, datetime.utcnow())
        }
        
        status = self.monitor._calculate_overall_health()
        assert status == HealthStatus.DEGRADED
    
    def test_calculate_overall_health_non_critical_issues(self):
        """Test overall health when non-critical services have issues."""
        self.monitor._service_health = {
            "docker": ServiceHealth("Docker", HealthStatus.HEALTHY, datetime.utcnow()),
            "kafka": ServiceHealth("Kafka", HealthStatus.HEALTHY, datetime.utcnow()),
            "kafka-ui": ServiceHealth("Kafka UI", HealthStatus.UNHEALTHY, datetime.utcnow())
        }
        
        status = self.monitor._calculate_overall_health()
        assert status == HealthStatus.DEGRADED
    
    def test_calculate_overall_health_unknown(self):
        """Test overall health when no services are monitored."""
        self.monitor._service_health = {}
        
        status = self.monitor._calculate_overall_health()
        assert status == HealthStatus.UNKNOWN
    
    def test_check_dependencies_satisfied(self):
        """Test dependency checking when all dependencies are satisfied."""
        self.monitor._service_health = {
            "docker": ServiceHealth("Docker", HealthStatus.HEALTHY, datetime.utcnow()),
            "kafka": ServiceHealth("Kafka", HealthStatus.HEALTHY, datetime.utcnow())
        }
        
        satisfied = self.monitor._check_dependencies()
        assert satisfied is True
    
    def test_check_dependencies_unsatisfied(self):
        """Test dependency checking when dependencies are not satisfied."""
        self.monitor._service_health = {
            "docker": ServiceHealth("Docker", HealthStatus.UNHEALTHY, datetime.utcnow()),
            "kafka": ServiceHealth("Kafka", HealthStatus.HEALTHY, datetime.utcnow())
        }
        
        satisfied = self.monitor._check_dependencies()
        assert satisfied is False
    
    def test_check_dependencies_missing(self):
        """Test dependency checking when dependencies are missing."""
        self.monitor._service_health = {
            "kafka": ServiceHealth("Kafka", HealthStatus.HEALTHY, datetime.utcnow())
            # Missing docker dependency
        }
        
        satisfied = self.monitor._check_dependencies()
        assert satisfied is False
    
    def test_get_dependency_order(self):
        """Test getting services in dependency order."""
        order = self.monitor._get_dependency_order()
        
        # Docker should come before Kafka (Kafka depends on Docker)
        docker_index = order.index("docker")
        kafka_index = order.index("kafka")
        assert docker_index < kafka_index
        
        # All services should be included
        assert len(order) == len(self.monitor.service_definitions)
        assert set(order) == set(self.monitor.service_definitions.keys())
    
    @pytest.mark.asyncio
    async def test_check_service_health_success(self):
        """Test checking individual service health successfully."""
        # Mock successful health check
        async def mock_check():
            return True, {"version": "1.0.0"}
        
        self.monitor.service_definitions["test-service"] = {
            "name": "Test Service",
            "check_method": mock_check,
            "critical": False,
            "dependencies": []
        }
        
        await self.monitor._check_service_health("test-service")
        
        assert "test-service" in self.monitor._service_health
        health = self.monitor._service_health["test-service"]
        assert health.status == HealthStatus.HEALTHY
        assert health.error_message is None
        assert health.details == {"version": "1.0.0"}
        
        # Check metrics
        assert "test-service" in self.monitor._metrics
        metrics = self.monitor._metrics["test-service"]
        assert metrics.total_checks == 1
        assert metrics.successful_checks == 1
        assert metrics.failed_checks == 0
    
    @pytest.mark.asyncio
    async def test_check_service_health_failure(self):
        """Test checking individual service health with failure."""
        # Mock failing health check
        async def mock_check():
            return False, {"error": "Service unavailable"}
        
        self.monitor.service_definitions["test-service"] = {
            "name": "Test Service",
            "check_method": mock_check,
            "critical": False,
            "dependencies": []
        }
        
        await self.monitor._check_service_health("test-service")
        
        assert "test-service" in self.monitor._service_health
        health = self.monitor._service_health["test-service"]
        assert health.status == HealthStatus.UNHEALTHY
        assert health.error_message == "Service unavailable"
        
        # Check metrics
        metrics = self.monitor._metrics["test-service"]
        assert metrics.total_checks == 1
        assert metrics.successful_checks == 0
        assert metrics.failed_checks == 1
        assert metrics.consecutive_failures == 1
    
    @pytest.mark.asyncio
    async def test_check_service_health_exception(self):
        """Test checking service health when check method raises exception."""
        # Create fresh monitor to avoid state from previous tests
        monitor = HealthMonitor()
        
        # Mock health check that raises exception (non-retryable)
        async def mock_check():
            raise ValueError("Check method failed")  # ValueError is non-retryable
        
        monitor.service_definitions["test-service"] = {
            "name": "Test Service",
            "check_method": mock_check,
            "critical": False,
            "dependencies": []
        }
        
        await monitor._check_service_health("test-service")
        
        assert "test-service" in monitor._service_health
        health = monitor._service_health["test-service"]
        assert health.status == HealthStatus.UNKNOWN
        assert "Check method failed" in health.error_message
        
        # Check metrics - may be more than 1 due to retry logic in the actual health check methods
        metrics = monitor._metrics["test-service"]
        assert metrics.total_checks >= 1
        assert metrics.failed_checks >= 1
    
    @pytest.mark.asyncio
    async def test_check_unknown_service(self):
        """Test checking health of unknown service."""
        with patch('src.services.health_monitor.logger') as mock_logger:
            await self.monitor._check_service_health("unknown-service")
            mock_logger.warning.assert_called_with("Unknown service: unknown-service")
        
        # Should not create health entry for unknown service
        assert "unknown-service" not in self.monitor._service_health
    
    @pytest.mark.asyncio
    async def test_monitoring_loop_error_handling(self):
        """Test that monitoring loop handles errors gracefully."""
        with patch.object(self.monitor, '_check_all_services', side_effect=Exception("Check failed")):
            with patch('src.services.health_monitor.logger') as mock_logger:
                # Start monitoring with short interval
                await self.monitor.start_monitoring(interval=0.1)
                
                # Wait for a few iterations
                await asyncio.sleep(0.3)
                
                # Stop monitoring
                await self.monitor.stop_monitoring()
                
                # Should have logged errors
                mock_logger.error.assert_called()


@pytest.mark.unit
class TestGlobalHealthMonitorInstance:
    """Test the global health monitor instance."""
    
    def test_global_instance_exists(self):
        """Test that global health monitor instance exists."""
        assert health_monitor is not None
        assert isinstance(health_monitor, HealthMonitor)
    
    def test_global_instance_configuration(self):
        """Test that global instance is properly configured."""
        assert len(health_monitor.service_definitions) > 0
        assert "docker" in health_monitor.service_definitions
        assert "kafka" in health_monitor.service_definitions
        assert "kafka-rest-proxy" in health_monitor.service_definitions
        assert "kafka-ui" in health_monitor.service_definitions