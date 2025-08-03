"""Tests for cluster data models."""

import pytest
from pydantic import ValidationError

from src.models.base import ServiceStatus
from src.models.cluster import (
    ServiceHealth,
    ClusterStatus,
    ClusterStartRequest,
    ClusterStopRequest,
)


@pytest.mark.unit
class TestServiceHealth:
    """Test ServiceHealth model."""

    def test_service_health_creation(self):
        """Test ServiceHealth creation."""
        health = ServiceHealth(status=ServiceStatus.RUNNING)
        assert health.status == ServiceStatus.RUNNING
        assert health.uptime is None
        assert health.last_check is None
        assert health.error_message is None

    def test_service_health_with_all_fields(self):
        """Test ServiceHealth with all fields."""
        health = ServiceHealth(
            status=ServiceStatus.ERROR,
            uptime=3600,
            last_check="2024-01-15T10:30:00Z",
            error_message="Connection failed"
        )
        assert health.status == ServiceStatus.ERROR
        assert health.uptime == 3600
        assert health.last_check == "2024-01-15T10:30:00Z"
        assert health.error_message == "Connection failed"


@pytest.mark.unit
class TestClusterStatus:
    """Test ClusterStatus model."""

    def test_cluster_status_minimal(self):
        """Test ClusterStatus with minimal fields."""
        status = ClusterStatus(status=ServiceStatus.RUNNING)
        assert status.status == ServiceStatus.RUNNING
        assert status.broker_count == 0
        assert status.version is None
        assert status.endpoints == {}
        assert status.uptime is None
        assert status.services == {}

    def test_cluster_status_complete(self):
        """Test ClusterStatus with all fields."""
        services = {
            "kafka": ServiceHealth(status=ServiceStatus.RUNNING, uptime=3600),
            "rest-proxy": ServiceHealth(status=ServiceStatus.RUNNING, uptime=3500)
        }
        endpoints = {
            "kafka": "localhost:9092",
            "rest-proxy": "http://localhost:8082"
        }
        
        status = ClusterStatus(
            status=ServiceStatus.RUNNING,
            broker_count=3,
            version="7.4.0",
            endpoints=endpoints,
            uptime=3600,
            services=services
        )
        
        assert status.broker_count == 3
        assert status.version == "7.4.0"
        assert status.endpoints == endpoints
        assert status.services == services

    def test_cluster_status_broker_count_validation(self):
        """Test broker count validation."""
        # Negative broker count should fail
        with pytest.raises(ValidationError) as exc_info:
            ClusterStatus(status=ServiceStatus.RUNNING, broker_count=-1)
        assert "greater than or equal to 0" in str(exc_info.value) or "Broker count cannot be negative" in str(exc_info.value)

        # Very high broker count should fail
        with pytest.raises(ValidationError) as exc_info:
            ClusterStatus(status=ServiceStatus.RUNNING, broker_count=101)
        assert "unreasonably high" in str(exc_info.value)

    def test_cluster_status_endpoint_validation(self):
        """Test endpoint URL validation."""
        # Valid endpoints
        valid_endpoints = {
            "kafka": "localhost:9092",
            "rest-proxy": "http://localhost:8082",
            "ui": "https://localhost:8080"
        }
        status = ClusterStatus(
            status=ServiceStatus.RUNNING,
            endpoints=valid_endpoints
        )
        assert status.endpoints == valid_endpoints

        # Invalid endpoint format
        with pytest.raises(ValidationError) as exc_info:
            ClusterStatus(
                status=ServiceStatus.RUNNING,
                endpoints={"kafka": "invalid-endpoint"}
            )
        assert "Invalid endpoint format" in str(exc_info.value)

    def test_cluster_status_uptime_validation(self):
        """Test uptime validation."""
        # Negative uptime should fail
        with pytest.raises(ValidationError):
            ClusterStatus(status=ServiceStatus.RUNNING, uptime=-1)

        # Valid uptime
        status = ClusterStatus(status=ServiceStatus.RUNNING, uptime=3600)
        assert status.uptime == 3600


@pytest.mark.unit
class TestClusterStartRequest:
    """Test ClusterStartRequest model."""

    def test_cluster_start_request_defaults(self):
        """Test ClusterStartRequest with defaults."""
        request = ClusterStartRequest()
        assert request.force is False
        assert request.timeout == 60

    def test_cluster_start_request_custom(self):
        """Test ClusterStartRequest with custom values."""
        request = ClusterStartRequest(force=True, timeout=120)
        assert request.force is True
        assert request.timeout == 120

    def test_cluster_start_request_timeout_validation(self):
        """Test timeout validation."""
        # Too low timeout
        with pytest.raises(ValidationError):
            ClusterStartRequest(timeout=5)

        # Too high timeout
        with pytest.raises(ValidationError):
            ClusterStartRequest(timeout=400)

        # Valid timeout
        request = ClusterStartRequest(timeout=90)
        assert request.timeout == 90


@pytest.mark.unit
class TestClusterStopRequest:
    """Test ClusterStopRequest model."""

    def test_cluster_stop_request_defaults(self):
        """Test ClusterStopRequest with defaults."""
        request = ClusterStopRequest()
        assert request.force is False
        assert request.cleanup is False
        assert request.timeout == 30

    def test_cluster_stop_request_custom(self):
        """Test ClusterStopRequest with custom values."""
        request = ClusterStopRequest(force=True, cleanup=True, timeout=60)
        assert request.force is True
        assert request.cleanup is True
        assert request.timeout == 60

    def test_cluster_stop_request_timeout_validation(self):
        """Test timeout validation."""
        # Too low timeout
        with pytest.raises(ValidationError):
            ClusterStopRequest(timeout=2)

        # Too high timeout
        with pytest.raises(ValidationError):
            ClusterStopRequest(timeout=200)

        # Valid timeout
        request = ClusterStopRequest(timeout=45)
        assert request.timeout == 45