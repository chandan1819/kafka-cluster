"""
Tests for error recovery and handling systems.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch

from src.exceptions.multi_cluster_exceptions import (
    MultiClusterError, ClusterCreationError, ClusterStartupError,
    ClusterUnavailableError, PortAllocationError, ResourceExhaustionError,
    ErrorSeverity, ErrorCategory, RecoveryAction
)
from src.recovery.error_recovery import (
    ErrorRecoveryManager, RecoveryPlan, RecoveryAttempt, RecoveryStatus
)
from src.monitoring.cluster_health_monitor import (
    ClusterHealthMonitor, HealthStatus, HealthCheckType, HealthCheckResult,
    ClusterHealthState
)
from src.services.graceful_degradation import (
    GracefulDegradationService, DegradationLevel, ServiceMode,
    DegradationRule, ClusterDegradationState
)


class TestMultiClusterExceptions:
    """Test multi-cluster exception classes."""
    
    def test_multi_cluster_error_creation(self):
        """Test basic MultiClusterError creation."""
        error = MultiClusterError(
            message="Test error",
            cluster_id="test-cluster",
            error_code="TEST_ERROR",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.CLUSTER_LIFECYCLE
        )
        
        assert error.message == "Test error"
        assert error.cluster_id == "test-cluster"
        assert error.error_code == "TEST_ERROR"
        assert error.severity == ErrorSeverity.HIGH
        assert error.category == ErrorCategory.CLUSTER_LIFECYCLE
        assert isinstance(error.timestamp, datetime)
    
    def test_error_to_dict(self):
        """Test error serialization to dictionary."""
        error = ClusterCreationError(
            message="Failed to create cluster",
            cluster_id="test-cluster",
            context={"template_id": "development"}
        )
        
        error_dict = error.to_dict()
        
        assert error_dict["error_type"] == "ClusterCreationError"
        assert error_dict["message"] == "Failed to create cluster"
        assert error_dict["cluster_id"] == "test-cluster"
        assert error_dict["error_code"] == "CLUSTER_CREATION_FAILED"
        assert error_dict["severity"] == "high"
        assert error_dict["category"] == "cluster_lifecycle"
        assert "retry" in error_dict["recovery_actions"]
        assert error_dict["context"]["template_id"] == "development"
    
    def test_cluster_startup_error(self):
        """Test ClusterStartupError specific properties."""
        error = ClusterStartupError(
            message="Cluster failed to start",
            cluster_id="test-cluster"
        )
        
        assert error.error_code == "CLUSTER_STARTUP_FAILED"
        assert error.severity == ErrorSeverity.HIGH
        assert RecoveryAction.RETRY in error.recovery_actions
        assert RecoveryAction.RESTART_CLUSTER in error.recovery_actions
    
    def test_port_allocation_error(self):
        """Test PortAllocationError specific properties."""
        error = PortAllocationError(
            message="Port 9092 already in use",
            cluster_id="test-cluster"
        )
        
        assert error.error_code == "PORT_ALLOCATION_FAILED"
        assert error.category == ErrorCategory.NETWORK
        assert RecoveryAction.RETRY in error.recovery_actions
    
    def test_resource_exhaustion_error(self):
        """Test ResourceExhaustionError specific properties."""
        error = ResourceExhaustionError(
            message="Out of memory",
            resource_type="memory",
            cluster_id="test-cluster"
        )
        
        assert error.error_code == "RESOURCE_EXHAUSTION"
        assert error.severity == ErrorSeverity.CRITICAL
        assert error.context["resource_type"] == "memory"
        assert RecoveryAction.SCALE_DOWN in error.recovery_actions


class TestErrorRecoveryManager:
    """Test error recovery manager."""
    
    @pytest.fixture
    def recovery_manager(self):
        """Create error recovery manager for testing."""
        return ErrorRecoveryManager()
    
    def test_recovery_plan_creation(self, recovery_manager):
        """Test recovery plan creation."""
        error = ClusterStartupError(
            message="Failed to start",
            cluster_id="test-cluster"
        )
        
        plan = recovery_manager._create_recovery_plan(error)
        
        assert plan.error == error
        assert RecoveryAction.RETRY in plan.actions
        assert plan.max_attempts > 0
        assert plan.backoff_multiplier > 1.0
    
    @pytest.mark.asyncio
    async def test_retry_recovery_action(self, recovery_manager):
        """Test retry recovery action."""
        error = ClusterStartupError(
            message="Failed to start",
            cluster_id="test-cluster"
        )
        
        success = await recovery_manager._handle_retry(error)
        assert success is True
    
    @pytest.mark.asyncio
    async def test_ignore_recovery_action(self, recovery_manager):
        """Test ignore recovery action."""
        error = MultiClusterError(
            message="Minor error",
            cluster_id="test-cluster"
        )
        
        success = await recovery_manager._handle_ignore(error)
        assert success is True
    
    @pytest.mark.asyncio
    async def test_manual_intervention_action(self, recovery_manager):
        """Test manual intervention recovery action."""
        error = MultiClusterError(
            message="Critical error",
            cluster_id="test-cluster"
        )
        
        success = await recovery_manager._handle_manual_intervention(error)
        assert success is False  # Manual intervention doesn't auto-resolve
    
    @pytest.mark.asyncio
    async def test_recovery_plan_execution(self, recovery_manager):
        """Test recovery plan execution."""
        error = MultiClusterError(
            message="Test error",
            cluster_id="test-cluster",
            recovery_actions=[RecoveryAction.RETRY, RecoveryAction.IGNORE]
        )
        
        plan = recovery_manager._create_recovery_plan(error)
        
        # Mock successful retry
        recovery_manager._execute_recovery_action = AsyncMock(return_value=True)
        
        success = await recovery_manager._execute_recovery_plan(plan)
        
        assert success is True
        assert len(plan.attempts) > 0
        assert plan.attempts[0].status == RecoveryStatus.SUCCESS
    
    @pytest.mark.asyncio
    async def test_recovery_plan_failure(self, recovery_manager):
        """Test recovery plan execution with failures."""
        error = MultiClusterError(
            message="Test error",
            cluster_id="test-cluster",
            recovery_actions=[RecoveryAction.RETRY]
        )
        
        plan = recovery_manager._create_recovery_plan(error)
        plan.max_attempts = 2
        
        # Mock failed recovery
        recovery_manager._execute_recovery_action = AsyncMock(return_value=False)
        
        success = await recovery_manager._execute_recovery_plan(plan)
        
        assert success is False
        assert len(plan.attempts) == 2  # Should try max_attempts times
        assert all(attempt.status == RecoveryStatus.FAILED for attempt in plan.attempts)
    
    @pytest.mark.asyncio
    async def test_handle_error_integration(self, recovery_manager):
        """Test complete error handling flow."""
        error = ClusterStartupError(
            message="Failed to start",
            cluster_id="test-cluster"
        )
        
        # Mock successful recovery
        recovery_manager._execute_recovery_plan = AsyncMock(return_value=True)
        
        success = await recovery_manager.handle_error(error)
        
        assert success is True
        assert len(recovery_manager.recovery_history) == 1
        assert recovery_manager.recovery_history[0].error == error
    
    def test_recovery_statistics(self, recovery_manager):
        """Test recovery statistics generation."""
        # Add some mock recovery history
        error1 = ClusterStartupError("Error 1", "cluster-1")
        error2 = PortAllocationError("Error 2", "cluster-2")
        
        plan1 = RecoveryPlan(error=error1, actions=[RecoveryAction.RETRY])
        plan1.attempts.append(RecoveryAttempt(
            action=RecoveryAction.RETRY,
            timestamp=datetime.utcnow(),
            status=RecoveryStatus.SUCCESS
        ))
        
        plan2 = RecoveryPlan(error=error2, actions=[RecoveryAction.RETRY])
        plan2.attempts.append(RecoveryAttempt(
            action=RecoveryAction.RETRY,
            timestamp=datetime.utcnow(),
            status=RecoveryStatus.FAILED
        ))
        
        recovery_manager.recovery_history = [plan1, plan2]
        
        stats = recovery_manager.get_recovery_statistics()
        
        assert stats["total_recoveries"] == 2
        assert stats["successful_recoveries"] == 1
        assert stats["success_rate"] == 0.5
        assert "cluster_lifecycle" in stats["category_statistics"]
        assert "network" in stats["category_statistics"]


class TestClusterHealthMonitor:
    """Test cluster health monitoring."""
    
    @pytest.fixture
    def health_monitor(self):
        """Create health monitor for testing."""
        return ClusterHealthMonitor(check_interval_seconds=1)
    
    def test_health_check_result_creation(self):
        """Test health check result creation."""
        result = HealthCheckResult(
            check_type=HealthCheckType.KAFKA_BROKER,
            status=HealthStatus.HEALTHY,
            message="Broker is responding",
            response_time_ms=50.0
        )
        
        assert result.check_type == HealthCheckType.KAFKA_BROKER
        assert result.status == HealthStatus.HEALTHY
        assert result.response_time_ms == 50.0
        assert isinstance(result.timestamp, datetime)
    
    def test_cluster_health_state(self):
        """Test cluster health state management."""
        state = ClusterHealthState(
            cluster_id="test-cluster",
            overall_status=HealthStatus.HEALTHY,
            last_check=datetime.utcnow()
        )
        
        assert state.is_healthy() is True
        assert state.needs_restart() is False
        
        # Simulate failures
        state.consecutive_failures = 5
        assert state.needs_restart() is True
        
        # Test restart cooldown
        state.last_restart = datetime.utcnow()
        assert state.needs_restart() is False
    
    def test_overall_status_calculation(self, health_monitor):
        """Test overall status calculation from check results."""
        # All healthy
        results = [
            HealthCheckResult(HealthCheckType.KAFKA_BROKER, HealthStatus.HEALTHY, "OK"),
            HealthCheckResult(HealthCheckType.REST_PROXY, HealthStatus.HEALTHY, "OK")
        ]
        status = health_monitor._calculate_overall_status(results)
        assert status == HealthStatus.HEALTHY
        
        # One warning
        results[1].status = HealthStatus.WARNING
        status = health_monitor._calculate_overall_status(results)
        assert status == HealthStatus.WARNING
        
        # One critical
        results[1].status = HealthStatus.CRITICAL
        status = health_monitor._calculate_overall_status(results)
        assert status == HealthStatus.CRITICAL
        
        # One unavailable
        results[1].status = HealthStatus.UNAVAILABLE
        status = health_monitor._calculate_overall_status(results)
        assert status == HealthStatus.UNAVAILABLE
    
    @pytest.mark.asyncio
    async def test_kafka_broker_health_check(self, health_monitor):
        """Test Kafka broker health check."""
        with patch('socket.socket') as mock_socket:
            mock_sock = Mock()
            mock_sock.connect_ex.return_value = 0  # Success
            mock_socket.return_value.__enter__.return_value = mock_sock
            
            with patch('src.registry.cluster_registry.ClusterRegistry') as mock_registry:
                mock_cluster = Mock()
                mock_cluster.port_allocation.kafka_port = 9092
                mock_registry.return_value.get_cluster.return_value = mock_cluster
                
                result = await health_monitor._check_kafka_broker("test-cluster")
                
                assert result.check_type == HealthCheckType.KAFKA_BROKER
                assert result.status == HealthStatus.HEALTHY
                assert "responding" in result.message.lower()
    
    @pytest.mark.asyncio
    async def test_kafka_broker_health_check_failure(self, health_monitor):
        """Test Kafka broker health check failure."""
        with patch('socket.socket') as mock_socket:
            mock_sock = Mock()
            mock_sock.connect_ex.return_value = 1  # Connection refused
            mock_socket.return_value.__enter__.return_value = mock_sock
            
            with patch('src.registry.cluster_registry.ClusterRegistry') as mock_registry:
                mock_cluster = Mock()
                mock_cluster.port_allocation.kafka_port = 9092
                mock_registry.return_value.get_cluster.return_value = mock_cluster
                
                result = await health_monitor._check_kafka_broker("test-cluster")
                
                assert result.check_type == HealthCheckType.KAFKA_BROKER
                assert result.status == HealthStatus.CRITICAL
                assert "cannot connect" in result.message.lower()
    
    @pytest.mark.asyncio
    async def test_memory_health_check(self, health_monitor):
        """Test memory health check."""
        with patch('psutil.virtual_memory') as mock_memory:
            mock_memory.return_value.percent = 75.0
            mock_memory.return_value.total = 8000000000
            mock_memory.return_value.available = 2000000000
            mock_memory.return_value.used = 6000000000
            
            result = await health_monitor._check_memory("test-cluster")
            
            assert result.check_type == HealthCheckType.MEMORY
            assert result.status == HealthStatus.HEALTHY
            assert "75.0%" in result.message
            assert result.details["usage_percent"] == 75.0
    
    @pytest.mark.asyncio
    async def test_memory_health_check_critical(self, health_monitor):
        """Test memory health check with critical usage."""
        with patch('psutil.virtual_memory') as mock_memory:
            mock_memory.return_value.percent = 96.0
            mock_memory.return_value.total = 8000000000
            mock_memory.return_value.available = 320000000
            mock_memory.return_value.used = 7680000000
            
            result = await health_monitor._check_memory("test-cluster")
            
            assert result.check_type == HealthCheckType.MEMORY
            assert result.status == HealthStatus.CRITICAL
            assert "critical" in result.message.lower()
    
    @pytest.mark.asyncio
    async def test_health_monitoring_lifecycle(self, health_monitor):
        """Test health monitoring start/stop lifecycle."""
        cluster_ids = ["cluster-1", "cluster-2"]
        
        # Mock health check methods to avoid actual checks
        health_monitor._perform_health_check = AsyncMock()
        
        # Start monitoring
        await health_monitor.start_monitoring(cluster_ids)
        
        assert health_monitor.is_running is True
        assert len(health_monitor.monitoring_tasks) == 2
        assert all(cluster_id in health_monitor.cluster_states for cluster_id in cluster_ids)
        
        # Stop monitoring
        await health_monitor.stop_monitoring()
        
        assert health_monitor.is_running is False
        assert len(health_monitor.monitoring_tasks) == 0
    
    def test_health_listeners(self, health_monitor):
        """Test health state change listeners."""
        listener_calls = []
        
        def test_listener(cluster_id, state):
            listener_calls.append((cluster_id, state.overall_status))
        
        health_monitor.add_health_listener(test_listener)
        
        # Simulate health state change
        state = ClusterHealthState(
            cluster_id="test-cluster",
            overall_status=HealthStatus.CRITICAL,
            last_check=datetime.utcnow()
        )
        
        # Manually trigger listener (in real scenario, this happens in _perform_health_check)
        for listener in health_monitor.health_listeners:
            listener("test-cluster", state)
        
        assert len(listener_calls) == 1
        assert listener_calls[0] == ("test-cluster", HealthStatus.CRITICAL)


class TestGracefulDegradationService:
    """Test graceful degradation service."""
    
    @pytest.fixture
    def degradation_service(self):
        """Create degradation service for testing."""
        return GracefulDegradationService()
    
    def test_degradation_rule_creation(self):
        """Test degradation rule creation."""
        rule = DegradationRule(
            cluster_id="test-cluster",
            unavailable_threshold_minutes=5,
            degradation_level=DegradationLevel.MODERATE,
            service_mode=ServiceMode.READ_ONLY,
            fallback_clusters=["fallback-cluster"],
            disabled_features={"advanced_features", "cross_cluster_operations"}
        )
        
        assert rule.cluster_id == "test-cluster"
        assert rule.degradation_level == DegradationLevel.MODERATE
        assert rule.service_mode == ServiceMode.READ_ONLY
        assert "fallback-cluster" in rule.fallback_clusters
        assert "advanced_features" in rule.disabled_features
    
    def test_cluster_degradation_state(self):
        """Test cluster degradation state management."""
        state = ClusterDegradationState(cluster_id="test-cluster")
        
        assert state.is_degraded is False
        assert state.degradation_level == DegradationLevel.NONE
        assert state.service_mode == ServiceMode.FULL
        assert state.get_degradation_duration() is None
        
        # Simulate degradation
        state.is_degraded = True
        state.degraded_since = datetime.utcnow() - timedelta(minutes=10)
        
        duration = state.get_degradation_duration()
        assert duration is not None
        assert duration.total_seconds() >= 600  # At least 10 minutes
    
    @pytest.mark.asyncio
    async def test_handle_cluster_unavailable(self, degradation_service):
        """Test handling cluster unavailable."""
        cluster_id = "test-cluster"
        
        # Register degradation rule
        rule = DegradationRule(
            cluster_id=cluster_id,
            degradation_level=DegradationLevel.MINIMAL,
            service_mode=ServiceMode.READ_ONLY
        )
        degradation_service.register_degradation_rule(rule)
        
        # Mock feature degradation handlers
        degradation_service._apply_feature_degradation = AsyncMock()
        
        await degradation_service.handle_cluster_unavailable(cluster_id, "Test failure")
        
        # Check state
        state = degradation_service.get_cluster_degradation_state(cluster_id)
        assert state is not None
        assert state.is_degraded is True
        assert state.degradation_level == DegradationLevel.MINIMAL
        assert state.service_mode == ServiceMode.READ_ONLY
        assert state.degradation_reason == "Test failure"
    
    @pytest.mark.asyncio
    async def test_handle_cluster_available(self, degradation_service):
        """Test handling cluster becoming available."""
        cluster_id = "test-cluster"
        
        # Set up degraded state
        state = ClusterDegradationState(cluster_id=cluster_id)
        state.is_degraded = True
        state.degradation_level = DegradationLevel.MODERATE
        state.service_mode = ServiceMode.READ_ONLY
        state.degraded_since = datetime.utcnow()
        degradation_service.cluster_states[cluster_id] = state
        
        # Mock feature restoration
        degradation_service._restore_feature = AsyncMock()
        
        await degradation_service.handle_cluster_available(cluster_id)
        
        # Check state restored
        assert state.is_degraded is False
        assert state.degradation_level == DegradationLevel.NONE
        assert state.service_mode == ServiceMode.FULL
        assert state.degraded_since is None
    
    @pytest.mark.asyncio
    async def test_find_available_fallback(self, degradation_service):
        """Test finding available fallback cluster."""
        fallback_clusters = ["fallback-1", "fallback-2", "fallback-3"]
        
        # Mock cluster health checks
        with patch('src.monitoring.cluster_health_monitor.cluster_health_monitor') as mock_monitor:
            # fallback-1 is unhealthy
            mock_health_1 = Mock()
            mock_health_1.is_healthy.return_value = False
            
            # fallback-2 is healthy
            mock_health_2 = Mock()
            mock_health_2.is_healthy.return_value = True
            
            mock_monitor.get_cluster_health.side_effect = [mock_health_1, mock_health_2]
            
            fallback = await degradation_service._find_available_fallback(fallback_clusters)
            
            assert fallback == "fallback-2"
    
    def test_degradation_statistics(self, degradation_service):
        """Test degradation statistics generation."""
        # Set up some degraded clusters
        state1 = ClusterDegradationState(cluster_id="cluster-1")
        state1.is_degraded = True
        state1.degradation_level = DegradationLevel.MINIMAL
        state1.service_mode = ServiceMode.READ_ONLY
        
        state2 = ClusterDegradationState(cluster_id="cluster-2")
        state2.is_degraded = True
        state2.degradation_level = DegradationLevel.MODERATE
        state2.service_mode = ServiceMode.ESSENTIAL_ONLY
        
        state3 = ClusterDegradationState(cluster_id="cluster-3")
        # Not degraded
        
        degradation_service.cluster_states = {
            "cluster-1": state1,
            "cluster-2": state2,
            "cluster-3": state3
        }
        
        stats = degradation_service.get_degradation_statistics()
        
        assert stats["total_clusters"] == 3
        assert stats["degraded_clusters"] == 2
        assert stats["degradation_percentage"] == pytest.approx(66.67, rel=1e-2)
        assert stats["degradation_levels"]["minimal"] == 1
        assert stats["degradation_levels"]["moderate"] == 1
        assert stats["service_modes"]["read_only"] == 1
        assert stats["service_modes"]["essential_only"] == 1
    
    def test_cluster_degradation_queries(self, degradation_service):
        """Test cluster degradation query methods."""
        # Set up degraded cluster
        state = ClusterDegradationState(cluster_id="test-cluster")
        state.is_degraded = True
        degradation_service.cluster_states["test-cluster"] = state
        
        assert degradation_service.is_cluster_degraded("test-cluster") is True
        assert degradation_service.is_cluster_degraded("non-existent") is False
        
        degraded_clusters = degradation_service.get_degraded_clusters()
        assert "test-cluster" in degraded_clusters
        assert len(degraded_clusters) == 1
        
        degradation_state = degradation_service.get_cluster_degradation_state("test-cluster")
        assert degradation_state == state
    
    @pytest.mark.asyncio
    async def test_force_degradation(self, degradation_service):
        """Test forcing cluster degradation."""
        cluster_id = "test-cluster"
        
        # Mock feature degradation
        degradation_service._apply_feature_degradation = AsyncMock()
        
        await degradation_service.force_degradation(
            cluster_id, 
            DegradationLevel.SEVERE, 
            "Manual test"
        )
        
        state = degradation_service.get_cluster_degradation_state(cluster_id)
        assert state is not None
        assert state.is_degraded is True
        assert state.degradation_level == DegradationLevel.SEVERE
        assert state.degradation_reason == "Manual test"
    
    @pytest.mark.asyncio
    async def test_force_restoration(self, degradation_service):
        """Test forcing cluster restoration."""
        cluster_id = "test-cluster"
        
        # Set up degraded state
        state = ClusterDegradationState(cluster_id=cluster_id)
        state.is_degraded = True
        degradation_service.cluster_states[cluster_id] = state
        
        # Mock feature restoration
        degradation_service._restore_feature = AsyncMock()
        
        await degradation_service.force_restoration(cluster_id)
        
        assert state.is_degraded is False
        assert state.degradation_level == DegradationLevel.NONE
    
    def test_degradation_listeners(self, degradation_service):
        """Test degradation state change listeners."""
        listener_calls = []
        
        def test_listener(cluster_id, state):
            listener_calls.append((cluster_id, state.is_degraded))
        
        degradation_service.add_degradation_listener(test_listener)
        
        # Manually trigger listener (in real scenario, this happens in _apply_degradation)
        state = ClusterDegradationState(cluster_id="test-cluster")
        state.is_degraded = True
        
        for listener in degradation_service.degradation_listeners:
            listener("test-cluster", state)
        
        assert len(listener_calls) == 1
        assert listener_calls[0] == ("test-cluster", True)


@pytest.mark.asyncio
async def test_integration_error_recovery_and_degradation():
    """Test integration between error recovery and graceful degradation."""
    recovery_manager = ErrorRecoveryManager()
    degradation_service = GracefulDegradationService()
    
    # Mock the degradation service in recovery manager
    with patch('src.recovery.error_recovery.graceful_degradation_service', degradation_service):
        # Create an error that should trigger degradation
        error = ClusterUnavailableError(
            message="Cluster is completely unavailable",
            cluster_id="test-cluster"
        )
        
        # Mock recovery actions to fail
        recovery_manager._handle_restart_cluster = AsyncMock(return_value=False)
        recovery_manager._handle_fallback_cluster = AsyncMock(return_value=True)
        
        # Handle the error
        success = await recovery_manager.handle_error(error)
        
        # Recovery should succeed via fallback
        assert success is True
        
        # Check that recovery was attempted
        assert len(recovery_manager.recovery_history) == 1
        recovery_plan = recovery_manager.recovery_history[0]
        assert recovery_plan.error == error
        assert len(recovery_plan.attempts) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])