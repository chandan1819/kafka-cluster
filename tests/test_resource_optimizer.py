"""
Tests for resource optimization service.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
from typing import Dict, List, Any

from src.services.resource_optimizer import (
    ResourceOptimizer,
    OptimizationStrategy,
    OptimizationGoal,
    OptimizationRule,
    OptimizationResult
)
from src.services.resource_manager import (
    ResourceManager,
    ResourceUsage,
    ResourceType,
    ScalingRecommendation,
    ScalingRecommendationResult
)
from src.services.multi_cluster_manager import MultiClusterManager
from src.monitoring.multi_cluster_monitor import MultiClusterMonitor
from src.models.multi_cluster import ClusterDefinition, PortAllocation
from src.exceptions import OptimizationError


class TestResourceOptimizer:
    """Test resource optimization functionality."""
    
    @pytest.fixture
    def mock_resource_manager(self):
        """Create mock resource manager."""
        manager = Mock(spec=ResourceManager)
        manager.get_system_resource_usage = AsyncMock()
        manager.get_all_clusters_resource_usage = AsyncMock()
        manager.get_scaling_recommendations = AsyncMock()
        manager.run_cleanup = AsyncMock()
        return manager
    
    @pytest.fixture
    def mock_multi_cluster_manager(self):
        """Create mock multi-cluster manager."""
        manager = Mock(spec=MultiClusterManager)
        manager.list_clusters = AsyncMock()
        manager.get_cluster_status = AsyncMock()
        manager.delete_cluster = AsyncMock()
        manager.stop_cluster = AsyncMock()
        return manager
    
    @pytest.fixture
    def mock_multi_cluster_monitor(self):
        """Create mock multi-cluster monitor."""
        monitor = Mock(spec=MultiClusterMonitor)
        monitor.get_active_alerts = AsyncMock()
        return monitor
    
    @pytest.fixture
    def sample_cluster(self):
        """Create sample cluster definition."""
        return ClusterDefinition(
            id="test-cluster",
            name="Test Cluster",
            description="Test cluster for optimization",
            environment="testing",
            template_id="test-template",
            port_allocation=PortAllocation(
                kafka_port=9092,
                rest_proxy_port=8082,
                ui_port=8080
            ),
            tags={"test": "true"},
            created_at=datetime.now(),
            last_stopped=datetime.now() - timedelta(hours=25)  # Stopped 25 hours ago
        )
    
    @pytest.fixture
    async def resource_optimizer(self, mock_resource_manager, mock_multi_cluster_manager, mock_multi_cluster_monitor):
        """Create resource optimizer instance."""
        optimizer = ResourceOptimizer(
            resource_manager=mock_resource_manager,
            multi_cluster_manager=mock_multi_cluster_manager,
            multi_cluster_monitor=mock_multi_cluster_monitor
        )
        yield optimizer
        
        # Cleanup
        if optimizer._optimization_active:
            await optimizer.stop_optimization()
    
    @pytest.mark.asyncio
    async def test_optimizer_initialization(self, resource_optimizer):
        """Test optimizer initialization."""
        assert resource_optimizer.resource_manager is not None
        assert resource_optimizer.multi_cluster_manager is not None
        assert resource_optimizer.multi_cluster_monitor is not None
        assert resource_optimizer.strategy == OptimizationStrategy.BALANCED
        assert resource_optimizer.goal == OptimizationGoal.BALANCE_COST_PERFORMANCE
        assert not resource_optimizer._optimization_active
        
        # Check default rules
        assert len(resource_optimizer.optimization_rules) > 0
        assert "scale_down_idle" in resource_optimizer.optimization_rules
        assert "optimize_disk" in resource_optimizer.optimization_rules
        assert "cleanup_stopped" in resource_optimizer.optimization_rules
    
    @pytest.mark.asyncio
    async def test_start_stop_optimization(self, resource_optimizer):
        """Test starting and stopping optimization."""
        # Start optimization
        await resource_optimizer.start_optimization()
        
        assert resource_optimizer._optimization_active is True
        assert resource_optimizer._optimization_task is not None
        
        # Stop optimization
        await resource_optimizer.stop_optimization()
        
        assert resource_optimizer._optimization_active is False
    
    def test_set_optimization_strategy(self, resource_optimizer):
        """Test setting optimization strategy."""
        # Set conservative strategy
        resource_optimizer.set_optimization_strategy(
            OptimizationStrategy.CONSERVATIVE,
            OptimizationGoal.MINIMIZE_COST
        )
        
        assert resource_optimizer.strategy == OptimizationStrategy.CONSERVATIVE
        assert resource_optimizer.goal == OptimizationGoal.MINIMIZE_COST
        
        # Check that consolidate rule is disabled for conservative strategy
        consolidate_rule = resource_optimizer.optimization_rules.get("consolidate_clusters")
        if consolidate_rule:
            assert consolidate_rule.enabled is False
    
    def test_optimization_rule_management(self, resource_optimizer):
        """Test adding, removing, enabling, and disabling rules."""
        # Create test rule
        test_rule = OptimizationRule(
            name="test_rule",
            enabled=True,
            priority=50,
            conditions={"test_condition": 100},
            actions={"action_type": "test_action"}
        )
        
        # Add rule
        resource_optimizer.add_optimization_rule(test_rule)
        assert "test_rule" in resource_optimizer.optimization_rules
        
        # Disable rule
        disabled = resource_optimizer.disable_rule("test_rule")
        assert disabled is True
        assert resource_optimizer.optimization_rules["test_rule"].enabled is False
        
        # Enable rule
        enabled = resource_optimizer.enable_rule("test_rule")
        assert enabled is True
        assert resource_optimizer.optimization_rules["test_rule"].enabled is True
        
        # Remove rule
        removed = resource_optimizer.remove_optimization_rule("test_rule")
        assert removed is True
        assert "test_rule" not in resource_optimizer.optimization_rules
        
        # Try to remove non-existent rule
        removed = resource_optimizer.remove_optimization_rule("non_existent")
        assert removed is False
    
    def test_optimization_rule_can_execute(self, resource_optimizer):
        """Test optimization rule execution conditions."""
        rule = OptimizationRule(
            name="test_rule",
            enabled=True,
            priority=50,
            conditions={},
            actions={},
            cooldown_minutes=30
        )
        
        # Rule should be executable when enabled and no last execution
        assert rule.can_execute() is True
        
        # Rule should not be executable when disabled
        rule.enabled = False
        assert rule.can_execute() is False
        
        # Rule should not be executable during cooldown
        rule.enabled = True
        rule.last_executed = datetime.now() - timedelta(minutes=15)  # 15 minutes ago
        assert rule.can_execute() is False
        
        # Rule should be executable after cooldown
        rule.last_executed = datetime.now() - timedelta(minutes=35)  # 35 minutes ago
        assert rule.can_execute() is True
    
    @pytest.mark.asyncio
    async def test_collect_optimization_metrics(self, resource_optimizer, mock_resource_manager, mock_multi_cluster_monitor):
        """Test collecting optimization metrics."""
        # Setup mocks
        system_usage = {
            "cpu": ResourceUsage(
                resource_type=ResourceType.CPU,
                cluster_id=None,
                current_usage=75.0,
                limit=100.0,
                unit="percent",
                timestamp=datetime.now()
            ),
            "memory": ResourceUsage(
                resource_type=ResourceType.MEMORY,
                cluster_id=None,
                current_usage=60.0,
                limit=100.0,
                unit="percent",
                timestamp=datetime.now()
            )
        }
        
        clusters_usage = {
            "cluster1": {
                "disk": ResourceUsage(
                    resource_type=ResourceType.DISK,
                    cluster_id="cluster1",
                    current_usage=5_000_000_000,  # 5GB
                    limit=None,
                    unit="bytes",
                    timestamp=datetime.now()
                ),
                "ports": ResourceUsage(
                    resource_type=ResourceType.PORTS,
                    cluster_id="cluster1",
                    current_usage=4,
                    limit=None,
                    unit="count",
                    timestamp=datetime.now()
                )
            }
        }
        
        mock_resource_manager.get_system_resource_usage.return_value = system_usage
        mock_resource_manager.get_all_clusters_resource_usage.return_value = clusters_usage
        mock_multi_cluster_monitor.get_active_alerts.return_value = [
            {"severity": "warning", "message": "Test alert"},
            {"severity": "critical", "message": "Critical alert"}
        ]
        
        # Collect metrics
        metrics = await resource_optimizer._collect_optimization_metrics()
        
        assert "system_cpu_usage" in metrics
        assert "system_memory_usage" in metrics
        assert "total_clusters" in metrics
        assert "total_disk_usage_gb" in metrics
        assert "total_ports_used" in metrics
        assert "active_alerts_count" in metrics
        assert "critical_alerts_count" in metrics
        
        assert metrics["system_cpu_usage"] == 75.0
        assert metrics["system_memory_usage"] == 60.0
        assert metrics["total_clusters"] == 1
        assert metrics["total_disk_usage_gb"] == 5.0
        assert metrics["total_ports_used"] == 4
        assert metrics["active_alerts_count"] == 2
        assert metrics["critical_alerts_count"] == 1
    
    @pytest.mark.asyncio
    async def test_check_rule_conditions(self, resource_optimizer):
        """Test checking rule conditions."""
        # Create rule with conditions
        rule = OptimizationRule(
            name="test_rule",
            enabled=True,
            priority=50,
            conditions={
                "system_cpu_usage": 80.0,
                "disk_usage_gb": 10.0,
                "cluster_count_threshold": 2
            },
            actions={}
        )
        
        # Test with metrics that meet conditions
        metrics_meet = {
            "system_cpu_usage": 85.0,  # Above threshold
            "total_disk_usage_gb": 15.0,  # Above threshold
            "total_clusters": 3  # Above threshold
        }
        
        conditions_met = await resource_optimizer._check_rule_conditions(rule, metrics_meet)
        assert conditions_met is True
        
        # Test with metrics that don't meet conditions
        metrics_dont_meet = {
            "system_cpu_usage": 75.0,  # Below threshold
            "total_disk_usage_gb": 5.0,  # Below threshold
            "total_clusters": 1  # Below threshold
        }
        
        conditions_met = await resource_optimizer._check_rule_conditions(rule, metrics_dont_meet)
        assert conditions_met is False
    
    @pytest.mark.asyncio
    async def test_get_applicable_rules(self, resource_optimizer, mock_resource_manager):
        """Test getting applicable rules."""
        # Setup mock to return metrics that trigger some rules
        system_usage = {
            "cpu": ResourceUsage(
                resource_type=ResourceType.CPU,
                cluster_id=None,
                current_usage=85.0,  # High CPU usage
                limit=100.0,
                unit="percent",
                timestamp=datetime.now()
            )
        }
        
        clusters_usage = {
            "cluster1": {
                "disk": ResourceUsage(
                    resource_type=ResourceType.DISK,
                    cluster_id="cluster1",
                    current_usage=10_000_000_000,  # 10GB - high disk usage
                    limit=None,
                    unit="bytes",
                    timestamp=datetime.now()
                )
            }
        }
        
        mock_resource_manager.get_system_resource_usage.return_value = system_usage
        mock_resource_manager.get_all_clusters_resource_usage.return_value = clusters_usage
        
        # Get applicable rules
        applicable_rules = await resource_optimizer._get_applicable_rules()
        
        # Should have some applicable rules based on high resource usage
        assert len(applicable_rules) > 0
        
        # All returned rules should be enabled
        for rule in applicable_rules:
            assert rule.enabled is True
    
    @pytest.mark.asyncio
    async def test_execute_cleanup_actions(self, resource_optimizer, mock_multi_cluster_manager, mock_resource_manager, sample_cluster):
        """Test executing cleanup actions."""
        # Create cleanup rule
        cleanup_rule = OptimizationRule(
            name="test_cleanup",
            enabled=True,
            priority=50,
            conditions={"stopped_duration_hours": 24},
            actions={
                "action_type": "cleanup",
                "delete_cluster": True,
                "compress_logs": True
            }
        )
        
        # Setup mocks
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster]
        
        mock_status = Mock()
        mock_status.kafka_status = "stopped"
        mock_multi_cluster_manager.get_cluster_status.return_value = mock_status
        
        mock_resource_manager.run_cleanup.return_value = {
            "policies_executed": ["old_logs"],
            "space_freed_bytes": 1_000_000_000  # 1GB
        }
        
        # Execute cleanup actions
        actions = await resource_optimizer._execute_cleanup_actions(cleanup_rule, dry_run=True)
        
        assert len(actions) >= 1
        
        # Should have delete cluster action
        delete_action = next((a for a in actions if a["action"] == "delete_cluster"), None)
        assert delete_action is not None
        assert delete_action["cluster_id"] == sample_cluster.id
        assert delete_action["dry_run"] is True
        
        # Should have cleanup action
        cleanup_action = next((a for a in actions if a["action"] == "run_cleanup"), None)
        assert cleanup_action is not None
        assert cleanup_action["space_freed_gb"] == 1.0
    
    @pytest.mark.asyncio
    async def test_execute_scale_down_actions(self, resource_optimizer, mock_multi_cluster_manager, mock_resource_manager, sample_cluster):
        """Test executing scale down actions."""
        # Create scale down rule
        scale_down_rule = OptimizationRule(
            name="test_scale_down",
            enabled=True,
            priority=50,
            conditions={},
            actions={
                "action_type": "scale_down",
                "stop_cluster": True
            }
        )
        
        # Setup mocks
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster]
        mock_resource_manager.get_cluster_resource_usage.return_value = {}  # No usage data (idle)
        
        # Execute scale down actions
        actions = await resource_optimizer._execute_scale_down_actions(scale_down_rule, dry_run=True)
        
        assert len(actions) >= 1
        
        # Should have stop cluster action
        stop_action = actions[0]
        assert stop_action["action"] == "stop_cluster"
        assert stop_action["cluster_id"] == sample_cluster.id
        assert stop_action["reason"] == "idle_cluster"
        assert stop_action["dry_run"] is True
    
    @pytest.mark.asyncio
    async def test_run_optimization_dry_run(self, resource_optimizer, mock_resource_manager, mock_multi_cluster_manager):
        """Test running optimization in dry run mode."""
        # Setup mocks for high resource usage to trigger rules
        system_usage = {
            "cpu": ResourceUsage(
                resource_type=ResourceType.CPU,
                cluster_id=None,
                current_usage=85.0,
                limit=100.0,
                unit="percent",
                timestamp=datetime.now()
            )
        }
        
        clusters_usage = {
            "cluster1": {
                "disk": ResourceUsage(
                    resource_type=ResourceType.DISK,
                    cluster_id="cluster1",
                    current_usage=10_000_000_000,  # 10GB
                    limit=None,
                    unit="bytes",
                    timestamp=datetime.now()
                )
            }
        }
        
        mock_resource_manager.get_system_resource_usage.return_value = system_usage
        mock_resource_manager.get_all_clusters_resource_usage.return_value = clusters_usage
        mock_multi_cluster_manager.list_clusters.return_value = []
        
        # Run optimization
        result = await resource_optimizer.run_optimization(dry_run=True)
        
        assert isinstance(result, OptimizationResult)
        assert result.success is True
        assert result.strategy == OptimizationStrategy.BALANCED
        assert result.goal == OptimizationGoal.BALANCE_COST_PERFORMANCE
        assert len(result.metrics_before) > 0
        assert result.metrics_after is None  # Should be None for dry run
    
    @pytest.mark.asyncio
    async def test_get_optimization_recommendations(self, resource_optimizer, mock_resource_manager):
        """Test getting optimization recommendations."""
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
        
        clusters_usage = {}
        
        scaling_recommendations = [
            ScalingRecommendationResult(
                cluster_id="test-cluster",
                recommendation=ScalingRecommendation.OPTIMIZE,
                reason="High disk usage",
                current_metrics={"disk_usage": 5_000_000_000},
                suggested_changes={"reduce_retention": True},
                confidence=0.8,
                timestamp=datetime.now()
            )
        ]
        
        mock_resource_manager.get_system_resource_usage.return_value = system_usage
        mock_resource_manager.get_all_clusters_resource_usage.return_value = clusters_usage
        mock_resource_manager.get_scaling_recommendations.return_value = scaling_recommendations
        
        # Get recommendations
        recommendations = await resource_optimizer.get_optimization_recommendations()
        
        assert len(recommendations) > 0
        
        # Should have scaling recommendation
        scaling_rec = next((r for r in recommendations if r["type"] == "scaling"), None)
        assert scaling_rec is not None
        assert scaling_rec["cluster_id"] == "test-cluster"
        assert scaling_rec["recommendation"] == "optimize"
        assert scaling_rec["confidence"] == 0.8
    
    def test_get_optimization_history(self, resource_optimizer):
        """Test getting optimization history."""
        # Add some test results to history
        result1 = OptimizationResult(
            timestamp=datetime.now() - timedelta(hours=1),
            strategy=OptimizationStrategy.BALANCED,
            goal=OptimizationGoal.MINIMIZE_COST,
            rules_executed=["test_rule"],
            actions_taken=[{"action": "test_action"}],
            metrics_before={"cpu": 80.0},
            metrics_after={"cpu": 70.0},
            estimated_savings={"cpu_percent": 10.0},
            success=True
        )
        
        result2 = OptimizationResult(
            timestamp=datetime.now(),
            strategy=OptimizationStrategy.AGGRESSIVE,
            goal=OptimizationGoal.MAXIMIZE_PERFORMANCE,
            rules_executed=["another_rule"],
            actions_taken=[{"action": "another_action"}],
            metrics_before={"memory": 90.0},
            metrics_after={"memory": 85.0},
            estimated_savings={"memory_mb": 100.0},
            success=True
        )
        
        resource_optimizer._add_to_history(result1)
        resource_optimizer._add_to_history(result2)
        
        # Get all history
        history = resource_optimizer.get_optimization_history()
        assert len(history) == 2
        
        # Get limited history
        limited_history = resource_optimizer.get_optimization_history(limit=1)
        assert len(limited_history) == 1
        assert limited_history[0] == result2  # Should be the most recent
    
    def test_get_optimization_statistics(self, resource_optimizer):
        """Test getting optimization statistics."""
        # Add some test results
        successful_result = OptimizationResult(
            timestamp=datetime.now(),
            strategy=OptimizationStrategy.BALANCED,
            goal=OptimizationGoal.MINIMIZE_COST,
            rules_executed=["rule1", "rule2"],
            actions_taken=[{"action": "action1"}, {"action": "action2"}],
            metrics_before={},
            metrics_after={},
            estimated_savings={"disk_space_gb": 5.0, "cpu_percent": 10.0},
            success=True
        )
        
        failed_result = OptimizationResult(
            timestamp=datetime.now(),
            strategy=OptimizationStrategy.BALANCED,
            goal=OptimizationGoal.MINIMIZE_COST,
            rules_executed=[],
            actions_taken=[],
            metrics_before={},
            metrics_after={},
            estimated_savings={},
            success=False,
            error_message="Test error"
        )
        
        resource_optimizer._add_to_history(successful_result)
        resource_optimizer._add_to_history(failed_result)
        
        # Get statistics
        stats = resource_optimizer.get_optimization_statistics()
        
        assert stats["total_optimizations"] == 2
        assert stats["successful_optimizations"] == 1
        assert stats["success_rate"] == 50.0
        assert stats["total_actions"] == 2
        assert stats["total_estimated_savings"]["disk_space_gb"] == 5.0
        assert stats["total_estimated_savings"]["cpu_percent"] == 10.0
        assert len(stats["most_executed_rules"]) > 0
    
    def test_calculate_estimated_savings(self, resource_optimizer):
        """Test calculating estimated savings."""
        metrics_before = {
            "total_disk_usage_gb": 20.0,
            "system_cpu_usage": 80.0
        }
        
        metrics_after = {
            "total_disk_usage_gb": 15.0,
            "system_cpu_usage": 70.0
        }
        
        actions_taken = [
            {"action": "delete_cluster", "cluster_id": "test1"},
            {"action": "run_cleanup", "space_freed_gb": 2.0},
            {"action": "stop_cluster", "cluster_id": "test2"}
        ]
        
        savings = resource_optimizer._calculate_estimated_savings(
            metrics_before, metrics_after, actions_taken
        )
        
        assert "disk_space_gb" in savings
        assert "memory_mb" in savings
        assert "cpu_percent" in savings
        assert "cost_reduction_percent" in savings
        
        # Should have actual savings from metrics difference
        assert savings["disk_space_gb"] == 5.0  # 20 - 15
        assert savings["cpu_percent"] == 10.0   # 80 - 70
        
        # Should have some cost reduction estimate
        assert savings["cost_reduction_percent"] > 0
    
    def test_estimate_recommendation_impact(self, resource_optimizer):
        """Test estimating recommendation impact."""
        scaling_rec = ScalingRecommendationResult(
            cluster_id="test-cluster",
            recommendation=ScalingRecommendation.SCALE_DOWN,
            reason="Test reason",
            current_metrics={},
            suggested_changes={},
            confidence=0.8,
            timestamp=datetime.now()
        )
        
        impact = resource_optimizer._estimate_recommendation_impact(scaling_rec)
        
        assert impact["confidence"] == 0.8
        assert impact["estimated_savings_gb"] == 2.0  # Scale down savings
        assert impact["estimated_cost_reduction"] == 10.0
    
    @pytest.mark.asyncio
    async def test_estimate_rule_impact(self, resource_optimizer):
        """Test estimating rule impact."""
        rule = OptimizationRule(
            name="test_rule",
            enabled=True,
            priority=80,
            conditions={},
            actions={"action_type": "cleanup"}
        )
        
        metrics = {
            "total_disk_usage_gb": 10.0,
            "total_clusters": 3
        }
        
        impact = await resource_optimizer._estimate_rule_impact(rule, metrics)
        
        assert impact["priority"] == 80
        assert impact["estimated_savings_gb"] == 2.0  # 20% of 10GB
        assert impact["estimated_cost_reduction"] == 5.0
    
    def test_add_to_history_with_limit(self, resource_optimizer):
        """Test adding to history with size limit."""
        # Set a small history limit for testing
        resource_optimizer.max_history_size = 3
        
        # Add more results than the limit
        for i in range(5):
            result = OptimizationResult(
                timestamp=datetime.now() - timedelta(hours=i),
                strategy=OptimizationStrategy.BALANCED,
                goal=OptimizationGoal.MINIMIZE_COST,
                rules_executed=[],
                actions_taken=[],
                metrics_before={},
                metrics_after={},
                estimated_savings={},
                success=True
            )
            resource_optimizer._add_to_history(result)
        
        # Should only keep the most recent results
        assert len(resource_optimizer.optimization_history) == 3
    
    def test_adjust_rules_for_strategy(self, resource_optimizer):
        """Test adjusting rules based on strategy."""
        # Test conservative strategy
        resource_optimizer._adjust_rules_for_strategy(
            OptimizationStrategy.CONSERVATIVE,
            OptimizationGoal.MINIMIZE_COST
        )
        
        # Consolidate rule should be disabled for conservative
        consolidate_rule = resource_optimizer.optimization_rules.get("consolidate_clusters")
        if consolidate_rule:
            assert consolidate_rule.enabled is False
        
        # Test aggressive strategy
        resource_optimizer._adjust_rules_for_strategy(
            OptimizationStrategy.AGGRESSIVE,
            OptimizationGoal.MINIMIZE_COST
        )
        
        # Consolidate rule should be enabled for aggressive
        consolidate_rule = resource_optimizer.optimization_rules.get("consolidate_clusters")
        if consolidate_rule:
            assert consolidate_rule.enabled is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])