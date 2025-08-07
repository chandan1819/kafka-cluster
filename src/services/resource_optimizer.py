"""
Resource optimization service for intelligent resource management.

This module provides advanced optimization algorithms and automated
resource management for multi-cluster Kafka deployments.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

from src.services.resource_manager import (
    ResourceManager,
    ResourceUsage,
    ResourceType,
    ScalingRecommendation,
    ScalingRecommendationResult
)
from src.services.multi_cluster_manager import MultiClusterManager
from src.monitoring.multi_cluster_monitor import MultiClusterMonitor
from src.models.multi_cluster import ClusterDefinition
from src.exceptions import OptimizationError


class OptimizationStrategy(Enum):
    """Optimization strategies."""
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    AGGRESSIVE = "aggressive"
    CUSTOM = "custom"


class OptimizationGoal(Enum):
    """Optimization goals."""
    MINIMIZE_COST = "minimize_cost"
    MAXIMIZE_PERFORMANCE = "maximize_performance"
    BALANCE_COST_PERFORMANCE = "balance_cost_performance"
    MINIMIZE_RESOURCE_USAGE = "minimize_resource_usage"


@dataclass
class OptimizationRule:
    """Optimization rule configuration."""
    name: str
    enabled: bool
    priority: int  # Higher number = higher priority
    conditions: Dict[str, Any]
    actions: Dict[str, Any]
    cooldown_minutes: int = 30
    last_executed: Optional[datetime] = None
    
    def can_execute(self) -> bool:
        """Check if rule can be executed (not in cooldown)."""
        if not self.enabled:
            return False
        
        if self.last_executed is None:
            return True
        
        cooldown_end = self.last_executed + timedelta(minutes=self.cooldown_minutes)
        return datetime.now() > cooldown_end


@dataclass
class OptimizationResult:
    """Result of an optimization operation."""
    timestamp: datetime
    strategy: OptimizationStrategy
    goal: OptimizationGoal
    rules_executed: List[str]
    actions_taken: List[Dict[str, Any]]
    metrics_before: Dict[str, float]
    metrics_after: Optional[Dict[str, float]]
    estimated_savings: Dict[str, float]
    success: bool
    error_message: Optional[str] = None


class ResourceOptimizer:
    """
    Advanced resource optimization service.
    
    Provides intelligent resource optimization using machine learning-inspired
    algorithms and configurable optimization strategies.
    """
    
    def __init__(
        self,
        resource_manager: ResourceManager,
        multi_cluster_manager: MultiClusterManager,
        multi_cluster_monitor: Optional[MultiClusterMonitor] = None
    ):
        self.resource_manager = resource_manager
        self.multi_cluster_manager = multi_cluster_manager
        self.multi_cluster_monitor = multi_cluster_monitor
        
        # Optimization configuration
        self.strategy = OptimizationStrategy.BALANCED
        self.goal = OptimizationGoal.BALANCE_COST_PERFORMANCE
        self.optimization_rules: Dict[str, OptimizationRule] = {}
        
        # Optimization history
        self.optimization_history: List[OptimizationResult] = []
        self.max_history_size = 100
        
        # Monitoring
        self._optimization_active = False
        self._optimization_task: Optional[asyncio.Task] = None
        self.optimization_interval = 300  # 5 minutes
        
        self.logger = logging.getLogger(__name__)
        
        # Initialize default optimization rules
        self._initialize_default_rules()
    
    def _initialize_default_rules(self) -> None:
        """Initialize default optimization rules."""
        
        # Rule 1: Scale down idle clusters
        self.optimization_rules["scale_down_idle"] = OptimizationRule(
            name="Scale Down Idle Clusters",
            enabled=True,
            priority=80,
            conditions={
                "cluster_idle_hours": 2,
                "cpu_usage_threshold": 5.0,
                "memory_usage_threshold": 10.0
            },
            actions={
                "action_type": "scale_down",
                "stop_cluster": True,
                "notify_users": True
            },
            cooldown_minutes=60
        )
        
        # Rule 2: Optimize disk usage
        self.optimization_rules["optimize_disk"] = OptimizationRule(
            name="Optimize Disk Usage",
            enabled=True,
            priority=70,
            conditions={
                "disk_usage_gb": 5.0,
                "log_retention_days": 30
            },
            actions={
                "action_type": "cleanup",
                "compress_logs": True,
                "reduce_retention": True,
                "target_retention_days": 7
            },
            cooldown_minutes=120
        )
        
        # Rule 3: Consolidate underutilized clusters
        self.optimization_rules["consolidate_clusters"] = OptimizationRule(
            name="Consolidate Underutilized Clusters",
            enabled=False,  # Disabled by default as it's more aggressive
            priority=60,
            conditions={
                "cluster_count_threshold": 3,
                "average_utilization_threshold": 30.0
            },
            actions={
                "action_type": "consolidate",
                "merge_clusters": True,
                "preserve_data": True
            },
            cooldown_minutes=240
        )
        
        # Rule 4: Auto-cleanup stopped clusters
        self.optimization_rules["cleanup_stopped"] = OptimizationRule(
            name="Auto-cleanup Stopped Clusters",
            enabled=True,
            priority=90,
            conditions={
                "stopped_duration_hours": 24,
                "cluster_state": "stopped"
            },
            actions={
                "action_type": "cleanup",
                "delete_cluster": True,
                "backup_data": True
            },
            cooldown_minutes=30
        )
        
        # Rule 5: Optimize resource allocation
        self.optimization_rules["optimize_allocation"] = OptimizationRule(
            name="Optimize Resource Allocation",
            enabled=True,
            priority=50,
            conditions={
                "system_cpu_usage": 80.0,
                "system_memory_usage": 85.0
            },
            actions={
                "action_type": "rebalance",
                "adjust_heap_sizes": True,
                "redistribute_load": True
            },
            cooldown_minutes=180
        )
    
    async def start_optimization(self) -> None:
        """Start automatic optimization."""
        if self._optimization_active:
            self.logger.warning("Resource optimization is already active")
            return
        
        self._optimization_active = True
        self.logger.info("Starting automatic resource optimization")
        
        self._optimization_task = asyncio.create_task(self._optimization_loop())
    
    async def stop_optimization(self) -> None:
        """Stop automatic optimization."""
        if not self._optimization_active:
            return
        
        self.logger.info("Stopping automatic resource optimization")
        self._optimization_active = False
        
        if self._optimization_task:
            self._optimization_task.cancel()
            try:
                await self._optimization_task
            except asyncio.CancelledError:
                pass
    
    def set_optimization_strategy(self, strategy: OptimizationStrategy, goal: OptimizationGoal) -> None:
        """Set optimization strategy and goal."""
        self.strategy = strategy
        self.goal = goal
        
        # Adjust rule priorities based on strategy
        self._adjust_rules_for_strategy(strategy, goal)
        
        self.logger.info(f"Set optimization strategy to {strategy.value} with goal {goal.value}")
    
    def add_optimization_rule(self, rule: OptimizationRule) -> None:
        """Add custom optimization rule."""
        self.optimization_rules[rule.name] = rule
        self.logger.info(f"Added optimization rule: {rule.name}")
    
    def remove_optimization_rule(self, rule_name: str) -> bool:
        """Remove optimization rule."""
        if rule_name in self.optimization_rules:
            del self.optimization_rules[rule_name]
            self.logger.info(f"Removed optimization rule: {rule_name}")
            return True
        return False
    
    def enable_rule(self, rule_name: str) -> bool:
        """Enable optimization rule."""
        if rule_name in self.optimization_rules:
            self.optimization_rules[rule_name].enabled = True
            self.logger.info(f"Enabled optimization rule: {rule_name}")
            return True
        return False
    
    def disable_rule(self, rule_name: str) -> bool:
        """Disable optimization rule."""
        if rule_name in self.optimization_rules:
            self.optimization_rules[rule_name].enabled = False
            self.logger.info(f"Disabled optimization rule: {rule_name}")
            return True
        return False
    
    async def run_optimization(self, dry_run: bool = False) -> OptimizationResult:
        """Run optimization process."""
        try:
            start_time = datetime.now()
            
            # Collect current metrics
            metrics_before = await self._collect_optimization_metrics()
            
            # Get applicable rules
            applicable_rules = await self._get_applicable_rules()
            
            # Sort rules by priority (highest first)
            applicable_rules.sort(key=lambda r: r.priority, reverse=True)
            
            # Execute rules
            rules_executed = []
            actions_taken = []
            
            for rule in applicable_rules:
                try:
                    if rule.can_execute():
                        actions = await self._execute_optimization_rule(rule, dry_run)
                        if actions:
                            rules_executed.append(rule.name)
                            actions_taken.extend(actions)
                            
                            if not dry_run:
                                rule.last_executed = datetime.now()
                                
                except Exception as e:
                    self.logger.error(f"Error executing rule {rule.name}: {e}")
            
            # Collect metrics after optimization
            metrics_after = None
            if not dry_run and actions_taken:
                # Wait a bit for changes to take effect
                await asyncio.sleep(30)
                metrics_after = await self._collect_optimization_metrics()
            
            # Calculate estimated savings
            estimated_savings = self._calculate_estimated_savings(
                metrics_before, metrics_after, actions_taken
            )
            
            # Create result
            result = OptimizationResult(
                timestamp=start_time,
                strategy=self.strategy,
                goal=self.goal,
                rules_executed=rules_executed,
                actions_taken=actions_taken,
                metrics_before=metrics_before,
                metrics_after=metrics_after,
                estimated_savings=estimated_savings,
                success=True
            )
            
            # Store in history
            self._add_to_history(result)
            
            self.logger.info(f"Optimization completed: {len(rules_executed)} rules executed, {len(actions_taken)} actions taken")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error during optimization: {e}")
            
            result = OptimizationResult(
                timestamp=datetime.now(),
                strategy=self.strategy,
                goal=self.goal,
                rules_executed=[],
                actions_taken=[],
                metrics_before={},
                metrics_after=None,
                estimated_savings={},
                success=False,
                error_message=str(e)
            )
            
            self._add_to_history(result)
            return result
    
    async def get_optimization_recommendations(self) -> List[Dict[str, Any]]:
        """Get optimization recommendations without executing them."""
        try:
            recommendations = []
            
            # Get current metrics
            current_metrics = await self._collect_optimization_metrics()
            
            # Get scaling recommendations from resource manager
            scaling_recs = await self.resource_manager.get_scaling_recommendations()
            
            # Convert scaling recommendations to optimization recommendations
            for scaling_rec in scaling_recs:
                recommendation = {
                    "type": "scaling",
                    "cluster_id": scaling_rec.cluster_id,
                    "recommendation": scaling_rec.recommendation.value,
                    "reason": scaling_rec.reason,
                    "confidence": scaling_rec.confidence,
                    "suggested_changes": scaling_rec.suggested_changes,
                    "estimated_impact": self._estimate_recommendation_impact(scaling_rec)
                }
                recommendations.append(recommendation)
            
            # Add rule-based recommendations
            applicable_rules = await self._get_applicable_rules()
            
            for rule in applicable_rules:
                if rule.can_execute():
                    recommendation = {
                        "type": "rule_based",
                        "rule_name": rule.name,
                        "priority": rule.priority,
                        "conditions": rule.conditions,
                        "actions": rule.actions,
                        "estimated_impact": await self._estimate_rule_impact(rule, current_metrics)
                    }
                    recommendations.append(recommendation)
            
            # Sort by priority/confidence
            recommendations.sort(key=lambda r: r.get("priority", 0) + r.get("confidence", 0), reverse=True)
            
            return recommendations
            
        except Exception as e:
            self.logger.error(f"Error getting optimization recommendations: {e}")
            raise OptimizationError(f"Failed to get optimization recommendations: {e}")
    
    def get_optimization_history(self, limit: Optional[int] = None) -> List[OptimizationResult]:
        """Get optimization history."""
        history = self.optimization_history.copy()
        if limit:
            history = history[-limit:]
        return history
    
    def get_optimization_statistics(self) -> Dict[str, Any]:
        """Get optimization statistics."""
        try:
            if not self.optimization_history:
                return {
                    "total_optimizations": 0,
                    "successful_optimizations": 0,
                    "success_rate": 0.0,
                    "total_actions": 0,
                    "total_estimated_savings": {},
                    "most_executed_rules": [],
                    "average_execution_time": 0.0
                }
            
            successful = [r for r in self.optimization_history if r.success]
            total_actions = sum(len(r.actions_taken) for r in self.optimization_history)
            
            # Calculate total estimated savings
            total_savings = {}
            for result in successful:
                for key, value in result.estimated_savings.items():
                    total_savings[key] = total_savings.get(key, 0) + value
            
            # Count rule executions
            rule_counts = {}
            for result in self.optimization_history:
                for rule_name in result.rules_executed:
                    rule_counts[rule_name] = rule_counts.get(rule_name, 0) + 1
            
            most_executed_rules = sorted(rule_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            
            return {
                "total_optimizations": len(self.optimization_history),
                "successful_optimizations": len(successful),
                "success_rate": len(successful) / len(self.optimization_history) * 100,
                "total_actions": total_actions,
                "total_estimated_savings": total_savings,
                "most_executed_rules": most_executed_rules,
                "average_execution_time": 0.0  # Would need to track execution time
            }
            
        except Exception as e:
            self.logger.error(f"Error getting optimization statistics: {e}")
            return {}
    
    async def _optimization_loop(self) -> None:
        """Main optimization loop."""
        while self._optimization_active:
            try:
                # Run optimization
                result = await self.run_optimization(dry_run=False)
                
                if result.success:
                    self.logger.info(f"Automatic optimization completed: {len(result.actions_taken)} actions taken")
                else:
                    self.logger.warning(f"Automatic optimization failed: {result.error_message}")
                
                await asyncio.sleep(self.optimization_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in optimization loop: {e}")
                await asyncio.sleep(60)  # Wait before retrying
    
    def _adjust_rules_for_strategy(self, strategy: OptimizationStrategy, goal: OptimizationGoal) -> None:
        """Adjust rule priorities based on strategy and goal."""
        
        if strategy == OptimizationStrategy.CONSERVATIVE:
            # Conservative: Lower priorities for aggressive actions
            for rule in self.optimization_rules.values():
                if "delete_cluster" in rule.actions.get("action_type", ""):
                    rule.priority = max(10, rule.priority - 30)
                elif "consolidate" in rule.actions.get("action_type", ""):
                    rule.enabled = False
        
        elif strategy == OptimizationStrategy.AGGRESSIVE:
            # Aggressive: Higher priorities for cost-saving actions
            for rule in self.optimization_rules.values():
                if rule.name == "consolidate_clusters":
                    rule.enabled = True
                    rule.priority += 20
                elif "cleanup" in rule.actions.get("action_type", ""):
                    rule.priority += 10
        
        elif strategy == OptimizationStrategy.BALANCED:
            # Balanced: Default priorities
            pass
        
        # Adjust based on goal
        if goal == OptimizationGoal.MINIMIZE_COST:
            # Prioritize cost-saving rules
            for rule in self.optimization_rules.values():
                if "cleanup" in rule.actions.get("action_type", "") or \
                   "scale_down" in rule.actions.get("action_type", ""):
                    rule.priority += 15
        
        elif goal == OptimizationGoal.MAXIMIZE_PERFORMANCE:
            # Prioritize performance rules
            for rule in self.optimization_rules.values():
                if "rebalance" in rule.actions.get("action_type", "") or \
                   "optimize_allocation" in rule.name.lower():
                    rule.priority += 15
    
    async def _collect_optimization_metrics(self) -> Dict[str, float]:
        """Collect current metrics for optimization."""
        try:
            metrics = {}
            
            # System metrics
            system_usage = await self.resource_manager.get_system_resource_usage()
            
            for key, usage in system_usage.items():
                metrics[f"system_{key}_usage"] = usage.current_usage
                if usage.usage_percentage is not None:
                    metrics[f"system_{key}_percentage"] = usage.usage_percentage
            
            # Cluster metrics
            clusters_usage = await self.resource_manager.get_all_clusters_resource_usage()
            
            total_clusters = len(clusters_usage)
            total_disk_usage = 0
            total_ports = 0
            
            for cluster_id, cluster_usage in clusters_usage.items():
                disk_usage = cluster_usage.get("disk")
                if disk_usage:
                    total_disk_usage += disk_usage.current_usage
                
                port_usage = cluster_usage.get("ports")
                if port_usage:
                    total_ports += port_usage.current_usage
            
            metrics["total_clusters"] = total_clusters
            metrics["total_disk_usage_gb"] = total_disk_usage / 1_000_000_000
            metrics["total_ports_used"] = total_ports
            
            # Add monitoring metrics if available
            if self.multi_cluster_monitor:
                try:
                    active_alerts = await self.multi_cluster_monitor.get_active_alerts()
                    metrics["active_alerts_count"] = len(active_alerts)
                    
                    critical_alerts = [a for a in active_alerts if a.get("severity") == "critical"]
                    metrics["critical_alerts_count"] = len(critical_alerts)
                except Exception as e:
                    self.logger.debug(f"Could not get monitoring metrics: {e}")
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error collecting optimization metrics: {e}")
            return {}
    
    async def _get_applicable_rules(self) -> List[OptimizationRule]:
        """Get rules that are applicable based on current conditions."""
        applicable_rules = []
        current_metrics = await self._collect_optimization_metrics()
        
        for rule in self.optimization_rules.values():
            if not rule.enabled:
                continue
            
            # Check if rule conditions are met
            conditions_met = await self._check_rule_conditions(rule, current_metrics)
            
            if conditions_met:
                applicable_rules.append(rule)
        
        return applicable_rules
    
    async def _check_rule_conditions(self, rule: OptimizationRule, metrics: Dict[str, float]) -> bool:
        """Check if rule conditions are met."""
        try:
            conditions = rule.conditions
            
            # Check system resource thresholds
            if "system_cpu_usage" in conditions:
                if metrics.get("system_cpu_usage", 0) < conditions["system_cpu_usage"]:
                    return False
            
            if "system_memory_usage" in conditions:
                if metrics.get("system_memory_usage", 0) < conditions["system_memory_usage"]:
                    return False
            
            # Check disk usage thresholds
            if "disk_usage_gb" in conditions:
                if metrics.get("total_disk_usage_gb", 0) < conditions["disk_usage_gb"]:
                    return False
            
            # Check cluster count thresholds
            if "cluster_count_threshold" in conditions:
                if metrics.get("total_clusters", 0) < conditions["cluster_count_threshold"]:
                    return False
            
            # Check time-based conditions
            if "stopped_duration_hours" in conditions:
                # This would require checking individual cluster states
                # For now, we'll assume the condition is met if there are clusters
                if metrics.get("total_clusters", 0) == 0:
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking rule conditions for {rule.name}: {e}")
            return False
    
    async def _execute_optimization_rule(self, rule: OptimizationRule, dry_run: bool = False) -> List[Dict[str, Any]]:
        """Execute an optimization rule."""
        try:
            actions = []
            action_type = rule.actions.get("action_type", "")
            
            if action_type == "cleanup":
                actions.extend(await self._execute_cleanup_actions(rule, dry_run))
            elif action_type == "scale_down":
                actions.extend(await self._execute_scale_down_actions(rule, dry_run))
            elif action_type == "rebalance":
                actions.extend(await self._execute_rebalance_actions(rule, dry_run))
            elif action_type == "consolidate":
                actions.extend(await self._execute_consolidate_actions(rule, dry_run))
            
            return actions
            
        except Exception as e:
            self.logger.error(f"Error executing rule {rule.name}: {e}")
            return []
    
    async def _execute_cleanup_actions(self, rule: OptimizationRule, dry_run: bool = False) -> List[Dict[str, Any]]:
        """Execute cleanup actions."""
        actions = []
        
        try:
            if rule.actions.get("delete_cluster"):
                # Find stopped clusters to delete
                clusters = await self.multi_cluster_manager.list_clusters()
                
                for cluster in clusters:
                    try:
                        status = await self.multi_cluster_manager.get_cluster_status(cluster.id)
                        if status and hasattr(status, 'kafka_status'):
                            kafka_status = getattr(status, 'kafka_status')
                            
                            if kafka_status == "stopped":
                                # Check if cluster has been stopped long enough
                                if cluster.last_stopped:
                                    stopped_duration = datetime.now() - cluster.last_stopped
                                    min_duration = timedelta(hours=rule.conditions.get("stopped_duration_hours", 24))
                                    
                                    if stopped_duration > min_duration:
                                        if not dry_run:
                                            await self.multi_cluster_manager.delete_cluster(cluster.id, force=True)
                                        
                                        actions.append({
                                            "action": "delete_cluster",
                                            "cluster_id": cluster.id,
                                            "cluster_name": cluster.name,
                                            "stopped_duration_hours": stopped_duration.total_seconds() / 3600,
                                            "dry_run": dry_run
                                        })
                    except Exception as e:
                        self.logger.warning(f"Error checking cluster {cluster.id} for cleanup: {e}")
            
            if rule.actions.get("compress_logs") or rule.actions.get("reduce_retention"):
                # Run cleanup policies
                cleanup_result = await self.resource_manager.run_cleanup(dry_run=dry_run)
                
                actions.append({
                    "action": "run_cleanup",
                    "policies_executed": cleanup_result.get("policies_executed", []),
                    "space_freed_gb": cleanup_result.get("space_freed_bytes", 0) / 1_000_000_000,
                    "dry_run": dry_run
                })
            
        except Exception as e:
            self.logger.error(f"Error executing cleanup actions: {e}")
        
        return actions
    
    async def _execute_scale_down_actions(self, rule: OptimizationRule, dry_run: bool = False) -> List[Dict[str, Any]]:
        """Execute scale down actions."""
        actions = []
        
        try:
            if rule.actions.get("stop_cluster"):
                # Find idle clusters to stop
                clusters = await self.multi_cluster_manager.list_clusters()
                
                for cluster in clusters:
                    try:
                        # Check if cluster is idle (simplified check)
                        cluster_usage = await self.resource_manager.get_cluster_resource_usage(cluster.id)
                        
                        # For now, we'll use a simple heuristic
                        # In practice, you'd want more sophisticated idle detection
                        if len(cluster_usage) == 0:  # No recent usage data
                            if not dry_run:
                                await self.multi_cluster_manager.stop_cluster(cluster.id)
                            
                            actions.append({
                                "action": "stop_cluster",
                                "cluster_id": cluster.id,
                                "cluster_name": cluster.name,
                                "reason": "idle_cluster",
                                "dry_run": dry_run
                            })
                    except Exception as e:
                        self.logger.warning(f"Error checking cluster {cluster.id} for scale down: {e}")
            
        except Exception as e:
            self.logger.error(f"Error executing scale down actions: {e}")
        
        return actions
    
    async def _execute_rebalance_actions(self, rule: OptimizationRule, dry_run: bool = False) -> List[Dict[str, Any]]:
        """Execute rebalance actions."""
        actions = []
        
        try:
            if rule.actions.get("adjust_heap_sizes"):
                # This would involve adjusting JVM heap sizes for clusters
                # For now, we'll just log the action
                actions.append({
                    "action": "adjust_heap_sizes",
                    "description": "Would adjust JVM heap sizes based on usage patterns",
                    "dry_run": dry_run
                })
            
            if rule.actions.get("redistribute_load"):
                # This would involve load balancing between clusters
                actions.append({
                    "action": "redistribute_load",
                    "description": "Would redistribute load between clusters",
                    "dry_run": dry_run
                })
            
        except Exception as e:
            self.logger.error(f"Error executing rebalance actions: {e}")
        
        return actions
    
    async def _execute_consolidate_actions(self, rule: OptimizationRule, dry_run: bool = False) -> List[Dict[str, Any]]:
        """Execute consolidate actions."""
        actions = []
        
        try:
            if rule.actions.get("merge_clusters"):
                # This would involve merging underutilized clusters
                # This is a complex operation that would require careful implementation
                actions.append({
                    "action": "merge_clusters",
                    "description": "Would merge underutilized clusters (not implemented)",
                    "dry_run": dry_run
                })
            
        except Exception as e:
            self.logger.error(f"Error executing consolidate actions: {e}")
        
        return actions
    
    def _calculate_estimated_savings(
        self,
        metrics_before: Dict[str, float],
        metrics_after: Optional[Dict[str, float]],
        actions_taken: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """Calculate estimated savings from optimization."""
        savings = {
            "disk_space_gb": 0.0,
            "memory_mb": 0.0,
            "cpu_percent": 0.0,
            "cost_reduction_percent": 0.0
        }
        
        try:
            # Calculate savings based on actions taken
            for action in actions_taken:
                action_type = action.get("action", "")
                
                if action_type == "delete_cluster":
                    # Estimate savings from deleted cluster
                    savings["disk_space_gb"] += 1.0  # Estimate 1GB per cluster
                    savings["memory_mb"] += 512.0   # Estimate 512MB per cluster
                    savings["cpu_percent"] += 5.0   # Estimate 5% CPU per cluster
                
                elif action_type == "run_cleanup":
                    space_freed = action.get("space_freed_gb", 0)
                    savings["disk_space_gb"] += space_freed
                
                elif action_type == "stop_cluster":
                    # Estimate savings from stopped cluster
                    savings["memory_mb"] += 256.0   # Estimate 256MB per stopped cluster
                    savings["cpu_percent"] += 3.0   # Estimate 3% CPU per stopped cluster
            
            # Calculate actual savings if we have after metrics
            if metrics_after:
                disk_before = metrics_before.get("total_disk_usage_gb", 0)
                disk_after = metrics_after.get("total_disk_usage_gb", 0)
                if disk_before > disk_after:
                    savings["disk_space_gb"] = disk_before - disk_after
                
                cpu_before = metrics_before.get("system_cpu_usage", 0)
                cpu_after = metrics_after.get("system_cpu_usage", 0)
                if cpu_before > cpu_after:
                    savings["cpu_percent"] = cpu_before - cpu_after
            
            # Estimate cost reduction (simplified)
            if savings["disk_space_gb"] > 0 or savings["cpu_percent"] > 0:
                savings["cost_reduction_percent"] = min(20.0, 
                    (savings["disk_space_gb"] * 0.5) + (savings["cpu_percent"] * 0.3))
            
        except Exception as e:
            self.logger.error(f"Error calculating estimated savings: {e}")
        
        return savings
    
    def _estimate_recommendation_impact(self, scaling_rec: ScalingRecommendationResult) -> Dict[str, float]:
        """Estimate impact of a scaling recommendation."""
        impact = {
            "confidence": scaling_rec.confidence,
            "estimated_savings_gb": 0.0,
            "estimated_cost_reduction": 0.0
        }
        
        if scaling_rec.recommendation == ScalingRecommendation.SCALE_DOWN:
            impact["estimated_savings_gb"] = 2.0
            impact["estimated_cost_reduction"] = 10.0
        elif scaling_rec.recommendation == ScalingRecommendation.OPTIMIZE:
            impact["estimated_savings_gb"] = 1.0
            impact["estimated_cost_reduction"] = 5.0
        
        return impact
    
    async def _estimate_rule_impact(self, rule: OptimizationRule, metrics: Dict[str, float]) -> Dict[str, float]:
        """Estimate impact of executing a rule."""
        impact = {
            "priority": rule.priority,
            "estimated_savings_gb": 0.0,
            "estimated_cost_reduction": 0.0
        }
        
        action_type = rule.actions.get("action_type", "")
        
        if action_type == "cleanup":
            impact["estimated_savings_gb"] = metrics.get("total_disk_usage_gb", 0) * 0.2  # 20% cleanup
            impact["estimated_cost_reduction"] = 5.0
        elif action_type == "scale_down":
            cluster_count = metrics.get("total_clusters", 0)
            impact["estimated_savings_gb"] = cluster_count * 0.5  # 0.5GB per cluster
            impact["estimated_cost_reduction"] = cluster_count * 2.0  # 2% per cluster
        
        return impact
    
    def _add_to_history(self, result: OptimizationResult) -> None:
        """Add optimization result to history."""
        self.optimization_history.append(result)
        
        # Keep only recent history
        if len(self.optimization_history) > self.max_history_size:
            self.optimization_history = self.optimization_history[-self.max_history_size:]