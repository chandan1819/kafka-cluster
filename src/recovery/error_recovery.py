"""
Error recovery system for multi-cluster operations.

This module provides automatic error recovery, retry logic, and graceful degradation
for multi-cluster Kafka manager operations.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Callable, Any, Tuple
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field

from src.exceptions.multi_cluster_exceptions import (
    MultiClusterError, RecoveryAction, ErrorSeverity, ErrorCategory,
    ClusterUnavailableError, ClusterTimeoutError, RecoveryError
)


class RecoveryStatus(Enum):
    """Status of recovery operations."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class RecoveryAttempt:
    """Represents a single recovery attempt."""
    action: RecoveryAction
    timestamp: datetime
    status: RecoveryStatus
    error: Optional[Exception] = None
    duration_seconds: Optional[float] = None
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryPlan:
    """Recovery plan for a specific error."""
    error: MultiClusterError
    actions: List[RecoveryAction]
    max_attempts: int = 3
    backoff_multiplier: float = 2.0
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    attempts: List[RecoveryAttempt] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def is_complete(self) -> bool:
        """Check if recovery plan is complete."""
        return len(self.attempts) >= self.max_attempts or any(
            attempt.status == RecoveryStatus.SUCCESS for attempt in self.attempts
        )
    
    @property
    def is_successful(self) -> bool:
        """Check if recovery was successful."""
        return any(attempt.status == RecoveryStatus.SUCCESS for attempt in self.attempts)


class ErrorRecoveryManager:
    """Manages error recovery for multi-cluster operations."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.recovery_handlers: Dict[RecoveryAction, Callable] = {}
        self.active_recoveries: Dict[str, RecoveryPlan] = {}
        self.recovery_history: List[RecoveryPlan] = []
        self.cluster_health_cache: Dict[str, Tuple[bool, datetime]] = {}
        self.degraded_clusters: Dict[str, datetime] = {}
        
        # Register default recovery handlers
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Register default recovery action handlers."""
        self.recovery_handlers = {
            RecoveryAction.RETRY: self._handle_retry,
            RecoveryAction.RESTART_CLUSTER: self._handle_restart_cluster,
            RecoveryAction.RECREATE_CLUSTER: self._handle_recreate_cluster,
            RecoveryAction.FALLBACK_CLUSTER: self._handle_fallback_cluster,
            RecoveryAction.SCALE_DOWN: self._handle_scale_down,
            RecoveryAction.GRACEFUL_DEGRADATION: self._handle_graceful_degradation,
            RecoveryAction.MANUAL_INTERVENTION: self._handle_manual_intervention,
            RecoveryAction.IGNORE: self._handle_ignore
        }
    
    def register_recovery_handler(self, action: RecoveryAction, handler: Callable):
        """Register a custom recovery handler."""
        self.recovery_handlers[action] = handler
        self.logger.info(f"Registered custom recovery handler for {action.value}")
    
    async def handle_error(self, error: MultiClusterError) -> bool:
        """
        Handle an error with automatic recovery.
        
        Args:
            error: The error to handle
            
        Returns:
            True if recovery was successful, False otherwise
        """
        self.logger.error(f"Handling error: {error.message}", extra=error.to_dict())
        
        # Create recovery plan
        recovery_plan = self._create_recovery_plan(error)
        
        # Store active recovery
        recovery_key = f"{error.cluster_id}_{error.error_code}_{error.timestamp.isoformat()}"
        self.active_recoveries[recovery_key] = recovery_plan
        
        try:
            # Execute recovery plan
            success = await self._execute_recovery_plan(recovery_plan)
            
            # Move to history
            self.recovery_history.append(recovery_plan)
            del self.active_recoveries[recovery_key]
            
            if success:
                self.logger.info(f"Recovery successful for error: {error.error_code}")
            else:
                self.logger.error(f"Recovery failed for error: {error.error_code}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Recovery execution failed: {str(e)}")
            recovery_plan.attempts.append(RecoveryAttempt(
                action=RecoveryAction.MANUAL_INTERVENTION,
                timestamp=datetime.utcnow(),
                status=RecoveryStatus.FAILED,
                error=e
            ))
            self.recovery_history.append(recovery_plan)
            del self.active_recoveries[recovery_key]
            return False
    
    def _create_recovery_plan(self, error: MultiClusterError) -> RecoveryPlan:
        """Create a recovery plan for the given error."""
        # Determine recovery actions based on error type and severity
        actions = error.recovery_actions.copy() if error.recovery_actions else []
        
        # Add default actions based on error category
        if error.category == ErrorCategory.CLUSTER_LIFECYCLE:
            if RecoveryAction.RETRY not in actions:
                actions.insert(0, RecoveryAction.RETRY)
        elif error.category == ErrorCategory.NETWORK:
            if RecoveryAction.RESTART_CLUSTER not in actions:
                actions.append(RecoveryAction.RESTART_CLUSTER)
        elif error.category == ErrorCategory.RESOURCE:
            if RecoveryAction.SCALE_DOWN not in actions:
                actions.insert(0, RecoveryAction.SCALE_DOWN)
        
        # Adjust max attempts based on severity
        max_attempts = {
            ErrorSeverity.LOW: 2,
            ErrorSeverity.MEDIUM: 3,
            ErrorSeverity.HIGH: 4,
            ErrorSeverity.CRITICAL: 5
        }.get(error.severity, 3)
        
        return RecoveryPlan(
            error=error,
            actions=actions,
            max_attempts=max_attempts
        )
    
    async def _execute_recovery_plan(self, plan: RecoveryPlan) -> bool:
        """Execute a recovery plan."""
        self.logger.info(f"Executing recovery plan for {plan.error.error_code}")
        
        for action in plan.actions:
            if plan.is_complete:
                break
            
            attempt_count = len([a for a in plan.attempts if a.action == action])
            if attempt_count >= plan.max_attempts:
                continue
            
            # Calculate delay
            delay = min(
                plan.initial_delay_seconds * (plan.backoff_multiplier ** attempt_count),
                plan.max_delay_seconds
            )
            
            if delay > 0:
                self.logger.info(f"Waiting {delay:.1f}s before recovery attempt")
                await asyncio.sleep(delay)
            
            # Execute recovery action
            attempt = RecoveryAttempt(
                action=action,
                timestamp=datetime.utcnow(),
                status=RecoveryStatus.IN_PROGRESS
            )
            plan.attempts.append(attempt)
            
            try:
                start_time = datetime.utcnow()
                success = await self._execute_recovery_action(action, plan.error)
                end_time = datetime.utcnow()
                
                attempt.duration_seconds = (end_time - start_time).total_seconds()
                attempt.status = RecoveryStatus.SUCCESS if success else RecoveryStatus.FAILED
                
                if success:
                    self.logger.info(f"Recovery action {action.value} succeeded")
                    return True
                else:
                    self.logger.warning(f"Recovery action {action.value} failed")
                    
            except Exception as e:
                attempt.status = RecoveryStatus.FAILED
                attempt.error = e
                self.logger.error(f"Recovery action {action.value} raised exception: {str(e)}")
        
        return False
    
    async def _execute_recovery_action(self, action: RecoveryAction, error: MultiClusterError) -> bool:
        """Execute a specific recovery action."""
        handler = self.recovery_handlers.get(action)
        if not handler:
            self.logger.error(f"No handler registered for recovery action: {action.value}")
            return False
        
        try:
            return await handler(error)
        except Exception as e:
            self.logger.error(f"Recovery handler {action.value} failed: {str(e)}")
            return False
    
    async def _handle_retry(self, error: MultiClusterError) -> bool:
        """Handle retry recovery action."""
        self.logger.info(f"Retrying operation for cluster {error.cluster_id}")
        
        # For retry, we just return True to indicate the operation should be retried
        # The actual retry logic should be implemented in the calling code
        return True
    
    async def _handle_restart_cluster(self, error: MultiClusterError) -> bool:
        """Handle cluster restart recovery action."""
        if not error.cluster_id:
            return False
        
        self.logger.info(f"Restarting cluster {error.cluster_id}")
        
        try:
            # Import here to avoid circular imports
            from src.services.multi_cluster_manager import MultiClusterManager
            
            manager = MultiClusterManager()
            
            # Stop cluster
            await manager.stop_cluster(error.cluster_id)
            await asyncio.sleep(5)  # Wait for graceful shutdown
            
            # Start cluster
            await manager.start_cluster(error.cluster_id)
            
            # Wait for cluster to be ready
            await self._wait_for_cluster_ready(error.cluster_id)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to restart cluster {error.cluster_id}: {str(e)}")
            return False
    
    async def _handle_recreate_cluster(self, error: MultiClusterError) -> bool:
        """Handle cluster recreation recovery action."""
        if not error.cluster_id:
            return False
        
        self.logger.info(f"Recreating cluster {error.cluster_id}")
        
        try:
            from src.services.multi_cluster_manager import MultiClusterManager
            from src.registry.cluster_registry import ClusterRegistry
            
            manager = MultiClusterManager()
            registry = ClusterRegistry()
            
            # Get cluster definition
            cluster_def = await registry.get_cluster(error.cluster_id)
            if not cluster_def:
                return False
            
            # Delete existing cluster
            await manager.delete_cluster(error.cluster_id)
            await asyncio.sleep(10)  # Wait for cleanup
            
            # Recreate cluster
            await manager.create_cluster(cluster_def)
            await manager.start_cluster(error.cluster_id)
            
            # Wait for cluster to be ready
            await self._wait_for_cluster_ready(error.cluster_id)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to recreate cluster {error.cluster_id}: {str(e)}")
            return False
    
    async def _handle_fallback_cluster(self, error: MultiClusterError) -> bool:
        """Handle fallback cluster recovery action."""
        if not error.cluster_id:
            return False
        
        self.logger.info(f"Setting up fallback for cluster {error.cluster_id}")
        
        try:
            # Mark cluster as degraded
            self.degraded_clusters[error.cluster_id] = datetime.utcnow()
            
            # In a real implementation, this would:
            # 1. Find or create a fallback cluster
            # 2. Redirect traffic to the fallback
            # 3. Set up data replication if needed
            
            # For now, we'll just mark it as handled
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup fallback for cluster {error.cluster_id}: {str(e)}")
            return False
    
    async def _handle_scale_down(self, error: MultiClusterError) -> bool:
        """Handle scale down recovery action."""
        self.logger.info(f"Scaling down resources for cluster {error.cluster_id}")
        
        try:
            from src.services.resource_manager import ResourceManager
            
            resource_manager = ResourceManager()
            
            # Run cleanup to free resources
            cleanup_result = await resource_manager.run_cleanup(dry_run=False)
            
            # Check if we freed enough resources
            if cleanup_result.get('space_freed_bytes', 0) > 0:
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to scale down resources: {str(e)}")
            return False
    
    async def _handle_graceful_degradation(self, error: MultiClusterError) -> bool:
        """Handle graceful degradation recovery action."""
        if not error.cluster_id:
            return False
        
        self.logger.info(f"Enabling graceful degradation for cluster {error.cluster_id}")
        
        try:
            # Mark cluster as degraded
            self.degraded_clusters[error.cluster_id] = datetime.utcnow()
            
            # In a real implementation, this would:
            # 1. Reduce cluster functionality
            # 2. Disable non-essential features
            # 3. Set up read-only mode if possible
            # 4. Notify monitoring systems
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to enable graceful degradation: {str(e)}")
            return False
    
    async def _handle_manual_intervention(self, error: MultiClusterError) -> bool:
        """Handle manual intervention recovery action."""
        self.logger.warning(f"Manual intervention required for error: {error.error_code}")
        
        # Log detailed error information for manual review
        self.logger.error(
            f"Manual intervention needed for cluster {error.cluster_id}",
            extra={
                "error_details": error.to_dict(),
                "requires_manual_intervention": True
            }
        )
        
        # In a real implementation, this would:
        # 1. Create a support ticket
        # 2. Send alerts to operations team
        # 3. Update monitoring dashboards
        
        return False  # Manual intervention doesn't automatically resolve the issue
    
    async def _handle_ignore(self, error: MultiClusterError) -> bool:
        """Handle ignore recovery action."""
        self.logger.info(f"Ignoring error: {error.error_code}")
        return True
    
    async def _wait_for_cluster_ready(self, cluster_id: str, timeout_seconds: int = 120) -> bool:
        """Wait for cluster to be ready."""
        start_time = datetime.utcnow()
        
        while (datetime.utcnow() - start_time).total_seconds() < timeout_seconds:
            try:
                from src.monitoring.enhanced_health_monitor import EnhancedHealthMonitor
                
                monitor = EnhancedHealthMonitor()
                health = await monitor.get_cluster_specific_health(cluster_id)
                
                if health['health']['overall_status'] == 'healthy':
                    return True
                
                await asyncio.sleep(5)
                
            except Exception as e:
                self.logger.debug(f"Health check failed during wait: {str(e)}")
                await asyncio.sleep(5)
        
        return False
    
    def is_cluster_degraded(self, cluster_id: str) -> bool:
        """Check if a cluster is in degraded state."""
        return cluster_id in self.degraded_clusters
    
    def get_cluster_degradation_time(self, cluster_id: str) -> Optional[datetime]:
        """Get when a cluster was marked as degraded."""
        return self.degraded_clusters.get(cluster_id)
    
    def restore_cluster_from_degradation(self, cluster_id: str):
        """Restore cluster from degraded state."""
        if cluster_id in self.degraded_clusters:
            del self.degraded_clusters[cluster_id]
            self.logger.info(f"Restored cluster {cluster_id} from degraded state")
    
    def get_recovery_statistics(self) -> Dict[str, Any]:
        """Get recovery statistics."""
        total_recoveries = len(self.recovery_history)
        successful_recoveries = len([r for r in self.recovery_history if r.is_successful])
        
        # Group by error category
        category_stats = {}
        for recovery in self.recovery_history:
            category = recovery.error.category.value
            if category not in category_stats:
                category_stats[category] = {"total": 0, "successful": 0}
            category_stats[category]["total"] += 1
            if recovery.is_successful:
                category_stats[category]["successful"] += 1
        
        # Group by recovery action
        action_stats = {}
        for recovery in self.recovery_history:
            for attempt in recovery.attempts:
                action = attempt.action.value
                if action not in action_stats:
                    action_stats[action] = {"total": 0, "successful": 0}
                action_stats[action]["total"] += 1
                if attempt.status == RecoveryStatus.SUCCESS:
                    action_stats[action]["successful"] += 1
        
        return {
            "total_recoveries": total_recoveries,
            "successful_recoveries": successful_recoveries,
            "success_rate": successful_recoveries / total_recoveries if total_recoveries > 0 else 0,
            "active_recoveries": len(self.active_recoveries),
            "degraded_clusters": len(self.degraded_clusters),
            "category_statistics": category_stats,
            "action_statistics": action_stats
        }


# Global error recovery manager instance
error_recovery_manager = ErrorRecoveryManager()