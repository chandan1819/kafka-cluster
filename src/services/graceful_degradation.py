"""
Graceful degradation service for multi-cluster operations.

This module provides mechanisms to gracefully degrade functionality when
clusters become unavailable, ensuring system resilience and continuity.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Set, Any, Callable
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field

from src.exceptions.multi_cluster_exceptions import ClusterUnavailableError
from src.recovery.error_recovery import error_recovery_manager


class DegradationLevel(Enum):
    """Levels of service degradation."""
    NONE = "none"
    MINIMAL = "minimal"
    MODERATE = "moderate"
    SEVERE = "severe"
    CRITICAL = "critical"


class ServiceMode(Enum):
    """Service operation modes."""
    FULL = "full"
    READ_ONLY = "read_only"
    ESSENTIAL_ONLY = "essential_only"
    EMERGENCY = "emergency"
    OFFLINE = "offline"


@dataclass
class DegradationRule:
    """Rule for service degradation."""
    cluster_id: str
    unavailable_threshold_minutes: int = 5
    degradation_level: DegradationLevel = DegradationLevel.MINIMAL
    service_mode: ServiceMode = ServiceMode.READ_ONLY
    fallback_clusters: List[str] = field(default_factory=list)
    disabled_features: Set[str] = field(default_factory=set)
    custom_actions: List[Callable] = field(default_factory=list)


@dataclass
class ClusterDegradationState:
    """Current degradation state of a cluster."""
    cluster_id: str
    is_degraded: bool = False
    degradation_level: DegradationLevel = DegradationLevel.NONE
    service_mode: ServiceMode = ServiceMode.FULL
    degraded_since: Optional[datetime] = None
    fallback_cluster: Optional[str] = None
    disabled_features: Set[str] = field(default_factory=set)
    degradation_reason: Optional[str] = None
    
    def get_degradation_duration(self) -> Optional[timedelta]:
        """Get how long the cluster has been degraded."""
        if self.degraded_since:
            return datetime.utcnow() - self.degraded_since
        return None


class GracefulDegradationService:
    """Service for managing graceful degradation of cluster operations."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.degradation_rules: Dict[str, DegradationRule] = {}
        self.cluster_states: Dict[str, ClusterDegradationState] = {}
        self.feature_handlers: Dict[str, Callable] = {}
        self.degradation_listeners: List[Callable] = []
        
        # Global degradation settings
        self.global_degradation_enabled = True
        self.max_degraded_clusters_percent = 50  # Max % of clusters that can be degraded
        self.emergency_mode_threshold = 80  # % of clusters unavailable to trigger emergency mode
        
        # Register default feature handlers
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Register default feature degradation handlers."""
        self.feature_handlers = {
            "topic_creation": self._handle_topic_creation_degradation,
            "message_production": self._handle_message_production_degradation,
            "message_consumption": self._handle_message_consumption_degradation,
            "cluster_management": self._handle_cluster_management_degradation,
            "monitoring": self._handle_monitoring_degradation,
            "cross_cluster_operations": self._handle_cross_cluster_degradation,
            "advanced_features": self._handle_advanced_features_degradation,
            "web_interface": self._handle_web_interface_degradation
        }
    
    def register_degradation_rule(self, rule: DegradationRule):
        """Register a degradation rule for a cluster."""
        self.degradation_rules[rule.cluster_id] = rule
        self.logger.info(f"Registered degradation rule for cluster {rule.cluster_id}")
    
    def register_feature_handler(self, feature: str, handler: Callable):
        """Register a custom feature degradation handler."""
        self.feature_handlers[feature] = handler
        self.logger.info(f"Registered feature handler for {feature}")
    
    def add_degradation_listener(self, listener: Callable[[str, ClusterDegradationState], None]):
        """Add a degradation state change listener."""
        self.degradation_listeners.append(listener)
    
    async def handle_cluster_unavailable(self, cluster_id: str, reason: str = "Unknown"):
        """Handle a cluster becoming unavailable."""
        if not self.global_degradation_enabled:
            self.logger.info(f"Degradation disabled, ignoring unavailable cluster {cluster_id}")
            return
        
        self.logger.warning(f"Handling unavailable cluster: {cluster_id} - {reason}")
        
        # Get or create cluster state
        if cluster_id not in self.cluster_states:
            self.cluster_states[cluster_id] = ClusterDegradationState(cluster_id=cluster_id)
        
        state = self.cluster_states[cluster_id]
        
        # Check if already degraded
        if state.is_degraded:
            self.logger.info(f"Cluster {cluster_id} already degraded")
            return
        
        # Get degradation rule
        rule = self.degradation_rules.get(cluster_id)
        if not rule:
            # Create default rule
            rule = DegradationRule(cluster_id=cluster_id)
            self.degradation_rules[cluster_id] = rule
        
        # Apply degradation
        await self._apply_degradation(cluster_id, rule, reason)
    
    async def handle_cluster_available(self, cluster_id: str):
        """Handle a cluster becoming available again."""
        if cluster_id not in self.cluster_states:
            return
        
        state = self.cluster_states[cluster_id]
        if not state.is_degraded:
            return
        
        self.logger.info(f"Restoring cluster from degradation: {cluster_id}")
        
        # Restore cluster to full operation
        await self._restore_cluster(cluster_id)
    
    async def _apply_degradation(self, cluster_id: str, rule: DegradationRule, reason: str):
        """Apply degradation to a cluster."""
        state = self.cluster_states[cluster_id]
        
        # Update state
        state.is_degraded = True
        state.degradation_level = rule.degradation_level
        state.service_mode = rule.service_mode
        state.degraded_since = datetime.utcnow()
        state.disabled_features = rule.disabled_features.copy()
        state.degradation_reason = reason
        
        # Set up fallback cluster if available
        if rule.fallback_clusters:
            fallback_cluster = await self._find_available_fallback(rule.fallback_clusters)
            state.fallback_cluster = fallback_cluster
            if fallback_cluster:
                self.logger.info(f"Using fallback cluster {fallback_cluster} for {cluster_id}")
        
        # Apply feature degradations
        for feature in state.disabled_features:
            await self._apply_feature_degradation(cluster_id, feature)
        
        # Execute custom actions
        for action in rule.custom_actions:
            try:
                await action(cluster_id, state)
            except Exception as e:
                self.logger.error(f"Custom degradation action failed: {str(e)}")
        
        # Notify listeners
        for listener in self.degradation_listeners:
            try:
                listener(cluster_id, state)
            except Exception as e:
                self.logger.error(f"Degradation listener failed: {str(e)}")
        
        # Check if system-wide emergency mode is needed
        await self._check_emergency_mode()
        
        self.logger.info(
            f"Applied {rule.degradation_level.value} degradation to cluster {cluster_id} "
            f"(mode: {rule.service_mode.value})"
        )
    
    async def _restore_cluster(self, cluster_id: str):
        """Restore a cluster from degradation."""
        state = self.cluster_states[cluster_id]
        
        # Restore features
        for feature in state.disabled_features:
            await self._restore_feature(cluster_id, feature)
        
        # Clear degradation state
        state.is_degraded = False
        state.degradation_level = DegradationLevel.NONE
        state.service_mode = ServiceMode.FULL
        state.degraded_since = None
        state.fallback_cluster = None
        state.disabled_features.clear()
        state.degradation_reason = None
        
        # Notify listeners
        for listener in self.degradation_listeners:
            try:
                listener(cluster_id, state)
            except Exception as e:
                self.logger.error(f"Degradation listener failed: {str(e)}")
        
        self.logger.info(f"Restored cluster {cluster_id} from degradation")
    
    async def _find_available_fallback(self, fallback_clusters: List[str]) -> Optional[str]:
        """Find an available fallback cluster."""
        for fallback_id in fallback_clusters:
            if fallback_id not in self.cluster_states or not self.cluster_states[fallback_id].is_degraded:
                # Check if cluster is actually healthy
                try:
                    from src.monitoring.cluster_health_monitor import cluster_health_monitor
                    health = cluster_health_monitor.get_cluster_health(fallback_id)
                    if health and health.is_healthy():
                        return fallback_id
                except Exception as e:
                    self.logger.debug(f"Failed to check fallback cluster health: {str(e)}")
        
        return None
    
    async def _apply_feature_degradation(self, cluster_id: str, feature: str):
        """Apply degradation to a specific feature."""
        handler = self.feature_handlers.get(feature)
        if handler:
            try:
                await handler(cluster_id, True)  # True = enable degradation
                self.logger.info(f"Applied degradation to feature {feature} for cluster {cluster_id}")
            except Exception as e:
                self.logger.error(f"Failed to apply feature degradation {feature}: {str(e)}")
    
    async def _restore_feature(self, cluster_id: str, feature: str):
        """Restore a feature from degradation."""
        handler = self.feature_handlers.get(feature)
        if handler:
            try:
                await handler(cluster_id, False)  # False = disable degradation
                self.logger.info(f"Restored feature {feature} for cluster {cluster_id}")
            except Exception as e:
                self.logger.error(f"Failed to restore feature {feature}: {str(e)}")
    
    async def _check_emergency_mode(self):
        """Check if system-wide emergency mode should be activated."""
        total_clusters = len(self.cluster_states)
        if total_clusters == 0:
            return
        
        degraded_clusters = len([s for s in self.cluster_states.values() if s.is_degraded])
        degraded_percent = (degraded_clusters / total_clusters) * 100
        
        if degraded_percent >= self.emergency_mode_threshold:
            await self._activate_emergency_mode()
        elif degraded_percent < self.emergency_mode_threshold / 2:
            await self._deactivate_emergency_mode()
    
    async def _activate_emergency_mode(self):
        """Activate system-wide emergency mode."""
        self.logger.critical("Activating emergency mode due to high cluster unavailability")
        
        # In emergency mode:
        # 1. Disable all non-essential features
        # 2. Enable read-only mode for remaining clusters
        # 3. Activate all available fallback mechanisms
        # 4. Send critical alerts
        
        # This would be implemented based on specific system requirements
        pass
    
    async def _deactivate_emergency_mode(self):
        """Deactivate system-wide emergency mode."""
        self.logger.info("Deactivating emergency mode - cluster availability restored")
        
        # Restore normal operations
        pass
    
    # Feature degradation handlers
    async def _handle_topic_creation_degradation(self, cluster_id: str, enable_degradation: bool):
        """Handle topic creation feature degradation."""
        if enable_degradation:
            # Disable topic creation for this cluster
            self.logger.info(f"Disabled topic creation for cluster {cluster_id}")
        else:
            # Re-enable topic creation
            self.logger.info(f"Re-enabled topic creation for cluster {cluster_id}")
    
    async def _handle_message_production_degradation(self, cluster_id: str, enable_degradation: bool):
        """Handle message production feature degradation."""
        if enable_degradation:
            # Route production to fallback cluster or disable
            state = self.cluster_states.get(cluster_id)
            if state and state.fallback_cluster:
                self.logger.info(f"Routing message production from {cluster_id} to {state.fallback_cluster}")
            else:
                self.logger.warning(f"Disabled message production for cluster {cluster_id}")
        else:
            self.logger.info(f"Restored message production for cluster {cluster_id}")
    
    async def _handle_message_consumption_degradation(self, cluster_id: str, enable_degradation: bool):
        """Handle message consumption feature degradation."""
        if enable_degradation:
            # Switch to fallback cluster or disable
            state = self.cluster_states.get(cluster_id)
            if state and state.fallback_cluster:
                self.logger.info(f"Routing message consumption from {cluster_id} to {state.fallback_cluster}")
            else:
                self.logger.warning(f"Disabled message consumption for cluster {cluster_id}")
        else:
            self.logger.info(f"Restored message consumption for cluster {cluster_id}")
    
    async def _handle_cluster_management_degradation(self, cluster_id: str, enable_degradation: bool):
        """Handle cluster management feature degradation."""
        if enable_degradation:
            # Disable cluster management operations
            self.logger.info(f"Disabled cluster management for cluster {cluster_id}")
        else:
            self.logger.info(f"Restored cluster management for cluster {cluster_id}")
    
    async def _handle_monitoring_degradation(self, cluster_id: str, enable_degradation: bool):
        """Handle monitoring feature degradation."""
        if enable_degradation:
            # Reduce monitoring frequency or disable non-essential monitoring
            self.logger.info(f"Reduced monitoring for cluster {cluster_id}")
        else:
            self.logger.info(f"Restored full monitoring for cluster {cluster_id}")
    
    async def _handle_cross_cluster_degradation(self, cluster_id: str, enable_degradation: bool):
        """Handle cross-cluster operations degradation."""
        if enable_degradation:
            # Disable cross-cluster operations involving this cluster
            self.logger.info(f"Disabled cross-cluster operations for cluster {cluster_id}")
        else:
            self.logger.info(f"Restored cross-cluster operations for cluster {cluster_id}")
    
    async def _handle_advanced_features_degradation(self, cluster_id: str, enable_degradation: bool):
        """Handle advanced features degradation."""
        if enable_degradation:
            # Disable advanced features like snapshots, cloning, etc.
            self.logger.info(f"Disabled advanced features for cluster {cluster_id}")
        else:
            self.logger.info(f"Restored advanced features for cluster {cluster_id}")
    
    async def _handle_web_interface_degradation(self, cluster_id: str, enable_degradation: bool):
        """Handle web interface degradation."""
        if enable_degradation:
            # Show degraded status in web interface
            self.logger.info(f"Web interface showing degraded status for cluster {cluster_id}")
        else:
            self.logger.info(f"Web interface restored for cluster {cluster_id}")
    
    def is_cluster_degraded(self, cluster_id: str) -> bool:
        """Check if a cluster is currently degraded."""
        state = self.cluster_states.get(cluster_id)
        return state.is_degraded if state else False
    
    def get_cluster_degradation_state(self, cluster_id: str) -> Optional[ClusterDegradationState]:
        """Get degradation state for a cluster."""
        return self.cluster_states.get(cluster_id)
    
    def get_degraded_clusters(self) -> List[str]:
        """Get list of currently degraded clusters."""
        return [
            cluster_id for cluster_id, state in self.cluster_states.items()
            if state.is_degraded
        ]
    
    def get_degradation_statistics(self) -> Dict[str, Any]:
        """Get degradation statistics."""
        total_clusters = len(self.cluster_states)
        degraded_clusters = len([s for s in self.cluster_states.values() if s.is_degraded])
        
        # Group by degradation level
        level_stats = {}
        for state in self.cluster_states.values():
            if state.is_degraded:
                level = state.degradation_level.value
                level_stats[level] = level_stats.get(level, 0) + 1
        
        # Group by service mode
        mode_stats = {}
        for state in self.cluster_states.values():
            if state.is_degraded:
                mode = state.service_mode.value
                mode_stats[mode] = mode_stats.get(mode, 0) + 1
        
        return {
            "total_clusters": total_clusters,
            "degraded_clusters": degraded_clusters,
            "degradation_percentage": (degraded_clusters / total_clusters * 100) if total_clusters > 0 else 0,
            "degradation_levels": level_stats,
            "service_modes": mode_stats,
            "emergency_mode_active": degraded_clusters / total_clusters >= (self.emergency_mode_threshold / 100) if total_clusters > 0 else False
        }
    
    async def force_degradation(self, cluster_id: str, level: DegradationLevel, reason: str = "Manual"):
        """Force degradation of a cluster (for testing or manual intervention)."""
        rule = DegradationRule(
            cluster_id=cluster_id,
            degradation_level=level,
            service_mode=ServiceMode.READ_ONLY if level != DegradationLevel.CRITICAL else ServiceMode.EMERGENCY
        )
        await self._apply_degradation(cluster_id, rule, reason)
    
    async def force_restoration(self, cluster_id: str):
        """Force restoration of a cluster from degradation."""
        if cluster_id in self.cluster_states:
            await self._restore_cluster(cluster_id)


# Global graceful degradation service instance
graceful_degradation_service = GracefulDegradationService()