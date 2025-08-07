"""
Enhanced health monitoring that integrates multi-cluster monitoring
with the existing health monitoring system.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass

from src.services.health_monitor import HealthMonitor, ServiceHealth, HealthStatus
from src.monitoring.multi_cluster_monitor import (
    MultiClusterMonitor,
    MonitoringConfig,
    Alert,
    AlertSeverity,
    console_alert_handler,
    log_alert_handler
)
from src.services.multi_cluster_manager import MultiClusterManager
from src.models.multi_cluster import ClusterDefinition
from src.exceptions import MonitoringError


@dataclass
class EnhancedHealthSummary:
    """Enhanced health summary including multi-cluster information."""
    overall_status: HealthStatus
    single_cluster_health: Dict[str, ServiceHealth]
    multi_cluster_health: Dict[str, Dict[str, Any]]
    system_metrics: Dict[str, Any]
    active_alerts: List[Dict[str, Any]]
    last_updated: datetime
    cluster_count: int
    running_clusters: int


class EnhancedHealthMonitor:
    """
    Enhanced health monitor that combines single-cluster and multi-cluster monitoring.
    
    This class integrates the existing HealthMonitor with the new MultiClusterMonitor
    to provide comprehensive monitoring across all deployment scenarios.
    """
    
    def __init__(
        self,
        single_cluster_monitor: HealthMonitor,
        multi_cluster_manager: Optional[MultiClusterManager] = None,
        monitoring_config: Optional[MonitoringConfig] = None
    ):
        self.single_cluster_monitor = single_cluster_monitor
        self.multi_cluster_manager = multi_cluster_manager
        self.multi_cluster_monitor: Optional[MultiClusterMonitor] = None
        
        # Initialize multi-cluster monitoring if manager is provided
        if multi_cluster_manager:
            self.multi_cluster_monitor = MultiClusterMonitor(
                multi_cluster_manager=multi_cluster_manager,
                config=monitoring_config or MonitoringConfig()
            )
            
            # Add default alert handlers
            self.multi_cluster_monitor.add_alert_handler(console_alert_handler)
            self.multi_cluster_monitor.add_alert_handler(log_alert_handler)
        
        self.logger = logging.getLogger(__name__)
        self._is_monitoring = False
    
    async def start_monitoring(self) -> None:
        """Start both single-cluster and multi-cluster monitoring."""
        try:
            self.logger.info("Starting enhanced health monitoring")
            
            # Start single-cluster monitoring
            await self.single_cluster_monitor.start_monitoring()
            
            # Start multi-cluster monitoring if available
            if self.multi_cluster_monitor:
                await self.multi_cluster_monitor.start_monitoring()
            
            self._is_monitoring = True
            self.logger.info("Enhanced health monitoring started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start enhanced health monitoring: {e}")
            raise MonitoringError(f"Failed to start monitoring: {e}")
    
    async def stop_monitoring(self) -> None:
        """Stop both single-cluster and multi-cluster monitoring."""
        try:
            self.logger.info("Stopping enhanced health monitoring")
            
            # Stop single-cluster monitoring
            await self.single_cluster_monitor.stop_monitoring()
            
            # Stop multi-cluster monitoring if available
            if self.multi_cluster_monitor:
                await self.multi_cluster_monitor.stop_monitoring()
            
            self._is_monitoring = False
            self.logger.info("Enhanced health monitoring stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error stopping enhanced health monitoring: {e}")
    
    async def get_comprehensive_health(self) -> EnhancedHealthSummary:
        """Get comprehensive health status including all monitoring data."""
        try:
            # Get single-cluster health
            single_cluster_health = await self.single_cluster_monitor.get_health_status()
            
            # Get multi-cluster health if available
            multi_cluster_health = {}
            system_metrics = {}
            active_alerts = []
            cluster_count = 0
            running_clusters = 0
            
            if self.multi_cluster_monitor:
                # Get all clusters health
                multi_cluster_health = await self.multi_cluster_monitor.get_all_clusters_health()
                
                # Get system metrics
                system_metrics = await self.multi_cluster_monitor.get_system_metrics()
                
                # Get active alerts
                active_alerts = await self.multi_cluster_monitor.get_active_alerts()
                
                # Count clusters
                if self.multi_cluster_manager:
                    clusters = await self.multi_cluster_manager.list_clusters()
                    cluster_count = len(clusters)
                    
                    # Count running clusters
                    for cluster in clusters:
                        try:
                            status = await self.multi_cluster_manager.get_cluster_status(cluster.id)
                            if status and hasattr(status, 'kafka_status') and getattr(status, 'kafka_status') == 'running':
                                running_clusters += 1
                        except Exception:
                            pass  # Ignore errors for individual clusters
            
            # Determine overall status
            overall_status = self._determine_overall_status(
                single_cluster_health,
                multi_cluster_health,
                active_alerts
            )
            
            return EnhancedHealthSummary(
                overall_status=overall_status,
                single_cluster_health=single_cluster_health,
                multi_cluster_health=multi_cluster_health,
                system_metrics=system_metrics,
                active_alerts=active_alerts,
                last_updated=datetime.now(),
                cluster_count=cluster_count,
                running_clusters=running_clusters
            )
            
        except Exception as e:
            self.logger.error(f"Error getting comprehensive health: {e}")
            raise MonitoringError(f"Failed to get comprehensive health: {e}")
    
    async def get_cluster_specific_health(self, cluster_id: str) -> Dict[str, Any]:
        """Get health status for a specific cluster."""
        try:
            if not self.multi_cluster_monitor:
                raise MonitoringError("Multi-cluster monitoring not available")
            
            cluster_health = await self.multi_cluster_monitor.get_cluster_health(cluster_id)
            cluster_metrics = await self.multi_cluster_monitor.get_cluster_metrics(cluster_id)
            cluster_alerts = await self.multi_cluster_monitor.get_active_alerts(cluster_id=cluster_id)
            
            return {
                "health": cluster_health,
                "metrics": cluster_metrics,
                "alerts": cluster_alerts,
                "last_updated": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting health for cluster {cluster_id}: {e}")
            raise MonitoringError(f"Failed to get cluster health: {e}")
    
    async def get_system_overview(self) -> Dict[str, Any]:
        """Get system-wide overview including resource usage and performance."""
        try:
            overview = {
                "timestamp": datetime.now().isoformat(),
                "monitoring_status": {
                    "single_cluster_monitoring": self.single_cluster_monitor.is_monitoring,
                    "multi_cluster_monitoring": self.multi_cluster_monitor is not None and self.multi_cluster_monitor._is_monitoring
                }
            }
            
            # Add single-cluster information
            single_health = await self.single_cluster_monitor.get_health_status()
            overview["single_cluster"] = {
                "services": {name: {
                    "status": health.status.value,
                    "last_check": health.last_check.isoformat(),
                    "response_time_ms": health.response_time_ms,
                    "error_message": health.error_message
                } for name, health in single_health.items()}
            }
            
            # Add multi-cluster information if available
            if self.multi_cluster_monitor:
                system_metrics = await self.multi_cluster_monitor.get_system_metrics()
                active_alerts = await self.multi_cluster_monitor.get_active_alerts()
                
                overview["system_resources"] = system_metrics
                overview["alerts"] = {
                    "total_active": len(active_alerts),
                    "by_severity": self._count_alerts_by_severity(active_alerts),
                    "recent_alerts": active_alerts[:5]  # Last 5 alerts
                }
                
                if self.multi_cluster_manager:
                    clusters = await self.multi_cluster_manager.list_clusters()
                    overview["clusters"] = {
                        "total": len(clusters),
                        "by_environment": self._count_clusters_by_environment(clusters),
                        "by_status": await self._count_clusters_by_status(clusters)
                    }
            
            return overview
            
        except Exception as e:
            self.logger.error(f"Error getting system overview: {e}")
            raise MonitoringError(f"Failed to get system overview: {e}")
    
    async def get_performance_metrics(
        self,
        time_range: Optional[timedelta] = None,
        cluster_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get performance metrics for specified time range and cluster."""
        try:
            if not self.multi_cluster_monitor:
                return {"error": "Multi-cluster monitoring not available"}
            
            time_range = time_range or timedelta(hours=1)
            start_time = datetime.now() - time_range
            
            if cluster_id:
                # Get metrics for specific cluster
                metrics = await self.multi_cluster_monitor.get_cluster_metrics(
                    cluster_id=cluster_id,
                    start_time=start_time
                )
                return {
                    "cluster_id": cluster_id,
                    "time_range": str(time_range),
                    "metrics": metrics
                }
            else:
                # Get system-wide metrics
                system_metrics = await self.multi_cluster_monitor.get_cluster_metrics(
                    cluster_id="system",  # System metrics are stored with "system" key
                    start_time=start_time
                )
                return {
                    "time_range": str(time_range),
                    "system_metrics": system_metrics
                }
                
        except Exception as e:
            self.logger.error(f"Error getting performance metrics: {e}")
            raise MonitoringError(f"Failed to get performance metrics: {e}")
    
    async def trigger_health_check(self, cluster_id: Optional[str] = None) -> Dict[str, Any]:
        """Trigger immediate health check for all services or specific cluster."""
        try:
            results = {}
            
            # Trigger single-cluster health check
            single_health = await self.single_cluster_monitor.check_all_services()
            results["single_cluster"] = {
                name: {
                    "status": health.status.value,
                    "response_time_ms": health.response_time_ms,
                    "error_message": health.error_message
                }
                for name, health in single_health.items()
            }
            
            # Trigger multi-cluster health check if available
            if self.multi_cluster_monitor and self.multi_cluster_manager:
                if cluster_id:
                    # Check specific cluster
                    cluster = await self.multi_cluster_manager.registry.get_cluster(cluster_id)
                    if cluster:
                        await self.multi_cluster_monitor._check_cluster_health(cluster)
                        cluster_health = await self.multi_cluster_monitor.get_cluster_health(cluster_id)
                        results["cluster"] = cluster_health
                    else:
                        results["error"] = f"Cluster {cluster_id} not found"
                else:
                    # Check all clusters
                    await self.multi_cluster_monitor._check_all_clusters_health()
                    all_health = await self.multi_cluster_monitor.get_all_clusters_health()
                    results["all_clusters"] = all_health
            
            results["timestamp"] = datetime.now().isoformat()
            return results
            
        except Exception as e:
            self.logger.error(f"Error triggering health check: {e}")
            raise MonitoringError(f"Failed to trigger health check: {e}")
    
    def add_alert_handler(self, handler) -> None:
        """Add custom alert handler to multi-cluster monitoring."""
        if self.multi_cluster_monitor:
            self.multi_cluster_monitor.add_alert_handler(handler)
    
    def remove_alert_handler(self, handler) -> None:
        """Remove alert handler from multi-cluster monitoring."""
        if self.multi_cluster_monitor:
            self.multi_cluster_monitor.remove_alert_handler(handler)
    
    def _determine_overall_status(
        self,
        single_cluster_health: Dict[str, ServiceHealth],
        multi_cluster_health: Dict[str, Dict[str, Any]],
        active_alerts: List[Dict[str, Any]]
    ) -> HealthStatus:
        """Determine overall system health status."""
        try:
            # Check for critical alerts
            critical_alerts = [a for a in active_alerts if a.get("severity") == "critical"]
            if critical_alerts:
                return HealthStatus.UNHEALTHY
            
            # Check single-cluster health
            single_unhealthy = any(
                health.status == HealthStatus.UNHEALTHY
                for health in single_cluster_health.values()
            )
            
            single_degraded = any(
                health.status == HealthStatus.DEGRADED
                for health in single_cluster_health.values()
            )
            
            # Check multi-cluster health
            multi_unhealthy = any(
                cluster_health.get("overall_status") == "unhealthy"
                for cluster_health in multi_cluster_health.values()
            )
            
            multi_degraded = any(
                cluster_health.get("overall_status") == "degraded"
                for cluster_health in multi_cluster_health.values()
            )
            
            # Determine overall status
            if single_unhealthy or multi_unhealthy:
                return HealthStatus.UNHEALTHY
            elif single_degraded or multi_degraded:
                return HealthStatus.DEGRADED
            elif single_cluster_health or multi_cluster_health:
                return HealthStatus.HEALTHY
            else:
                return HealthStatus.UNKNOWN
                
        except Exception as e:
            self.logger.error(f"Error determining overall status: {e}")
            return HealthStatus.UNKNOWN
    
    def _count_alerts_by_severity(self, alerts: List[Dict[str, Any]]) -> Dict[str, int]:
        """Count alerts by severity level."""
        counts = {"info": 0, "warning": 0, "error": 0, "critical": 0}
        
        for alert in alerts:
            severity = alert.get("severity", "info")
            if severity in counts:
                counts[severity] += 1
        
        return counts
    
    def _count_clusters_by_environment(self, clusters: List[ClusterDefinition]) -> Dict[str, int]:
        """Count clusters by environment."""
        counts = {}
        
        for cluster in clusters:
            env = cluster.environment
            counts[env] = counts.get(env, 0) + 1
        
        return counts
    
    async def _count_clusters_by_status(self, clusters: List[ClusterDefinition]) -> Dict[str, int]:
        """Count clusters by status."""
        counts = {"running": 0, "stopped": 0, "unknown": 0}
        
        if not self.multi_cluster_manager:
            return counts
        
        for cluster in clusters:
            try:
                status = await self.multi_cluster_manager.get_cluster_status(cluster.id)
                if status and hasattr(status, 'kafka_status'):
                    kafka_status = getattr(status, 'kafka_status')
                    if kafka_status == 'running':
                        counts["running"] += 1
                    elif kafka_status == 'stopped':
                        counts["stopped"] += 1
                    else:
                        counts["unknown"] += 1
                else:
                    counts["unknown"] += 1
            except Exception:
                counts["unknown"] += 1
        
        return counts


# Convenience function to create enhanced monitor
def create_enhanced_monitor(
    single_cluster_monitor: HealthMonitor,
    multi_cluster_manager: Optional[MultiClusterManager] = None,
    monitoring_config: Optional[MonitoringConfig] = None
) -> EnhancedHealthMonitor:
    """Create an enhanced health monitor with appropriate configuration."""
    return EnhancedHealthMonitor(
        single_cluster_monitor=single_cluster_monitor,
        multi_cluster_manager=multi_cluster_manager,
        monitoring_config=monitoring_config
    )