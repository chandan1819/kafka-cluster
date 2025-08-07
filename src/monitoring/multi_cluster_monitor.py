"""
Multi-cluster monitoring and observability system.

This module provides comprehensive monitoring capabilities for multi-cluster
Kafka deployments, including health monitoring, metrics collection, alerting,
and performance tracking.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import json
import psutil
import aiofiles

from src.models.multi_cluster import ClusterDefinition, ClusterStatus
from src.services.multi_cluster_manager import MultiClusterManager
from src.exceptions import MonitoringError


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class MetricType(Enum):
    """Types of metrics collected."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class HealthStatus:
    """Health status for a component."""
    component: str
    cluster_id: Optional[str]
    status: str  # healthy, degraded, unhealthy
    message: str
    last_check: datetime
    response_time_ms: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Metric:
    """Represents a collected metric."""
    name: str
    type: MetricType
    value: float
    timestamp: datetime
    cluster_id: Optional[str] = None
    labels: Dict[str, str] = field(default_factory=dict)
    description: str = ""


@dataclass
class Alert:
    """Represents an alert condition."""
    id: str
    name: str
    severity: AlertSeverity
    message: str
    cluster_id: Optional[str]
    component: str
    triggered_at: datetime
    resolved_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MonitoringConfig:
    """Configuration for monitoring system."""
    health_check_interval: int = 30  # seconds
    metrics_collection_interval: int = 15  # seconds
    alert_evaluation_interval: int = 60  # seconds
    
    # Health check timeouts
    kafka_health_timeout: int = 10
    rest_proxy_health_timeout: int = 5
    ui_health_timeout: int = 5
    
    # Metric retention
    metrics_retention_days: int = 30
    health_status_retention_days: int = 7
    
    # Alert thresholds
    cpu_usage_warning_threshold: float = 70.0
    cpu_usage_critical_threshold: float = 90.0
    memory_usage_warning_threshold: float = 80.0
    memory_usage_critical_threshold: float = 95.0
    disk_usage_warning_threshold: float = 85.0
    disk_usage_critical_threshold: float = 95.0
    
    # Response time thresholds (ms)
    response_time_warning_threshold: float = 1000.0
    response_time_critical_threshold: float = 5000.0
    
    # Enable/disable features
    enable_health_monitoring: bool = True
    enable_metrics_collection: bool = True
    enable_alerting: bool = True
    enable_performance_monitoring: bool = True


class MultiClusterMonitor:
    """
    Comprehensive monitoring system for multi-cluster Kafka deployments.
    
    Provides health monitoring, metrics collection, alerting, and performance
    tracking across all managed clusters.
    """
    
    def __init__(
        self,
        multi_cluster_manager: MultiClusterManager,
        config: Optional[MonitoringConfig] = None,
        storage_path: Optional[Path] = None
    ):
        self.multi_cluster_manager = multi_cluster_manager
        self.config = config or MonitoringConfig()
        self.storage_path = storage_path or Path("monitoring_data")
        
        # Internal state
        self._health_status: Dict[str, List[HealthStatus]] = {}
        self._metrics: Dict[str, List[Metric]] = {}
        self._active_alerts: Dict[str, Alert] = {}
        self._alert_handlers: List[Callable[[Alert], None]] = []
        
        # Monitoring tasks
        self._monitoring_tasks: Set[asyncio.Task] = set()
        self._is_monitoring = False
        
        # Logger
        self.logger = logging.getLogger(__name__)
        
        # Ensure storage directory exists
        self.storage_path.mkdir(parents=True, exist_ok=True)
    
    async def start_monitoring(self) -> None:
        """Start all monitoring tasks."""
        if self._is_monitoring:
            self.logger.warning("Monitoring is already running")
            return
        
        self._is_monitoring = True
        self.logger.info("Starting multi-cluster monitoring")
        
        # Start monitoring tasks
        if self.config.enable_health_monitoring:
            task = asyncio.create_task(self._health_monitoring_loop())
            self._monitoring_tasks.add(task)
        
        if self.config.enable_metrics_collection:
            task = asyncio.create_task(self._metrics_collection_loop())
            self._monitoring_tasks.add(task)
        
        if self.config.enable_alerting:
            task = asyncio.create_task(self._alert_evaluation_loop())
            self._monitoring_tasks.add(task)
        
        if self.config.enable_performance_monitoring:
            task = asyncio.create_task(self._performance_monitoring_loop())
            self._monitoring_tasks.add(task)
        
        # Start cleanup task
        task = asyncio.create_task(self._cleanup_loop())
        self._monitoring_tasks.add(task)
    
    async def stop_monitoring(self) -> None:
        """Stop all monitoring tasks."""
        if not self._is_monitoring:
            return
        
        self.logger.info("Stopping multi-cluster monitoring")
        self._is_monitoring = False
        
        # Cancel all monitoring tasks
        for task in self._monitoring_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if self._monitoring_tasks:
            await asyncio.gather(*self._monitoring_tasks, return_exceptions=True)
        
        self._monitoring_tasks.clear()
    
    async def get_cluster_health(self, cluster_id: str) -> Dict[str, Any]:
        """Get comprehensive health status for a cluster."""
        try:
            cluster_health = {
                "cluster_id": cluster_id,
                "overall_status": "unknown",
                "last_updated": datetime.now().isoformat(),
                "components": {},
                "summary": {
                    "healthy_components": 0,
                    "degraded_components": 0,
                    "unhealthy_components": 0
                }
            }
            
            # Get health status for cluster components
            if cluster_id in self._health_status:
                for health in self._health_status[cluster_id]:
                    cluster_health["components"][health.component] = {
                        "status": health.status,
                        "message": health.message,
                        "last_check": health.last_check.isoformat(),
                        "response_time_ms": health.response_time_ms,
                        "details": health.details
                    }
                    
                    # Update summary
                    if health.status == "healthy":
                        cluster_health["summary"]["healthy_components"] += 1
                    elif health.status == "degraded":
                        cluster_health["summary"]["degraded_components"] += 1
                    else:
                        cluster_health["summary"]["unhealthy_components"] += 1
            
            # Determine overall status
            summary = cluster_health["summary"]
            if summary["unhealthy_components"] > 0:
                cluster_health["overall_status"] = "unhealthy"
            elif summary["degraded_components"] > 0:
                cluster_health["overall_status"] = "degraded"
            elif summary["healthy_components"] > 0:
                cluster_health["overall_status"] = "healthy"
            
            return cluster_health
            
        except Exception as e:
            self.logger.error(f"Error getting cluster health for {cluster_id}: {e}")
            raise MonitoringError(f"Failed to get cluster health: {e}")
    
    async def get_all_clusters_health(self) -> Dict[str, Dict[str, Any]]:
        """Get health status for all clusters."""
        try:
            clusters = await self.multi_cluster_manager.list_clusters()
            health_data = {}
            
            for cluster in clusters:
                health_data[cluster.id] = await self.get_cluster_health(cluster.id)
            
            return health_data
            
        except Exception as e:
            self.logger.error(f"Error getting all clusters health: {e}")
            raise MonitoringError(f"Failed to get all clusters health: {e}")
    
    async def get_cluster_metrics(
        self,
        cluster_id: str,
        metric_names: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get metrics for a specific cluster."""
        try:
            metrics_data = {}
            
            if cluster_id in self._metrics:
                cluster_metrics = self._metrics[cluster_id]
                
                # Filter by time range
                if start_time or end_time:
                    filtered_metrics = []
                    for metric in cluster_metrics:
                        if start_time and metric.timestamp < start_time:
                            continue
                        if end_time and metric.timestamp > end_time:
                            continue
                        filtered_metrics.append(metric)
                    cluster_metrics = filtered_metrics
                
                # Filter by metric names
                if metric_names:
                    cluster_metrics = [
                        m for m in cluster_metrics 
                        if m.name in metric_names
                    ]
                
                # Group by metric name
                for metric in cluster_metrics:
                    if metric.name not in metrics_data:
                        metrics_data[metric.name] = []
                    
                    metrics_data[metric.name].append({
                        "value": metric.value,
                        "timestamp": metric.timestamp.isoformat(),
                        "labels": metric.labels,
                        "description": metric.description
                    })
            
            return metrics_data
            
        except Exception as e:
            self.logger.error(f"Error getting cluster metrics for {cluster_id}: {e}")
            raise MonitoringError(f"Failed to get cluster metrics: {e}")
    
    async def get_system_metrics(self) -> Dict[str, Any]:
        """Get system-wide metrics."""
        try:
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            
            # Memory metrics
            memory = psutil.virtual_memory()
            
            # Disk metrics
            disk = psutil.disk_usage('/')
            
            # Network metrics
            network = psutil.net_io_counters()
            
            return {
                "timestamp": datetime.now().isoformat(),
                "cpu": {
                    "usage_percent": cpu_percent,
                    "count": cpu_count,
                    "load_average": list(psutil.getloadavg()) if hasattr(psutil, 'getloadavg') else None
                },
                "memory": {
                    "total_bytes": memory.total,
                    "available_bytes": memory.available,
                    "used_bytes": memory.used,
                    "usage_percent": memory.percent
                },
                "disk": {
                    "total_bytes": disk.total,
                    "used_bytes": disk.used,
                    "free_bytes": disk.free,
                    "usage_percent": (disk.used / disk.total) * 100
                },
                "network": {
                    "bytes_sent": network.bytes_sent,
                    "bytes_recv": network.bytes_recv,
                    "packets_sent": network.packets_sent,
                    "packets_recv": network.packets_recv
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting system metrics: {e}")
            raise MonitoringError(f"Failed to get system metrics: {e}")
    
    async def get_active_alerts(
        self,
        cluster_id: Optional[str] = None,
        severity: Optional[AlertSeverity] = None
    ) -> List[Dict[str, Any]]:
        """Get active alerts with optional filtering."""
        try:
            alerts = []
            
            for alert in self._active_alerts.values():
                # Filter by cluster
                if cluster_id and alert.cluster_id != cluster_id:
                    continue
                
                # Filter by severity
                if severity and alert.severity != severity:
                    continue
                
                alerts.append({
                    "id": alert.id,
                    "name": alert.name,
                    "severity": alert.severity.value,
                    "message": alert.message,
                    "cluster_id": alert.cluster_id,
                    "component": alert.component,
                    "triggered_at": alert.triggered_at.isoformat(),
                    "metadata": alert.metadata
                })
            
            # Sort by severity and time
            severity_order = {
                AlertSeverity.CRITICAL: 0,
                AlertSeverity.ERROR: 1,
                AlertSeverity.WARNING: 2,
                AlertSeverity.INFO: 3
            }
            
            alerts.sort(key=lambda x: (
                severity_order[AlertSeverity(x["severity"])],
                x["triggered_at"]
            ))
            
            return alerts
            
        except Exception as e:
            self.logger.error(f"Error getting active alerts: {e}")
            raise MonitoringError(f"Failed to get active alerts: {e}")
    
    def add_alert_handler(self, handler: Callable[[Alert], None]) -> None:
        """Add a custom alert handler."""
        self._alert_handlers.append(handler)
    
    def remove_alert_handler(self, handler: Callable[[Alert], None]) -> None:
        """Remove an alert handler."""
        if handler in self._alert_handlers:
            self._alert_handlers.remove(handler)
    
    async def _health_monitoring_loop(self) -> None:
        """Main health monitoring loop."""
        while self._is_monitoring:
            try:
                await self._check_all_clusters_health()
                await asyncio.sleep(self.config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in health monitoring loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retrying
    
    async def _metrics_collection_loop(self) -> None:
        """Main metrics collection loop."""
        while self._is_monitoring:
            try:
                await self._collect_all_metrics()
                await asyncio.sleep(self.config.metrics_collection_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in metrics collection loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retrying
    
    async def _alert_evaluation_loop(self) -> None:
        """Main alert evaluation loop."""
        while self._is_monitoring:
            try:
                await self._evaluate_alerts()
                await asyncio.sleep(self.config.alert_evaluation_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in alert evaluation loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retrying
    
    async def _performance_monitoring_loop(self) -> None:
        """Performance monitoring loop."""
        while self._is_monitoring:
            try:
                await self._collect_performance_metrics()
                await asyncio.sleep(30)  # Performance metrics every 30 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in performance monitoring loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retrying
    
    async def _cleanup_loop(self) -> None:
        """Cleanup old data loop."""
        while self._is_monitoring:
            try:
                await self._cleanup_old_data()
                await asyncio.sleep(3600)  # Cleanup every hour
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(60)  # Brief pause before retrying    

    async def _check_all_clusters_health(self) -> None:
        """Check health of all clusters."""
        try:
            clusters = await self.multi_cluster_manager.list_clusters()
            
            for cluster in clusters:
                await self._check_cluster_health(cluster)
                
        except Exception as e:
            self.logger.error(f"Error checking all clusters health: {e}")
    
    async def _check_cluster_health(self, cluster: ClusterDefinition) -> None:
        """Check health of a specific cluster."""
        try:
            cluster_id = cluster.id
            
            # Initialize health status list for cluster if not exists
            if cluster_id not in self._health_status:
                self._health_status[cluster_id] = []
            
            # Check Kafka health
            kafka_health = await self._check_kafka_health(cluster)
            self._update_health_status(cluster_id, kafka_health)
            
            # Check REST Proxy health
            rest_proxy_health = await self._check_rest_proxy_health(cluster)
            self._update_health_status(cluster_id, rest_proxy_health)
            
            # Check UI health
            ui_health = await self._check_ui_health(cluster)
            self._update_health_status(cluster_id, ui_health)
            
            # Check overall cluster health
            overall_health = await self._check_overall_cluster_health(cluster)
            self._update_health_status(cluster_id, overall_health)
            
        except Exception as e:
            self.logger.error(f"Error checking health for cluster {cluster.id}: {e}")
            
            # Record error health status
            error_health = HealthStatus(
                component="cluster",
                cluster_id=cluster.id,
                status="unhealthy",
                message=f"Health check failed: {str(e)}",
                last_check=datetime.now(),
                details={"error": str(e)}
            )
            self._update_health_status(cluster.id, error_health)
    
    async def _check_kafka_health(self, cluster: ClusterDefinition) -> HealthStatus:
        """Check Kafka broker health."""
        start_time = time.time()
        
        try:
            # Try to get cluster status from multi-cluster manager
            status = await asyncio.wait_for(
                self.multi_cluster_manager.get_cluster_status(cluster.id),
                timeout=self.config.kafka_health_timeout
            )
            
            response_time = (time.time() - start_time) * 1000
            
            if status and hasattr(status, 'kafka_status'):
                kafka_status = getattr(status, 'kafka_status', 'unknown')
                
                if kafka_status == 'running':
                    return HealthStatus(
                        component="kafka",
                        cluster_id=cluster.id,
                        status="healthy",
                        message="Kafka broker is running",
                        last_check=datetime.now(),
                        response_time_ms=response_time,
                        details={"port": cluster.port_allocation.kafka_port}
                    )
                else:
                    return HealthStatus(
                        component="kafka",
                        cluster_id=cluster.id,
                        status="unhealthy",
                        message=f"Kafka broker status: {kafka_status}",
                        last_check=datetime.now(),
                        response_time_ms=response_time,
                        details={"status": kafka_status}
                    )
            else:
                return HealthStatus(
                    component="kafka",
                    cluster_id=cluster.id,
                    status="unhealthy",
                    message="Unable to get Kafka status",
                    last_check=datetime.now(),
                    response_time_ms=response_time
                )
                
        except asyncio.TimeoutError:
            response_time = (time.time() - start_time) * 1000
            return HealthStatus(
                component="kafka",
                cluster_id=cluster.id,
                status="unhealthy",
                message="Kafka health check timed out",
                last_check=datetime.now(),
                response_time_ms=response_time,
                details={"timeout": self.config.kafka_health_timeout}
            )
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthStatus(
                component="kafka",
                cluster_id=cluster.id,
                status="unhealthy",
                message=f"Kafka health check failed: {str(e)}",
                last_check=datetime.now(),
                response_time_ms=response_time,
                details={"error": str(e)}
            )
    
    async def _check_rest_proxy_health(self, cluster: ClusterDefinition) -> HealthStatus:
        """Check REST Proxy health."""
        start_time = time.time()
        
        try:
            # Simulate REST Proxy health check
            # In a real implementation, this would make an HTTP request to the REST Proxy
            await asyncio.sleep(0.1)  # Simulate network call
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthStatus(
                component="rest_proxy",
                cluster_id=cluster.id,
                status="healthy",
                message="REST Proxy is responding",
                last_check=datetime.now(),
                response_time_ms=response_time,
                details={"port": cluster.port_allocation.rest_proxy_port}
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthStatus(
                component="rest_proxy",
                cluster_id=cluster.id,
                status="unhealthy",
                message=f"REST Proxy health check failed: {str(e)}",
                last_check=datetime.now(),
                response_time_ms=response_time,
                details={"error": str(e)}
            )
    
    async def _check_ui_health(self, cluster: ClusterDefinition) -> HealthStatus:
        """Check Kafka UI health."""
        start_time = time.time()
        
        try:
            # Simulate UI health check
            # In a real implementation, this would make an HTTP request to the UI
            await asyncio.sleep(0.05)  # Simulate network call
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthStatus(
                component="ui",
                cluster_id=cluster.id,
                status="healthy",
                message="Kafka UI is responding",
                last_check=datetime.now(),
                response_time_ms=response_time,
                details={"port": cluster.port_allocation.ui_port}
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthStatus(
                component="ui",
                cluster_id=cluster.id,
                status="unhealthy",
                message=f"UI health check failed: {str(e)}",
                last_check=datetime.now(),
                response_time_ms=response_time,
                details={"error": str(e)}
            )
    
    async def _check_overall_cluster_health(self, cluster: ClusterDefinition) -> HealthStatus:
        """Check overall cluster health based on component health."""
        try:
            cluster_id = cluster.id
            
            if cluster_id not in self._health_status:
                return HealthStatus(
                    component="cluster",
                    cluster_id=cluster_id,
                    status="unknown",
                    message="No health data available",
                    last_check=datetime.now()
                )
            
            # Get recent health statuses for this cluster
            recent_statuses = [
                h for h in self._health_status[cluster_id]
                if h.last_check > datetime.now() - timedelta(minutes=5)
                and h.component != "cluster"  # Exclude previous overall status
            ]
            
            if not recent_statuses:
                return HealthStatus(
                    component="cluster",
                    cluster_id=cluster_id,
                    status="unknown",
                    message="No recent health data",
                    last_check=datetime.now()
                )
            
            # Count component statuses
            healthy_count = sum(1 for h in recent_statuses if h.status == "healthy")
            degraded_count = sum(1 for h in recent_statuses if h.status == "degraded")
            unhealthy_count = sum(1 for h in recent_statuses if h.status == "unhealthy")
            
            total_components = len(recent_statuses)
            
            # Determine overall status
            if unhealthy_count > 0:
                status = "unhealthy"
                message = f"{unhealthy_count}/{total_components} components unhealthy"
            elif degraded_count > 0:
                status = "degraded"
                message = f"{degraded_count}/{total_components} components degraded"
            elif healthy_count == total_components:
                status = "healthy"
                message = f"All {total_components} components healthy"
            else:
                status = "unknown"
                message = "Unable to determine overall status"
            
            return HealthStatus(
                component="cluster",
                cluster_id=cluster_id,
                status=status,
                message=message,
                last_check=datetime.now(),
                details={
                    "healthy_components": healthy_count,
                    "degraded_components": degraded_count,
                    "unhealthy_components": unhealthy_count,
                    "total_components": total_components
                }
            )
            
        except Exception as e:
            return HealthStatus(
                component="cluster",
                cluster_id=cluster.id,
                status="unhealthy",
                message=f"Error determining overall health: {str(e)}",
                last_check=datetime.now(),
                details={"error": str(e)}
            )
    
    def _update_health_status(self, cluster_id: str, health: HealthStatus) -> None:
        """Update health status for a cluster component."""
        if cluster_id not in self._health_status:
            self._health_status[cluster_id] = []
        
        # Remove old health status for the same component
        self._health_status[cluster_id] = [
            h for h in self._health_status[cluster_id]
            if h.component != health.component
        ]
        
        # Add new health status
        self._health_status[cluster_id].append(health)
        
        # Keep only recent health statuses (last 24 hours)
        cutoff_time = datetime.now() - timedelta(hours=24)
        self._health_status[cluster_id] = [
            h for h in self._health_status[cluster_id]
            if h.last_check > cutoff_time
        ]
    
    async def _collect_all_metrics(self) -> None:
        """Collect metrics from all clusters."""
        try:
            clusters = await self.multi_cluster_manager.list_clusters()
            
            # Collect system metrics
            await self._collect_system_metrics()
            
            # Collect cluster-specific metrics
            for cluster in clusters:
                await self._collect_cluster_metrics(cluster)
                
        except Exception as e:
            self.logger.error(f"Error collecting all metrics: {e}")
    
    async def _collect_system_metrics(self) -> None:
        """Collect system-wide metrics."""
        try:
            timestamp = datetime.now()
            
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=0.1)
            self._add_metric(Metric(
                name="system_cpu_usage_percent",
                type=MetricType.GAUGE,
                value=cpu_percent,
                timestamp=timestamp,
                description="System CPU usage percentage"
            ))
            
            # Memory metrics
            memory = psutil.virtual_memory()
            self._add_metric(Metric(
                name="system_memory_usage_percent",
                type=MetricType.GAUGE,
                value=memory.percent,
                timestamp=timestamp,
                description="System memory usage percentage"
            ))
            
            self._add_metric(Metric(
                name="system_memory_available_bytes",
                type=MetricType.GAUGE,
                value=memory.available,
                timestamp=timestamp,
                description="System available memory in bytes"
            ))
            
            # Disk metrics
            disk = psutil.disk_usage('/')
            disk_usage_percent = (disk.used / disk.total) * 100
            self._add_metric(Metric(
                name="system_disk_usage_percent",
                type=MetricType.GAUGE,
                value=disk_usage_percent,
                timestamp=timestamp,
                description="System disk usage percentage"
            ))
            
            # Network metrics
            network = psutil.net_io_counters()
            self._add_metric(Metric(
                name="system_network_bytes_sent",
                type=MetricType.COUNTER,
                value=network.bytes_sent,
                timestamp=timestamp,
                description="Total network bytes sent"
            ))
            
            self._add_metric(Metric(
                name="system_network_bytes_received",
                type=MetricType.COUNTER,
                value=network.bytes_recv,
                timestamp=timestamp,
                description="Total network bytes received"
            ))
            
        except Exception as e:
            self.logger.error(f"Error collecting system metrics: {e}")
    
    async def _collect_cluster_metrics(self, cluster: ClusterDefinition) -> None:
        """Collect metrics for a specific cluster."""
        try:
            timestamp = datetime.now()
            cluster_id = cluster.id
            
            # Get cluster health metrics
            if cluster_id in self._health_status:
                healthy_components = sum(
                    1 for h in self._health_status[cluster_id]
                    if h.status == "healthy" and h.component != "cluster"
                )
                
                self._add_metric(Metric(
                    name="cluster_healthy_components",
                    type=MetricType.GAUGE,
                    value=healthy_components,
                    timestamp=timestamp,
                    cluster_id=cluster_id,
                    description="Number of healthy components in cluster"
                ))
                
                # Response time metrics
                for health in self._health_status[cluster_id]:
                    if health.response_time_ms is not None:
                        self._add_metric(Metric(
                            name=f"cluster_{health.component}_response_time_ms",
                            type=MetricType.GAUGE,
                            value=health.response_time_ms,
                            timestamp=timestamp,
                            cluster_id=cluster_id,
                            labels={"component": health.component},
                            description=f"Response time for {health.component} in milliseconds"
                        ))
            
            # Cluster status metrics
            try:
                status = await self.multi_cluster_manager.get_cluster_status(cluster_id)
                if status:
                    # Convert status to numeric value for metrics
                    status_value = 1 if hasattr(status, 'kafka_status') and getattr(status, 'kafka_status') == 'running' else 0
                    
                    self._add_metric(Metric(
                        name="cluster_status",
                        type=MetricType.GAUGE,
                        value=status_value,
                        timestamp=timestamp,
                        cluster_id=cluster_id,
                        description="Cluster status (1=running, 0=stopped)"
                    ))
            except Exception as e:
                self.logger.debug(f"Could not get status for cluster {cluster_id}: {e}")
            
            # Port allocation metrics
            self._add_metric(Metric(
                name="cluster_kafka_port",
                type=MetricType.GAUGE,
                value=cluster.port_allocation.kafka_port,
                timestamp=timestamp,
                cluster_id=cluster_id,
                description="Kafka port for cluster"
            ))
            
        except Exception as e:
            self.logger.error(f"Error collecting metrics for cluster {cluster_id}: {e}")
    
    def _add_metric(self, metric: Metric) -> None:
        """Add a metric to the collection."""
        key = metric.cluster_id or "system"
        
        if key not in self._metrics:
            self._metrics[key] = []
        
        self._metrics[key].append(metric)
        
        # Keep only recent metrics (based on retention policy)
        cutoff_time = datetime.now() - timedelta(days=self.config.metrics_retention_days)
        self._metrics[key] = [
            m for m in self._metrics[key]
            if m.timestamp > cutoff_time
        ]
    
    async def _collect_performance_metrics(self) -> None:
        """Collect performance-related metrics."""
        try:
            timestamp = datetime.now()
            
            # Collect cluster count
            clusters = await self.multi_cluster_manager.list_clusters()
            self._add_metric(Metric(
                name="total_clusters",
                type=MetricType.GAUGE,
                value=len(clusters),
                timestamp=timestamp,
                description="Total number of registered clusters"
            ))
            
            # Collect running clusters count
            running_clusters = 0
            for cluster in clusters:
                try:
                    status = await self.multi_cluster_manager.get_cluster_status(cluster.id)
                    if status and hasattr(status, 'kafka_status') and getattr(status, 'kafka_status') == 'running':
                        running_clusters += 1
                except Exception:
                    pass  # Ignore errors for individual clusters
            
            self._add_metric(Metric(
                name="running_clusters",
                type=MetricType.GAUGE,
                value=running_clusters,
                timestamp=timestamp,
                description="Number of currently running clusters"
            ))
            
            # Collect active alerts count
            self._add_metric(Metric(
                name="active_alerts",
                type=MetricType.GAUGE,
                value=len(self._active_alerts),
                timestamp=timestamp,
                description="Number of active alerts"
            ))
            
        except Exception as e:
            self.logger.error(f"Error collecting performance metrics: {e}")
    
    async def _evaluate_alerts(self) -> None:
        """Evaluate alert conditions and trigger alerts."""
        try:
            # Evaluate system-level alerts
            await self._evaluate_system_alerts()
            
            # Evaluate cluster-level alerts
            clusters = await self.multi_cluster_manager.list_clusters()
            for cluster in clusters:
                await self._evaluate_cluster_alerts(cluster)
            
            # Clean up resolved alerts
            await self._cleanup_resolved_alerts()
            
        except Exception as e:
            self.logger.error(f"Error evaluating alerts: {e}")
    
    async def _evaluate_system_alerts(self) -> None:
        """Evaluate system-level alert conditions."""
        try:
            # Get recent system metrics
            system_metrics = self._metrics.get("system", [])
            recent_metrics = [
                m for m in system_metrics
                if m.timestamp > datetime.now() - timedelta(minutes=5)
            ]
            
            # CPU usage alerts
            cpu_metrics = [m for m in recent_metrics if m.name == "system_cpu_usage_percent"]
            if cpu_metrics:
                latest_cpu = cpu_metrics[-1].value
                
                if latest_cpu >= self.config.cpu_usage_critical_threshold:
                    await self._trigger_alert(
                        alert_id="system_cpu_critical",
                        name="System CPU Usage Critical",
                        severity=AlertSeverity.CRITICAL,
                        message=f"System CPU usage is {latest_cpu:.1f}% (threshold: {self.config.cpu_usage_critical_threshold}%)",
                        cluster_id=None,
                        component="system",
                        metadata={"cpu_usage": latest_cpu, "threshold": self.config.cpu_usage_critical_threshold}
                    )
                elif latest_cpu >= self.config.cpu_usage_warning_threshold:
                    await self._trigger_alert(
                        alert_id="system_cpu_warning",
                        name="System CPU Usage Warning",
                        severity=AlertSeverity.WARNING,
                        message=f"System CPU usage is {latest_cpu:.1f}% (threshold: {self.config.cpu_usage_warning_threshold}%)",
                        cluster_id=None,
                        component="system",
                        metadata={"cpu_usage": latest_cpu, "threshold": self.config.cpu_usage_warning_threshold}
                    )
                else:
                    # Resolve CPU alerts if usage is back to normal
                    await self._resolve_alert("system_cpu_critical")
                    await self._resolve_alert("system_cpu_warning")
            
            # Memory usage alerts
            memory_metrics = [m for m in recent_metrics if m.name == "system_memory_usage_percent"]
            if memory_metrics:
                latest_memory = memory_metrics[-1].value
                
                if latest_memory >= self.config.memory_usage_critical_threshold:
                    await self._trigger_alert(
                        alert_id="system_memory_critical",
                        name="System Memory Usage Critical",
                        severity=AlertSeverity.CRITICAL,
                        message=f"System memory usage is {latest_memory:.1f}% (threshold: {self.config.memory_usage_critical_threshold}%)",
                        cluster_id=None,
                        component="system",
                        metadata={"memory_usage": latest_memory, "threshold": self.config.memory_usage_critical_threshold}
                    )
                elif latest_memory >= self.config.memory_usage_warning_threshold:
                    await self._trigger_alert(
                        alert_id="system_memory_warning",
                        name="System Memory Usage Warning",
                        severity=AlertSeverity.WARNING,
                        message=f"System memory usage is {latest_memory:.1f}% (threshold: {self.config.memory_usage_warning_threshold}%)",
                        cluster_id=None,
                        component="system",
                        metadata={"memory_usage": latest_memory, "threshold": self.config.memory_usage_warning_threshold}
                    )
                else:
                    # Resolve memory alerts if usage is back to normal
                    await self._resolve_alert("system_memory_critical")
                    await self._resolve_alert("system_memory_warning")
            
            # Disk usage alerts
            disk_metrics = [m for m in recent_metrics if m.name == "system_disk_usage_percent"]
            if disk_metrics:
                latest_disk = disk_metrics[-1].value
                
                if latest_disk >= self.config.disk_usage_critical_threshold:
                    await self._trigger_alert(
                        alert_id="system_disk_critical",
                        name="System Disk Usage Critical",
                        severity=AlertSeverity.CRITICAL,
                        message=f"System disk usage is {latest_disk:.1f}% (threshold: {self.config.disk_usage_critical_threshold}%)",
                        cluster_id=None,
                        component="system",
                        metadata={"disk_usage": latest_disk, "threshold": self.config.disk_usage_critical_threshold}
                    )
                elif latest_disk >= self.config.disk_usage_warning_threshold:
                    await self._trigger_alert(
                        alert_id="system_disk_warning",
                        name="System Disk Usage Warning",
                        severity=AlertSeverity.WARNING,
                        message=f"System disk usage is {latest_disk:.1f}% (threshold: {self.config.disk_usage_warning_threshold}%)",
                        cluster_id=None,
                        component="system",
                        metadata={"disk_usage": latest_disk, "threshold": self.config.disk_usage_warning_threshold}
                    )
                else:
                    # Resolve disk alerts if usage is back to normal
                    await self._resolve_alert("system_disk_critical")
                    await self._resolve_alert("system_disk_warning")
            
        except Exception as e:
            self.logger.error(f"Error evaluating system alerts: {e}")
    
    async def _evaluate_cluster_alerts(self, cluster: ClusterDefinition) -> None:
        """Evaluate alert conditions for a specific cluster."""
        try:
            cluster_id = cluster.id
            
            # Check for cluster health alerts
            if cluster_id in self._health_status:
                recent_health = [
                    h for h in self._health_status[cluster_id]
                    if h.last_check > datetime.now() - timedelta(minutes=5)
                ]
                
                # Check for unhealthy components
                unhealthy_components = [h for h in recent_health if h.status == "unhealthy"]
                if unhealthy_components:
                    for health in unhealthy_components:
                        await self._trigger_alert(
                            alert_id=f"cluster_{cluster_id}_{health.component}_unhealthy",
                            name=f"Cluster Component Unhealthy",
                            severity=AlertSeverity.ERROR,
                            message=f"Component {health.component} in cluster {cluster_id} is unhealthy: {health.message}",
                            cluster_id=cluster_id,
                            component=health.component,
                            metadata={"health_status": health.status, "health_message": health.message}
                        )
                else:
                    # Resolve component health alerts if all are healthy
                    for component in ["kafka", "rest_proxy", "ui", "cluster"]:
                        await self._resolve_alert(f"cluster_{cluster_id}_{component}_unhealthy")
                
                # Check for slow response times
                for health in recent_health:
                    if health.response_time_ms is not None:
                        if health.response_time_ms >= self.config.response_time_critical_threshold:
                            await self._trigger_alert(
                                alert_id=f"cluster_{cluster_id}_{health.component}_slow_response",
                                name=f"Cluster Component Slow Response",
                                severity=AlertSeverity.WARNING,
                                message=f"Component {health.component} in cluster {cluster_id} has slow response time: {health.response_time_ms:.1f}ms",
                                cluster_id=cluster_id,
                                component=health.component,
                                metadata={"response_time_ms": health.response_time_ms, "threshold": self.config.response_time_critical_threshold}
                            )
                        elif health.response_time_ms < self.config.response_time_warning_threshold:
                            # Resolve slow response alerts if response time is back to normal
                            await self._resolve_alert(f"cluster_{cluster_id}_{health.component}_slow_response")
            
        except Exception as e:
            self.logger.error(f"Error evaluating alerts for cluster {cluster_id}: {e}")
    
    async def _trigger_alert(
        self,
        alert_id: str,
        name: str,
        severity: AlertSeverity,
        message: str,
        cluster_id: Optional[str],
        component: str,
        metadata: Dict[str, Any]
    ) -> None:
        """Trigger an alert."""
        try:
            # Check if alert is already active
            if alert_id in self._active_alerts:
                # Update existing alert
                existing_alert = self._active_alerts[alert_id]
                existing_alert.message = message
                existing_alert.metadata.update(metadata)
                return
            
            # Create new alert
            alert = Alert(
                id=alert_id,
                name=name,
                severity=severity,
                message=message,
                cluster_id=cluster_id,
                component=component,
                triggered_at=datetime.now(),
                metadata=metadata
            )
            
            self._active_alerts[alert_id] = alert
            
            # Log alert
            self.logger.warning(f"Alert triggered: {name} - {message}")
            
            # Notify alert handlers
            for handler in self._alert_handlers:
                try:
                    handler(alert)
                except Exception as e:
                    self.logger.error(f"Error in alert handler: {e}")
            
        except Exception as e:
            self.logger.error(f"Error triggering alert {alert_id}: {e}")
    
    async def _resolve_alert(self, alert_id: str) -> None:
        """Resolve an active alert."""
        try:
            if alert_id in self._active_alerts:
                alert = self._active_alerts[alert_id]
                alert.resolved_at = datetime.now()
                
                # Log resolution
                self.logger.info(f"Alert resolved: {alert.name}")
                
                # Remove from active alerts
                del self._active_alerts[alert_id]
                
        except Exception as e:
            self.logger.error(f"Error resolving alert {alert_id}: {e}")
    
    async def _cleanup_resolved_alerts(self) -> None:
        """Clean up old resolved alerts."""
        try:
            # Remove alerts that have been resolved for more than 24 hours
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            alerts_to_remove = []
            for alert_id, alert in self._active_alerts.items():
                if alert.resolved_at and alert.resolved_at < cutoff_time:
                    alerts_to_remove.append(alert_id)
            
            for alert_id in alerts_to_remove:
                del self._active_alerts[alert_id]
                
        except Exception as e:
            self.logger.error(f"Error cleaning up resolved alerts: {e}")
    
    async def _cleanup_old_data(self) -> None:
        """Clean up old monitoring data."""
        try:
            current_time = datetime.now()
            
            # Clean up old health status data
            health_cutoff = current_time - timedelta(days=self.config.health_status_retention_days)
            for cluster_id in self._health_status:
                self._health_status[cluster_id] = [
                    h for h in self._health_status[cluster_id]
                    if h.last_check > health_cutoff
                ]
            
            # Clean up old metrics data
            metrics_cutoff = current_time - timedelta(days=self.config.metrics_retention_days)
            for key in self._metrics:
                self._metrics[key] = [
                    m for m in self._metrics[key]
                    if m.timestamp > metrics_cutoff
                ]
            
            # Save data to disk periodically
            await self._save_monitoring_data()
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")
    
    async def _save_monitoring_data(self) -> None:
        """Save monitoring data to disk."""
        try:
            # Save health status data
            health_file = self.storage_path / "health_status.json"
            health_data = {}
            
            for cluster_id, health_list in self._health_status.items():
                health_data[cluster_id] = [
                    {
                        "component": h.component,
                        "cluster_id": h.cluster_id,
                        "status": h.status,
                        "message": h.message,
                        "last_check": h.last_check.isoformat(),
                        "response_time_ms": h.response_time_ms,
                        "details": h.details
                    }
                    for h in health_list
                ]
            
            async with aiofiles.open(health_file, 'w') as f:
                await f.write(json.dumps(health_data, indent=2))
            
            # Save metrics data (sample of recent metrics to avoid large files)
            metrics_file = self.storage_path / "recent_metrics.json"
            recent_cutoff = datetime.now() - timedelta(hours=1)
            
            recent_metrics = {}
            for key, metrics_list in self._metrics.items():
                recent_metrics[key] = [
                    {
                        "name": m.name,
                        "type": m.type.value,
                        "value": m.value,
                        "timestamp": m.timestamp.isoformat(),
                        "cluster_id": m.cluster_id,
                        "labels": m.labels,
                        "description": m.description
                    }
                    for m in metrics_list
                    if m.timestamp > recent_cutoff
                ]
            
            async with aiofiles.open(metrics_file, 'w') as f:
                await f.write(json.dumps(recent_metrics, indent=2))
            
        except Exception as e:
            self.logger.error(f"Error saving monitoring data: {e}")
    
    async def _load_monitoring_data(self) -> None:
        """Load monitoring data from disk."""
        try:
            # Load health status data
            health_file = self.storage_path / "health_status.json"
            if health_file.exists():
                async with aiofiles.open(health_file, 'r') as f:
                    content = await f.read()
                    health_data = json.loads(content)
                    
                    for cluster_id, health_list in health_data.items():
                        self._health_status[cluster_id] = [
                            HealthStatus(
                                component=h["component"],
                                cluster_id=h["cluster_id"],
                                status=h["status"],
                                message=h["message"],
                                last_check=datetime.fromisoformat(h["last_check"]),
                                response_time_ms=h.get("response_time_ms"),
                                details=h.get("details", {})
                            )
                            for h in health_list
                        ]
            
            # Load recent metrics data
            metrics_file = self.storage_path / "recent_metrics.json"
            if metrics_file.exists():
                async with aiofiles.open(metrics_file, 'r') as f:
                    content = await f.read()
                    metrics_data = json.loads(content)
                    
                    for key, metrics_list in metrics_data.items():
                        self._metrics[key] = [
                            Metric(
                                name=m["name"],
                                type=MetricType(m["type"]),
                                value=m["value"],
                                timestamp=datetime.fromisoformat(m["timestamp"]),
                                cluster_id=m.get("cluster_id"),
                                labels=m.get("labels", {}),
                                description=m.get("description", "")
                            )
                            for m in metrics_list
                        ]
            
        except Exception as e:
            self.logger.error(f"Error loading monitoring data: {e}")


# Default alert handlers
def console_alert_handler(alert: Alert) -> None:
    """Simple console alert handler."""
    severity_colors = {
        AlertSeverity.INFO: "\033[94m",      # Blue
        AlertSeverity.WARNING: "\033[93m",   # Yellow
        AlertSeverity.ERROR: "\033[91m",     # Red
        AlertSeverity.CRITICAL: "\033[95m"   # Magenta
    }
    
    reset_color = "\033[0m"
    color = severity_colors.get(alert.severity, "")
    
    print(f"{color}[{alert.severity.value.upper()}] {alert.name}: {alert.message}{reset_color}")


def log_alert_handler(alert: Alert) -> None:
    """Log-based alert handler."""
    logger = logging.getLogger("alerts")
    
    log_methods = {
        AlertSeverity.INFO: logger.info,
        AlertSeverity.WARNING: logger.warning,
        AlertSeverity.ERROR: logger.error,
        AlertSeverity.CRITICAL: logger.critical
    }
    
    log_method = log_methods.get(alert.severity, logger.info)
    log_method(f"{alert.name}: {alert.message} (Cluster: {alert.cluster_id}, Component: {alert.component})")