"""
Cluster health monitoring with automatic recovery capabilities.

This module provides continuous health monitoring for clusters with automatic
restart and recovery mechanisms when clusters become unhealthy.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Callable, Any, Set
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field

from src.exceptions.multi_cluster_exceptions import (
    ClusterHealthError, ClusterUnavailableError, ClusterTimeoutError
)
from src.recovery.error_recovery import error_recovery_manager


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNAVAILABLE = "unavailable"


class HealthCheckType(Enum):
    """Types of health checks."""
    KAFKA_BROKER = "kafka_broker"
    REST_PROXY = "rest_proxy"
    UI = "ui"
    JMX = "jmx"
    DISK_SPACE = "disk_space"
    MEMORY = "memory"
    CPU = "cpu"
    NETWORK = "network"


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    check_type: HealthCheckType
    status: HealthStatus
    message: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    response_time_ms: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ClusterHealthState:
    """Current health state of a cluster."""
    cluster_id: str
    overall_status: HealthStatus
    last_check: datetime
    check_results: List[HealthCheckResult] = field(default_factory=list)
    consecutive_failures: int = 0
    last_healthy: Optional[datetime] = None
    restart_count: int = 0
    last_restart: Optional[datetime] = None
    
    def is_healthy(self) -> bool:
        """Check if cluster is healthy."""
        return self.overall_status == HealthStatus.HEALTHY
    
    def needs_restart(self, max_failures: int = 3, restart_cooldown_minutes: int = 10) -> bool:
        """Check if cluster needs restart."""
        if self.consecutive_failures < max_failures:
            return False
        
        if self.last_restart:
            cooldown_expired = datetime.utcnow() - self.last_restart > timedelta(minutes=restart_cooldown_minutes)
            return cooldown_expired
        
        return True


class ClusterHealthMonitor:
    """Monitors cluster health and performs automatic recovery."""
    
    def __init__(self, check_interval_seconds: int = 30):
        self.logger = logging.getLogger(__name__)
        self.check_interval_seconds = check_interval_seconds
        self.cluster_states: Dict[str, ClusterHealthState] = {}
        self.health_check_handlers: Dict[HealthCheckType, Callable] = {}
        self.monitoring_tasks: Dict[str, asyncio.Task] = {}
        self.is_running = False
        self.health_listeners: List[Callable] = []
        
        # Configuration
        self.max_consecutive_failures = 3
        self.restart_cooldown_minutes = 10
        self.health_check_timeout_seconds = 30
        self.auto_restart_enabled = True
        self.auto_recovery_enabled = True
        
        # Register default health check handlers
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Register default health check handlers."""
        self.health_check_handlers = {
            HealthCheckType.KAFKA_BROKER: self._check_kafka_broker,
            HealthCheckType.REST_PROXY: self._check_rest_proxy,
            HealthCheckType.UI: self._check_ui,
            HealthCheckType.JMX: self._check_jmx,
            HealthCheckType.DISK_SPACE: self._check_disk_space,
            HealthCheckType.MEMORY: self._check_memory,
            HealthCheckType.CPU: self._check_cpu,
            HealthCheckType.NETWORK: self._check_network
        }
    
    def register_health_check_handler(self, check_type: HealthCheckType, handler: Callable):
        """Register a custom health check handler."""
        self.health_check_handlers[check_type] = handler
        self.logger.info(f"Registered custom health check handler for {check_type.value}")
    
    def add_health_listener(self, listener: Callable[[str, ClusterHealthState], None]):
        """Add a health state change listener."""
        self.health_listeners.append(listener)
    
    async def start_monitoring(self, cluster_ids: List[str]):
        """Start monitoring specified clusters."""
        self.is_running = True
        self.logger.info(f"Starting health monitoring for {len(cluster_ids)} clusters")
        
        for cluster_id in cluster_ids:
            if cluster_id not in self.cluster_states:
                self.cluster_states[cluster_id] = ClusterHealthState(
                    cluster_id=cluster_id,
                    overall_status=HealthStatus.UNAVAILABLE,
                    last_check=datetime.utcnow()
                )
            
            # Start monitoring task for each cluster
            task = asyncio.create_task(self._monitor_cluster(cluster_id))
            self.monitoring_tasks[cluster_id] = task
    
    async def stop_monitoring(self, cluster_ids: Optional[List[str]] = None):
        """Stop monitoring specified clusters or all clusters."""
        if cluster_ids is None:
            cluster_ids = list(self.monitoring_tasks.keys())
        
        self.logger.info(f"Stopping health monitoring for {len(cluster_ids)} clusters")
        
        for cluster_id in cluster_ids:
            if cluster_id in self.monitoring_tasks:
                task = self.monitoring_tasks[cluster_id]
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                del self.monitoring_tasks[cluster_id]
        
        if not self.monitoring_tasks:
            self.is_running = False
    
    async def _monitor_cluster(self, cluster_id: str):
        """Monitor a single cluster continuously."""
        self.logger.info(f"Starting health monitoring for cluster {cluster_id}")
        
        while self.is_running:
            try:
                await self._perform_health_check(cluster_id)
                await asyncio.sleep(self.check_interval_seconds)
            except asyncio.CancelledError:
                self.logger.info(f"Health monitoring cancelled for cluster {cluster_id}")
                break
            except Exception as e:
                self.logger.error(f"Error in health monitoring for cluster {cluster_id}: {str(e)}")
                await asyncio.sleep(self.check_interval_seconds)
    
    async def _perform_health_check(self, cluster_id: str):
        """Perform comprehensive health check for a cluster."""
        state = self.cluster_states[cluster_id]
        check_results = []
        
        # Perform all health checks
        for check_type in HealthCheckType:
            handler = self.health_check_handlers.get(check_type)
            if handler:
                try:
                    result = await asyncio.wait_for(
                        handler(cluster_id),
                        timeout=self.health_check_timeout_seconds
                    )
                    check_results.append(result)
                except asyncio.TimeoutError:
                    check_results.append(HealthCheckResult(
                        check_type=check_type,
                        status=HealthStatus.CRITICAL,
                        message=f"Health check timeout after {self.health_check_timeout_seconds}s"
                    ))
                except Exception as e:
                    check_results.append(HealthCheckResult(
                        check_type=check_type,
                        status=HealthStatus.CRITICAL,
                        message=f"Health check failed: {str(e)}"
                    ))
        
        # Update cluster state
        previous_status = state.overall_status
        state.check_results = check_results
        state.last_check = datetime.utcnow()
        state.overall_status = self._calculate_overall_status(check_results)
        
        # Update failure tracking
        if state.overall_status in [HealthStatus.CRITICAL, HealthStatus.UNAVAILABLE]:
            state.consecutive_failures += 1
        else:
            state.consecutive_failures = 0
            state.last_healthy = datetime.utcnow()
        
        # Log status changes
        if previous_status != state.overall_status:
            self.logger.info(
                f"Cluster {cluster_id} health status changed: {previous_status.value} -> {state.overall_status.value}"
            )
            
            # Notify listeners
            for listener in self.health_listeners:
                try:
                    listener(cluster_id, state)
                except Exception as e:
                    self.logger.error(f"Health listener failed: {str(e)}")
        
        # Handle unhealthy clusters
        if state.overall_status in [HealthStatus.CRITICAL, HealthStatus.UNAVAILABLE]:
            await self._handle_unhealthy_cluster(cluster_id, state)
    
    def _calculate_overall_status(self, check_results: List[HealthCheckResult]) -> HealthStatus:
        """Calculate overall health status from individual check results."""
        if not check_results:
            return HealthStatus.UNAVAILABLE
        
        # Count status occurrences
        status_counts = {status: 0 for status in HealthStatus}
        for result in check_results:
            status_counts[result.status] += 1
        
        # Determine overall status
        if status_counts[HealthStatus.CRITICAL] > 0:
            return HealthStatus.CRITICAL
        elif status_counts[HealthStatus.UNAVAILABLE] > 0:
            return HealthStatus.UNAVAILABLE
        elif status_counts[HealthStatus.WARNING] > 0:
            return HealthStatus.WARNING
        else:
            return HealthStatus.HEALTHY
    
    async def _handle_unhealthy_cluster(self, cluster_id: str, state: ClusterHealthState):
        """Handle an unhealthy cluster."""
        if not self.auto_recovery_enabled:
            return
        
        # Check if automatic restart is needed
        if self.auto_restart_enabled and state.needs_restart(
            self.max_consecutive_failures, 
            self.restart_cooldown_minutes
        ):
            await self._attempt_cluster_restart(cluster_id, state)
        
        # Create error for recovery system
        error = ClusterUnavailableError(
            message=f"Cluster {cluster_id} is unhealthy with {state.consecutive_failures} consecutive failures",
            cluster_id=cluster_id,
            context={
                "consecutive_failures": state.consecutive_failures,
                "last_healthy": state.last_healthy.isoformat() if state.last_healthy else None,
                "check_results": [
                    {
                        "type": result.check_type.value,
                        "status": result.status.value,
                        "message": result.message
                    }
                    for result in state.check_results
                ]
            }
        )
        
        # Let recovery manager handle the error
        await error_recovery_manager.handle_error(error)
    
    async def _attempt_cluster_restart(self, cluster_id: str, state: ClusterHealthState):
        """Attempt to restart an unhealthy cluster."""
        self.logger.warning(f"Attempting automatic restart of cluster {cluster_id}")
        
        try:
            from src.services.multi_cluster_manager import MultiClusterManager
            
            manager = MultiClusterManager()
            
            # Stop cluster
            await manager.stop_cluster(cluster_id)
            await asyncio.sleep(10)  # Wait for graceful shutdown
            
            # Start cluster
            await manager.start_cluster(cluster_id)
            
            # Update state
            state.restart_count += 1
            state.last_restart = datetime.utcnow()
            state.consecutive_failures = 0  # Reset failure count after restart
            
            self.logger.info(f"Successfully restarted cluster {cluster_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to restart cluster {cluster_id}: {str(e)}")
            
            # Create restart failure error
            error = ClusterHealthError(
                message=f"Failed to restart unhealthy cluster {cluster_id}: {str(e)}",
                cluster_id=cluster_id,
                cause=e
            )
            await error_recovery_manager.handle_error(error)
    
    # Health check handlers
    async def _check_kafka_broker(self, cluster_id: str) -> HealthCheckResult:
        """Check Kafka broker health."""
        try:
            from src.registry.cluster_registry import ClusterRegistry
            
            registry = ClusterRegistry()
            cluster = await registry.get_cluster(cluster_id)
            
            if not cluster:
                return HealthCheckResult(
                    check_type=HealthCheckType.KAFKA_BROKER,
                    status=HealthStatus.UNAVAILABLE,
                    message="Cluster not found in registry"
                )
            
            # Try to connect to Kafka broker
            import socket
            start_time = datetime.utcnow()
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                result = sock.connect_ex(('localhost', cluster.port_allocation.kafka_port))
                
            end_time = datetime.utcnow()
            response_time = (end_time - start_time).total_seconds() * 1000
            
            if result == 0:
                return HealthCheckResult(
                    check_type=HealthCheckType.KAFKA_BROKER,
                    status=HealthStatus.HEALTHY,
                    message="Kafka broker is responding",
                    response_time_ms=response_time
                )
            else:
                return HealthCheckResult(
                    check_type=HealthCheckType.KAFKA_BROKER,
                    status=HealthStatus.CRITICAL,
                    message=f"Cannot connect to Kafka broker on port {cluster.port_allocation.kafka_port}"
                )
                
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.KAFKA_BROKER,
                status=HealthStatus.CRITICAL,
                message=f"Kafka broker health check failed: {str(e)}"
            )
    
    async def _check_rest_proxy(self, cluster_id: str) -> HealthCheckResult:
        """Check REST Proxy health."""
        try:
            from src.registry.cluster_registry import ClusterRegistry
            import aiohttp
            
            registry = ClusterRegistry()
            cluster = await registry.get_cluster(cluster_id)
            
            if not cluster:
                return HealthCheckResult(
                    check_type=HealthCheckType.REST_PROXY,
                    status=HealthStatus.UNAVAILABLE,
                    message="Cluster not found in registry"
                )
            
            url = f"http://localhost:{cluster.port_allocation.rest_proxy_port}/topics"
            start_time = datetime.utcnow()
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    end_time = datetime.utcnow()
                    response_time = (end_time - start_time).total_seconds() * 1000
                    
                    if response.status == 200:
                        return HealthCheckResult(
                            check_type=HealthCheckType.REST_PROXY,
                            status=HealthStatus.HEALTHY,
                            message="REST Proxy is responding",
                            response_time_ms=response_time
                        )
                    else:
                        return HealthCheckResult(
                            check_type=HealthCheckType.REST_PROXY,
                            status=HealthStatus.WARNING,
                            message=f"REST Proxy returned status {response.status}"
                        )
                        
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.REST_PROXY,
                status=HealthStatus.CRITICAL,
                message=f"REST Proxy health check failed: {str(e)}"
            )
    
    async def _check_ui(self, cluster_id: str) -> HealthCheckResult:
        """Check UI health."""
        try:
            from src.registry.cluster_registry import ClusterRegistry
            import aiohttp
            
            registry = ClusterRegistry()
            cluster = await registry.get_cluster(cluster_id)
            
            if not cluster:
                return HealthCheckResult(
                    check_type=HealthCheckType.UI,
                    status=HealthStatus.UNAVAILABLE,
                    message="Cluster not found in registry"
                )
            
            url = f"http://localhost:{cluster.port_allocation.ui_port}"
            start_time = datetime.utcnow()
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    end_time = datetime.utcnow()
                    response_time = (end_time - start_time).total_seconds() * 1000
                    
                    if response.status == 200:
                        return HealthCheckResult(
                            check_type=HealthCheckType.UI,
                            status=HealthStatus.HEALTHY,
                            message="UI is responding",
                            response_time_ms=response_time
                        )
                    else:
                        return HealthCheckResult(
                            check_type=HealthCheckType.UI,
                            status=HealthStatus.WARNING,
                            message=f"UI returned status {response.status}"
                        )
                        
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.UI,
                status=HealthStatus.CRITICAL,
                message=f"UI health check failed: {str(e)}"
            )
    
    async def _check_jmx(self, cluster_id: str) -> HealthCheckResult:
        """Check JMX health."""
        try:
            from src.registry.cluster_registry import ClusterRegistry
            
            registry = ClusterRegistry()
            cluster = await registry.get_cluster(cluster_id)
            
            if not cluster or not cluster.port_allocation.jmx_port:
                return HealthCheckResult(
                    check_type=HealthCheckType.JMX,
                    status=HealthStatus.HEALTHY,
                    message="JMX not configured for this cluster"
                )
            
            # Try to connect to JMX port
            import socket
            start_time = datetime.utcnow()
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                result = sock.connect_ex(('localhost', cluster.port_allocation.jmx_port))
                
            end_time = datetime.utcnow()
            response_time = (end_time - start_time).total_seconds() * 1000
            
            if result == 0:
                return HealthCheckResult(
                    check_type=HealthCheckType.JMX,
                    status=HealthStatus.HEALTHY,
                    message="JMX is responding",
                    response_time_ms=response_time
                )
            else:
                return HealthCheckResult(
                    check_type=HealthCheckType.JMX,
                    status=HealthStatus.WARNING,
                    message=f"Cannot connect to JMX on port {cluster.port_allocation.jmx_port}"
                )
                
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.JMX,
                status=HealthStatus.WARNING,
                message=f"JMX health check failed: {str(e)}"
            )
    
    async def _check_disk_space(self, cluster_id: str) -> HealthCheckResult:
        """Check disk space health."""
        try:
            import shutil
            
            # Check disk space for cluster data directory
            data_dir = f"./data/clusters/{cluster_id}"
            
            try:
                total, used, free = shutil.disk_usage(data_dir)
                usage_percent = (used / total) * 100
                
                if usage_percent > 90:
                    status = HealthStatus.CRITICAL
                    message = f"Disk usage critical: {usage_percent:.1f}%"
                elif usage_percent > 80:
                    status = HealthStatus.WARNING
                    message = f"Disk usage high: {usage_percent:.1f}%"
                else:
                    status = HealthStatus.HEALTHY
                    message = f"Disk usage normal: {usage_percent:.1f}%"
                
                return HealthCheckResult(
                    check_type=HealthCheckType.DISK_SPACE,
                    status=status,
                    message=message,
                    details={
                        "total_bytes": total,
                        "used_bytes": used,
                        "free_bytes": free,
                        "usage_percent": usage_percent
                    }
                )
                
            except FileNotFoundError:
                return HealthCheckResult(
                    check_type=HealthCheckType.DISK_SPACE,
                    status=HealthStatus.WARNING,
                    message="Cluster data directory not found"
                )
                
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.DISK_SPACE,
                status=HealthStatus.WARNING,
                message=f"Disk space check failed: {str(e)}"
            )
    
    async def _check_memory(self, cluster_id: str) -> HealthCheckResult:
        """Check memory usage health."""
        try:
            import psutil
            
            memory = psutil.virtual_memory()
            usage_percent = memory.percent
            
            if usage_percent > 95:
                status = HealthStatus.CRITICAL
                message = f"Memory usage critical: {usage_percent:.1f}%"
            elif usage_percent > 85:
                status = HealthStatus.WARNING
                message = f"Memory usage high: {usage_percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                message = f"Memory usage normal: {usage_percent:.1f}%"
            
            return HealthCheckResult(
                check_type=HealthCheckType.MEMORY,
                status=status,
                message=message,
                details={
                    "total_bytes": memory.total,
                    "available_bytes": memory.available,
                    "used_bytes": memory.used,
                    "usage_percent": usage_percent
                }
            )
            
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.MEMORY,
                status=HealthStatus.WARNING,
                message=f"Memory check failed: {str(e)}"
            )
    
    async def _check_cpu(self, cluster_id: str) -> HealthCheckResult:
        """Check CPU usage health."""
        try:
            import psutil
            
            # Get CPU usage over 1 second interval
            cpu_percent = psutil.cpu_percent(interval=1)
            
            if cpu_percent > 95:
                status = HealthStatus.CRITICAL
                message = f"CPU usage critical: {cpu_percent:.1f}%"
            elif cpu_percent > 80:
                status = HealthStatus.WARNING
                message = f"CPU usage high: {cpu_percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                message = f"CPU usage normal: {cpu_percent:.1f}%"
            
            return HealthCheckResult(
                check_type=HealthCheckType.CPU,
                status=status,
                message=message,
                details={
                    "usage_percent": cpu_percent,
                    "cpu_count": psutil.cpu_count()
                }
            )
            
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.CPU,
                status=HealthStatus.WARNING,
                message=f"CPU check failed: {str(e)}"
            )
    
    async def _check_network(self, cluster_id: str) -> HealthCheckResult:
        """Check network connectivity health."""
        try:
            # Simple network connectivity check
            import socket
            
            # Try to resolve localhost
            socket.gethostbyname('localhost')
            
            return HealthCheckResult(
                check_type=HealthCheckType.NETWORK,
                status=HealthStatus.HEALTHY,
                message="Network connectivity normal"
            )
            
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.NETWORK,
                status=HealthStatus.CRITICAL,
                message=f"Network check failed: {str(e)}"
            )
    
    def get_cluster_health(self, cluster_id: str) -> Optional[ClusterHealthState]:
        """Get current health state for a cluster."""
        return self.cluster_states.get(cluster_id)
    
    def get_all_cluster_health(self) -> Dict[str, ClusterHealthState]:
        """Get health states for all monitored clusters."""
        return self.cluster_states.copy()
    
    def get_unhealthy_clusters(self) -> List[str]:
        """Get list of unhealthy cluster IDs."""
        return [
            cluster_id for cluster_id, state in self.cluster_states.items()
            if state.overall_status in [HealthStatus.CRITICAL, HealthStatus.UNAVAILABLE]
        ]


# Global cluster health monitor instance
cluster_health_monitor = ClusterHealthMonitor()