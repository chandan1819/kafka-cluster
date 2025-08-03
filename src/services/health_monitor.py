"""
Health monitoring service for the Local Kafka Manager.

This module provides comprehensive health monitoring capabilities including
periodic status checks, service dependency validation, and metrics collection.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum

import httpx
from pydantic import BaseModel

from ..models.base import ServiceStatus
from ..exceptions import (
    ServiceUnavailableError,
    DockerNotAvailableError,
    KafkaNotAvailableError,
    KafkaRestProxyNotAvailableError,
    ServiceDiscoveryError
)
from ..utils.retry import retry_async, RetryConfig, QUICK_RETRY


logger = logging.getLogger(__name__)


class HealthStatus(str, Enum):
    """Health status enumeration."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ServiceHealth:
    """Health information for a service."""
    name: str
    status: HealthStatus
    last_check: datetime
    response_time_ms: Optional[float] = None
    error_message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "status": self.status.value,
            "last_check": self.last_check.isoformat(),
            "response_time_ms": self.response_time_ms,
            "error_message": self.error_message,
            "details": self.details
        }


@dataclass
class SystemHealth:
    """Overall system health information."""
    status: HealthStatus
    timestamp: datetime
    services: Dict[str, ServiceHealth]
    dependencies_satisfied: bool
    uptime_seconds: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "status": self.status.value,
            "timestamp": self.timestamp.isoformat(),
            "services": {name: service.to_dict() for name, service in self.services.items()},
            "dependencies_satisfied": self.dependencies_satisfied,
            "uptime_seconds": self.uptime_seconds
        }


class HealthMetrics(BaseModel):
    """Health metrics for monitoring."""
    total_checks: int = 0
    successful_checks: int = 0
    failed_checks: int = 0
    average_response_time_ms: float = 0.0
    last_failure_time: Optional[datetime] = None
    consecutive_failures: int = 0
    
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_checks == 0:
            return 0.0
        return (self.successful_checks / self.total_checks) * 100.0


class HealthMonitor:
    """Comprehensive health monitoring service."""
    
    def __init__(self):
        """Initialize the health monitor."""
        from ..config import settings
        
        self.start_time = time.time()
        self.monitoring_active = False
        self.monitoring_task: Optional[asyncio.Task] = None
        self.check_interval = settings.monitoring.health_check_interval
        self.timeout = settings.monitoring.health_check_timeout
        
        # Service health cache
        self._service_health: Dict[str, ServiceHealth] = {}
        self._metrics: Dict[str, HealthMetrics] = {}
        
        # HTTP client for health checks
        self._http_client: Optional[httpx.AsyncClient] = None
        
        # Service definitions with health check configurations
        self._init_service_definitions()
    
    def _init_service_definitions(self):
        """Initialize service definitions with configuration values."""
        from ..config import settings
        
        self.service_definitions = {
            "docker": {
                "name": "Docker",
                "check_method": self._check_docker_health,
                "critical": True,
                "dependencies": []
            },
            "kafka": {
                "name": "Kafka Broker",
                "check_method": self._check_kafka_health,
                "critical": True,
                "dependencies": ["docker"]
            },
            "kafka-rest-proxy": {
                "name": "Kafka REST Proxy",
                "check_method": self._check_rest_proxy_health,
                "critical": False,
                "dependencies": ["kafka"],
                "url": settings.get_rest_proxy_url(),
                "health_path": "/v3"
            },
            "kafka-ui": {
                "name": "Kafka UI",
                "check_method": self._check_kafka_ui_health,
                "critical": False,
                "dependencies": ["kafka"],
                "url": settings.get_kafka_ui_url(),
                "health_path": settings.kafka_ui.health_check_path
            }
        }
    
    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                follow_redirects=True
            )
        return self._http_client
    
    async def start_monitoring(self, interval: int = 30) -> None:
        """Start periodic health monitoring.
        
        Args:
            interval: Check interval in seconds
        """
        if self.monitoring_active:
            logger.warning("Health monitoring is already active")
            return
        
        self.check_interval = interval
        self.monitoring_active = True
        
        logger.info(f"Starting health monitoring with {interval}s interval")
        
        # Start background monitoring task
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
    
    async def stop_monitoring(self) -> None:
        """Stop periodic health monitoring."""
        if not self.monitoring_active:
            return
        
        logger.info("Stopping health monitoring")
        self.monitoring_active = False
        
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        # Close HTTP client
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
    
    async def get_system_health(self, force_check: bool = False) -> SystemHealth:
        """Get overall system health status.
        
        Args:
            force_check: Force immediate health checks instead of using cache
            
        Returns:
            SystemHealth object with current status
        """
        if force_check or not self._service_health:
            await self._check_all_services()
        
        # Determine overall system health
        overall_status = self._calculate_overall_health()
        dependencies_satisfied = self._check_dependencies()
        uptime = time.time() - self.start_time
        
        return SystemHealth(
            status=overall_status,
            timestamp=datetime.utcnow(),
            services=self._service_health.copy(),
            dependencies_satisfied=dependencies_satisfied,
            uptime_seconds=uptime
        )
    
    async def get_service_health(self, service_name: str, force_check: bool = False) -> Optional[ServiceHealth]:
        """Get health status for a specific service.
        
        Args:
            service_name: Name of the service to check
            force_check: Force immediate health check
            
        Returns:
            ServiceHealth object or None if service not found
        """
        if service_name not in self.service_definitions:
            return None
        
        if force_check or service_name not in self._service_health:
            await self._check_service_health(service_name)
        
        return self._service_health.get(service_name)
    
    def get_metrics(self, service_name: Optional[str] = None) -> Dict[str, HealthMetrics]:
        """Get health metrics for services.
        
        Args:
            service_name: Specific service name, or None for all services
            
        Returns:
            Dictionary of service metrics
        """
        if service_name:
            return {service_name: self._metrics.get(service_name, HealthMetrics())}
        return self._metrics.copy()
    
    async def _monitoring_loop(self) -> None:
        """Background monitoring loop."""
        while self.monitoring_active:
            try:
                await self._check_all_services()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(min(self.check_interval, 10))  # Shorter retry interval on error
    
    async def _check_all_services(self) -> None:
        """Check health of all configured services."""
        logger.debug("Performing health checks for all services")
        
        # Check services in dependency order
        for service_name in self._get_dependency_order():
            await self._check_service_health(service_name)
    
    async def _check_service_health(self, service_name: str) -> None:
        """Check health of a specific service.
        
        Args:
            service_name: Name of the service to check
        """
        if service_name not in self.service_definitions:
            logger.warning(f"Unknown service: {service_name}")
            return
        
        service_def = self.service_definitions[service_name]
        start_time = time.time()
        
        try:
            # Initialize metrics if not exists
            if service_name not in self._metrics:
                self._metrics[service_name] = HealthMetrics()
            
            metrics = self._metrics[service_name]
            metrics.total_checks += 1
            
            # Perform health check
            check_method = service_def["check_method"]
            is_healthy, details = await check_method()
            
            response_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            if is_healthy:
                status = HealthStatus.HEALTHY
                metrics.successful_checks += 1
                metrics.consecutive_failures = 0
                error_message = None
            else:
                status = HealthStatus.UNHEALTHY
                metrics.failed_checks += 1
                metrics.consecutive_failures += 1
                metrics.last_failure_time = datetime.utcnow()
                error_message = details.get("error", "Health check failed")
            
            # Update average response time
            if metrics.total_checks > 0:
                metrics.average_response_time_ms = (
                    (metrics.average_response_time_ms * (metrics.total_checks - 1) + response_time) 
                    / metrics.total_checks
                )
            
            # Store health status
            self._service_health[service_name] = ServiceHealth(
                name=service_def["name"],
                status=status,
                last_check=datetime.utcnow(),
                response_time_ms=response_time,
                error_message=error_message,
                details=details
            )
            
            logger.debug(f"Health check for {service_name}: {status.value} ({response_time:.1f}ms)")
            
        except Exception as e:
            logger.error(f"Health check failed for {service_name}: {e}")
            
            # Update metrics
            metrics = self._metrics.get(service_name, HealthMetrics())
            metrics.total_checks += 1
            metrics.failed_checks += 1
            metrics.consecutive_failures += 1
            metrics.last_failure_time = datetime.utcnow()
            self._metrics[service_name] = metrics
            
            # Store error status
            self._service_health[service_name] = ServiceHealth(
                name=service_def["name"],
                status=HealthStatus.UNKNOWN,
                last_check=datetime.utcnow(),
                error_message=str(e),
                details={"exception": type(e).__name__}
            )
    
    @retry_async(QUICK_RETRY)
    async def _check_docker_health(self) -> tuple[bool, Dict[str, Any]]:
        """Check Docker daemon health."""
        try:
            import docker
            client = docker.from_env()
            client.ping()
            
            # Get Docker info
            info = client.info()
            return True, {
                "version": info.get("ServerVersion", "unknown"),
                "containers_running": info.get("ContainersRunning", 0),
                "containers_total": info.get("Containers", 0)
            }
        except Exception as e:
            return False, {"error": str(e)}
    
    @retry_async(QUICK_RETRY)
    async def _check_kafka_health(self) -> tuple[bool, Dict[str, Any]]:
        """Check Kafka broker health."""
        try:
            # Try to connect to Kafka using admin client
            from kafka import KafkaAdminClient
            from kafka.errors import KafkaError
            
            from ..config import settings
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=[settings.get_kafka_bootstrap_servers()],
                request_timeout_ms=5000
            )
            
            # List topics to verify connectivity
            metadata = admin_client.list_topics()
            
            return True, {
                "broker_count": len(metadata),
                "topics_count": len(admin_client.list_topics())
            }
        except Exception as e:
            return False, {"error": str(e)}
    
    @retry_async(QUICK_RETRY)
    async def _check_rest_proxy_health(self) -> tuple[bool, Dict[str, Any]]:
        """Check Kafka REST Proxy health with comprehensive functionality verification."""
        try:
            client = self.http_client
            
            from ..config import settings
            
            # Check basic connectivity
            response = await client.get(f"{settings.get_rest_proxy_url()}/v3")
            
            if response.status_code == 200:
                details = {
                    "status_code": response.status_code,
                    "response_time_ms": response.elapsed.total_seconds() * 1000,
                    "api_version": "v3"
                }
                
                # Test topics endpoint to verify Kafka connectivity
                try:
                    topics_response = await client.get(f"{settings.get_rest_proxy_url()}/topics")
                    if topics_response.status_code == 200:
                        topics_data = topics_response.json()
                        details["topics_accessible"] = True
                        details["topics_count"] = len(topics_data) if isinstance(topics_data, list) else 0
                        details["kafka_connectivity"] = True
                    else:
                        details["topics_accessible"] = False
                        details["kafka_connectivity"] = False
                except Exception as e:
                    details["topics_accessible"] = False
                    details["kafka_connectivity"] = False
                    details["kafka_error"] = str(e)
                
                # Test brokers endpoint
                try:
                    brokers_response = await client.get(f"{settings.get_rest_proxy_url()}/brokers")
                    if brokers_response.status_code == 200:
                        brokers_data = brokers_response.json()
                        details["brokers_accessible"] = True
                        details["brokers_count"] = len(brokers_data.get("brokers", [])) if isinstance(brokers_data, dict) else 0
                    else:
                        details["brokers_accessible"] = False
                except Exception:
                    details["brokers_accessible"] = False
                
                # Overall health is good if basic endpoint works
                return True, details
            else:
                return False, {
                    "error": f"HTTP {response.status_code}",
                    "status_code": response.status_code,
                    "kafka_connectivity": False
                }
        except Exception as e:
            return False, {
                "error": str(e),
                "kafka_connectivity": False
            }
    
    @retry_async(QUICK_RETRY)
    async def _check_kafka_ui_health(self) -> tuple[bool, Dict[str, Any]]:
        """Check Kafka UI health with comprehensive accessibility verification."""
        try:
            client = self.http_client
            
            from ..config import settings
            
            # First try the main UI endpoint
            response = await client.get(settings.get_kafka_ui_url())
            
            if response.status_code == 200:
                # Check if the response contains expected UI content
                content = response.text.lower()
                ui_indicators = ["kafka", "topics", "brokers", "consumers"]
                ui_accessible = any(indicator in content for indicator in ui_indicators)
                
                details = {
                    "status_code": response.status_code,
                    "response_time_ms": response.elapsed.total_seconds() * 1000,
                    "ui_accessible": ui_accessible,
                    "content_length": len(response.text)
                }
                
                # Try to get cluster info from UI API if available
                try:
                    api_response = await client.get(f"{settings.get_kafka_ui_url()}/api/clusters")
                    if api_response.status_code == 200:
                        cluster_data = api_response.json()
                        details["clusters_detected"] = len(cluster_data) if isinstance(cluster_data, list) else 1
                        details["api_accessible"] = True
                    else:
                        details["api_accessible"] = False
                except Exception:
                    details["api_accessible"] = False
                
                return ui_accessible, details
            else:
                return False, {
                    "error": f"HTTP {response.status_code}",
                    "status_code": response.status_code,
                    "ui_accessible": False
                }
        except Exception as e:
            return False, {
                "error": str(e),
                "ui_accessible": False
            }
    
    def _calculate_overall_health(self) -> HealthStatus:
        """Calculate overall system health based on service health."""
        if not self._service_health:
            return HealthStatus.UNKNOWN
        
        critical_services = [
            name for name, definition in self.service_definitions.items()
            if definition.get("critical", False)
        ]
        
        # Check critical services
        critical_unhealthy = []
        critical_degraded = []
        
        for service_name in critical_services:
            if service_name in self._service_health:
                health = self._service_health[service_name]
                if health.status == HealthStatus.UNHEALTHY:
                    critical_unhealthy.append(service_name)
                elif health.status == HealthStatus.DEGRADED:
                    critical_degraded.append(service_name)
        
        # Determine overall status
        if critical_unhealthy:
            return HealthStatus.UNHEALTHY
        elif critical_degraded:
            return HealthStatus.DEGRADED
        
        # Check non-critical services
        non_critical_issues = []
        for service_name, health in self._service_health.items():
            if service_name not in critical_services:
                if health.status in [HealthStatus.UNHEALTHY, HealthStatus.DEGRADED]:
                    non_critical_issues.append(service_name)
        
        if non_critical_issues:
            return HealthStatus.DEGRADED
        
        return HealthStatus.HEALTHY
    
    def _check_dependencies(self) -> bool:
        """Check if all service dependencies are satisfied."""
        for service_name, service_def in self.service_definitions.items():
            dependencies = service_def.get("dependencies", [])
            
            for dep_name in dependencies:
                if dep_name not in self._service_health:
                    return False
                
                dep_health = self._service_health[dep_name]
                if dep_health.status == HealthStatus.UNHEALTHY:
                    return False
        
        return True
    
    def _get_dependency_order(self) -> List[str]:
        """Get services in dependency order for checking."""
        ordered = []
        remaining = set(self.service_definitions.keys())
        
        while remaining:
            # Find services with no unresolved dependencies
            ready = []
            for service_name in remaining:
                dependencies = self.service_definitions[service_name].get("dependencies", [])
                if all(dep in ordered for dep in dependencies):
                    ready.append(service_name)
            
            if not ready:
                # Circular dependency or missing dependency
                logger.warning(f"Circular or missing dependencies detected: {remaining}")
                ordered.extend(remaining)
                break
            
            # Add ready services to ordered list
            for service_name in ready:
                ordered.append(service_name)
                remaining.remove(service_name)
        
        return ordered


# Global health monitor instance
health_monitor = HealthMonitor()