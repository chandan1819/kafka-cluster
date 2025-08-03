"""
Metrics collection and monitoring utilities for Local Kafka Manager.

This module provides performance metrics collection, aggregation, and reporting
for service monitoring and alerting.
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from collections import defaultdict, deque
from threading import Lock
from enum import Enum

from .logging import get_logger, log_metrics


logger = get_logger(__name__)


class MetricType(str, Enum):
    """Types of metrics that can be collected."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class MetricValue:
    """Individual metric value with timestamp."""
    value: Union[int, float]
    timestamp: datetime
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricSummary:
    """Summary statistics for a metric."""
    name: str
    metric_type: MetricType
    count: int
    sum: float
    min: float
    max: float
    avg: float
    p50: float
    p95: float
    p99: float
    last_value: float
    last_updated: datetime
    tags: Dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """Thread-safe metrics collector with aggregation capabilities."""
    
    def __init__(self, max_values_per_metric: int = 1000):
        self.max_values_per_metric = max_values_per_metric
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_values_per_metric))
        self._metric_types: Dict[str, MetricType] = {}
        self._lock = Lock()
        self._start_time = datetime.utcnow()
    
    def counter(self, name: str, value: int = 1, tags: Dict[str, str] = None):
        """
        Record a counter metric (monotonically increasing).
        
        Args:
            name: Metric name
            value: Counter increment value
            tags: Optional tags for filtering
        """
        self._record_metric(name, MetricType.COUNTER, value, tags)
    
    def gauge(self, name: str, value: Union[int, float], tags: Dict[str, str] = None):
        """
        Record a gauge metric (point-in-time value).
        
        Args:
            name: Metric name
            value: Gauge value
            tags: Optional tags for filtering
        """
        self._record_metric(name, MetricType.GAUGE, value, tags)
    
    def histogram(self, name: str, value: Union[int, float], tags: Dict[str, str] = None):
        """
        Record a histogram metric (distribution of values).
        
        Args:
            name: Metric name
            value: Histogram value
            tags: Optional tags for filtering
        """
        self._record_metric(name, MetricType.HISTOGRAM, value, tags)
    
    def timer(self, name: str, duration_ms: float, tags: Dict[str, str] = None):
        """
        Record a timer metric (duration measurement).
        
        Args:
            name: Metric name
            duration_ms: Duration in milliseconds
            tags: Optional tags for filtering
        """
        self._record_metric(name, MetricType.TIMER, duration_ms, tags)
    
    def _record_metric(self, name: str, metric_type: MetricType, value: Union[int, float], tags: Dict[str, str] = None):
        """Record a metric value with thread safety."""
        with self._lock:
            # Store metric type
            if name not in self._metric_types:
                self._metric_types[name] = metric_type
            elif self._metric_types[name] != metric_type:
                logger.warning(f"Metric type mismatch for {name}: expected {self._metric_types[name]}, got {metric_type}")
            
            # Create metric value
            metric_value = MetricValue(
                value=value,
                timestamp=datetime.utcnow(),
                tags=tags or {}
            )
            
            # Store in deque (automatically handles max size)
            self._metrics[name].append(metric_value)
            
            # Log metric for external monitoring
            log_metrics(name, value, tags=tags, metric_type=metric_type.value)
    
    def get_metric_summary(self, name: str, since: datetime = None) -> Optional[MetricSummary]:
        """
        Get summary statistics for a metric.
        
        Args:
            name: Metric name
            since: Only include values since this timestamp
        
        Returns:
            Metric summary or None if metric doesn't exist
        """
        with self._lock:
            if name not in self._metrics:
                return None
            
            values = list(self._metrics[name])
            if not values:
                return None
            
            # Filter by timestamp if specified
            if since:
                values = [v for v in values if v.timestamp >= since]
                if not values:
                    return None
            
            # Extract numeric values for calculations
            numeric_values = [v.value for v in values]
            numeric_values.sort()
            
            # Calculate statistics
            count = len(numeric_values)
            sum_val = sum(numeric_values)
            min_val = min(numeric_values)
            max_val = max(numeric_values)
            avg_val = sum_val / count
            
            # Calculate percentiles
            p50_idx = int(count * 0.5)
            p95_idx = int(count * 0.95)
            p99_idx = int(count * 0.99)
            
            p50 = numeric_values[min(p50_idx, count - 1)]
            p95 = numeric_values[min(p95_idx, count - 1)]
            p99 = numeric_values[min(p99_idx, count - 1)]
            
            return MetricSummary(
                name=name,
                metric_type=self._metric_types[name],
                count=count,
                sum=sum_val,
                min=min_val,
                max=max_val,
                avg=avg_val,
                p50=p50,
                p95=p95,
                p99=p99,
                last_value=values[-1].value,
                last_updated=values[-1].timestamp,
                tags=values[-1].tags
            )
    
    def get_all_metrics(self, since: datetime = None) -> Dict[str, MetricSummary]:
        """
        Get summary statistics for all metrics.
        
        Args:
            since: Only include values since this timestamp
        
        Returns:
            Dictionary of metric summaries
        """
        with self._lock:
            summaries = {}
            for name in self._metrics.keys():
                summary = self.get_metric_summary(name, since)
                if summary:
                    summaries[name] = summary
            return summaries
    
    def get_metric_names(self) -> List[str]:
        """Get list of all metric names."""
        with self._lock:
            return list(self._metrics.keys())
    
    def clear_metrics(self, name: str = None):
        """
        Clear metrics data.
        
        Args:
            name: Specific metric name to clear, or None to clear all
        """
        with self._lock:
            if name:
                if name in self._metrics:
                    self._metrics[name].clear()
                    logger.info(f"Cleared metrics for: {name}")
            else:
                self._metrics.clear()
                self._metric_types.clear()
                logger.info("Cleared all metrics")
    
    def get_uptime_seconds(self) -> float:
        """Get application uptime in seconds."""
        return (datetime.utcnow() - self._start_time).total_seconds()


class TimerContext:
    """Context manager for timing operations."""
    
    def __init__(self, collector: MetricsCollector, metric_name: str, tags: Dict[str, str] = None):
        self.collector = collector
        self.metric_name = metric_name
        self.tags = tags
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration_ms = (time.time() - self.start_time) * 1000
            self.collector.timer(self.metric_name, duration_ms, self.tags)


# Global metrics collector instance
metrics_collector = MetricsCollector()


def counter(name: str, value: int = 1, tags: Dict[str, str] = None):
    """Record a counter metric."""
    metrics_collector.counter(name, value, tags)


def gauge(name: str, value: Union[int, float], tags: Dict[str, str] = None):
    """Record a gauge metric."""
    metrics_collector.gauge(name, value, tags)


def histogram(name: str, value: Union[int, float], tags: Dict[str, str] = None):
    """Record a histogram metric."""
    metrics_collector.histogram(name, value, tags)


def timer(name: str, duration_ms: float, tags: Dict[str, str] = None):
    """Record a timer metric."""
    metrics_collector.timer(name, duration_ms, tags)


def time_operation(name: str, tags: Dict[str, str] = None):
    """Context manager for timing operations."""
    return TimerContext(metrics_collector, name, tags)


def get_metric_summary(name: str, since: datetime = None) -> Optional[MetricSummary]:
    """Get summary statistics for a metric."""
    return metrics_collector.get_metric_summary(name, since)


def get_all_metrics(since: datetime = None) -> Dict[str, MetricSummary]:
    """Get summary statistics for all metrics."""
    return metrics_collector.get_all_metrics(since)


def clear_metrics(name: str = None):
    """Clear metrics data."""
    metrics_collector.clear_metrics(name)


def get_system_metrics() -> Dict[str, Any]:
    """
    Get system-level metrics for monitoring.
    
    Returns:
        Dictionary containing system metrics
    """
    import psutil
    import os
    
    try:
        # Process metrics
        process = psutil.Process(os.getpid())
        
        # Memory metrics
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        
        # CPU metrics
        cpu_percent = process.cpu_percent()
        
        # System metrics
        system_memory = psutil.virtual_memory()
        system_cpu = psutil.cpu_percent(interval=1)
        
        # Disk metrics for current directory
        disk_usage = psutil.disk_usage('.')
        
        system_metrics = {
            'process': {
                'pid': os.getpid(),
                'memory_rss_bytes': memory_info.rss,
                'memory_vms_bytes': memory_info.vms,
                'memory_percent': memory_percent,
                'cpu_percent': cpu_percent,
                'num_threads': process.num_threads(),
                'num_fds': process.num_fds() if hasattr(process, 'num_fds') else None,
                'create_time': datetime.fromtimestamp(process.create_time()).isoformat()
            },
            'system': {
                'memory_total_bytes': system_memory.total,
                'memory_available_bytes': system_memory.available,
                'memory_used_bytes': system_memory.used,
                'memory_percent': system_memory.percent,
                'cpu_percent': system_cpu,
                'cpu_count': psutil.cpu_count(),
                'disk_total_bytes': disk_usage.total,
                'disk_used_bytes': disk_usage.used,
                'disk_free_bytes': disk_usage.free,
                'disk_percent': (disk_usage.used / disk_usage.total) * 100
            },
            'application': {
                'uptime_seconds': metrics_collector.get_uptime_seconds(),
                'start_time': metrics_collector._start_time.isoformat()
            }
        }
        
        # Record system metrics
        gauge('system.memory.percent', system_memory.percent)
        gauge('system.cpu.percent', system_cpu)
        gauge('system.disk.percent', (disk_usage.used / disk_usage.total) * 100)
        gauge('process.memory.percent', memory_percent)
        gauge('process.cpu.percent', cpu_percent)
        gauge('application.uptime.seconds', metrics_collector.get_uptime_seconds())
        
        return system_metrics
        
    except Exception as e:
        logger.error(f"Failed to collect system metrics: {e}")
        return {
            'error': str(e),
            'application': {
                'uptime_seconds': metrics_collector.get_uptime_seconds(),
                'start_time': metrics_collector._start_time.isoformat()
            }
        }


def record_api_metrics(
    method: str,
    path: str,
    status_code: int,
    duration_ms: float,
    request_size_bytes: int = None,
    response_size_bytes: int = None
):
    """
    Record API request metrics.
    
    Args:
        method: HTTP method
        path: Request path
        status_code: HTTP status code
        duration_ms: Request duration in milliseconds
        request_size_bytes: Request body size in bytes
        response_size_bytes: Response body size in bytes
    """
    tags = {
        'method': method,
        'path': path,
        'status_code': str(status_code),
        'status_class': f"{status_code // 100}xx"
    }
    
    # Record metrics
    counter('api.requests.total', 1, tags)
    timer('api.requests.duration_ms', duration_ms, tags)
    
    if request_size_bytes is not None:
        histogram('api.requests.size_bytes', request_size_bytes, tags)
    
    if response_size_bytes is not None:
        histogram('api.responses.size_bytes', response_size_bytes, tags)
    
    # Record error metrics
    if status_code >= 400:
        counter('api.requests.errors', 1, tags)
        if status_code >= 500:
            counter('api.requests.server_errors', 1, tags)
        else:
            counter('api.requests.client_errors', 1, tags)


def record_service_metrics(
    service: str,
    operation: str,
    success: bool,
    duration_ms: float,
    error_type: str = None
):
    """
    Record service operation metrics.
    
    Args:
        service: Service name
        operation: Operation name
        success: Whether operation succeeded
        duration_ms: Operation duration in milliseconds
        error_type: Type of error if operation failed
    """
    tags = {
        'service': service,
        'operation': operation,
        'success': str(success).lower()
    }
    
    if error_type:
        tags['error_type'] = error_type
    
    # Record metrics
    counter('service.operations.total', 1, tags)
    timer('service.operations.duration_ms', duration_ms, tags)
    
    if success:
        counter('service.operations.success', 1, tags)
    else:
        counter('service.operations.errors', 1, tags)


def record_health_metrics(
    service: str,
    status: str,
    response_time_ms: float,
    check_type: str = 'periodic'
):
    """
    Record health check metrics.
    
    Args:
        service: Service name
        status: Health status (running, stopped, error, etc.)
        response_time_ms: Health check response time in milliseconds
        check_type: Type of health check (periodic, on-demand, etc.)
    """
    tags = {
        'service': service,
        'status': status,
        'check_type': check_type
    }
    
    # Record metrics
    counter('health.checks.total', 1, tags)
    timer('health.checks.duration_ms', response_time_ms, tags)
    gauge(f'health.status.{service}', 1 if status == 'running' else 0, tags)
    
    if status != 'running':
        counter('health.checks.failures', 1, tags)


def get_performance_report(since_minutes: int = 60) -> Dict[str, Any]:
    """
    Generate a performance report for the specified time period.
    
    Args:
        since_minutes: Number of minutes to look back
    
    Returns:
        Performance report with key metrics and insights
    """
    since = datetime.utcnow() - timedelta(minutes=since_minutes)
    all_metrics = get_all_metrics(since)
    
    report = {
        'period': {
            'since': since.isoformat(),
            'duration_minutes': since_minutes
        },
        'api_performance': {},
        'service_performance': {},
        'health_status': {},
        'system_metrics': get_system_metrics(),
        'summary': {
            'total_metrics': len(all_metrics),
            'generated_at': datetime.utcnow().isoformat()
        }
    }
    
    # Analyze API performance
    api_metrics = {k: v for k, v in all_metrics.items() if k.startswith('api.')}
    if api_metrics:
        total_requests = sum(m.sum for m in api_metrics.values() if 'requests.total' in m.name)
        avg_response_time = sum(m.avg for m in api_metrics.values() if 'duration_ms' in m.name) / max(1, len([m for m in api_metrics.values() if 'duration_ms' in m.name]))
        error_rate = sum(m.sum for m in api_metrics.values() if 'errors' in m.name) / max(1, total_requests) * 100
        
        report['api_performance'] = {
            'total_requests': total_requests,
            'average_response_time_ms': round(avg_response_time, 2),
            'error_rate_percent': round(error_rate, 2),
            'metrics_count': len(api_metrics)
        }
    
    # Analyze service performance
    service_metrics = {k: v for k, v in all_metrics.items() if k.startswith('service.')}
    if service_metrics:
        total_operations = sum(m.sum for m in service_metrics.values() if 'operations.total' in m.name)
        success_rate = sum(m.sum for m in service_metrics.values() if 'success' in m.name) / max(1, total_operations) * 100
        
        report['service_performance'] = {
            'total_operations': total_operations,
            'success_rate_percent': round(success_rate, 2),
            'metrics_count': len(service_metrics)
        }
    
    # Analyze health status
    health_metrics = {k: v for k, v in all_metrics.items() if k.startswith('health.')}
    if health_metrics:
        total_checks = sum(m.sum for m in health_metrics.values() if 'checks.total' in m.name)
        failure_rate = sum(m.sum for m in health_metrics.values() if 'failures' in m.name) / max(1, total_checks) * 100
        
        report['health_status'] = {
            'total_checks': total_checks,
            'failure_rate_percent': round(failure_rate, 2),
            'metrics_count': len(health_metrics)
        }
    
    return report