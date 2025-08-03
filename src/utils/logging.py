"""
Structured logging utilities for Local Kafka Manager.

This module provides structured logging capabilities with request tracking,
performance metrics, and contextual information for debugging and monitoring.
"""

import json
import logging
import time
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, Union
from contextvars import ContextVar
from functools import wraps
from enum import Enum

from ..config import settings


class LogLevel(str, Enum):
    """Log levels for structured logging."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogContext:
    """Context manager for structured logging with request tracking."""
    
    def __init__(self, operation: str, **kwargs):
        self.operation = operation
        self.context = kwargs
        self.start_time = None
        self.request_id = kwargs.get('request_id', str(uuid.uuid4())[:8])
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000
        
        if exc_type is None:
            self.log_success(duration_ms)
        else:
            self.log_error(exc_val, duration_ms)
    
    def log_success(self, duration_ms: float):
        """Log successful operation completion."""
        logger = get_logger(self.context.get('logger_name', __name__))
        logger.info(
            f"Operation completed: {self.operation}",
            extra={
                'operation': self.operation,
                'request_id': self.request_id,
                'duration_ms': round(duration_ms, 2),
                'status': 'success',
                **self.context
            }
        )
    
    def log_error(self, error: Exception, duration_ms: float):
        """Log operation failure."""
        logger = get_logger(self.context.get('logger_name', __name__))
        logger.error(
            f"Operation failed: {self.operation} - {str(error)}",
            extra={
                'operation': self.operation,
                'request_id': self.request_id,
                'duration_ms': round(duration_ms, 2),
                'status': 'error',
                'error_type': type(error).__name__,
                'error_message': str(error),
                **self.context
            },
            exc_info=True
        )


# Context variable for request tracking
request_context: ContextVar[Dict[str, Any]] = ContextVar('request_context', default={})


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        # Base log entry
        log_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add request context if available
        try:
            context = request_context.get({})
            if context:
                log_entry['request_context'] = context
        except LookupError:
            pass
        
        # Add extra fields from log record
        if hasattr(record, '__dict__'):
            for key, value in record.__dict__.items():
                if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                              'filename', 'module', 'lineno', 'funcName', 'created', 
                              'msecs', 'relativeCreated', 'thread', 'threadName', 
                              'processName', 'process', 'getMessage', 'exc_info', 
                              'exc_text', 'stack_info']:
                    log_entry[key] = value
        
        # Add exception information if present
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': self.formatException(record.exc_info) if record.exc_info else None
            }
        
        return json.dumps(log_entry, default=str, ensure_ascii=False)


class RequestTrackingFilter(logging.Filter):
    """Filter to add request tracking information to log records."""
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add request context to log record."""
        try:
            context = request_context.get({})
            if context:
                for key, value in context.items():
                    setattr(record, key, value)
        except LookupError:
            pass
        
        return True


def setup_logging(
    log_level: str = None,
    structured: bool = True,
    enable_request_tracking: bool = True
) -> None:
    """
    Set up application logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        structured: Whether to use structured JSON logging
        enable_request_tracking: Whether to enable request tracking
    """
    # Use settings if no level provided
    if log_level is None:
        log_level = settings.monitoring.log_level.value
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Set formatter
    if structured:
        formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter(settings.monitoring.log_format)
    
    console_handler.setFormatter(formatter)
    
    # Add request tracking filter if enabled
    if enable_request_tracking:
        console_handler.addFilter(RequestTrackingFilter())
    
    # Add handler to root logger
    root_logger.addHandler(console_handler)
    
    # Configure specific loggers
    configure_logger_levels()


def configure_logger_levels():
    """Configure specific logger levels to reduce noise."""
    # Reduce noise from third-party libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('docker').setLevel(logging.WARNING)
    logging.getLogger('kafka').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
    
    # Set application loggers to appropriate levels
    logging.getLogger('src').setLevel(logging.DEBUG if settings.debug else logging.INFO)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name (typically __name__)
    
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


def set_request_context(**kwargs):
    """
    Set request context for logging.
    
    Args:
        **kwargs: Context key-value pairs
    """
    current_context = request_context.get({})
    current_context.update(kwargs)
    request_context.set(current_context)


def clear_request_context():
    """Clear the current request context."""
    request_context.set({})


def get_request_context() -> Dict[str, Any]:
    """Get the current request context."""
    return request_context.get({})


def log_performance(operation: str, **context):
    """
    Decorator for logging operation performance.
    
    Args:
        operation: Operation name for logging
        **context: Additional context for logging
    """
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            with LogContext(operation, **context):
                return await func(*args, **kwargs)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            with LogContext(operation, **context):
                return func(*args, **kwargs)
        
        # Return appropriate wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def log_api_request(
    method: str,
    path: str,
    status_code: int,
    duration_ms: float,
    request_id: str,
    user_agent: str = None,
    client_ip: str = None,
    **extra_context
):
    """
    Log API request with structured information.
    
    Args:
        method: HTTP method
        path: Request path
        status_code: HTTP status code
        duration_ms: Request duration in milliseconds
        request_id: Unique request identifier
        user_agent: Client user agent
        client_ip: Client IP address
        **extra_context: Additional context
    """
    logger = get_logger('api.requests')
    
    log_data = {
        'request_id': request_id,
        'method': method,
        'path': path,
        'status_code': status_code,
        'duration_ms': round(duration_ms, 2),
        'user_agent': user_agent,
        'client_ip': client_ip,
        **extra_context
    }
    
    # Determine log level based on status code
    if status_code >= 500:
        level = logging.ERROR
        message = f"API Error: {method} {path} -> {status_code}"
    elif status_code >= 400:
        level = logging.WARNING
        message = f"API Client Error: {method} {path} -> {status_code}"
    else:
        level = logging.INFO
        message = f"API Request: {method} {path} -> {status_code}"
    
    logger.log(level, message, extra=log_data)


def log_service_operation(
    service: str,
    operation: str,
    success: bool,
    duration_ms: float,
    error: str = None,
    **context
):
    """
    Log service operation with performance metrics.
    
    Args:
        service: Service name
        operation: Operation name
        success: Whether operation succeeded
        duration_ms: Operation duration in milliseconds
        error: Error message if operation failed
        **context: Additional context
    """
    logger = get_logger(f'services.{service}')
    
    log_data = {
        'service': service,
        'operation': operation,
        'success': success,
        'duration_ms': round(duration_ms, 2),
        'error': error,
        **context
    }
    
    if success:
        logger.info(f"Service operation completed: {service}.{operation}", extra=log_data)
    else:
        logger.error(f"Service operation failed: {service}.{operation} - {error}", extra=log_data)


def log_health_check(
    service: str,
    status: str,
    response_time_ms: float,
    error: str = None,
    **details
):
    """
    Log health check results.
    
    Args:
        service: Service name
        status: Health status
        response_time_ms: Response time in milliseconds
        error: Error message if health check failed
        **details: Additional health check details
    """
    logger = get_logger('health.checks')
    
    log_data = {
        'service': service,
        'status': status,
        'response_time_ms': round(response_time_ms, 2),
        'error': error,
        **details
    }
    
    if status == 'running':
        logger.info(f"Health check passed: {service}", extra=log_data)
    elif status == 'degraded':
        logger.warning(f"Health check degraded: {service}", extra=log_data)
    else:
        logger.error(f"Health check failed: {service} - {error}", extra=log_data)


def log_metrics(
    metric_name: str,
    value: Union[int, float],
    unit: str = None,
    tags: Dict[str, str] = None,
    **context
):
    """
    Log metrics for monitoring and alerting.
    
    Args:
        metric_name: Name of the metric
        value: Metric value
        unit: Unit of measurement
        tags: Metric tags for filtering
        **context: Additional context
    """
    logger = get_logger('metrics')
    
    log_data = {
        'metric_name': metric_name,
        'value': value,
        'unit': unit,
        'tags': tags or {},
        **context
    }
    
    logger.info(f"Metric: {metric_name}={value}{unit or ''}", extra=log_data)


# Initialize logging on module import
if not logging.getLogger().handlers:
    setup_logging()