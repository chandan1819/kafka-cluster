"""
Logging middleware for FastAPI request/response tracking.

This middleware provides comprehensive request/response logging with performance
metrics, error tracking, and structured logging for monitoring and debugging.
"""

import time
import uuid
import json
from typing import Callable, Dict, Any, Optional
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from ..utils.logging import (
    get_logger, 
    set_request_context, 
    clear_request_context,
    log_api_request
)
from ..utils.metrics import record_api_metrics


logger = get_logger(__name__)


class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for logging HTTP requests and responses with performance metrics.
    
    Features:
    - Request/response logging with structured data
    - Performance timing and metrics collection
    - Error tracking and debugging information
    - Request ID generation for tracing
    - Context management for nested logging
    """
    
    def __init__(
        self,
        app: ASGIApp,
        log_requests: bool = True,
        log_responses: bool = True,
        log_request_body: bool = False,
        log_response_body: bool = False,
        max_body_size: int = 1024,
        exclude_paths: list = None
    ):
        """
        Initialize logging middleware.
        
        Args:
            app: ASGI application
            log_requests: Whether to log incoming requests
            log_responses: Whether to log outgoing responses
            log_request_body: Whether to log request body content
            log_response_body: Whether to log response body content
            max_body_size: Maximum body size to log (in bytes)
            exclude_paths: List of paths to exclude from logging
        """
        super().__init__(app)
        self.log_requests = log_requests
        self.log_responses = log_responses
        self.log_request_body = log_request_body
        self.log_response_body = log_response_body
        self.max_body_size = max_body_size
        self.exclude_paths = exclude_paths or ['/health', '/metrics', '/favicon.ico']
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and response with logging."""
        # Generate unique request ID
        request_id = str(uuid.uuid4())[:8]
        
        # Extract request information
        method = request.method
        path = request.url.path
        query_params = str(request.query_params) if request.query_params else None
        client_ip = self._get_client_ip(request)
        user_agent = request.headers.get('user-agent')
        
        # Skip logging for excluded paths
        if path in self.exclude_paths:
            return await call_next(request)
        
        # Set request context for nested logging
        set_request_context(
            request_id=request_id,
            method=method,
            path=path,
            client_ip=client_ip,
            user_agent=user_agent
        )
        
        # Read request body if needed
        request_body = None
        request_size = 0
        if self.log_request_body and method in ['POST', 'PUT', 'PATCH']:
            try:
                body_bytes = await request.body()
                request_size = len(body_bytes)
                if request_size <= self.max_body_size:
                    request_body = body_bytes.decode('utf-8')
                else:
                    request_body = f"<body too large: {request_size} bytes>"
            except Exception as e:
                logger.warning(f"Failed to read request body: {e}", extra={'request_id': request_id})
                request_body = "<failed to read body>"
        
        # Log incoming request
        if self.log_requests:
            self._log_request(
                request_id=request_id,
                method=method,
                path=path,
                query_params=query_params,
                headers=dict(request.headers),
                client_ip=client_ip,
                user_agent=user_agent,
                body=request_body,
                body_size=request_size
            )
        
        # Process request and measure timing
        start_time = time.time()
        response = None
        error = None
        
        try:
            response = await call_next(request)
        except Exception as e:
            error = e
            logger.error(
                f"Request processing failed: {method} {path}",
                extra={
                    'request_id': request_id,
                    'error_type': type(e).__name__,
                    'error_message': str(e)
                },
                exc_info=True
            )
            # Create error response
            response = JSONResponse(
                status_code=500,
                content={
                    'error': 'INTERNAL_SERVER_ERROR',
                    'message': 'An unexpected error occurred',
                    'request_id': request_id
                }
            )
        
        # Calculate timing
        duration_ms = (time.time() - start_time) * 1000
        
        # Extract response information
        status_code = response.status_code
        response_headers = dict(response.headers)
        response_size = int(response_headers.get('content-length', 0))
        
        # Read response body if needed
        response_body = None
        if self.log_response_body and hasattr(response, 'body'):
            try:
                if hasattr(response, 'body') and response.body:
                    if len(response.body) <= self.max_body_size:
                        response_body = response.body.decode('utf-8')
                    else:
                        response_body = f"<body too large: {len(response.body)} bytes>"
            except Exception as e:
                logger.warning(f"Failed to read response body: {e}", extra={'request_id': request_id})
                response_body = "<failed to read body>"
        
        # Log outgoing response
        if self.log_responses:
            self._log_response(
                request_id=request_id,
                method=method,
                path=path,
                status_code=status_code,
                headers=response_headers,
                body=response_body,
                body_size=response_size,
                duration_ms=duration_ms,
                error=error
            )
        
        # Log API request metrics
        log_api_request(
            method=method,
            path=path,
            status_code=status_code,
            duration_ms=duration_ms,
            request_id=request_id,
            user_agent=user_agent,
            client_ip=client_ip
        )
        
        # Record metrics
        record_api_metrics(
            method=method,
            path=path,
            status_code=status_code,
            duration_ms=duration_ms,
            request_size_bytes=request_size if request_size > 0 else None,
            response_size_bytes=response_size if response_size > 0 else None
        )
        
        # Clear request context
        clear_request_context()
        
        return response
    
    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address from request."""
        # Check for forwarded headers first
        forwarded_for = request.headers.get('x-forwarded-for')
        if forwarded_for:
            return forwarded_for.split(',')[0].strip()
        
        real_ip = request.headers.get('x-real-ip')
        if real_ip:
            return real_ip
        
        # Fall back to direct client IP
        if hasattr(request, 'client') and request.client:
            return request.client.host
        
        return 'unknown'
    
    def _log_request(
        self,
        request_id: str,
        method: str,
        path: str,
        query_params: Optional[str],
        headers: Dict[str, str],
        client_ip: str,
        user_agent: Optional[str],
        body: Optional[str],
        body_size: int
    ):
        """Log incoming request details."""
        # Filter sensitive headers
        filtered_headers = self._filter_sensitive_headers(headers)
        
        log_data = {
            'request_id': request_id,
            'method': method,
            'path': path,
            'query_params': query_params,
            'headers': filtered_headers,
            'client_ip': client_ip,
            'user_agent': user_agent,
            'body_size': body_size
        }
        
        if body is not None:
            log_data['body'] = body
        
        logger.info(f"Incoming request: {method} {path}", extra=log_data)
    
    def _log_response(
        self,
        request_id: str,
        method: str,
        path: str,
        status_code: int,
        headers: Dict[str, str],
        body: Optional[str],
        body_size: int,
        duration_ms: float,
        error: Optional[Exception]
    ):
        """Log outgoing response details."""
        log_data = {
            'request_id': request_id,
            'method': method,
            'path': path,
            'status_code': status_code,
            'headers': headers,
            'body_size': body_size,
            'duration_ms': round(duration_ms, 2)
        }
        
        if body is not None:
            log_data['body'] = body
        
        if error:
            log_data['error'] = {
                'type': type(error).__name__,
                'message': str(error)
            }
        
        # Determine log level based on status code
        if status_code >= 500:
            logger.error(f"Response sent: {method} {path} -> {status_code}", extra=log_data)
        elif status_code >= 400:
            logger.warning(f"Response sent: {method} {path} -> {status_code}", extra=log_data)
        else:
            logger.info(f"Response sent: {method} {path} -> {status_code}", extra=log_data)
    
    def _filter_sensitive_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Filter out sensitive header values."""
        sensitive_headers = {
            'authorization', 'cookie', 'x-api-key', 'x-auth-token',
            'x-access-token', 'x-csrf-token', 'x-session-id'
        }
        
        filtered = {}
        for key, value in headers.items():
            if key.lower() in sensitive_headers:
                filtered[key] = '<redacted>'
            else:
                filtered[key] = value
        
        return filtered


class DebugLoggingMiddleware(LoggingMiddleware):
    """
    Extended logging middleware with debug capabilities.
    
    Provides additional debugging information including:
    - Full request/response body logging
    - Detailed header information
    - Performance profiling data
    - Memory usage tracking
    """
    
    def __init__(self, app: ASGIApp):
        super().__init__(
            app,
            log_requests=True,
            log_responses=True,
            log_request_body=True,
            log_response_body=True,
            max_body_size=10240,  # 10KB for debug mode
            exclude_paths=['/favicon.ico']  # Only exclude favicon in debug mode
        )
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Enhanced dispatch with debug information."""
        # Add memory usage tracking
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss
        
        # Call parent dispatch
        response = await super().dispatch(request, call_next)
        
        # Log memory usage change
        memory_after = process.memory_info().rss
        memory_delta = memory_after - memory_before
        
        if abs(memory_delta) > 1024 * 1024:  # Log if change > 1MB
            logger.debug(
                f"Memory usage change: {memory_delta / 1024 / 1024:.2f} MB",
                extra={
                    'memory_before_mb': memory_before / 1024 / 1024,
                    'memory_after_mb': memory_after / 1024 / 1024,
                    'memory_delta_mb': memory_delta / 1024 / 1024
                }
            )
        
        return response