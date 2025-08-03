"""
Tests for structured logging functionality.

This module tests the logging utilities, middleware, and metrics collection
to ensure proper logging behavior and performance tracking.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from starlette.responses import JSONResponse

from src.utils.logging import (
    setup_logging,
    get_logger,
    set_request_context,
    clear_request_context,
    get_request_context,
    log_performance,
    log_api_request,
    log_service_operation,
    log_health_check,
    log_metrics,
    StructuredFormatter,
    RequestTrackingFilter,
    LogContext
)
from src.utils.metrics import (
    MetricsCollector,
    MetricType,
    counter,
    gauge,
    histogram,
    timer,
    time_operation,
    get_metric_summary,
    get_all_metrics,
    clear_metrics,
    record_api_metrics,
    record_service_metrics,
    record_health_metrics,
    get_performance_report
)
from src.middleware.logging_middleware import LoggingMiddleware, DebugLoggingMiddleware


class TestStructuredLogging:
    """Test structured logging functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        # Clear any existing request context
        clear_request_context()
        
        # Set up test logger with string buffer
        self.log_buffer = StringIO()
        self.test_logger = logging.getLogger('test_logger')
        self.test_logger.setLevel(logging.DEBUG)
        
        # Remove existing handlers
        for handler in self.test_logger.handlers[:]:
            self.test_logger.removeHandler(handler)
        
        # Add string buffer handler
        handler = logging.StreamHandler(self.log_buffer)
        handler.setFormatter(StructuredFormatter())
        self.test_logger.addHandler(handler)
    
    def teardown_method(self):
        """Clean up test environment."""
        clear_request_context()
        clear_metrics()
    
    def test_structured_formatter(self):
        """Test structured JSON log formatting."""
        # Log a test message
        self.test_logger.info("Test message", extra={'key': 'value', 'number': 42})
        
        # Get log output
        log_output = self.log_buffer.getvalue().strip()
        
        # Parse JSON
        log_data = json.loads(log_output)
        
        # Verify structure
        assert log_data['level'] == 'INFO'
        assert log_data['message'] == 'Test message'
        assert log_data['logger'] == 'test_logger'
        assert log_data['key'] == 'value'
        assert log_data['number'] == 42
        assert 'timestamp' in log_data
        assert 'module' in log_data
        assert 'function' in log_data
        assert 'line' in log_data
    
    def test_request_context_tracking(self):
        """Test request context tracking in logs."""
        # Set request context
        set_request_context(
            request_id='test-123',
            method='GET',
            path='/test',
            client_ip='127.0.0.1'
        )
        
        # Log a message
        self.test_logger.info("Test with context")
        
        # Get log output
        log_output = self.log_buffer.getvalue().strip()
        log_data = json.loads(log_output)
        
        # Verify context is included
        assert 'request_context' in log_data
        context = log_data['request_context']
        assert context['request_id'] == 'test-123'
        assert context['method'] == 'GET'
        assert context['path'] == '/test'
        assert context['client_ip'] == '127.0.0.1'
    
    def test_log_context_manager(self):
        """Test LogContext context manager."""
        with patch('src.utils.logging.get_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            # Test successful operation
            with LogContext("test_operation", param1="value1"):
                pass
            
            # Verify success log was called
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            assert "Operation completed: test_operation" in call_args[0][0]
            assert call_args[1]['extra']['operation'] == 'test_operation'
            assert call_args[1]['extra']['status'] == 'success'
            assert 'duration_ms' in call_args[1]['extra']
    
    def test_log_context_manager_with_error(self):
        """Test LogContext context manager with exception."""
        with patch('src.utils.logging.get_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            # Test operation with error
            try:
                with LogContext("test_operation", param1="value1"):
                    raise ValueError("Test error")
            except ValueError:
                pass
            
            # Verify error log was called
            mock_logger.error.assert_called_once()
            call_args = mock_logger.error.call_args
            assert "Operation failed: test_operation" in call_args[0][0]
            assert call_args[1]['extra']['operation'] == 'test_operation'
            assert call_args[1]['extra']['status'] == 'error'
            assert call_args[1]['extra']['error_type'] == 'ValueError'
            assert call_args[1]['extra']['error_message'] == 'Test error'
    
    def test_log_performance_decorator(self):
        """Test log_performance decorator."""
        with patch('src.utils.logging.get_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            @log_performance("test_function")
            def test_func():
                time.sleep(0.01)  # Small delay
                return "result"
            
            result = test_func()
            
            assert result == "result"
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            assert "Operation completed: test_function" in call_args[0][0]
    
    def test_log_performance_decorator_async(self):
        """Test log_performance decorator with async function."""
        import asyncio
        
        with patch('src.utils.logging.get_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            @log_performance("test_async_function")
            async def test_async_func():
                await asyncio.sleep(0.01)  # Small delay
                return "async_result"
            
            result = asyncio.run(test_async_func())
            
            assert result == "async_result"
            mock_logger.info.assert_called_once()
    
    def test_log_api_request(self):
        """Test API request logging."""
        with patch('src.utils.logging.get_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            log_api_request(
                method='GET',
                path='/test',
                status_code=200,
                duration_ms=123.45,
                request_id='req-123',
                user_agent='test-agent',
                client_ip='127.0.0.1'
            )
            
            mock_logger.log.assert_called_once()
            call_args = mock_logger.log.call_args
            assert call_args[0][0] == logging.INFO  # Log level
            assert "API Request: GET /test -> 200" in call_args[0][1]
            
            extra = call_args[1]['extra']
            assert extra['method'] == 'GET'
            assert extra['path'] == '/test'
            assert extra['status_code'] == 200
            assert extra['duration_ms'] == 123.45
            assert extra['request_id'] == 'req-123'
    
    def test_log_service_operation(self):
        """Test service operation logging."""
        with patch('src.utils.logging.get_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            # Test successful operation
            log_service_operation(
                service='test_service',
                operation='test_operation',
                success=True,
                duration_ms=50.0,
                param1='value1'
            )
            
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            assert "Service operation completed: test_service.test_operation" in call_args[0][0]
            
            extra = call_args[1]['extra']
            assert extra['service'] == 'test_service'
            assert extra['operation'] == 'test_operation'
            assert extra['success'] is True
            assert extra['duration_ms'] == 50.0
            assert extra['param1'] == 'value1'
    
    def test_log_health_check(self):
        """Test health check logging."""
        with patch('src.utils.logging.get_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            log_health_check(
                service='kafka',
                status='running',
                response_time_ms=25.5,
                broker_count=1
            )
            
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            assert "Health check passed: kafka" in call_args[0][0]
            
            extra = call_args[1]['extra']
            assert extra['service'] == 'kafka'
            assert extra['status'] == 'running'
            assert extra['response_time_ms'] == 25.5
            assert extra['broker_count'] == 1


class TestMetricsCollection:
    """Test metrics collection functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        clear_metrics()
    
    def teardown_method(self):
        """Clean up test environment."""
        clear_metrics()
    
    def test_counter_metric(self):
        """Test counter metric collection."""
        counter('test.counter', 1)
        counter('test.counter', 5)
        
        summary = get_metric_summary('test.counter')
        assert summary is not None
        assert summary.metric_type == MetricType.COUNTER
        assert summary.count == 2
        assert summary.sum == 6
        assert summary.last_value == 5
    
    def test_gauge_metric(self):
        """Test gauge metric collection."""
        gauge('test.gauge', 10)
        gauge('test.gauge', 20)
        gauge('test.gauge', 15)
        
        summary = get_metric_summary('test.gauge')
        assert summary is not None
        assert summary.metric_type == MetricType.GAUGE
        assert summary.count == 3
        assert summary.last_value == 15
        assert summary.min == 10
        assert summary.max == 20
        assert summary.avg == 15
    
    def test_histogram_metric(self):
        """Test histogram metric collection."""
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        for value in values:
            histogram('test.histogram', value)
        
        summary = get_metric_summary('test.histogram')
        assert summary is not None
        assert summary.metric_type == MetricType.HISTOGRAM
        assert summary.count == 10
        assert summary.sum == 55
        assert summary.min == 1
        assert summary.max == 10
        assert summary.avg == 5.5
        assert summary.p50 == 5
        assert summary.p95 == 10
        assert summary.p99 == 10
    
    def test_timer_metric(self):
        """Test timer metric collection."""
        timer('test.timer', 100.5)
        timer('test.timer', 200.0)
        timer('test.timer', 150.25)
        
        summary = get_metric_summary('test.timer')
        assert summary is not None
        assert summary.metric_type == MetricType.TIMER
        assert summary.count == 3
        assert abs(summary.avg - 150.25) < 0.01
    
    def test_time_operation_context_manager(self):
        """Test time_operation context manager."""
        with time_operation('test.operation'):
            time.sleep(0.01)  # Small delay
        
        summary = get_metric_summary('test.operation')
        assert summary is not None
        assert summary.metric_type == MetricType.TIMER
        assert summary.count == 1
        assert summary.last_value > 0  # Should have some duration
    
    def test_metrics_with_tags(self):
        """Test metrics with tags."""
        counter('test.tagged', 1, tags={'service': 'api', 'method': 'GET'})
        counter('test.tagged', 2, tags={'service': 'api', 'method': 'POST'})
        
        summary = get_metric_summary('test.tagged')
        assert summary is not None
        assert summary.count == 2
        assert summary.sum == 3
        # Last tags should be from the most recent metric
        assert summary.tags['service'] == 'api'
        assert summary.tags['method'] == 'POST'
    
    def test_get_all_metrics(self):
        """Test getting all metrics."""
        counter('metric1', 1)
        gauge('metric2', 10)
        histogram('metric3', 5)
        
        all_metrics = get_all_metrics()
        assert len(all_metrics) == 3
        assert 'metric1' in all_metrics
        assert 'metric2' in all_metrics
        assert 'metric3' in all_metrics
    
    def test_metrics_time_filtering(self):
        """Test metrics filtering by time."""
        # Add some metrics
        counter('test.time_filter', 1)
        time.sleep(0.01)  # Small delay
        
        # Get metrics since a time in the future (should return empty)
        future_time = datetime.utcnow() + timedelta(minutes=1)
        summary = get_metric_summary('test.time_filter', since=future_time)
        assert summary is None
        
        # Get metrics since the past (should return data)
        past_time = datetime.utcnow() - timedelta(minutes=1)
        summary = get_metric_summary('test.time_filter', since=past_time)
        assert summary is not None
        assert summary.count == 1
    
    def test_record_api_metrics(self):
        """Test API metrics recording."""
        record_api_metrics(
            method='GET',
            path='/test',
            status_code=200,
            duration_ms=123.45,
            request_size_bytes=1024,
            response_size_bytes=2048
        )
        
        # Check that metrics were recorded
        all_metrics = get_all_metrics()
        
        # Should have multiple metrics
        assert any('api.requests.total' in name for name in all_metrics.keys())
        assert any('api.requests.duration_ms' in name for name in all_metrics.keys())
        assert any('api.requests.size_bytes' in name for name in all_metrics.keys())
        assert any('api.responses.size_bytes' in name for name in all_metrics.keys())
    
    def test_record_service_metrics(self):
        """Test service metrics recording."""
        record_service_metrics(
            service='test_service',
            operation='test_operation',
            success=True,
            duration_ms=50.0
        )
        
        all_metrics = get_all_metrics()
        
        # Should have service metrics
        assert any('service.operations.total' in name for name in all_metrics.keys())
        assert any('service.operations.duration_ms' in name for name in all_metrics.keys())
        assert any('service.operations.success' in name for name in all_metrics.keys())
    
    def test_record_health_metrics(self):
        """Test health metrics recording."""
        record_health_metrics(
            service='kafka',
            status='running',
            response_time_ms=25.5
        )
        
        all_metrics = get_all_metrics()
        
        # Should have health metrics
        assert any('health.checks.total' in name for name in all_metrics.keys())
        assert any('health.checks.duration_ms' in name for name in all_metrics.keys())
        assert any('health.status.kafka' in name for name in all_metrics.keys())
    
    def test_performance_report(self):
        """Test performance report generation."""
        # Add some test metrics
        record_api_metrics('GET', '/test', 200, 100.0)
        record_api_metrics('POST', '/test', 201, 150.0)
        record_api_metrics('GET', '/error', 500, 200.0)
        
        record_service_metrics('test_service', 'operation1', True, 50.0)
        record_service_metrics('test_service', 'operation2', False, 75.0, 'timeout')
        
        record_health_metrics('kafka', 'running', 25.0)
        record_health_metrics('rest-proxy', 'error', 100.0)
        
        # Generate report
        report = get_performance_report(since_minutes=60)
        
        # Verify report structure
        assert 'period' in report
        assert 'api_performance' in report
        assert 'service_performance' in report
        assert 'health_status' in report
        assert 'system_metrics' in report
        assert 'summary' in report
        
        # Verify API performance data
        api_perf = report['api_performance']
        assert api_perf['total_requests'] > 0
        assert api_perf['average_response_time_ms'] > 0
        assert api_perf['error_rate_percent'] >= 0
        
        # Verify service performance data
        service_perf = report['service_performance']
        assert service_perf['total_operations'] > 0
        assert service_perf['success_rate_percent'] >= 0
        
        # Verify health status data
        health_status = report['health_status']
        assert health_status['total_checks'] > 0
        assert health_status['failure_rate_percent'] >= 0


class TestLoggingMiddleware:
    """Test logging middleware functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        clear_request_context()
        clear_metrics()
    
    def teardown_method(self):
        """Clean up test environment."""
        clear_request_context()
        clear_metrics()
    
    def test_logging_middleware_basic(self):
        """Test basic logging middleware functionality."""
        app = FastAPI()
        app.add_middleware(LoggingMiddleware)
        
        @app.get("/test")
        async def test_endpoint():
            return {"message": "test"}
        
        client = TestClient(app)
        
        with patch('src.middleware.logging_middleware.log_api_request') as mock_log:
            response = client.get("/test")
            
            assert response.status_code == 200
            mock_log.assert_called_once()
            
            # Verify log call arguments
            call_args = mock_log.call_args[1]
            assert call_args['method'] == 'GET'
            assert call_args['path'] == '/test'
            assert call_args['status_code'] == 200
            assert 'duration_ms' in call_args
            assert 'request_id' in call_args
    
    def test_logging_middleware_excludes_paths(self):
        """Test that middleware excludes specified paths."""
        app = FastAPI()
        app.add_middleware(
            LoggingMiddleware,
            exclude_paths=['/health', '/metrics']
        )
        
        @app.get("/health")
        async def health():
            return {"status": "ok"}
        
        @app.get("/test")
        async def test():
            return {"message": "test"}
        
        client = TestClient(app)
        
        with patch('src.middleware.logging_middleware.log_api_request') as mock_log:
            # Request to excluded path should not be logged
            client.get("/health")
            assert mock_log.call_count == 0
            
            # Request to non-excluded path should be logged
            client.get("/test")
            assert mock_log.call_count == 1
    
    def test_logging_middleware_error_handling(self):
        """Test middleware error handling."""
        app = FastAPI()
        app.add_middleware(LoggingMiddleware)
        
        @app.get("/error")
        async def error_endpoint():
            raise ValueError("Test error")
        
        client = TestClient(app)
        
        with patch('src.middleware.logging_middleware.log_api_request') as mock_log:
            response = client.get("/error")
            
            # Should return 500 error
            assert response.status_code == 500
            
            # Should still log the request
            mock_log.assert_called_once()
            call_args = mock_log.call_args[1]
            assert call_args['status_code'] == 500
    
    def test_debug_logging_middleware(self):
        """Test debug logging middleware with enhanced features."""
        app = FastAPI()
        app.add_middleware(DebugLoggingMiddleware)
        
        @app.post("/test")
        async def test_endpoint(data: dict):
            return {"received": data}
        
        client = TestClient(app)
        
        with patch('src.middleware.logging_middleware.log_api_request') as mock_log:
            response = client.post("/test", json={"key": "value"})
            
            assert response.status_code == 200
            mock_log.assert_called_once()
            
            # Debug middleware should log more details
            call_args = mock_log.call_args[1]
            assert call_args['method'] == 'POST'
            assert call_args['path'] == '/test'


if __name__ == "__main__":
    pytest.main([__file__])