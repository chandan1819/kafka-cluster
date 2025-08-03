"""
Tests for global error handlers and exception handling in the FastAPI application.
"""

import pytest
import json
from unittest.mock import Mock, AsyncMock, patch
from fastapi.testclient import TestClient
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.main import app
from src.exceptions import (
    LocalKafkaManagerError,
    ErrorCode,
    ServiceUnavailableError,
    DockerNotAvailableError,
    TopicNotFoundError,
    TopicAlreadyExistsError,
    MessageProduceError,
    ValidationError,
    TimeoutError
)


@pytest.mark.unit
class TestGlobalExceptionHandlers:
    """Test global exception handlers in the FastAPI application."""
    
    def setup_method(self):
        """Set up test client."""
        self.client = TestClient(app)
    
    def test_local_kafka_manager_error_handler_service_unavailable(self):
        """Test handling of service unavailable errors."""
        # Mock a route that raises ServiceUnavailableError
        with patch('src.api.routes.cluster_manager') as mock_cluster_manager:
            mock_cluster_manager.get_status.side_effect = ServiceUnavailableError(
                "Docker", "Docker daemon not running"
            )
            
            response = self.client.get("/cluster/status")
            
            assert response.status_code == 503
            data = response.json()
            assert data["error"] == ErrorCode.SERVICE_UNAVAILABLE.value
            assert "Docker is not available" in data["message"]
            assert "request_id" in data
    
    def test_local_kafka_manager_error_handler_not_found(self):
        """Test handling of not found errors."""
        with patch('src.api.routes.topic_manager') as mock_topic_manager:
            mock_topic_manager.get_topic_metadata.side_effect = TopicNotFoundError("nonexistent-topic")
            
            response = self.client.get("/topics/nonexistent-topic/metadata")
            
            assert response.status_code == 404
            data = response.json()
            assert data["error"] == ErrorCode.TOPIC_NOT_FOUND.value
            assert "Topic 'nonexistent-topic' not found" in data["message"]
            assert "request_id" in data
    
    def test_local_kafka_manager_error_handler_conflict(self):
        """Test handling of conflict errors."""
        with patch('src.api.routes.topic_manager') as mock_topic_manager:
            mock_topic_manager.create_topic.side_effect = TopicAlreadyExistsError("existing-topic")
            
            response = self.client.post("/topics", json={
                "name": "existing-topic",
                "partitions": 1,
                "replication_factor": 1
            })
            
            assert response.status_code == 409
            data = response.json()
            assert data["error"] == ErrorCode.TOPIC_ALREADY_EXISTS.value
            assert "Topic 'existing-topic' already exists" in data["message"]
            assert "request_id" in data
    
    def test_local_kafka_manager_error_handler_timeout(self):
        """Test handling of timeout errors."""
        with patch('src.api.routes.cluster_manager') as mock_cluster_manager:
            mock_cluster_manager.start_cluster.side_effect = TimeoutError("cluster_start", 60)
            
            response = self.client.post("/cluster/start")
            
            assert response.status_code == 408
            data = response.json()
            assert data["error"] == ErrorCode.TIMEOUT.value
            assert "timed out after 60 seconds" in data["message"]
            assert "request_id" in data
    
    def test_local_kafka_manager_error_handler_validation_error(self):
        """Test handling of validation errors."""
        with patch('src.api.routes.message_manager') as mock_message_manager:
            mock_message_manager.produce_message.side_effect = ValidationError(
                "topic", "invalid-topic", "Topic name contains invalid characters"
            )
            
            response = self.client.post("/produce", json={
                "topic": "invalid-topic",
                "value": {"test": "data"}
            })
            
            assert response.status_code == 422
            data = response.json()
            assert data["error"] == ErrorCode.VALIDATION_ERROR.value
            assert "Validation failed for field 'topic'" in data["message"]
            assert "request_id" in data
    
    def test_local_kafka_manager_error_handler_generic_error(self):
        """Test handling of generic LocalKafkaManagerError."""
        with patch('src.api.routes.message_manager') as mock_message_manager:
            mock_message_manager.produce_message.side_effect = MessageProduceError(
                "test-topic", "Serialization failed"
            )
            
            response = self.client.post("/produce", json={
                "topic": "test-topic",
                "value": {"test": "data"}
            })
            
            assert response.status_code == 400
            data = response.json()
            assert data["error"] == ErrorCode.MESSAGE_PRODUCE_FAILED.value
            assert "Failed to produce message to topic 'test-topic'" in data["message"]
            assert "request_id" in data
    
    def test_request_validation_error_handler(self):
        """Test handling of FastAPI request validation errors."""
        # Send invalid request data
        response = self.client.post("/topics", json={
            "name": "",  # Empty name should fail validation
            "partitions": -1,  # Negative partitions should fail validation
            "replication_factor": "invalid"  # String instead of int
        })
        
        assert response.status_code == 422
        data = response.json()
        assert data["error"] == ErrorCode.VALIDATION_ERROR.value
        assert data["message"] == "Request validation failed"
        assert "field_errors" in data["details"]
        assert "validation_errors" in data["details"]
        assert "request_id" in data
    
    def test_http_exception_handler(self):
        """Test handling of HTTP exceptions."""
        # Test 404 for non-existent endpoint
        response = self.client.get("/nonexistent-endpoint")
        
        assert response.status_code == 404
        data = response.json()
        assert data["error"] == ErrorCode.NOT_FOUND.value
        assert "request_id" in data
    
    def test_general_exception_handler(self):
        """Test handling of unexpected exceptions."""
        with patch('src.api.routes.service_catalog') as mock_service_catalog:
            # Simulate an unexpected exception
            mock_service_catalog.get_catalog.side_effect = RuntimeError("Unexpected error")
            
            response = self.client.get("/catalog")
            
            assert response.status_code == 500
            data = response.json()
            assert data["error"] == ErrorCode.INTERNAL_SERVER_ERROR.value
            assert "An unexpected error occurred" in data["message"]
            assert "request_id" in data
            assert data["details"]["type"] == "RuntimeError"
    
    def test_error_response_structure(self):
        """Test that error responses have the correct structure."""
        with patch('src.api.routes.topic_manager') as mock_topic_manager:
            mock_topic_manager.list_topics.side_effect = TopicNotFoundError("test-topic")
            
            response = self.client.get("/topics")
            
            assert response.status_code == 404
            data = response.json()
            
            # Check required fields
            assert "error" in data
            assert "message" in data
            assert "details" in data
            assert "timestamp" in data
            assert "request_id" in data
            
            # Check data types
            assert isinstance(data["error"], str)
            assert isinstance(data["message"], str)
            assert isinstance(data["details"], dict)
            assert isinstance(data["timestamp"], str)
            assert isinstance(data["request_id"], str)
    
    def test_request_id_generation(self):
        """Test that request IDs are generated and unique."""
        with patch('src.api.routes.topic_manager') as mock_topic_manager:
            mock_topic_manager.list_topics.side_effect = TopicNotFoundError("test-topic")
            
            # Make multiple requests
            response1 = self.client.get("/topics")
            response2 = self.client.get("/topics")
            
            data1 = response1.json()
            data2 = response2.json()
            
            # Both should have request IDs
            assert "request_id" in data1
            assert "request_id" in data2
            
            # Request IDs should be different
            assert data1["request_id"] != data2["request_id"]
            
            # Request IDs should be 8 characters long
            assert len(data1["request_id"]) == 8
            assert len(data2["request_id"]) == 8


@pytest.mark.unit
class TestErrorHandlerEdgeCases:
    """Test edge cases and special scenarios for error handlers."""
    
    def setup_method(self):
        """Set up test client."""
        self.client = TestClient(app)
    
    def test_error_with_nested_cause(self):
        """Test error handling with nested exception causes."""
        original_error = ConnectionError("Network connection failed")
        kafka_error = DockerNotAvailableError("Docker service unavailable", cause=original_error)
        
        with patch('src.api.routes.cluster_manager') as mock_cluster_manager:
            mock_cluster_manager.get_status.side_effect = kafka_error
            
            response = self.client.get("/cluster/status")
            
            assert response.status_code == 503
            data = response.json()
            assert data["error"] == ErrorCode.DOCKER_NOT_AVAILABLE.value
            assert "cause" in data["details"]
            assert "cause_type" in data["details"]
            assert data["details"]["cause"] == "Network connection failed"
            assert data["details"]["cause_type"] == "ConnectionError"
    
    def test_error_with_complex_details(self):
        """Test error handling with complex details."""
        complex_details = {
            "topic": "test-topic",
            "partition_count": 3,
            "config_errors": {
                "retention.ms": "Invalid value",
                "cleanup.policy": "Unknown policy"
            },
            "nested_data": {
                "level1": {
                    "level2": ["item1", "item2"]
                }
            }
        }
        
        error = LocalKafkaManagerError(
            message="Complex error with details",
            error_code=ErrorCode.TOPIC_CONFIG_INVALID,
            details=complex_details
        )
        
        with patch('src.api.routes.topic_manager') as mock_topic_manager:
            mock_topic_manager.create_topic.side_effect = error
            
            response = self.client.post("/topics", json={
                "name": "test-topic",
                "partitions": 3
            })
            
            assert response.status_code == 400
            data = response.json()
            assert data["error"] == ErrorCode.TOPIC_CONFIG_INVALID.value
            assert data["details"]["topic"] == "test-topic"
            assert data["details"]["partition_count"] == 3
            assert "config_errors" in data["details"]
            assert "nested_data" in data["details"]
    
    def test_validation_error_with_multiple_fields(self):
        """Test validation error handling with multiple field errors."""
        # Send request with multiple validation errors
        response = self.client.post("/topics", json={
            "name": "",  # Empty name
            "partitions": 0,  # Zero partitions
            "replication_factor": -1,  # Negative replication factor
            "config": "invalid"  # String instead of object
        })
        
        assert response.status_code == 422
        data = response.json()
        assert data["error"] == ErrorCode.VALIDATION_ERROR.value
        assert "field_errors" in data["details"]
        assert "validation_errors" in data["details"]
        
        # Should have multiple field errors
        field_errors = data["details"]["field_errors"]
        assert len(field_errors) > 1
    
    def test_error_logging_levels(self):
        """Test that different error types are logged at appropriate levels."""
        with patch('src.main.logger') as mock_logger:
            # Test 500 error (should be logged as error)
            with patch('src.api.routes.service_catalog') as mock_service_catalog:
                mock_service_catalog.get_catalog.side_effect = RuntimeError("Server error")
                
                response = self.client.get("/catalog")
                assert response.status_code == 500
                
                # Should log as error
                mock_logger.error.assert_called()
            
            mock_logger.reset_mock()
            
            # Test 400 error (should be logged as warning)
            with patch('src.api.routes.topic_manager') as mock_topic_manager:
                mock_topic_manager.list_topics.side_effect = TopicNotFoundError("test-topic")
                
                response = self.client.get("/topics")
                assert response.status_code == 404
                
                # Should log as warning
                mock_logger.warning.assert_called()
    
    @patch.dict('os.environ', {'ENVIRONMENT': 'development'})
    def test_development_error_details(self):
        """Test that development environment includes more error details."""
        with patch('src.api.routes.service_catalog') as mock_service_catalog:
            mock_service_catalog.get_catalog.side_effect = RuntimeError("Development error details")
            
            response = self.client.get("/catalog")
            
            assert response.status_code == 500
            data = response.json()
            
            # In development, should include error message
            assert "message" in data["details"]
            assert data["details"]["message"] == "Development error details"
    
    @patch.dict('os.environ', {'ENVIRONMENT': 'production'})
    def test_production_error_details(self):
        """Test that production environment hides sensitive error details."""
        with patch('src.api.routes.service_catalog') as mock_service_catalog:
            mock_service_catalog.get_catalog.side_effect = RuntimeError("Sensitive production error")
            
            response = self.client.get("/catalog")
            
            assert response.status_code == 500
            data = response.json()
            
            # In production, should not include sensitive error message
            assert "message" not in data["details"] or data["details"]["message"] != "Sensitive production error"
            assert data["message"] == "An unexpected error occurred. Please try again or contact support."


@pytest.mark.unit
class TestErrorHandlerIntegration:
    """Test error handler integration with actual API endpoints."""
    
    def setup_method(self):
        """Set up test client."""
        self.client = TestClient(app)
    
    def test_health_endpoint_error_handling(self):
        """Test error handling in health endpoint."""
        with patch('src.api.routes.cluster_manager') as mock_cluster_manager:
            mock_cluster_manager.get_status.side_effect = DockerNotAvailableError("Docker not running")
            
            response = self.client.get("/health")
            
            # Health endpoint should handle errors gracefully
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "unhealthy"
            assert "error" in data
    
    def test_detailed_health_endpoint(self):
        """Test detailed health endpoint."""
        with patch('src.services.health_monitor.health_monitor') as mock_monitor:
            mock_system_health = Mock()
            mock_system_health.to_dict.return_value = {
                "status": "healthy",
                "timestamp": "2024-01-01T00:00:00",
                "services": {},
                "dependencies_satisfied": True,
                "uptime_seconds": 3600.0
            }
            mock_monitor.get_system_health = AsyncMock(return_value=mock_system_health)
            
            response = self.client.get("/health/detailed")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert "timestamp" in data
            assert "services" in data
            assert "dependencies_satisfied" in data
            assert "uptime_seconds" in data
    
    def test_service_health_endpoint(self):
        """Test individual service health endpoint."""
        with patch('src.services.health_monitor.health_monitor') as mock_monitor:
            mock_service_health = Mock()
            mock_service_health.to_dict.return_value = {
                "name": "Docker",
                "status": "healthy",
                "last_check": "2024-01-01T00:00:00",
                "response_time_ms": 50.0,
                "error_message": None,
                "details": {"version": "20.10.0"}
            }
            mock_monitor.get_service_health = AsyncMock(return_value=mock_service_health)
            
            response = self.client.get("/health/services/docker")
            
            assert response.status_code == 200
            data = response.json()
            assert data["name"] == "Docker"
            assert data["status"] == "healthy"
            assert data["response_time_ms"] == 50.0
    
    def test_service_health_endpoint_not_found(self):
        """Test service health endpoint for unknown service."""
        with patch('src.services.health_monitor.health_monitor') as mock_monitor:
            mock_monitor.get_service_health = AsyncMock(return_value=None)
            
            response = self.client.get("/health/services/unknown")
            
            assert response.status_code == 404
            data = response.json()
            assert "Service 'unknown' not found" in data["message"]
    
    def test_health_metrics_endpoint(self):
        """Test health metrics endpoint."""
        with patch('src.services.health_monitor.health_monitor') as mock_monitor:
            mock_monitor.get_metrics.return_value = {
                "docker": Mock(
                    total_checks=100,
                    successful_checks=95,
                    failed_checks=5,
                    success_rate=lambda: 95.0,
                    average_response_time_ms=50.0,
                    last_failure_time=None,
                    consecutive_failures=0
                )
            }
            
            response = self.client.get("/health/metrics")
            
            assert response.status_code == 200
            data = response.json()
            assert "metrics" in data
            assert "docker" in data["metrics"]
            assert data["metrics"]["docker"]["total_checks"] == 100
            assert data["metrics"]["docker"]["success_rate_percent"] == 95.0
    
    def test_start_health_monitoring_endpoint(self):
        """Test starting health monitoring endpoint."""
        with patch('src.services.health_monitor.health_monitor') as mock_monitor:
            mock_monitor.start_monitoring = AsyncMock()
            
            response = self.client.post("/health/monitoring/start?interval=60")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "started"
            assert "60s interval" in data["message"]
            mock_monitor.start_monitoring.assert_called_once_with(interval=60)
    
    def test_stop_health_monitoring_endpoint(self):
        """Test stopping health monitoring endpoint."""
        with patch('src.services.health_monitor.health_monitor') as mock_monitor:
            mock_monitor.stop_monitoring = AsyncMock()
            
            response = self.client.post("/health/monitoring/stop")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "stopped"
            mock_monitor.stop_monitoring.assert_called_once()
    
    def test_root_endpoint_always_works(self):
        """Test that root endpoint always works even with service errors."""
        # Root endpoint should not depend on external services
        response = self.client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "version" in data
        assert "endpoints" in data
    
    def test_openapi_spec_generation(self):
        """Test that OpenAPI spec generation works with error handling."""
        response = self.client.get("/openapi.json")
        
        assert response.status_code == 200
        spec = response.json()
        assert "openapi" in spec
        assert "paths" in spec
        assert "components" in spec
    
    def test_docs_endpoint_accessibility(self):
        """Test that documentation endpoints are accessible."""
        # Test Swagger UI
        response = self.client.get("/docs")
        assert response.status_code == 200
        
        # Test ReDoc
        response = self.client.get("/redoc")
        assert response.status_code == 200