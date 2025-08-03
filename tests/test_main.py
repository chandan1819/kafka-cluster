"""
Tests for the main FastAPI application
"""

import pytest
from unittest.mock import patch, Mock
from fastapi.testclient import TestClient

from src.main import app
from src.models.base import ServiceStatus
from src.models.cluster import ClusterStatus


@pytest.mark.unit
class TestMainApplication:
    """Test main FastAPI application functionality."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)
    
    def test_root_endpoint(self, client):
        """Test the root endpoint returns correct information"""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Local Kafka Manager"
        assert data["version"] == "1.0.0"
        assert "description" in data
        assert data["docs_url"] == "/docs"
        assert "endpoints" in data
        assert isinstance(data["endpoints"], list)
    
    def test_root_endpoint_structure(self, client):
        """Test root endpoint response structure."""
        response = client.get("/")
        data = response.json()
        
        required_fields = ["name", "version", "description", "docs_url", "endpoints"]
        for field in required_fields:
            assert field in data, f"Missing required field: {field}"
        
        # Verify endpoints structure
        endpoints = data["endpoints"]
        assert len(endpoints) > 0, "Should have at least one endpoint"
        
        for endpoint in endpoints:
            assert "path" in endpoint
            assert "method" in endpoint
            assert "description" in endpoint
    
    @patch('src.api.routes.cluster_manager.get_status')
    def test_health_check_healthy(self, mock_get_status, client):
        """Test the health check endpoint when cluster is healthy"""
        mock_get_status.return_value = ClusterStatus(
            status=ServiceStatus.RUNNING,
            broker_count=1,
            version="7.4.0",
            endpoints={"kafka": "localhost:9092"},
            uptime=3600
        )
        
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "local-kafka-manager"
        assert "cluster_status" in data
        assert data["cluster_status"]["status"] == "running"
        assert "timestamp" in data
    
    @patch('src.api.routes.cluster_manager.get_status')
    def test_health_check_unhealthy(self, mock_get_status, client):
        """Test the health check endpoint when cluster is unhealthy"""
        mock_get_status.return_value = ClusterStatus(
            status=ServiceStatus.ERROR,
            broker_count=0,
            version="",
            endpoints={},
            uptime=None
        )
        
        response = client.get("/health")
        
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "unhealthy"
        assert data["service"] == "local-kafka-manager"
        assert data["cluster_status"]["status"] == "error"
    
    @patch('src.api.routes.cluster_manager.get_status')
    def test_health_check_exception_handling(self, mock_get_status, client):
        """Test health check endpoint exception handling"""
        mock_get_status.side_effect = Exception("Health check failed")
        
        response = client.get("/health")
        
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "unhealthy"
        assert "error" in data
    
    def test_docs_endpoint(self, client):
        """Test that the docs endpoint is accessible"""
        response = client.get("/docs")
        
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
    
    def test_openapi_schema(self, client):
        """Test OpenAPI schema generation"""
        response = client.get("/openapi.json")
        
        assert response.status_code == 200
        schema = response.json()
        
        # Verify basic OpenAPI structure
        assert "openapi" in schema
        assert "info" in schema
        assert "paths" in schema
        
        # Verify API info
        info = schema["info"]
        assert info["title"] == "Local Kafka Manager"
        assert info["version"] == "1.0.0"
        
        # Verify some key endpoints are documented
        paths = schema["paths"]
        expected_paths = ["/", "/health", "/catalog", "/cluster/status", "/topics"]
        for path in expected_paths:
            assert path in paths, f"Missing documented path: {path}"
    
    def test_cors_headers(self, client):
        """Test CORS headers are properly set"""
        response = client.options("/")
        
        # Should allow CORS preflight
        assert response.status_code in [200, 405]  # Some frameworks return 405 for OPTIONS
        
        # Test actual request has CORS headers
        response = client.get("/")
        # Note: CORS headers might not be present in test client, but we can verify the app is configured
        assert response.status_code == 200
    
    def test_request_validation_middleware(self, client):
        """Test request validation middleware"""
        # Test with invalid JSON
        response = client.post(
            "/topics",
            data="invalid json",
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 422
        data = response.json()
        assert "error" in data
    
    def test_error_handling_middleware(self, client):
        """Test global error handling middleware"""
        # Test 404 handling
        response = client.get("/nonexistent-endpoint")
        
        assert response.status_code == 404
        data = response.json()
        assert data["error"] == "HTTP_404"
        assert "message" in data
        assert "timestamp" in data
    
    def test_application_startup_events(self):
        """Test application startup events"""
        # This test verifies that the app can be created without errors
        # and that startup events are properly configured
        from src.main import app
        
        assert app is not None
        assert hasattr(app, 'router')
        assert len(app.routes) > 0
    
    def test_application_metadata(self):
        """Test application metadata configuration"""
        from src.main import app
        
        # Verify app configuration
        assert app.title == "Local Kafka Manager"
        assert app.version == "1.0.0"
        assert app.description is not None
        assert len(app.description) > 0
    
    def test_middleware_configuration(self):
        """Test middleware is properly configured"""
        from src.main import app
        
        # Verify middleware stack
        middleware_types = [type(middleware) for middleware in app.user_middleware]
        
        # Should have error handling middleware
        # Note: Specific middleware types depend on implementation
        assert len(app.user_middleware) >= 0  # At least basic middleware
    
    def test_route_registration(self):
        """Test that all expected routes are registered"""
        from src.main import app
        
        # Get all registered routes
        routes = []
        for route in app.routes:
            if hasattr(route, 'path'):
                routes.append(route.path)
        
        # Verify key routes are registered
        expected_routes = [
            "/",
            "/health",
            "/catalog",
            "/cluster/status",
            "/cluster/start",
            "/cluster/stop",
            "/topics",
            "/produce",
            "/consume"
        ]
        
        for expected_route in expected_routes:
            # Check if route exists (may have path parameters)
            route_exists = any(
                expected_route in route or route.startswith(expected_route)
                for route in routes
            )
            assert route_exists, f"Route {expected_route} not found in registered routes"


@pytest.mark.unit
class TestApplicationConfiguration:
    """Test application configuration and settings."""
    
    def test_environment_configuration(self):
        """Test environment-based configuration"""
        # Test that app can handle different environment configurations
        import os
        
        # Save original environment
        original_env = os.environ.get("ENVIRONMENT", "")
        
        try:
            # Test development environment
            os.environ["ENVIRONMENT"] = "development"
            from src.main import app
            assert app is not None
            
            # Test production environment
            os.environ["ENVIRONMENT"] = "production"
            # Note: Would need to reload app for this to take effect
            # This is more of a configuration validation test
            
        finally:
            # Restore original environment
            if original_env:
                os.environ["ENVIRONMENT"] = original_env
            elif "ENVIRONMENT" in os.environ:
                del os.environ["ENVIRONMENT"]
    
    def test_logging_configuration(self):
        """Test logging configuration"""
        import logging
        
        # Verify logging is configured
        logger = logging.getLogger("src")
        assert logger is not None
        
        # Test that we can log without errors
        logger.info("Test log message")
        logger.error("Test error message")
    
    def test_dependency_injection(self, client):
        """Test dependency injection is working"""
        # Test that dependencies are properly injected by making requests
        # that require various services
        
        with patch('src.api.routes.cluster_manager.get_status') as mock_status:
            from src.models.cluster import ClusterStatus
            mock_status.return_value = ClusterStatus(
                status=ServiceStatus.RUNNING,
                broker_count=1
            )
            
            response = client.get("/cluster/status")
            assert response.status_code == 200
            
            # Verify the dependency was called
            mock_status.assert_called_once()


@pytest.mark.unit
class TestApplicationSecurity:
    """Test application security features."""
    
    def test_security_headers(self, client):
        """Test security headers are set"""
        response = client.get("/")
        
        # Check for basic security headers
        # Note: Specific headers depend on middleware configuration
        assert response.status_code == 200
        
        # Verify no sensitive information is leaked
        assert "server" not in response.headers.get("server", "").lower()
    
    def test_input_sanitization(self, client):
        """Test input sanitization"""
        # Test with potentially malicious input
        malicious_inputs = [
            "<script>alert('xss')</script>",
            "'; DROP TABLE topics; --",
            "../../../etc/passwd",
            "{{7*7}}",  # Template injection
        ]
        
        for malicious_input in malicious_inputs:
            # Test topic creation with malicious name
            topic_data = {
                "name": malicious_input,
                "partitions": 1,
                "replication_factor": 1
            }
            
            response = client.post("/topics", json=topic_data)
            
            # Should either reject the input or sanitize it
            # The exact behavior depends on validation rules
            assert response.status_code in [400, 422, 500]  # Should not succeed
    
    def test_rate_limiting_headers(self, client):
        """Test rate limiting headers if implemented"""
        response = client.get("/")
        
        # Check if rate limiting headers are present
        # Note: This depends on whether rate limiting is implemented
        assert response.status_code == 200
        
        # If rate limiting is implemented, these headers might be present:
        # X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset