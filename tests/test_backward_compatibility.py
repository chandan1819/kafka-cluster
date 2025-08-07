"""
Tests for backward compatibility middleware.

This module tests the backward compatibility layer that ensures existing
single-cluster APIs continue to work with the new multi-cluster architecture.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime

from src.middleware.backward_compatibility import (
    BackwardCompatibilityManager,
    LegacyAPIMiddleware,
    ensure_backward_compatibility,
    create_legacy_api_middleware
)
from src.models.multi_cluster import ClusterDefinition, ClusterStatus, PortAllocation
from src.exceptions import ClusterNotFoundError, MultiClusterError


class TestBackwardCompatibilityManager:
    """Test backward compatibility manager functionality."""
    
    @pytest.fixture
    def mock_multi_cluster_manager(self):
        """Mock multi-cluster manager."""
        manager = AsyncMock()
        manager.get_cluster_status.return_value = Mock(status="running")
        manager.start_cluster.return_value = None
        manager.create_cluster.return_value = None
        return manager
    
    @pytest.fixture
    def mock_cluster_registry(self):
        """Mock cluster registry."""
        registry = AsyncMock()
        registry.get_cluster.return_value = None
        registry.register_cluster.return_value = True
        return registry
    
    @pytest.fixture
    def mock_template_manager(self):
        """Mock template manager."""
        manager = AsyncMock()
        
        # Mock builtin template
        builtin_template = Mock()
        builtin_template.id = "development"
        builtin_template.name = "Development"
        
        manager.get_template.return_value = builtin_template
        manager.get_builtin_templates.return_value = [builtin_template]
        
        # Mock cluster definition from template
        cluster_def = Mock()
        cluster_def.id = "default"
        cluster_def.name = "Default Cluster"
        manager.apply_template.return_value = cluster_def
        
        return manager
    
    @pytest.fixture
    def compatibility_manager(self, mock_multi_cluster_manager, mock_cluster_registry, mock_template_manager):
        """Create compatibility manager with mocked dependencies."""
        return BackwardCompatibilityManager(
            mock_multi_cluster_manager,
            mock_cluster_registry,
            mock_template_manager
        )
    
    @pytest.mark.asyncio
    async def test_ensure_default_cluster_creates_new_cluster(
        self, compatibility_manager, mock_cluster_registry, mock_template_manager
    ):
        """Test that ensure_default_cluster creates a new default cluster when none exists."""
        # Setup: No existing default cluster
        mock_cluster_registry.get_cluster.return_value = None
        
        # Execute
        result = await compatibility_manager.ensure_default_cluster()
        
        # Verify
        assert result is True
        mock_cluster_registry.get_cluster.assert_called_with("default")
        mock_template_manager.get_template.assert_called_with("development")
        mock_template_manager.apply_template.assert_called_once()
        mock_cluster_registry.register_cluster.assert_called_once()
        assert compatibility_manager._default_cluster_initialized is True
    
    @pytest.mark.asyncio
    async def test_ensure_default_cluster_uses_existing_cluster(
        self, compatibility_manager, mock_cluster_registry, mock_multi_cluster_manager
    ):
        """Test that ensure_default_cluster uses existing default cluster."""
        # Setup: Existing default cluster that's already running
        existing_cluster = Mock()
        existing_cluster.id = "default"
        mock_cluster_registry.get_cluster.return_value = existing_cluster
        
        # Mock cluster status as already running
        status_mock = Mock()
        status_mock.status = "running"
        mock_multi_cluster_manager.get_cluster_status.return_value = status_mock
        
        # Execute
        result = await compatibility_manager.ensure_default_cluster()
        
        # Verify
        assert result is True
        mock_cluster_registry.get_cluster.assert_called_with("default")
        mock_multi_cluster_manager.get_cluster_status.assert_called_with("default")
        # Should not call start_cluster since it's already running
        mock_multi_cluster_manager.start_cluster.assert_not_called()
        assert compatibility_manager._default_cluster_initialized is True
    
    @pytest.mark.asyncio
    async def test_ensure_default_cluster_handles_errors(
        self, compatibility_manager, mock_cluster_registry
    ):
        """Test error handling in ensure_default_cluster."""
        # Setup: Registry throws exception
        mock_cluster_registry.get_cluster.side_effect = Exception("Registry error")
        
        # Execute
        result = await compatibility_manager.ensure_default_cluster()
        
        # Verify
        assert result is False
        assert compatibility_manager._default_cluster_initialized is False
    
    @pytest.mark.asyncio
    async def test_get_default_cluster_id_success(self, compatibility_manager):
        """Test successful retrieval of default cluster ID."""
        # Setup: Mock successful cluster initialization
        compatibility_manager._default_cluster_initialized = True
        
        # Execute
        cluster_id = await compatibility_manager.get_default_cluster_id()
        
        # Verify
        assert cluster_id == "default"
    
    @pytest.mark.asyncio
    async def test_get_default_cluster_id_failure(self, compatibility_manager, mock_cluster_registry):
        """Test failure to get default cluster ID."""
        # Setup: Mock cluster initialization failure
        mock_cluster_registry.get_cluster.side_effect = Exception("Registry error")
        
        # Execute & Verify
        with pytest.raises(HTTPException) as exc_info:
            await compatibility_manager.get_default_cluster_id()
        
        assert exc_info.value.status_code == 503
        assert "Default cluster is not available" in exc_info.value.detail
    
    @pytest.mark.asyncio
    async def test_migrate_single_cluster_config(
        self, compatibility_manager, mock_template_manager, mock_cluster_registry, mock_multi_cluster_manager
    ):
        """Test migration of single-cluster configuration."""
        # Setup
        config_data = {
            "environment": "production",
            "kafka": {"heap_size": "2G"},
            "rest_proxy": {"heap_size": "1G"},
            "ui": {"readonly_mode": True}
        }
        
        # Execute
        result = await compatibility_manager.migrate_single_cluster_config(config_data)
        
        # Verify
        assert result is True
        mock_template_manager.apply_template.assert_called_once()
        mock_cluster_registry.register_cluster.assert_called_once()
        mock_multi_cluster_manager.create_cluster.assert_called_once()
        
        # Verify template override parameters
        call_args = mock_template_manager.apply_template.call_args
        overrides = call_args[1]["overrides"]
        assert overrides["environment"] == "production"
        assert overrides["kafka_config"] == {"heap_size": "2G"}
        assert "migrated" in overrides["tags"]
    
    @pytest.mark.asyncio
    async def test_migrate_single_cluster_config_failure(
        self, compatibility_manager, mock_template_manager
    ):
        """Test migration failure handling."""
        # Setup: Template manager throws exception
        mock_template_manager.get_template.side_effect = Exception("Template error")
        
        # Execute
        result = await compatibility_manager.migrate_single_cluster_config({})
        
        # Verify
        assert result is False
    
    def test_is_legacy_endpoint(self, compatibility_manager):
        """Test legacy endpoint detection."""
        # Test legacy endpoints
        assert compatibility_manager.is_legacy_endpoint("/topics") is True
        assert compatibility_manager.is_legacy_endpoint("/produce") is True
        assert compatibility_manager.is_legacy_endpoint("/consume") is True
        assert compatibility_manager.is_legacy_endpoint("/consumer-groups") is True
        assert compatibility_manager.is_legacy_endpoint("/brokers") is True
        assert compatibility_manager.is_legacy_endpoint("/cluster-info") is True
        assert compatibility_manager.is_legacy_endpoint("/health") is True
        
        # Test non-legacy endpoints
        assert compatibility_manager.is_legacy_endpoint("/clusters/test/topics") is False
        assert compatibility_manager.is_legacy_endpoint("/templates") is False
        assert compatibility_manager.is_legacy_endpoint("/operations/migrate") is False
        assert compatibility_manager.is_legacy_endpoint("/unknown") is False
    
    def test_convert_legacy_path_to_cluster_path(self, compatibility_manager):
        """Test conversion of legacy paths to cluster-specific paths."""
        # Test various path conversions
        assert compatibility_manager.convert_legacy_path_to_cluster_path(
            "/topics", "test-cluster"
        ) == "/clusters/test-cluster/topics"
        
        assert compatibility_manager.convert_legacy_path_to_cluster_path(
            "/produce", "default"
        ) == "/clusters/default/produce"
        
        assert compatibility_manager.convert_legacy_path_to_cluster_path(
            "health", "prod"
        ) == "/clusters/prod/health"
    
    @pytest.mark.asyncio
    async def test_handle_legacy_request_success(self, compatibility_manager):
        """Test successful handling of legacy request."""
        # Setup
        compatibility_manager._default_cluster_initialized = True
        request = Mock()
        request.url.path = "/topics"
        
        # Execute
        cluster_id = await compatibility_manager.handle_legacy_request(request)
        
        # Verify
        assert cluster_id == "default"
    
    @pytest.mark.asyncio
    async def test_handle_legacy_request_non_legacy(self, compatibility_manager):
        """Test handling of non-legacy request."""
        # Setup
        request = Mock()
        request.url.path = "/clusters/test/topics"
        
        # Execute
        cluster_id = await compatibility_manager.handle_legacy_request(request)
        
        # Verify
        assert cluster_id is None
    
    @pytest.mark.asyncio
    async def test_handle_legacy_request_cluster_unavailable(
        self, compatibility_manager, mock_cluster_registry
    ):
        """Test handling of legacy request when default cluster is unavailable."""
        # Setup: Registry throws exception
        mock_cluster_registry.get_cluster.side_effect = Exception("Registry error")
        request = Mock()
        request.url.path = "/topics"
        
        # Execute & Verify
        with pytest.raises(HTTPException):
            await compatibility_manager.handle_legacy_request(request)
    
    @pytest.mark.asyncio
    async def test_get_cluster_endpoints_info(self, compatibility_manager, mock_cluster_registry):
        """Test retrieval of cluster endpoint information."""
        # Setup
        port_allocation = PortAllocation(
            kafka_port=9092,
            rest_proxy_port=8082,
            ui_port=8080,
            jmx_port=9999
        )
        
        cluster = Mock()
        cluster.port_allocation = port_allocation
        mock_cluster_registry.get_cluster.return_value = cluster
        
        # Execute
        endpoints = await compatibility_manager.get_cluster_endpoints_info("test-cluster")
        
        # Verify
        expected_endpoints = {
            "kafka": "http://localhost:9092",
            "rest_proxy": "http://localhost:8082",
            "ui": "http://localhost:8080",
            "jmx": "http://localhost:9999"
        }
        assert endpoints == expected_endpoints
    
    @pytest.mark.asyncio
    async def test_get_cluster_endpoints_info_cluster_not_found(
        self, compatibility_manager, mock_cluster_registry
    ):
        """Test endpoint info retrieval for non-existent cluster."""
        # Setup
        mock_cluster_registry.get_cluster.return_value = None
        
        # Execute
        endpoints = await compatibility_manager.get_cluster_endpoints_info("nonexistent")
        
        # Verify
        assert endpoints == {}
    
    @pytest.mark.asyncio
    async def test_get_compatibility_status(
        self, compatibility_manager, mock_cluster_registry, mock_multi_cluster_manager
    ):
        """Test retrieval of compatibility status."""
        # Setup
        cluster = Mock()
        cluster.name = "Default Cluster"
        mock_cluster_registry.get_cluster.return_value = cluster
        
        status = Mock()
        status.status = "running"
        mock_multi_cluster_manager.get_cluster_status.return_value = status
        
        compatibility_manager._default_cluster_initialized = True
        
        # Mock get_cluster_endpoints_info
        compatibility_manager.get_cluster_endpoints_info = AsyncMock(
            return_value={"kafka": "http://localhost:9092"}
        )
        
        # Execute
        status_info = await compatibility_manager.get_compatibility_status()
        
        # Verify
        assert status_info["backward_compatibility_enabled"] is True
        assert status_info["default_cluster_available"] is True
        assert status_info["default_cluster_initialized"] is True
        assert "default_cluster" in status_info
        assert "legacy_endpoints_supported" in status_info
    
    @pytest.mark.asyncio
    async def test_get_compatibility_status_error(
        self, compatibility_manager, mock_cluster_registry
    ):
        """Test compatibility status retrieval with error."""
        # Setup: Registry throws exception
        mock_cluster_registry.get_cluster.side_effect = Exception("Registry error")
        
        # Execute
        status_info = await compatibility_manager.get_compatibility_status()
        
        # Verify
        assert status_info["backward_compatibility_enabled"] is False
        assert "error" in status_info


class TestLegacyAPIMiddleware:
    """Test legacy API middleware functionality."""
    
    @pytest.fixture
    def mock_compatibility_manager(self):
        """Mock compatibility manager."""
        manager = Mock()
        manager.handle_legacy_request = AsyncMock(return_value=None)
        manager.convert_legacy_path_to_cluster_path = Mock(return_value="/clusters/default/topics")
        return manager
    
    @pytest.fixture
    def middleware(self, mock_compatibility_manager):
        """Create middleware with mocked compatibility manager."""
        return LegacyAPIMiddleware(mock_compatibility_manager)
    
    @pytest.mark.asyncio
    async def test_middleware_non_legacy_request(self, middleware, mock_compatibility_manager):
        """Test middleware handling of non-legacy request."""
        # Setup
        request = Mock()
        request.url.path = "/clusters/test/topics"
        request.state = Mock(spec=[])  # Empty spec to prevent automatic attribute creation
        request.scope = {"path": "/clusters/test/topics", "raw_path": b"/clusters/test/topics"}
        
        mock_compatibility_manager.handle_legacy_request.return_value = None
        
        # Mock call_next
        async def mock_call_next(req):
            return Mock(status_code=200)
        
        # Execute
        response = await middleware(request, mock_call_next)
        
        # Verify
        assert response.status_code == 200
        mock_compatibility_manager.handle_legacy_request.assert_called_once_with(request)
        # Should not modify request for non-legacy requests
        assert not hasattr(request.state, 'cluster_id')
    
    @pytest.mark.asyncio
    async def test_middleware_legacy_request(self, middleware, mock_compatibility_manager):
        """Test middleware handling of legacy request."""
        # Setup
        request = Mock()
        request.url.path = "/topics"
        request.state = Mock()
        request.scope = {"path": "/topics", "raw_path": b"/topics"}
        
        # Configure mock to return cluster ID for legacy request
        mock_compatibility_manager.handle_legacy_request.return_value = "default"
        mock_compatibility_manager.convert_legacy_path_to_cluster_path.return_value = "/clusters/default/topics"
        
        # Mock call_next
        async def mock_call_next(req):
            return Mock(status_code=200)
        
        # Execute
        response = await middleware(request, mock_call_next)
        
        # Verify
        assert response.status_code == 200
        mock_compatibility_manager.handle_legacy_request.assert_called_once_with(request)
        mock_compatibility_manager.convert_legacy_path_to_cluster_path.assert_called_once_with(
            "/topics", "default"
        )
        
        # Verify request state was modified
        assert request.state.cluster_id == "default"
        assert request.state.is_legacy_request is True
        assert request.scope["path"] == "/clusters/default/topics"
        assert request.scope["raw_path"] == b"/clusters/default/topics"
    
    @pytest.mark.asyncio
    async def test_middleware_http_exception(self, middleware, mock_compatibility_manager):
        """Test middleware handling of HTTP exceptions."""
        # Setup
        request = Mock()
        request.url.path = "/topics"
        
        http_exception = HTTPException(status_code=503, detail="Service unavailable")
        mock_compatibility_manager.handle_legacy_request.side_effect = http_exception
        
        # Execute & Verify
        with pytest.raises(HTTPException) as exc_info:
            await middleware(request, lambda req: Mock())
        
        assert exc_info.value.status_code == 503
        assert exc_info.value.detail == "Service unavailable"
    
    @pytest.mark.asyncio
    async def test_middleware_general_exception(self, middleware, mock_compatibility_manager):
        """Test middleware handling of general exceptions."""
        # Setup
        request = Mock()
        request.url.path = "/topics"
        
        mock_compatibility_manager.handle_legacy_request.side_effect = Exception("Unexpected error")
        
        # Execute
        response = await middleware(request, lambda req: Mock())
        
        # Verify
        assert isinstance(response, JSONResponse)
        assert response.status_code == 500
        
        # Check response content
        import json
        content = json.loads(response.body.decode())
        assert "error" in content
        assert "Internal server error" in content["error"]


class TestUtilityFunctions:
    """Test utility functions for backward compatibility."""
    
    @pytest.mark.asyncio
    async def test_ensure_backward_compatibility(self):
        """Test ensure_backward_compatibility function."""
        # Setup mocks
        mock_multi_cluster_manager = AsyncMock()
        mock_cluster_registry = AsyncMock()
        mock_template_manager = AsyncMock()
        
        # Mock the ensure_default_cluster method
        with patch.object(BackwardCompatibilityManager, 'ensure_default_cluster', new_callable=AsyncMock) as mock_ensure:
            mock_ensure.return_value = True
            
            # Execute
            compatibility_manager = await ensure_backward_compatibility(
                mock_multi_cluster_manager,
                mock_cluster_registry,
                mock_template_manager
            )
            
            # Verify
            assert isinstance(compatibility_manager, BackwardCompatibilityManager)
            mock_ensure.assert_called_once()
    
    def test_create_legacy_api_middleware(self):
        """Test create_legacy_api_middleware function."""
        # Setup
        mock_compatibility_manager = Mock()
        
        # Execute
        middleware = create_legacy_api_middleware(mock_compatibility_manager)
        
        # Verify
        assert isinstance(middleware, LegacyAPIMiddleware)
        assert middleware.compatibility_manager == mock_compatibility_manager


class TestIntegrationScenarios:
    """Test integration scenarios for backward compatibility."""
    
    @pytest.mark.asyncio
    async def test_full_legacy_request_flow(self):
        """Test complete flow of legacy request processing."""
        # This would be an integration test that tests the full flow
        # from receiving a legacy request to routing it to the default cluster
        pass
    
    @pytest.mark.asyncio
    async def test_migration_and_compatibility(self):
        """Test migration from single-cluster and subsequent compatibility."""
        # This would test the migration process and verify that
        # legacy APIs work correctly after migration
        pass
    
    @pytest.mark.asyncio
    async def test_concurrent_legacy_requests(self):
        """Test handling of concurrent legacy requests."""
        # This would test that the middleware can handle multiple
        # concurrent legacy requests without issues
        pass