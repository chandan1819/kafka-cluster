"""
Tests for web interface API routes.

This module tests the web interface API endpoints that provide data
and functionality for multi-cluster web interface consumption.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.web_interface_routes import router, init_web_interface_services
from src.services.multi_cluster_manager import MultiClusterManager
from src.services.multi_cluster_service_catalog import MultiClusterServiceCatalog
from src.services.template_manager import TemplateManager
from src.services.cross_cluster_operations import CrossClusterOperations


class TestWebInterfaceRoutes:
    """Test web interface API routes."""
    
    @pytest.fixture
    def app(self):
        """Create FastAPI app with web interface routes."""
        app = FastAPI()
        app.include_router(router)
        return app
    
    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app)
    
    @pytest.fixture
    def mock_multi_cluster_manager(self):
        """Mock multi-cluster manager."""
        manager = AsyncMock()
        
        # Mock cluster data
        mock_cluster = Mock()
        mock_cluster.id = "test-cluster"
        mock_cluster.name = "Test Cluster"
        mock_cluster.environment = "development"
        mock_cluster.status = Mock()
        mock_cluster.status.value = "running"
        mock_cluster.endpoints = {"ui": "http://localhost:8080", "kafka": "http://localhost:9092"}
        mock_cluster.created_at = datetime.now()
        mock_cluster.last_started = datetime.now()
        mock_cluster.tags = {"env": "test"}
        mock_cluster.description = "Test cluster"
        mock_cluster.template_id = "development"
        mock_cluster.updated_at = datetime.now()
        mock_cluster.last_stopped = None
        mock_cluster.kafka_config = Mock()
        mock_cluster.kafka_config.__dict__ = {"heap_size": "1G", "num_partitions": 3}
        mock_cluster.rest_proxy_config = Mock()
        mock_cluster.rest_proxy_config.__dict__ = {"heap_size": "512M"}
        mock_cluster.ui_config = Mock()
        mock_cluster.ui_config.__dict__ = {"readonly_mode": False}
        mock_cluster.port_allocation = Mock()
        mock_cluster.port_allocation.kafka_port = 9092
        mock_cluster.port_allocation.rest_proxy_port = 8082
        mock_cluster.port_allocation.ui_port = 8080
        mock_cluster.port_allocation.jmx_port = 9999
        
        manager.list_clusters.return_value = [mock_cluster]
        manager.registry.get_cluster.return_value = mock_cluster
        manager.get_cluster_health.return_value = {"overall_health": "healthy"}
        manager.get_cluster_status.return_value = Mock(value="running")
        manager.start_cluster.return_value = Mock(value="running")
        manager.stop_cluster.return_value = True
        
        return manager
    
    @pytest.fixture
    def mock_multi_cluster_catalog(self):
        """Mock multi-cluster service catalog."""
        catalog = AsyncMock()
        
        # Mock topics data
        mock_topics = [
            {"name": "test-topic-1", "partitions": 3, "replication_factor": 1},
            {"name": "test-topic-2", "partitions": 1, "replication_factor": 1}
        ]
        catalog.get_cluster_topics.return_value = mock_topics
        
        return catalog
    
    @pytest.fixture
    def mock_template_manager(self):
        """Mock template manager."""
        manager = AsyncMock()
        
        # Mock template data
        mock_template = Mock()
        mock_template.id = "development"
        mock_template.name = "Development"
        mock_template.description = "Development template"
        mock_template.category = "development"
        mock_template.is_builtin = True
        mock_template.tags = ["dev", "local"]
        mock_template.min_memory_mb = 512
        mock_template.min_disk_gb = 1
        mock_template.recommended_memory_mb = 1024
        mock_template.recommended_disk_gb = 5
        mock_template.default_kafka_config = Mock()
        mock_template.default_kafka_config.__dict__ = {"heap_size": "1G"}
        mock_template.default_rest_proxy_config = Mock()
        mock_template.default_rest_proxy_config.__dict__ = {"heap_size": "512M"}
        mock_template.default_ui_config = Mock()
        mock_template.default_ui_config.__dict__ = {"readonly_mode": False}
        
        manager.list_templates.return_value = [mock_template]
        
        return manager
    
    @pytest.fixture
    def mock_cross_cluster_ops(self):
        """Mock cross-cluster operations."""
        ops = AsyncMock()
        
        # Mock operation data
        mock_operation = Mock()
        mock_operation.id = "op-123"
        mock_operation.operation_type = "migrate"
        mock_operation.status = Mock()
        mock_operation.status.value = "completed"
        mock_operation.progress_percent = 100.0
        mock_operation.started_at = datetime.now()
        mock_operation.completed_at = datetime.now()
        mock_operation.source_cluster_id = "source-cluster"
        mock_operation.target_cluster_ids = ["target-cluster"]
        mock_operation.error_message = None
        mock_operation.topics = ["test-topic"]
        
        ops.list_operations.return_value = [mock_operation]
        
        return ops
    
    @pytest.fixture
    def initialized_services(self, mock_multi_cluster_manager, mock_multi_cluster_catalog, 
                           mock_template_manager, mock_cross_cluster_ops):
        """Initialize web interface services with mocks."""
        init_web_interface_services(
            mock_multi_cluster_manager,
            mock_multi_cluster_catalog,
            mock_template_manager,
            mock_cross_cluster_ops
        )
        return {
            "manager": mock_multi_cluster_manager,
            "catalog": mock_multi_cluster_catalog,
            "templates": mock_template_manager,
            "operations": mock_cross_cluster_ops
        }
    
    @pytest.mark.asyncio
    async def test_get_dashboard_data(self, client, initialized_services):
        """Test dashboard data endpoint."""
        response = client.get("/web/dashboard")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check structure
        assert "summary" in data
        assert "clusters" in data
        assert "recent_operations" in data
        assert "timestamp" in data
        
        # Check summary
        summary = data["summary"]
        assert summary["total_clusters"] == 1
        assert summary["total_topics"] == 2
        assert "status_distribution" in summary
        
        # Check clusters
        clusters = data["clusters"]
        assert len(clusters) == 1
        cluster = clusters[0]
        assert cluster["id"] == "test-cluster"
        assert cluster["name"] == "Test Cluster"
        assert cluster["status"] == "running"
        assert cluster["topic_count"] == 2
    
    @pytest.mark.asyncio
    async def test_get_clusters_overview(self, client, initialized_services):
        """Test clusters overview endpoint."""
        response = client.get("/web/clusters/overview")
        
        assert response.status_code == 200
        data = response.json()
        
        assert len(data) == 1
        cluster = data[0]
        assert cluster["id"] == "test-cluster"
        assert cluster["name"] == "Test Cluster"
        assert cluster["environment"] == "development"
        assert cluster["status"] == "running"
        assert cluster["topic_count"] == 2
        assert "endpoints" in cluster
        assert "tags" in cluster
    
    @pytest.mark.asyncio
    async def test_get_cluster_details(self, client, initialized_services):
        """Test cluster details endpoint."""
        response = client.get("/web/clusters/test-cluster/details")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check structure
        assert "cluster" in data
        assert "configuration" in data
        assert "health" in data
        assert "topics" in data
        assert "timestamp" in data
        
        # Check cluster info
        cluster = data["cluster"]
        assert cluster["id"] == "test-cluster"
        assert cluster["name"] == "Test Cluster"
        assert cluster["status"] == "running"
        
        # Check configuration
        config = data["configuration"]
        assert "kafka_config" in config
        assert "rest_proxy_config" in config
        assert "ui_config" in config
        assert "port_allocation" in config
        
        # Check topics
        topics = data["topics"]
        assert topics["count"] == 2
        assert len(topics["list"]) == 2
    
    @pytest.mark.asyncio
    async def test_get_cluster_details_not_found(self, client, initialized_services):
        """Test cluster details endpoint with non-existent cluster."""
        # Mock cluster not found
        initialized_services["manager"].registry.get_cluster.side_effect = Exception("Cluster not found")
        
        response = client.get("/web/clusters/nonexistent/details")
        
        assert response.status_code == 500  # Internal server error due to exception
    
    @pytest.mark.asyncio
    async def test_perform_cluster_action_start(self, client, initialized_services):
        """Test cluster start action."""
        response = client.post("/web/clusters/test-cluster/actions/start")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["cluster_id"] == "test-cluster"
        assert data["action"] == "start"
        assert data["status"] == "running"
        assert "message" in data
        assert "timestamp" in data
        
        # Verify manager was called
        initialized_services["manager"].start_cluster.assert_called_once_with(
            "test-cluster", force=False, timeout=60
        )
    
    @pytest.mark.asyncio
    async def test_perform_cluster_action_stop(self, client, initialized_services):
        """Test cluster stop action."""
        response = client.post("/web/clusters/test-cluster/actions/stop")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["cluster_id"] == "test-cluster"
        assert data["action"] == "stop"
        assert data["success"] is True
        assert "message" in data
        
        # Verify manager was called
        initialized_services["manager"].stop_cluster.assert_called_once_with(
            "test-cluster", force=False, timeout=60
        )
    
    @pytest.mark.asyncio
    async def test_perform_cluster_action_restart(self, client, initialized_services):
        """Test cluster restart action."""
        response = client.post("/web/clusters/test-cluster/actions/restart")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["cluster_id"] == "test-cluster"
        assert data["action"] == "restart"
        assert data["status"] == "running"
        assert "message" in data
        
        # Verify manager was called for both stop and start
        initialized_services["manager"].stop_cluster.assert_called_once()
        initialized_services["manager"].start_cluster.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_perform_cluster_action_invalid(self, client, initialized_services):
        """Test invalid cluster action."""
        response = client.post("/web/clusters/test-cluster/actions/invalid")
        
        # The HTTPException should be properly handled
        assert response.status_code == 400
        data = response.json()
        assert "Invalid action" in data["detail"]
    
    @pytest.mark.asyncio
    async def test_get_templates_for_creation(self, client, initialized_services):
        """Test templates for creation endpoint."""
        response = client.get("/web/templates/for-creation")
        
        assert response.status_code == 200
        data = response.json()
        
        assert len(data) == 1
        template = data[0]
        assert template["id"] == "development"
        assert template["name"] == "Development"
        assert template["category"] == "development"
        assert template["is_builtin"] is True
        assert "resource_requirements" in template
        assert "default_config" in template
        
        # Check resource requirements
        resources = template["resource_requirements"]
        assert resources["min_memory_mb"] == 512
        assert resources["recommended_memory_mb"] == 1024
    
    @pytest.mark.asyncio
    async def test_create_cluster_from_template(self, client, initialized_services):
        """Test cluster creation from template."""
        # Mock cluster creation
        mock_created_cluster = Mock()
        mock_created_cluster.id = "new-cluster"
        mock_created_cluster.name = "New Cluster"
        mock_created_cluster.environment = "development"
        mock_created_cluster.endpoints = {"ui": "http://localhost:8081"}
        mock_created_cluster.created_at = datetime.now()
        
        initialized_services["manager"].create_cluster.return_value = mock_created_cluster
        
        request_data = {
            "name": "New Cluster",
            "description": "A new test cluster",
            "template_id": "development",
            "environment": "development",
            "tags": {"purpose": "testing"},
            "auto_start": True
        }
        
        response = client.post("/web/clusters/create-from-template", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["success"] is True
        assert "cluster" in data
        assert "message" in data
        assert "next_steps" in data
        
        cluster = data["cluster"]
        assert cluster["id"] == "new-cluster"
        assert cluster["name"] == "New Cluster"
        
        # Verify manager was called
        initialized_services["manager"].create_cluster.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_operations_dashboard(self, client, initialized_services):
        """Test operations dashboard endpoint."""
        response = client.get("/web/operations/dashboard")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check structure
        assert "summary" in data
        assert "running_operations" in data
        assert "recent_completed" in data
        assert "recent_failed" in data
        assert "timestamp" in data
        
        # Check summary
        summary = data["summary"]
        assert summary["total_operations"] == 1
        assert summary["completed"] == 1
        assert summary["running"] == 0
        assert summary["failed"] == 0
        
        # Check completed operations
        completed = data["recent_completed"]
        assert len(completed) == 1
        operation = completed[0]
        assert operation["id"] == "op-123"
        assert operation["type"] == "migrate"
        assert operation["status"] == "completed"
        assert operation["progress"] == 100.0
    
    @pytest.mark.asyncio
    async def test_get_system_health(self, client, initialized_services):
        """Test system health endpoint."""
        response = client.get("/web/system/health")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check structure
        assert "overall_status" in data
        assert "total_clusters" in data
        assert "healthy_clusters" in data
        assert "unhealthy_clusters" in data
        assert "cluster_health" in data
        assert "timestamp" in data
        
        # Check values
        assert data["overall_status"] == "healthy"
        assert data["total_clusters"] == 1
        assert data["healthy_clusters"] == 1
        assert data["unhealthy_clusters"] == 0
        
        # Check cluster health
        cluster_health = data["cluster_health"]
        assert "test-cluster" in cluster_health
        assert cluster_health["test-cluster"]["status"] == "healthy"
    
    @pytest.mark.asyncio
    async def test_get_cluster_environments(self, client, initialized_services):
        """Test cluster environments endpoint."""
        response = client.get("/web/settings/cluster-environments")
        
        assert response.status_code == 200
        data = response.json()
        
        expected_environments = ["development", "testing", "staging", "production"]
        assert data == expected_environments
    
    @pytest.mark.asyncio
    async def test_get_port_ranges(self, client, initialized_services):
        """Test port ranges endpoint."""
        response = client.get("/web/settings/port-ranges")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check structure
        assert "kafka_port_range" in data
        assert "rest_proxy_port_range" in data
        assert "ui_port_range" in data
        assert "jmx_port_range" in data
        assert "allocated_ports" in data
        assert "available_ports" in data
        assert "timestamp" in data
        
        # Check port ranges
        kafka_range = data["kafka_port_range"]
        assert kafka_range["start"] == 9092
        assert kafka_range["end"] == 9199


class TestWebInterfaceErrorHandling:
    """Test error handling in web interface routes."""
    
    @pytest.fixture
    def app(self):
        """Create FastAPI app with web interface routes."""
        app = FastAPI()
        app.include_router(router)
        return app
    
    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app)
    
    @pytest.fixture
    def failing_services(self):
        """Initialize web interface services with failing mocks."""
        manager = AsyncMock()
        catalog = AsyncMock()
        templates = AsyncMock()
        operations = AsyncMock()
        
        # Make services fail
        manager.list_clusters.side_effect = Exception("Service unavailable")
        catalog.get_cluster_topics.side_effect = Exception("Catalog error")
        templates.list_templates.side_effect = Exception("Template error")
        operations.list_operations.side_effect = Exception("Operations error")
        
        init_web_interface_services(manager, catalog, templates, operations)
        
        return {
            "manager": manager,
            "catalog": catalog,
            "templates": templates,
            "operations": operations
        }
    
    @pytest.mark.asyncio
    async def test_dashboard_data_error_handling(self, client, failing_services):
        """Test dashboard data endpoint error handling."""
        response = client.get("/web/dashboard")
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to get dashboard data" in data["detail"]
    
    @pytest.mark.asyncio
    async def test_clusters_overview_error_handling(self, client, failing_services):
        """Test clusters overview endpoint error handling."""
        response = client.get("/web/clusters/overview")
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to get clusters overview" in data["detail"]
    
    @pytest.mark.asyncio
    async def test_templates_error_handling(self, client, failing_services):
        """Test templates endpoint error handling."""
        response = client.get("/web/templates/for-creation")
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to get templates" in data["detail"]
    
    @pytest.mark.asyncio
    async def test_operations_dashboard_error_handling(self, client, failing_services):
        """Test operations dashboard endpoint error handling."""
        response = client.get("/web/operations/dashboard")
        
        assert response.status_code == 500
        data = response.json()
        assert "Failed to get operations dashboard" in data["detail"]


class TestWebInterfaceIntegration:
    """Integration tests for web interface routes."""
    
    @pytest.mark.asyncio
    async def test_full_cluster_lifecycle_via_web_interface(self):
        """Test complete cluster lifecycle through web interface endpoints."""
        # This would be an integration test that tests the full flow
        # of creating, managing, and deleting clusters via web interface
        pass
    
    @pytest.mark.asyncio
    async def test_dashboard_real_time_updates(self):
        """Test that dashboard data reflects real-time cluster changes."""
        # This would test that dashboard data updates when clusters change
        pass
    
    @pytest.mark.asyncio
    async def test_concurrent_web_interface_requests(self):
        """Test handling of concurrent web interface requests."""
        # This would test that the web interface can handle multiple
        # concurrent requests without issues
        pass