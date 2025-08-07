"""
Tests for multi-cluster API endpoints.
"""

import pytest
import asyncio
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch
from fastapi.testclient import TestClient
from fastapi import FastAPI

from src.api.multi_cluster_routes import router, init_multi_cluster_services
from src.services.multi_cluster_manager import MultiClusterManager
from src.services.multi_cluster_service_catalog import MultiClusterServiceCatalog
from src.services.template_manager import TemplateManager
from src.services.cross_cluster_operations import CrossClusterOperations
from src.models.multi_cluster import (
    ClusterDefinition,
    ClusterSummary,
    ClusterTemplate,
    CrossClusterOperation,
    OperationStatus,
    ClusterEnvironment,
    TemplateCategory
)
from src.models.base import ServiceStatus
from src.models.topic import TopicInfo
from src.registry.exceptions import ClusterNotFoundError, ClusterAlreadyExistsError


@pytest.fixture
def mock_multi_cluster_manager():
    """Mock multi-cluster manager."""
    manager = AsyncMock(spec=MultiClusterManager)
    manager.registry = AsyncMock()
    manager.list_clusters = AsyncMock(return_value=[])
    manager.create_cluster = AsyncMock()
    manager.start_cluster = AsyncMock(return_value=ServiceStatus.RUNNING)
    manager.stop_cluster = AsyncMock(return_value=True)
    manager.delete_cluster = AsyncMock(return_value=True)
    manager.get_cluster_status = AsyncMock(return_value=ServiceStatus.RUNNING)
    manager.get_cluster_health = AsyncMock(return_value={"overall_health": "healthy"})
    manager.get_all_cluster_status = AsyncMock(return_value={})
    manager.get_manager_stats = AsyncMock(return_value={"total_clusters": 0})
    manager.start_multiple_clusters = AsyncMock(return_value={})
    manager.stop_multiple_clusters = AsyncMock(return_value={})
    return manager


@pytest.fixture
def mock_multi_cluster_catalog():
    """Mock multi-cluster service catalog."""
    catalog = AsyncMock(spec=MultiClusterServiceCatalog)
    catalog.get_aggregated_catalog = AsyncMock(return_value={"total_clusters": 0})
    catalog.get_cluster_topics = AsyncMock(return_value=[])
    catalog.create_cluster_topic_manager = AsyncMock()
    catalog.create_cluster_message_manager = AsyncMock()
    return catalog


@pytest.fixture
def mock_template_manager():
    """Mock template manager."""
    manager = AsyncMock(spec=TemplateManager)
    manager.list_templates = AsyncMock(return_value=[])
    manager.create_template = AsyncMock()
    manager.get_template = AsyncMock()
    manager.delete_template = AsyncMock(return_value=True)
    return manager


@pytest.fixture
def mock_cross_cluster_ops():
    """Mock cross-cluster operations."""
    ops = AsyncMock(spec=CrossClusterOperations)
    ops.migrate_topics = AsyncMock(return_value="operation-123")
    ops.replicate_topics = AsyncMock(return_value="operation-456")
    ops.compare_clusters = AsyncMock()
    ops.list_operations = AsyncMock(return_value=[])
    ops.get_operation_status = AsyncMock()
    ops.cancel_operation = AsyncMock(return_value=True)
    return ops


@pytest.fixture
def app(mock_multi_cluster_manager, mock_multi_cluster_catalog, 
        mock_template_manager, mock_cross_cluster_ops):
    """FastAPI test application."""
    app = FastAPI()
    app.include_router(router)
    
    # Initialize services
    init_multi_cluster_services(
        mock_multi_cluster_manager,
        mock_multi_cluster_catalog,
        mock_template_manager,
        mock_cross_cluster_ops
    )
    
    return app


@pytest.fixture
def client(app):
    """Test client."""
    return TestClient(app)


@pytest.fixture
def sample_cluster_definition():
    """Sample cluster definition."""
    return ClusterDefinition(
        id="test-cluster",
        name="Test Cluster",
        environment=ClusterEnvironment.DEVELOPMENT,
        description="Test cluster for API testing"
    )


@pytest.fixture
def sample_cluster_summary():
    """Sample cluster summary."""
    return ClusterSummary(
        id="test-cluster",
        name="Test Cluster",
        environment=ClusterEnvironment.DEVELOPMENT,
        status=ServiceStatus.RUNNING,
        created_at=datetime.utcnow(),
        endpoints={"kafka": "localhost:9092"},
        tags={"env": "test"}
    )


@pytest.fixture
def sample_template():
    """Sample cluster template."""
    return ClusterTemplate(
        id="dev-template",
        name="Development Template",
        description="Template for development clusters",
        category=TemplateCategory.DEVELOPMENT
    )


class TestClusterRegistryEndpoints:
    """Test cluster registry management endpoints."""
    
    def test_list_clusters_empty(self, client, mock_multi_cluster_manager):
        """Test listing clusters when none exist."""
        mock_multi_cluster_manager.list_clusters.return_value = []
        
        response = client.get("/multi-cluster/clusters")
        
        assert response.status_code == 200
        assert response.json() == []
        mock_multi_cluster_manager.list_clusters.assert_called_once()
    
    def test_list_clusters_with_data(self, client, mock_multi_cluster_manager, sample_cluster_summary):
        """Test listing clusters with data."""
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster_summary]
        
        response = client.get("/multi-cluster/clusters")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == "test-cluster"
        assert data[0]["name"] == "Test Cluster"
    
    def test_list_clusters_with_filters(self, client, mock_multi_cluster_manager):
        """Test listing clusters with filters."""
        response = client.get("/multi-cluster/clusters?status_filter=running&environment_filter=development")
        
        assert response.status_code == 200
        mock_multi_cluster_manager.list_clusters.assert_called_once_with(
            status_filter="running",
            environment_filter="development",
            tag_filter=None
        )
    
    def test_create_cluster_success(self, client, mock_multi_cluster_manager, sample_cluster_definition):
        """Test successful cluster creation."""
        mock_multi_cluster_manager.create_cluster.return_value = sample_cluster_definition
        
        request_data = {
            "name": "Test Cluster",
            "environment": "development",
            "description": "Test cluster",
            "auto_start": True,
            "tags": {"env": "test"}
        }
        
        response = client.post("/multi-cluster/clusters", json=request_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["id"] == "test-cluster"
        assert data["name"] == "Test Cluster"
        mock_multi_cluster_manager.create_cluster.assert_called_once()
    
    def test_create_cluster_already_exists(self, client, mock_multi_cluster_manager):
        """Test creating cluster that already exists."""
        mock_multi_cluster_manager.create_cluster.side_effect = ClusterAlreadyExistsError("test-cluster")
        
        request_data = {
            "name": "Test Cluster",
            "environment": "development"
        }
        
        response = client.post("/multi-cluster/clusters", json=request_data)
        
        assert response.status_code == 409
        assert "already exists" in response.json()["detail"]
    
    def test_get_cluster_success(self, client, mock_multi_cluster_manager, sample_cluster_definition):
        """Test getting cluster details."""
        mock_multi_cluster_manager.registry.get_cluster.return_value = sample_cluster_definition
        
        response = client.get("/multi-cluster/clusters/test-cluster")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "test-cluster"
        mock_multi_cluster_manager.registry.get_cluster.assert_called_once_with("test-cluster")
    
    def test_get_cluster_not_found(self, client, mock_multi_cluster_manager):
        """Test getting non-existent cluster."""
        mock_multi_cluster_manager.registry.get_cluster.side_effect = ClusterNotFoundError("test-cluster", [])
        
        response = client.get("/multi-cluster/clusters/test-cluster")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    def test_update_cluster_success(self, client, mock_multi_cluster_manager, sample_cluster_definition):
        """Test updating cluster configuration."""
        mock_multi_cluster_manager.registry.update_cluster.return_value = sample_cluster_definition
        
        update_data = {
            "name": "Updated Cluster Name",
            "description": "Updated description"
        }
        
        response = client.put("/multi-cluster/clusters/test-cluster", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "test-cluster"
        mock_multi_cluster_manager.registry.update_cluster.assert_called_once()
    
    def test_delete_cluster_success(self, client, mock_multi_cluster_manager):
        """Test deleting cluster."""
        mock_multi_cluster_manager.delete_cluster.return_value = True
        
        response = client.delete("/multi-cluster/clusters/test-cluster?force=true")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        mock_multi_cluster_manager.delete_cluster.assert_called_once_with(
            "test-cluster", force=True, cleanup_data=True
        )


class TestClusterLifecycleEndpoints:
    """Test cluster lifecycle management endpoints."""
    
    def test_start_cluster_success(self, client, mock_multi_cluster_manager):
        """Test starting cluster."""
        mock_multi_cluster_manager.start_cluster.return_value = ServiceStatus.RUNNING
        
        response = client.post("/multi-cluster/clusters/test-cluster/start?force=true&timeout=120")
        
        assert response.status_code == 200
        data = response.json()
        assert data["cluster_id"] == "test-cluster"
        assert data["status"] == "running"
        mock_multi_cluster_manager.start_cluster.assert_called_once_with(
            "test-cluster", force=True, timeout=120
        )
    
    def test_stop_cluster_success(self, client, mock_multi_cluster_manager):
        """Test stopping cluster."""
        mock_multi_cluster_manager.stop_cluster.return_value = True
        
        response = client.post("/multi-cluster/clusters/test-cluster/stop?force=true&cleanup=true")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        mock_multi_cluster_manager.stop_cluster.assert_called_once_with(
            "test-cluster", force=True, cleanup=True, timeout=30
        )
    
    def test_get_cluster_status(self, client, mock_multi_cluster_manager):
        """Test getting cluster status."""
        mock_multi_cluster_manager.get_cluster_status.return_value = ServiceStatus.RUNNING
        
        response = client.get("/multi-cluster/clusters/test-cluster/status")
        
        assert response.status_code == 200
        data = response.json()
        assert data["cluster_id"] == "test-cluster"
        assert data["status"] == "running"
    
    def test_get_cluster_health(self, client, mock_multi_cluster_manager):
        """Test getting cluster health."""
        health_data = {"overall_health": "healthy", "services": {"kafka": "running"}}
        mock_multi_cluster_manager.get_cluster_health.return_value = health_data
        
        response = client.get("/multi-cluster/clusters/test-cluster/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["overall_health"] == "healthy"


class TestTopicManagementEndpoints:
    """Test cluster-specific topic management endpoints."""
    
    def test_list_cluster_topics(self, client, mock_multi_cluster_catalog):
        """Test listing topics in a cluster."""
        topics = [
            {"name": "topic1", "partitions": 3, "cluster_id": "test-cluster"},
            {"name": "__internal", "partitions": 1, "cluster_id": "test-cluster"}
        ]
        mock_multi_cluster_catalog.get_cluster_topics.return_value = topics
        
        response = client.get("/multi-cluster/clusters/test-cluster/topics")
        
        assert response.status_code == 200
        data = response.json()
        # Should filter out internal topics by default
        assert len(data) == 1
        assert data[0]["name"] == "topic1"
    
    def test_list_cluster_topics_include_internal(self, client, mock_multi_cluster_catalog):
        """Test listing topics including internal ones."""
        topics = [
            {"name": "topic1", "partitions": 3, "cluster_id": "test-cluster"},
            {"name": "__internal", "partitions": 1, "cluster_id": "test-cluster"}
        ]
        mock_multi_cluster_catalog.get_cluster_topics.return_value = topics
        
        response = client.get("/multi-cluster/clusters/test-cluster/topics?include_internal=true")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
    
    def test_create_cluster_topic(self, client, mock_multi_cluster_catalog):
        """Test creating topic in a cluster."""
        mock_topic_manager = AsyncMock()
        mock_topic_manager.create_topic.return_value = True
        mock_topic_manager.close = Mock()
        mock_multi_cluster_catalog.create_cluster_topic_manager.return_value = mock_topic_manager
        
        topic_data = {
            "name": "test-topic",
            "partitions": 3,
            "replication_factor": 1,
            "config": {"retention.ms": "86400000"}
        }
        
        response = client.post("/multi-cluster/clusters/test-cluster/topics", json=topic_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["topic_name"] == "test-topic"
        assert data["success"] is True
    
    def test_delete_cluster_topic(self, client, mock_multi_cluster_catalog):
        """Test deleting topic from a cluster."""
        mock_topic_manager = AsyncMock()
        mock_topic_manager.delete_topic.return_value = True
        mock_topic_manager.close = Mock()
        mock_multi_cluster_catalog.create_cluster_topic_manager.return_value = mock_topic_manager
        
        response = client.delete("/multi-cluster/clusters/test-cluster/topics/test-topic?force=true")
        
        assert response.status_code == 200
        data = response.json()
        assert data["topic_name"] == "test-topic"
        assert data["success"] is True


class TestMessageOperationEndpoints:
    """Test cluster-specific message operation endpoints."""
    
    def test_produce_message_to_cluster(self, client, mock_multi_cluster_catalog):
        """Test producing message to a cluster."""
        from src.models.message import ProduceResult
        
        mock_message_manager = AsyncMock()
        mock_result = ProduceResult(
            topic="test-topic",
            partition=0,
            offset=123,
            timestamp=1234567890
        )
        mock_message_manager.produce_message.return_value = mock_result
        mock_message_manager.close = AsyncMock()
        mock_multi_cluster_catalog.create_cluster_message_manager.return_value = mock_message_manager
        
        message_data = {
            "topic": "test-topic",
            "key": "test-key",
            "value": {"message": "Hello World"}
        }
        
        response = client.post("/multi-cluster/clusters/test-cluster/produce", json=message_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["topic"] == "test-topic"
        assert data["partition"] == 0
        assert data["offset"] == 123
    
    def test_consume_messages_from_cluster(self, client, mock_multi_cluster_catalog):
        """Test consuming messages from a cluster."""
        from src.models.message import ConsumeResponse, Message
        
        mock_message_manager = AsyncMock()
        mock_message = Message(
            topic="test-topic",
            partition=0,
            offset=123,
            value={"message": "Hello World"},
            timestamp=1234567890
        )
        mock_response = ConsumeResponse(
            messages=[mock_message],
            consumer_group="test-group",
            topic="test-topic",
            total_consumed=1,
            has_more=False
        )
        mock_message_manager.consume_messages.return_value = mock_response
        mock_message_manager.close = AsyncMock()
        mock_multi_cluster_catalog.create_cluster_message_manager.return_value = mock_message_manager
        
        params = {
            "topic": "test-topic",
            "consumer_group": "test-group",
            "max_messages": 10,
            "timeout_ms": 5000
        }
        
        response = client.get("/multi-cluster/clusters/test-cluster/consume", params=params)
        
        assert response.status_code == 200
        data = response.json()
        assert data["topic"] == "test-topic"
        assert data["total_consumed"] == 1
        assert len(data["messages"]) == 1


class TestTemplateManagementEndpoints:
    """Test template management endpoints."""
    
    def test_list_templates(self, client, mock_template_manager, sample_template):
        """Test listing templates."""
        mock_template_manager.list_templates.return_value = [sample_template]
        
        response = client.get("/multi-cluster/templates")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == "dev-template"
    
    def test_list_templates_with_category_filter(self, client, mock_template_manager):
        """Test listing templates with category filter."""
        response = client.get("/multi-cluster/templates?category=development")
        
        assert response.status_code == 200
        mock_template_manager.list_templates.assert_called_once_with(category_filter="development")
    
    def test_create_template(self, client, mock_template_manager, sample_template):
        """Test creating template."""
        mock_template_manager.create_template.return_value = sample_template
        
        template_data = {
            "name": "Development Template",
            "description": "Template for development clusters",
            "category": "development",
            "min_memory_mb": 2048,
            "min_disk_gb": 5
        }
        
        response = client.post("/multi-cluster/templates", json=template_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Development Template"
    
    def test_get_template(self, client, mock_template_manager, sample_template):
        """Test getting template details."""
        mock_template_manager.get_template.return_value = sample_template
        
        response = client.get("/multi-cluster/templates/dev-template")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "dev-template"
    
    def test_delete_template(self, client, mock_template_manager):
        """Test deleting template."""
        mock_template_manager.delete_template.return_value = True
        
        response = client.delete("/multi-cluster/templates/dev-template")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True


class TestCrossClusterOperationEndpoints:
    """Test cross-cluster operation endpoints."""
    
    def test_start_migration(self, client, mock_cross_cluster_ops):
        """Test starting migration operation."""
        mock_cross_cluster_ops.migrate_topics.return_value = "operation-123"
        
        migration_data = {
            "source_cluster_id": "source-cluster",
            "target_cluster_id": "target-cluster",
            "topics": ["topic1", "topic2"],
            "preserve_timestamps": True,
            "dry_run": False
        }
        
        response = client.post("/multi-cluster/operations/migrate", json=migration_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["operation_id"] == "operation-123"
        assert data["operation_type"] == "migrate"
    
    def test_start_replication(self, client, mock_cross_cluster_ops):
        """Test starting replication operation."""
        mock_cross_cluster_ops.replicate_topics.return_value = "operation-456"
        
        replication_data = {
            "source_cluster_id": "source-cluster",
            "target_cluster_ids": ["target-cluster-1", "target-cluster-2"],
            "topics": ["topic1"],
            "mode": "mirror",
            "continuous": False
        }
        
        response = client.post("/multi-cluster/operations/replicate", json=replication_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["operation_id"] == "operation-456"
        assert data["operation_type"] == "replicate"
    
    def test_start_replication_invalid_mode(self, client, mock_cross_cluster_ops):
        """Test starting replication with invalid mode."""
        replication_data = {
            "source_cluster_id": "source-cluster",
            "target_cluster_ids": ["target-cluster"],
            "mode": "invalid-mode"
        }
        
        response = client.post("/multi-cluster/operations/replicate", json=replication_data)
        
        assert response.status_code == 400
        assert "Invalid replication mode" in response.json()["detail"]
    
    def test_compare_clusters(self, client, mock_cross_cluster_ops):
        """Test comparing clusters."""
        from src.models.multi_cluster import ClusterComparisonResult
        
        comparison_result = ClusterComparisonResult(
            source_cluster="source-cluster",
            target_cluster="target-cluster",
            topics_only_in_source=["topic1"],
            topics_only_in_target=["topic2"],
            topics_with_different_config=[],
            summary={"identical": False}
        )
        mock_cross_cluster_ops.compare_clusters.return_value = comparison_result
        
        comparison_data = {
            "source_cluster_id": "source-cluster",
            "target_cluster_id": "target-cluster",
            "include_data": True,
            "topics": ["topic1", "topic2"]
        }
        
        response = client.post("/multi-cluster/operations/compare", json=comparison_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["source_cluster"] == "source-cluster"
        assert data["target_cluster"] == "target-cluster"
    
    def test_list_operations(self, client, mock_cross_cluster_ops):
        """Test listing operations."""
        operation = CrossClusterOperation(
            id="operation-123",
            operation_type="migrate",
            source_cluster_id="source",
            target_cluster_ids=["target"],
            status=OperationStatus.RUNNING
        )
        mock_cross_cluster_ops.list_operations.return_value = [operation]
        
        response = client.get("/multi-cluster/operations")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == "operation-123"
    
    def test_list_operations_with_filters(self, client, mock_cross_cluster_ops):
        """Test listing operations with filters."""
        response = client.get("/multi-cluster/operations?status_filter=running&operation_type_filter=migrate")
        
        assert response.status_code == 200
        mock_cross_cluster_ops.list_operations.assert_called_once_with(
            status_filter=OperationStatus.RUNNING,
            operation_type_filter="migrate"
        )
    
    def test_get_operation_status(self, client, mock_cross_cluster_ops):
        """Test getting operation status."""
        operation = CrossClusterOperation(
            id="operation-123",
            operation_type="migrate",
            source_cluster_id="source",
            target_cluster_ids=["target"],
            status=OperationStatus.RUNNING,
            progress_percent=50.0
        )
        mock_cross_cluster_ops.get_operation_status.return_value = operation
        
        response = client.get("/multi-cluster/operations/operation-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "operation-123"
        assert data["progress_percent"] == 50.0
    
    def test_cancel_operation(self, client, mock_cross_cluster_ops):
        """Test cancelling operation."""
        mock_cross_cluster_ops.cancel_operation.return_value = True
        
        response = client.post("/multi-cluster/operations/operation-123/cancel")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True


class TestBatchOperationEndpoints:
    """Test multi-cluster batch operation endpoints."""
    
    def test_start_multiple_clusters(self, client, mock_multi_cluster_manager):
        """Test starting multiple clusters."""
        results = {
            "cluster1": ServiceStatus.RUNNING,
            "cluster2": ServiceStatus.RUNNING,
            "cluster3": ServiceStatus.ERROR
        }
        mock_multi_cluster_manager.start_multiple_clusters.return_value = results
        
        request_data = {
            "cluster_ids": ["cluster1", "cluster2", "cluster3"],
            "max_concurrent": 2,
            "timeout": 120
        }
        
        response = client.post("/multi-cluster/operations/start-multiple", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_clusters"] == 3
        assert data["successful"] == 2
        assert data["failed"] == 1
    
    def test_stop_multiple_clusters(self, client, mock_multi_cluster_manager):
        """Test stopping multiple clusters."""
        results = {
            "cluster1": True,
            "cluster2": True,
            "cluster3": False
        }
        mock_multi_cluster_manager.stop_multiple_clusters.return_value = results
        
        request_data = {
            "cluster_ids": ["cluster1", "cluster2", "cluster3"],
            "max_concurrent": 3,
            "force": True
        }
        
        response = client.post("/multi-cluster/operations/stop-multiple", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_clusters"] == 3
        assert data["successful"] == 2
        assert data["failed"] == 1


class TestServiceCatalogEndpoints:
    """Test service catalog and health endpoints."""
    
    def test_get_multi_cluster_catalog(self, client, mock_multi_cluster_catalog):
        """Test getting multi-cluster catalog."""
        catalog_data = {
            "total_clusters": 2,
            "active_clusters": 1,
            "clusters": {"cluster1": {"status": "running"}},
            "aggregated_stats": {"total_topics": 5}
        }
        mock_multi_cluster_catalog.get_aggregated_catalog.return_value = catalog_data
        
        response = client.get("/multi-cluster/catalog?force_refresh=true")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_clusters"] == 2
        assert data["active_clusters"] == 1
    
    def test_get_multi_cluster_health(self, client, mock_multi_cluster_manager):
        """Test getting multi-cluster health."""
        cluster_status = {
            "cluster1": ServiceStatus.RUNNING,
            "cluster2": ServiceStatus.ERROR
        }
        manager_stats = {"total_clusters": 2, "active_managers": 1}
        
        mock_multi_cluster_manager.get_all_cluster_status.return_value = cluster_status
        mock_multi_cluster_manager.get_manager_stats.return_value = manager_stats
        
        response = client.get("/multi-cluster/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["overall_health"] == "degraded"  # Has both running and error clusters
        assert data["total_clusters"] == 2
        assert data["running_clusters"] == 1
        assert data["error_clusters"] == 1
    
    def test_get_multi_cluster_stats(self, client, mock_multi_cluster_manager):
        """Test getting multi-cluster statistics."""
        stats_data = {
            "manager_info": {"active_cluster_managers": 2},
            "registry_stats": {"total_clusters": 3},
            "operation_stats": {"total_operations": 1}
        }
        mock_multi_cluster_manager.get_manager_stats.return_value = stats_data
        
        response = client.get("/multi-cluster/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert data["manager_info"]["active_cluster_managers"] == 2
        assert data["registry_stats"]["total_clusters"] == 3


class TestErrorHandling:
    """Test error handling in API endpoints."""
    
    def test_cluster_not_found_error(self, client, mock_multi_cluster_manager):
        """Test handling of cluster not found errors."""
        mock_multi_cluster_manager.registry.get_cluster.side_effect = ClusterNotFoundError("test-cluster", [])
        
        response = client.get("/multi-cluster/clusters/test-cluster")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    def test_internal_server_error(self, client, mock_multi_cluster_manager):
        """Test handling of internal server errors."""
        mock_multi_cluster_manager.list_clusters.side_effect = Exception("Internal error")
        
        response = client.get("/multi-cluster/clusters")
        
        assert response.status_code == 500
        assert "Internal error" in response.json()["detail"]
    
    def test_validation_error_in_create_cluster(self, client, mock_multi_cluster_manager):
        """Test validation error in cluster creation."""
        from src.exceptions import ValidationError
        mock_multi_cluster_manager.create_cluster.side_effect = ValidationError("name", "test", "Invalid name")
        
        request_data = {
            "name": "",  # Invalid name
            "environment": "development"
        }
        
        response = client.post("/multi-cluster/clusters", json=request_data)
        
        assert response.status_code == 400
        assert "Invalid" in response.json()["detail"]


@pytest.mark.integration
class TestMultiClusterAPIIntegration:
    """Integration tests for multi-cluster API endpoints."""
    
    def test_full_cluster_lifecycle_via_api(self, client, mock_multi_cluster_manager, sample_cluster_definition):
        """Test complete cluster lifecycle through API."""
        # Setup mocks for full lifecycle
        mock_multi_cluster_manager.create_cluster.return_value = sample_cluster_definition
        mock_multi_cluster_manager.start_cluster.return_value = ServiceStatus.RUNNING
        mock_multi_cluster_manager.get_cluster_status.return_value = ServiceStatus.RUNNING
        mock_multi_cluster_manager.stop_cluster.return_value = True
        mock_multi_cluster_manager.delete_cluster.return_value = True
        
        # 1. Create cluster
        create_data = {
            "name": "Test Cluster",
            "environment": "development",
            "auto_start": False
        }
        response = client.post("/multi-cluster/clusters", json=create_data)
        assert response.status_code == 201
        
        # 2. Start cluster
        response = client.post("/multi-cluster/clusters/test-cluster/start")
        assert response.status_code == 200
        
        # 3. Check status
        response = client.get("/multi-cluster/clusters/test-cluster/status")
        assert response.status_code == 200
        assert response.json()["status"] == "running"
        
        # 4. Stop cluster
        response = client.post("/multi-cluster/clusters/test-cluster/stop")
        assert response.status_code == 200
        
        # 5. Delete cluster
        response = client.delete("/multi-cluster/clusters/test-cluster")
        assert response.status_code == 200
    
    def test_cross_cluster_operation_workflow(self, client, mock_cross_cluster_ops):
        """Test cross-cluster operation workflow through API."""
        # Setup mocks
        operation = CrossClusterOperation(
            id="operation-123",
            operation_type="migrate",
            source_cluster_id="source",
            target_cluster_ids=["target"],
            status=OperationStatus.RUNNING,
            progress_percent=25.0
        )
        
        mock_cross_cluster_ops.migrate_topics.return_value = "operation-123"
        mock_cross_cluster_ops.get_operation_status.return_value = operation
        mock_cross_cluster_ops.cancel_operation.return_value = True
        
        # 1. Start migration
        migration_data = {
            "source_cluster_id": "source-cluster",
            "target_cluster_id": "target-cluster",
            "dry_run": False
        }
        response = client.post("/multi-cluster/operations/migrate", json=migration_data)
        assert response.status_code == 200
        operation_id = response.json()["operation_id"]
        
        # 2. Check operation status
        response = client.get(f"/multi-cluster/operations/{operation_id}")
        assert response.status_code == 200
        assert response.json()["progress_percent"] == 25.0
        
        # 3. Cancel operation
        response = client.post(f"/multi-cluster/operations/{operation_id}/cancel")
        assert response.status_code == 200
        assert response.json()["success"] is True