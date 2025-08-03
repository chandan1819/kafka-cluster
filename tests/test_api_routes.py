"""Tests for FastAPI routes."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock

from src.main import app
from src.models.base import ServiceStatus
from src.models.cluster import ClusterStatus
from src.models.topic import TopicInfo
from src.models.message import ProduceResult, ConsumeResponse
from src.models.catalog import CatalogResponse


@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def mock_cluster_status():
    """Mock cluster status."""
    return ClusterStatus(
        status=ServiceStatus.RUNNING,
        broker_count=1,
        version="7.4.0",
        endpoints={"kafka": "localhost:9092"},
        uptime=3600
    )


@pytest.fixture
def mock_topic_info():
    """Mock topic info."""
    return TopicInfo(
        name="test-topic",
        partitions=3,
        replication_factor=1,
        size_bytes=1024
    )


@pytest.mark.unit
class TestGeneralEndpoints:
    """Test general API endpoints."""
    
    def test_root_endpoint(self, client):
        """Test root endpoint returns API information."""
        response = client.get("/")
        assert response.status_code == 200
        
        data = response.json()
        assert data["name"] == "Local Kafka Manager"
        assert data["version"] == "1.0.0"
        assert "endpoints" in data


@pytest.mark.unit
class TestServiceCatalogEndpoints:
    """Test service catalog endpoints."""
    
    @patch('src.api.routes.service_catalog.get_catalog')
    def test_get_catalog(self, mock_get_catalog, client, mock_cluster_status, mock_topic_info):
        """Test get service catalog endpoint."""
        # Setup mock
        mock_catalog = CatalogResponse(
            cluster=mock_cluster_status,
            topics=[mock_topic_info],
            available_apis=[],
            services={}
        )
        mock_get_catalog.return_value = mock_catalog
        
        # Make request
        response = client.get("/catalog")
        assert response.status_code == 200
        
        data = response.json()
        assert "cluster" in data
        assert "topics" in data
        assert len(data["topics"]) == 1
        
        # Test with force_refresh parameter
        response = client.get("/catalog?force_refresh=true")
        assert response.status_code == 200


@pytest.mark.unit
class TestClusterManagementEndpoints:
    """Test cluster management endpoints."""
    
    @patch('src.api.routes.cluster_manager.get_status')
    def test_get_cluster_status(self, mock_get_status, client, mock_cluster_status):
        """Test get cluster status endpoint."""
        mock_get_status.return_value = mock_cluster_status
        
        response = client.get("/cluster/status")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "running"
        assert data["broker_count"] == 1
    
    @patch('src.api.routes.cluster_manager.start_cluster')
    def test_start_cluster(self, mock_start_cluster, client, mock_cluster_status):
        """Test start cluster endpoint."""
        mock_start_cluster.return_value = mock_cluster_status
        
        # Test with default parameters
        response = client.post("/cluster/start")
        assert response.status_code == 200
        
        # Test with custom parameters
        request_data = {
            "force": True,
            "timeout": 120
        }
        response = client.post("/cluster/start", json=request_data)
        assert response.status_code == 200
    
    @patch('src.api.routes.cluster_manager.stop_cluster')
    def test_stop_cluster(self, mock_stop_cluster, client):
        """Test stop cluster endpoint."""
        mock_stop_cluster.return_value = True
        
        # Test with default parameters
        response = client.post("/cluster/stop")
        assert response.status_code == 200
        
        data = response.json()
        assert data["success"] is True
        
        # Test with custom parameters
        request_data = {
            "force": True,
            "cleanup": True,
            "timeout": 60
        }
        response = client.post("/cluster/stop", json=request_data)
        assert response.status_code == 200


@pytest.mark.unit
class TestTopicManagementEndpoints:
    """Test topic management endpoints."""
    
    @patch('src.api.routes.topic_manager.list_topics')
    def test_list_topics(self, mock_list_topics, client, mock_topic_info):
        """Test list topics endpoint."""
        mock_list_topics.return_value = [mock_topic_info]
        
        response = client.get("/topics")
        assert response.status_code == 200
        
        data = response.json()
        assert "topics" in data
        assert data["total_count"] == 1
        assert len(data["topics"]) == 1
        
        # Test with include_internal parameter
        response = client.get("/topics?include_internal=true")
        assert response.status_code == 200
    
    @patch('src.api.routes.topic_manager.create_topic')
    def test_create_topic(self, mock_create_topic, client):
        """Test create topic endpoint."""
        mock_create_topic.return_value = True
        
        request_data = {
            "name": "test-topic",
            "partitions": 3,
            "replication_factor": 1,
            "config": {"retention.ms": "86400000"}
        }
        
        response = client.post("/topics", json=request_data)
        assert response.status_code == 201
        
        data = response.json()
        assert data["success"] is True
        assert data["topic"] == "test-topic"
    
    @patch('src.api.routes.topic_manager.delete_topic')
    def test_delete_topic(self, mock_delete_topic, client):
        """Test delete topic endpoint."""
        mock_delete_topic.return_value = True
        
        response = client.delete("/topics/test-topic")
        assert response.status_code == 200
        
        data = response.json()
        assert data["success"] is True
        assert data["topic"] == "test-topic"
        
        # Test with force parameter
        response = client.delete("/topics/test-topic?force=true")
        assert response.status_code == 200


@pytest.mark.unit
class TestMessageOperationsEndpoints:
    """Test message operations endpoints."""
    
    @patch('src.api.routes.message_manager.produce_message')
    def test_produce_message(self, mock_produce_message, client):
        """Test produce message endpoint."""
        mock_result = ProduceResult(
            topic="test-topic",
            partition=0,
            offset=123,
            key="test-key",
            timestamp=1642234567000
        )
        mock_produce_message.return_value = mock_result
        
        request_data = {
            "topic": "test-topic",
            "key": "test-key",
            "value": {"message": "Hello, Kafka!"},
            "partition": 0
        }
        
        response = client.post("/produce", json=request_data)
        assert response.status_code == 201
        
        data = response.json()
        assert data["topic"] == "test-topic"
        assert data["partition"] == 0
        assert data["offset"] == 123
    
    @patch('src.api.routes.message_manager.consume_messages')
    def test_consume_messages(self, mock_consume_messages, client):
        """Test consume messages endpoint."""
        mock_response = ConsumeResponse(
            messages=[],
            consumer_group="test-group",
            topic="test-topic",
            total_consumed=0,
            has_more=False
        )
        mock_consume_messages.return_value = mock_response
        
        params = {
            "topic": "test-topic",
            "consumer_group": "test-group",
            "max_messages": 10,
            "timeout_ms": 5000
        }
        
        response = client.get("/consume", params=params)
        assert response.status_code == 200
        
        data = response.json()
        assert data["topic"] == "test-topic"
        assert data["consumer_group"] == "test-group"
        assert data["total_consumed"] == 0


@pytest.mark.unit
class TestHealthEndpoints:
    """Test health endpoints."""
    
    @patch('src.api.routes.cluster_manager.get_status')
    def test_health_check(self, mock_get_status, client, mock_cluster_status):
        """Test health check endpoint."""
        mock_get_status.return_value = mock_cluster_status
        
        response = client.get("/health")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "local-kafka-manager"
        assert "cluster_status" in data


@pytest.mark.unit
class TestErrorHandling:
    """Test error handling."""
    
    def test_validation_error(self, client):
        """Test request validation error handling."""
        # Send invalid data for topic creation
        invalid_data = {
            "name": "",  # Empty name should fail validation
            "partitions": -1  # Negative partitions should fail
        }
        
        response = client.post("/topics", json=invalid_data)
        assert response.status_code == 422
        
        data = response.json()
        assert data["error"] == "VALIDATION_ERROR"
        assert "errors" in data["details"]
    
    def test_not_found_error(self, client):
        """Test 404 error handling."""
        response = client.get("/nonexistent-endpoint")
        assert response.status_code == 404
        
        data = response.json()
        assert data["error"] == "HTTP_404"